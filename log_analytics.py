"""
Phase 2: Apache Spark Structured Streaming - Log Analytics & Anomaly Detection
----------------------------------------------------------------------------------
This script connects to a local socket stream (localhost:9999), reads incoming
web server log lines in real-time, parses them using regex, applies a sliding
window aggregation, and alerts when the 500-error count exceeds a threshold.

Run this after starting stream_simulator.py in a separate terminal:
    Terminal 1: python stream_simulator.py
    Terminal 2: spark-submit log_analytics.py
              OR: python log_analytics.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, window, col, count, current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
STREAM_HOST = "localhost"
STREAM_PORT = 9999

# Anomaly detection threshold: alert if 500 errors exceed this in any window
THRESHOLD = 50

# Window duration and slide interval for stateful aggregation
WINDOW_DURATION = "5 minutes"
SLIDE_INTERVAL  = "1 minute"

# Checkpoint location for fault-tolerant stateful processing
CHECKPOINT_DIR = "/tmp/spark_checkpoint"

# ──────────────────────────────────────────────────────────────────────────────
# APACHE COMMON LOG FORMAT (CLF) REGEX PATTERN
# Example log line:
#   83.149.9.216 - - [17/May/2015:10:05:21 +0000] "GET /index.html HTTP/1.1" 500 1234
# ──────────────────────────────────────────────────────────────────────────────
LOG_PATTERN = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) \S+" (\d{3}) (\S+)'

# Capture group indices (1-based for regexp_extract)
IDX_IP      = 1
IDX_TS      = 2
IDX_METHOD  = 3
IDX_PATH    = 4
IDX_STATUS  = 5
IDX_BYTES   = 6


def create_spark_session():
    """Create and configure the SparkSession for local streaming."""
    return (
        SparkSession.builder
        .appName("RealTimeLogAnalytics")
        # Run locally using all available CPU cores
        .master("local[*]")
        # Bypass hostname resolution issues (e.g. underscores in hostname)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        # Reduce shuffle partitions for local mode efficiency
        .config("spark.sql.shuffle.partitions", "4")
        # Java 17/21 compatibility flags for Spark on newer JDKs
        .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED")
        .getOrCreate()
    )


def read_socket_stream(spark):
    """
    Phase 2, Step 1 — Ingestion Layer
    Attach to the socket stream from stream_simulator.py.
    Each received line becomes a row in the DataFrame.
    """
    raw_stream = (
        spark.readStream
        .format("socket")
        .option("host", STREAM_HOST)
        .option("port", STREAM_PORT)
        # Ask Spark to include a timestamp for each received line
        .option("includeTimestamp", "true")
        .load()
    )
    return raw_stream


def parse_logs(raw_df):
    """
    Phase 2, Step 2 — Distributed Regex Pattern Matching (Map Phase)
    Apply regexp_extract to each row in parallel across CPU cores.
    This transforms raw unstructured strings into a structured DataFrame.
    """
    parsed_df = raw_df.select(
        # 'timestamp' col comes from socket source when includeTimestamp=true
        col("timestamp").alias("event_time"),

        # Distributively extract fields from each log line
        regexp_extract(col("value"), LOG_PATTERN, IDX_IP).alias("ip"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_METHOD).alias("method"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_PATH).alias("path"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_STATUS).alias("status_code"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_BYTES).alias("bytes"),

        # Keep the raw line for debugging
        col("value").alias("raw_log")
    )

    # Filter out lines that didn't match the regex (empty status)
    filtered_df = parsed_df.filter(col("status_code") != "")
    return filtered_df


def apply_window_aggregation(parsed_df):
    """
    Phase 2, Step 3 — Stateful Sliding Window Aggregation
    Group log events into time-based sliding windows and count 500 errors.

    Window: 5-minute window sliding every 1 minute.
    This means Spark maintains rolling state in memory for each active window.
    When a new event arrives, Spark adds it to all overlapping windows.
    When a window expires, its results are finalized and state is dropped.
    """
    windowed_counts = (
        parsed_df
        .groupBy(
            # time-based sliding window on the event timestamp from the log
            window(col("event_time"), WINDOW_DURATION, SLIDE_INTERVAL),
            col("status_code")
        )
        .count()
        .orderBy("window", "status_code")
    )
    return windowed_counts


def detect_anomalies(windowed_df):
    """
    Phase 2, Step 4 — Threshold-Based Anomaly Detection
    Filter windows where 500-error count exceeds the THRESHOLD.
    Returns a stream of anomaly alerts.
    """
    anomalies = (
        windowed_df
        .filter(
            (col("status_code") == "500") &
            (col("count") > THRESHOLD)
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("status_code"),
            col("count").alias("error_count"),
            lit(f"⚠️  ANOMALY: {THRESHOLD}+ server errors detected!").alias("alert")
        )
    )
    return anomalies


def process_batch(batch_df, batch_id):
    """
    Callback for foreachBatch output sink.
    Called for each micro-batch (every ~2 seconds of data).
    Prints all rows and highlights anomalies.
    """
    if batch_df.count() == 0:
        print(f"\n[Batch {batch_id}] No anomalies in this window.")
        return

    print(f"\n{'='*70}")
    print(f"  BATCH {batch_id} — ANOMALY ALERT(S) DETECTED")
    print(f"{'='*70}")
    batch_df.show(truncate=False)


def main():
    print("=" * 70)
    print("  Real-Time Log Analytics & Anomaly Detection — Apache Spark")
    print("=" * 70)
    print(f"  Connecting to stream at {STREAM_HOST}:{STREAM_PORT}...")
    print(f"  Window: {WINDOW_DURATION} | Slide: {SLIDE_INTERVAL}")
    print(f"  Alert threshold: {THRESHOLD} x HTTP 500 errors per window")
    print("=" * 70)

    # Step 1: Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  # Suppress verbose Spark logs

    # Step 2: Ingest raw stream from socket
    raw_stream = read_socket_stream(spark)

    # Step 3: Parse raw text into structured columns (distributed map)
    parsed_df = parse_logs(raw_stream)

    # Step 4: Apply sliding window aggregation (stateful reduce)
    windowed_df = apply_window_aggregation(parsed_df)

    # Step 5: Filter to anomaly conditions only
    anomaly_df = detect_anomalies(windowed_df)

    # Step 6: Start the streaming query with console output
    # Trigger: process a micro-batch every 2 seconds
    query = (
        anomaly_df.writeStream
        .outputMode("complete")   # Re-output full aggregated state every batch
        .format("console")        # Print results to the terminal
        .option("truncate", "false")
        .trigger(processingTime="2 seconds")
        .start()
    )

    print("\n  Streaming query started. Waiting for logs...")
    print("  (Press Ctrl+C to stop)\n")

    # Keep the stream running until manually stopped
    query.awaitTermination()


if __name__ == "__main__":
    main()
