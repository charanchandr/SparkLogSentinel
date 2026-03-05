"""
Phase 3: Apache Spark Structured Streaming - Enterprise Database Analytics
----------------------------------------------------------------------------------
This script is the core of the pipeline diagram. It runs as a distributed Spark application.
It connects to an Apache Kafka broker, streams incoming web logs, parses them, 
aggregates them via a sliding window, and finally writes anomaly alerts into 
a reliable PostgreSQL Output Sink.
"""
import os

# Critical Windows Fix: Force PySpark to download and load external packages BEFORE JVM starts
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.6.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, window, col, from_json, current_timestamp, lit
)

# ──────────────────────────────────────────────────────────────────────────────
# ENTERPRISE COMPONENT CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "server_logs"

JDBC_URL  = "jdbc:postgresql://localhost:5432/analytics_db"
JDBC_USER = "spark_admin"
JDBC_PASS = "spark_password"

THRESHOLD = 50
WINDOW_DURATION = "5 minutes"
SLIDE_INTERVAL  = "1 minute"
CHECKPOINT_DIR  = "/tmp/spark_checkpoint_enterprise"

# Apache Common Log Format Regex
LOG_PATTERN = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) \S+" (\d{3}) (\S+)'
IDX_IP=1; IDX_TS=2; IDX_METHOD=3; IDX_PATH=4; IDX_STATUS=5; IDX_BYTES=6


def create_spark_session():
    """Create Spark session injecting Kafka and Postgres dependencies."""
    return (
        SparkSession.builder
        .appName("EnterpriseLogAnalytics")
        .master("local[*]")
        # Instruct Spark to dynamically download the Kafka & Postgres dependencies
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,org.postgresql:postgresql:42.6.0")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED")
        .getOrCreate()
    )


def read_kafka_stream(spark):
    """
    Phase 2, Step 1 - Ingestion Layer (Kafka Source)
    Connects to the enterprise message queue instead of a raw socket.
    """
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    # Kafka payloads come back as binary; cast the 'value' column to string
    return raw_kafka_df.selectExpr("CAST(value AS STRING) as value", "timestamp")


def parse_logs(raw_df):
    """Distributed Regex Pattern Matching (Map Phase)"""
    parsed_df = raw_df.select(
        col("timestamp").alias("event_time"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_IP).alias("ip"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_METHOD).alias("method"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_PATH).alias("path"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_STATUS).alias("status_code"),
        regexp_extract(col("value"), LOG_PATTERN, IDX_BYTES).alias("bytes"),
        col("value").alias("raw_log")
    )
    return parsed_df.filter(col("status_code") != "")


def apply_window_aggregation(parsed_df):
    """Stateful Sliding Window Aggregation"""
    return (
        parsed_df
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_INTERVAL),
            col("status_code")
        )
        .count()
    )


def detect_anomalies(windowed_df):
    """Threshold-Based Anomaly Detection"""
    return (
        windowed_df
        .filter((col("status_code") == "500") & (col("count") > THRESHOLD))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("status_code"),
            col("count").alias("error_count"),
            current_timestamp().alias("detected_at")
        )
    )


def write_to_postgres(batch_df, batch_id):
    """
    Callback for foreachBatch output sink.
    Instead of printing to the terminal, this opens a JDBC connection 
    and inserts the anomalies into the Dockerized PostgreSQL database.
    """
    if batch_df.count() == 0:
        print(f"\n[Batch {batch_id}] No anomalies detected in Kafka stream.")
        return

    print(f"\n[Batch {batch_id}] ⚠️  ANOMALIES DETECTED! Writing to PostgreSQL Database...")
    batch_df.show(truncate=False)

    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "anomalies") # It auto-creates this table
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    print(" -> Successfully saved to database!")


def main():
    print("=" * 70)
    print("  Enterprise Anomaly Analytics - Spark -> Postgres")
    print(f"  Kafka: {KAFKA_BROKER}/{KAFKA_TOPIC} | DB: {JDBC_URL}")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # The Pipeline
    raw_stream = read_kafka_stream(spark)
    parsed_df = parse_logs(raw_stream)
    windowed_df = apply_window_aggregation(parsed_df)
    anomaly_df = detect_anomalies(windowed_df)

    # Sink to PostgreSQL
    query = (
        anomaly_df.writeStream
        .outputMode("complete")
        .foreachBatch(write_to_postgres)
        .trigger(processingTime="2 seconds")
        .start()
    )

    print("\n  Streaming query started. Waiting for Kafka messages...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
