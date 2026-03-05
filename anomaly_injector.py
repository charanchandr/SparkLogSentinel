"""
Phase 2: Anomaly Injector — Enterprise Kafka Producer
---------------------------------------------------------------------------
This script acts as the 'Data Source' in the enterprise architecture diagram.
Instead of an archaic raw TCP socket, it publishes simulated web traffic 
directly into an Apache Kafka topic ('server_logs').

Dependencies:
    pip install kafka-python-ng
"""

import time
import random
from datetime import datetime
import json
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'server_logs'

# ── Configurable parameters ──────────────────────────────────────────────────
NORMAL_LOG_COUNT   = 100   # Normal traffic logs to send first
BURST_ERROR_COUNT  = 80    # Number of 500 errors to inject (exceeds threshold of 50)
NORMAL_DELAY       = 0.05  # Seconds between normal log lines
BURST_DELAY        = 0.01  # Seconds between burst error lines (fires fast)
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_IPS = [
    "192.168.1.10", "10.0.0.5", "172.16.0.3",
    "83.149.9.216",  "66.249.73.135", "46.105.14.53"
]

SAMPLE_PATHS = [
    "/index.html", "/api/users", "/static/main.js",
    "/api/products", "/login", "/dashboard", "/favicon.ico"
]

def make_log_line(status_code, ip=None):
    """Generate a synthetic Apache CLF log line."""
    ip     = ip or random.choice(SAMPLE_IPS)
    ts     = datetime.now().strftime("%d/%b/%Y:%H:%M:%S +0000")
    method = "GET"
    path   = random.choice(SAMPLE_PATHS)
    size   = random.randint(200, 9999)
    return f'{ip} - - [{ts}] "{method} {path} HTTP/1.1" {status_code} {size}'

def stream_logs(producer):
    """Publish logs directly to the Apache Kafka Broker."""

    print(f"\n[Phase A] Publishing {NORMAL_LOG_COUNT} normal log lines to Kafka topic '{KAFKA_TOPIC}'...")
    for i in range(NORMAL_LOG_COUNT):
        line = make_log_line(200)
        producer.send(KAFKA_TOPIC, line.encode('utf-8'))
        time.sleep(NORMAL_DELAY)
    print("[Phase A] Done. Normal traffic complete.\n")

    print(f"[Phase B] ⚠️  INJECTING {BURST_ERROR_COUNT} HTTP 500 errors (SIMULATED CRASH)...")
    offending_ip = "66.249.73.135"
    for i in range(BURST_ERROR_COUNT):
        line = make_log_line(500, ip=offending_ip)
        producer.send(KAFKA_TOPIC, line.encode('utf-8'))
        time.sleep(BURST_DELAY)
    print("[Phase B] Done. Burst of errors sent.\n")

    print("[Phase C] Resuming normal traffic (recovery)...")
    for i in range(50):
        line = make_log_line(200)
        producer.send(KAFKA_TOPIC, line.encode('utf-8'))
        time.sleep(NORMAL_DELAY)
    
    # Ensure all asynchronous messages are actually sent before closing
    producer.flush()
    print("[Phase C] Done. Stream complete.")


def main():
    print("=" * 60)
    print("  Enterprise Anomaly Injector — Apache Kafka Producer")
    print(f"  Target Broker: {KAFKA_BROKER}")
    print("=" * 60)

    try:
        # Connect to the Kafka container hosted by Docker
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print(f"\n✅ Successfully connected to Kafka Broker at {KAFKA_BROKER}!\n")
        
        stream_logs(producer)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\n❌ Failed to connect to Kafka: {e}")
        print("Ensure 'docker compose up -d' is running and Kafka is healthy.")
    finally:
        print("\nProducer shutdown.")


if __name__ == '__main__':
    main()
