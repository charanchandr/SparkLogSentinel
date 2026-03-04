"""
Phase 3: Anomaly Injector — Simulates a Server Crash (Spike of 500 Errors)
---------------------------------------------------------------------------
This script is an ENHANCED version of stream_simulator.py.
It streams normal logs first, then injects a sudden burst of HTTP 500 errors
to trigger the anomaly detection in log_analytics.py.

Use this INSTEAD of stream_simulator.py for a more dramatic demo.

Run BEFORE starting log_analytics.py:
    Terminal 1: python anomaly_injector.py
    Terminal 2: python log_analytics.py
"""

import socket
import time
import random
from datetime import datetime

HOST = 'localhost'
PORT = 9999

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
    return f'{ip} - - [{ts}] "{method} {path} HTTP/1.1" {status_code} {size}\n'

def stream_logs(conn):
    """Stream normal traffic, then inject a burst of 500 errors."""

    print(f"\n[Phase A] Sending {NORMAL_LOG_COUNT} normal log lines (200 OK)...")
    for i in range(NORMAL_LOG_COUNT):
        line = make_log_line(200)
        conn.sendall(line.encode('utf-8'))
        time.sleep(NORMAL_DELAY)
    print("[Phase A] Done. Normal traffic complete.\n")

    print(f"[Phase B] ⚠️  INJECTING {BURST_ERROR_COUNT} HTTP 500 errors (SIMULATED CRASH)...")
    offending_ip = "66.249.73.135"  # Simulate one bad actor / crashing endpoint
    for i in range(BURST_ERROR_COUNT):
        line = make_log_line(500, ip=offending_ip)
        conn.sendall(line.encode('utf-8'))
        time.sleep(BURST_DELAY)
    print("[Phase B] Done. Burst of errors sent.\n")

    print("[Phase C] Resuming normal traffic (recovery)...")
    for i in range(50):
        line = make_log_line(200)
        conn.sendall(line.encode('utf-8'))
        time.sleep(NORMAL_DELAY)
    print("[Phase C] Done. Stream complete.")


def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(1)
        print("=" * 60)
        print("  Anomaly Injector — Waiting for Spark to connect...")
        print(f"  Listening on {HOST}:{PORT}")
        print("=" * 60)

        conn, addr = server_socket.accept()
        print(f"\n✅ Spark connected from {addr}. Starting stream...\n")
        stream_logs(conn)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        server_socket.close()
        print("\nServer socket closed.")


if __name__ == '__main__':
    main()
