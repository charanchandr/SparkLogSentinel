# ⚡ SparkLogSentinel

**Real-Time Log Analytics & Anomaly Detection with Apache Spark**

SparkLogSentinel is a robust, real-time streaming analytics pipeline that monitors web server traffic and detects simulated DoS attacks (HTTP 500 error bursts) within milliseconds using Apache Spark Structured Streaming.

---

## 🏗️ Architecture Overview

This project consists of two core components running in tandem:

1. **The Log Anomaly Injector** (`anomaly_injector.py`): 
   A Python daemon that acts as a web server, streaming simulated access logs over a raw TCP socket. It normally streams healthy `HTTP 200` traffic but is programmed to abruptly inject a severe simulated server crash (a burst of 80x `HTTP 500` errors) to test the downstream analytics engine.

2. **The Spark Analytics Engine** (`log_analytics.py`):
   A PySpark Structured Streaming application. It ingests the unbounded log stream, parses the Apache Common Log Format using distributed regular expressions, and applies a **5-minute sliding window** (sliding every 1 minute) to aggregate traffic status codes. If it detects more than 50 server errors in any single window, it throws a real-time anomaly alert.

---

## 🚀 Quick Start Guide (Windows)

This project has been pre-configured to run easily on Windows machines leveraging a helper batch script that bypasses common PySpark Windows environment bugs.

### Prerequisites
* **Python 3.x**
* **Apache Spark** (PySpark module installed via `pip install pyspark`)
* **Java 17+** (Ensure `JAVA_HOME` is set)
* **Hadoop WinUtils** (Automatically handled by the start script)

### Running the Demo

1. Clone this repository to your local machine.
2. Navigate to the project directory.
3. Double-click the **`run_demo.bat`** file.

*Alternatively, from a terminal:*
```powershell
.\run_demo.bat
```

### What Happens Next?
The batch script will safely clean up any old Spark checkpoint data to prevent offset mismatch crashes and will simultaneously launch **two PowerShell windows**:

*   **Window 1 (The Server):** Starts streaming logs and visually indicates when the HTTP 500 attack phase begins.
*   **Window 2 (The Sentinel):** Boots the Apache Spark engine. Watch this window as it processes the micro-batches. When the attack phase hits, you will see a table output a critical `⚠️ ANOMALY: 50+ server errors detected!` alert.

---

## 🛠️ Built With

*   **Apache Spark (Structured Streaming)** - State-of-the-art micro-batch streaming engine.
*   **Py4J / PySpark** - The Python API for Spark.
*   **Python TCP Sockets** - For high-throughput log simulation.

---
*Created as a Case Study for Real-Time Distributed Systems Analysis.*
