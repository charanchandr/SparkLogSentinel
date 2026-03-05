# ⚡ SparkLogSentinel — Enterprise Edition

**Real-Time Log Analytics & Anomaly Detection**
*Built with Apache Kafka → Apache Spark Structured Streaming → PostgreSQL*

SparkLogSentinel is a production-grade streaming data pipeline that monitors live web server traffic, detects HTTP 500 error bursts using a sliding-window algorithm, and permanently stores anomaly alerts inside a relational database.

---

## 🏗️ Architecture

This project replicates the enterprise **Kafka → Spark Cluster → Reliable Storage** pattern:

```
[anomaly_injector.py]              [Docker Enterprise Cluster]               [Output Sink]
   Kafka Producer          →    Kafka Broker → Spark Cluster (1 Master,    →  PostgreSQL DB
(Publishes web logs                              2 Workers) → JDBC Write         (anomalies table)
 to 'server_logs' topic)
```

| Component | Technology | Role |
|---|---|---|
| Data Source | `anomaly_injector.py` + Kafka | Streams simulated web logs into a message queue |
| Message Queue | Apache Kafka (Docker) | Buffers and reliably delivers logs to Spark |
| Stream Processor | `log_analytics.py` + Spark Cluster | Parses, aggregates, detects anomalies |
| Reliable Storage | PostgreSQL (Docker) | Permanently stores all detected anomaly alerts |
| Orchestration | Docker Compose | Manages all infrastructure containers |

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (running)
- Python 3.x with `pyspark` and `kafka-python-ng` installed

### 1. Start the Enterprise Cluster
```powershell
cd "case study\enterprise"
docker compose up -d
```
Wait ~30 seconds for Kafka, Spark, and Postgres to fully boot.

### 2. Run the Kafka Log Injector (Terminal 1)
```powershell
python anomaly_injector.py
```
This publishes 100 normal logs, followed by an 80-error HTTP 500 crash burst, directly into Kafka.

### 3. Run the Spark Analytics Engine (Terminal 2)
```powershell
$env:HADOOP_HOME="C:\hadoop"; python log_analytics.py
```
Spark reads from Kafka, detects the anomaly using a 5-minute sliding window, and writes the alert into PostgreSQL.

### 4. Query the Database
```powershell
docker exec -it enterprise-postgres-1 psql -U spark_admin -d analytics_db -c "SELECT * FROM anomalies;"
```

### Stop the Cluster
```powershell
docker compose down
```

---

## 📁 Project Structure
```
case study/
├── anomaly_injector.py       # Kafka Producer (Data Source)
├── log_analytics.py          # PySpark Streaming Engine
├── postgresql-42.6.0.jar     # Postgres JDBC Driver for Spark
├── spark-sql-kafka-*.jar     # Kafka Integration for Spark
└── enterprise/
    └── docker-compose.yml    # Full enterprise cluster definition
```

---
*Case Study — Real-Time Distributed Systems with Apache Spark*
