@echo off
echo ============================================================
echo   SparkLogSentinel Enterprise Edition
echo   Kafka + Apache Spark Cluster + PostgreSQL
echo ============================================================

echo [1/3] Starting Enterprise Docker Cluster (Kafka, Spark, Postgres)...
cd /d "%~dp0enterprise"
docker compose up -d
timeout /t 20 /nobreak >nul
echo Docker cluster is up!

echo [2/3] Launching Spark Analytics Engine (reads from Kafka)...
start "Spark Analytics" powershell -NoExit -Command "Set-Location '%~dp0'; $env:HADOOP_HOME='C:\hadoop'; Write-Host 'Starting Spark -> Kafka -> Postgres pipeline...'; & 'C:\Python314\python.exe' log_analytics.py"

echo [3/3] Launching Kafka Log Injector (publishes simulated web traffic)...
timeout /t 10 /nobreak >nul
start "Anomaly Injector" powershell -NoExit -Command "Set-Location '%~dp0'; Write-Host 'Starting Kafka Producer...'; & 'C:\Python314\python.exe' anomaly_injector.py"

echo.
echo ============================================================
echo   All systems running!
echo   Spark Web UI: http://localhost:8080
echo   View DB:  docker exec -it enterprise-postgres-1 psql -U spark_admin -d analytics_db -c "SELECT * FROM anomalies;"
echo   Stop All: docker compose -f enterprise\docker-compose.yml down
echo ============================================================
