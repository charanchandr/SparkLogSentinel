@echo off
echo =======================================================
echo   Starting Apache Spark Log Analytics Project Demo
echo =======================================================

echo.
echo Cleaning up previous Spark streaming state...
if exist "C:\tmp\spark_checkpoint" rmdir /s /q "C:\tmp\spark_checkpoint"
if exist "\tmp\spark_checkpoint" rmdir /s /q "\tmp\spark_checkpoint"

echo [1/2] Starting Log Anomaly Injector (Background Window)...
start "Anomaly Injector" powershell -NoExit -Command "Set-Location '%~dp0'; Write-Host 'Starting Injector...'; & 'C:\Python314\python.exe' anomaly_injector.py"

echo [2/2] Starting Spark Log Analytics Engine...
start "Spark Analytics" powershell -NoExit -Command "Set-Location '%~dp0'; $env:HADOOP_HOME='C:\hadoop'; Write-Host 'Starting Spark...'; & 'C:\Python314\python.exe' log_analytics.py"

echo.
echo Both services are now running in their own windows!
pause
