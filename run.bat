@echo off
setlocal enabledelayedexpansion

echo ==========================================================
echo  Starting Pre-flight Checks (Docker Compose ^& Kafka)
echo ==========================================================

:: 1. Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running or not found in PATH.
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

:: 2. Check if Docker Compose services are running
echo Checking Docker Compose services...
for /f %%i in ('docker compose ps --status running -q 2^>nul ^| find /c /v ""') do set RUNNING_CONTAINERS=%%i

if "%RUNNING_CONTAINERS%"=="0" (
    echo Containers not running. Starting Docker Compose...
    docker compose up -d
) else (
    echo Docker Compose services are already running.
)

:: 3. Wait for Kafka on localhost:9092
echo Waiting for Kafka broker on localhost:9092 to be ready...
set MAX_RETRIES=30
set RETRY_COUNT=0
set KAFKA_READY=0

:check_kafka
powershell -Command "$client = New-Object System.Net.Sockets.TcpClient; try { $client.Connect('127.0.0.1', 9092); Write-Output 'OPEN'; $client.Close() } catch { Write-Output 'CLOSED' }" | findstr "OPEN" >nul
if not errorlevel 1 (
    echo Kafka broker is ready and listening on port 9092!
    goto start_app
)

set /a RETRY_COUNT+=1
if %RETRY_COUNT% geq %MAX_RETRIES% (
    echo [WARNING] Kafka did not respond on port 9092 within the timeout.
    echo Proceeding to start application anyway...
    goto start_app
)

echo    Kafka not ready yet (attempt %RETRY_COUNT%/%MAX_RETRIES%^)... waiting 2s
timeout /t 2 /nobreak >nul
goto check_kafka

:start_app
echo ==========================================================
echo  Launching Spring Boot Application...
echo ==========================================================

if exist "target\hello-world-0.0.1-SNAPSHOT.jar" (
    java -jar target\hello-world-0.0.1-SNAPSHOT.jar
) else (
    mvn spring-boot:run
)
