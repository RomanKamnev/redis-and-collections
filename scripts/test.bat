@echo off
REM Automated test script for Redis Collections (Windows)
REM This script:
REM 1. Starts Redis Cluster
REM 2. Waits for initialization
REM 3. Runs tests
REM 4. Cleans up (stops cluster)

setlocal enabledelayedexpansion

echo === Redis Collections Test Runner ===
echo.

REM Check if docker-compose is available
where docker-compose >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: docker-compose is not installed
    exit /b 1
)

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Docker is not running
    exit /b 1
)

REM Start Redis Cluster
echo Starting Redis Cluster...
docker-compose -f infra/docker-compose.yml up -d
if %errorlevel% neq 0 (
    echo Error: Failed to start Redis Cluster
    exit /b 1
)

REM Wait for cluster initialization
echo Waiting for Redis Cluster initialization (30 seconds)...
timeout /t 30 /nobreak >nul

REM Verify cluster is ready
echo Verifying cluster status...
docker exec infra-redis-node-1-1 redis-cli -p 7000 cluster info | findstr "cluster_state:ok" >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Redis Cluster failed to initialize
    echo Check logs: docker-compose -f infra/docker-compose.yml logs redis-cluster-init
    docker-compose -f infra/docker-compose.yml down -v
    exit /b 1
)

echo Redis Cluster is ready!
echo.

REM Show cluster info
echo Cluster Info:
docker exec infra-redis-node-1-1 redis-cli -p 7000 cluster nodes

REM Run tests
echo.
echo Running tests...
echo.
call mvn test

set TEST_RESULT=%errorlevel%

if %TEST_RESULT% equ 0 (
    echo.
    echo Running demo application (mvn exec:java)...
    echo.
    call mvn exec:java
    set TEST_RESULT=%errorlevel%
)

REM Cleanup
echo.
echo Stopping Redis Cluster...
docker-compose -f infra/docker-compose.yml down -v

if %TEST_RESULT% equ 0 (
    echo.
    echo Tests completed successfully!
) else (
    echo.
    echo Tests failed!
)

exit /b %TEST_RESULT%
