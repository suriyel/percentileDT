@echo off
echo Quick Test - Small Dataset
echo ==========================
echo.

if not exist "data" mkdir data
if not exist "results" mkdir results

echo Compiling...
sbt compile

if %errorlevel% neq 0 (
    echo Compilation failed!
    pause
    exit /b 1
)

echo.
echo Running quick test with 1M records...
sbt "runMain com.example.percentile.Main 1000000"

echo.
echo Quick test completed!
pause