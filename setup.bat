@echo off
echo Spark Percentile Benchmark Setup
echo ================================
echo.

REM 检查Java
echo Checking Java installation...
java -version
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Java is not installed or not in PATH
    echo Please install Java 8 or 11 from: https://adoptium.net/
    echo.
    pause
    exit /b 1
)

REM 检查SBT
echo.
echo Checking SBT installation...
sbt --version
if %errorlevel% neq 0 (
    echo.
    echo ERROR: SBT is not installed or not in PATH
    echo Please install SBT from: https://www.scala-sbt.org/download.html
    echo.
    pause
    exit /b 1
)

echo.
echo Environment check completed successfully!
echo You can now run the benchmark using: run.bat
echo.
pause