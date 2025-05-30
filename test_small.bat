@echo off
echo 小数据量测试 (10万条记录)
echo =============================
echo.

if not exist "data" mkdir data
if not exist "results" mkdir results

echo 检查环境...
java -version >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误: Java未安装
    pause
    exit /b 1
)

sbt --version >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误: SBT未安装
    pause
    exit /b 1
)

echo 编译项目...
sbt compile

if %errorlevel% neq 0 (
    echo 编译失败!
    pause
    exit /b 1
)

echo.
echo 运行小数据量测试 (10万条记录)...
echo 预计耗时: 1-3分钟
echo.

sbt "run 100000"

echo.
echo 小数据量测试完成!
echo 查看 results/ 目录中的结果文件
pause
