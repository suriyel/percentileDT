@echo off
setlocal EnableDelayedExpansion

echo ========================================
echo Spark Percentile Benchmark - 启动脚本
echo ========================================
echo.

REM 设置编码为UTF-8
chcp 65001 >nul

REM 检查Java环境
echo [1/5] 检查Java环境...
java -version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ 错误: 未找到Java。请安装Java 8或11。
    echo 下载地址: https://adoptium.net/temurin/releases/
    pause
    exit /b 1
)
echo ✅ Java环境正常

REM 检查SBT
echo [2/5] 检查SBT环境...
sbt --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ 错误: 未找到SBT。请安装SBT。
    echo 下载地址: https://www.scala-sbt.org/download.html
    pause
    exit /b 1
)
echo ✅ SBT环境正常

REM 创建必要的目录
echo [3/5] 创建工作目录...
if not exist "data" mkdir data
if not exist "results" mkdir results
echo ✅ 目录创建完成

REM 清理缓存（如果之前有问题）
if exist "target" (
    echo [额外] 检测到之前的构建，清理缓存...
    sbt clean >nul 2>&1
)

REM 设置JVM参数
set "JAVA_OPTS=-Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
set "SBT_OPTS=-Xmx4g -XX:+UseG1GC"

echo [4/5] 编译项目...
echo 正在下载依赖和编译代码，首次运行可能需要几分钟...

REM 尝试编译，如果失败则重试
set attempts=0
:compile_loop
set /a attempts+=1
echo 编译尝试 %attempts%/3...

sbt compile
if %errorlevel% equ 0 (
    echo ✅ 编译成功
    goto run_program
)

if %attempts% lss 3 (
    echo ⚠️  编译失败，正在重试...
    timeout /t 5 >nul
    goto compile_loop
)

echo ❌ 编译失败!
echo 可能的解决方案:
echo 1. 检查网络连接
echo 2. 运行 sbt clean 清理缓存
echo 3. 检查防火墙设置
echo 4. 尝试使用VPN或代理
pause
exit /b 1

:run_program
echo.
echo [5/5] 运行性能测试...
echo ========================================
echo 🚀 开始运行Spark分位点性能测试
echo ⏱️  这可能需要10-30分钟，取决于您的硬件配置
echo 💾 测试数据将保存到 data/ 目录
echo 📊 结果将保存到 results/ 目录
echo ========================================
echo.

sbt "run"

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo ✅ 测试完成！
    echo 📁 请查看 results 文件夹中的测试结果
    echo 📊 结果文件: benchmark_results_*.csv
    echo ========================================
) else (
    echo.
    echo ❌ 程序运行出错
    echo 请检查错误信息并重试
)

echo.
echo 按任意键退出...
pause >nul