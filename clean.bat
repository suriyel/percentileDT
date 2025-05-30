@echo off
echo 清理项目缓存和生成文件...

if exist "target" (
    echo 删除 target 目录...
    rmdir /s /q target
)

if exist "project\target" (
    echo 删除 project\target 目录...
    rmdir /s /q project\target
)

if exist "data" (
    echo 清理数据文件...
    del /q data\*.parquet 2>nul
)

if exist "results" (
    echo 清理结果文件...
    del /q results\*.csv 2>nul
)

echo 清理SBT缓存...
sbt clean >nul 2>&1

echo ✅ 清理完成！
pause