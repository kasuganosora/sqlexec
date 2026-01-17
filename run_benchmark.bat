@echo off
echo === 运行性能基准测试 ===
echo.

echo 正在编译测试程序...
go build test_benchmark_full.go
if %errorlevel% neq 0 (
    echo 编译失败！
    exit /b 1
)

echo.
echo 开始执行基准测试...
echo.

.\test_benchmark_full.exe

echo.
echo === 测试完成 ===
pause
