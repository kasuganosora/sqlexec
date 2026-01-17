@echo off
REM 性能监控和优化测试脚本

echo ======================================
echo 编译性能监控测试程序
echo ======================================

REM 编译 example_monitor.go
go build -o example_monitor.exe example_monitor.go
if %errorlevel% neq 0 (
    echo 编译 example_monitor.exe 失败
    exit /b 1
)
echo [OK] example_monitor.exe 编译成功

REM 编译 test_performance_benchmark.go
go build -o test_performance_benchmark.exe test_performance_benchmark.go
if %errorlevel% neq 0 (
    echo 编译 test_performance_benchmark.exe 失败
    exit /b 1
)
echo [OK] test_performance_benchmark.exe 编译成功

echo.
echo ======================================
echo 运行性能监控测试
echo ======================================

echo.
echo [运行] example_monitor.exe
example_monitor.exe
if %errorlevel% neq 0 (
    echo 运行 example_monitor.exe 失败
    exit /b 1
)

echo.
echo ======================================
echo 运行性能基准测试
echo ======================================

echo.
echo [运行] test_performance_benchmark.exe
test_performance_benchmark.exe
if %errorlevel% neq 0 (
    echo 运行 test_performance_benchmark.exe 失败
    exit /b 1
)

echo.
echo ======================================
echo 所有测试完成！
echo ======================================
pause
