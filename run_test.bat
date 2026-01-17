@echo off
echo ========================================
echo 运行更新后的 COM_STMT_EXECUTE 测试
echo ========================================
echo.

cd mysql

echo 正在编译...
go run test_com_stmt_execute_simple.go

echo.
echo ========================================
echo 测试完成
echo ========================================
pause
