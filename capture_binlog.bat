@echo off
echo ========================================
echo Binlog 协议包捕获工具
echo ========================================
echo.
echo 这个工具会:
echo   1. 检查 MariaDB 的 binlog 配置
echo   2. 执行 INSERT/UPDATE/DELETE 操作产生 binlog 事件
echo   3. 显示 binlog 状态
echo.
echo 请确保:
echo   1. MariaDB 正在运行 (端口 3306)
echo   2. Wireshark 正在抓取 tcp.port == 3306
echo   3. 数据库 'test' 存在
echo   4. MariaDB 已启用 binlog
echo.
pause

cd /d d:\code\db
go run mysql/resource/capture_binlog.go

echo.
echo ========================================
echo 测试完成！
echo ========================================
echo.
echo 💡 在 Wireshark 中应该能看到:
echo   - COM_REGISTER_SLAVE (0x14)
echo   - COM_BINLOG_DUMP (0x12)
echo   - Binlog 事件包 (各种事件类型)
echo.
echo 建议保存为: d:/code/db/mysql/resource/binlog_test.pcapng
echo.
pause
