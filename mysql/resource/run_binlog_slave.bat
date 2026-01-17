@echo off
echo ========================================
echo Binlog Slave 客户端 - 抓包工具
echo ========================================
echo.
echo 这个工具会:
echo   1. 使用项目的 MySQL 协议实现连接 MariaDB
echo   2. 发送 COM_REGISTER_SLAVE 注册为 slave
echo   3. 发送 COM_BINLOG_DUMP 请求 binlog
echo   4. 接收并显示 binlog 事件
echo.
echo 请确保:
echo   1. MariaDB 正在运行 (端口 3306)
echo   2. Wireshark 正在抓取 tcp.port == 3306
echo   3. MariaDB 已启用 binlog
echo   4. root 用户有 REPLICATION SLAVE 权限
echo.
echo 预期看到的包:
echo   - COM_REGISTER_SLAVE (0x14)
echo   - COM_BINLOG_DUMP (0x12)
echo   - Binlog Events (各种事件类型)
echo.
pause

cd /d d:\code\db\mysql\resource
go run binlog_slave_protocol.go

echo.
echo ========================================
echo 测试完成！
echo ========================================
echo.
pause
