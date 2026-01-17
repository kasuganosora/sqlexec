@echo off
echo ========================================
echo Binlog Slave 客户端 - 使用 go-mysql 库
echo ========================================
echo.
echo 这个工具会:
echo   1. 使用 go-mysql 库连接 MariaDB
echo   2. 发送 COM_REGISTER_SLAVE 注册为 slave
echo   3. 发送 COM_BINLOG_DUMP 请求 binlog
echo   4. 接收并显示 binlog 事件
echo.
echo 首先需要安装依赖...
echo.

cd /d d:\code\db\mysql\resource

echo 安装 go-mysql 库...
go get github.com/go-mysql-org/go-mysql

if errorlevel 1 (
    echo.
    echo ❌ 安装依赖失败
    echo.
    pause
    exit /b 1
)

echo.
echo ✅ 依赖安装成功
echo.
echo 请确保:
echo   1. MariaDB 正在运行 (端口 3306)
echo   2. Wireshark 正在抓取 tcp.port == 3306
echo   3. MariaDB 已启用 binlog
echo   4. root 用户有 REPLICATION SLAVE 权限
echo.
pause

echo.
echo 开始运行 binlog slave 客户端...
go run binlog_slave_gomysql.go

echo.
echo ========================================
echo 测试完成！
echo ========================================
echo.
pause
