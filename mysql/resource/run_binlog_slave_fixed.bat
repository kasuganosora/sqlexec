@echo off
echo ========================================
echo Binlog Slave 客户端 - 修复版
echo ========================================
echo.
echo 检测到 binlog 校验和问题！
echo.
echo 第一步：在 MariaDB 中运行 SQL 脚本
echo ----------------------------------------
echo.
echo 请连接到 MariaDB 并运行以下 SQL：
echo.
echo   mysql -u root -p
echo.
echo   然后执行:
echo   SOURCE d:/code/db/mysql/resource/fix_binlog_checksum.sql;
echo.
echo 或者直接运行:
echo   SET GLOBAL binlog_checksum=NONE;
echo.
echo ----------------------------------------
echo.
pause

echo.
echo 第二步：启动 Wireshark 抓包
echo ----------------------------------------
echo.
echo 请确保 Wireshark 正在抓取:
echo   tcp.port == 3306 and mysql
echo.
pause

echo.
echo 第三步：运行 binlog slave 客户端
echo ----------------------------------------
echo.

cd /d d:\code\db\mysql\resource
go run binlog_slave_protocol.go

echo.
echo ========================================
echo 测试完成！
echo ========================================
echo.
pause
