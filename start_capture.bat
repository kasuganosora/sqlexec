@echo off
echo ========================================
echo MySQL/MariaDB 协议包捕获工具
echo ========================================
echo.
echo 这个程序会使用官方 MySQL 客户端库
echo 连接到本地 MariaDB 服务器并执行查询
echo 来生成真实的协议包用于分析
echo.
echo 请确保:
echo   1. MariaDB 正在运行 (端口 3306)
echo   2. Wireshark 正在抓取 tcp.port == 3306
echo   3. 数据库 'test' 和表 'mysql_data_types_demo' 存在
echo.
pause

cd /d d:\code\db
go run mysql/resource/capture_with_official_client.go

echo.
echo ========================================
echo 测试完成！
echo ========================================
echo.
echo 请在 Wireshark 中:
echo   1. 停止抓包
echo   2. 保存抓包数据 (例如: test_maria_db.pcapng)
echo   3. 保存到: d:/code/db/mysql/resource/test_maria_db.pcapng
echo   4. 使用分析工具查看包内容:
echo      cd d:/code/db/mysql/resource
echo      go run analyze.go test_maria_db.pcapng
echo.
pause
