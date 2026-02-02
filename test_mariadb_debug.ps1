$ErrorActionPreference = "Continue"

# 启动服务器
Write-Host "Starting server..."
$serverProcess = Start-Process -FilePath ".\service.exe" -ArgumentList "-port", "13306" -NoNewWindow -RedirectStandardOutput "server_out.log" -RedirectStandardError "server_err.log" -PassThru

Write-Host "Server PID: $($serverProcess.Id)"
Start-Sleep -Seconds 3

# 测试连接
Write-Host "Testing MariaDB client connection..."
& "D:\tools\mariaDB\bin\mysql.exe" -h 127.0.0.1 -u root -P13306 -e "SELECT 1" 2>&1

# 等待一秒
Start-Sleep -Seconds 1

# 停止服务器
Write-Host "Stopping server..."
Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue

# 显示日志
Write-Host "`n=== SERVER OUTPUT ==="
Get-Content server_out.log -Tail 30

Write-Host "`n=== SERVER ERROR ==="
Get-Content server_err.log -Tail 30
