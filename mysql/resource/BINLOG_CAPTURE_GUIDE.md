# Binlog 协议包抓取指南

## 目标
搭建环境抓取 binlog 协议包，分析和修复 binlog 相关协议实现问题。

## 环境准备

### 1. 启用 MariaDB Binlog

编辑 MariaDB 配置文件 (Windows 通常在 `C:\Program Files\MariaDB xx.x\data\my.ini`):

```ini
[mysqld]
log-bin=mariadb-bin
server-id=1
binlog_format=ROW
binlog_row_image=FULL
expire_logs_days=7
max_binlog_size=100M
```

重启 MariaDB 服务:

```powershell
# 以管理员身份运行
net stop MariaDB
net start MariaDB
```

### 2. 验证 Binlog 已启用

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW MASTER STATUS;
```

应该看到 `log_bin = ON` 和 `binlog_format = ROW`。

### 3. 授予复制权限

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
```

## 抓包方法

### 方法 1: 使用 Wireshark 抓包

1. **启动 Wireshark**
   - 选择网卡 (通常是 "Wi-Fi" 或 "Ethernet")
   - 设置过滤器: `tcp.port == 3306 and mysql`

2. **开始抓包**
   - 点击蓝色的鲨鱼鳍图标开始抓包
   - 准备好之后执行测试程序

3. **运行测试程序**

   #### 选项 A: 产生 binlog 事件
   ```bash
   cd d:\code\db
   capture_binlog.bat
   ```
   这会执行 INSERT/UPDATE/DELETE 操作产生 binlog 事件。

   #### 选项 B: 使用简单 slave 客户端请求 binlog
   ```bash
   cd d:\code\db\mysql\resource
   go run simple_binlog_slave.go
   ```
   这会发送 COM_REGISTER_SLAVE + COM_BINLOG_DUMP 请求并接收 binlog。

4. **保存抓包文件**
   - 停止抓包 (红色方块图标)
   - File -> Save As -> 选择 PCAPNG 格式
   - 保存到: `d:/code/db/mysql/resource/binlog_test.pcapng`

### 方法 2: 使用 Windows 内置工具

```powershell
# 安装 tcpdump (如果还没有)
choco install wireshark -y

# 或者使用 PowerShell 的网络捕获 (Windows 8+)
New-NetEventSession -Name "MySQLCapture" -LocalFilePath "C:\temp\mysql.etl"
Add-NetEventProvider -Name "Microsoft-Windows-TCPIP" -SessionName "MySQLCapture"
Start-NetEventSession -Name "MySQLCapture"

# 运行测试程序
# ...

# 停止捕获
Stop-NetEventSession -Name "MySQLCapture"
Remove-NetEventSession -Name "MySQLCapture"
```

## 分析包内容

### 在 Wireshark 中查看

1. **过滤 binlog 相关包**
   ```
   mysql.command == 0x12 or mysql.command == 0x14
   ```

2. **查看包详情**
   - 展开包查看每个字段的值
   - 查看十六进制和 ASCII 表示
   - 右键 -> Packet Details -> Copy -> as Printable Text

3. **关注的字段**
   - COM_REGISTER_SLAVE (0x14)
     - Server ID
     - Hostname
     - User
     - Password
     - Port
     - Rank
     - Master ID

   - COM_BINLOG_DUMP (0x12)
     - Binlog Pos
     - Flags
     - Server ID
     - Binlog Filename

   - Binlog Event
     - Timestamp
     - Event Type
     - Server ID
     - Event Size
     - Next Position
     - Flags
     - Event-specific data

### 使用 Wireshark 过滤器示例

```
# 查看所有 binlog dump 请求
mysql.command == 0x12

# 查看所有 register slave 请求
mysql.command == 0x14

# 查看特定的 binlog 事件类型
mysql.event_type == 0x0f  # Format Description Event
mysql.event_type == 0x13  # Table Map Event
mysql.event_type == 0x19  # Write Rows Event v1

# 查看特定时间段
frame.time >= "2025-01-17 10:00:00" && frame.time <= "2025-01-17 10:05:00"
```

## 预期结果

### 成功的协议交互应该看到:

1. **连接阶段**
   - TCP 三次握手
   - MariaDB Handshake Packet
   - 客户端认证包
   - 认证响应 (OK 0x00)

2. **Slave 注册阶段**
   - COM_REGISTER_SLAVE (0x14)
   - OK 响应 (0x00)

3. **Binlog 请求阶段**
   - COM_BINLOG_DUMP (0x12)
   - Binlog Event Stream:
     - Format Description Event (0x0F)
     - Rotate Event (0x04)
     - Query Events (0x02) - 如果 binlog_format=STATEMENT
     - Table Map Events (0x13) - 如果 binlog_format=ROW
     - Row Events (0x19/0x1A/0x1B 或 0x1E/0x1F/0x20)

### 常见问题

#### 1. 认证失败
```
ERROR 1045 (28000): Access denied for user 'root'@'localhost' (using password: NO)
```
解决:
```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
```

#### 2. Binlog 未启用
```
SHOW VARIABLES LIKE 'log_bin';
-- log_bin = OFF
```
解决: 在 my.ini 中添加 `log-bin=mariadb-bin` 并重启服务。

#### 3. 权限不足
```
ERROR 1227 (42000): Access denied; you need (at least one of) the REPLICATION SLAVE privilege(s)
```
解决: 授予 REPLICATION SLAVE 权限。

#### 4. 看不到 binlog 事件
可能原因:
- Binlog_format 设置错误 (应该是 ROW 或 MIXED)
- 没有执行写操作
- 连接的是 slave 而不是 master
- Binlog 被清理了 (expire_logs_days 太小)

## 对比分析

### 使用抓包数据对比你的实现

1. **COM_REGISTER_SLAVE 实现**
   ```go
   // 你的实现: d:/code/db/mysql/protocol/replication.go
   type ComRegisterSlave struct {
       ServerID uint32
       Hostname string
       User     string
       Password string
       Port     uint16
       Rank     uint32
       MasterID uint32
   }
   ```
   对比 Wireshark 中看到的包结构和你的 Marshal/Unmarshal 实现。

2. **COM_BINLOG_DUMP 实现**
   ```go
   type ComBinlogDump struct {
       Pos      uint32
       Flags    uint16
       ServerID uint32
       Filename string
   }
   ```
   同样对比实际包数据。

3. **Binlog Event 解析**
   查看实际的 binlog 事件格式:
   - 事件头 (19字节): timestamp, event_type, server_id, event_size, position, flags
   - 事件体: 根据事件类型不同而不同
   - 事件尾: 校验和 (如果启用)

## 测试步骤总结

1. **准备环境**
   - [ ] 启用 MariaDB binlog
   - [ ] 授予复制权限
   - [ ] 安装 Wireshark
   - [ ] 准备测试数据库

2. **抓包分析**
   - [ ] 启动 Wireshark 开始抓包
   - [ ] 运行 simple_binlog_slave.go
   - [ ] 保存抓包文件
   - [ ] 分析包内容

3. **对比验证**
   - [ ] 对比 COM_REGISTER_SLAVE 实现
   - [ ] 对比 COM_BINLOG_DUMP 实现
   - [ ] 分析 binlog 事件格式
   - [ ] 找出你的代码问题

4. **修复测试**
   - [ ] 修复协议实现
   - [ ] 重新运行测试
   - [ ] 验证修复效果

## 参考资料

- MySQL Replication Protocol: https://dev.mysql.com/doc/internals/en/replication-protocol.html
- MariaDB Replication: https://mariadb.com/kb/en/replication/
- Binlog Event Types: https://dev.mysql.com/doc/internals/en/binary-log-event.html
- Wireshark MySQL Dissector: https://wiki.wireshark.org/MariaDB

## 文件清单

```
d:/code/db/
├── capture_binlog.bat                # 产生 binlog 事件的脚本
├── mysql/resource/
│   ├── capture_binlog.go             # 产生 binlog 事件的程序
│   ├── binlog_slave_client.go        # 使用 go-mysql 库的 slave 客户端
│   └── simple_binlog_slave.go        # 使用项目自己实现的简单 slave 客户端
└── mysql/resource/BINLOG_CAPTURE_GUIDE.md  # 本指南
```

## 快速开始

```bash
# 1. 确保 MariaDB 启用 binlog 并重启服务
# 2. 打开 Wireshark 开始抓包
# 3. 运行简单 slave 客户端
cd d:\code\db\mysql\resource
go run simple_binlog_slave.go

# 4. 在 Wireshark 中分析包
# 5. 对比你的协议实现
# 6. 修复问题
```
