# MySQL/MariaDB 协议包捕获指南

## 📋 目标

连接真实的 MariaDB 服务器，执行各种预处理语句，捕获包含以下协议的数据包：
- COM_STMT_PREPARE (0x16)
- COM_STMT_EXECUTE (0x17)
- 其他常见 MySQL 命令

## 🛠️ 步骤

### 步骤 1: 启动 Wireshark 抓包

1. 打开 Wireshark
2. 选择捕获接口：`Adapter for loopback traffic` 或 `Loopback: lo`
3. 设置抓包过滤器：
   ```
   tcp.port == 3306
   ```
4. 开始捕获

### 步骤 2: 运行测试程序

**命令：**
```powershell
cd d:/code/db
go run mysql/resource/capture_mysql_packets.go
```

### 步骤 3: 测试程序会执行的操作

测试程序会依次执行以下场景：

| 场景 | 描述 | 参数类型 | 参数值 |
|------|------|---------|--------|
| 场景1 | 单个 INT 参数 | INT | 500 |
| 场景2 | 单个 VARCHAR 参数 | VAR_STRING | "variable length" |
| 场景3 | 多个参数 (INT + VARCHAR) | INT, VAR_STRING | 500, "variable length" |
| 场景4 | 带 NULL 参数 | TINYINT | NULL |
| 场景5 | TINYINT 参数 | TINYINT | 100 |
| 场景6 | BIGINT 参数 | BIGINT | 9000000000000000000 |
| 场景7 | FLOAT 参数 | FLOAT | 3.14159 |
| 场景8 | DOUBLE 参数 | DOUBLE | 2.718281828459045 |

对于每个场景，程序会：
1. 执行 `COM_STMT_PREPARE`
2. 执行 `COM_STMT_EXECUTE`
3. 读取结果集

### 步骤 4: 在 Wireshark 中分析

1. 在 Wireshark 中停止捕获
2. 保存抓包数据（建议保存为 `test_maria_db.pcapng`）
3. 使用过滤器查看特定包：
   - 查看 PREPARE 包：`mysql.command == 22` (COM_STMT_PREPARE)
   - 查看 EXECUTE 包：`mysql.command == 23` (COM_STMT_EXECUTE)

### 步骤 5: 分析关键数据

#### 对于每个 COM_STMT_EXECUTE 包，查看：

**1. NULL bitmap：**
- 长度：多少字节？
- 值：十六进制和二进制表示
- 使用的协议：MySQL 还是 MariaDB？

**2. 参数类型：**
- 每个参数的类型代码
- 每个参数的标志

**3. 参数值：**
- 编码格式（二进制、长度编码等）
- 与参数类型的对应关系

## 📊 预期的包结构

### COM_STMT_PREPARE (0x16)

```
请求包:
[包头] + [0x16] + [查询字符串长度] + [查询字符串]

响应包:
[包头] + [StatementID] + [列数] + [参数数] + [保留] + [警告数]
+ [参数定义] * n
+ [列定义] * m
+ [EOF]
```

### COM_STMT_EXECUTE (0x16)

**标准 MySQL 协议：**
```
[包头] + [0x17]
+ [StatementID: 4字节]
+ [Flags: 1字节]
+ [IterationCount: 4字节]
+ [NULL bitmap: ceil((n + 7) / 8) 字节]
+ [NewParamsBindFlag: 1字节]
+ [ParamTypes: 2字节 * n] (如果 flag=1)
+ [ParamValues...]
```

**MariaDB 协议（如果不同）：**
```
[包头] + [0x17]
+ [StatementID: 4字节]
+ [Flags: 1字节]
+ [IterationCount: 4字节]
+ [NULL bitmap: ceil((n + 2 + 7) / 8) 字节]
+ [NewParamsBindFlag: 1字节]
+ [ParamTypes: 2字节 * n] (如果 flag=1)
+ [ParamValues...]
```

## 🔍 关键检查点

### 1. NULL Bitmap 计算

**检查方法：**
1. 在 Wireshark 中找到一个 COM_STMT_EXECUTE 包
2. 查看包详情中的 NULL bitmap 字段
3. 记录：
   - 参数数量（从 ParamTypes 推断）
   - NULL bitmap 的字节数
   - NULL bitmap 的值（hex）

**对比公式：**
```go
// MySQL 标准
nullBitmapLen = (numParams + 7) / 8

// MariaDB
nullBitmapLen = (numParams + 2 + 7) / 8
```

**示例：**
- 1 个参数：
  - MySQL: (1 + 7) / 8 = 1 字节
  - MariaDB: (1 + 2 + 7) / 8 = 1 字节

- 9 个参数：
  - MySQL: (9 + 7) / 8 = 2 字节
  - MariaDB: (9 + 2 + 7) / 8 = 2 字节

### 2. NULL 标志位映射

**MySQL 协议：**
```
位 0 → 参数 1
位 1 → 参数 2
位 2 → 参数 3
...
```

**MariaDB 协议：**
```
位 0, 1 → 保留
位 2 → 参数 1
位 3 → 参数 2
...
```

**检查方法：**
1. 找到一个带 NULL 参数的包
2. 查看哪个参数是 NULL
3. 查看 NULL bitmap 中对应的位
4. 确定使用的是哪种映射方式

### 3. 参数类型验证

**验证参数类型代码：**
```go
0x01 = TINYINT
0x02 = SMALLINT
0x03 = INT
0x04 = FLOAT
0x05 = DOUBLE
0x06 = NULL
0x07 = TIMESTAMP
0x08 = BIGINT
0x09 = MEDIUMINT
0x0a = DATE
0x0b = TIME
0x0c = DATETIME
0x0d = YEAR
0x0e = NEWDATE
0x0f = VARCHAR
0x10 = BIT
0xfd = VAR_STRING
0xfe = BLOB
0xff = GEOMETRY
```

### 4. 参数值编码

**INT 类型：**
- 固定 4 字节，小端序
- 示例：`01 00 00 00` = 1

**VARCHAR 类型：**
- 长度编码：1 字节长度（如果 < 251）
- 示例：`09 76 61 72 69 61 62 6c 65` = 9 字节的 "variable"

**NULL 值：**
- 只在 NULL bitmap 中标记，不发送数据

## 📝 记录表格

建议创建一个表格来记录分析结果：

| 场景 | 参数数量 | NULL bitmap 长度 | NULL bitmap 值 | 参数类型 | 参数值 | 协议类型 |
|------|---------|------------------|----------------|---------|---------|---------|
| 场景1 | 1 | ? | ? | 0x03 | 500 | MySQL/MariaDB |
| 场景2 | 1 | ? | ? | 0xfd | "variable length" | MySQL/MariaDB |
| 场景3 | 2 | ? | ? | 0x03, 0xfd | 500, "variable length" | MySQL/MariaDB |
| 场景4 | 1 | ? | ? | 0x01 | NULL | MySQL/MariaDB |
| ... | ... | ... | ... | ... | ... | ... |

## 🚀 分析完成后

完成抓包和分析后：

1. **保存抓包文件：**
   - 建议保存到：`d:/code/db/mysql/resource/test_maria_db.pcapng`

2. **运行分析工具：**
   ```powershell
   cd d:/code/db/mysql/resource
   go run analyze.go test_maria_db.pcapng
   ```

3. **更新代码：**
   - 根据分析结果修改 `mysql/protocol/packet.go`
   - 修正 NULL bitmap 计算公式
   - 修正 Unmarshal/Marshal 方法

4. **运行测试：**
   ```powershell
   cd d:/code/db
   go test -v ./mysql/protocol -run TestComStmtExecute
   ```

## 🎯 预期结果

通过这次抓包分析，我们能够：
1. ✅ 确认 MariaDB 使用的协议标准
2. ✅ 验证 NULL bitmap 的计算公式
3. ✅ 确认 NULL 标志位的映射方式
4. ✅ 获取各种类型的真实包数据
5. ✅ 用于验证和修复当前实现

## 💡 注意事项

1. **确保 MariaDB 正在运行：**
   ```powershell
   mysql -uroot -h 127.0.0.1 -e "SELECT 1"
   ```

2. **检查端口：**
   - 默认是 3306
   - 如果不同，需要修改测试程序中的端口

3. **权限问题：**
   - 如果使用非 root 用户，可能需要调整测试程序

4. **测试数据库：**
   - 确保数据库 `test` 存在
   - 确保表 `mysql_data_types_demo` 存在

## 📚 相关文件

- `mysql/resource/capture_mysql_packets.go` - 测试程序
- `mysql/resource/analyze.go` - 抓包分析工具
- `mysql/protocol/packet.go` - 需要修复的代码
- `mysql/STMT_EXECUTE_FIX_SUMMARY.md` - 修复指南

## 🔗 参考资源

- [MariaDB Protocol](https://mariadb.com/docs/server/reference/clientserver-protocol/)
- [MySQL Protocol](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [Wireshark MySQL Dissector](https://gitlab.com/wireshark/wireshark/-/wikis/MySQL)

---

**开始抓包分析吧！** 🎉
