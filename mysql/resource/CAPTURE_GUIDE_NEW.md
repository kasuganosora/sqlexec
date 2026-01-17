# MySQL/MariaDB 协议包捕获完整指南

## 📋 概述

本指南帮助你捕获真实的 MySQL/MariaDB 协议包，用于分析和修复 `ComStmtExecutePacket` 实现问题。

测试程序使用官方 `go-sql-driver/mysql` 客户端库，确保生成最真实、最可靠的协议包。

---

## 🎯 测试场景覆盖（33个场景）

### 📁 数据库操作（4个）
| 场景 | 命令类型 | 说明 |
|------|---------|------|
| 1 | COM_QUERY | SHOW DATABASES - 显示所有数据库 |
| 2 | COM_QUERY | SHOW TABLES - 显示所有表 |
| 3 | COM_QUERY | SHOW CREATE TABLE - 显示建表语句 |
| 4 | COM_QUERY | DESCRIBE TABLE - 描述表结构 |

### 📤 预处理 SELECT 操作（15个）
| 场景 | 说明 | 关键点 |
|------|------|--------|
| 5 | 单个 INT 参数 | 基础预处理查询 |
| 6 | 单个 VARCHAR 参数 | 字符串参数 |
| 7 | 多个参数 | INT + VARCHAR |
| 8 | **NULL 参数** | ⭐ **关键测试！验证 NULL bitmap** |
| 9 | TINYINT 参数 | 小整数类型 |
| 10 | BIGINT 参数 | 大整数类型 |
| 11 | FLOAT 参数 | 单精度浮点 |
| 12 | DOUBLE 参数 | 双精度浮点 |
| 13 | DATE 参数 | 日期类型 |
| 14 | DATETIME 参数 | 日期时间类型 |
| 15 | **9个参数** | ⭐ **测试多字节 NULL bitmap** |
| 16 | LIKE 查询 | 模糊匹配 |
| 17 | IN 查询 | 多值匹配 |
| 18 | BETWEEN 查询 | 范围查询 |
| 19 | ORDER BY + LIMIT | 排序和分页 |
| 20 | COUNT | 统计函数 |
| 21 | SUM | 聚合函数 |
| 22 | AVG | 聚合函数 |

### ➕ INSERT 操作（3个）
| 场景 | 说明 |
|------|------|
| 23 | 预处理插入单行数据 |
| 24 | 预处理插入带NULL值 |
| 25 | 预处理批量插入 |

### ✏️ UPDATE 操作（3个）
| 场景 | 说明 |
|------|------|
| 26 | 预处理更新单行 |
| 27 | 预处理更新为NULL值 |
| 28 | 预处理多条件更新 |

### 🗑️ DELETE 操作（2个）
| 场景 | 说明 |
|------|------|
| 29 | 预处理删除单行 |
| 30 | 预处理多条件删除 |

### 🔧 SET 变量操作（2个）
| 场景 | 说明 |
|------|------|
| 31 | SET SESSION 变量 |
| 32 | SET 用户变量 |

### 📦 DROP 操作（1个）
| 场景 | 说明 |
|------|------|
| 33 | DROP TABLE - 删除测试表 |

---

## 🚀 快速开始

### 第一步：启动 Wireshark

1. **打开 Wireshark**
2. **选择接口**：
   - Windows: `Adapter for loopback traffic capture`
   - Linux/Mac: `lo` (loopback)
3. **设置过滤器**：
   ```
   tcp.port == 3306
   ```
   或更精确：
   ```
   tcp.port == 3306 and mysql
   ```
4. **开始捕获** (Start capturing packets)

### 第二步：运行测试程序

#### 方式 1：使用批处理脚本（推荐）
```powershell
d:\code\db\start_capture.bat
```

#### 方式 2：直接运行
```powershell
cd d:\code\db
go run mysql/resource/capture_with_official_client.go
```

### 第三步：查看测试输出

测试程序会依次执行 33 个场景，每个场景显示：
- 场景名称和说明
- 查询语句
- 参数列表（如果有）
- 执行结果

**示例输出：**
```
【测试场景 8: PREPARE SELECT - NULL 参数】
  说明: 使用 NULL 参数查询（关键测试！）
  查询: SELECT * FROM mysql_data_types_demo WHERE type_bool = ?
  参数数量: 1
    参数 1: <nil> (<nil>)

  → 执行查询...
  ✅ 查询成功
  返回 45 列: [type_tinyint type_smallint ...]
  行 1: 100, 32000, ...
```

### 第四步：在 Wireshark 中分析

#### 查找关键包

**1. COM_STMT_PREPARE (命令 0x16)**
- 过滤器：`mysql.command == 22` (0x16 = 22)
- 查看准备语句的参数信息

**2. COM_STMT_EXECUTE (命令 0x17) ⭐**
- 过滤器：`mysql.command == 23` (0x17 = 23)
- **重点关注：**
  - NULL bitmap 的格式
  - NULL bitmap 的长度
  - 参数类型的编码
  - 参数值的编码

**3. 其他重要命令**
- COM_QUERY (0x03) - 普通查询
- COM_STMT_CLOSE (0x19) - 关闭预处理语句
- COM_QUIT (0x01) - 退出连接

#### 分析方法

**方法 1：按命令类型过滤**
```
mysql.command == 23  # 只看 COM_STMT_EXECUTE
```

**方法 2：按包长度过滤**
```
tcp.len > 20 and mysql  # 排除空包
```

**方法 3：查找特定场景**
1. 在 Wireshark 中找到场景 8（NULL 参数）的包
2. 右键 → Follow → TCP Stream
3. 查看完整的通信过程

**方法 4：十六进制分析**
```
选中包 → 点击 Packet Bytes 面板
```

### 第五步：保存抓包文件

1. **停止捕获**
2. **保存为：** `d:/code/db/mysql/resource/test_maria_db.pcapng`
3. **建议格式：** `pcapng` (较新的格式，支持更多信息)

---

## 🔍 关键检查点

### ⭐ 检查点 1：NULL Bitmap 计算公式

**场景 8：NULL 参数（1 个参数）**
- 查看包中的 NULL bitmap 长度
- MySQL 协议：`(1 + 7) / 8 = 1 字节`
- MariaDB 协议：`(1 + 2 + 7) / 8 = 1 字节`
- **观察：** 两种协议在此场景结果相同

**场景 15：9 个参数**
- MySQL 协议：`(9 + 7) / 8 = 2 字节`
- MariaDB 协议：`(9 + 2 + 7) / 8 = 2 字节`
- **观察：** 两种协议在此场景结果也相同

**场景 23：多个不同类型的参数**
- 观察 NULL bitmap 的实际长度
- 确认计算公式

### ⭐ 检查点 2：NULL 标志位映射

**在 Wireshark 中查看：**

**场景 8：NULL 参数（1 个参数）**
```
NULL bitmap: 01 00 00 00 ...
           或 04 00 00 00 ...
```

- `01` (二进制 `00000001`) → 位 0 被设置 → **MySQL 协议**
- `04` (二进制 `00000100`) → 位 2 被设置 → **MariaDB 协议**

**场景 15：9 个参数**
```
NULL bitmap: 00 00 ...    (如果没有 NULL)
NULL bitmap: 01 00 ...    (如果参数 1 是 NULL)
NULL bitmap: 02 00 ...    (如果参数 2 是 NULL)
NULL bitmap: 04 00 ...    (如果参数 3 是 NULL - MariaDB)
```

**判断方法：**
- 位 0 对应参数 1 → MySQL
- 位 2 对应参数 1 → MariaDB
- 位 3 对应参数 2 → MariaDB

### ⭐ 检查点 3：参数类型编码

查看 `types` 字段的值：

| 场景 | 参数类型 | 预期值 (MySQL) |
|------|---------|--------------|
| 5 | INT | `MYSQL_TYPE_LONG (3)` |
| 6 | VAR_STRING | `MYSQL_TYPE_VAR_STRING (253)` |
| 9 | TINYINT | `MYSQL_TYPE_TINY (1)` |
| 10 | BIGINT | `MYSQL_TYPE_LONGLONG (8)` |
| 11 | FLOAT | `MYSQL_TYPE_FLOAT (4)` |
| 12 | DOUBLE | `MYSQL_TYPE_DOUBLE (5)` |
| 13 | DATE | `MYSQL_TYPE_DATE (10)` |
| 14 | DATETIME | `MYSQL_TYPE_DATETIME (12)` |

### ⭐ 检查点 4：参数值编码

查看 `values` 字段的编码：

**整数类型（场景 5）：**
```
00 00 01 F4  (500 的十六进制)
```

**字符串类型（场景 6）：**
```
0E 76 61 72 69 61 62 6C 65 20 6C 65 6E 67 74 68
长度(14) + "variable length" 的 ASCII
```

**NULL 值（场景 8）：**
```
NULL bitmap 中相应位被设置
values 中不包含该参数的数据
```

---

## 📊 抓包分析工具

### 使用 Go 分析工具

```powershell
cd d:/code/db/mysql/resource
go run analyze.go test_maria_db.pcapng
```

**分析工具功能：**
- 提取所有 MySQL 协议包
- 按命令类型分类
- 显示包结构
- 重点关注 COM_STMT_EXECUTE

### 使用 Wireshark 过滤器

**查看所有预处理语句执行：**
```
mysql.command == 23
```

**查看特定场景：**
```
frame.number >= N && frame.number <= M
```
（找到对应场景的帧范围）

**查看 NULL bitmap：**
```
mysql.command == 23 && data contains "00 00 00 17"
```

---

## 🎯 确认协议标准

通过以上分析，你应该能够确认：

### 1. NULL Bitmap 计算公式

**观察结果：**
- [ ] 1 个参数时的 NULL bitmap 长度：___ 字节
- [ ] 9 个参数时的 NULL bitmap 长度：___ 字节

**判断：**
- [ ] MySQL: `(n + 7) / 8`
- [ ] MariaDB: `(n + 2 + 7) / 8`

### 2. NULL 标志位映射

**观察结果（场景 8）：**
- [ ] NULL bitmap 的十六进制值：`__`
- [ ] 设置的位位置：第 ___ 位（从 0 开始）

**判断：**
- [ ] MySQL：位 0 → 参数 1，位 1 → 参数 2，...
- [ ] MariaDB：位 2 → 参数 1，位 3 → 参数 2，...

### 3. 参数类型编码

**验证是否与标准协议一致：**

| 场景 | 类型 | 实际值 | 预期值 | 一致 |
|------|------|--------|--------|------|
| 5 | INT | ___ | 3 | [ ] |
| 6 | VAR_STRING | ___ | 253 | [ ] |
| 9 | TINYINT | ___ | 1 | [ ] |
| 10 | BIGINT | ___ | 8 | [ ] |

---

## 📝 下一步

### 如果确认是 MySQL 协议

修改 `mysql/protocol/packet.go`：

```go
// 1. 修改 NULL bitmap 计算
requiredNullBitmapLen := (paramCount + 7) / 8

// 2. 修改 NULL bitmap 读取
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, int64(requiredNullBitmapLen)))

// 3. 位映射保持不变（位 0 → 参数 1）
```

### 如果确认是 MariaDB 协议

修改 `mysql/protocol/packet.go`：

```go
// 1. 保持 NULL bitmap 计算
requiredNullBitmapLen := (paramCount + 2 + 7) / 8

// 2. 修改 NULL bitmap 读取
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, int64(requiredNullBitmapLen)))

// 3. 位映射保持不变（位 2 → 参数 1）
```

---

## 📚 参考资料

### MySQL 协议文档
- [MySQL Protocol - COM_STMT_EXECUTE](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL_COM_STMT_EXECUTE.html)
- [MySQL Data Types](https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type)

### MariaDB 协议文档
- [MariaDB Protocol](https://mariadb.com/kb/en/about-mariadb-connector-c/)
- [MariaDB Binary Protocol](https://mariadb.com/kb/en/binary-protocol-prepared-statement-execution/)

### 相关文档（本项目中）
- `mysql/STMT_EXECUTE_FIX_SUMMARY.md` - 完整修复指南
- `mysql/COM_STMT_EXECUTE_ANALYSIS.md` - 详细技术分析
- `mysql/PCAP_ANALYSIS.md` - 抓包数据分析

---

## ❓ 常见问题

### Q1: 为什么有些场景没有生成包？
A: 可能是因为：
- 查询失败（检查错误信息）
- 结果为空（正常情况）
- 服务器优化导致某些包被合并

### Q2: 如何区分 COM_STMT_EXECUTE 的各个场景？
A:
1. 在 Wireshark 中按时间顺序查看
2. 使用 "Follow → TCP Stream" 查看完整通信
3. 结合查询语句识别场景

### Q3: 如果抓不到包怎么办？
A:
1. 确认 MariaDB 正在运行
2. 确认选择了正确的网络接口（loopback）
3. 尝试不设置过滤器，抓取所有包
4. 确认端口 3306 正确

### Q4: 如何确认参数的值是否正确？
A:
1. 在 Wireshark 中查看 `values` 字段的十六进制
2. 手动计算预期值的十六进制表示
3. 对比验证

---

## ✅ 检查清单

完成抓包后，请确认：

- [ ] 成功捕获 33 个场景的协议包
- [ ] 保存为 `test_maria_db.pcapng`
- [ ] 找到至少 10 个 COM_STMT_EXECUTE 包
- [ ] 记录 NULL bitmap 的计算公式
- [ ] 记录 NULL 标志位的映射关系
- [ ] 验证参数类型编码的正确性
- [ ] 确认是 MySQL 还是 MariaDB 协议

---

**抓包完成后，根据分析结果应用修复方案！** 🚀
