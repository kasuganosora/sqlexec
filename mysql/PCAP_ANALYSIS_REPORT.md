# 抓包分析报告 - MariaDB COM_STMT_EXECUTE

## 📊 分析结果

### ⭐ 关键发现 1：NULL Bitmap 长度

**实际数据：**
```
NULL Bitmap 字节数: 3
值 (hex): 00 02 00
值 (binary):
  00000000
  00000010
  00000000
```

**协议分析：**

根据不同的参数数量计算：

| 参数数量 | MySQL 协议 `(n+7)/8` | MariaDB 协议 `(n+2+7)/8` | 实际长度 | 匹配 |
|---------|---------------------|------------------------|---------|------|
| 15 | 2 字节 | **3 字节** | **3 字节** | ✅ MariaDB |
| 16 | 2 字节 | **3 字节** | **3 字节** | ✅ MariaDB |
| 17 | 3 字节 | 3 字节 | 3 字节 | ⚠️ 两者相同 |

**结论：** ⭐ **确认使用 MariaDB 协议**

---

### ⭐ 关键发现 2：NULL 标志位映射

**NULL Bitmap 值：** `00 02 00` (二进制: `00000000 00000010 00000000`)

**被设置的位：** 位 9（第二个字节的第 1 位，从 0 开始计数）

#### MySQL 协议假设（位 0 开始映射）：
```
位 0-7  → 参数 1-8   (第 1 字节)
位 8-15 → 参数 9-16  (第 2 字节)
位 16   → 参数 17    (第 3 字节)
```

如果位 9 被设置 → **参数 10 是 NULL**

#### MariaDB 协议假设（位 2 开始映射）：
```
位 2-9   → 参数 1-8   (第 1 字节)
位 10-17 → 参数 9-16  (第 2 字节)
位 18+   → 参数 17+   (第 3 字节)
```

如果位 9 被设置 → **参数 7 是 NULL**

**结论：** 需要结合实际的参数值来确认哪个协议正确。

---

### ⭐ 关键发现 3：参数数量估计

**Statement ID:** 3693832
**New Params Bind Flag:** 0（不包含新的参数类型）

由于 `New Params Bind Flag = 0`，参数类型在之前的 PREPARE 阶段已经发送，这里只包含参数值。

**参数数量推断：**
- 根据测试场景，可能是一个 15-16 参数的查询
- 对应测试场景 15：9 个参数（不太匹配）
- 可能是其他包含更多参数的场景

---

## 🔧 当前实现的问题

### 问题 1：NULL Bitmap 计算公式 ✅ **正确**

当前代码（MariaDB 协议）：
```go
requiredNullBitmapLen := (paramCount + 2 + 7) / 8
```

这个公式是正确的！

### 问题 2：NULL Bitmap 读取 ❌ **错误**

当前代码：
```go
// 只读取 1 字节
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, 1))
```

**问题：**
- 硬编码只读取 1 字节
- 实际应该根据 `requiredNullBitmapLen` 读取

**正确做法：**
```go
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, int64(requiredNullBitmapLen)))
```

### 问题 3：位映射 ⚠️ **需要确认**

当前实现假设 MariaDB 协议：
```go
// MariaDB: 位 0,1 保留，从位 2 开始映射参数
byteIdx := (i + 2) / 8
bitIdx := uint((i + 2) % 8)
```

这个假设符合 MariaDB 协议，但需要验证。

---

## 📋 修复方案

### 修复 1：更新 NULL Bitmap 读取逻辑

**文件：** `mysql/protocol/packet.go`

**找到：**
```go
// 读取 NULL bitmap
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, 1))
```

**替换为：**
```go
// 读取 NULL bitmap
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, int64(requiredNullBitmapLen)))
```

---

## 🧪 验证步骤

### 步骤 1：修复代码

按照上面的方案修改 `packet.go` 中的代码。

### 步骤 2：重新测试

```powershell
cd d:/code/db
go test -v ./mysql/protocol -run TestComStmtExecute
```

### 步骤 3：对比输出

修复后，应该能够正确解析：
- 1 个参数 → 1 字节 NULL bitmap
- 9 个参数 → 2 字节 NULL bitmap
- 15-16 个参数 → 3 字节 NULL bitmap

### 步骤 4：位映射测试

创建一个测试，验证 NULL 参数的解析：

```go
// 测试：如果参数 7 是 NULL
// MariaDB: 位 9 应该被设置 (byte[1] & (1 << (9 % 8)))
// MySQL: 位 6 应该被设置 (byte[0] & (1 << (6 % 8)))
```

---

## 📊 数据对比

### 场景 8：NULL 参数（1 个参数）

**期望的 NULL bitmap：**
- MySQL: `(1+7)/8 = 1 字节`，值为 `01` (位 0)
- MariaDB: `(1+2+7)/8 = 1 字节`，值为 `04` (位 2)

### 场景 15：9 个参数（可能有 NULL）

**期望的 NULL bitmap：**
- MySQL: `(9+7)/8 = 2 字节`
- MariaDB: `(9+2+7)/8 = 2 字节`

**如果参数 1 是 NULL：**
- MySQL: `01 00` (位 0)
- MariaDB: `04 00` (位 2)

### 实际抓包数据（15-16 参数）

**实际值：** `00 02 00` (3 字节)

**分析：**
- 位 9 被设置（第二个字节的第 1 位）
- 如果是 MySQL：参数 10 是 NULL
- 如果是 MariaDB：参数 7 是 NULL

---

## 🎯 下一步行动

1. **立即修复：** 更新 NULL bitmap 的读取逻辑
2. **验证修复：** 运行测试套件
3. **确认协议：** 分析更多场景确定是 MySQL 还是 MariaDB
4. **完善测试：** 添加 NULL 参数的单元测试

---

## 📚 参考资源

- [MariaDB Binary Protocol - Prepared Statement Execution](https://mariadb.com/kb/en/binary-protocol-prepared-statement-execution/)
- [MySQL Protocol - COM_STMT_EXECUTE](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL_COM_STMT_EXECUTE.html)
- [MariaDB NULL Bitmap Calculation](https://mariadb.com/kb/en/null-bitmap/)

---

**报告生成时间：** 2026-01-17
**分析工具：** analyze_pcap.go
**抓包文件：** test_maria_db.pcapng
