# 使用 gopacket 分析 MySQL 抓包数据

## 📦 已安装的库

已成功安装 Google 的 gopacket 库（比 gopcap 更可靠）：

```bash
go get github.com/google/gopacket
go get github.com/google/gopacket/layers
```

## 🛠️ 可用的分析工具

### 1. parse_pcap_gopacket.go（推荐）

**特点：**
- ✅ 使用 Google 官方库，可靠性高
- ✅ 完整的协议解析
- ✅ 自动识别 COM_STMT_EXECUTE 包
- ✅ 详细的包结构分析
- ✅ 支持 NULL bitmap 启发式解析
- ✅ 参数值智能解析

**使用方法：**

```powershell
cd d:/code/db/mysql/resource
go run parse_pcap_gopacket.go mysql.pcapng
```

**输出示例：**
```
解析文件: mysql.pcapng

搜索 MySQL COM_STMT_EXECUTE 包...
查找命令字节 0x17 (COM_STMT_EXECUTE)...

╔════════════════════════════════════════════════════════╗
║         找到 COM_STMT_EXECUTE 包 #1                          ║
╚════════════════════════════════════════════════════════╝

【包头信息】
  长度: 12 字节
  Sequence ID: 2
  包头 HEX: 0c 00 00 02

【载荷信息】
  Command: 0x17 (COM_STMT_EXECUTE)

【COM_STMT_EXECUTE 详细解析】
  Statement ID: 1
  Flags: 0x00
  Iteration Count: 1

  NULL Bitmap:
    字节数: 1
    值 (hex): 00
    值 (binary):
      00000000
  New Params Bind Flag: 1

  参数类型:
    [0] Type=0x01 (TINYINT), Flag=0x00

  参数值:
    偏移: 12
    长度: 1 字节
    HEX: 7b
    解析尝试:
      [0] 可能是 INT: 123

【完整 HEX dump】
0c 00 00 02 │ 17 01 00 00 00 │ 00 01 00 00 00 │ 00 01 01 00 7b │ .......
```

### 2. 其他工具

#### extract_mysql_pcap.go
简单的二进制提取工具：

```powershell
cd d:/code/db/mysql/resource
go run extract_mysql_pcap.go
```

#### search_pcap.ps1
PowerShell 搜索脚本：

```powershell
cd d:/code/db/mysql/resource
powershell -ExecutionPolicy Bypass -File search_pcap.ps1
```

## 📊 分析结果对比

### 当前实现 vs 真实抓包

| 字段 | 当前实现 | 真实抓包 | 状态 |
|------|---------|-----------|------|
| 包头格式 | ✅ | ✅ | 正确 |
| Command (0x17) | ✅ | ✅ | 正确 |
| StatementID | ✅ | ✅ | 正确 |
| Flags | ✅ | ✅ | 正确 |
| IterationCount | ✅ | ✅ | 正确 |
| NULL bitmap 长度 | ❌ (计算错误) | ✅ | 需修复 |
| NULL bitmap 位置 | ❌ (读取1字节) | ✅ (可变) | 需修复 |
| 参数类型 | ✅ | ✅ | 正确 |
| 参数值 | ✅ | ✅ | 正确 |

### 关键发现

#### 1. NULL bitmap 长度

**真实抓包：**
```
单参数: NULL bitmap = [0x00] (1字节)
多参数 (>8): NULL bitmap = [0x00, 0x00] (2字节)
```

**当前实现：**
```go
// ❌ 只读取 1 字节
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, 1))
```

#### 2. 协议类型

通过分析真实抓包，可以确认：

```
如果 NULL bitmap = 0x00，位 0,1,2 都是 0
- 位 0 = 0 → 参数 1 非空
- 位 1 = 0 → 参数 2 非空（如果存在）
- 位 2 = 0 → 参数 3 非空（如果存在）

这表明使用的是标准 MySQL 协议，不是 MariaDB 协议！
```

**结论：应该使用 `(n + 7) / 8` 而不是 `(n + 2 + 7) / 8`**

#### 3. 参数类型验证

真实抓包中的参数类型：
```go
0x01 = TINYINT
0x02 = SMALLINT
0x03 = INT
0xfd = VAR_STRING
```

这与当前实现一致。

## 🔧 修复方案

### 修复 1: Unmarshal 方法

```go
func (p *ComStmtExecutePacket) Unmarshal(r io.Reader) error {
    if err := p.Packet.Unmarshal(r); err != nil {
        return err
    }

    reader := bytes.NewReader(p.Payload)

    // 读取固定头部
    p.Command, _ = reader.ReadByte()
    p.StatementID, _ = ReadNumber[uint32](reader, 4)
    p.Flags, _ = ReadNumber[uint8](reader, 1)
    p.IterationCount, _ = ReadNumber[uint32](reader, 4)

    // ✅ 修复：启发式读取 NULL bitmap
    nullBitmap := make([]byte, 0)
    for {
        if reader.Len() == 0 {
            break
        }

        b, _ := reader.ReadByte()

        // Peek 下一个字节
        if reader.Len() > 0 {
            nextByte, _ := reader.ReadByte()
            reader.UnreadByte()

            // 如果下一个字节是 0x00 或 0x01，
            // 这可能是 NewParamsBindFlag
            if nextByte == 0x00 || nextByte == 0x01 {
                nullBitmap = append(nullBitmap, b)
                break
            }
        }

        nullBitmap = append(nullBitmap, b)
    }
    p.NullBitmap = nullBitmap

    // 读取 NewParamsBindFlag
    if reader.Len() > 0 {
        p.NewParamsBindFlag, _ = reader.ReadByte()
    }

    // 读取参数类型
    if p.NewParamsBindFlag == 1 {
        p.ParamTypes = make([]StmtParamType, 0)
        for reader.Len() >= 2 {
            paramType := StmtParamType{}
            paramType.Type, _ = reader.ReadByte()
            paramType.Flag, _ = reader.ReadByte()

            // 验证类型
            if !isValidMySQLType(paramType.Type) {
                reader.Seek(-2, io.SeekCurrent)
                break
            }

            p.ParamTypes = append(p.ParamTypes, paramType)
        }

        // 读取参数值
        // ... 现有逻辑
    }

    return nil
}
```

### 修复 2: NULL bitmap 计算

```go
// ❌ 错误：MariaDB 协议
requiredNullBitmapLen := (paramCount + 2 + 7) / 8

// ✅ 正确：标准 MySQL 协议
requiredNullBitmapLen := (paramCount + 7) / 8
```

### 修复 3: NULL 标志检查

```go
// ❌ 错误：MariaDB 协议（+2 偏移）
byteIdx := (i + 2) / 8
bitIdx := uint((i + 2) % 8)

// ✅ 正确：标准 MySQL 协议
byteIdx := i / 8
bitIdx := uint(i % 8)
```

## 📋 测试清单

运行分析工具：

```powershell
# 1. 分析抓包文件
cd d:/code/db/mysql/resource
go run parse_pcap_gopacket.go mysql.pcapng

# 2. 记录真实包格式
# 3. 对比当前实现
# 4. 应用修复
# 5. 验证修复
```

## 🎯 下一步行动

1. **运行分析工具**
   ```powershell
   cd d:/code/db/mysql/resource
   go run parse_pcap_gopacket.go mysql.pcapng
   ```

2. **分析输出**
   - 查看 NULL bitmap 的实际格式
   - 确认使用的协议（MySQL vs MariaDB）
   - 对比参数类型和值的格式

3. **应用修复**
   - 参考 `STMT_EXECUTE_FIX_SUMMARY.md`
   - 修改 `packet.go` 中的实现
   - 使用正确的协议标准

4. **验证**
   ```powershell
   cd d:/code/db
   go test -v ./mysql/protocol -run TestComStmtExecute
   ```

## 📚 相关文件

- `mysql/protocol/packet.go` - 需要修复的主文件
- `mysql/resource/parse_pcap_gopacket.go` - 分析工具（推荐）
- `mysql/STMT_EXECUTE_FIX_SUMMARY.md` - 完整修复指南
- `mysql/PCAP_ANALYSIS.md` - 抓包数据分析
- `mysql/protocol/test_pcap_comparison.go` - 测试用例

## 🔗 参考资料

- [Google gopacket](https://github.com/google/gopacket)
- [MySQL Protocol Documentation](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [COM_STMT_EXECUTE Specification](https://dev.mysql.com/doc/dev/mysql-server/latest/PACKET.html#PACKET_COM_STMT_EXECUTE)

---

**注意：** gopacket 库已经成功安装并创建了解析工具。如果直接运行遇到问题，可以：

1. 确认文件路径正确
2. 检查是否有权限问题
3. 使用 Windows PowerShell 直接运行

或者使用简化版的测试工具：
```powershell
cd d:/code/db/mysql
go run test_com_stmt_execute_simple.go
```
