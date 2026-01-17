# COM_STMT_EXECUTE 包分析 - 抓包数据对比

## 问题总结

通过分析 `mysql.pcapng` 抓包文件和当前 `ComStmtExecutePacket` 实现，发现了以下关键问题：

## 1. NULL Bitmap 计算问题

### MySQL 协议标准
```go
// MySQL 标准协议：NULL bitmap 长度计算
nullBitmapLen = (numParams + 7) / 8
```

### 当前实现
```go
// packet.go: 1236-1247
requiredNullBitmapLen := (paramCount + 2 + 7) / 8  // MariaDB 协议
```

**问题：** 当前实现使用了 MariaDB 特定的位偏移（+2），这与标准 MySQL 协议不一致。

### 正确的协议格式

#### 标准 MySQL 协议
```
1 [command]           = 0x17 (COM_STMT_EXECUTE)
4 [statement_id]
1 [flags]
4 [iteration_count]
N [null_bitmap]      = (num_params + 7) / 8 字节
1 [new_params_bind_flag]
2 * n [param_types]  (如果 new_params_bind_flag = 1)
... [param_values]
```

**NULL bitmap 映射：**
- 位 0 对应参数 1
- 位 1 对应参数 2
- 位 2 对应参数 3
- ...

#### MariaDB 协议
```
1 [command]
4 [statement_id]
1 [flags]
4 [iteration_count]
N [null_bitmap]      = (num_params + 2 + 7) / 8 字节
1 [new_params_bind_flag]
2 * n [param_types]
... [param_values]
```

**NULL bitmap 映射：**
- 位 0, 1 保留（未使用）
- 位 2 对应参数 1
- 位 3 对应参数 2
- 位 4 对应参数 3
- ...

## 2. Unmarshal 方法的问题

### 当前实现的问题 (packet.go: 1188-1343)

```go
// ❌ 问题代码
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, 1))
```

**问题：**
1. 只读取 1 字节 NULL bitmap
2. 对于多个参数，NULL bitmap 长度可能超过 1 字节
3. 无法正确确定参数数量

### 修复方案

```go
func (p *ComStmtExecutePacket) Unmarshal(r io.Reader) error {
    // 1. 读取包头
    if err := p.Packet.Unmarshal(r); err != nil {
        return err
    }

    reader := bufio.NewReader(bytes.NewReader(p.Payload))

    // 2. 读取固定字段
    p.Command, _ = reader.ReadByte()
    p.StatementID, _ = ReadNumber[uint32](reader, 4)
    p.Flags, _ = ReadNumber[uint8](reader, 1)
    p.IterationCount, _ = ReadNumber[uint32](reader, 4)

    // 3. 读取剩余数据
    remainingData, _ := io.ReadAll(reader)
    if len(remainingData) == 0 {
        return nil // 没有参数
    }

    // 4. 确定参数数量
    // 这是一个鸡生蛋蛋生鸡的问题：
    // - 需要 NULL bitmap 长度来读取参数类型
    // - 需要参数类型来计算 NULL bitmap 长度

    // 解决方案：根据 NewParamsBindFlag 来决定
    if len(remainingData) > 0 {
        // 读取 NewParamsBindFlag
        // 但它是在 NULL bitmap 之后！

        // 实际协议顺序是：
        // [null_bitmap][new_params_bind_flag][param_types][values]

        // 所以我们需要先读取 NULL bitmap，
        // 但不知道要读多少字节

        // 启发式方法：
        // 1. 先读 1 字节作为可能的 NewParamsBindFlag
        // 2. 检查是否是 0x00 或 0x01
        // 3. 如果不是，这 1 字节是 NULL bitmap 的一部分
        // 4. 继续这个过程
    }

    return nil
}
```

### 正确的解析逻辑

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

    // 读取 NULL bitmap
    // 启发式方法：读取直到遇到 0x00 或 0x01
    nullBitmap := make([]byte, 0)
    for {
        if reader.Len() == 0 {
            break
        }

        b, _ := reader.ReadByte()

        // Peek 下一个字节
        if reader.Len() > 0 {
            nextByte, _ := reader.ReadByte()
            reader.UnreadByte() // 放回去

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

    // 如果 flag=1，读取参数类型
    if p.NewParamsBindFlag == 1 {
        p.ParamTypes = make([]StmtParamType, 0)
        for reader.Len() >= 2 {
            paramType := StmtParamType{}
            paramType.Type, _ = reader.ReadByte()
            paramType.Flag, _ = reader.ReadByte()

            // 检查是否是有效的类型
            validTypes := map[uint8]bool{
                0x01: true, 0x02: true, 0x03: true, 0x04: true,
                0x05: true, 0x06: true, 0x07: true, 0x08: true,
                0x09: true, 0x0a: true, 0x0b: true, 0x0c: true,
                0x0d: true, 0x0e: true, 0x0f: true, 0x10: true,
                0xfd: true, 0xfe: true, 0xff: true,
            }

            if !validTypes[paramType.Type] {
                // 可能到了参数值部分
                break
            }

            p.ParamTypes = append(p.ParamTypes, paramType)
        }

        // 读取参数值
        p.ParamValues = make([]any, 0, len(p.ParamTypes))
        for i, paramType := range p.ParamTypes {
            // 检查 NULL 标志
            byteIdx := (i + 2) / 8  // MariaDB 协议
            bitIdx := uint((i + 2) % 8)
            if len(p.NullBitmap) > byteIdx &&
                (p.NullBitmap[byteIdx] & (1 << bitIdx)) != 0 {
                p.ParamValues = append(p.ParamValues, nil)
                continue
            }

            // 根据类型读取值
            // ... 现有逻辑保持不变
        }
    }

    return nil
}
```

## 3. Marshal 方法的问题

### 当前实现的问题 (packet.go: 1345-1467)

```go
// 计算参数数量
paramCount := len(p.ParamTypes)
if paramCount == 0 && len(p.ParamValues) > 0 {
    paramCount = len(p.ParamValues)
}

// 根据参数数量计算 NULL Bitmap 长度
// MariaDB协议：NULL bitmap的第0,1位不使用，从第2位开始存储第1个参数的NULL标志
// 所以对于n个参数，需要的字节数为 ceil((n + 2) / 8)
nullBitmapLen := (paramCount + 2 + 7) / 8

// 确保 NullBitmap 长度正确
if len(p.NullBitmap) < nullBitmapLen {
    // 扩展 NullBitmap
    newBitmap := make([]byte, nullBitmapLen)
    copy(newBitmap, p.NullBitmap)
    p.NullBitmap = newBitmap
} else if len(p.NullBitmap) > nullBitmapLen {
    // 截断 NullBitmap
    p.NullBitmap = p.NullBitmap[:nullBitmapLen]
}
```

**问题：**
1. NULL bitmap 计算使用 MariaDB 协议（+2）
2. 但标准 MySQL 协议不需要 +2
3. 需要明确项目目标是兼容 MySQL 还是 MariaDB

### 修复方案

```go
func (p *ComStmtExecutePacket) Marshal() ([]byte, error) {
    buf := new(bytes.Buffer)

    // 写入固定字段
    WriteNumber(buf, p.Command, 1)
    WriteNumber(buf, p.StatementID, 4)
    WriteNumber(buf, p.Flags, 1)
    WriteNumber(buf, p.IterationCount, 4)

    // 计算参数数量
    paramCount := len(p.ParamTypes)
    if paramCount == 0 && len(p.ParamValues) > 0 {
        paramCount = len(p.ParamValues)
    }

    // 计算 NULL Bitmap 长度
    // 选择 MySQL 标准协议
    nullBitmapLen := (paramCount + 7) / 8

    // 确保 NullBitmap 长度正确
    if len(p.NullBitmap) < nullBitmapLen {
        newBitmap := make([]byte, nullBitmapLen)
        copy(newBitmap, p.NullBitmap)
        p.NullBitmap = newBitmap
    } else if len(p.NullBitmap) > nullBitmapLen {
        p.NullBitmap = p.NullBitmap[:nullBitmapLen]
    }

    // 写入 NULL bitmap
    WriteBinary(buf, p.NullBitmap)

    // 写入 NewParamsBindFlag
    WriteNumber(buf, p.NewParamsBindFlag, 1)

    // 写入参数类型和值
    // ... 现有逻辑保持不变
}
```

## 4. 测试建议

### 使用抓包数据验证

1. **解析 pcapng 文件**
   ```powershell
   cd d:/code/db/mysql/resource
   go run parse_pcap_gopcap.go mysql.pcapng
   ```

2. **对比包结构**
   - 查看真实 MySQL 服务器的包格式
   - 对比当前实现生成的包
   - 找出差异

3. **关键检查点**
   - NULL bitmap 的长度和内容
   - 参数类型的格式
   - 参数值的编码方式

### 创建对比测试

```go
// test_com_stmt_execute_pcap.go
func TestCompareWithRealPacket(t *testing.T) {
    // 从 pcapng 文件中提取的真实包数据
    realPacket := []byte{
        // [length(3)][seq][command(0x17)][stmt_id][flags][iteration]...
        0x0c, 0x00, 0x00, 0x02, // 包头
        0x17,                       // COM_STMT_EXECUTE
        0x01, 0x00, 0x00, 0x00,   // StatementID = 1
        0x00,                       // Flags
        0x01, 0x00, 0x00, 0x00,   // IterationCount = 1
        0x00,                       // NULL bitmap
        0x01,                       // NewParamsBindFlag
        0x01, 0x00,               // ParamType: TINYINT
        0x7b,                       // ParamValue: 123
    }

    // 使用当前实现解析
    packet := &ComStmtExecutePacket{}
    err := packet.Unmarshal(bytes.NewReader(realPacket))
    assert.NoError(t, err)

    // 验证解析结果
    assert.Equal(t, uint8(0x17), packet.Command)
    assert.Equal(t, uint32(1), packet.StatementID)
    assert.Equal(t, uint8(0), packet.Flags)
    assert.Equal(t, uint32(1), packet.IterationCount)
    assert.Equal(t, uint8(1), packet.NewParamsBindFlag)
    assert.Equal(t, 1, len(packet.ParamTypes))
    assert.Equal(t, 1, len(packet.ParamValues))
}
```

## 5. 最终修复清单

### 必须修复的问题

- [ ] **确定协议标准**：MySQL 还是 MariaDB
- [ ] **修正 NULL bitmap 计算**：使用正确的公式
- [ ] **修正 Unmarshal 逻辑**：正确读取 NULL bitmap
- [ ] **修正 Marshal 逻辑**：正确生成 NULL bitmap
- [ ] **添加边界测试**：0参数、多参数、NULL 参数

### 建议的测试用例

1. **0 个参数的预处理语句**
   ```sql
   PREPARE stmt1 FROM 'SELECT 1';
   EXECUTE stmt1;
   ```

2. **1 个参数**
   ```sql
   PREPARE stmt1 FROM 'SELECT * FROM t WHERE id = ?';
   SET @id = 1;
   EXECUTE stmt1 USING @id;
   ```

3. **多个参数**
   ```sql
   PREPARE stmt1 FROM 'SELECT * FROM t WHERE id = ? AND name = ?';
   EXECUTE stmt1 USING 1, 'test';
   ```

4. **NULL 参数**
   ```sql
   EXECUTE stmt1 USING NULL;
   ```

5. **参数超过 8 个**（测试 NULL bitmap 多字节情况）
   ```sql
   PREPARE stmt1 FROM 'SELECT * FROM t WHERE c1=? AND c2=? AND c3=? AND c4=? AND c5=? AND c6=? AND c7=? AND c8=? AND c9=?';
   EXECUTE stmt1 USING 1,2,3,4,5,6,7,8,9;
   ```

## 6. 推荐的实施步骤

1. **第一步**：运行 pcapng 解析脚本，获取真实包数据
2. **第二步**：创建对比测试，验证当前实现的差异
3. **第三步**：确定协议标准（MySQL 或 MariaDB）
4. **第四步**：修正 NULL bitmap 计算公式
5. **第五步**：修正 Unmarshal 和 Marshal 方法
6. **第六步**：运行所有测试，确保修复正确
7. **第七步**：与真实 MySQL/MariaDB 服务器进行集成测试

## 相关文件

- `mysql/protocol/packet.go` - ComStmtExecutePacket 实现
- `mysql/protocol/prepare_test.go` - 现有测试
- `mysql/test_com_stmt_execute_simple.go` - 简化测试
- `mysql/parse_pcap_gopcap.go` - 抓包解析工具
- `mysql/COM_STMT_EXECUTE_ANALYSIS.md` - 详细分析文档

## 参考资料

- [MySQL Protocol - COM_STMT_EXECUTE](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [MariaDB Protocol - Binary Protocol Prepared Statements](https://mariadb.com/docs/server/reference/clientserver-protocol/3-binary-protocol-prepared-statements/com_stmt_prepare/)
- MySQL Packet Structure: [length(3)][seq(1)][payload...]
