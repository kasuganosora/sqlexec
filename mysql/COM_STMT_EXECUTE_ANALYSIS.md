# ComStmtExecutePacket 问题分析与测试方案

## 当前实现的问题

基于代码分析，`ComStmtExecutePacket` 存在以下潜在问题：

### 1. NULL Bitmap 计算问题

**当前实现：**
```go
// Unmarshal (行 1236-1247)
requiredNullBitmapLen := (paramCount + 2 + 7) / 8
```

**问题：**
- MariaDB 协议规定 NULL bitmap 的第 0、1 位不使用
- 第 n 个参数的 NULL 标志应该存储在位 (n + 2)
- 计算公式 `(paramCount + 2 + 7) / 8` 可能不正确

**正确的公式应该是：**
```go
// 对于 n 个参数，需要覆盖位 2 到位 (n + 1)
// 字节数 = ceil((n + 2) / 8)
requiredNullBitmapLen := (paramCount + 2 + 7) / 8  // 这个公式是正确的
```

### 2. NULL Bitmap 初始化问题

**当前实现 (Marshal, 行 1206-1211)：**
```go
// 读取NULL bitmap - 假设至少1字节（对于少量参数）
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, 1))
```

**问题：**
- 只读取 1 字节，对于多个参数可能不够
- 应该根据参数数量动态计算 NULL bitmap 长度

**正确做法：**
```go
// 先读取所有剩余数据
remainingData, _ := io.ReadAll(reader)
dataReader := bytes.NewReader(remainingData)

// 计算需要的 NULL bitmap 长度
// 在读取参数类型之前，我们还不知道参数数量
// 这是一个鸡生蛋蛋生鸡的问题
```

### 3. 参数类型和参数值的读取顺序

**当前实现流程：**
1. 读取固定头部 (11 字节)
2. 读取 NULL bitmap (只读 1 字节)
3. 读取 NewParamsBindFlag
4. 如果 flag=1，读取所有可能的参数类型
5. 根据参数类型数量，重新确定 NULL bitmap 长度
6. 读取参数值

**问题：**
- 步骤 2 只读取 1 字节，但步骤 5 可能需要更多字节
- 这会导致数据错位

**正确做法：**
- 先读取 NewParamsBindFlag
- 根据 flag 决定如何读取后续数据
- 对于 flag=1 的情况：
  - 读取参数类型数据（每 2 字节一个参数）
  - 计算参数数量
  - 根据参数数量计算 NULL bitmap 长度
  - 从剩余数据中读取正确长度的 NULL bitmap
  - 但问题是 NULL bitmap 应该在参数类型之前！

**这揭示了一个设计问题：**

MariaDB 协议的 COM_STMT_EXECUTE 包结构：
```
1 [command] COM_STMT_EXECUTE (0x17)
4 [statement_id]
1 [flags]
4 [iteration_count]
N [null_bitmap] (可变长度)
1 [new_params_bind_flag]
2 * n [param_types] (如果 new_params_bind_flag = 1)
... [param_values]
```

**关键问题：** NULL bitmap 的长度取决于参数数量，但参数类型在 NULL bitmap 之后读取！

**解决方案：**
1. 读取固定头部 (10 字节，不包括 NULL bitmap)
2. 读取 NewParamsBindFlag
3. 如果 flag=0，参数类型已经缓存，可以直接计算 NULL bitmap 长度
4. 如果 flag=1，需要先读取参数类型才能知道参数数量

**但实际上，协议的设计是：**
- NULL bitmap 的长度应该在准备阶段就知道（通过 COM_STMT_PREPARE）
- 所以客户端和服务器都应该知道参数数量

### 4. Unmarshal 中的 NULL Bitmap 读取逻辑

**当前实现 (行 1210-1216)：**
```go
// 读取NULL bitmap - 假设至少1字节（对于少量参数）
p.NullBitmap, _ = io.ReadAll(io.LimitReader(dataReader, 1))

// 读取NewParamsBindFlag
if dataReader.Len() > 0 {
    p.NewParamsBindFlag, _ = dataReader.ReadByte()
}
```

**问题：**
- 使用 `io.LimitReader(dataReader, 1)` 只读取 1 字节
- 但后续可能需要更多字节

**建议修改：**
```go
// 先读取所有剩余数据
remainingData, _ := io.ReadAll(reader)
dataReader := bytes.NewReader(remainingData)

// 读取 NewParamsBindFlag（如果存在）
if dataReader.Len() > 0 {
    // 尝试解析参数类型来确定需要的 NULL bitmap 长度
    // 但这是后向解析，很困难

    // 更好的方法：假设参数类型数据在最后
    // 先尝试读取参数类型，然后推断 NULL bitmap 长度
}
```

## 测试方案

### 方案 1：使用测试客户端和服务器

**文件：**
- `mysql/test_stmt_execute_server.go` - 测试服务器
- `mysql/test_stmt_execute_client.go` - 测试客户端
- `mysql/test_com_stmt_execute_simple.go` - 简化版测试程序

**运行方法：**

```powershell
# 终端 1：启动服务器
cd d:/code/db
go run mysql/test_stmt_execute_server.go

# 终端 2：运行客户端
cd d:/code/db
go run mysql/test_stmt_execute_client.go
```

**预期输出：**
服务器会显示：
```
收到 COM_STMT_EXECUTE
原始包数据 (hex): ...
StatementID: 1
Flags: 0x00
IterationCount: 1
NullBitmap: [0x00]
NewParamsBindFlag: 1
ParamTypes 数量: 1
  ParamTypes[0]: Type=0x03, Flag=0x00
ParamValues 数量: 1
  ParamValues[0]: 123 (int32)
```

### 方案 2：使用简化版测试程序

**文件：** `mysql/test_com_stmt_execute_simple.go`

**运行方法：**
```powershell
cd d:/code/db/mysql
go run test_com_stmt_execute_simple.go
```

**输出示例：**
```
=== 测试1：单个 INT 参数 ===
✅ 序列化成功
   数据 (hex): 0c0000001710000000000000010000000103
   数据长度: 12 字节

   包结构分析:
   包头 (4字节): 0c000000
   - 载荷长度: 12
   - SequenceID: 0
   载荷 (12字节): 1710000000000000010000000103

   载荷解析:
   - Command: 0x17 (COM_STMT_EXECUTE)
   - StatementID: 1
   - Flags: 0x00
   - IterationCount: 1
   - NullBitmap: 00
   - NewParamsBindFlag: 1
   - ParamTypes:
     [0] Type=0x03, Flag=0x00
   - ParamValues:
     [0] INT: 123
```

### 方案 3：使用真实 MySQL 服务器抓包

**步骤：**

1. 启动真实 MySQL 服务器或使用现有的
2. 使用 Wireshark 抓包
3. 执行预处理语句：
   ```sql
   PREPARE stmt1 FROM 'SELECT * FROM users WHERE id = ?';
   SET @id = 1;
   EXECUTE stmt1 USING @id;
   ```
4. 分析 COM_STMT_EXECUTE 包
5. 对比当前实现生成的包格式

## 建议的修复步骤

### 步骤 1：修正 Unmarshal 方法

```go
func (p *ComStmtExecutePacket) Unmarshal(r io.Reader) error {
    // 1. 读取包头
    if err := p.Packet.Unmarshal(r); err != nil {
        return err
    }

    // 2. 读取固定头部
    reader := bytes.NewReader(p.Payload)

    if len(p.Payload) < 11 {
        return errors.New("payload too short")
    }

    p.Command, _ = reader.ReadByte()
    p.StatementID, _ = ReadNumber[uint32](reader, 4)
    p.Flags, _ = ReadNumber[uint8](reader, 1)
    p.IterationCount, _ = ReadNumber[uint32](reader, 4)

    // 3. 读取剩余数据
    remainingData, _ := io.ReadAll(reader)
    if len(remainingData) == 0 {
        // 没有参数
        return nil
    }

    dataReader := bytes.NewReader(remainingData)

    // 4. 读取 NewParamsBindFlag
    if dataReader.Len() > 0 {
        p.NewParamsBindFlag, _ = dataReader.ReadByte()
    }

    // 5. 如果 flag=1，读取参数类型
    if p.NewParamsBindFlag == 1 {
        p.ParamTypes = make([]StmtParamType, 0)
        for dataReader.Len() >= 2 {
            paramType := StmtParamType{}
            paramType.Type, _ = dataReader.ReadByte()
            paramType.Flag, _ = dataReader.ReadByte()
            p.ParamTypes = append(p.ParamTypes, paramType)
        }

        // 6. 根据 ParamTypes 数量计算 NULL bitmap 长度
        paramCount := len(p.ParamTypes)
        nullBitmapLen := (paramCount + 2 + 7) / 8

        // 7. 从剩余数据的开头读取 NULL bitmap
        // 注意：NULL bitmap 实际上在 NewParamsBindFlag 之前！
        // 所以这个逻辑是错误的
    }

    // 正确的逻辑应该是：
    // - 先读取 NewParamsBindFlag
    // - 如果 flag=1，先读取参数类型
    // - 但 NULL bitmap 在 NewParamsBindFlag 之前
    // - 需要知道参数数量才能确定 NULL bitmap 长度

    return nil
}
```

**这个设计问题说明需要重新思考：**

实际协议结构应该是：
```
1 [command]
4 [statement_id]
1 [flags]
4 [iteration_count]
1 [new_params_bind_flag]
N [null_bitmap] - 长度取决于参数数量
2*n [param_types] (如果 new_params_bind_flag=1)
... [param_values]
```

但这样的话，NULL bitmap 在 new_params_bind_flag 之后，但参数类型在 NULL bitmap 之后，这导致无法确定 NULL bitmap 的长度。

**查看 MySQL 官方协议文档：**

根据 [MySQL Protocol - COM_STMT_EXECUTE](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)：

```
COM_STMT_EXECUTE
  command: 1 byte [17]
  statement_id: 4 bytes
  flags: 1 byte
  iteration_count: 4 bytes
  null_bitmap: length = (num_params + 7) / 8
  new_params_bind_flag: 1 byte
  if new_params_bind_flag:
    param_type: 2 bytes * num_params
  param_values: ...
```

**关键发现：**
- NULL bitmap 的长度是 `(num_params + 7) / 8`，不是 `(num_params + 2 + 7) / 8`
- 这意味着第 0 位对应第 1 个参数

**所以当前的实现使用了 MariaDB 特有的位偏移！**

需要确认：
1. 是否兼容 MySQL？
2. 如果兼容 MariaDB，需要使用 `(n + 2 + 7) / 8`
3. 如果兼容 MySQL，需要使用 `(n + 7) / 8`

## 测试命令

### 运行测试服务器
```powershell
cd d:/code/db
go run mysql/test_stmt_execute_server.go
```

### 运行测试客户端
```powershell
cd d:/code/db
go run mysql/test_stmt_execute_client.go
```

### 运行简化测试
```powershell
cd d:/code/db/mysql
go run test_com_stmt_execute_simple.go
```

### 运行协议测试
```powershell
cd d:/code/db
go test -v ./mysql/protocol -run TestComStmtExecutePacket
```

## 结论

`ComStmtExecutePacket` 的主要问题在于：

1. **NULL Bitmap 长度计算**：需要确认是使用 MySQL 还是 MariaDB 的计算方式
2. **NULL Bitmap 位置**：当前实现在 Unmarshal 中的读取逻辑有误
3. **参数数量确定**：需要在读取 NULL bitmap 之前知道参数数量

**建议：**
1. 先运行测试程序，查看当前实现的输出
2. 使用真实 MySQL/MariaDB 服务器抓包对比
3. 根据实际协议文档修正实现
4. 添加更多边界条件测试
