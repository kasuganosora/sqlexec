# MySQL/MariaDB Protocol 包开发指南

本文档旨在帮助开发者理解和使用 `mysql/protocol` 包，避免常见的开发错误。

## 目录

- [核心设计原则](#核心设计原则)
- [Packet 结构体](#packet-结构体)
- [Unmarshal 实现规范](#unmarshal-实现规范)
- [Marshal 实现规范](#marshal-实现规范)
- [常见错误](#常见错误)
- [最佳实践](#最佳实践)

---

## 核心设计原则

### 1. 分离包头和载荷

协议包的设计遵循 MySQL/MariaDB 协议规范，将数据分为两部分：
- **包头（Packet Header）**：4字节，包含载荷长度和序列ID
- **载荷（Payload）**：实际的数据内容

```go
type Packet struct {
    PayloadLength uint32 // 载荷长度（3字节小端）
    SequenceID    uint8  // 序列ID（1字节）
    Payload      []byte  // 载荷数据（已读取）
}
```

### 2. 两阶段反序列化

反序列化过程分为两个阶段：
1. **第一阶段**：读取包头和载荷到 `Packet` 结构体
2. **第二阶段**：从 `Payload` 中解析具体包的各个字段

---

## Packet 结构体

### 设计目的

`Packet` 嵌入到具体的包类型中（如 `HandshakeResponse`、`FieldMetaPacket` 等），提供统一的包头处理。

### 重要字段

```go
type Packet struct {
    PayloadLength uint32 // 从包头读取的载荷长度
    SequenceID    uint8  // 序列ID，用于包排序
    Payload      []byte  // ⚠️ 关键：已读取的载荷数据
}
```

### Unmarshal 方法

```go
func (p *Packet) Unmarshal(r io.Reader) error {
    // 1. 读取 4 字节包头
    buf := make([]byte, 4)
    _, err = io.ReadFull(r, buf)
    if err != nil {
        return err
    }

    // 2. 解析包头（小端序）
    p.PayloadLength = uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16
    p.SequenceID = buf[3]

    // 3. 读取载荷数据
    if p.PayloadLength > 0 {
        p.Payload = make([]byte, p.PayloadLength)
        _, err = io.ReadFull(r, p.Payload)
        if err != nil {
            return err
        }
    }
    return nil
}
```

**关键点**：
- `Payload` 数据在调用 `Unmarshal(r)` 后**已经被读取完毕**
- 原始 reader `r` 的当前位置已经移动到这个包的末尾

---

## Unmarshal 实现规范

### ❌ 错误实现（常见错误）

```go
func (p *HandshakeResponse) Unmarshal(r io.Reader, capabilities uint32) error {
    // 读取包头和载荷
    p.Packet.Unmarshal(r)
    
    // ❌ 错误：继续从原始 reader 读取
    reader := bufio.NewReader(r)
    p.ClientCapabilities, _ = ReadNumber[uint16](reader, 2)
    // ...
}
```

**问题**：
1. `Packet.Unmarshal(r)` 已经将载荷读取到 `p.Payload` 中
2. 原始 reader `r` 已经移到了载荷数据的末尾
3. 继续从 `r` 读取会读取**下一个包的数据**，而不是当前包的数据
4. 结果：所有字段都是 0 或空字符串

### ✅ 正确实现

```go
func (p *HandshakeResponse) Unmarshal(r io.Reader, capabilities uint32) error {
    // 读取包头和载荷
    p.Packet.Unmarshal(r)
    
    // ✅ 正确：从已读取的 Payload 创建新的 reader
    reader := bufio.NewReader(bytes.NewReader(p.Payload))
    
    // 从 Payload reader 读取各个字段
    p.ClientCapabilities, _ = ReadNumber[uint16](reader, 2)
    p.ExtendedClientCapabilities, _ = ReadNumber[uint16](reader, 2)
    p.MaxPacketSize, _ = ReadNumber[uint32](reader, 4)
    // ...
}
```

### 标准模板

```go
func (p *YourPacket) Unmarshal(r io.Reader, conditional uint32) error {
    // 1. 读取包头和载荷到 Packet
    if err := p.Packet.Unmarshal(r); err != nil {
        return err
    }
    
    // 2. ⚠️ 从 Payload 创建 reader，不要使用原始 reader
    reader := bufio.NewReader(bytes.NewReader(p.Payload))
    
    // 3. 使用辅助函数从 Payload reader 读取字段
    p.Field1, _ = ReadNumber[uint8](reader, 1)
    p.Field2, _ = ReadStringByLenencFromReader[uint8](reader)
    
    // 4. 根据条件标志读取可选字段
    if conditional&CLIENT_PROTOCOL_41 != 0 {
        p.Warnings, _ = ReadNumber[uint16](reader, 2)
        p.StatusFlags, _ = ReadNumber[uint16](reader, 2)
    }
    
    return nil
}
```

---

## Marshal 实现规范

### 标准模板

```go
func (p *YourPacket) Marshal() ([]byte, error) {
    // 1. 创建缓冲区
    buf := new(bytes.Buffer)
    
    // 2. 写入载荷数据
    WriteNumber(buf, p.Field1, 1)
    WriteStringByLenenc(buf, p.Field2)
    // ...
    
    payload := buf.Bytes()
    
    // 3. 创建完整包（包头 + 载荷）
    packetBuf := new(bytes.Buffer)
    
    // 写入包头
    packetBuf.WriteByte(byte(len(payload)))
    packetBuf.WriteByte(byte(len(payload) >> 8))
    packetBuf.WriteByte(byte(len(payload) >> 16))
    packetBuf.WriteByte(p.SequenceID)
    
    // 写入载荷
    packetBuf.Write(payload)
    
    return packetBuf.Bytes(), nil
}
```

### 能力标志参数

某些包需要根据能力标志来决定序列化方式：

```go
func (p *YourPacket) Marshal(capabilities uint32) ([]byte, error) {
    buf := new(bytes.Buffer)
    
    WriteNumber(buf, p.Field1, 1)
    
    // 根据能力标志决定是否写入某个字段
    if capabilities&CLIENT_PROTOCOL_41 != 0 {
        WriteNumber(buf, p.Warnings, 2)
        WriteNumber(buf, p.StatusFlags, 2)
    }
    
    // ... 构建完整包
}
```

### 兼容性方法

为了向后兼容，提供 `MarshalDefault` 方法：

```go
func (p *YourPacket) MarshalDefault() ([]byte, error) {
    // 使用默认能力标志
    return p.Marshal(0)
}
```

---

## 常见错误

### 1. 使用原始 reader 解析载荷

**错误代码**：
```go
func (p *FieldMetaPacket) Unmarshal(r io.Reader, capabilities uint32) error {
    p.Packet.Unmarshal(r)
    reader := bufio.NewReader(r)  // ❌ 错误！
    // ...
}
```

**症状**：
- 所有字段都是 0 或空字符串
- 测试失败：`Not equal: expected: 0x1, actual: 0x0`
- 反序列化后的数据与预期不符

**修复**：
```go
func (p *FieldMetaPacket) Unmarshal(r io.Reader, capabilities uint32) error {
    p.Packet.Unmarshal(r)
    reader := bufio.NewReader(bytes.NewReader(p.Payload))  // ✅ 正确
    // ...
}
```

### 2. 忘记调用 `Packet.Unmarshal()`

**错误代码**：
```go
func (p *YourPacket) Unmarshal(r io.Reader) error {
    // ❌ 忘记读取包头
    reader := bufio.NewReader(r)
    // 直接从流中读取字段...
}
```

**症状**：
- 将包头数据误认为是字段数据
- 字段值完全错误

**修复**：
```go
func (p *YourPacket) Unmarshal(r io.Reader) error {
    p.Packet.Unmarshal(r)  // ✅ 先读取包头和载荷
    reader := bufio.NewReader(bytes.NewReader(p.Payload))
    // 然后读取字段...
}
```

### 3. Marshal 时缺少 SequenceID

**错误代码**：
```go
func (p *YourPacket) Marshal() ([]byte, error) {
    buf := new(bytes.Buffer)
    // 写入载荷...
    payload := buf.Bytes()
    
    // ❌ 忘记写入 SequenceID
    packetBuf.Write([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16)})
    packetBuf.Write(payload)
}
```

**症状**：
- 包头只有 3 字节，缺少序列ID
- 服务器无法正确处理包

**修复**：
```go
func (p *YourPacket) Marshal() ([]byte, error) {
    // ... 
    // ✅ 包头必须包含序列ID（第4字节）
    packetBuf.Write([]byte{
        byte(len(payload)),
        byte(len(payload) >> 8),
        byte(len(payload) >> 16),
        p.SequenceID,  // ✅ 序列ID
    })
    // ...
}
```

### 4. 能力标志参数类型不匹配

**错误代码**：
```go
func (p *FieldMetaPacket) MarshalDefault() ([]byte, error) {
    return p.Marshal(0)  // ❌ Marshal 期望 uint32，传入了 int
}
```

**修复**：
```go
func (p *FieldMetaPacket) MarshalDefault() ([]byte, error) {
    return p.Marshal(uint32(0))  // ✅ 显式类型转换
}
```

---

## 最佳实践

### 1. 总是使用辅助函数

使用 `type.go` 中提供的辅助函数来读取和写入数据：

```go
// ✅ 使用辅助函数
p.Field, _ = ReadNumber[uint16](reader, 2)
p.Text, _ = ReadStringByLenencFromReader[uint8](reader)
p.Len, _ = ReadLenencNumber[uint64](reader)

WriteNumber(buf, p.Field, 2)
WriteStringByLenenc(buf, p.Text)
WriteLenencNumber(buf, p.Len)
```

### 2. 检查能力标志

在 Unmarshal 和 Marshal 时都要检查能力标志：

```go
// Unmarshal 时检查
if conditional&CLIENT_PROTOCOL_41 != 0 {
    p.Warnings, _ = ReadNumber[uint16](reader, 2)
    p.StatusFlags, _ = ReadNumber[uint16](reader, 2)
}

// Marshal 时检查
if capabilities&CLIENT_PROTOCOL_41 != 0 {
    WriteNumber(buf, p.Warnings, 2)
    WriteNumber(buf, p.StatusFlags, 2)
}
```

### 3. 处理可选字段

使用指针类型表示可选字段：

```go
type ColumnCountPacket struct {
    Packet
    ColumnCount     uint64
    MetadataFollows *uint8  // 可选字段，使用指针
}

// Unmarshal 时
if capabilities&MARIADB_CLIENT_CACHE_METADATA != 0 {
    peekBytes, err := reader.Peek(1)
    if err == nil && len(peekBytes) > 0 {
        metadataFollows, _ := reader.ReadByte()
        p.MetadataFollows = &metadataFollows  // 设置指针
    }
}

// Marshal 时
if capabilities&MARIADB_CLIENT_CACHE_METADATA != 0 && p.MetadataFollows != nil {
    buf.WriteByte(*p.MetadataFollows)
}
```

### 4. 提供兼容性方法

为需要能力标志参数的包提供 `Default` 方法：

```go
// 支持能力标志的完整方法
func (p *YourPacket) Unmarshal(r io.Reader, capabilities uint32) error { ... }

// 兼容性方法（使用默认能力标志）
func (p *YourPacket) UnmarshalDefault(r io.Reader) error {
    return p.Unmarshal(r, 0)
}

func (p *YourPacket) Marshal(capabilities uint32) ([]byte, error) { ... }

func (p *YourPacket) MarshalDefault() ([]byte, error) {
    return p.Marshal(0)
}
```

### 5. 编写完整的测试

测试应该覆盖：
- 序列化和反序列化的往返测试
- 边界情况（空值、最大值等）
- 不同能力标志组合
- 真实协议数据

```go
func TestYourPacket(t *testing.T) {
    // 1. 创建包
    packet := &YourPacket{
        Field1: 123,
        Field2: "test",
    }
    
    // 2. 序列化
    data, err := packet.Marshal()
    assert.NoError(t, err)
    
    // 3. 反序列化
    packet2 := &YourPacket{}
    err = packet2.Unmarshal(bytes.NewReader(data), capabilities)
    assert.NoError(t, err)
    
    // 4. 验证数据一致性
    assert.Equal(t, packet.Field1, packet2.Field1)
    assert.Equal(t, packet.Field2, packet2.Field2)
    
    // 5. 测试真实协议数据
    realData := []byte{...}
    realPacket := &YourPacket{}
    err = realPacket.Unmarshal(bytes.NewReader(realData), capabilities)
    assert.NoError(t, err)
    assert.Equal(t, expectedValue, realPacket.Field)
}
```

### 6. 使用 bufio.Reader

创建 reader 时使用 `bufio.NewReader` 提供性能和便利方法：

```go
// ✅ 使用 bufio.Reader
reader := bufio.NewReader(bytes.NewReader(p.Payload))

// 可以使用 Peek 方法
peekBytes, err := reader.Peek(1)
if err == nil && peekBytes[0] == '#' {
    // ...
}
```

---

## 快速检查清单

在实现新的包类型时，使用以下检查清单：

### Unmarshal 实现检查

- [ ] 调用了 `p.Packet.Unmarshal(r)` 读取包头和载荷
- [ ] 使用 `bytes.NewReader(p.Payload)` 创建 reader（**不是原始 reader**）
- [ ] 使用 `bufio.NewReader` 包装 reader
- [ ] 根据能力标志检查可选字段
- [ ] 正确处理所有字段类型（数字、字符串、长度编码等）
- [ ] 错误处理适当

### Marshal 实现检查

- [ ] 创建载荷缓冲区
- [ ] 使用辅助函数写入数据
- [ ] 根据能力标志写入可选字段
- [ ] 创建完整包缓冲区
- [ ] 正确写入包头（长度 + 序列ID）
- [ ] 写入载荷数据
- [ ] 返回完整包的字节切片

### 测试检查

- [ ] 序列化/反序列化往返测试
- [ ] 边界情况测试
- [ ] 不同能力标志组合测试
- [ ] 真实协议数据测试
- [ ] 所有断言通过

---

## 调试技巧

### 1. 打印原始数据

```go
func (p *YourPacket) Unmarshal(r io.Reader, capabilities uint32) error {
    // 打印原始数据用于调试
    data, _ := io.ReadAll(r)
    fmt.Printf("Raw data: %x\n", data)
    
    // 创建 reader 从原始数据读取
    reader := bytes.NewReader(data)
    // ...
}
```

### 2. 使用 hex.Dump

```go
import "encoding/hex"

func (p *YourPacket) Unmarshal(r io.Reader, capabilities uint32) error {
    data, _ := io.ReadAll(r)
    fmt.Printf("Raw data:\n%s", hex.Dump(data))
    // ...
}
```

### 3. 逐步验证

```go
func (p *YourPacket) Unmarshal(r io.Reader, capabilities uint32) error {
    p.Packet.Unmarshal(r)
    
    // 验证 Payload 是否正确读取
    log.Printf("PayloadLength: %d", p.PayloadLength)
    log.Printf("Payload: %x", p.Payload)
    
    reader := bufio.NewReader(bytes.NewReader(p.Payload))
    
    // 逐步读取并打印
    field1, _ := ReadNumber[uint8](reader, 1)
    log.Printf("Field1: %d", field1)
    // ...
}
```

---

## 参考资源

- [MySQL Protocol Documentation](https://dev.mysql.com/doc/dev/mysql-server/latest/PACKET.html)
- [MariaDB Protocol Documentation](https://mariadb.com/docs/server/reference/clientserver-protocol/)
- [Helper Functions](./type.go) - 读写辅助函数
- [Constants](./const.go) - 协议常量和能力标志
- [Existing Packet Implementations](./packet.go) - 参考现有实现

---

## 贡献指南

添加新的包类型时：

1. 在 `packet.go` 中定义结构体和 Unmarshal/Marshal 方法
2. 添加测试用例到 `*_test.go` 文件
3. 确保所有测试通过
4. 更新本文档（如果需要）
5. 提交前运行 `go test -v ./mysql/protocol`

---

**版本**: 1.0  
**最后更新**: 2026-01-17  
**维护者**: 开发团队
