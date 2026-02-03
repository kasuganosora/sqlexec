# 单元测试 (Unit Tests)

单元测试使用 Mock 对象测试单个组件，不涉及真实的网络连接或数据库。

## 测试文件

### boundary_test.go

**测试内容：** 包序列化/反序列化的边界条件

**主要测试：**
- 大包处理（1MB, 100KB）
- 空包和单字节包
- 特殊字符处理（ASCII, Unicode, Emoji, 控制字符）
- UTF-8 编码验证
- 数据库名称和错误消息的特殊字符
- 连接关闭和错误处理
- 多个连续包
- 序列号255和0的边界
- 最大载荷长度

**运行方式：**
```bash
go test ./server/testing/unit -run TestBoundary
```

### protocol_flow_test.go

**测试内容：** 完整的协议流程

**主要测试：**
- 握手流程（HandshakeV10Packet 序列化/反序列化）
- 查询流程（COM_QUERY 命令包）
- 错误包流程（ErrorPacket 序列化/反序列化）
- OK 包流程（OkPacket 序列化/反序列化）
- 序列号流程管理（完整的命令序列）
- 多包结果集流程（列数→列定义→EOF→行→EOF）
- 多个命令序列（PING→OK→QUERY→结果→INIT_DB→OK）
- 序列号溢出处理（255→0）
- 包边界条件（小包、中等包、大包）
- 命令往返序列化/反序列化
- 不同错误码的流程

**运行方式：**
```bash
go test ./server/testing/unit -run TestProtocol
```

### sequence_test.go

**测试内容：** MockSession 的序列号管理

**主要测试：**
- 初始序列号验证
- 单次获取序列号
- 多次调用序列号递增
- 序列号重置
- 设置序列号
- 序列号255溢出后回绕到0
- 多次溢出（超过255）
- 恰好为255的情况
- 并发访问序列号
- 并发重置和递增
- GetSequenceID 不修改内部状态
- 重置后的行为验证
- Clone 保留序列号

**运行方式：**
```bash
go test ./server/testing/unit -run TestMockSession_SequenceID
```

## 运行所有单元测试

```bash
# 运行所有单元测试
go test ./server/testing/unit

# 运行并显示详细输出
go test ./server/testing/unit -v

# 运行并显示覆盖率
go test ./server/testing/unit -cover
```

## 编写单元测试的指南

### 1. 使用 Mock 对象

```go
func TestExample(t *testing.T) {
    conn := mock.NewMockConnection()
    defer conn.Close()

    // 准备测试数据
    packet := &protocol.Packet{}
    packet.SequenceID = 0
    packet.Payload = []byte{0x01}
    packet.PayloadLength = 1

    // 执行测试
    data, err := packet.MarshalBytes()
    assert.NoError(t, err)

    // 验证结果
    assert.Equal(t, byte(0x01), data[4])
}
```

### 2. 测试边界条件

```go
func TestBoundary_Empty(t *testing.T) {
    // 测试空值、最大值、最小值等边界
    emptyPacket := &protocol.Packet{}
    emptyPacket.Payload = []byte{}
    emptyPacket.PayloadLength = 0

    data, err := emptyPacket.MarshalBytes()
    assert.NoError(t, err)
    assert.Equal(t, 4, len(data)) // 只有4字节头
}
```

### 3. 测试序列号管理

```go
func TestSequenceID_Overflow(t *testing.T) {
    sess := mock.NewMockSession()
    sess.SetSequenceID(255)

    // 测试溢出
    nextID := sess.GetNextSequenceID()
    assert.Equal(t, uint8(255), nextID)
    assert.Equal(t, uint8(0), sess.GetSequenceID())
}
```

### 4. 使用子测试

```go
func TestPacket(t *testing.T) {
    tests := []struct {
        name     string
        size     int
        expected int
    }{
        {"small", 1, 5},
        {"medium", 100, 104},
        {"large", 1024, 1028},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            packet := &protocol.Packet{}
            packet.Payload = make([]byte, tt.size)
            packet.PayloadLength = uint32(tt.size)

            data, _ := packet.MarshalBytes()
            assert.Equal(t, tt.expected, len(data))
        })
    }
}
```

## 注意事项

1. **包名** - 单元测试文件使用 `package mock` 包（导入 mock 包中的 Mock 对象）
2. **独立测试** - 每个测试应该独立运行，不依赖其他测试的状态
3. **并发安全** - Mock 对象是线程安全的，但测试代码本身也应该是线程安全的
4. **清理资源** - 使用 `defer` 确保 Mock 对象被正确关闭
5. **错误处理** - 测试错误场景时要验证错误类型和消息
