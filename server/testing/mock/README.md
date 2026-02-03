# Mock 工具

Mock 工具提供用于测试的模拟对象，支持单元测试和边界条件测试。

## 可用的 Mock 对象

### MockConnection

实现 `net.Conn` 接口，用于模拟网络连接。

**主要功能：**
- 记录所有写入的数据（`Write`, `GetWrittenData`, `GetWrittenDataBytes`）
- 支持模拟读取数据（`AddReadData`, `Read`）
- 错误注入（`SetReadError`, `SetWriteError`）
- 连接状态管理（`Close`, `IsClosed`）
- 地址和超时支持（`LocalAddr`, `RemoteAddr`, `SetDeadline`）

**使用示例：**
```go
conn := mock.NewMockConnection()
defer conn.Close()

// 模拟服务器的响应数据
conn.AddReadData(handshakeData)

// 发送数据并验证
client.Write(requestData)
writtenData := conn.GetWrittenData()
assert.Equal(t, requestData, writtenData)
```

### MockSession

实现 `pkg/session.Session` 接口，用于模拟会话。

**主要功能：**
- 序列号管理（`GetNextSequenceID`, `ResetSequenceID`, `SetSequenceID`）
- 会话数据存储（`Get`, `Set`, `Delete`）
- 用户和线程ID管理（`GetUser`, `SetUser`, `GetThreadID`, `SetThreadID`）
- 会话生命周期（`Close`, `IsClosed`, `Clone`）

**使用示例：**
```go
sess := mock.NewMockSession()
defer sess.Close()

// 获取序列号
seqID := sess.GetNextSequenceID()
assert.Equal(t, uint8(0), seqID)

// 存储会话数据
sess.Set("user_id", "12345")
userID := sess.Get("user_id")
```

### MockLogger

实现 `handler.Logger` 接口，用于模拟日志记录。

**主要功能：**
- 日志记录（`Printf`）
- 日志查询（`GetLogs`, `ContainsLog`, `GetLastLog`, `GetLogCount`）
- 日志管理（`ClearLogs`）
- 日志开关（`Enable`, `Disable`）

**使用示例：**
```go
logger := mock.NewMockLogger()
defer logger.ClearLogs()

// 记录日志
logger.Printf("Test started: %s", testName)

// 验证日志
assert.True(t, logger.ContainsLog("Test started"))
assert.Equal(t, 1, logger.GetLogCount())
```

## 设计原则

1. **线程安全** - 所有 Mock 对象都使用 `sync.Mutex` 保护
2. **独立性** - 每个 Mock 对象都是独立的，可克隆
3. **可测试性** - 支持错误注入和状态查询
4. **简单易用** - 提供清晰的 API 和构造函数

## 使用场景

Mock 工具主要用于以下场景：
- **单元测试** - 测试单个组件，隔离外部依赖
- **边界条件测试** - 测试错误处理和异常情况
- **协议测试** - 测试数据包的序列化/反序列化
- **序列号测试** - 验证序列号的生成和溢出处理

## 注意事项

1. 使用完毕后记得调用 `Close()` 方法清理资源
2. Mock 对象不应该在测试之间共享
3. 错误注入后要重置错误状态
4. 在并发测试中要注意线程安全
