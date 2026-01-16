# IO 模块

IO 模块是 MySQL 协议的网络收发和包解析层，提供独立的、解耦的协议处理能力。

## 特性

- ✅ **解耦设计**：不依赖 session/server，便于单元测试
- ✅ **Context 支持**：所有 handler 都接收 context.Context 参数
- ✅ **自动分包/合包**：支持 MySQL 协议的大包拆分和组装
- ✅ **压缩支持**：支持 zlib 压缩和解压
- ✅ **Handler 注册**：支持按命令类型注册处理器
- ✅ **真实协议兼容**：完全兼容真实 MySQL 协议包

## 快速开始

### 基本用法

```go
package main

import (
    "context"
    "net"
    "mysql-proxy/io"
)

func main() {
    // 创建网络连接
    conn, err := net.Dial("tcp", "localhost:3306")
    if err != nil {
        panic(err)
    }
    
    // 创建 IO 实例
    ioHandler := io.NewIO(conn)
    
    // 注册处理器
    ioHandler.RegisterHandler(0x03, func(ctx context.Context, data []byte) error {
        // 处理 COM_QUERY 命令
        println("收到查询命令")
        return nil
    })
    
    // 启动读取循环
    go func() {
        if err := ioHandler.StartReadLoop(); err != nil {
            println("读取循环错误:", err.Error())
        }
    }()
    
    // 发送数据
    packetData := []byte{0x01, 0x00, 0x00, 0x00, 0x03}
    ioHandler.WritePacket(packetData)
}
```

### 完整示例

```go
package main

import (
    "context"
    "fmt"
    "net"
    "mysql-proxy/io"
)

func main() {
    // 创建连接
    conn, err := net.Dial("tcp", "localhost:3306")
    if err != nil {
        panic(err)
    }
    defer conn.Close()
    
    // 创建 IO 实例
    ioHandler := io.NewIO(conn)
    
    // 配置选项
    ioHandler.SetReadTimeout(30 * time.Second)
    ioHandler.SetWriteTimeout(30 * time.Second)
    ioHandler.SetMaxPacketSize(16 * 1024 * 1024) // 16MB
    ioHandler.EnableCompression(false) // 默认不启用压缩
    
    // 注册各种命令处理器
    registerHandlers(ioHandler)
    
    // 启动读取循环
    go func() {
        if err := ioHandler.StartReadLoop(); err != nil {
            fmt.Printf("读取循环错误: %v\n", err)
        }
    }()
    
    // 发送握手包
    handshakePacket := createHandshakePacket()
    ioHandler.WritePacket(handshakePacket)
    
    // 等待处理完成
    select {
    case <-context.Background().Done():
        ioHandler.Stop()
    }
}

func registerHandlers(ioHandler *io.IO) {
    // COM_QUERY 处理器
    ioHandler.RegisterHandler(0x03, func(ctx context.Context, data []byte) error {
        fmt.Printf("收到查询命令，数据长度: %d\n", len(data))
        return nil
    })
    
    // COM_PING 处理器
    ioHandler.RegisterHandler(0x0e, func(ctx context.Context, data []byte) error {
        fmt.Println("收到 PING 命令")
        return nil
    })
    
    // COM_QUIT 处理器
    ioHandler.RegisterHandler(0x01, func(ctx context.Context, data []byte) error {
        fmt.Println("收到 QUIT 命令")
        return nil
    })
}

func createHandshakePacket() []byte {
    // 创建握手包数据
    return []byte{
        0x59, 0x00, 0x00, 0x00, 0x0a, 0x35, 0x2e, 0x35, 0x2e, 0x35,
        // ... 更多握手数据
    }
}
```

## API 参考

### 构造函数

#### `NewIO(conn io.ReadWriter) *IO`

创建新的 IO 实例。

**参数：**
- `conn`: 底层网络连接，必须实现 `io.ReadWriter` 接口

**返回：**
- `*IO`: IO 实例指针

### 配置方法

#### `SetReadTimeout(timeout time.Duration)`

设置读取超时时间。

#### `SetWriteTimeout(timeout time.Duration)`

设置写入超时时间。

#### `SetMaxPacketSize(size uint32)`

设置最大包大小（默认 16MB）。

#### `EnableCompression(enable bool)`

启用或禁用压缩功能。

### Handler 管理

#### `RegisterHandler(cmd uint8, handler PacketHandler)`

注册包处理器。

**参数：**
- `cmd`: MySQL 命令类型（如 0x03 表示 COM_QUERY）
- `handler`: 处理器函数

**示例：**
```go
ioHandler.RegisterHandler(0x03, func(ctx context.Context, data []byte) error {
    // 处理查询命令
    return nil
})
```

#### `UnregisterHandler(cmd uint8)`

注销包处理器。

### 核心方法

#### `StartReadLoop() error`

启动读取循环，自动读取、解包、分发。

**返回：**
- `error`: 错误信息

#### `Stop()`

停止 IO 模块。

#### `WritePacket(packetData []byte) error`

发送包数据。

**参数：**
- `packetData`: 完整的包数据（包含 4 字节包头）

**返回：**
- `error`: 错误信息

#### `WriteLargePacket(packetData []byte) error`

发送大包，自动拆分。

#### `ReadLargePacket() ([]byte, error)`

读取大包，自动组装。

### 压缩相关

#### `CompressPacket(data []byte) ([]byte, error)`

压缩数据。

#### `DecompressPacket(data []byte) ([]byte, error)`

解压数据。

### 分包相关

#### `SplitPacket(packetData []byte) ([][]byte, error)`

将大包拆分成多个小包。

#### `AssemblePacket(packets [][]byte) ([]byte, error)`

组装多个包成一个完整的包。

## 错误类型

```go
var (
    ErrAlreadyRunning = &IOError{"IO module is already running"}
    ErrPacketTooLarge = &IOError{"Packet size exceeds maximum allowed size"}
    ErrInvalidPacket  = &IOError{"Invalid packet format"}
)
```

## 最佳实践

### 1. 错误处理

```go
ioHandler := io.NewIO(conn)

// 注册错误处理器
ioHandler.RegisterHandler(0xff, func(ctx context.Context, data []byte) error {
    fmt.Println("收到错误包")
    return nil
})

// 启动读取循环
go func() {
    if err := ioHandler.StartReadLoop(); err != nil {
        if err == io.EOF {
            fmt.Println("连接已关闭")
        } else {
            fmt.Printf("读取错误: %v\n", err)
        }
    }
}()
```

### 2. Context 使用

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

ioHandler.RegisterHandler(0x03, func(ctx context.Context, data []byte) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
        // 处理数据
        return nil
    }
})
```

### 3. 大包处理

```go
// 发送大包
largeData := make([]byte, 1024*1024) // 1MB
err := ioHandler.WriteLargePacket(largeData)
if err != nil {
    fmt.Printf("发送大包错误: %v\n", err)
}

// 读取大包
largePacket, err := ioHandler.ReadLargePacket()
if err != nil {
    fmt.Printf("读取大包错误: %v\n", err)
}
```

### 4. 压缩使用

```go
// 启用压缩
ioHandler.EnableCompression(true)

// 压缩数据
originalData := []byte("要压缩的数据")
compressed, err := ioHandler.CompressPacket(originalData)
if err != nil {
    fmt.Printf("压缩错误: %v\n", err)
}

// 解压数据
decompressed, err := ioHandler.DecompressPacket(compressed)
if err != nil {
    fmt.Printf("解压错误: %v\n", err)
}
```

## 测试

运行所有测试：

```bash
go test ./io -v
```

运行特定测试：

```bash
go test ./io -run TestIOWithRealPackets -v
```

## 注意事项

1. **线程安全**：IO 模块内部使用读写锁保证线程安全
2. **资源管理**：记得调用 `Stop()` 方法清理资源
3. **错误处理**：handler 返回的错误会被记录但不会中断读取循环
4. **包大小限制**：超过 `maxPacketSize` 的包会被拒绝
5. **压缩兼容**：只有启用压缩时才会尝试解压，避免误判

## 扩展

IO 模块设计为可扩展的，你可以：

1. 添加新的压缩算法（如 zstd）
2. 实现更复杂的包过滤逻辑
3. 添加统计和监控功能
4. 集成到更高层的 session/server 模块中

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个模块！ 