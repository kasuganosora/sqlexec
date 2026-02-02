---
name: server-ddd-refactor
overview: 将 server.go 按 DDD 领域划分 + 注册模式重构，将 1821 行代码拆分为多个模块，提高可维护性和扩展性
todos:
  - id: create-handler-base
    content: 创建 handler 包的基础接口和注册中心
    status: completed
  - id: create-response-builders
    content: 创建 response 包的响应构建器
    status: completed
    dependencies:
      - create-handler-base
  - id: implement-simple-handlers
    content: 实现简单命令处理器（PING, QUIT, SET_OPTION, REFRESH, STATISTICS, DEBUG, SHUTDOWN）
    status: completed
    dependencies:
      - create-handler-base
      - create-response-builders
  - id: implement-query-handlers
    content: 实现查询相关处理器（QUERY, INIT_DB, FIELD_LIST）
    status: completed
    dependencies:
      - create-handler-base
      - create-response-builders
  - id: implement-stmt-handlers
    content: 实现预处理语句处理器（PREPARE, EXECUTE, CLOSE, SEND_LONG_DATA, RESET）
    status: completed
    dependencies:
      - create-handler-base
      - create-response-builders
  - id: implement-process-handlers
    content: 实现进程控制处理器（PROCESS_INFO, PROCESS_KILL）
    status: completed
    dependencies:
      - create-handler-base
      - create-response-builders
  - id: refactor-server-go
    content: 重构 server.go 使用注册中心
    status: completed
    dependencies:
      - implement-simple-handlers
      - implement-query-handlers
      - implement-stmt-handlers
      - implement-process-handlers
  - id: run-tests
    content: 运行所有测试验证重构结果
    status: completed
    dependencies:
      - refactor-server-go
---

# Server 层 DDD + 注册模式重构方案（增强单元测试版）

## 一、问题分析

### 当前问题

- **单文件过大**：server.go 1821 行代码，维护困难
- **职责混乱**：协议处理、业务逻辑、数据转换混杂
- **扩展性差**：新增命令需要修改多处代码
- **测试困难**：代码耦合度高，单元测试复杂

### 测试问题分析

**现有测试特点：**

```
// 现有测试需要启动完整服务器
testServer := mysqltest.NewTestServer()
err := testServer.Start(13306)
defer testServer.Stop()
```

**测试痛点：**

1. **依赖性强**：需要启动完整服务器才能测试单个命令
2. **Mock 困难**：Handler 耦合在 Server 中，无法单独测试
3. **响应构建器无法单独测试**：sendOK、sendError、sendResultSet 都是 Server 方法
4. **测试慢**：端到端测试耗时，不适合频繁运行
5. **覆盖率低**：无法有效测试边界情况和错误处理

### 命令类型统计

当前支持 17 种 MySQL 命令：

- 连接管理：COM_QUIT, COM_PING
- 查询执行：COM_QUERY, COM_INIT_DB
- 预处理语句：COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_CLOSE, COM_STMT_SEND_LONG_DATA, COM_STMT_RESET
- 系统管理：COM_SET_OPTION, COM_REFRESH, COM_STATISTICS, COM_DEBUG, COM_SHUTDOWN
- 元数据：COM_FIELD_LIST
- 进程控制：COM_PROCESS_INFO, COM_PROCESS_KILL

## 二、架构设计（增强可测试性）

### 2.1 DDD 领域划分

```
server/
├── handler/              # 命令处理器领域
│   ├── handler.go       # Handler 接口定义（支持依赖注入）
│   ├── registry.go      # 命令注册中心
│   ├── context.go       # 处理器上下文（支持 Mock）
│   ├── mock/           # 测试 Mock 工具
│   │   ├── handler_context_mock.go
│   │   └── response_writer_mock.go
│   ├── query/          # 查询相关处理器
│   │   ├── query_handler.go
│   │   ├── query_handler_test.go    # 单元测试
│   │   ├── init_db_handler.go
│   │   ├── init_db_handler_test.go
│   │   ├── field_list_handler.go
│   │   └── field_list_handler_test.go
│   ├── stmt/           # 预处理语句处理器
│   │   ├── prepare_handler.go
│   │   ├── prepare_handler_test.go
│   │   ├── execute_handler.go
│   │   ├── execute_handler_test.go
│   │   ├── close_handler.go
│   │   ├── send_long_data_handler.go
│   │   └── reset_handler.go
│   ├── process/        # 进程控制处理器
│   │   ├── process_info_handler.go
│   │   ├── process_kill_handler.go
│   │   └── process_kill_handler_test.go
│   └── simple/         # 简单处理器
│       ├── ping_handler.go
│       ├── ping_handler_test.go
│       ├── quit_handler.go
│       ├── set_option_handler.go
│       ├── refresh_handler.go
│       ├── statistics_handler.go
│       ├── debug_handler.go
│       └── shutdown_handler.go
├── response/           # 响应构建领域
│   ├── builder.go      # 响应构建器接口
│   ├── ok_builder.go   # OK 包构建器
│   ├── ok_builder_test.go    # 单元测试
│   ├── error_builder.go # 错误包构建器
│   ├── error_builder_test.go
│   ├── result_set_builder.go # 结果集构建器
│   ├── result_set_builder_test.go
│   └── mock/          # 测试 Mock 工具
│       └── response_writer_mock.go
├── handshake/          # 握手领域
│   ├── handshake.go    # 握手处理逻辑
│   └── handshake_test.go    # 单元测试
├── converter/          # 数据转换领域
│   ├── type_converter.go   # 类型转换
│   ├── type_converter_test.go
│   ├── field_converter.go  # 字段转换
│   ├── field_converter_test.go
│   ├── row_converter.go     # 行数据转换
│   └── row_converter_test.go
├── acl/                # 访问控制（保持不变）
├── protocol/           # 协议定义（保持不变）
├── session/            # 会话管理（保持不变）
├── tests/              # 集成测试（保持不变）
└── server.go           # 精简后的服务器入口
```

### 2.2 核心设计模式（增强可测试性）

#### 注册模式（支持依赖注入）

```
// Handler 接口
type Handler interface {
    Handle(ctx *HandlerContext) error
    Command() uint8
    Name() string
}

// HandlerContext 处理器上下文（支持 Mock）
type HandlerContext struct {
    Server     *Server
    Session    *session.Session
    Connection ResponseWriter  // 接口化，支持 Mock
    SequenceID *uint8
    Logger     Logger         // 接口化，支持 Mock
}

// ResponseWriter 响应写入器接口
type ResponseWriter interface {
    Write([]byte) (int, error)
}

// HandlerRegistry 命令处理器注册中心
type HandlerRegistry struct {
    handlers map[uint8]Handler
}

func (r *HandlerRegistry) Register(handler Handler) error
func (r *HandlerRegistry) Get(commandType uint8) (Handler, bool)
func (r *HandlerRegistry) Handle(ctx *HandlerContext, commandType uint8, packet interface{}) error
```

#### 测试辅助工具

```
// handler/mock/handler_context_mock.go
package mock

import (
    "bytes"
    "testing"
    
    "github.com/kasuganosora/sqlexec/server/handler"
    "github.com/stretchr/testify/assert"
)

type MockHandlerContext struct {
    *handler.HandlerContext
    WrittenBytes [][]byte
    Errors       []error
    t            *testing.T
}

func NewMockHandlerContext(t *testing.T) *MockHandlerContext {
    return &MockHandlerContext{
        WrittenBytes: make([][]byte, 0),
        Errors:      make([]error, 0),
        t:           t,
    }
}

func (m *MockHandlerContext) Write(data []byte) (int, error) {
    m.WrittenBytes = append(m.WrittenBytes, data)
    return len(data), nil
}

func (m *MockHandlerContext) AssertWrittenPacket(expected []byte) {
    assert.True(m.t, len(m.WrittenBytes) > 0, "Expected at least one packet written")
    assert.Equal(m.t, expected, m.WrittenBytes[0], "Written packet mismatch")
}

func (m *MockHandlerContext) AssertNoErrors() {
    assert.Empty(m.t, m.Errors, "Expected no errors, got: %v", m.Errors)
}
```

### 2.3 响应构建器设计（可独立测试）

```
// response/builder.go
package response

import (
    "github.com/kasuganosora/sqlexec/server/protocol"
)

// ResponseBuilder 响应构建器接口
type ResponseBuilder interface {
    BuildOK(sequenceID uint8) Response
    BuildError(sequenceID uint8, err error) Response
    BuildResultSet(sequenceID uint8, columns []protocol.FieldMeta, rows [][]string) []Response
}

// Response 响应接口
type Response interface {
    Serialize() ([]byte, error)
}

// OKBuilder OK 包构建器
type OKBuilder struct {
    protocol.Packet
    protocol.OkInPacket
}

func NewOKBuilder() *OKBuilder {
    return &OKBuilder{}
}

func (b *OKBuilder) Build(sequenceID uint8, affectedRows, lastInsertID uint64, warnings uint16) Response {
    b.SequenceID = sequenceID
    b.Header = 0x00
    b.AffectedRows = affectedRows
    b.LastInsertId = lastInsertID
    b.Warnings = warnings
    b.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
    return b
}

func (b *OKBuilder) Serialize() ([]byte, error) {
    return b.Marshal()
}
```

### 2.4 Handler 设计（支持依赖注入和测试）

```
// handler/simple/ping_handler.go
package simple

import (
    "github.com/kasuganosora/sqlexec/server/handler"
    "github.com/kasuganosora/sqlexec/server/response"
)

type PingHandler struct {
    okBuilder response.OKBuilder
}

func NewPingHandler(okBuilder response.OKBuilder) *PingHandler {
    return &PingHandler{okBuilder: okBuilder}
}

func (h *PingHandler) Handle(ctx *handler.HandlerContext) error {
    okResp := h.okBuilder.Build(ctx.SequenceID, 0, 0, 0)
    data, err := okResp.Serialize()
    if err != nil {
        return err
    }
    _, err = ctx.Connection.Write(data)
    return err
}

func (h *PingHandler) Command() uint8 {
    return protocol.COM_PING
}

func (h *PingHandler) Name() string {
    return "COM_PING"
}
```

### 2.5 单元测试示例

```
// handler/simple/ping_handler_test.go
package simple

import (
    "testing"
    
    "github.com/kasuganosora/sqlexec/server/handler/mock"
    "github.com/kasuganosora/sqlexec/server/protocol"
    "github.com/stretchr/testify/assert"
)

func TestPingHandler_Handle(t *testing.T) {
    tests := []struct {
        name      string
        sequenceID uint8
        expectOK  bool
    }{
        {
            name:      "成功处理 PING",
            sequenceID: 0,
            expectOK:  true,
        },
        {
            name:      "处理 PING（sequenceID=255）",
            sequenceID: 255,
            expectOK:  true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // 创建 Mock 上下文
            mockCtx := mock.NewMockHandlerContext(t)
            mockCtx.SequenceID = tt.sequenceID

            // 创建处理器（使用 Mock OKBuilder）
            mockOKBuilder := &mock.MockOKBuilder{}
            handler := NewPingHandler(mockOKBuilder)

            // 处理命令
            err := handler.Handle(mockCtx)

            // 验证结果
            if tt.expectOK {
                assert.NoError(t, err)
                mockCtx.AssertNoErrors()
                mockCtx.AssertWrittenPacket([]byte{0x00, 0x00, 0x00, 0x01}) // OK 包
            }
        })
    }
}

func TestPingHandler_Command(t *testing.T) {
    handler := NewPingHandler(nil)
    assert.Equal(t, protocol.COM_PING, handler.Command())
}

func TestPingHandler_Name(t *testing.T) {
    handler := NewPingHandler(nil)
    assert.Equal(t, "COM_PING", handler.Name())
}
```

```
// response/ok_builder_test.go
package response

import (
    "testing"
    
    "github.com/kasuganosora/sqlexec/server/protocol"
    "github.com/stretchr/testify/assert"
)

func TestOKBuilder_Build(t *testing.T) {
    tests := []struct {
        name         string
        sequenceID   uint8
        affectedRows uint64
        lastInsertID uint64
        warnings     uint16
        expectLen    int
    }{
        {
            name:         "标准 OK 包",
            sequenceID:   1,
            affectedRows: 10,
            lastInsertID: 5,
            warnings:     0,
            expectLen:    7, // Header(1) + AffectedRows(1-9) + LastInsertID(1-9) + Warnings(2) + StatusFlags(2)
        },
        {
            name:         "空结果 OK 包",
            sequenceID:   0,
            affectedRows: 0,
            lastInsertID: 0,
            warnings:     0,
            expectLen:    7,
        },
        {
            name:         "大数量 OK 包",
            sequenceID:   255,
            affectedRows: 1000000,
            lastInsertID: 999999,
            warnings:     5,
            expectLen:    13,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            builder := NewOKBuilder()
            resp := builder.Build(tt.sequenceID, tt.affectedRows, tt.lastInsertID, tt.warnings)
            
            data, err := resp.Serialize()
            assert.NoError(t, err)
            assert.Equal(t, tt.expectLen, len(data), "序列化长度不匹配")
            
            // 验证 Header
            assert.Equal(t, uint8(0x00), data[0], "OK 包 Header 错误")
            
            // 验证 SequenceID（OKBuilder 中管理）
            assert.Equal(t, tt.sequenceID, resp.SequenceID)
        })
    }
}

func TestOKBuilder_StatusFlags(t *testing.T) {
    builder := NewOKBuilder()
    resp := builder.Build(0, 0, 0, 0)
    
    assert.Equal(t, protocol.SERVER_STATUS_AUTOCOMMIT, resp.StatusFlags)
}
```

## 三、详细设计

### 3.1 Handler 接口（server/handler/handler.go）

```
package handler

import (
    "context"
    "net"
    "log"

    "github.com/kasuganosora/sqlexec/server/protocol"
    "github.com/kasuganosora/sqlexec/server/session"
)

// Logger 日志接口（支持 Mock）
type Logger interface {
    Printf(format string, v ...interface{})
}

// ResponseWriter 响应写入器接口（支持 Mock）
type ResponseWriter interface {
    Write([]byte) (int, error)
}

// Handler 命令处理器接口
type Handler interface {
    // Handle 处理命令
    Handle(ctx *HandlerContext, packet interface{}) error

    // Command 返回命令类型
    Command() uint8

    // Name 返回处理器名称
    Name() string
}

// HandlerContext 处理器上下文
type HandlerContext struct {
    Server     *Server
    Session    *session.Session
    Connection ResponseWriter
    Logger     Logger
}

// NewHandlerContext 创建处理器上下文
func NewHandlerContext(server *Server, sess *session.Session, conn net.Conn, logger Logger) *HandlerContext {
    return &HandlerContext{
        Server:     server,
        Session:    sess,
        Connection: conn,
        Logger:     logger,
    }
}

// SendOK 发送 OK 包
func (ctx *HandlerContext) SendOK() error {
    okPacket := &protocol.OkPacket{}
    okPacket.SequenceID = ctx.Session.GetNextSequenceID()
    okPacket.OkInPacket.Header = 0x00
    okPacket.OkInPacket.AffectedRows = 0
    okPacket.OkInPacket.LastInsertId = 0
    okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
    okPacket.OkInPacket.Warnings = 0

    packetBytes, err := okPacket.Marshal()
    if err != nil {
        return err
    }

    _, err = ctx.Connection.Write(packetBytes)
    return err
}

// SendError 发送错误包
func (ctx *HandlerContext) SendError(err error) error {
    errPacket := &protocol.ErrorPacket{}
    errPacket.SequenceID = ctx.Session.GetNextSequenceID()
    errPacket.ErrorInPacket.Header = 0xff
    errPacket.ErrorInPacket.ErrorCode, errPacket.ErrorInPacket.SqlState = ctx.Server.mapErrorCode(err)
    if errPacket.ErrorInPacket.SqlState != "" {
        errPacket.ErrorInPacket.SqlStateMarker = "#"
    }
    errPacket.ErrorInPacket.ErrorMessage = err.Error()

    packetBytes, marshalErr := errPacket.Marshal()
    if marshalErr != nil {
        return marshalErr
    }

    _, writeErr := ctx.Connection.Write(packetBytes)
    return writeErr
}
```

### 3.2 注册中心（server/handler/registry.go）

```
package handler

import (
    "fmt"
    "log"
    "sync"
)

// HandlerRegistry 命令处理器注册中心（并发安全）
type HandlerRegistry struct {
    handlers map[uint8]Handler
    mu       sync.RWMutex
    logger    Logger
}

// NewHandlerRegistry 创建注册中心
func NewHandlerRegistry(logger Logger) *HandlerRegistry {
    return &HandlerRegistry{
        handlers: make(map[uint8]Handler),
        logger:    logger,
    }
}

// Register 注册处理器（并发安全）
func (r *HandlerRegistry) Register(handler Handler) error {
    cmd := handler.Command()
    r.mu.Lock()
    defer r.mu.Unlock()
    
    if _, exists := r.handlers[cmd]; exists {
        return fmt.Errorf("handler for command 0x%02x already registered", cmd)
    }
    r.handlers[cmd] = handler
    r.logger.Printf("Registered handler: %s (0x%02x)", handler.Name(), cmd)
    return nil
}

// Get 获取处理器（并发安全）
func (r *HandlerRegistry) Get(commandType uint8) (Handler, bool) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    
    handler, exists := r.handlers[commandType]
    return handler, exists
}

// Handle 处理命令
func (r *HandlerRegistry) Handle(ctx *HandlerContext, commandType uint8, packet interface{}) error {
    handler, exists := r.Get(commandType)
    if !exists {
        return fmt.Errorf("no handler registered for command 0x%02x", commandType)
    }
    return handler.Handle(ctx, packet)
}

// List 列出所有注册的处理器
func (r *HandlerRegistry) List() []Handler {
    r.mu.RLock()
    defer r.mu.RUnlock()
    
    handlers := make([]Handler, 0, len(r.handlers))
    for _, h := range r.handlers {
        handlers = append(handlers, h)
    }
    return handlers
}
```

### 3.3 响应构建器接口（server/response/builder.go）

```
package response

import (
    "github.com/kasuganosora/sqlexec/server/protocol"
)

// ResponseBuilder 响应构建器接口
type ResponseBuilder interface {
    // BuildOK 构建OK包
    BuildOK(sequenceID uint8, affectedRows, lastInsertID uint64, warnings uint16) Response

    // BuildError 构建错误包
    BuildError(sequenceID uint8, err error) Response

    // BuildResultSet 构建结果集
    BuildResultSet(sequenceID uint8, columns []protocol.FieldMeta, rows [][]string) []Response

    // BuildColumnCount 构建列数包
    BuildColumnCount(sequenceID uint8, count uint64) Response
}

// Response 响应接口
type Response interface {
    // Serialize 序列化响应为字节
    Serialize() ([]byte, error)
}
```

### 3.4 重构后的 server.go 核心结构

```
package server

import (
    "context"
    "log"
    "net"

    "github.com/kasuganosora/sqlexec/server/acl"
    "github.com/kasuganosora/sqlexec/server/handler"
    simpleHandlers "github.com/kasuganosora/sqlexec/server/handler/simple"
    queryHandlers "github.com/kasuganosora/sqlexec/server/handler/query"
    stmtHandlers "github.com/kasuganosora/sqlexec/server/handler/stmt"
    processHandlers "github.com/kasuganosora/sqlexec/server/handler/process"
    "github.com/kasuganosora/sqlexec/server/protocol"
    "github.com/kasuganosora/sqlexec/server/session"
    pkg_session "github.com/kasuganosora/sqlexec/pkg/session"
)

type Server struct {
    ctx          context.Context
    listener     net.Listener
    sessionMgr   *session.SessionMgr
    config       *config.Config
    db           *api.DB
    aclManager   *acl.ACLManager
    handlerRegistry *handler.HandlerRegistry
    logger       log.Logger
}

func NewServer(ctx context.Context, listener net.Listener, cfg *config.Config) *Server {
    s := &Server{
        ctx:          ctx,
        listener:     listener,
        sessionMgr:   session.NewSessionMgr(ctx, session.NewMemoryDriver()),
        config:       cfg,
        logger:       log.New(os.Stdout, "[SERVER] ", log.LstdFlags),
    }

    // 初始化 DB 和 ACL（保持原有逻辑）
    // ...

    // 创建命令注册中心
    s.handlerRegistry = handler.NewHandlerRegistry(s.logger)

    // 注册所有处理器
    s.registerHandlers()

    return s
}

func (s *Server) registerHandlers() {
    // 注册简单处理器
    s.handlerRegistry.Register(simpleHandlers.NewPingHandler())
    s.handlerRegistry.Register(simpleHandlers.NewQuitHandler())
    s.handlerRegistry.Register(simpleHandlers.NewSetOptionHandler())
    s.handlerRegistry.Register(simpleHandlers.NewRefreshHandler())
    s.handlerRegistry.Register(simpleHandlers.NewStatisticsHandler())
    s.handlerRegistry.Register(simpleHandlers.NewDebugHandler())
    s.handlerRegistry.Register(simpleHandlers.NewShutdownHandler())

    // 注册查询处理器
    s.handlerRegistry.Register(queryHandlers.NewQueryHandler(s.db))
    s.handlerRegistry.Register(queryHandlers.NewInitDBHandler(s.db))
    s.handlerRegistry.Register(queryHandlers.NewFieldListHandler())

    // 注册预处理语句处理器
    s.handlerRegistry.Register(stmtHandlers.NewPrepareHandler())
    s.handlerRegistry.Register(stmtHandlers.NewExecuteHandler())
    s.handlerRegistry.Register(stmtHandlers.NewCloseHandler())
    s.handlerRegistry.Register(stmtHandlers.NewSendLongDataHandler())
    s.handlerRegistry.Register(stmtHandlers.NewResetHandler())

    // 注册进程控制处理器
    s.handlerRegistry.Register(processHandlers.NewProcessInfoHandler())
    s.handlerRegistry.Register(processHandlers.NewProcessKillHandler())
}

func (s *Server) Handle(ctx context.Context, conn net.Conn) error {
    // 握手逻辑（提取到 handshake 包）
    // ...

    for {
        // 读取数据包
        packetContent, err := s.readMySQLPacket(conn)
        if err != nil {
            return err
        }

        commandType := packetContent[4]

        // 解析数据包
        commandPack, err := s.parseCommandPacket(commandType, packetContent)
        if err != nil {
            s.sendError(conn, err, sess.GetNextSequenceID())
            return err
        }

        // 使用注册中心处理命令
        handlerCtx := handler.NewHandlerContext(s, sess, conn, s.logger)
        err = s.handlerRegistry.Handle(handlerCtx, commandType, commandPack)
        if err != nil {
            if commandType == protocol.COM_QUIT {
                return nil
            }
            if strings.Contains(err.Error(), "解析") || strings.Contains(err.Error(), "包") {
                return err
            }
            s.logger.Printf("处理命令失败: %v", err)
        }
    }
}

// 其他辅助方法...
```

## 四、测试策略

### 4.1 测试层次

```
1. 单元测试（Unit Tests）
   ├── Handler 单元测试
   │   ├── PingHandler 测试
   │   ├── QueryHandler 测试
   │   ├── ProcessKillHandler 测试
   │   └── ...
   ├── ResponseBuilder 单元测试
   │   ├── OKBuilder 测试
   │   ├── ErrorBuilder 测试
   │   └── ResultSetBuilder 测试
   ├── Converter 单元测试
   │   ├── TypeConverter 测试
   │   ├── FieldConverter 测试
   │   └── RowConverter 测试
   └── Handshake 单元测试
       └── Handshake 测试

2. 集成测试（Integration Tests）
   ├── HandlerRegistry 测试
   ├── Handler 集成测试（使用真实依赖）
   └── 命令流程测试

3. 端到端测试（E2E Tests）
   ├── 现有 tests/ 包的所有测试（保持不变）
   └── 完整命令流程测试
```

### 4.2 单元测试覆盖率目标

| 模块 | 目标覆盖率 | 说明 |
| --- | --- | --- |
| Handler | 90%+ | 核心业务逻辑 |
| ResponseBuilder | 95%+ | 序列化逻辑 |
| Converter | 95%+ | 类型转换 |
| Handshake | 85%+ | 握手流程 |
| Registry | 80%+ | 注册表逻辑 |


### 4.3 测试工具包

```
// server/testing/mocks.go
package testing

import (
    "bytes"
    "github.com/kasuganosora/sqlexec/server/handler"
)

// MockLogger Mock 日志器
type MockLogger struct {
    Logs []string
}

func (m *MockLogger) Printf(format string, v ...interface{}) {
    m.Logs = append(m.Logs, fmt.Sprintf(format, v...))
}

// MockResponseWriter Mock 响应写入器
type MockResponseWriter struct {
    Buffer bytes.Buffer
    WriteError error
}

func (m *MockResponseWriter) Write(data []byte) (int, error) {
    if m.WriteError != nil {
        return 0, m.WriteError
    }
    return m.Buffer.Write(data)
}

// NewTestHandlerContext 创建测试上下文
func NewTestHandlerContext() *handler.HandlerContext {
    return &handler.HandlerContext{
        Logger:     &MockLogger{},
        Connection: &MockResponseWriter{},
    }
}
```

## 五、重构优势

### 5.1 可维护性提升

- 单个文件行数从 1821 行降至 ~300 行
- 每个处理器独立文件，职责清晰
- 新增命令无需修改核心代码

### 5.2 可扩展性提升

- 注册模式：新增命令只需实现 Handler 接口
- 响应构建器：统一响应格式，易于修改
- 职责分离：协议、业务、转换逻辑独立

### 5.3 可测试性提升（核心改进）

- ✅ **每个 Handler 可独立单元测试**
- ✅ **响应构建器可独立测试**
- ✅ **Mock 简单：接口化依赖**
- ✅ **测试快速：无需启动完整服务器**
- ✅ **覆盖率高：可测试所有边界情况**

### 5.4 代码复用

- 响应构建器可复用
- 数据转换逻辑集中
- 错误处理统一
- 测试辅助工具可复用

## 六、实施步骤

### 第一阶段：基础设施（支持测试）

1. ✅ 创建 handler 包的基础接口（支持依赖注入）
2. ✅ 创建 handler 包的注册中心（并发安全）
3. ✅ 创建 response 包的响应构建器接口
4. ✅ 创建测试辅助工具（Mock）

### 第二阶段：处理器迁移（带单元测试）

1. ✅ 实现简单处理器 + 单元测试（PING, QUIT, SET_OPTION, REFRESH, STATISTICS, DEBUG, SHUTDOWN）
2. ✅ 实现查询处理器 + 单元测试（QUERY, INIT_DB, FIELD_LIST）
3. ✅ 实现预处理语句处理器 + 单元测试（PREPARE, EXECUTE, CLOSE, SEND_LONG_DATA, RESET）
4. ✅ 实现进程控制处理器 + 单元测试（PROCESS_INFO, PROCESS_KILL）

### 第三阶段：核心重构

1. ✅ 重构 server.go 使用注册中心
2. ✅ 提取握手逻辑到 handshake 包
3. ✅ 提取数据转换逻辑到 converter 包

### 第四阶段：测试验证

1. ✅ 运行所有单元测试（新增）
2. ✅ 运行所有集成测试（保持不变）
3. ✅ 验证测试覆盖率

## 七、向后兼容性

- 保持所有现有 API 接口不变
- 保持所有协议行为一致
- 测试用例无需修改
- 配置文件无需修改

## 八、风险评估与缓解

### 低风险

- 文件结构重构
- 代码提取和拆分

### 中风险

- 命令处理流程变更
- 错误处理逻辑调整

### 缓解措施

- 保留原有测试
- 分阶段实施
- 每个阶段测试通过后继续
- 保留原有 server.go 作为备份
- 单元测试覆盖关键路径

## 九、测试用例示例

### 9.1 ProcessKillHandler 单元测试

```
// handler/process/process_kill_handler_test.go
package process

import (
    "errors"
    "testing"
    
    "github.com/kasuganosora/sqlexec/pkg/session"
    "github.com/kasuganosora/sqlexec/server/handler/mock"
    "github.com/kasuganosora/sqlexec/server/protocol"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
)

func TestProcessKillHandler_Handle(t *testing.T) {
    tests := []struct {
        name          string
        threadID      uint32
        queryExists   bool
        expectOK      bool
        expectError   string
    }{
        {
            name:        "Kill存在的查询",
            threadID:    123,
            queryExists: true,
            expectOK:    true,
        },
        {
            name:        "Kill不存在的查询",
            threadID:    999,
            queryExists: false,
            expectOK:    false,
            expectError: "Unknown thread id",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // 设置测试查询
            if tt.queryExists {
                setupTestQuery(t, tt.threadID)
                defer cleanupTestQuery(t, tt.threadID)
            }

            // 创建 Mock 上下文
            mockCtx := mock.NewMockHandlerContext(t)

            // 创建处理器
            handler := NewProcessKillHandler()

            // 创建数据包
            packet := &protocol.ComProcessKillPacket{
                ProcessID: tt.threadID,
            }

            // 处理命令
            err := handler.Handle(mockCtx, packet)

            // 验证结果
            if tt.expectOK {
                assert.NoError(t, err)
                mockCtx.AssertNoErrors()
                mockCtx.AssertWrittenPacket([]byte{0x00, 0x00, 0x00, 0x01}) // OK 包
            } else {
                assert.Error(t, err)
                assert.Contains(t, err.Error(), tt.expectError)
            }
        })
    }
}
```

### 9.2 OKBuilder 单元测试（边界情况）

```
func TestOKBuilder_Build_BoundaryCases(t *testing.T) {
    tests := []struct {
        name         string
        sequenceID   uint8
        affectedRows uint64
        lastInsertID uint64
        warnings     uint16
    }{
        {
            name:         "最小值（全0）",
            sequenceID:   0,
            affectedRows: 0,
            lastInsertID: 0,
            warnings:     0,
        },
        {
            name:         "最大 sequenceID",
            sequenceID:   255,
            affectedRows: 0,
            lastInsertID: 0,
            warnings:     0,
        },
        {
            name:         "最大 affectedRows（uint64）",
            sequenceID:   0,
            affectedRows:  18446744073709551615,
            lastInsertID: 0,
            warnings:     0,
        },
        {
            name:         "最大 warnings",
            sequenceID:   0,
            affectedRows:  0,
            lastInsertID: 0,
            warnings:     65535,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            builder := NewOKBuilder()
            resp := builder.Build(tt.sequenceID, tt.affectedRows, tt.lastInsertID, tt.warnings)
            
            data, err := resp.Serialize()
            assert.NoError(t, err)
            assert.NotEmpty(t, data)
        })
    }
}
```

## 十、总结

### 关键改进点

1. **接口化依赖**：所有依赖都通过接口注入，支持 Mock
2. **分层测试**：单元测试、集成测试、端到端测试三层
3. **测试辅助工具**：提供 Mock 上下文和响应写入器
4. **独立测试**：每个 Handler 和 Builder 可独立测试
5. **高覆盖率**：单元测试覆盖核心逻辑，目标 90%+

### 预期效果

- ✅ 测试速度提升：从端到端测试（秒级）→ 单元测试（毫秒级）
- ✅ 测试覆盖率提升：从 ~60% → 90%+
- ✅ 开发效率提升：新增命令只需编写 Handler + 单元测试
- ✅ 维护成本降低：单个文件职责清晰，易于定位问题
- ✅ 代码质量提升：接口约束 + 测试驱动开发