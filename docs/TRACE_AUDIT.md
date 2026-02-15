# 追踪与审计机制

SQLExec 实现了贯穿 MySQL 协议、HTTP API、MCP 三个入口的 Trace-ID 请求追踪机制，配合审计日志系统实现全链路可观测。

## Trace-ID 数据流

```
┌─────────────────────────────────────────────────────────┐
│  客户端请求                                              │
│                                                         │
│  MySQL:  /*trace_id=abc*/ SELECT ...                    │
│          SET @trace_id = 'abc'                          │
│          (自动生成: {ThreadID}-{timestamp})               │
│                                                         │
│  HTTP:   POST /api/v1/query                             │
│          Body: {"sql":"...", "trace_id":"abc"}           │
│          Header: X-Trace-ID: abc                        │
│          (自动生成: http-{timestamp})                     │
│                                                         │
│  MCP:    query tool: {"sql":"...", "trace_id":"abc"}    │
│          (自动生成: mcp-{timestamp})                     │
└───────────────┬─────────────────────────────────────────┘
                │
                ▼
┌───────────────────────────────────┐
│  Session.TraceID                  │
│  ↓                                │
│  CoreSession.traceID              │
│  ↓                                │
│  QueryContext.TraceID (每次查询)    │
│  ↓                                │
│  AuditEvent.TraceID (写入审计日志)  │
└───────────────────────────────────┘
```

## Trace-ID 来源与优先级

### MySQL 协议连接

三个来源，优先级从高到低：

1. **SQL 注释提取**（每条 SQL 级别）：`/*trace_id=abc123*/ SELECT * FROM users`
   - 正则匹配 `/*\s*trace_id\s*=\s*([^\s*]+)\s**/`
   - 提取后从 SQL 中移除注释，传入查询引擎
   - 仅覆盖当前查询的 `QueryContext.TraceID`

2. **Session 变量**（连接级别）：`SET @trace_id = 'abc123'`
   - 拦截 `SetVariable("trace_id", ...)` 调用
   - 同步到 API Session 和 CoreSession
   - 后续所有查询继承此 TraceID

3. **自动生成**（连接建立时）：`{ThreadID}-{timestamp}`
   - 连接创建时在 `CreateSession()` 中生成
   - 作为默认值，客户端可随时覆盖

### HTTP API

优先级：请求体 `trace_id` 字段 > `X-Trace-ID` 请求头 > 自动生成 `http-{timestamp}`

### MCP

优先级：工具参数 `trace_id` > 自动生成 `mcp-{timestamp}`

## 传播链路

```
Session (协议层)
  │ sess.TraceID
  ▼
API Session (pkg/api)
  │ apiSess.SetTraceID() → coreSession.SetTraceID()
  ▼
CoreSession (pkg/session)
  │ s.traceID
  │ createQueryContext() 读取 traceID
  ▼
QueryContext (每次查询)
  │ qc.TraceID
  │ SQL 注释中的 trace_id 可覆盖
  ▼
AuditEvent (pkg/security)
  │ event.TraceID
  └→ 写入审计日志环形缓冲区
```

跨层传播需要显式调用，因为协议 Session、API Session、CoreSession 是不同结构体（避免循环依赖）：

```go
// server/server.go - 连接建立时
apiSess.SetTraceID(sess.TraceID)

// pkg/session/session.go - SET @trace_id 时
if name == "trace_id" {
    s.TraceID = v
    if setter, ok := apiSess.(interface{ SetTraceID(string) }); ok {
        setter.SetTraceID(v)
    }
}
```

## 审计日志

### AuditEvent 结构

```go
type AuditEvent struct {
    ID        int64     `json:"id"`
    Timestamp time.Time `json:"timestamp"`
    TraceID   string    `json:"trace_id,omitempty"`
    Type      string    `json:"type"`       // QUERY, INSERT, UPDATE, DELETE, LOGIN, ...
    User      string    `json:"user"`
    Database  string    `json:"database"`
    Query     string    `json:"query,omitempty"`
    Duration  int64     `json:"duration_ms"`
    Success   bool      `json:"success"`
    Error     string    `json:"error,omitempty"`
}
```

### AuditLogger

基于环形缓冲区 + 异步 channel 的高性能审计日志：

```go
auditLogger := security.NewAuditLogger(10000) // 缓冲区容量
```

支持的审计事件类型：

| 方法 | 事件类型 | 入口 |
|------|----------|------|
| `LogQuery` | QUERY | MySQL handler |
| `LogInsert/Update/Delete` | INSERT/UPDATE/DELETE | CoreSession |
| `LogDDL` | DDL | CoreSession |
| `LogLogin/LogLogout` | LOGIN/LOGOUT | 握手处理器 |
| `LogAPIRequest` | API_REQUEST | HTTP handler |
| `LogMCPToolCall` | MCP_TOOL_CALL | MCP tools |
| `LogError` | ERROR | 各层 |

### 按 Trace-ID 查询

```go
events := auditLogger.GetEventsByTraceID("abc123")
```

可检索同一请求链路中产生的所有审计事件。

## Handler 层集成

MySQL 协议的查询处理器在执行前后记录审计日志：

```go
// server/handler/query/query_handler.go
queryStart := time.Now()
// ... 执行查询 ...
if ctx.AuditLogger != nil {
    traceID := ctx.Session.GetTraceID()
    ctx.AuditLogger.LogQuery(traceID, user, db, query, duration, success)
}
```

`AuditLogger` 通过 `HandlerContext` 注入，接口定义在 handler 包内以避免循环依赖：

```go
type AuditLogger interface {
    LogQuery(traceID, user, database, query string, duration int64, success bool)
    LogError(traceID, user, database, message string, err error)
}
```
