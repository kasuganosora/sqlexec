# Trace-ID 与审计日志

SQLExec 提供 Trace-ID 机制贯穿所有入口协议，配合审计日志实现完整的请求追踪。

## Trace-ID

Trace-ID 是一个字符串标识符，用于关联同一请求在系统中产生的所有操作。

### 设置方式

**方式一：SQL 注释**

```sql
SELECT * FROM users /*trace_id=req_12345*/;
INSERT INTO logs (msg) VALUES ('hello') /*trace_id=req_12345*/;
```

**方式二：会话变量**

```sql
SET @trace_id = 'req_12345';
SELECT * FROM users;  -- 自动关联 trace_id
```

**方式三：HTTP Header**

```bash
curl -X POST http://localhost:8080/api/v1/query \
  -H "X-Trace-ID: req_12345" \
  -d '{"sql": "SELECT * FROM users"}'
```

**方式四：Go API**

```go
session.SetTraceID("req_12345")
session.QueryAll("SELECT * FROM users")
```

### 自动生成

如果未提供 Trace-ID，系统会自动生成：
- MySQL 协议：`mysql-{timestamp}`
- HTTP API：`http-{timestamp}`
- MCP：`mcp-{timestamp}`

## 审计日志

### 事件类型

| 事件类型 | 说明 |
|---------|------|
| `login` | 用户登录 |
| `logout` | 用户登出 |
| `query` | SELECT 查询 |
| `insert` | INSERT 操作 |
| `update` | UPDATE 操作 |
| `delete` | DELETE 操作 |
| `ddl` | DDL 操作（CREATE/ALTER/DROP） |
| `permission` | 权限检查 |
| `injection` | SQL 注入检测 |
| `error` | 错误事件 |
| `api_request` | HTTP API 请求 |
| `mcp_tool_call` | MCP 工具调用 |

### 审计级别

| 级别 | 说明 |
|------|------|
| Info | 常规操作（查询、登录） |
| Warning | 潜在风险（权限不足尝试） |
| Error | 操作失败 |
| Critical | 安全事件（SQL 注入检测） |

### 审计事件结构

每个审计事件包含：

| 字段 | 类型 | 说明 |
|------|------|------|
| `ID` | string | 事件唯一 ID |
| `TraceID` | string | 关联的 Trace-ID |
| `Timestamp` | time | 事件时间 |
| `Level` | enum | 审计级别 |
| `EventType` | string | 事件类型 |
| `User` | string | 操作用户 |
| `Database` | string | 数据库名 |
| `Table` | string | 表名 |
| `Query` | string | SQL 语句 |
| `Message` | string | 事件描述 |
| `Success` | bool | 是否成功 |
| `Duration` | int64 | 耗时（毫秒） |
| `Metadata` | map | 附加信息 |

### 查询审计日志

```go
auditLogger := security.NewAuditLogger(10000)

// 按 Trace-ID 查询
events := auditLogger.QueryByTraceID("req_12345")

// 按用户查询
events := auditLogger.QueryByUser("admin")

// 按事件类型查询
events := auditLogger.QueryByType("injection")

// 按级别查询
events := auditLogger.QueryByLevel(security.AuditLevelCritical)

// 按时间范围查询
events := auditLogger.QueryByTimeRange(startTime, endTime)

// 导出为 JSON
jsonData, _ := auditLogger.ExportJSON()
```

## 跨协议追踪

同一个 Trace-ID 可以串联跨协议的操作：

```
客户端 → HTTP API (trace_id=req_123)
       → SQL 引擎 (trace_id=req_123)
       → Memory DataSource (trace_id=req_123)
       → 审计日志 (trace_id=req_123)
```

所有相关的审计事件都可以通过 `QueryByTraceID("req_123")` 一次查出。
