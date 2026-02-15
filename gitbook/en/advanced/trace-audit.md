# Trace-ID and Audit Logging

SQLExec provides a Trace-ID mechanism that spans all entry protocols, combined with audit logging to enable complete request tracing.

## Trace-ID

A Trace-ID is a string identifier used to correlate all operations produced by a single request throughout the system.

### How to Set It

**Method 1: SQL Comment**

```sql
SELECT * FROM users /*trace_id=req_12345*/;
INSERT INTO logs (msg) VALUES ('hello') /*trace_id=req_12345*/;
```

**Method 2: Session Variable**

```sql
SET @trace_id = 'req_12345';
SELECT * FROM users;  -- Automatically associated with trace_id
```

**Method 3: HTTP Header**

```bash
curl -X POST http://localhost:8080/api/v1/query \
  -H "X-Trace-ID: req_12345" \
  -d '{"sql": "SELECT * FROM users"}'
```

**Method 4: Go API**

```go
session.SetTraceID("req_12345")
session.QueryAll("SELECT * FROM users")
```

### Auto-Generation

If no Trace-ID is provided, the system automatically generates one:
- MySQL protocol: `mysql-{timestamp}`
- HTTP API: `http-{timestamp}`
- MCP: `mcp-{timestamp}`

## Audit Logging

### Event Types

| Event Type | Description |
|---------|------|
| `login` | User login |
| `logout` | User logout |
| `query` | SELECT query |
| `insert` | INSERT operation |
| `update` | UPDATE operation |
| `delete` | DELETE operation |
| `ddl` | DDL operation (CREATE/ALTER/DROP) |
| `permission` | Permission check |
| `injection` | SQL injection detection |
| `error` | Error event |
| `api_request` | HTTP API request |
| `mcp_tool_call` | MCP tool call |

### Audit Levels

| Level | Description |
|------|------|
| Info | Routine operations (queries, logins) |
| Warning | Potential risks (insufficient permission attempts) |
| Error | Operation failures |
| Critical | Security events (SQL injection detection) |

### Audit Event Structure

Each audit event contains:

| Field | Type | Description |
|------|------|------|
| `ID` | string | Unique event ID |
| `TraceID` | string | Associated Trace-ID |
| `Timestamp` | time | Event timestamp |
| `Level` | enum | Audit level |
| `EventType` | string | Event type |
| `User` | string | Operating user |
| `Database` | string | Database name |
| `Table` | string | Table name |
| `Query` | string | SQL statement |
| `Message` | string | Event description |
| `Success` | bool | Whether it succeeded |
| `Duration` | int64 | Duration (milliseconds) |
| `Metadata` | map | Additional information |

### Querying Audit Logs

```go
auditLogger := security.NewAuditLogger(10000)

// Query by Trace-ID
events := auditLogger.QueryByTraceID("req_12345")

// Query by user
events := auditLogger.QueryByUser("admin")

// Query by event type
events := auditLogger.QueryByType("injection")

// Query by level
events := auditLogger.QueryByLevel(security.AuditLevelCritical)

// Query by time range
events := auditLogger.QueryByTimeRange(startTime, endTime)

// Export as JSON
jsonData, _ := auditLogger.ExportJSON()
```

## Cross-Protocol Tracing

A single Trace-ID can link operations across protocols:

```
Client -> HTTP API (trace_id=req_123)
       -> SQL Engine (trace_id=req_123)
       -> Memory DataSource (trace_id=req_123)
       -> Audit Log (trace_id=req_123)
```

All related audit events can be retrieved at once via `QueryByTraceID("req_123")`.
