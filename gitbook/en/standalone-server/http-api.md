# HTTP REST API

SQLExec provides an HTTP REST API access method, suitable for web applications, microservices, and any HTTP-capable client.

## Enabling Configuration

Enable the HTTP API in the configuration file:

```json
{
  "http_api": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080
  }
}
```

## Authentication

The HTTP API supports two authentication methods:

### Bearer Token

Pass via the `Authorization` header:

```
Authorization: Bearer <your-token>
```

### API Key

Pass via the `X-API-Key` header:

```
X-API-Key: <your-api-key>
```

## API Endpoints

### Health Check

Check whether the service is running normally.

**Request**

```
GET /api/v1/health
```

**Response**

```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

**Example**

```bash
curl http://127.0.0.1:8080/api/v1/health
```

---

### Execute Query

Execute an SQL statement and return the results.

**Request**

```
POST /api/v1/query
Content-Type: application/json
```

**Request Body**

```json
{
  "sql": "SELECT * FROM users WHERE age > 18",
  "database": "my_database",
  "trace_id": "req-20260215-abc123"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql` | string | Yes | The SQL statement to execute |
| `database` | string | No | Target data source name; uses the default data source if not specified |
| `trace_id` | string | No | Request trace ID for audit log correlation |

## Response Format

### SELECT Queries

Query statements return column definitions and data rows:

```json
{
  "columns": ["id", "name", "age", "email"],
  "rows": [
    [1, "张三", 25, "zhangsan@example.com"],
    [2, "李四", 30, "lisi@example.com"],
    [3, "王五", 28, "wangwu@example.com"]
  ],
  "total": 3
}
```

| Field | Type | Description |
|-------|------|-------------|
| `columns` | array | List of column names |
| `rows` | array | Data rows, each row is an array |
| `total` | number | Number of rows returned |

### DML Statements (INSERT / UPDATE / DELETE)

Data manipulation statements return the number of affected rows:

```json
{
  "affected_rows": 5
}
```

| Field | Type | Description |
|-------|------|-------------|
| `affected_rows` | number | Number of affected rows |

### Error Response

Error information is returned when a request fails:

```json
{
  "error": "table 'users' not found",
  "code": 400
}
```

| Field | Type | Description |
|-------|------|-------------|
| `error` | string | Error description |
| `code` | number | HTTP status code |

## Request Tracing (Trace-ID)

Each request can carry a Trace-ID for tracking the request chain in audit logs. Two methods of passing are supported:

### Via Request Header

```
X-Trace-ID: req-20260215-abc123
```

### Via Request Body

```json
{
  "sql": "SELECT * FROM users",
  "trace_id": "req-20260215-abc123"
}
```

If the Trace-ID is provided in both the request header and the request body, the header value takes precedence.

## Complete Examples

### Health Check

```bash
curl -s http://127.0.0.1:8080/api/v1/health | jq .
```

Output:

```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

### Query Data (with Authentication)

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-secret-token" \
  -H "X-Trace-ID: req-20260215-abc123" \
  -d '{
    "sql": "SELECT id, name, email FROM users WHERE age > 18 LIMIT 10",
    "database": "my_database"
  }' | jq .
```

Output:

```json
{
  "columns": ["id", "name", "email"],
  "rows": [
    [1, "张三", "zhangsan@example.com"],
    [2, "李四", "lisi@example.com"]
  ],
  "total": 2
}
```

### Insert Data

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -H "X-API-Key: my-api-key" \
  -d '{
    "sql": "INSERT INTO users (name, age, email) VALUES ('\''赵六'\'', 26, '\''zhaoliu@example.com'\'')",
    "database": "my_database",
    "trace_id": "req-20260215-def456"
  }' | jq .
```

Output:

```json
{
  "affected_rows": 1
}
```

### Using API Key Authentication

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -H "X-API-Key: my-api-key" \
  -d '{
    "sql": "SHOW TABLES",
    "database": "my_database"
  }' | jq .
```

### Error Handling Example

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer my-secret-token" \
  -d '{
    "sql": "SELECT * FROM nonexistent_table"
  }' | jq .
```

Output:

```json
{
  "error": "table 'nonexistent_table' not found",
  "code": 400
}
```
