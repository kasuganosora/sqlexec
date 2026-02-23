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

The HTTP API uses **HMAC-SHA256 signature authentication**. Each request must include the following headers:

| Header | Description |
|--------|-------------|
| `X-API-Key` | Your API key |
| `X-Timestamp` | Unix timestamp (seconds) |
| `X-Nonce` | Random nonce string |
| `X-Signature` | HMAC-SHA256 signature |

The signature is computed as:

```
message = METHOD + PATH + TIMESTAMP + NONCE + BODY
signature = HMAC-SHA256(api_secret, message)
```

The timestamp must be within 5 minutes of server time. Example:

```bash
# Compute signature
TIMESTAMP=$(date +%s)
NONCE="random-nonce-$(uuidgen)"
BODY='{"sql":"SELECT 1"}'
MESSAGE="POST/api/v1/query${TIMESTAMP}${NONCE}${BODY}"
SIGNATURE=$(echo -n "$MESSAGE" | openssl dgst -sha256 -hmac "$API_SECRET" | awk '{print $2}')

curl -X POST http://127.0.0.1:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "X-Timestamp: $TIMESTAMP" \
  -H "X-Nonce: $NONCE" \
  -H "X-Signature: $SIGNATURE" \
  -d "$BODY"
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
    [1, "Alice", 25, "alice@example.com"],
    [2, "Bob", 30, "bob@example.com"],
    [3, "Charlie", 28, "charlie@example.com"]
  ],
  "total": 3,
  "truncated": false
}
```

| Field | Type | Description |
|-------|------|-------------|
| `columns` | array | List of column names |
| `rows` | array | Data rows, each row is an array |
| `total` | number | Number of rows returned |
| `truncated` | boolean | `true` if the result was truncated due to the row limit (max 10,000 rows) |

> **Result size limit**: Read queries are limited to a maximum of **10,000 rows**. If a query returns more rows, the response will contain the first 10,000 rows with `"truncated": true`.

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
  "error": "query failed",
  "code": 400
}
```

| Field | Type | Description |
|-------|------|-------------|
| `error` | string | Sanitized error description |
| `code` | number | HTTP status code |

> **Security note**: Error messages are sanitized to avoid leaking internal implementation details. For SELECT errors the message will be `"query failed"`, and for DML errors it will be `"execute failed"`.

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
  -H "X-API-Key: my-api-key" \
  -H "X-Timestamp: $(date +%s)" \
  -H "X-Nonce: test-nonce" \
  -H "X-Signature: <computed-signature>" \
  -d '{
    "sql": "SELECT * FROM nonexistent_table"
  }' | jq .
```

Output:

```json
{
  "error": "query failed",
  "code": 400
}
```
