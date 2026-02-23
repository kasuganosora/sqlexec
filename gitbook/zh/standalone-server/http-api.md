# HTTP REST API

SQLExec 提供 HTTP REST API 接入方式，适用于 Web 应用、微服务及任何支持 HTTP 的客户端。

## 启用配置

在配置文件中启用 HTTP API：

```json
{
  "http_api": {
    "enabled": true,
    "host": "0.0.0.0",
    "port": 8080
  }
}
```

## 认证

HTTP API 使用 **HMAC-SHA256 签名认证**。每个请求必须包含以下请求头：

| 请求头 | 说明 |
|--------|------|
| `X-API-Key` | 你的 API Key |
| `X-Timestamp` | Unix 时间戳（秒） |
| `X-Nonce` | 随机字符串 |
| `X-Signature` | HMAC-SHA256 签名 |

签名计算方式：

```
message = METHOD + PATH + TIMESTAMP + NONCE + BODY
signature = HMAC-SHA256(api_secret, message)
```

时间戳必须在服务器时间的 5 分钟以内。示例：

```bash
# 计算签名
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

## API 端点

### 健康检查

检测服务是否正常运行。

**请求**

```
GET /api/v1/health
```

**响应**

```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

**示例**

```bash
curl http://127.0.0.1:8080/api/v1/health
```

---

### 执行查询

执行 SQL 语句并返回结果。

**请求**

```
POST /api/v1/query
Content-Type: application/json
```

**请求体**

```json
{
  "sql": "SELECT * FROM users WHERE age > 18",
  "database": "my_database",
  "trace_id": "req-20260215-abc123"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `sql` | string | 是 | 要执行的 SQL 语句 |
| `database` | string | 否 | 目标数据源名称，不指定则使用默认数据源 |
| `trace_id` | string | 否 | 请求追踪 ID，用于审计日志关联 |

## 响应格式

### SELECT 查询

查询语句返回列定义和数据行：

```json
{
  "columns": ["id", "name", "age", "email"],
  "rows": [
    [1, "张三", 25, "zhangsan@example.com"],
    [2, "李四", 30, "lisi@example.com"],
    [3, "王五", 28, "wangwu@example.com"]
  ],
  "total": 3,
  "truncated": false
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `columns` | array | 列名列表 |
| `rows` | array | 数据行，每行为一个数组 |
| `total` | number | 返回的行数 |
| `truncated` | boolean | 如果结果因行数限制被截断则为 `true`（最多 10,000 行） |

> **结果大小限制**：读取查询最多返回 **10,000 行**。如果查询返回更多行，响应将包含前 10,000 行，并设置 `"truncated": true`。

### DML 语句（INSERT / UPDATE / DELETE）

数据操作语句返回受影响的行数：

```json
{
  "affected_rows": 5
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `affected_rows` | number | 受影响的行数 |

### 错误响应

当请求出错时返回错误信息：

```json
{
  "error": "query failed",
  "code": 400
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `error` | string | 脱敏后的错误描述 |
| `code` | number | HTTP 状态码 |

> **安全说明**：错误信息经过脱敏处理，不会泄露内部实现细节。SELECT 错误返回 `"query failed"`，DML 错误返回 `"execute failed"`。

## 请求追踪（Trace-ID）

每个请求可以携带 Trace-ID，用于在审计日志中追踪请求链路。支持两种传递方式：

### 通过请求头

```
X-Trace-ID: req-20260215-abc123
```

### 通过请求体

```json
{
  "sql": "SELECT * FROM users",
  "trace_id": "req-20260215-abc123"
}
```

如果同时通过请求头和请求体传递，请求头中的值优先。

## 完整示例

### 健康检查

```bash
curl -s http://127.0.0.1:8080/api/v1/health | jq .
```

输出：

```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

### 查询数据（带认证）

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

输出：

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

### 插入数据

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

输出：

```json
{
  "affected_rows": 1
}
```

### 使用 API Key 认证

```bash
curl -s -X POST http://127.0.0.1:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -H "X-API-Key: my-api-key" \
  -d '{
    "sql": "SHOW TABLES",
    "database": "my_database"
  }' | jq .
```

### 错误处理示例

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

输出：

```json
{
  "error": "query failed",
  "code": 400
}
```
