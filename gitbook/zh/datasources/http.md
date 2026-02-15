# HTTP 数据源

HTTP 数据源允许你将远程 HTTP/REST API 映射为 SQL 表进行查询。通过配置 API 端点与表名的映射关系，可以使用标准 SQL 语法查询远程接口返回的数据。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `http` |
| `host` | string | 是 | API 基础 URL，例如 `https://api.example.com` |

## 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `auth_type` | _(无)_ | 认证类型：`bearer` 或 `basic` |
| `auth_token` | _(无)_ | 认证令牌（Bearer Token）或 `username:password`（Basic Auth） |
| `timeout_ms` | `30000` | 请求超时时间（毫秒） |
| `table_alias` | _(无)_ | 表名到 API 端点的映射（JSON 格式） |

## 表别名映射

通过 `table_alias` 选项将 SQL 表名映射到 API 端点路径：

```json
{
  "table_alias": {
    "users": "/api/v1/users",
    "orders": "/api/v1/orders",
    "products": "/api/v1/products"
  }
}
```

映射后，查询 `users` 表时，SQLExec 会向 `https://api.example.com/api/v1/users` 发送请求。

## 认证方式

### Bearer Token

```json
{
  "options": {
    "auth_type": "bearer",
    "auth_token": "your-api-token-here"
  }
}
```

请求头：`Authorization: Bearer your-api-token-here`

### Basic Auth

```json
{
  "options": {
    "auth_type": "basic",
    "auth_token": "username:password"
  }
}
```

请求头：`Authorization: Basic base64(username:password)`

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "api",
      "type": "http",
      "host": "https://api.example.com",
      "options": {
        "auth_type": "bearer",
        "auth_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "timeout_ms": "5000",
        "table_alias": "{\"users\": \"/api/v1/users\", \"orders\": \"/api/v1/orders\", \"products\": \"/api/v1/products\"}"
      }
    }
  ]
}
```

### 查询示例

```sql
-- 切换到 HTTP 数据源
USE api;

-- 查询用户列表（请求 GET /api/v1/users）
SELECT * FROM users;

-- 过滤查询
SELECT id, name, email FROM users WHERE status = 'active';

-- 查询订单
SELECT order_id, user_id, total_amount
FROM orders
WHERE created_at > '2025-01-01'
ORDER BY total_amount DESC
LIMIT 10;

-- 产品统计
SELECT category, COUNT(*) AS cnt
FROM products
GROUP BY category;
```

## 过滤下推

对于简单的等值过滤条件，SQLExec 会尝试将其转换为 URL 查询参数下推到 API 端：

```sql
-- 以下查询可能被转换为：GET /api/v1/users?status=active&role=admin
SELECT * FROM users WHERE status = 'active' AND role = 'admin';
```

过滤下推的限制：

| 支持下推 | 不支持下推 |
|----------|------------|
| `column = 'value'`（等值条件） | `column > value`（范围条件） |
| `AND` 连接的多个等值条件 | `OR` 条件 |
| 字符串和数值常量 | 函数调用 |
| | `LIKE` 模式匹配 |

不支持下推的过滤条件将在 SQLExec 本地对返回数据执行过滤。

## API 响应格式

HTTP 数据源期望 API 返回 JSON 格式的响应。支持以下结构：

**数组格式**（直接返回数据数组）：

```json
[
  {"id": 1, "name": "张三"},
  {"id": 2, "name": "李四"}
]
```

**包装格式**（数据在某个字段中）：

```json
{
  "code": 200,
  "data": [
    {"id": 1, "name": "张三"},
    {"id": 2, "name": "李四"}
  ]
}
```

## 注意事项

- HTTP 数据源为只读模式，不支持 INSERT、UPDATE、DELETE 操作。
- 每次查询都会向远程 API 发送 HTTP 请求，注意 API 的调用频率限制。
- 认证令牌不应硬编码在配置文件中，建议使用环境变量。
- 大数据量的 API 响应会全部加载到内存中进行处理。
- 网络延迟会影响查询响应时间，建议合理设置 `timeout_ms`。
- 复杂过滤条件无法下推时，SQLExec 会在本地处理全量数据，可能影响性能。
