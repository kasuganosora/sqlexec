# HTTP 数据源开发指南

HTTP 数据源允许外部应用通过实现约定的 HTTP API，将数据暴露为 sqlexec 中的虚拟表。这些虚拟表可以与其他数据源（如内存、MySQL 等）进行 JOIN 查询。

HTTP 数据源的核心特点：
- **查询下推**：过滤条件、排序、分页直接转发给外部 HTTP 服务，不在 sqlexec 内存中处理
- **灵活配置**：路径、认证、自定义头、签名模板均可配置
- **ACL 控制**：可限制哪些用户能访问、拥有什么权限
- **表名映射**：SQL 中的表名可以映射到 HTTP 端的不同表名

## 快速开始

### 1. 创建 HTTP 数据源

通过 SQL 在 `config` 数据库中创建：

```sql
USE config;

INSERT INTO datasource (name, type, host, writable, options) VALUES (
  'erp',
  'http',
  'https://erp.example.com',
  false,
  '{}'
);
```

### 2. 实现 HTTP API

外部应用需要实现以下 API 端点。

### 3. 使用

```sql
SELECT u.id, u.name, a.balance
FROM erp.employees u
JOIN default.accounts a ON u.id = a.user_id
WHERE u.status = 'active';
```

---

## HTTP API 合约

### 端点列表

| 操作 | 默认路径 | 方法 | 说明 |
|------|---------|------|------|
| 表列表 | `/_schema/tables` | GET | 返回所有表名 |
| 表结构 | `/_schema/tables/{table}` | GET | 返回列信息 |
| 查询 | `/_query/{table}` | POST | 支持 filters/排序/分页 |
| 插入 | `/_insert/{table}` | POST | 可选（writable 时） |
| 更新 | `/_update/{table}` | POST | 可选（writable 时） |
| 删除 | `/_delete/{table}` | POST | 可选（writable 时） |
| 健康 | `/_health` | GET | 连通性检测 |

`{table}` 为路径参数占位符，运行时替换为实际表名。

所有端点的路径均可通过配置自定义（见[路径配置](#路径配置)）。

---

### GET /_health

健康检查接口，用于 Connect 时验证连通性。

**响应**：
```json
{
  "status": "ok"
}
```

---

### GET /_schema/tables

返回所有可用表名。

**响应**：
```json
{
  "tables": ["users", "orders", "products"]
}
```

---

### GET /_schema/tables/{table}

返回指定表的列信息。

**响应**：
```json
{
  "name": "users",
  "columns": [
    {"name": "id", "type": "bigint", "nullable": false, "primary": true},
    {"name": "name", "type": "varchar(100)", "nullable": false},
    {"name": "email", "type": "varchar(255)", "nullable": true},
    {"name": "created_at", "type": "datetime", "nullable": false}
  ]
}
```

**列类型**：使用标准 SQL 类型字符串，如 `int`, `bigint`, `varchar(100)`, `text`, `decimal(10,2)`, `datetime`, `boolean` 等。

---

### POST /_query/{table}

查询数据，支持过滤、排序、分页和列裁剪。

**请求**：
```json
{
  "filters": [
    {"field": "age", "operator": ">", "value": 18},
    {"field": "status", "operator": "=", "value": "active"},
    {
      "logic": "OR",
      "sub_filters": [
        {"field": "role", "operator": "=", "value": "admin"},
        {"field": "role", "operator": "=", "value": "super"}
      ]
    }
  ],
  "order_by": "created_at",
  "order": "DESC",
  "limit": 20,
  "offset": 0,
  "select_columns": ["id", "name", "email"]
}
```

**请求参数说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `filters` | []Filter | 过滤条件列表（AND 关系） |
| `order_by` | string | 排序字段名 |
| `order` | string | 排序方向：`ASC` 或 `DESC` |
| `limit` | int | 返回行数限制（0 = 不限） |
| `offset` | int | 跳过的行数 |
| `select_columns` | []string | 需要返回的列名（空 = 全部列） |

**Filter 结构**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `field` | string | 列名（简单过滤时使用） |
| `operator` | string | 操作符 |
| `value` | any | 过滤值 |
| `logic` | string | 逻辑运算符：`AND` 或 `OR`（嵌套时使用） |
| `sub_filters` | []Filter | 子过滤条件（嵌套时使用） |

**支持的操作符**：

| 操作符 | 说明 | 值类型示例 |
|--------|------|-----------|
| `=` | 等于 | `"active"` |
| `!=` | 不等于 | `"deleted"` |
| `>` | 大于 | `18` |
| `<` | 小于 | `100` |
| `>=` | 大于等于 | `0` |
| `<=` | 小于等于 | `999` |
| `LIKE` | 模式匹配 | `"%alice%"` |
| `NOT LIKE` | 模式不匹配 | `"%test%"` |
| `IN` | 在列表中 | `[1, 2, 3]` |
| `NOT IN` | 不在列表中 | `["deleted", "banned"]` |
| `BETWEEN` | 范围 | `[10, 20]` |
| `NOT BETWEEN` | 范围外 | `[0, 5]` |

**响应**：
```json
{
  "columns": [
    {"name": "id", "type": "bigint", "nullable": false, "primary": true},
    {"name": "name", "type": "varchar(100)", "nullable": false}
  ],
  "rows": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ],
  "total": 150
}
```

`total` = 满足过滤条件的总行数（不受 limit 影响）。如果不支持总数计算，返回 `-1`。

---

### POST /_insert/{table}

插入数据（需要 `writable: true`）。

**请求**：
```json
{
  "rows": [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"}
  ],
  "options": {
    "replace": false
  }
}
```

**响应**：
```json
{
  "affected": 2
}
```

---

### POST /_update/{table}

更新数据（需要 `writable: true`）。

**请求**：
```json
{
  "filters": [
    {"field": "id", "operator": "=", "value": 1}
  ],
  "updates": {
    "email": "alice_new@example.com",
    "updated_at": "2024-01-15T10:30:00Z"
  },
  "options": {}
}
```

**响应**：
```json
{
  "affected": 1
}
```

---

### POST /_delete/{table}

删除数据（需要 `writable: true`）。

**请求**：
```json
{
  "filters": [
    {"field": "id", "operator": "=", "value": 1}
  ],
  "options": {}
}
```

**响应**：
```json
{
  "affected": 1
}
```

---

### 错误响应

所有端点在出错时返回 4xx/5xx 状态码和以下格式：

```json
{
  "error": {
    "code": "TABLE_NOT_FOUND",
    "message": "Table 'xxx' does not exist"
  }
}
```

常见错误码：

| 状态码 | 错误码 | 说明 |
|--------|--------|------|
| 404 | `TABLE_NOT_FOUND` | 表不存在 |
| 400 | `INVALID_FILTER` | 过滤条件无效 |
| 400 | `INVALID_REQUEST` | 请求格式错误 |
| 403 | `FORBIDDEN` | 权限不足 |
| 500 | `INTERNAL_ERROR` | 服务器内部错误 |

---

## 配置详解

### 完整配置示例

```json
{
  "type": "http",
  "name": "erp",
  "host": "https://erp.example.com",
  "username": "admin",
  "password": "secret",
  "writable": false,
  "options": {
    "base_path": "/api/v1",

    "paths": {
      "tables": "/_schema/tables",
      "schema": "/_schema/tables/{table}",
      "query": "/_query/{table}",
      "insert": "/_insert/{table}",
      "update": "/_update/{table}",
      "delete": "/_delete/{table}",
      "health": "/_health"
    },

    "auth_type": "bearer",
    "auth_token": "eyJhbGci...",

    "timeout_ms": 5000,
    "retry_count": 3,
    "retry_delay_ms": 1000,

    "tls_skip_verify": false,

    "headers": {
      "X-Tenant-ID": "acme",
      "X-Timestamp": "{{timestamp}}",
      "X-Nonce": "{{nonce}}",
      "X-Signature": "{{hmac_sha256(auth_token, timestamp+nonce+body)}}"
    },

    "database": "erp_db",
    "table_alias": {
      "employees": "users",
      "order_history": "orders"
    },

    "acl": {
      "allowed_users": ["admin", "analyst", "viewer"],
      "permissions": {
        "admin": ["SELECT", "INSERT", "UPDATE", "DELETE"],
        "analyst": ["SELECT"],
        "viewer": ["SELECT"]
      }
    }
  }
}
```

### 路径配置

每个操作的 URL 路径都可以自定义。`{table}` 是运行时替换的占位符。

```json
"paths": {
  "tables": "/my/custom/tables",
  "schema": "/my/custom/schema/{table}",
  "query":  "/data/{table}/search",
  "insert": "/data/{table}/create",
  "update": "/data/{table}/modify",
  "delete": "/data/{table}/remove",
  "health": "/ping"
}
```

最终 URL = `host` + `base_path` + `paths.xxx`

例如：`https://erp.example.com` + `/api/v1` + `/data/users/search`

### 认证配置

#### Bearer Token

```json
{
  "auth_type": "bearer",
  "auth_token": "eyJhbGciOiJIUzI1NiIs..."
}
```

#### Basic Auth

使用顶层 `username` / `password` 字段：

```json
{
  "username": "admin",
  "password": "secret",
  "options": {
    "auth_type": "basic"
  }
}
```

#### API Key

```json
{
  "auth_type": "api_key",
  "api_key_header": "X-API-Key",
  "api_key_value": "your-api-key-here"
}
```

`api_key_header` 默认为 `X-API-Key`。

### 自定义头与签名模板

自定义头支持模板语法，每次请求时动态计算值。

```json
"headers": {
  "X-Static": "fixed-value",
  "X-Timestamp": "{{timestamp}}",
  "X-Signature": "{{hmac_sha256(auth_token, timestamp+nonce+body)}}"
}
```

#### 模板变量

| 变量 | 说明 |
|------|------|
| `{{timestamp}}` | 当前 Unix 时间戳（秒） |
| `{{timestamp_ms}}` | 当前 Unix 时间戳（毫秒） |
| `{{uuid}}` | 随机 UUID v4 |
| `{{nonce}}` | 随机 16 位 hex 字符串 |
| `{{date}}` | 当前日期 `2006-01-02` |
| `{{datetime}}` | 当前时间 `2006-01-02T15:04:05Z` |
| `{{method}}` | 请求方法（GET/POST） |
| `{{path}}` | 请求路径 |
| `{{body}}` | 请求 body（GET 时为空） |
| `{{auth_token}}` | 配置中的 auth_token 值 |

#### 模板函数

| 函数 | 说明 | 示例 |
|------|------|------|
| `hmac_sha256(key, data)` | HMAC-SHA256，hex 输出 | `{{hmac_sha256(auth_token, timestamp+body)}}` |
| `hmac_md5(key, data)` | HMAC-MD5，hex 输出 | `{{hmac_md5(auth_token, path+body)}}` |
| `md5(data)` | MD5 哈希，hex 输出 | `{{md5(body)}}` |
| `sha256(data)` | SHA256 哈希，hex 输出 | `{{sha256(timestamp+body)}}` |
| `base64(data)` | Base64 编码 | `{{base64(auth_token)}}` |
| `upper(data)` | 转大写 | `{{upper(method)}}` |
| `lower(data)` | 转小写 | `{{lower(nonce)}}` |

#### 字符串拼接

参数中使用 `+` 进行字符串拼接：

```
{{hmac_sha256(auth_token, timestamp+nonce+body)}}
```

也可以使用字面字符串（引号包裹）：

```
{{md5(body+"salt123")}}
```

#### 签名示例

```json
{
  "headers": {
    "X-Timestamp": "{{timestamp}}",
    "X-Nonce": "{{nonce}}",
    "X-Signature": "{{hmac_sha256(auth_token, timestamp+nonce+body)}}"
  }
}
```

每次请求时，模板引擎先解析所有变量值，再执行函数调用，生成最终 header 值。

### 超时与重试

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `timeout_ms` | int | `30000` | HTTP 请求超时（毫秒） |
| `retry_count` | int | `0` | 失败重试次数（仅 5xx 和连接错误） |
| `retry_delay_ms` | int | `1000` | 重试间隔（毫秒） |

### TLS 配置

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `tls_skip_verify` | bool | `false` | 跳过 TLS 证书验证 |
| `tls_ca_cert` | string | `""` | 自定义 CA 证书文件路径 |

### 数据库/表名映射

#### database

`database` 选项指定此 HTTP 数据源在 SQL 中的数据库名。默认使用 datasource 的 `name`。

```json
{
  "name": "erp",
  "options": {
    "database": "erp_db"
  }
}
```

使用：`SELECT * FROM erp_db.users`

#### table_alias

`table_alias` 建立 SQL 表名到 HTTP 表名的映射。不在映射中的表名直接透传。

```json
"table_alias": {
  "employees": "users",
  "order_history": "orders"
}
```

`SELECT * FROM erp_db.employees` 实际请求 HTTP API 的 `users` 表。

### ACL 权限控制

#### allowed_users

限制哪些用户可以访问此 HTTP 数据源。空列表 = 不限制。

```json
"acl": {
  "allowed_users": ["admin", "analyst"]
}
```

#### permissions

为每个用户定义具体的操作权限。

```json
"acl": {
  "allowed_users": ["admin", "analyst", "viewer"],
  "permissions": {
    "admin": ["SELECT", "INSERT", "UPDATE", "DELETE"],
    "analyst": ["SELECT"],
    "viewer": ["SELECT"]
  }
}
```

如果用户在 `allowed_users` 中但没有 `permissions` 条目，则默认允许所有操作。

---

## SQL 创建示例

```sql
USE config;

-- 只读 HTTP 数据源
INSERT INTO datasource (name, type, host, writable, options) VALUES (
  'erp',
  'http',
  'https://erp.example.com',
  false,
  '{"base_path":"/api/v1","auth_type":"bearer","auth_token":"xxx","database":"erp_db"}'
);

-- 带签名的 HTTP 数据源
INSERT INTO datasource (name, type, host, writable, options) VALUES (
  'payment',
  'http',
  'https://pay.example.com',
  false,
  '{"auth_type":"bearer","auth_token":"sign_key","headers":{"X-Timestamp":"{{timestamp}}","X-Sign":"{{hmac_sha256(auth_token,timestamp+body)}}"}}'
);

-- 可写的 HTTP 数据源（带 ACL）
INSERT INTO datasource (name, type, host, writable, options) VALUES (
  'crm',
  'http',
  'https://crm.example.com',
  true,
  '{"database":"crm_db","acl":{"allowed_users":["admin","sales"],"permissions":{"admin":["SELECT","INSERT","UPDATE","DELETE"],"sales":["SELECT","INSERT"]}}}'
);
```

使用：

```sql
-- 跨数据源 JOIN
SELECT u.name, o.amount
FROM erp_db.employees u
JOIN default.orders o ON u.id = o.user_id
WHERE u.department = 'Engineering';
```

---

## 外部应用实现示例

### Go (net/http)

```go
package main

import (
    "encoding/json"
    "net/http"
)

type Column struct {
    Name     string `json:"name"`
    Type     string `json:"type"`
    Nullable bool   `json:"nullable"`
    Primary  bool   `json:"primary"`
}

func main() {
    http.HandleFunc("GET /_health", func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
    })

    http.HandleFunc("GET /_schema/tables", func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(map[string][]string{
            "tables": {"users", "products"},
        })
    })

    http.HandleFunc("GET /_schema/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
        table := r.PathValue("table")
        // 根据 table 返回列信息
        json.NewEncoder(w).Encode(map[string]interface{}{
            "name": table,
            "columns": []Column{
                {Name: "id", Type: "bigint", Primary: true},
                {Name: "name", Type: "varchar(100)"},
            },
        })
    })

    http.HandleFunc("POST /_query/{table}", func(w http.ResponseWriter, r *http.Request) {
        // 解析请求中的 filters/limit/offset
        var req struct {
            Filters []map[string]interface{} `json:"filters"`
            Limit   int                      `json:"limit"`
            Offset  int                      `json:"offset"`
        }
        json.NewDecoder(r.Body).Decode(&req)

        // 查询数据并返回
        json.NewEncoder(w).Encode(map[string]interface{}{
            "columns": []Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar(100)"}},
            "rows":    []map[string]interface{}{{"id": 1, "name": "Alice"}},
            "total":   1,
        })
    })

    http.ListenAndServe(":8080", nil)
}
```

### Python (Flask)

```python
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/_health')
def health():
    return jsonify(status='ok')

@app.route('/_schema/tables')
def tables():
    return jsonify(tables=['users', 'products'])

@app.route('/_schema/tables/<table>')
def schema(table):
    schemas = {
        'users': {
            'name': 'users',
            'columns': [
                {'name': 'id', 'type': 'bigint', 'nullable': False, 'primary': True},
                {'name': 'name', 'type': 'varchar(100)', 'nullable': False},
                {'name': 'email', 'type': 'varchar(255)', 'nullable': True},
            ]
        }
    }
    if table not in schemas:
        return jsonify(error={'code': 'TABLE_NOT_FOUND', 'message': f"Table '{table}' not found"}), 404
    return jsonify(schemas[table])

@app.route('/_query/<table>', methods=['POST'])
def query(table):
    req = request.get_json()
    filters = req.get('filters', [])
    limit = req.get('limit', 0)
    offset = req.get('offset', 0)

    # 从数据库查询数据，应用 filters/limit/offset
    rows = [{'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}]

    return jsonify(
        columns=[
            {'name': 'id', 'type': 'bigint'},
            {'name': 'name', 'type': 'varchar(100)'},
            {'name': 'email', 'type': 'varchar(255)'},
        ],
        rows=rows,
        total=len(rows),
    )

if __name__ == '__main__':
    app.run(port=8080)
```

### Node.js (Express)

```javascript
const express = require('express');
const app = express();
app.use(express.json());

app.get('/_health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/_schema/tables', (req, res) => {
  res.json({ tables: ['users', 'products'] });
});

app.get('/_schema/tables/:table', (req, res) => {
  res.json({
    name: req.params.table,
    columns: [
      { name: 'id', type: 'bigint', nullable: false, primary: true },
      { name: 'name', type: 'varchar(100)', nullable: false },
    ],
  });
});

app.post('/_query/:table', (req, res) => {
  const { filters, limit, offset } = req.body;
  // 查询数据
  res.json({
    columns: [{ name: 'id', type: 'bigint' }, { name: 'name', type: 'varchar(100)' }],
    rows: [{ id: 1, name: 'Alice' }],
    total: 1,
  });
});

app.listen(8080, () => console.log('HTTP datasource server on :8080'));
```

---

## 注意事项

1. **DDL 不支持**：HTTP 数据源不支持 `CREATE TABLE`、`DROP TABLE`、`TRUNCATE TABLE` 和 `EXECUTE` 操作
2. **事务不支持**：HTTP 数据源不支持事务（BEGIN/COMMIT/ROLLBACK）
3. **写操作可选**：只有 `writable: true` 时才支持 INSERT/UPDATE/DELETE
4. **total 字段**：查询响应中的 `total` 是满足过滤条件的总行数（不受 limit 影响），如果不支持返回 `-1`
5. **重试策略**：只有 5xx 错误和连接错误会触发重试，4xx 错误不会重试
6. **Filter 下推**：所有过滤条件直接转发给 HTTP 端点，由外部应用处理
