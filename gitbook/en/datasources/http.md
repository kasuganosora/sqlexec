# HTTP Data Source

The HTTP data source allows you to map remote HTTP/REST APIs to SQL tables for querying. By configuring the mapping between API endpoints and table names, you can use standard SQL syntax to query data returned by remote interfaces.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `http` |
| `host` | string | Yes | API base URL, e.g., `https://api.example.com` |

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `auth_type` | _(none)_ | Authentication type: `bearer` or `basic` |
| `auth_token` | _(none)_ | Authentication token (Bearer Token) or `username:password` (Basic Auth) |
| `timeout_ms` | `30000` | Request timeout in milliseconds |
| `table_alias` | _(none)_ | Mapping of table names to API endpoint paths (JSON format) |

## Table Alias Mapping

Use the `table_alias` option to map SQL table names to API endpoint paths:

```json
{
  "table_alias": {
    "users": "/api/v1/users",
    "orders": "/api/v1/orders",
    "products": "/api/v1/products"
  }
}
```

After mapping, when querying the `users` table, SQLExec will send a request to `https://api.example.com/api/v1/users`.

## Authentication Methods

### Bearer Token

```json
{
  "options": {
    "auth_type": "bearer",
    "auth_token": "your-api-token-here"
  }
}
```

Request header: `Authorization: Bearer your-api-token-here`

### Basic Auth

```json
{
  "options": {
    "auth_type": "basic",
    "auth_token": "username:password"
  }
}
```

Request header: `Authorization: Basic base64(username:password)`

## Configuration Examples

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

### Query Examples

```sql
-- Switch to the HTTP data source
USE api;

-- Query user list (sends GET /api/v1/users)
SELECT * FROM users;

-- Filtered query
SELECT id, name, email FROM users WHERE status = 'active';

-- Query orders
SELECT order_id, user_id, total_amount
FROM orders
WHERE created_at > '2025-01-01'
ORDER BY total_amount DESC
LIMIT 10;

-- Product statistics
SELECT category, COUNT(*) AS cnt
FROM products
GROUP BY category;
```

## Filter Pushdown

For simple equality filter conditions, SQLExec will attempt to convert them into URL query parameters and push them down to the API:

```sql
-- The following query may be converted to: GET /api/v1/users?status=active&role=admin
SELECT * FROM users WHERE status = 'active' AND role = 'admin';
```

Filter pushdown limitations:

| Supports Pushdown | Does Not Support Pushdown |
|-------------------|--------------------------|
| `column = 'value'` (equality conditions) | `column > value` (range conditions) |
| Multiple equality conditions connected by `AND` | `OR` conditions |
| String and numeric constants | Function calls |
| | `LIKE` pattern matching |

Filter conditions that cannot be pushed down will be applied locally by SQLExec on the returned data.

## API Response Format

The HTTP data source expects the API to return JSON-formatted responses. The following structures are supported:

**Array format** (data array returned directly):

```json
[
  {"id": 1, "name": "Zhang San"},
  {"id": 2, "name": "Li Si"}
]
```

**Wrapper format** (data nested within a field):

```json
{
  "code": 200,
  "data": [
    {"id": 1, "name": "Zhang San"},
    {"id": 2, "name": "Li Si"}
  ]
}
```

## Notes

- The HTTP data source is read-only and does not support INSERT, UPDATE, or DELETE operations.
- Each query sends an HTTP request to the remote API; be aware of API rate limits.
- Authentication tokens should not be hardcoded in configuration files; use environment variables instead.
- Large API responses will be fully loaded into memory for processing.
- Network latency affects query response times; set `timeout_ms` appropriately.
- When complex filter conditions cannot be pushed down, SQLExec processes the full dataset locally, which may impact performance.
