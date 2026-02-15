# PostgreSQL 数据源

PostgreSQL 数据源允许 SQLExec 连接外部 PostgreSQL 数据库（12 及以上版本），SQL 查询将直接下推到 PostgreSQL 执行。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `postgresql` |
| `host` | string | 是 | PostgreSQL 服务器地址 |
| `port` | int | 否 | 端口号，默认 `5432` |
| `username` | string | 是 | 数据库用户名 |
| `password` | string | 是 | 数据库密码 |
| `database` | string | 是 | 远程 PostgreSQL 数据库名称 |

## 连接选项

通过 `options` 字段可以配置高级连接参数：

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `schema` | `public` | 默认 schema |
| `ssl_mode` | `disable` | SSL 模式：`disable`、`allow`、`prefer`、`require`、`verify-ca`、`verify-full` |
| `connect_timeout` | `10s` | 连接超时时间 |
| `max_open_conns` | `25` | 最大打开连接数 |
| `max_idle_conns` | `5` | 最大空闲连接数 |

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "pgdb",
      "type": "postgresql",
      "host": "192.168.1.200",
      "port": 5432,
      "username": "app_user",
      "password": "your_password",
      "database": "analytics",
      "options": {
        "schema": "public",
        "ssl_mode": "require",
        "connect_timeout": "15s",
        "max_open_conns": "30",
        "max_idle_conns": "10"
      }
    }
  ]
}
```

### 嵌入模式

```go
package main

import (
    "fmt"
    "github.com/mySQLExec/db"
)

func main() {
    engine := db.NewEngine()

    engine.RegisterDataSource("pgdb", &db.DataSourceConfig{
        Type:     "postgresql",
        Host:     "192.168.1.200",
        Port:     5432,
        Username: "app_user",
        Password: "your_password",
        Database: "analytics",
        Options: map[string]string{
            "schema":   "public",
            "ssl_mode": "require",
        },
    })

    // 切换到 PostgreSQL 数据源
    engine.Execute("USE pgdb")

    // 查询将直接在 PostgreSQL 上执行
    result, err := engine.Query("SELECT * FROM events WHERE created_at > NOW() - INTERVAL '7 days'")
    if err != nil {
        panic(err)
    }
    fmt.Println(result)
}
```

## 查询下推

所有 SQL 查询都将直接下推到 PostgreSQL 执行：

```sql
-- 切换到 PostgreSQL 数据源
USE pgdb;

-- 使用 PostgreSQL 原生语法和函数
SELECT
    date_trunc('day', created_at) AS day,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM events
WHERE created_at >= '2025-01-01'
GROUP BY date_trunc('day', created_at)
ORDER BY day DESC;
```

可以使用 PostgreSQL 特有的功能，例如：

```sql
-- 使用 CTE
WITH active_users AS (
    SELECT user_id, COUNT(*) AS login_count
    FROM login_logs
    WHERE login_time > NOW() - INTERVAL '30 days'
    GROUP BY user_id
    HAVING COUNT(*) >= 5
)
SELECT u.name, a.login_count
FROM users u
JOIN active_users a ON u.id = a.user_id;

-- 使用 JSON 操作
SELECT id, profile->>'name' AS name
FROM users
WHERE profile @> '{"role": "admin"}';
```

## 注意事项

- 需要确保 PostgreSQL 服务器可达且凭证正确。
- `schema` 选项指定默认搜索路径，默认使用 `public`。
- 生产环境建议使用 `ssl_mode: require` 或更高安全级别。
- 密码不应硬编码在配置文件中，建议使用环境变量。
- 连接池参数应根据实际负载进行调优。
