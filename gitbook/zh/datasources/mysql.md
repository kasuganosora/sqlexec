# MySQL 数据源

MySQL 数据源允许 SQLExec 连接外部 MySQL 数据库（5.7 及以上版本），SQL 查询将直接下推到 MySQL 执行。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `mysql` |
| `host` | string | 是 | MySQL 服务器地址 |
| `port` | int | 否 | 端口号，默认 `3306` |
| `username` | string | 是 | 数据库用户名 |
| `password` | string | 是 | 数据库密码 |
| `database` | string | 是 | 远程 MySQL 数据库名称 |

## 连接选项

通过 `options` 字段可以配置高级连接参数：

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `max_open_conns` | `25` | 最大打开连接数 |
| `max_idle_conns` | `5` | 最大空闲连接数 |
| `conn_max_lifetime` | `300s` | 连接最大存活时间 |
| `conn_max_idle_time` | `60s` | 空闲连接最大存活时间 |
| `charset` | `utf8mb4` | 字符集 |
| `collation` | _(默认)_ | 排序规则，如 `utf8mb4_unicode_ci` |
| `ssl_mode` | _(禁用)_ | SSL 模式：`disabled`、`preferred`、`required` |
| `connect_timeout` | `10s` | 连接超时时间 |

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "mydb",
      "type": "mysql",
      "host": "192.168.1.100",
      "port": 3306,
      "username": "app_user",
      "password": "your_password",
      "database": "myapp",
      "options": {
        "max_open_conns": 50,
        "max_idle_conns": 10,
        "conn_max_lifetime": "600s",
        "charset": "utf8mb4",
        "collation": "utf8mb4_unicode_ci",
        "ssl_mode": "preferred",
        "connect_timeout": "15s"
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

    engine.RegisterDataSource("mydb", &db.DataSourceConfig{
        Type:     "mysql",
        Host:     "192.168.1.100",
        Port:     3306,
        Username: "app_user",
        Password: "your_password",
        Database: "myapp",
        Options: map[string]string{
            "max_open_conns":    "50",
            "max_idle_conns":    "10",
            "conn_max_lifetime": "600s",
            "charset":           "utf8mb4",
        },
    })

    // 切换到 MySQL 数据源
    engine.Execute("USE mydb")

    // 查询将直接在 MySQL 上执行
    result, err := engine.Query("SELECT id, name, email FROM users LIMIT 10")
    if err != nil {
        panic(err)
    }
    fmt.Println(result)
}
```

## 查询下推

所有 SQL 查询都将直接下推到 MySQL 执行，SQLExec 不会在本地解析或处理查询逻辑。这意味着：

- 可以使用 MySQL 原生的全部 SQL 语法和函数。
- 查询性能取决于 MySQL 服务器本身。
- 索引、执行计划等均由 MySQL 管理。

```sql
-- 切换到 MySQL 数据源
USE mydb;

-- 以下查询直接由 MySQL 执行
SELECT u.name, COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2025-01-01'
GROUP BY u.name
ORDER BY order_count DESC
LIMIT 20;
```

## 注意事项

- 需要确保 MySQL 服务器可达且凭证正确。
- 连接池参数应根据实际负载进行调优。
- 建议在生产环境启用 SSL 连接（`ssl_mode: required`）。
- 密码不应硬编码在配置文件中，建议使用环境变量。
