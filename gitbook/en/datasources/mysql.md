# MySQL Data Source

The MySQL data source allows SQLExec to connect to external MySQL databases (version 5.7 and above). SQL queries are pushed down directly to MySQL for execution.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `mysql` |
| `host` | string | Yes | MySQL server address |
| `port` | int | No | Port number, default `3306` |
| `username` | string | Yes | Database username |
| `password` | string | Yes | Database password |
| `database` | string | Yes | Remote MySQL database name |

## Connection Options

Advanced connection parameters can be configured through the `options` field:

| Option | Default | Description |
|--------|---------|-------------|
| `max_open_conns` | `25` | Maximum number of open connections |
| `max_idle_conns` | `5` | Maximum number of idle connections |
| `conn_max_lifetime` | `300s` | Maximum connection lifetime |
| `conn_max_idle_time` | `60s` | Maximum idle connection lifetime |
| `charset` | `utf8mb4` | Character set |
| `collation` | _(default)_ | Collation, e.g., `utf8mb4_unicode_ci` |
| `ssl_mode` | _(disabled)_ | SSL mode: `disabled`, `preferred`, `required` |
| `connect_timeout` | `10s` | Connection timeout |

## Configuration Examples

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

### Embedded Mode

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

    // Switch to the MySQL data source
    engine.Execute("USE mydb")

    // Queries will be executed directly on MySQL
    result, err := engine.Query("SELECT id, name, email FROM users LIMIT 10")
    if err != nil {
        panic(err)
    }
    fmt.Println(result)
}
```

## Query Pushdown

All SQL queries are pushed down directly to MySQL for execution. SQLExec does not parse or process the query logic locally. This means:

- You can use all of MySQL's native SQL syntax and functions.
- Query performance depends on the MySQL server itself.
- Indexes, execution plans, etc. are all managed by MySQL.

```sql
-- Switch to the MySQL data source
USE mydb;

-- The following query is executed directly by MySQL
SELECT u.name, COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2025-01-01'
GROUP BY u.name
ORDER BY order_count DESC
LIMIT 20;
```

## Notes

- Ensure that the MySQL server is reachable and the credentials are correct.
- Connection pool parameters should be tuned based on actual workload.
- It is recommended to enable SSL connections in production (`ssl_mode: required`).
- Passwords should not be hardcoded in configuration files; use environment variables instead.
