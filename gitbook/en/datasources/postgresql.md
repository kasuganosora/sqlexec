# PostgreSQL Data Source

The PostgreSQL data source allows SQLExec to connect to external PostgreSQL databases (version 12 and above). SQL queries are pushed down directly to PostgreSQL for execution.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `postgresql` |
| `host` | string | Yes | PostgreSQL server address |
| `port` | int | No | Port number, default `5432` |
| `username` | string | Yes | Database username |
| `password` | string | Yes | Database password |
| `database` | string | Yes | Remote PostgreSQL database name |

## Connection Options

Advanced connection parameters can be configured through the `options` field:

| Option | Default | Description |
|--------|---------|-------------|
| `schema` | `public` | Default schema |
| `ssl_mode` | `disable` | SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `connect_timeout` | `10s` | Connection timeout |
| `max_open_conns` | `25` | Maximum number of open connections |
| `max_idle_conns` | `5` | Maximum number of idle connections |

## Configuration Examples

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

### Embedded Mode

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

    // Switch to the PostgreSQL data source
    engine.Execute("USE pgdb")

    // Queries will be executed directly on PostgreSQL
    result, err := engine.Query("SELECT * FROM events WHERE created_at > NOW() - INTERVAL '7 days'")
    if err != nil {
        panic(err)
    }
    fmt.Println(result)
}
```

## Query Pushdown

All SQL queries are pushed down directly to PostgreSQL for execution:

```sql
-- Switch to the PostgreSQL data source
USE pgdb;

-- Use PostgreSQL native syntax and functions
SELECT
    date_trunc('day', created_at) AS day,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM events
WHERE created_at >= '2025-01-01'
GROUP BY date_trunc('day', created_at)
ORDER BY day DESC;
```

You can use PostgreSQL-specific features, for example:

```sql
-- Using CTEs
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

-- Using JSON operations
SELECT id, profile->>'name' AS name
FROM users
WHERE profile @> '{"role": "admin"}';
```

## Notes

- Ensure that the PostgreSQL server is reachable and the credentials are correct.
- The `schema` option specifies the default search path, defaulting to `public`.
- For production environments, use `ssl_mode: require` or a higher security level.
- Passwords should not be hardcoded in configuration files; use environment variables instead.
- Connection pool parameters should be tuned based on actual workload.
