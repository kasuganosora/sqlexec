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

SQLExec parses SQL queries and builds execution plans. The optimizer pushes down suitable operations to MySQL for execution, rather than simply passing through the entire query.

### Pushdown Mechanism

SQLExec's query optimizer implements the following pushdown rules:

| Rule | Description |
|------|-------------|
| **PredicatePushDown** | Pushes down WHERE conditions to the data source |
| **LimitPushDown** | Pushes down LIMIT/OFFSET to the data source |
| **TopNPushDown** | Pushes down ORDER BY + LIMIT combinations |

### Supported Pushdown Conditions

| Condition Type | Supports Pushdown | Example |
|----------------|-------------------|---------|
| Comparison operators | ✅ | `age > 30`, `name = 'Alice'` |
| LIKE pattern matching | ✅ | `name LIKE 'John%'` |
| IN list | ✅ | `id IN (1, 2, 3)` |
| BETWEEN | ✅ | `age BETWEEN 20 AND 30` |
| IS NULL / IS NOT NULL | ✅ | `email IS NULL` |
| AND combined conditions | ✅ | Decomposed and pushed separately |
| OR conditions | ⚠️ Partial | Converted to UNION then pushed |
| LIMIT / OFFSET | ✅ | Pushed directly |
| ORDER BY + LIMIT (TopN) | ✅ | Passthrough pushdown |

### Operations Not Supported for Pushdown

The following operations are executed locally by SQLExec:

- Aggregate functions (SUM, COUNT, AVG, etc.)
- Complex expressions and function calls
- Subqueries (may be pushed after decorrelation in some cases)

### Execution Flow

```
SQL Statement
    ↓
SQLExec parses and builds logical plan
    ↓
Optimizer applies pushdown rules
    ↓
Pushable conditions converted to MySQL SQL
    ↓
Sent to MySQL for execution, results returned
```

### Query Examples

```sql
-- WHERE condition and LIMIT are pushed down to MySQL
SELECT id, name FROM users WHERE age > 30 LIMIT 10;

-- Actual SQL sent to MySQL:
-- SELECT id, name FROM users WHERE age > 30 LIMIT 10

-- ORDER BY + LIMIT (TopN) is also pushed down
SELECT * FROM orders ORDER BY created_at DESC LIMIT 20;

-- Aggregate functions are executed locally
SELECT COUNT(*) FROM users WHERE status = 'active';
-- WHERE status = 'active' is pushed down, but COUNT(*) is calculated by SQLExec
```

## Cross-Data-Source JOIN

SQLExec supports cross-data-source JOIN queries, such as joining MySQL tables with memory tables or other external data source tables.

### Execution Flow

```
SELECT * FROM mysql_table m JOIN memory_table t ON m.id = t.ref_id WHERE m.status = 'active' AND t.type = 1
                                    ↓
                        SQLExec parses and builds logical plan
                                    ↓
                        Optimizer analyzes predicate references
                                    ↓
              ┌─────────────────────┴─────────────────────┐
              ↓                                           ↓
    mysql.status = 'active'                      memory.type = 1
    Pushed to MySQL                               Pushed to memory source
              ↓                                           ↓
    Returns filtered data                         Returns filtered data
              ↓                                           ↓
              └─────────────────────┬─────────────────────┘
                                    ↓
                        SQLExec executes JOIN locally
                                    ↓
                          Returns final result
```

### Predicate Pushdown Strategy

In cross-data-source JOIN scenarios, SQLExec intelligently pushes predicates to the corresponding data sources:

| Predicate Type | Pushdown Strategy |
|----------------|-------------------|
| Predicates referencing only left table | Pushed to left data source |
| Predicates referencing only right table | Pushed to right data source |
| JOIN conditions referencing both tables | Executed locally |
| Complex expressions | Executed locally |

### Example

```sql
-- JOIN MySQL users table with memory orders table
SELECT u.name, o.order_no
FROM mysql_db.users u
JOIN memory.orders o ON u.id = o.user_id
WHERE u.status = 'active'    -- Pushed to MySQL
  AND o.amount > 100;        -- Pushed to memory source

-- Execution process:
-- 1. MySQL executes: SELECT id, name FROM users WHERE status = 'active'
-- 2. Memory source executes: SELECT user_id, order_no, amount FROM orders WHERE amount > 100
-- 3. SQLExec executes JOIN locally: ON u.id = o.user_id
-- 4. Returns result
```

### Performance Tips

1. **Filter Early**: Ensure each data source receives filter conditions to reduce data transfer
2. **Small Table Drives**: Use the smaller table as the driving table in JOINs
3. **Avoid Full Table Scans**: Create indexes on JOIN and filter fields
4. **Limit Result Set**: Use LIMIT to constrain final result size

```sql
-- Good practice: Each table has filter conditions
SELECT *
FROM mysql_db.large_table m
JOIN memory.small_table s ON m.id = s.ref_id
WHERE m.created_at > '2025-01-01'  -- MySQL filter
  AND s.status = 'valid';          -- Memory filter

-- Avoid: Cross-data-source JOIN without filters (poor performance with large data)
SELECT * FROM mysql_db.large_table m JOIN memory.small_table s ON m.id = s.ref_id;
```

## Notes

- Ensure that the MySQL server is reachable and the credentials are correct.
- Connection pool parameters should be tuned based on actual workload.
- It is recommended to enable SSL connections in production (`ssl_mode: required`).
- Passwords should not be hardcoded in configuration files; use environment variables instead.
- Cross-data-source JOINs are executed locally; be mindful of memory usage with large datasets.
