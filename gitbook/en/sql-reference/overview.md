# SQL Support Overview

SQLExec builds its SQL parsing layer on top of TiDB Parser, supporting MySQL-compatible SQL syntax. You can use familiar SQL statements to query and manage data without learning a new query language.

## SQL Statement Categories

SQLExec supports the following four categories of SQL statements:

| Category | Statements | Description |
|----------|-----------|-------------|
| **DQL** (Data Query) | `SELECT` | Query data with support for JOINs, subqueries, aggregation, window functions, etc. |
| **DML** (Data Manipulation) | `INSERT`, `UPDATE`, `DELETE` | Insert, update, and delete data |
| **DDL** (Data Definition) | `CREATE`, `ALTER`, `DROP`, `TRUNCATE` | Create, modify, and drop table structures |
| **Admin** (Administrative Commands) | `SHOW`, `DESCRIBE`, `USE`, `SET`, `EXPLAIN` | View metadata, switch datasources, set variables, etc. |

## Supported Data Types

| Type | Description | Example |
|------|-------------|---------|
| `INT` | 32-bit integer | `42` |
| `BIGINT` | 64-bit integer | `9223372036854775807` |
| `FLOAT` | 32-bit floating point | `3.14` |
| `DOUBLE` | 64-bit floating point | `3.141592653589793` |
| `DECIMAL` | High-precision decimal | `99999.99` |
| `VARCHAR(n)` | Variable-length string | `'hello'` |
| `TEXT` | Long text | `'Long content...'` |
| `BOOL` | Boolean | `TRUE`, `FALSE` |
| `DATE` | Date | `'2026-01-15'` |
| `DATETIME` | Date and time | `'2026-01-15 10:30:00'` |
| `TIMESTAMP` | Timestamp | `'2026-01-15 10:30:00'` |
| `VECTOR(dim)` | Vector type | `VECTOR(768)` |

## Parameterized Queries

SQLExec supports parameterized queries using `?` placeholders, which effectively prevent SQL injection:

```sql
SELECT * FROM users WHERE name = ? AND age > ?
```

Using parameterized queries in Go code:

```go
rows, err := db.Query("SELECT * FROM users WHERE name = ? AND age > ?", "张三", 18)
```

## SQL Comments

SQLExec supports three comment formats:

### Single-Line Comments

Comments starting with `--`:

```sql
-- This is a single-line comment
SELECT * FROM users; -- End-of-line comment
```

### Multi-Line Comments

Comments enclosed in `/* */`:

```sql
/*
  This is a multi-line comment
  that can span multiple lines
*/
SELECT * FROM users;
```

### Trace-ID Comments

Use specially formatted comments to pass a trace-id for request tracing and audit logging:

```sql
/*trace_id=abc-123-def*/ SELECT * FROM users WHERE id = 1;
```

The trace-id is automatically extracted and carried throughout the entire query execution process, making it easy to trace specific requests in logs.
