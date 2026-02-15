# Administrative Commands

Administrative commands are used to view database metadata, switch datasources, set variables, and analyze query plans.

## SHOW DATABASES

List all registered datasources:

```sql
SHOW DATABASES;
```

Example result:

| Database |
|----------|
| memory |
| mysql_prod |
| pg_analytics |
| csv_files |

## SHOW TABLES

### List All Tables in the Current Datasource

```sql
SHOW TABLES;
```

### List Tables in a Specific Datasource

```sql
SHOW TABLES FROM mysql_prod;
```

### Pattern Matching on Table Names

```sql
SHOW TABLES LIKE 'user%';
```

```sql
SHOW TABLES FROM mysql_prod LIKE '%order%';
```

## SHOW COLUMNS / DESCRIBE / DESC

View column definition information for a table. The following three forms are equivalent:

```sql
SHOW COLUMNS FROM users;
```

```sql
DESCRIBE users;
```

```sql
DESC users;
```

Example result:

| Field | Type | Null | Key | Default | Extra |
|-------|------|------|-----|---------|-------|
| id | BIGINT | NO | PRI | NULL | auto_increment |
| name | VARCHAR(100) | NO | | NULL | |
| email | VARCHAR(255) | YES | | NULL | |
| age | INT | YES | | 0 | |
| created_at | DATETIME | YES | | CURRENT_TIMESTAMP | |

## SHOW PROCESSLIST

View currently active connections and running queries:

```sql
SHOW PROCESSLIST;
```

Example result:

| Id | User | Host | db | Command | Time | State | Info |
|----|------|------|----|---------|------|-------|------|
| 1 | root | localhost | memory | Query | 0 | executing | SELECT * FROM users |
| 2 | app | 192.168.1.10 | mysql_prod | Sleep | 120 | waiting | NULL |

## SHOW VARIABLES

View variable settings for the current session:

```sql
SHOW VARIABLES;
```

```sql
SHOW VARIABLES LIKE 'trace%';
```

## SHOW STATUS

View server runtime status information:

```sql
SHOW STATUS;
```

## USE

Switch the current datasource:

```sql
USE mysql_prod;
```

After switching, subsequent SQL statements will be executed on that datasource:

```sql
USE mysql_prod;
SELECT * FROM users;  -- Queries the users table in mysql_prod

USE pg_analytics;
SELECT * FROM events; -- Queries the events table in pg_analytics
```

## SET Variables

### Setting Session Variables

```sql
SET @max_results = 100;
```

### Setting Trace-ID

By setting the `@trace_id` variable, subsequent queries will automatically carry that trace-id for request tracing and audit logging:

```sql
SET @trace_id = 'req-abc-123-def';
```

Once set, all subsequent queries will be associated with that trace-id in the logs until the connection is closed or the variable is reset.

This is similar in effect to using the comment approach (`/*trace_id=xxx*/`), but is more convenient when you need to trace multiple consecutive queries.

## EXPLAIN

View the execution plan of a query to understand how the query optimizer will execute the SQL statement:

```sql
EXPLAIN SELECT * FROM users WHERE age > 18;
```

Example result:

| id | select_type | table | type | possible_keys | key | rows | filtered | Extra |
|----|-------------|-------|------|---------------|-----|------|----------|-------|
| 1 | SIMPLE | users | ALL | NULL | NULL | 1000 | 33.33 | Using where |

```sql
EXPLAIN SELECT u.name, COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.name;
```

`EXPLAIN` helps you identify potential performance issues such as full table scans, missing indexes, and more.

## Comprehensive Example

```sql
-- View available datasources
SHOW DATABASES;

-- Switch to the production database
USE mysql_prod;

-- View all order-related tables
SHOW TABLES LIKE '%order%';

-- View the orders table structure
DESC orders;

-- Set a trace-id for tracing
SET @trace_id = 'debug-2026-0215';

-- Analyze the query plan
EXPLAIN SELECT * FROM orders WHERE status = 'pending' AND created_at > '2026-01-01';

-- Execute the query
SELECT * FROM orders WHERE status = 'pending' AND created_at > '2026-01-01' LIMIT 20;
```
