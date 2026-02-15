# 管理命令

管理命令用于查看数据库元信息、切换数据源、设置变量和分析查询计划。

## SHOW DATABASES

列出所有已注册的数据源：

```sql
SHOW DATABASES;
```

结果示例：

| Database |
|----------|
| memory |
| mysql_prod |
| pg_analytics |
| csv_files |

## SHOW TABLES

### 列出当前数据源的所有表

```sql
SHOW TABLES;
```

### 列出指定数据源的表

```sql
SHOW TABLES FROM mysql_prod;
```

### 模糊匹配表名

```sql
SHOW TABLES LIKE 'user%';
```

```sql
SHOW TABLES FROM mysql_prod LIKE '%order%';
```

## SHOW COLUMNS / DESCRIBE / DESC

查看表的列定义信息。以下三种写法等价：

```sql
SHOW COLUMNS FROM users;
```

```sql
DESCRIBE users;
```

```sql
DESC users;
```

结果示例：

| Field | Type | Null | Key | Default | Extra |
|-------|------|------|-----|---------|-------|
| id | BIGINT | NO | PRI | NULL | auto_increment |
| name | VARCHAR(100) | NO | | NULL | |
| email | VARCHAR(255) | YES | | NULL | |
| age | INT | YES | | 0 | |
| created_at | DATETIME | YES | | CURRENT_TIMESTAMP | |

## SHOW PROCESSLIST

查看当前活跃的连接和正在执行的查询：

```sql
SHOW PROCESSLIST;
```

结果示例：

| Id | User | Host | db | Command | Time | State | Info |
|----|------|------|----|---------|------|-------|------|
| 1 | root | localhost | memory | Query | 0 | executing | SELECT * FROM users |
| 2 | app | 192.168.1.10 | mysql_prod | Sleep | 120 | waiting | NULL |

## SHOW VARIABLES

查看当前会话的变量设置：

```sql
SHOW VARIABLES;
```

```sql
SHOW VARIABLES LIKE 'trace%';
```

## SHOW STATUS

查看服务器运行状态信息：

```sql
SHOW STATUS;
```

## USE

切换当前使用的数据源：

```sql
USE mysql_prod;
```

切换后，后续的 SQL 语句将在该数据源上执行：

```sql
USE mysql_prod;
SELECT * FROM users;  -- 查询 mysql_prod 中的 users 表

USE pg_analytics;
SELECT * FROM events; -- 查询 pg_analytics 中的 events 表
```

## SET 设置变量

### 设置会话变量

```sql
SET @max_results = 100;
```

### 设置 Trace-ID

通过设置 `@trace_id` 变量，后续查询将自动附带该 trace-id，便于请求追踪和审计日志：

```sql
SET @trace_id = 'req-abc-123-def';
```

设置后，后续所有查询都会在日志中关联该 trace-id，直到连接关闭或重新设置。

这与使用注释方式（`/*trace_id=xxx*/`）的效果类似，但更方便在需要追踪多条连续查询时使用。

## EXPLAIN

查看查询的执行计划，了解查询优化器如何执行 SQL 语句：

```sql
EXPLAIN SELECT * FROM users WHERE age > 18;
```

结果示例：

| id | select_type | table | type | possible_keys | key | rows | filtered | Extra |
|----|-------------|-------|------|---------------|-----|------|----------|-------|
| 1 | SIMPLE | users | ALL | NULL | NULL | 1000 | 33.33 | Using where |

```sql
EXPLAIN SELECT u.name, COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.name;
```

`EXPLAIN` 帮助你识别潜在的性能问题，例如全表扫描、缺少索引等。

## 综合示例

```sql
-- 查看可用的数据源
SHOW DATABASES;

-- 切换到生产数据库
USE mysql_prod;

-- 查看所有订单相关的表
SHOW TABLES LIKE '%order%';

-- 查看 orders 表结构
DESC orders;

-- 设置 trace-id 用于追踪
SET @trace_id = 'debug-2026-0215';

-- 分析查询计划
EXPLAIN SELECT * FROM orders WHERE status = 'pending' AND created_at > '2026-01-01';

-- 执行查询
SELECT * FROM orders WHERE status = 'pending' AND created_at > '2026-01-01' LIMIT 20;
```
