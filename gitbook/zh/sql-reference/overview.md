# SQL 支持概述

SQLExec 基于 TiDB Parser 构建 SQL 解析层，支持 MySQL 兼容的 SQL 语法。你可以使用熟悉的 SQL 语句来查询和管理数据，无需学习新的查询语言。

## SQL 语句分类

SQLExec 支持以下四类 SQL 语句：

| 分类 | 语句 | 说明 |
|------|------|------|
| **DQL** (数据查询) | `SELECT` | 查询数据，支持 JOIN、子查询、聚合、窗口函数等 |
| **DML** (数据操作) | `INSERT`, `UPDATE`, `DELETE` | 插入、更新、删除数据 |
| **DDL** (数据定义) | `CREATE`, `ALTER`, `DROP`, `TRUNCATE` | 创建、修改、删除表结构 |
| **Admin** (管理命令) | `SHOW`, `DESCRIBE`, `USE`, `SET`, `EXPLAIN` | 查看元数据、切换数据源、设置变量等 |

## 支持的数据类型

| 类型 | 说明 | 示例 |
|------|------|------|
| `INT` | 32 位整数 | `42` |
| `BIGINT` | 64 位整数 | `9223372036854775807` |
| `FLOAT` | 32 位浮点数 | `3.14` |
| `DOUBLE` | 64 位浮点数 | `3.141592653589793` |
| `DECIMAL` | 高精度小数 | `99999.99` |
| `VARCHAR(n)` | 变长字符串 | `'hello'` |
| `TEXT` | 长文本 | `'长篇内容...'` |
| `BOOL` | 布尔值 | `TRUE`, `FALSE` |
| `DATE` | 日期 | `'2026-01-15'` |
| `DATETIME` | 日期时间 | `'2026-01-15 10:30:00'` |
| `TIMESTAMP` | 时间戳 | `'2026-01-15 10:30:00'` |
| `VECTOR(dim)` | 向量类型 | `VECTOR(768)` |

## 参数化查询

SQLExec 支持使用 `?` 占位符进行参数化查询，有效防止 SQL 注入：

```sql
SELECT * FROM users WHERE name = ? AND age > ?
```

在 Go 代码中使用参数化查询：

```go
rows, err := db.Query("SELECT * FROM users WHERE name = ? AND age > ?", "张三", 18)
```

## SQL 注释

SQLExec 支持三种注释格式：

### 单行注释

使用 `--` 开头的注释：

```sql
-- 这是一条单行注释
SELECT * FROM users; -- 行末注释
```

### 多行注释

使用 `/* */` 包裹的注释：

```sql
/*
  这是一条多行注释
  可以跨越多行
*/
SELECT * FROM users;
```

### Trace-ID 注释

使用特殊格式的注释传递 trace-id，用于请求追踪和审计日志：

```sql
/*trace_id=abc-123-def*/ SELECT * FROM users WHERE id = 1;
```

trace-id 会被自动提取并贯穿整个查询执行过程，方便在日志中追踪特定请求。
