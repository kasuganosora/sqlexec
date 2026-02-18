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

SQLExec 会解析 SQL 查询并构建执行计划，通过优化器将适合下推的操作下发到 MySQL 执行，而非简单地透传整个查询。

### 下推机制

SQLExec 的查询优化器实现了以下下推规则：

| 规则 | 说明 |
|------|------|
| **PredicatePushDown** | 将 WHERE 条件下推到数据源 |
| **LimitPushDown** | 将 LIMIT/OFFSET 下推到数据源 |
| **TopNPushDown** | 将 ORDER BY + LIMIT 组合下推 |

### 支持下推的条件

| 条件类型 | 支持下推 | 示例 |
|----------|----------|------|
| 比较运算符 | ✅ | `age > 30`, `name = 'Alice'` |
| LIKE 模糊匹配 | ✅ | `name LIKE 'John%'` |
| IN 列表 | ✅ | `id IN (1, 2, 3)` |
| BETWEEN | ✅ | `age BETWEEN 20 AND 30` |
| IS NULL / IS NOT NULL | ✅ | `email IS NULL` |
| AND 组合条件 | ✅ | 分解后分别下推 |
| OR 条件 | ⚠️ 部分 | 转换为 UNION 后下推 |
| LIMIT / OFFSET | ✅ | 直接下推 |
| ORDER BY + LIMIT (TopN) | ✅ | 穿透下推 |

### 不支持下推的操作

以下操作由 SQLExec 在本地执行：

- 聚合函数（SUM, COUNT, AVG 等）
- 复杂表达式和函数调用
- 子查询（视情况可能解关联后下推）

### 执行流程

```
SQL 语句
    ↓
SQLExec 解析并构建逻辑计划
    ↓
优化器应用下推规则
    ↓
将可下推的条件转换为 MySQL SQL
    ↓
发送到 MySQL 执行，返回结果
```

### 查询示例

```sql
-- WHERE 条件和 LIMIT 会被下推到 MySQL
SELECT id, name FROM users WHERE age > 30 LIMIT 10;

-- 实际发送到 MySQL 的 SQL：
-- SELECT id, name FROM users WHERE age > 30 LIMIT 10

-- ORDER BY + LIMIT (TopN) 也会被下推
SELECT * FROM orders ORDER BY created_at DESC LIMIT 20;

-- 聚合函数在本地执行
SELECT COUNT(*) FROM users WHERE status = 'active';
-- WHERE status = 'active' 会被下推，但 COUNT(*) 由 SQLExec 计算
```

## 混合数据源 JOIN

SQLExec 支持跨数据源的 JOIN 查询，例如 MySQL 表与内存表、或其他外部数据源表之间的连接。

### 执行流程

```
SELECT * FROM mysql_table m JOIN memory_table t ON m.id = t.ref_id WHERE m.status = 'active' AND t.type = 1
                                    ↓
                        SQLExec 解析并构建逻辑计划
                                    ↓
                        优化器分析谓词引用的表
                                    ↓
              ┌─────────────────────┴─────────────────────┐
              ↓                                           ↓
    mysql.status = 'active'                      memory.type = 1
    下推到 MySQL 执行                              下推到内存数据源
              ↓                                           ↓
    返回过滤后的数据                               返回过滤后的数据
              ↓                                           ↓
              └─────────────────────┬─────────────────────┘
                                    ↓
                        SQLExec 本地执行 JOIN
                                    ↓
                          返回最终结果
```

### 谓词下推策略

在混合数据源 JOIN 场景中，SQLExec 会智能地将谓词下推到对应的数据源：

| 谓词类型 | 下推策略 |
|----------|----------|
| 仅引用左表的谓词 | 下推到左表数据源 |
| 仅引用右表的谓词 | 下推到右表数据源 |
| 同时引用两表的 JOIN 条件 | 本地执行 |
| 复杂表达式 | 本地执行 |

### 示例

```sql
-- MySQL 表 users 与内存表 orders 进行 JOIN
SELECT u.name, o.order_no
FROM mysql_db.users u
JOIN memory.orders o ON u.id = o.user_id
WHERE u.status = 'active'    -- 下推到 MySQL
  AND o.amount > 100;        -- 下推到内存数据源

-- 执行过程：
-- 1. MySQL 执行: SELECT id, name FROM users WHERE status = 'active'
-- 2. 内存数据源执行: SELECT user_id, order_no, amount FROM orders WHERE amount > 100
-- 3. SQLExec 本地执行 JOIN: ON u.id = o.user_id
-- 4. 返回结果
```

### 性能建议

1. **优先过滤**：确保每个数据源都能收到过滤条件，减少数据传输量
2. **小表驱动**：让数据量较小的表作为 JOIN 的驱动表
3. **避免全表扫描**：为 JOIN 字段和过滤字段建立索引
4. **限制结果集**：使用 LIMIT 限制最终结果大小

```sql
-- 好的做法：每个表都有过滤条件
SELECT *
FROM mysql_db.large_table m
JOIN memory.small_table s ON m.id = s.ref_id
WHERE m.created_at > '2025-01-01'  -- MySQL 过滤
  AND s.status = 'valid';          -- 内存过滤

-- 避免：无过滤条件的跨数据源 JOIN（数据量大时性能差）
SELECT * FROM mysql_db.large_table m JOIN memory.small_table s ON m.id = s.ref_id;
```

## 注意事项

- 需要确保 MySQL 服务器可达且凭证正确。
- 连接池参数应根据实际负载进行调优。
- 建议在生产环境启用 SSL 连接（`ssl_mode: required`）。
- 密码不应硬编码在配置文件中，建议使用环境变量。
- 混合数据源 JOIN 在本地执行，大数据量时需注意内存使用。
