# Memory 内存数据源

Memory 是 SQLExec 的默认数据源，所有数据存储在内存中，支持完整的读写操作和 MVCC 事务。它是 SQLExec 的核心引擎，所有文件类数据源（CSV、JSON、Excel 等）都基于 Memory 数据源实现。

适用于临时数据处理、测试环境、嵌入式应用以及不需要持久化的场景。如需持久化，可配合 [XML 持久化存储](xml-persistence.md) 使用。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `memory` |
| `writable` | bool | 否 | 始终支持读写，默认 `true` |

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "default",
      "type": "memory",
      "writable": true
    }
  ]
}
```

### 嵌入模式

```go
package main

import (
    "fmt"
    "github.com/kasuganosora/sqlexec/pkg/api"
)

func main() {
    // 创建数据库实例（自动创建默认内存数据源）
    db, _ := api.NewDB(nil)
    session := db.Session()
    defer session.Close()

    // 创建表
    session.Execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), email VARCHAR(200))")

    // 插入数据
    session.Execute("INSERT INTO users (name, email) VALUES ('张三', 'zhangsan@example.com')")
    session.Execute("INSERT INTO users (name, email) VALUES ('李四', 'lisi@example.com')")

    // 查询数据
    result, _ := session.Query("SELECT * FROM users WHERE id = 1")
    fmt.Println(result)
}
```

## 支持的数据类型

Memory 数据源支持以下 SQL 数据类型：

| 类别 | 数据类型 | 说明 |
|------|----------|------|
| 整数 | `INT`, `INTEGER`, `TINYINT`, `SMALLINT`, `MEDIUMINT`, `BIGINT` | 整数类型 |
| 浮点数 | `FLOAT`, `DOUBLE`, `DECIMAL`, `NUMERIC` | 浮点数和定点数 |
| 字符串 | `VARCHAR(n)`, `CHAR(n)`, `TEXT` | 字符串类型 |
| 布尔 | `BOOLEAN`, `BOOL` | 布尔值 |
| 日期时间 | `DATE`, `DATETIME`, `TIMESTAMP`, `TIME` | 日期和时间类型 |
| 二进制 | `BLOB` | 二进制数据 |
| 向量 | `VECTOR(dim)` | 向量类型，`dim` 为维度数 |

## 功能特性

Memory 数据源支持 SQLExec 的全部 SQL 功能。

### DDL 操作

```sql
-- 创建表（支持约束）
CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) DEFAULT 0.00,
    category VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP
);

-- 创建临时表（会话结束自动删除）
CREATE TEMPORARY TABLE temp_results (
    id INT,
    score FLOAT
);

-- 删除表
DROP TABLE products;

-- 清空表数据（保留表结构）
TRUNCATE TABLE products;
```

### 约束支持

| 约束 | 语法 | 说明 |
|------|------|------|
| 主键 | `PRIMARY KEY` | 唯一标识行，不允许 NULL |
| 自增 | `AUTO_INCREMENT` | 自动递增的整数列 |
| 非空 | `NOT NULL` | 列值不允许为空 |
| 唯一 | `UNIQUE` | 列值不允许重复 |
| 默认值 | `DEFAULT value` | 插入时未指定则使用默认值 |

```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(200) NOT NULL,
    status INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### DML 操作

```sql
-- 插入
INSERT INTO products (name, price, category) VALUES ('笔记本电脑', 5999.00, '电子产品');

-- 批量插入
INSERT INTO products (name, price, category) VALUES
    ('鼠标', 99.00, '配件'),
    ('键盘', 299.00, '配件'),
    ('显示器', 1999.00, '电子产品');

-- 查询（支持 WHERE、ORDER BY、LIMIT、GROUP BY、HAVING）
SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM products
WHERE price > 100
GROUP BY category
HAVING cnt > 1
ORDER BY avg_price DESC
LIMIT 10;

-- 子查询
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);

-- 更新
UPDATE products SET price = 5499.00 WHERE id = 1;

-- 删除
DELETE FROM products WHERE category = '已下架';
```

### 索引支持

Memory 数据源支持多种索引类型，可以显著提升查询性能：

| 索引类型 | 创建语法 | 适用场景 |
|----------|----------|----------|
| B-Tree | `CREATE INDEX` | 等值查询、范围查询、排序 |
| Hash | `CREATE HASH INDEX` | 等值查询（更快） |
| Unique | `CREATE UNIQUE INDEX` | 等值查询 + 唯一约束 |
| Fulltext | `CREATE FULLTEXT INDEX` | 全文搜索（支持中文分词） |
| Vector | `CREATE VECTOR INDEX` | 向量相似度搜索 |

```sql
-- 创建 B-Tree 索引（默认类型）
CREATE INDEX idx_name ON products (name);

-- 创建 Hash 索引
CREATE HASH INDEX idx_id ON products (id);

-- 创建唯一索引
CREATE UNIQUE INDEX idx_email ON users (email);

-- 创建全文索引（支持 BM25 评分和中文 Jieba 分词）
CREATE FULLTEXT INDEX idx_desc ON products (description);

-- 创建向量索引（支持 HNSW、IVF-Flat 等算法）
CREATE VECTOR INDEX idx_embedding ON documents (embedding);

-- 删除索引
DROP INDEX idx_name ON products;
```

#### 全文搜索

全文索引使用 BM25 评分算法，支持中文 Jieba 分词：

```sql
-- 全文搜索
SELECT * FROM articles WHERE MATCH(content) AGAINST('数据库 性能优化');
```

#### 向量搜索

向量索引支持多种距离度量方式和索引算法：

```sql
-- 创建向量列
CREATE TABLE documents (
    id INT PRIMARY KEY,
    title TEXT,
    embedding VECTOR(768)
);

-- 向量相似度搜索（余弦距离）
SELECT id, title, VEC_COSINE_DISTANCE(embedding, '[0.1, 0.2, ...]') AS distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

### 事务支持

Memory 数据源基于 MVCC（多版本并发控制）实现完整的事务支持，提供快照隔离。

#### 事务隔离级别

| 隔离级别 | 说明 |
|----------|------|
| `READ UNCOMMITTED` | 允许读取未提交数据 |
| `READ COMMITTED` | 只读取已提交数据 |
| `REPEATABLE READ` | 默认级别，事务内可重复读 |
| `SERIALIZABLE` | 最高隔离级别，完全串行化 |

```sql
-- 设置隔离级别
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- 开始事务
BEGIN;

INSERT INTO accounts (name, balance) VALUES ('储蓄账户', 10000.00);
UPDATE accounts SET balance = balance - 500 WHERE id = 1;

-- 提交事务
COMMIT;

-- 或者回滚事务
-- ROLLBACK;
```

#### MVCC 工作原理

```
事务 A（写入）                          事务 B（读取）
    │                                      │
    ├── BEGIN                              ├── BEGIN
    ├── UPDATE products ...                │
    │   (创建新版本 v2)                     ├── SELECT * FROM products
    │                                      │   (读取快照版本 v1，不受影响)
    ├── COMMIT                             │
    │   (v2 对新事务可见)                   ├── SELECT * FROM products
    │                                      │   (仍然读取 v1，可重复读)
                                           ├── COMMIT
```

- 读操作不会阻塞写操作，写操作不会阻塞读操作。
- 每个事务看到的是事务开始时的数据快照。
- 旧版本数据由 GC 自动清理，不需要手动维护。

## 混合数据源 JOIN

Memory 数据源可以与 MySQL、PostgreSQL 等外部数据源进行跨数据源 JOIN 查询。

```sql
-- 内存表 orders 与 MySQL 表 users 进行 JOIN
SELECT u.name, o.order_no, o.amount
FROM mysql_db.users u
JOIN memory.orders o ON u.id = o.user_id
WHERE u.status = 'active'    -- 下推到 MySQL
  AND o.amount > 100;        -- 在内存数据源过滤
```

执行过程：

1. MySQL 执行：`SELECT id, name FROM users WHERE status = 'active'`
2. 内存数据源过滤：`SELECT * FROM orders WHERE amount > 100`
3. SQLExec 本地执行 JOIN
4. 返回最终结果

## 持久化

Memory 数据源默认不持久化数据。如需将表数据持久化到磁盘，可以使用 XML 持久化引擎：

```sql
-- 创建持久化表
CREATE TABLE important_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    value TEXT
) ENGINE=xml;

-- 数据会在每次 DML 后自动写入磁盘
-- 重启后通过 USE 命令自动恢复
```

详细说明请参考 [XML 持久化存储](xml-persistence.md)。

## 注意事项

- 所有数据默认存储在内存中，进程退出后数据将丢失。需要持久化请使用 `ENGINE=xml`。
- 适合数据量较小到中等的场景，大数据集请使用 MySQL 或 PostgreSQL。
- 默认即为可写数据源，无需额外配置。
- MVCC 事务支持并发读写，读操作不会阻塞写操作。
- 支持临时表（`CREATE TEMPORARY TABLE`），会话结束自动删除。
- Memory 数据源是所有文件类数据源（CSV、JSON、Excel、Parquet、JSONL）的底层引擎。
