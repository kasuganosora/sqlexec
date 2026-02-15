# Memory 内存数据源

Memory 是 SQLExec 的默认数据源，所有数据存储在内存中，支持完整的读写操作和 MVCC 事务。适用于临时数据处理、测试环境以及不需要持久化的场景。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `memory` |
| `writable` | bool | 否 | 始终支持读写，默认 `true` |

### datasources.json 配置

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

### 嵌入模式配置

```go
package main

import (
    "fmt"
    "github.com/mySQLExec/db"
)

func main() {
    // 创建 MVCC 内存数据源
    ds := db.NewMVCCDataSource()
    ds.Connect()
    defer ds.Close()

    // 创建表
    ds.Execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")

    // 插入数据
    ds.Execute("INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com')")
    ds.Execute("INSERT INTO users VALUES (2, '李四', 'lisi@example.com')")

    // 查询数据
    result, _ := ds.Query("SELECT * FROM users WHERE id = 1")
    fmt.Println(result)
}
```

## 功能特性

Memory 数据源支持 SQLExec 的全部 SQL 功能：

### DDL 操作

```sql
-- 创建表
CREATE TABLE products (
    id INT,
    name TEXT,
    price FLOAT,
    description TEXT
);

-- 删除表
DROP TABLE products;

-- 清空表数据（保留表结构）
TRUNCATE TABLE products;
```

### DML 操作

```sql
-- 插入
INSERT INTO products VALUES (1, '笔记本电脑', 5999.00, '高性能商务本');

-- 查询
SELECT * FROM products WHERE price > 1000 ORDER BY price DESC;

-- 更新
UPDATE products SET price = 5499.00 WHERE id = 1;

-- 删除
DELETE FROM products WHERE id = 1;
```

### 索引支持

Memory 数据源支持多种索引类型，可以显著提升查询性能：

| 索引类型 | 创建语法 | 适用场景 |
|----------|----------|----------|
| B-Tree | `CREATE INDEX` | 等值查询、范围查询、排序 |
| Hash | `CREATE HASH INDEX` | 等值查询（更快） |
| Fulltext | `CREATE FULLTEXT INDEX` | 全文搜索 |
| Vector | `CREATE VECTOR INDEX` | 向量相似度搜索 |

```sql
-- 创建 B-Tree 索引
CREATE INDEX idx_name ON products (name);

-- 创建 Hash 索引
CREATE HASH INDEX idx_id ON products (id);

-- 创建全文索引
CREATE FULLTEXT INDEX idx_desc ON products (description);

-- 创建向量索引
CREATE VECTOR INDEX idx_embedding ON documents (embedding);
```

### 事务支持

Memory 数据源基于 MVCC（多版本并发控制）实现事务隔离：

```sql
-- 开始事务
BEGIN;

INSERT INTO accounts VALUES (1, '储蓄账户', 10000.00);
UPDATE accounts SET balance = balance - 500 WHERE id = 1;

-- 提交事务
COMMIT;

-- 或者回滚事务
-- ROLLBACK;
```

## 注意事项

- 所有数据存储在内存中，进程退出后数据将丢失。
- 适合数据量较小的场景，大数据集请使用 MySQL 或 PostgreSQL。
- 默认即为可写数据源，无需额外配置。
- MVCC 事务支持并发读写，读操作不会阻塞写操作。
