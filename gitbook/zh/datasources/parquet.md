# Parquet 数据源

Parquet 数据源提供基于 [Apache Parquet](https://parquet.apache.org/) 格式的全功能持久化列式存储引擎。支持多表管理、完整的 DDL/DML 操作、基于 WAL 的崩溃恢复、定期刷盘，以及所有内存引擎特性（包括 MVCC 事务和高级索引）。

数据采用**目录模式**组织：一个目录代表一个数据库，每张表存储为独立的 `.parquet` 文件。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据目录路径（一个目录 = 一个数据库） |
| `type` | string | 是 | 固定值 `parquet` |
| `writable` | bool | 否 | 是否允许写入操作，默认 `false` |

## 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `writable` | `false` | 是否允许写入操作 |
| `compression` | `snappy` | 压缩算法：`snappy`、`gzip`、`zstd`、`lz4`、`none` |
| `flush_interval` | `30s` | 定期刷盘间隔（如 `10s`、`1m`、`5m`） |

## 架构

```
data_directory/
  ├── users.parquet          # 表 "users"
  ├── orders.parquet         # 表 "orders"
  ├── products.parquet       # 表 "products"
  ├── .wal                   # Write-Ahead Log
  └── .sqlexec_meta          # 索引元数据（sidecar 文件）
```

- 每个 `.parquet` 文件以原生 Apache Parquet 格式存储一张表。
- `.wal` 文件记录所有写入操作，用于崩溃恢复。
- `.sqlexec_meta` 文件持久化索引定义，确保重启后恢复。

## 功能特性

### DDL 操作

支持 `CREATE TABLE`、`DROP TABLE`、`TRUNCATE TABLE`：

```sql
USE my_parquet_db;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    email VARCHAR,
    score FLOAT
);

TRUNCATE TABLE users;

DROP TABLE users;
```

### DML 操作

支持 `INSERT`、`UPDATE`、`DELETE`、`SELECT`：

```sql
-- 插入数据
INSERT INTO users (id, name, email, score)
VALUES (1, 'Alice', 'alice@example.com', 95.5);

-- 更新数据
UPDATE users SET score = 98.0 WHERE id = 1;

-- 删除数据
DELETE FROM users WHERE score < 60;

-- 带过滤、排序和分页的查询
SELECT name, score
FROM users
WHERE score >= 80
ORDER BY score DESC
LIMIT 10;
```

### 索引

继承内存引擎的所有索引类型：

- **B-Tree 索引** -- 范围查询、排序
- **Hash 索引** -- 精确匹配查找
- **全文索引** -- BM25 评分的文本搜索
- **向量索引** -- 相似度搜索（HNSW、IVF）
- **空间索引** -- 地理查询（R-Tree）

索引自动持久化到 `.sqlexec_meta` sidecar 文件，重连时自动重建。

```sql
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);
```

### MVCC 与事务

完整继承内存引擎的 MVCC 支持：

```sql
BEGIN;
INSERT INTO orders (id, user_id, total) VALUES (1, 1, 99.99);
UPDATE users SET score = score + 1 WHERE id = 1;
COMMIT;
```

### WAL 与持久化

所有写入操作在应用到内存之前，先记录到 Write-Ahead Log (WAL)：

1. **WAL 写入** -- 每个 INSERT/UPDATE/DELETE/DDL 操作追加到 `.wal` 并 `fsync`。
2. **定期刷盘** -- 后台协程按配置的间隔（默认 30 秒）将脏表写入 `.parquet` 文件。
3. **崩溃恢复** -- 重连时回放 WAL，重建所有未刷盘的变更。
4. **检查点** -- 刷盘成功后，WAL 写入检查点标记并截断。

`Close()` 时会将所有脏表刷盘并清理 WAL，确保数据不丢失。

### 跨重启的数据持久化

```go
// 阶段 1：创建并填充数据
adapter := parquet.NewParquetAdapter(config)
adapter.Connect(ctx)
adapter.CreateTable(ctx, tableInfo)
adapter.Insert(ctx, "users", rows, nil)
adapter.Close(ctx)  // 刷盘到 .parquet 文件

// 阶段 2：重连 -- 数据仍然存在
adapter2 := parquet.NewParquetAdapter(config)
adapter2.Connect(ctx)  // 读取 .parquet 文件 + 回放 WAL
result, _ := adapter2.Query(ctx, "users", &domain.QueryOptions{})
// result.Rows 包含之前插入的所有数据
```

## 类型映射

| Go / SQL 类型 | Parquet 类型 |
|---------------|-------------|
| `int64`、`bigint` | INT64 |
| `int32`、`int`、`integer` | INT32 |
| `float64`、`double` | DOUBLE |
| `float32`、`float` | FLOAT |
| `bool`、`boolean` | BOOLEAN |
| `string`、`varchar`、`text` | BYTE_ARRAY (UTF8) |
| `bytes`、`blob`、`binary` | BYTE_ARRAY |
| `time`、`datetime`、`timestamp` | INT64（毫秒） |

可空列使用 Parquet 的 `OPTIONAL` 重复类型。

## 配置示例

### datasources.json

```json
{
  "datasources": [
    {
      "name": "analytics",
      "type": "parquet",
      "writable": true,
      "options": {
        "writable": true,
        "compression": "snappy",
        "flush_interval": "30s"
      }
    }
  ]
}
```

### 嵌入式用法

```go
package main

import (
    "context"

    "github.com/kasuganosora/sqlexec/pkg/resource/parquet"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func main() {
    config := &domain.DataSourceConfig{
        Type:     "parquet",
        Name:     "./parquet_data",
        Writable: true,
        Options: map[string]interface{}{
            "writable":       true,
            "compression":    "zstd",
            "flush_interval": "10s",
        },
    }

    adapter := parquet.NewParquetAdapter(config)
    ctx := context.Background()
    adapter.Connect(ctx)
    defer adapter.Close(ctx)

    // 创建表
    adapter.CreateTable(ctx, &domain.TableInfo{
        Name: "events",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64", Primary: true},
            {Name: "event_type", Type: "string"},
            {Name: "timestamp", Type: "int64"},
            {Name: "payload", Type: "string", Nullable: true},
        },
    })

    // 插入数据
    adapter.Insert(ctx, "events", []domain.Row{
        {"id": int64(1), "event_type": "click", "timestamp": int64(1700000000), "payload": "{}"},
        {"id": int64(2), "event_type": "view", "timestamp": int64(1700000001), "payload": nil},
    }, nil)

    // 查询
    result, _ := adapter.Query(ctx, "events", &domain.QueryOptions{})
    // result.Rows = [{id:1, event_type:"click", ...}, {id:2, event_type:"view", ...}]
}
```

### 查询示例

```sql
USE analytics;

-- 列出所有表
SHOW TABLES;

-- 创建表
CREATE TABLE metrics (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    value FLOAT,
    recorded_at INT
);

-- 插入数据
INSERT INTO metrics (id, name, value, recorded_at)
VALUES
    (1, 'cpu_usage', 75.5, 1700000000),
    (2, 'mem_usage', 82.3, 1700000001);

-- 聚合查询
SELECT
    name,
    COUNT(*) AS sample_count,
    AVG(value) AS avg_value,
    MAX(value) AS max_value
FROM metrics
GROUP BY name
ORDER BY avg_value DESC;

-- 多表关联
SELECT u.name, COUNT(o.id) AS order_count
FROM users u
JOIN orders o ON u.id = o.user_id
GROUP BY u.name;
```

## 与其他数据源对比

| 特性 | Memory | Parquet | Badger | JSON |
|------|--------|---------|--------|------|
| 持久化 | 否 | 是 (.parquet) | 是 (LSM-Tree) | 是 (.json) |
| 文件格式 | 无 | Apache Parquet | Badger KV | JSON |
| 多表 | 是 | 是 | 是 | 否 |
| DDL | 是 | 是 | 是 | 否 |
| MVCC | 是 | 是 | 基础事务 | 否 |
| 索引类型 | 全部 | 全部 | 主键、唯一 | B-Tree、Hash |
| 全文搜索 | 是 | 是 | 否 | 否 |
| 向量搜索 | 是 | 是 | 否 | 否 |
| WAL | 否 | 是 | 内建 | 否 |
| 压缩 | 无 | Snappy/Gzip/Zstd/LZ4 | Snappy/ZSTD | 无 |
| 互操作性 | 无 | 标准 Parquet 工具 | 仅 Badger | 任何 JSON 工具 |

## 注意事项

- 数据目录在首次连接时如不存在会自动创建。
- 生成的 `.parquet` 文件为标准 Apache Parquet 格式，可被 Apache Arrow、PyArrow、DuckDB、Spark 等工具直接读取。
- 写入操作需要在配置中设置 `writable: true`，否则 INSERT/UPDATE/DELETE/DDL 将返回只读错误。
- 数据在连接时加载到内存，大数据集需注意内存占用。
- WAL 使用 gob 编码，每条记录 `fsync` 保证持久性。
- 不支持嵌套 Parquet 类型（LIST、MAP、STRUCT），请使用扁平列结构。
