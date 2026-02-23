# Parquet Data Source

The Parquet data source provides a fully-featured, persistent columnar storage engine based on the [Apache Parquet](https://parquet.apache.org/) format. It supports multi-table management, full DDL/DML operations, WAL-based crash recovery, periodic flush, and all in-memory features including MVCC transactions and advanced indexes.

Data is organized in a **directory mode**: one directory represents one database, with each table stored as a separate `.parquet` file.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Path to the data directory (one directory = one database) |
| `type` | string | Yes | Fixed value `parquet` |
| `writable` | bool | No | Whether to allow write operations, default `false` |

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `writable` | `false` | Whether to allow write operations |
| `compression` | `snappy` | Compression algorithm: `snappy`, `gzip`, `zstd`, `lz4`, `none` |
| `flush_interval` | `30s` | Periodic flush interval (e.g., `10s`, `1m`, `5m`) |

## Architecture

```
data_directory/
  ├── users.parquet          # Table "users"
  ├── orders.parquet         # Table "orders"
  ├── products.parquet       # Table "products"
  ├── .wal                   # Write-Ahead Log
  └── .sqlexec_meta          # Index metadata (sidecar)
```

- Each `.parquet` file stores one table in native Apache Parquet format.
- The `.wal` file records all write operations for crash recovery.
- The `.sqlexec_meta` file persists index definitions across restarts.

## Features

### DDL Operations

Supports `CREATE TABLE`, `DROP TABLE`, `TRUNCATE TABLE`:

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

### DML Operations

Supports `INSERT`, `UPDATE`, `DELETE`, `SELECT`:

```sql
-- Insert data
INSERT INTO users (id, name, email, score)
VALUES (1, 'Alice', 'alice@example.com', 95.5);

-- Update data
UPDATE users SET score = 98.0 WHERE id = 1;

-- Delete data
DELETE FROM users WHERE score < 60;

-- Query with filtering, sorting, and pagination
SELECT name, score
FROM users
WHERE score >= 80
ORDER BY score DESC
LIMIT 10;
```

### Indexes

Inherits all index types from the Memory engine:

- **B-Tree Index** -- range queries, sorting
- **Hash Index** -- exact match lookups
- **Full-Text Index** -- text search with BM25 scoring
- **Vector Index** -- similarity search (HNSW, IVF)
- **Spatial Index** -- geographic queries (R-Tree)

Indexes are automatically persisted to the `.sqlexec_meta` sidecar file and rebuilt on reconnection.

```sql
CREATE INDEX idx_name ON users (name);
CREATE UNIQUE INDEX idx_email ON users (email);
```

### MVCC & Transactions

Full MVCC support inherited from the Memory engine:

```sql
BEGIN;
INSERT INTO orders (id, user_id, total) VALUES (1, 1, 99.99);
UPDATE users SET score = score + 1 WHERE id = 1;
COMMIT;
```

### WAL & Persistence

All write operations are recorded to a Write-Ahead Log (WAL) before being applied in memory:

1. **WAL write** -- each INSERT/UPDATE/DELETE/DDL is appended to `.wal` with `fsync`.
2. **Periodic flush** -- a background goroutine writes dirty tables to `.parquet` files at the configured interval (default 30s).
3. **Crash recovery** -- on reconnection, the WAL is replayed to reconstruct any unflushed changes.
4. **Checkpoint** -- after a successful flush, the WAL is checkpointed and truncated.

On `Close()`, all dirty tables are flushed and the WAL is cleaned up, ensuring no data loss.

### Data Persistence Across Restarts

```go
// Phase 1: Create and populate
adapter := parquet.NewParquetAdapter(config)
adapter.Connect(ctx)
adapter.CreateTable(ctx, tableInfo)
adapter.Insert(ctx, "users", rows, nil)
adapter.Close(ctx)  // Flush to .parquet files

// Phase 2: Reconnect -- data is still there
adapter2 := parquet.NewParquetAdapter(config)
adapter2.Connect(ctx)  // Reads .parquet files + replays WAL
result, _ := adapter2.Query(ctx, "users", &domain.QueryOptions{})
// result.Rows contains all previously inserted data
```

## Type Mapping

| Go / SQL Type | Parquet Type |
|---------------|-------------|
| `int64`, `bigint` | INT64 |
| `int32`, `int`, `integer` | INT32 |
| `float64`, `double` | DOUBLE |
| `float32`, `float` | FLOAT |
| `bool`, `boolean` | BOOLEAN |
| `string`, `varchar`, `text` | BYTE_ARRAY (UTF8) |
| `bytes`, `blob`, `binary` | BYTE_ARRAY |
| `time`, `datetime`, `timestamp` | INT64 (milliseconds) |

Nullable columns use the Parquet `OPTIONAL` repetition type.

## Configuration Examples

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

### Embedded Usage

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

    // Create table
    adapter.CreateTable(ctx, &domain.TableInfo{
        Name: "events",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "int64", Primary: true},
            {Name: "event_type", Type: "string"},
            {Name: "timestamp", Type: "int64"},
            {Name: "payload", Type: "string", Nullable: true},
        },
    })

    // Insert data
    adapter.Insert(ctx, "events", []domain.Row{
        {"id": int64(1), "event_type": "click", "timestamp": int64(1700000000), "payload": "{}"},
        {"id": int64(2), "event_type": "view", "timestamp": int64(1700000001), "payload": nil},
    }, nil)

    // Query
    result, _ := adapter.Query(ctx, "events", &domain.QueryOptions{})
    // result.Rows = [{id:1, event_type:"click", ...}, {id:2, event_type:"view", ...}]
}
```

### Query Examples

```sql
USE analytics;

-- List all tables
SHOW TABLES;

-- Create a table
CREATE TABLE metrics (
    id INT PRIMARY KEY,
    name VARCHAR NOT NULL,
    value FLOAT,
    recorded_at INT
);

-- Insert data
INSERT INTO metrics (id, name, value, recorded_at)
VALUES
    (1, 'cpu_usage', 75.5, 1700000000),
    (2, 'mem_usage', 82.3, 1700000001);

-- Aggregation query
SELECT
    name,
    COUNT(*) AS sample_count,
    AVG(value) AS avg_value,
    MAX(value) AS max_value
FROM metrics
GROUP BY name
ORDER BY avg_value DESC;

-- Multi-table join
SELECT u.name, COUNT(o.id) AS order_count
FROM users u
JOIN orders o ON u.id = o.user_id
GROUP BY u.name;
```

## Comparison with Other Data Sources

| Feature | Memory | Parquet | Badger | JSON |
|---------|--------|---------|--------|------|
| Persistence | No | Yes (.parquet) | Yes (LSM-Tree) | Yes (.json) |
| File Format | N/A | Apache Parquet | Badger KV | JSON |
| Multi-Table | Yes | Yes | Yes | No |
| DDL | Yes | Yes | Yes | No |
| MVCC | Yes | Yes | Basic | No |
| Index Types | All | All | Primary, Unique | B-Tree, Hash |
| Full-Text Search | Yes | Yes | No | No |
| Vector Search | Yes | Yes | No | No |
| WAL | No | Yes | Built-in | No |
| Compression | N/A | Snappy/Gzip/Zstd/LZ4 | Snappy/ZSTD | N/A |
| Interoperability | N/A | Standard Parquet tools | Badger-only | Any JSON tool |

## Notes

- The data directory is created automatically on first connection if it does not exist.
- `.parquet` files produced are standard Apache Parquet format, readable by tools such as Apache Arrow, PyArrow, DuckDB, and Spark.
- Write operations require `writable: true` in the configuration; otherwise, INSERT/UPDATE/DELETE/DDL will return a read-only error.
- Data is loaded into memory on connection. For very large datasets, monitor memory usage.
- The WAL uses gob encoding with `fsync` per entry for durability.
- Nested Parquet types (LIST, MAP, STRUCT) are not supported; use flat column schemas.
