# Badger KV Persistent Storage

The Badger data source is built on the [Badger](https://github.com/dgraph-io/badger) embedded KV database, providing disk-persistent data storage. Data is stored locally in an LSM-Tree structure and survives process restarts.

The Badger data source is typically not used directly. Instead, it serves as the backend storage for the [Hybrid Data Source](hybrid.md), which manages it automatically. For direct use, it is suitable for embedded scenarios requiring pure persistence without memory caching.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `badger` |
| `writable` | bool | No | Always supports read/write, default `true` |

## Storage Options

Configure storage parameters via the `options` field:

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `""` | Data file storage directory; empty uses in-memory mode |
| `in_memory` | `false` | Whether to use pure in-memory mode (no disk writes) |
| `sync_writes` | `false` | Whether to sync each write to disk (safer but slower) |
| `value_threshold` | `1024` | Values exceeding this size (bytes) are stored in Value Log |
| `num_memtables` | `5` | Number of in-memory tables |
| `base_table_size` | `2097152` | LSM base table size (2MB) |
| `compression` | `1` | Compression algorithm: `0`=None, `1`=Snappy, `2`=ZSTD |

## Configuration Examples

### Embedded Mode

```go
package main

import (
    "context"
    "fmt"

    "github.com/kasuganosora/sqlexec/pkg/resource/badger"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func main() {
    // Create Badger data source
    ds := badger.NewBadgerDataSource(&domain.DataSourceConfig{
        Type:     "badger",
        Name:     "persistent",
        Writable: true,
        Options: map[string]interface{}{
            "data_dir": "./badger_data",
        },
    })

    ctx := context.Background()
    ds.Connect(ctx)
    defer ds.Close(ctx)

    // Create table
    ds.CreateTable(ctx, &domain.TableInfo{
        Name: "users",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "INT", Primary: true, AutoIncrement: true},
            {Name: "name", Type: "VARCHAR", Nullable: false},
            {Name: "email", Type: "VARCHAR", Nullable: true},
        },
    })

    // Insert data
    ds.Insert(ctx, "users", []domain.Row{
        {"name": "Alice", "email": "alice@example.com"},
        {"name": "Bob", "email": "bob@example.com"},
    }, nil)

    // Query data
    result, _ := ds.Query(ctx, "users", &domain.QueryOptions{})
    fmt.Printf("Total: %d records\n", result.Total)
}
```

### Custom Storage Configuration

```go
cfg := badger.DefaultDataSourceConfig("./data")
cfg.SyncWrites = true    // Sync every write to disk
cfg.Compression = 2      // Use ZSTD compression

ds := badger.NewBadgerDataSourceWithConfig(domainCfg, cfg)
```

## Features

### DDL Operations

Supports `CreateTable`, `DropTable`, `TruncateTable`:

- Auto-increment sequences are initialized when creating tables.
- `TRUNCATE` clears all rows and indexes, and resets auto-increment sequences.
- `DROP TABLE` removes table metadata, all row data, and indexes.

### DML Operations

Supports `Insert`, `Query`, `Update`, `Delete`:

- Queries support filter conditions (`=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `IN`).
- Queries support `ORDER BY` sorting and `LIMIT` / `OFFSET` pagination.
- Supports `AND` / `OR` combined filter conditions.
- Auto-increment primary keys are generated automatically on insert.

### Indexes

- Indexes are automatically maintained for `PRIMARY KEY` and `UNIQUE` columns.
- Indexes are persisted alongside row data in Badger.

### Data Persistence

- Data is stored as key-value pairs in Badger's LSM-Tree.
- Supports Snappy and ZSTD compression.
- Supports data encryption (via `EncryptionKey` configuration).
- Table metadata and data are automatically loaded on process restart.

## Comparison with Memory Data Source

| Feature | Memory | Badger |
|---------|--------|--------|
| Persistence | No (lost on exit) | Yes (disk storage) |
| Query Performance | Extremely fast (pure memory) | Fast (LSM + cache) |
| MVCC Transactions | Full support | Basic transactions |
| Index Types | B-Tree, Hash, Fulltext, Vector | Primary, Unique |
| Full-Text Search | Supported | Not supported |
| Vector Search | Supported | Not supported |
| SQL Execution | Full SQL | Via upper-layer routing |
| Use Case | Temporary data, high-performance queries | Data requiring persistence |

## Notes

- The Badger data source does not support direct raw SQL execution (`Execute` method); queries must use interface methods like `Query`.
- For both in-memory query performance and disk persistence, use the [Hybrid Data Source](hybrid.md).
- When `data_dir` is empty, it automatically enters in-memory mode, behaving similarly to Memory but without MVCC and advanced indexes.
- For production environments, enable `sync_writes` to ensure data safety.
- After heavy write workloads, consider running Badger GC periodically via the maintenance API.
