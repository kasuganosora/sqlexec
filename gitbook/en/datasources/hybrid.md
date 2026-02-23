# Hybrid Data Source

The Hybrid data source combines the Memory engine with Badger KV persistent storage, enabling **per-table persistence configuration**. By default, all tables run in memory (high performance), and tables requiring persistence can be individually configured to write to disk.

Suitable for embedded applications that need both high-performance in-memory queries and selective data persistence.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name |
| `type` | string | Yes | Fixed value `hybrid` |
| `writable` | bool | No | Default `true` |

## Hybrid Storage Options

| Option | Default | Description |
|--------|---------|-------------|
| `data_dir` | `""` | Badger persistent data directory |
| `default_persistent` | `false` | Whether new tables are persistent by default |
| `enable_badger` | `true` | Whether to enable the Badger backend |

### Cache Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `cache.enabled` | `true` | Whether to enable memory cache for persistent tables |
| `cache.max_size_mb` | `256` | Maximum cache memory (MB) |
| `cache.eviction_policy` | `lru` | Eviction policy: `lru`, `lfu` |

## Configuration Examples

### Embedded Mode

```go
package main

import (
    "context"
    "fmt"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/hybrid"
)

func main() {
    // Create Hybrid data source
    ds := hybrid.NewHybridDataSource(
        &domain.DataSourceConfig{
            Type:     "hybrid",
            Name:     "mydb",
            Writable: true,
        },
        &hybrid.HybridDataSourceConfig{
            DataDir:           "./data",
            DefaultPersistent: false,   // New tables default to memory-only
            EnableBadger:      true,
        },
    )

    ctx := context.Background()
    ds.Connect(ctx)
    defer ds.Close(ctx)

    // Register with DB
    db, _ := api.NewDB(nil)
    db.RegisterDataSource("mydb", ds)

    session := db.Session()
    defer session.Close()
    session.Execute("USE mydb")

    // Create a regular memory table (not persistent)
    session.Execute("CREATE TABLE cache_data (id INT, value TEXT)")

    // Enable persistence for a specific table
    ds.EnablePersistence(ctx, "important_data",
        hybrid.WithSyncOnWrite(true),    // Sync to disk on each write
        hybrid.WithCacheInMemory(true),  // Keep an in-memory cache
    )

    fmt.Println("Hybrid data source is ready")
}
```

## How It Works

### Routing Architecture

```
SQL Operation
    ↓
HybridDataSource (Router)
    ↓
┌─────────────────┬──────────────────┐
│  Memory Tables  │ Persistent Tables │
│  Memory Engine  │  Badger Backend   │
│  (default)      │  (per-table)      │
└─────────────────┴──────────────────┘
```

- The **router** decides which backend to use based on each table's persistence configuration.
- By default, all tables use the Memory engine (fast queries, MVCC transactions).
- Use `EnablePersistence()` to switch a specific table to the Badger backend.
- Supports **dual-write mode**: writes to both memory and disk, reads from memory (maximum performance + persistence guarantee).

### Read/Write Routing Rules

| Table Config | Read | Write |
|-------------|------|-------|
| Memory-only (default) | Memory | Memory |
| Persistent | Badger | Badger |
| Dual-write (CacheInMemory) | Memory (preferred) | Memory + Badger |

## Persistence Control

### Enable Persistence

```go
// Basic enable
ds.EnablePersistence(ctx, "orders")

// Enable with options
ds.EnablePersistence(ctx, "orders",
    hybrid.WithSyncOnWrite(true),    // Sync writes to disk
    hybrid.WithCacheInMemory(true),  // Keep memory cache
)
```

### Disable Persistence

```go
// Data remains in memory after disabling
ds.DisablePersistence(ctx, "orders")
```

### Query Persistence Status

```go
config, _ := ds.GetPersistenceConfig("orders")
if config.Persistent {
    fmt.Printf("Table %s is persistent\n", config.TableName)
}

// List all persistent tables
tables := ds.ListPersistentTables()
```

## Data Migration

### Memory to Disk

Migrate an existing memory table to Badger persistent storage:

```go
// Migration process:
// 1. Read schema and data from memory table
// 2. Create table in Badger and write data
// 3. Update routing configuration
// 4. Drop original table from memory
err := ds.MigrateToPersistent(ctx, "orders")
```

### Disk to Memory

Load a persistent table into memory for better query performance:

```go
// Loading process:
// 1. Read schema and data from Badger
// 2. Create table in memory and write data
// 3. Update routing to memory-only
err := ds.LoadToMemory(ctx, "orders")
```

## Transaction Support

The Hybrid data source supports transactions that coordinate both Memory and Badger backends:

```go
txn, err := ds.BeginTransaction(ctx, nil)
if err != nil {
    panic(err)
}

// Execute operations within the transaction
txn.Insert(ctx, "orders", rows, nil)
txn.Update(ctx, "orders", filters, updates, nil)

// Commit (Memory first, then sync to Badger)
err = txn.Commit(ctx)
// Or rollback
// err = txn.Rollback(ctx)
```

## Statistics

```go
stats := ds.Stats()
fmt.Printf("Total reads: %d (Memory: %d, Badger: %d)\n",
    stats.TotalReads, stats.MemoryReads, stats.BadgerReads)
fmt.Printf("Total writes: %d (Memory: %d, Badger: %d)\n",
    stats.TotalWrites, stats.MemoryWrites, stats.BadgerWrites)
```

## Comparison with Other Persistence Options

| Feature | Hybrid | XML Persistence | Pure Badger |
|---------|--------|----------------|-------------|
| Persistence Granularity | Per-table config | Per-table (ENGINE=xml) | All tables |
| In-Memory Queries | Supported (routed to Memory) | Supported (memory engine) | Not supported |
| Advanced SQL | Full support | Full support | Basic operations |
| MVCC Transactions | Supported | Supported | Basic transactions |
| Configuration | Go API | SQL (ENGINE=xml) | Go API |
| Runtime Switching | Supported (Enable/Disable) | Not supported | Not supported |
| Data Migration | Built-in (Migrate/Load) | Not supported | Not supported |
| Use Case | Embedded applications | Standalone server / embedded | Pure persistence |

## Notes

- The Hybrid data source currently supports embedded mode only (Go API); it cannot be configured via `datasources.json`.
- `EnablePersistence` and `DisablePersistence` only change routing configuration; they do not automatically migrate data. Use `MigrateToPersistent` or `LoadToMemory` for data migration.
- In dual-write mode, if the Badger write fails, the Memory write is not rolled back (eventual consistency).
- Table persistence configurations are stored in Badger itself and automatically restored on restart.
- Memory tables support all SQL features (MVCC, indexes, vector search, etc.); persistent tables support basic CRUD only.
