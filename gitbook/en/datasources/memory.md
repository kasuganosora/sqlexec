# Memory Data Source

Memory is the default data source for SQLExec. All data is stored in memory with full read/write operations and MVCC transaction support. It is suitable for temporary data processing, testing environments, and scenarios that do not require persistence.

> **New Feature**: Memory data source now supports optional persistence! You can enable disk persistence for specific tables using `ENGINE=PERSISTENT` or the `EnablePersistence` API.

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as the database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `memory` |
| `writable` | bool | No | Always supports read/write, default `true` |

### Hybrid Mode Configuration (NEW)

You can configure the Memory data source with optional Badger-based persistence:

```json
{
  "datasources": [
    {
      "name": "persistent_db",
      "type": "memory",
      "writable": true,
      "persistent": {
        "enabled": true,
        "data_dir": "./data",
        "default_persistent": false
      }
    }
  ]
}
```

### datasources.json Configuration

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

### Embedded Mode Configuration

```go
package main

import (
    "fmt"
    "github.com/mySQLExec/db"
)

func main() {
    // Create MVCC in-memory data source
    ds := db.NewMVCCDataSource()
    ds.Connect()
    defer ds.Close()

    // Create table
    ds.Execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")

    // Insert data
    ds.Execute("INSERT INTO users VALUES (1, 'Zhang San', 'zhangsan@example.com')")
    ds.Execute("INSERT INTO users VALUES (2, 'Li Si', 'lisi@example.com')")

    // Query data
    result, _ := ds.Query("SELECT * FROM users WHERE id = 1")
    fmt.Println(result)
}
```

## Features

The Memory data source supports all SQL features of SQLExec:

### DDL Operations

```sql
-- Create table
CREATE TABLE products (
    id INT,
    name TEXT,
    price FLOAT,
    description TEXT
);

-- Drop table
DROP TABLE products;

-- Truncate table data (preserves table structure)
TRUNCATE TABLE products;
```

### DML Operations

```sql
-- Insert
INSERT INTO products VALUES (1, 'Laptop', 5999.00, 'High-performance business laptop');

-- Query
SELECT * FROM products WHERE price > 1000 ORDER BY price DESC;

-- Update
UPDATE products SET price = 5499.00 WHERE id = 1;

-- Delete
DELETE FROM products WHERE id = 1;
```

### Index Support

The Memory data source supports multiple index types that can significantly improve query performance:

| Index Type | Creation Syntax | Use Case |
|-----------|----------------|----------|
| B-Tree | `CREATE INDEX` | Equality queries, range queries, sorting |
| Hash | `CREATE HASH INDEX` | Equality queries (faster) |
| Fulltext | `CREATE FULLTEXT INDEX` | Full-text search |
| Vector | `CREATE VECTOR INDEX` | Vector similarity search |

```sql
-- Create B-Tree index
CREATE INDEX idx_name ON products (name);

-- Create Hash index
CREATE HASH INDEX idx_id ON products (id);

-- Create fulltext index
CREATE FULLTEXT INDEX idx_desc ON products (description);

-- Create vector index
CREATE VECTOR INDEX idx_embedding ON documents (embedding);
```

### Transaction Support

The Memory data source implements transaction isolation based on MVCC (Multi-Version Concurrency Control):

```sql
-- Begin transaction
BEGIN;

INSERT INTO accounts VALUES (1, 'Savings Account', 10000.00);
UPDATE accounts SET balance = balance - 500 WHERE id = 1;

-- Commit transaction
COMMIT;

-- Or rollback transaction
-- ROLLBACK;
```

## Table-Level Persistence (NEW)

You can enable disk persistence for specific tables using SQL:

```sql
-- Create a persistent table that will survive restarts
CREATE TABLE important_data (
    id INT PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP
) ENGINE = PERSISTENT;

-- The table is now persisted to disk
INSERT INTO important_data VALUES (1, 'Critical information', NOW());

-- This data will survive application restarts
```

Or using the Go API:

```go
// Enable persistence for a table
ds.EnablePersistence(ctx, "important_data")

// Configure persistence options
ds.EnablePersistence(ctx, "cached_data",
    WithSyncOnWrite(true),      // Sync to disk on every write
    WithCacheInMemory(true),    // Keep a copy in memory for fast reads
)

// Disable persistence (data remains in memory)
ds.DisablePersistence(ctx, "temp_data")

// Check persistence status
config, exists := ds.GetPersistenceConfig("important_data")
if exists && config.Persistent {
    fmt.Printf("Table %s is persisted to disk\n", config.TableName)
}
```

### Migration

You can migrate tables between memory and disk:

```go
// Migrate memory table to persistent storage
ds.MigrateToPersistent(ctx, "existing_table")

// Load persistent table into memory
ds.LoadToMemory(ctx, "large_table")

// Sync changes between memory and disk
ds.SyncTable(ctx, "important_data")
```

## Notes

- By default, all data is stored in memory and will be lost when the process exits.
- Use `ENGINE=PERSISTENT` or `EnablePersistence()` for critical data that needs to survive restarts.
- The `CacheInMemory` option provides a hybrid mode: data is persisted to disk but also cached in memory for fast reads.
- Suitable for scenarios with smaller data volumes; use MySQL or PostgreSQL for large datasets.
- Writable by default; no additional configuration needed.
- MVCC transactions support concurrent reads and writes; read operations do not block write operations.
