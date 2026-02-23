# Slice Data Source

The Slice data source wraps Go `[]map[string]any` or `[]struct` slices as SQL-queryable data tables. No files or manual table creation needed — directly expose in-memory Go data structures as a SQL interface.

Suitable for running SQL queries on application runtime data, quickly building test data in unit tests, and intermediate data processing in ETL pipelines.

> For a complete usage guide, see [Embedded Usage - Slice Adapter](../embedded/slice-adapter.md).

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name |
| `type` | string | Yes | Fixed value `slice` |
| `writable` | bool | No | Whether writes are allowed, default `true` |

## Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `data` | `interface{}` | Yes | Source data (`[]map[string]any` or `[]struct`), pointer recommended |
| `table_name` | string | Yes | Table name |
| `database_name` | string | No | Database name, default `"default"` |
| `writable` | bool | No | Whether writable, default `true` (non-pointer data auto-set to `false`) |
| `mvcc_supported` | bool | No | Whether to enable MVCC transactions, default `true` |

## Quick Start

### From Struct Slice

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/slice"

type User struct {
    ID   int    `db:"id"`
    Name string `db:"name"`
    Age  int    `json:"age"`
}

users := []User{
    {ID: 1, Name: "Alice", Age: 30},
    {ID: 2, Name: "Bob", Age: 25},
    {ID: 3, Name: "Charlie", Age: 35},
}

// Create adapter (pass pointer to enable writes and sync)
adapter, err := slice.FromStructSlice(
    &users,
    "users",
    slice.WithWritable(true),
    slice.WithMVCC(true),
)
```

### From Map Slice

```go
data := &[]map[string]any{
    {"id": 1, "name": "Product A", "price": 99.9},
    {"id": 2, "name": "Product B", "price": 199.9},
}

adapter, err := slice.FromMapSlice(data, "products",
    slice.WithWritable(true),
)
```

### Struct Tag Mapping Rules

| Priority | Tag | Example | Description |
|----------|-----|---------|-------------|
| 1 | `db` | `db:"user_name"` | Highest priority |
| 2 | `json` | `json:"userName"` | Second priority |
| 3 | Field name | `Name` | Default: use field name |
| - | `db:"-"` | `db:"-"` | Skip this field |

### Type Mapping

| Go Type | SQL Type |
|---------|----------|
| `int`, `int64`, etc. | `INT` |
| `float32`, `float64` | `FLOAT` |
| `bool` | `BOOLEAN` |
| `string` | `TEXT` |
| `time.Time` | `DATETIME` |
| `[]byte` | `BLOB` |

## Register and Query

```go
import "github.com/kasuganosora/sqlexec/pkg/api"

db, _ := api.NewDB(nil)
db.RegisterDataSource("hr", adapter)

session := db.Session()
defer session.Close()
session.Execute("USE hr")

// Standard SQL queries
rows, _ := session.QueryAll("SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC")
for _, row := range rows {
    fmt.Printf("%s: %v\n", row["name"], row["age"])
}
```

## Write and Sync

When `WithWritable(true)` and pointer data is provided, SQL writes are supported:

```go
// SQL write operations
session.Execute("INSERT INTO users (id, name, age) VALUES (4, 'Diana', 28)")
session.Execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
session.Execute("DELETE FROM users WHERE id = 4")

// Sync changes back to original Go slice
adapter.SyncToOriginal()
fmt.Println(users[0].Age) // 31 (modified by UPDATE)
```

When external code modifies the original slice, call `Reload()` to refresh the in-memory table:

```go
users = append(users, User{ID: 5, Name: "Eve", Age: 22})
adapter.Reload()
```

## Factory Creation

The Slice data source also supports creation via the Factory pattern:

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/slice"

factory := slice.NewFactory()
ds, err := factory.Create(&domain.DataSourceConfig{
    Type:     "slice",
    Name:     "mydata",
    Writable: true,
    Options: map[string]interface{}{
        "data":          &mySlice,
        "table_name":    "records",
        "database_name": "app",
    },
})
```

## Notes

- To enable write operations and `SyncToOriginal()`, you must pass a pointer (e.g., `&users`). Non-pointer data is automatically set to read-only.
- Data is loaded into the underlying Memory engine once when the adapter is created; subsequent SQL operations execute in memory.
- `SyncToOriginal()` writes in-memory modifications back to the original Go slice. Without calling this method, the original data remains unchanged.
- `Reload()` reloads data from the original slice into the in-memory table, overwriting any unsynced SQL modifications.
- The Slice data source is built on the Memory engine and supports full SQL features (WHERE, JOIN, GROUP BY, ORDER BY, etc.) and MVCC transactions.
