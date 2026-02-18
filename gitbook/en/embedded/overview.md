# Embedded Usage Overview

SQLExec can not only run as a standalone server but also be integrated directly into your application as an **in-process embedded Go library**. Embedded mode requires no external dependencies or network connections -- all SQL parsing, optimization, and execution happen within the same process.

## Minimal Example

With just 5 lines of core code, you can have a full SQL engine in your Go program:

```go
db, _ := api.NewDB(nil)                                          // 1. Create database instance
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{Writable: true})
memDS.Connect(context.Background())
db.RegisterDataSource("default", memDS)                           // 2. Register data source
session := db.Session()                                           // 3. Create session
session.Execute("CREATE TABLE t (id INT, name VARCHAR(50))")      // 4. Execute DDL
rows, _ := session.QueryAll("SELECT * FROM t")                   // 5. Query data
```

## Embedded vs Standalone Server

| Feature | Embedded Library | Standalone Server |
|---------|-----------------|-------------------|
| Deployment | Go package import, compiled into the application | Separate process, connected via network |
| Network Dependency | No network required | Requires TCP connection |
| Latency | Microsecond-level (in-process function calls) | Millisecond-level (network round-trip) |
| Concurrent Access | Multiple goroutines within the same process | Multiple clients via protocol connections |
| Protocol Support | Direct Go API calls | MySQL Protocol / HTTP REST / MCP |
| Language Support | Go only | Any language supporting the MySQL protocol |
| Resource Isolation | Shares application process resources | Separate process resources |
| Data Persistence | Determined by data source (memory/file) | Determined by data source |

## Typical Use Cases

### Unit Testing

Use an in-memory data source in tests -- no need to start an external database service, tests run fast and in isolation:

```go
func TestUserService(t *testing.T) {
    db, _ := api.NewDB(nil)
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{Writable: true})
    memDS.Connect(context.Background())
    db.RegisterDataSource("test", memDS)
    session := db.Session()
    defer session.Close()

    session.Execute("CREATE TABLE users (id INT, name VARCHAR(100), email VARCHAR(200))")
    session.Execute("INSERT INTO users VALUES (1, 'test_user', 'test@example.com')")

    row, err := session.QueryOne("SELECT * FROM users WHERE id = 1")
    assert.NoError(t, err)
    assert.Equal(t, "test_user", row["name"])
}
```

### CLI Tools

Build command-line data processing tools that require no database installation:

```go
// Load a CSV file as a data source and analyze it directly with SQL
csvDS, _ := csv.NewCSVFactory().Create(&domain.DataSourceConfig{
    Database: "/path/to/sales.csv",
})
csvDS.Connect(context.Background())
db.RegisterDataSource("sales", csvDS)

session.Execute("USE sales")
rows, _ := session.QueryAll(`
    SELECT product, SUM(amount) as total
    FROM csv_data
    GROUP BY product
    ORDER BY total DESC
    LIMIT 10
`)
```

### Local Data Analysis

Execute SQL queries directly against Go data structures:

```go
users := []User{{ID: 1, Name: "Alice", Age: 30}, {ID: 2, Name: "Bob", Age: 25}}
adapter, _ := slice.FromStructSlice(&users, "users", slice.WithWritable(true))
db.RegisterDataSource("users", adapter)

session.Execute("USE users")
rows, _ := session.QueryAll("SELECT name, age FROM users WHERE age > 20 ORDER BY age DESC")
```

### In-App SQL Engine

Provide SQL query capabilities for your application, such as user-defined reports, data filtering, etc.:

```go
// Use SQL as a data query DSL within the application
func (app *App) RunReport(sqlQuery string) ([]map[string]interface{}, error) {
    session := app.db.Session()
    defer session.Close()
    return session.QueryAll(sqlQuery)
}
```

### ETL Pipeline

Use SQL for data transformation in data pipelines:

```go
// Read from a JSON data source, transform via SQL, then write to an in-memory table
session.Execute("USE source_json")
rows, _ := session.QueryAll(`
    SELECT
        UPPER(name) as name,
        COALESCE(email, 'unknown') as email,
        YEAR(created_at) as year
    FROM raw_data
    WHERE status = 'active'
`)
```

## Core Package Structure

| Package Path | Description |
|-------------|-------------|
| `pkg/api` | Core API entry point -- `DB` (database instance), `Session` (session), `Query` (query result), `Result` (execution result), `Transaction` (transaction) |
| `pkg/resource/memory` | In-memory data source implementation with full MVCC multi-version concurrency control |
| `pkg/resource/domain` | Data source interface definitions -- `DataSource`, `Row`, `ColumnInfo`, `TableInfo`, etc. |
| `pkg/resource/slice` | Slice adapter -- use Go `[]struct` or `[]map[string]any` directly as SQL tables |
| `pkg/api/gorm` | GORM ORM driver -- access SQLExec through the standard GORM interface |
| `pkg/parser` | SQL parser |
| `pkg/optimizer` | Query optimizer |

## Next Steps

- [DB and Session](db-and-session.md) -- Database instance and session management
- [Query and Result](query-and-result.md) -- Executing queries and handling results
- [Transaction Management](transactions.md) -- Transaction operations and isolation levels
- [GORM Driver](gorm-driver.md) -- Using SQLExec through GORM ORM
- [Slice Adapter](slice-adapter.md) -- Execute SQL directly on Go data structures
- [Testing Best Practices](testing.md) -- Writing well-isolated unit tests
