# DB and Session

`DB` is the core entry point for embedded usage of SQLExec, responsible for managing data sources and creating sessions. `Session` represents a database session (similar to a MySQL connection) and provides query, execution, and transaction operations.

## Import

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)
```

## Creating a DB Instance

Use `api.NewDB(config)` to create a database instance:

```go
// Using default configuration
db, err := api.NewDB(nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

```go
// Using custom configuration
db, err := api.NewDB(&api.DBConfig{
    CacheEnabled:          true,
    CacheSize:             2000,
    CacheTTL:              600,  // seconds
    DefaultLogger:         api.NewDefaultLogger(api.LogDebug),
    DebugMode:             true,
    QueryTimeout:          30 * time.Second,
    UseEnhancedOptimizer:  true,
})
```

### DBConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `CacheEnabled` | `bool` | `true` | Whether to enable query caching |
| `CacheSize` | `int` | `1000` | Maximum number of cache entries |
| `CacheTTL` | `int` | `300` | Cache expiration time (seconds) |
| `DefaultLogger` | `Logger` | `LogInfo` level | Logger implementation |
| `DebugMode` | `bool` | `false` | Debug mode |
| `QueryTimeout` | `time.Duration` | `0` (unlimited) | Global query timeout |
| `UseEnhancedOptimizer` | `bool` | `true` | Whether to use the enhanced query optimizer |

## Data Source Management

The DB instance manages different data sources through data source management methods for registering, querying, and managing them.

### RegisterDataSource -- Register a Data Source

```go
// Create an in-memory data source
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
    Type:     domain.DataSourceTypeMemory,
    Name:     "default",
    Writable: true,
})
memDS.Connect(context.Background())

// Register with DB
err := db.RegisterDataSource("default", memDS)
if err != nil {
    log.Fatal(err)
}
```

Note: The first registered data source automatically becomes the default data source.

### SetDefaultDataSource -- Set the Default Data Source

```go
// After registering multiple data sources, switch the default
db.RegisterDataSource("ds1", memDS1)
db.RegisterDataSource("ds2", memDS2)

err := db.SetDefaultDataSource("ds2")
```

### GetDataSource -- Get a Data Source

```go
ds, err := db.GetDataSource("default")
if err != nil {
    log.Printf("Data source not found: %v", err)
}
```

### GetDataSourceNames -- List All Data Sources

```go
names := db.GetDataSourceNames()
for _, name := range names {
    fmt.Println("Data source:", name)
}
```

### Close -- Close and Release Resources

```go
// Close the DB instance and release all data source resources
err := db.Close()
```

`Close()` iterates through all registered data sources and closes them one by one, while also clearing the query cache.

## Creating a Session

### Session -- Using Default Options

```go
session := db.Session()
defer session.Close()
```

`db.Session()` uses the default data source with the following default options:
- Isolation level: `IsolationRepeatableRead`
- Non-read-only mode
- Cache state inherited from DB configuration

### SessionWithOptions -- Using Custom Options

```go
useEnhanced := true
session := db.SessionWithOptions(&api.SessionOptions{
    DataSourceName:       "analytics",
    Isolation:            api.IsolationReadCommitted,
    ReadOnly:             true,
    CacheEnabled:         false,
    QueryTimeout:         10 * time.Second,
    UseEnhancedOptimizer: &useEnhanced,
})
defer session.Close()
```

### SessionOptions Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `DataSourceName` | `string` | Default data source | Name of the data source to use |
| `Isolation` | `IsolationLevel` | `IsolationRepeatableRead` | Transaction isolation level |
| `ReadOnly` | `bool` | `false` | Whether the session is read-only |
| `CacheEnabled` | `bool` | Inherited from DB config | Whether to enable query caching |
| `QueryTimeout` | `time.Duration` | Inherited from DB config | Session-level query timeout, overrides DB global setting |
| `UseEnhancedOptimizer` | `*bool` | `nil` (inherited from DB config) | Whether to use the enhanced optimizer |

## Session Context Settings

Session provides several methods to set session-level context information:

### SetUser -- Set Current User

```go
session.SetUser("admin")
user := session.GetUser() // "admin"
```

### SetTraceID -- Set Trace ID

Used for request tracing and audit logging. The TraceID propagates to the underlying CoreSession:

```go
session.SetTraceID("req-abc-123")
traceID := session.GetTraceID() // "req-abc-123"
```

### SetThreadID -- Set Thread ID

Used for administrative operations such as KILL queries:

```go
session.SetThreadID(42)
threadID := session.GetThreadID() // 42
```

### SetCurrentDB -- Set Current Database

Equivalent to the SQL `USE database_name` statement:

```go
session.SetCurrentDB("analytics")
currentDB := session.GetCurrentDB() // "analytics"
```

### Close -- Close Session

```go
err := session.Close()
```

`Close()` performs the following cleanup operations:
1. Rolls back uncommitted transactions
2. Drops temporary tables created during the session
3. Closes the underlying CoreSession

## Logging

### Logger Interface

```go
type Logger interface {
    Debug(format string, args ...interface{})
    Info(format string, args ...interface{})
    Warn(format string, args ...interface{})
    Error(format string, args ...interface{})
    SetLevel(level LogLevel)
    GetLevel() LogLevel
}
```

### Log Levels

| Constant | Value | Description |
|----------|-------|-------------|
| `api.LogError` | `0` | Errors only |
| `api.LogWarn` | `1` | Warnings and errors |
| `api.LogInfo` | `2` | Info, warnings, and errors |
| `api.LogDebug` | `3` | All levels (including debug) |

### Using Built-in Loggers

```go
// Create default logger (outputs to stdout)
logger := api.NewDefaultLogger(api.LogDebug)

// Create logger with custom output
var buf bytes.Buffer
logger := api.NewDefaultLoggerWithOutput(api.LogInfo, &buf)

// Create no-op logger (disables log output)
logger := api.NewNoOpLogger()
```

### Setting DB Logger

```go
// Set via configuration
db, _ := api.NewDB(&api.DBConfig{
    DefaultLogger: api.NewDefaultLogger(api.LogDebug),
})

// Switch dynamically at runtime
db.SetLogger(api.NewDefaultLogger(api.LogWarn))

// Get current logger
currentLogger := db.GetLogger()
```

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

func main() {
    // 1. Create DB instance
    db, err := api.NewDB(&api.DBConfig{
        CacheEnabled:          true,
        CacheSize:             500,
        CacheTTL:              120,
        DefaultLogger:         api.NewDefaultLogger(api.LogInfo),
        QueryTimeout:          15 * time.Second,
        UseEnhancedOptimizer:  true,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 2. Create and register an in-memory data source
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "main",
        Writable: true,
    })
    memDS.Connect(context.Background())

    if err := db.RegisterDataSource("main", memDS); err != nil {
        log.Fatal(err)
    }

    // 3. List all data sources
    fmt.Println("Registered data sources:", db.GetDataSourceNames())

    // 4. Create a session
    session := db.Session()
    defer session.Close()

    // 5. Set session context
    session.SetUser("app_service")
    session.SetTraceID("trace-001")

    // 6. Execute SQL
    session.Execute("CREATE TABLE products (id INT, name VARCHAR(100), price FLOAT)")
    session.Execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")

    rows, err := session.QueryAll("SELECT * FROM products")
    if err != nil {
        log.Fatal(err)
    }

    for _, row := range rows {
        fmt.Printf("Product: %v - $%v\n", row["name"], row["price"])
    }
}
```

## Next Steps

- [Query and Result](query-and-result.md) -- Learn more about query execution and result handling
- [Transaction Management](transactions.md) -- Transaction operations and isolation level control
