# Testing Best Practices

This document explains how to write well-isolated unit tests when using SQLExec in embedded mode.

## Test Isolation Principles

Good tests should be:
1. **Independent**: Each test should not depend on the results of other tests
2. **Repeatable**: Running multiple times produces consistent results
3. **Parallel-safe**: Can run concurrently with other tests

## Approach 1: Independent Data Source per Test (Recommended)

This is the cleanest approach where each test has its own independent database instance.
The memory data source has negligible overhead for creating tables (microseconds),
so this approach scales well even with many tests:

```go
package mypackage_test

import (
    "context"
    "testing"

    "github.com/google/uuid"
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// SetupTestDB creates a completely independent test database
func SetupTestDB(t *testing.T) *api.DB {
    // Use unique name to ensure complete isolation
    dsName := "test_" + uuid.New().String()[:8]

    db, err := api.NewDB(nil) // nil uses default config
    if err != nil {
        t.Fatalf("failed to create DB: %v", err)
    }
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     dsName,
        Writable: true,
    })
    if err := memDS.Connect(context.Background()); err != nil {
        t.Fatalf("failed to connect datasource: %v", err)
    }
    if err := db.RegisterDataSource(dsName, memDS); err != nil {
        t.Fatalf("failed to register datasource: %v", err)
    }
    // Set default data source so session can find it
    if err := db.SetDefaultDataSource(dsName); err != nil {
        t.Fatalf("failed to set default datasource: %v", err)
    }

    // Initialize schema
    initTestSchema(t, db)

    // Auto cleanup
    t.Cleanup(func() {
        db.Close()
    })

    return db
}

func initTestSchema(t *testing.T, db *api.DB) {
    session := db.Session()
    defer session.Close()

    _, err := session.Execute(`
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100)
        )
    `)
    if err != nil {
        t.Fatalf("failed to create table: %v", err)
    }
}

// Usage example
func TestUserOperations(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    defer session.Close()

    // Insert test data
    session.Execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")

    // Test query
    rows, err := session.QueryAll("SELECT * FROM users WHERE id = 1")
    if err != nil {
        t.Fatal(err)
    }

    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}

// Parallel test example
func TestParallel(t *testing.T) {
    tests := []struct {
        name string
        id   int
    }{
        {"case1", 1},
        {"case2", 2},
        {"case3", 3},
    }

    for _, tt := range tests {
        tt := tt // Capture loop variable
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel() // Safe to run in parallel

            db := SetupTestDB(t)
            session := db.Session()
            defer session.Close()

            // Each test has its own database
            session.Execute("INSERT INTO users VALUES (?, ?, ?)",
                tt.id, "User"+tt.name, "user"+tt.name+"@example.com")
        })
    }
}
```

### Advantages
- Complete isolation, tests don't affect each other
- Safe to run tests in parallel
- Simple implementation, easy to understand
- Negligible overhead with memory data source

### Disadvantages
- Each test creates table schema (but overhead is negligible for memory data sources)

## Approach 2: Shared DB + Data Cleanup

When you have a large number of tests with complex schemas, you can share the DB instance
but clean data before each test:

```go
package mypackage_test

import (
    "context"
    "sync"
    "testing"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

var (
    sharedDB     *api.DB
    sharedDBOnce sync.Once
)

// SetupSharedTestDB gets the shared test database
func SetupSharedTestDB(t *testing.T) *api.Session {
    // Create DB only once
    sharedDBOnce.Do(func() {
        sharedDB = createSharedDB()
    })

    session := sharedDB.Session()

    // Clean all table data
    truncateAllTables(t, session)

    t.Cleanup(func() {
        session.Close()
    })

    return session
}

func createSharedDB() *api.DB {
    db, err := api.NewDB(nil)
    if err != nil {
        panic("failed to create DB: " + err.Error())
    }
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "shared_test",
        Writable: true,
    })
    if err := memDS.Connect(context.Background()); err != nil {
        panic("failed to connect: " + err.Error())
    }
    if err := db.RegisterDataSource("shared_test", memDS); err != nil {
        panic("failed to register datasource: " + err.Error())
    }

    // Create all table schemas (executed once)
    session := db.Session()
    session.Execute(`
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100)
        )
    `)
    session.Execute(`
        CREATE TABLE orders (
            id INT PRIMARY KEY,
            user_id INT,
            amount FLOAT
        )
    `)
    session.Close()

    return db
}

func truncateAllTables(t *testing.T, session *api.Session) {
    tables := []string{"orders", "users"} // Note the order - clean tables with FK dependencies first
    for _, table := range tables {
        _, err := session.Execute("TRUNCATE TABLE " + table)
        if err != nil {
            t.Fatalf("failed to truncate %s: %v", table, err)
        }
    }
}

// Usage example
func TestWithSharedDB(t *testing.T) {
    session := SetupSharedTestDB(t)

    // Database is clean, ready to test
    session.Execute("INSERT INTO users VALUES (1, 'Bob', 'bob@example.com')")

    rows, _ := session.QueryAll("SELECT * FROM users")
    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}
```

### Advantages
- Table schema created only once
- Suitable for complex schema scenarios

### Disadvantages
- Cannot use `t.Parallel()` for parallel testing
- Need to maintain table list for cleanup

## Approach 3: GORM Integration Testing

If your project uses SQLExec through GORM, the recommended pattern wraps `api.DB`,
`gorm.DB`, and `api.Session` in a `TestDB` struct for both ORM and raw SQL access:

```go
package testutil

import (
    "context"

    "github.com/google/uuid"
    "github.com/kasuganosora/sqlexec/pkg/api"
    sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "gorm.io/gorm"
)

// TestDB wraps api.DB + gorm.DB + api.Session
type TestDB struct {
    DB      *api.DB
    GormDB  *gorm.DB
    Session *api.Session
}

// NewIsolatedTestDB creates a fully isolated test database (recommended for parallel tests)
func NewIsolatedTestDB(t interface{ Cleanup(func()) }) (*TestDB, error) {
    dsName := "test_" + uuid.New().String()[:8]

    db, err := api.NewDB(nil)
    if err != nil {
        return nil, err
    }

    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     dsName,
        Writable: true,
    })
    if err := memDS.Connect(context.Background()); err != nil {
        db.Close()
        return nil, err
    }
    db.RegisterDataSource(dsName, memDS)
    db.SetDefaultDataSource(dsName) // Required: set default data source

    session := db.Session()

    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(session),
        &gorm.Config{SkipDefaultTransaction: true}, // Recommended: improves performance
    )
    if err != nil {
        session.Close()
        db.Close()
        return nil, err
    }

    testDB := &TestDB{DB: db, GormDB: gormDB, Session: session}

    t.Cleanup(func() {
        testDB.Close()
    })

    return testDB, nil
}

// Close closes the test database
func (t *TestDB) Close() {
    if t.Session != nil {
        t.Session.Close()
    }
    if t.DB != nil {
        t.DB.Close()
    }
}

// AutoMigrate runs auto migration for given models
func (t *TestDB) AutoMigrate(models ...interface{}) error {
    return t.GormDB.AutoMigrate(models...)
}

// TruncateTables clears data from specified tables
func (t *TestDB) TruncateTables(tables ...string) error {
    for _, table := range tables {
        if _, err := t.Session.Execute("TRUNCATE TABLE " + table); err != nil {
            return err
        }
    }
    return nil
}
```

Usage example:

```go
package mypackage_test

import (
    "testing"
    "time"

    "gorm.io/gorm"
)

type User struct {
    ID        uint           `gorm:"primarykey"`
    Name      string         `gorm:"size:100"`
    Email     string         `gorm:"size:100;uniqueIndex"`
    DeletedAt gorm.DeletedAt `gorm:"index"` // Soft delete
    CreatedAt time.Time
}

func TestGORMCreateAndFind(t *testing.T) {
    testDB, err := NewIsolatedTestDB(t)
    if err != nil {
        t.Fatal(err)
    }

    // Auto-create table schema
    testDB.AutoMigrate(&User{})

    // Create record using GORM
    user := &User{Name: "Alice", Email: "alice@example.com"}
    if err := testDB.GormDB.Create(user).Error; err != nil {
        t.Fatal(err)
    }

    // Query using GORM (auto-handles soft delete: WHERE deleted_at IS NULL)
    var found User
    err = testDB.GormDB.Where("email = ?", "alice@example.com").First(&found).Error
    if err != nil {
        t.Fatal(err)
    }

    if found.Name != "Alice" {
        t.Errorf("expected Alice, got %s", found.Name)
    }

    // Raw SQL also works
    rows, _ := testDB.Session.QueryAll("SELECT * FROM users")
    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}
```

### Advantages
- Supports both GORM ORM and raw SQL
- `t.Cleanup` handles automatic cleanup
- Interface parameter `interface{ Cleanup(func()) }` is more flexible than `*testing.T`
- Use `AutoMigrate` to auto-create/sync table schemas

### Key Configuration Notes
- **`SkipDefaultTransaction: true`**: GORM wraps each operation in a transaction by default; disabling this improves performance
- **`SetDefaultDataSource(dsName)`**: Must be called when using unique data source names, otherwise sessions can't find the data source

## Comparison

| Approach | Isolation | Performance | Parallel-safe | Complexity | Use Case |
|----------|-----------|-------------|---------------|------------|----------|
| Approach 1: Independent DS | Complete | Fast | Yes | Low | Default choice for most scenarios |
| Approach 2: Shared+Cleanup | Data | Fast | No | Medium | Complex schemas, serial tests |
| Approach 3: GORM Integration | Complete | Fast | Yes | Medium | Projects using GORM |

## Recommendation

1. **Raw SQL projects**: Use Approach 1 (Independent Data Source), simple and reliable
2. **GORM projects**: Use Approach 3 (GORM Integration), supports both ORM and raw SQL
3. **Complex schema + serial tests**: Use Approach 2 (Shared+Cleanup)

## Common Mistakes

### Mistake 1: Not checking Connect() return value

```go
// Wrong: Ignoring the error return from Connect
memDS.Connect(context.Background())

// Correct: Check the error
if err := memDS.Connect(context.Background()); err != nil {
    t.Fatalf("failed to connect: %v", err)
}
```

### Mistake 2: Using unique data source name without setting default

```go
// Wrong: Using unique name but not setting default data source
dsName := "test_" + uuid.New().String()[:8]
db.RegisterDataSource(dsName, memDS)
session := db.Session() // Session doesn't know which data source to use!

// Correct: Set default data source
db.RegisterDataSource(dsName, memDS)
db.SetDefaultDataSource(dsName)  // Required
session := db.Session()
```

### Mistake 3: Session has no UseDataSource method

```go
// Wrong: Session doesn't have UseDataSource method
session.UseDataSource("another_ds")  // Compile error!

// Correct way 1: Specify when creating session
session := db.SessionWithOptions(&api.SessionOptions{
    DataSourceName: "another_ds",
})

// Correct way 2: Execute USE statement
session.Execute("USE another_ds")
```

### Mistake 4: Forgetting to cleanup resources

```go
// Wrong: No cleanup
func TestBad(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    // No defer session.Close()
}

// Correct: Use Cleanup for automatic cleanup
func TestGood(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    t.Cleanup(func() {
        session.Close()
    })
}
```

### Mistake 5: Data pollution between tests

```go
// Wrong: Shared variable causes pollution
var sharedRows []domain.Row

func Test1(t *testing.T) {
    sharedRows, _ = session.QueryAll("SELECT * FROM users")
}

func Test2(t *testing.T) {
    // Might read Test1's data!
    if len(sharedRows) > 0 { ... }
}

// Correct: Each test gets its own data
func TestGood(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    defer session.Close()

    rows, _ := session.QueryAll("SELECT * FROM users")
}
```

## Complete Test Example

```go
package user_test

import (
    "context"
    "testing"

    "github.com/google/uuid"
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

func setupTestDB(t *testing.T) *api.DB {
    dsName := "test_" + uuid.New().String()[:8]

    db, err := api.NewDB(nil)
    if err != nil {
        t.Fatalf("failed to create DB: %v", err)
    }
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     dsName,
        Writable: true,
    })
    if err := memDS.Connect(context.Background()); err != nil {
        t.Fatalf("failed to connect: %v", err)
    }
    if err := db.RegisterDataSource(dsName, memDS); err != nil {
        t.Fatalf("failed to register datasource: %v", err)
    }
    db.SetDefaultDataSource(dsName)

    session := db.Session()
    session.Execute(`
        CREATE TABLE users (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
    session.Close()

    t.Cleanup(func() {
        db.Close()
    })

    return db
}

func TestCreateUser(t *testing.T) {
    db := setupTestDB(t)
    session := db.Session()
    defer session.Close()

    result, err := session.Execute(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        "Alice", "alice@example.com",
    )
    if err != nil {
        t.Fatalf("failed to create user: %v", err)
    }

    if result.RowsAffected != 1 {
        t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
    }
}

func TestGetUserByEmail(t *testing.T) {
    db := setupTestDB(t)
    session := db.Session()
    defer session.Close()

    // Prepare test data
    session.Execute("INSERT INTO users (name, email) VALUES (?, ?)", "Bob", "bob@example.com")

    // Test query
    row, err := session.QueryOne(
        "SELECT * FROM users WHERE email = ?",
        "bob@example.com",
    )
    if err != nil {
        t.Fatalf("failed to get user: %v", err)
    }

    if row["name"] != "Bob" {
        t.Errorf("expected name Bob, got %v", row["name"])
    }
}

func TestDuplicateEmail(t *testing.T) {
    db := setupTestDB(t)
    session := db.Session()
    defer session.Close()

    // Insert first user
    session.Execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")

    // Try to insert duplicate email, should fail
    _, err := session.Execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice2", "alice@example.com")
    if err == nil {
        t.Error("expected error for duplicate email, got nil")
    }
}
```

## Next Steps

- [DB and Session](db-and-session.md) -- Learn more about DB and Session usage
- [Query and Result](query-and-result.md) -- Query execution and result handling
- [Transaction Management](transactions.md) -- Transaction operations and isolation level control
