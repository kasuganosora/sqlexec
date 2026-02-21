# Testing Best Practices

This document explains how to write well-isolated unit tests when using SQLExec in embedded mode.

## Test Isolation Principles

Good tests should be:
1. **Independent**: Each test should not depend on the results of other tests
2. **Repeatable**: Running multiple times produces consistent results
3. **Parallel-safe**: Can run concurrently with other tests

## Approach 1: Independent Data Source per Test (Recommended)

This is the cleanest approach where each test has its own independent database instance:

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
    memDS.Connect(context.Background())
    if err := db.RegisterDataSource(dsName, memDS); err != nil {
        t.Fatalf("failed to register datasource: %v", err)
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

### Disadvantages
- Slightly slower as each test creates table schema
- May impact performance with a large number of tests

## Approach 2: Shared DB + Data Cleanup (High Performance)

When you have a large number of tests, you can share the DB instance but clean data before each test:

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
    memDS.Connect(context.Background())
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
    tables := []string{"orders", "users"} // Note the order - clean tables with foreign key dependencies first
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
- Higher performance, table schema created only once
- Suitable for large test suites

### Disadvantages
- Cannot use `t.Parallel()` for parallel testing
- Need to maintain table list for cleanup

## Approach 3: Using Unique Test Data

Another approach is to use the same DB for all tests but distinguish data by unique identifiers:

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
    testCounter  int64
    counterMu    sync.Mutex
)

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
    memDS.Connect(context.Background())
    if err := db.RegisterDataSource("shared_test", memDS); err != nil {
        panic("failed to register datasource: " + err.Error())
    }

    session := db.Session()
    session.Execute(`
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            test_id INT
        )
    `)
    session.Close()

    return db
}

func getUniqueID() int64 {
    counterMu.Lock()
    defer counterMu.Unlock()
    testCounter++
    return testCounter
}

func SetupTestWithUniqueData(t *testing.T) (*api.Session, int64) {
    sharedDBOnce.Do(func() {
        sharedDB = createSharedDB()
    })

    session := sharedDB.Session()
    uniqueID := getUniqueID()

    t.Cleanup(func() {
        // Clean this test's data
        session.Execute("DELETE FROM users WHERE test_id = ?", uniqueID)
        session.Close()
    })

    return session, uniqueID
}

// Usage example
func TestWithUniqueData(t *testing.T) {
    session, testID := SetupTestWithUniqueData(t)

    // Use testID to distinguish data
    session.Execute("INSERT INTO users (id, name, email, test_id) VALUES (?, ?, ?, ?)",
        1, "Alice", "alice@example.com", testID)

    // Only query this test's data
    rows, _ := session.QueryAll("SELECT * FROM users WHERE test_id = ?", testID)
    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}
```

### Advantages
- Can run tests in parallel
- No need to create table schema each time

### Disadvantages
- Table schema requires extra `test_id` field
- Cleanup logic is more complex

## Comparison

| Approach | Isolation Level | Performance | Parallel-safe | Complexity | Use Case |
|----------|-----------------|-------------|---------------|------------|----------|
| Approach 1: Independent DS | Complete | Slower | Yes | Low | Few tests, need absolute isolation |
| Approach 2: Shared+Cleanup | Data | Fast | No | Medium | Many tests, serial execution |
| Approach 3: Unique Data | Data | Medium | Yes | High | Need parallel, controllable schema |

## Recommendation

1. **Test count < 50**: Use Approach 1 (Independent Data Source), simple and reliable
2. **Test count > 50**: Use Approach 2 (Shared+Cleanup), better performance
3. **Need parallel tests**: Use Approach 1 or Approach 3

## Common Mistakes

### Mistake 1: Session has no UseDataSource method

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

### Mistake 2: Forgetting to cleanup resources

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

### Mistake 3: Data pollution between tests

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
    memDS.Connect(context.Background())
    if err := db.RegisterDataSource(dsName, memDS); err != nil {
        t.Fatalf("failed to register datasource: %v", err)
    }

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
