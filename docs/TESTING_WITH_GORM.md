# Using SQLExec as a SQL Mock for GORM Testing

> A zero-dependency, in-memory alternative to sqlmock for GORM unit tests.

---

## Why SQLExec for Testing?

| Feature | sqlmock | SQLExec |
|---------|---------|---------|
| Setup complexity | High (expectations, regex) | Low (just create DB) |
| Real SQL execution | No | Yes |
| Actual data operations | No | Yes (in-memory) |
| Query validation | Manual expectations | Automatic |
| Learning curve | Medium | Low |
| Dependencies | None | None |

SQLExec runs actual SQL queries against an in-memory database, so you get:
- Real query results (no mocking row data)
- Real CRUD operations
- Real transaction semantics
- No network overhead

---

## Quick Start

### Minimal Setup

```go
package myservice_test

import (
    "context"
    "testing"

    "github.com/kasuganosora/sqlexec/pkg/api"
    sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "gorm.io/gorm"
)

// TestHelper sets up an in-memory GORM DB for testing
func SetupTestDB(t *testing.T) *gorm.DB {
    t.Helper()

    // 1. Create SQLExec database
    db, err := api.NewDB(nil)
    if err != nil {
        t.Fatalf("failed to create DB: %v", err)
    }
    t.Cleanup(func() { db.Close() })

    // 2. Create and register memory data source
    // IMPORTANT: Writable must be true for INSERT/UPDATE/DELETE
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "default",  // MUST be "default"
        Writable: true,       // MUST be true
    })

    // 3. Connect the data source (REQUIRED!)
    if err := memDS.Connect(context.Background()); err != nil {
        t.Fatalf("failed to connect data source: %v", err)
    }

    // 4. Register the data source
    db.RegisterDataSource("default", memDS)

    // 5. Create GORM connection
    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(db.Session()),
        &gorm.Config{
            SkipDefaultTransaction: true,  // Recommended for performance
        },
    )
    if err != nil {
        t.Fatalf("failed to open GORM: %v", err)
    }

    return gormDB
}
```

### Complete Test Example

```go
package myservice_test

import (
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    "gorm.io/gorm"
)

type User struct {
    ID        uint      `gorm:"primaryKey;autoIncrement"`
    Name      string    `gorm:"size:100;not null"`
    Email     string    `gorm:"size:255;unique"`
    Age       int       `gorm:"default:0"`
    Active    bool      `gorm:"default:true"`
    CreatedAt time.Time
}

func TestUserRepository_Create(t *testing.T) {
    db := SetupTestDB(t)

    // Auto-migrate creates the table
    err := db.AutoMigrate(&User{})
    assert.NoError(t, err)

    // Test create
    user := &User{Name: "Alice", Email: "alice@example.com", Age: 30}
    result := db.Create(user)

    assert.NoError(t, result.Error)
    assert.Equal(t, int64(1), result.RowsAffected)
    assert.NotZero(t, user.ID)  // Auto-increment ID is set
    assert.True(t, user.Active) // Default value applied
}

func TestUserRepository_FindByEmail(t *testing.T) {
    db := SetupTestDB(t)
    db.AutoMigrate(&User{})

    // Setup test data
    db.Create(&User{Name: "Alice", Email: "alice@example.com"})
    db.Create(&User{Name: "Bob", Email: "bob@example.com"})

    // Test query
    var user User
    err := db.Where("email = ?", "bob@example.com").First(&user).Error

    assert.NoError(t, err)
    assert.Equal(t, "Bob", user.Name)
}

func TestUserRepository_Update(t *testing.T) {
    db := SetupTestDB(t)
    db.AutoMigrate(&User{})

    // Setup
    user := &User{Name: "Alice", Age: 30}
    db.Create(user)

    // Test update
    err := db.Model(user).Update("age", 31).Error
    assert.NoError(t, err)

    // Verify
    var found User
    db.First(&found, user.ID)
    assert.Equal(t, 31, found.Age)
}

func TestUserRepository_Delete(t *testing.T) {
    db := SetupTestDB(t)
    db.AutoMigrate(&User{})

    // Setup
    user := &User{Name: "Alice"}
    db.Create(user)

    // Test delete
    err := db.Delete(user).Error
    assert.NoError(t, err)

    // Verify
    var count int64
    db.Model(&User{}).Count(&count)
    assert.Equal(t, int64(0), count)
}

func TestUserRepository_BoolField(t *testing.T) {
    db := SetupTestDB(t)
    db.AutoMigrate(&User{})

    // IMPORTANT: Bool fields work correctly
    // SQL TRUE/FALSE is properly converted to Go bool
    user := &User{Name: "Alice", Active: true}
    db.Create(user)

    var found User
    db.First(&found, user.ID)

    assert.True(t, found.Active) // bool type, not int64(1)
}
```

---

## Common Pitfalls & Solutions

### Pitfall 1: Missing Data Source Registration

**Error:**
```
Error: table not found
```

**Cause:** Data source not registered.

**Solution:**
```go
// WRONG
db, _ := api.NewDB(nil)
gormDB, _ := gorm.Open(sqlexecgorm.NewDialector(db.Session()), ...)

// CORRECT
db, _ := api.NewDB(nil)
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
    Type:     domain.DataSourceTypeMemory,
    Name:     "default",
    Writable: true,
})
memDS.Connect(context.Background())
db.RegisterDataSource("default", memDS)  // <-- Required!
gormDB, _ := gorm.Open(sqlexecgorm.NewDialector(db.Session()), ...)
```

### Pitfall 2: Writable Not Set

**Error:**
```
Error: data source is read-only, INSERT operation not allowed
```

**Cause:** `Writable` not set to `true`.

**Solution:**
```go
// WRONG
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
    Type: domain.DataSourceTypeMemory,
    Name: "default",
    // Writable is false by default!
})

// CORRECT
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
    Type:     domain.DataSourceTypeMemory,
    Name:     "default",
    Writable: true,  // <-- Required for INSERT/UPDATE/DELETE
})
```

### Pitfall 3: Missing Connect() Call

**Error:**
```
Error: data source not connected
```

**Cause:** `Connect()` not called.

**Solution:**
```go
memDS := memory.NewMVCCDataSource(...)
memDS.Connect(context.Background())  // <-- Required!
db.RegisterDataSource("default", memDS)
```

### Pitfall 4: Wrong Data Source Name

**Error:**
```
Error: no data source found
```

**Cause:** Data source name must be `"default"` for GORM operations.

**Solution:**
```go
// WRONG
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
    Name: "mydb",  // Wrong name
    ...
})

// CORRECT
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
    Name: "default",  // Must be "default"
    ...
})
```

### Pitfall 5: Bool Type Not Working with GORM

**Error:**
```
Error: couldn't convert 0 (float64) into type bool
```

**Cause:** This was a bug in older versions. SQLExec now properly converts `int64(0/1)` to `bool` for BOOLEAN columns.

**Solution:** Update to the latest version of SQLExec. The conversion happens automatically in:
- `pkg/resource/memory/mutation.go` - during INSERT
- `pkg/resource/memory/query.go` - during SELECT

---

## Reusable Test Helper

Create a helper file for your project:

```go
// testutil/sqlexec.go
package testutil

import (
    "context"
    "testing"

    "github.com/kasuganosora/sqlexec/pkg/api"
    sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "gorm.io/gorm"
)

// NewTestGORM creates an in-memory GORM DB for testing.
// It automatically handles cleanup when the test completes.
func NewTestGORM(t *testing.T) *gorm.DB {
    t.Helper()

    db, err := api.NewDB(nil)
    if err != nil {
        t.Fatalf("failed to create sqlexec DB: %v", err)
    }

    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "default",
        Writable: true,
    })

    if err := memDS.Connect(context.Background()); err != nil {
        t.Fatalf("failed to connect data source: %v", err)
    }

    db.RegisterDataSource("default", memDS)

    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(db.Session()),
        &gorm.Config{SkipDefaultTransaction: true},
    )
    if err != nil {
        t.Fatalf("failed to open GORM: %v", err)
    }

    t.Cleanup(func() {
        db.Close()
    })

    return gormDB
}

// MigrateTables is a helper to migrate multiple models.
func MigrateTables(t *testing.T, db *gorm.DB, models ...interface{}) {
    t.Helper()
    if err := db.AutoMigrate(models...); err != nil {
        t.Fatalf("failed to migrate: %v", err)
    }
}
```

Usage:

```go
func TestMyService(t *testing.T) {
    db := testutil.NewTestGORM(t)
    testutil.MigrateTables(t, db, &User{}, &Order{}, &Product{})

    // Your test code here
}
```

---

## Comparison with sqlmock

### sqlmock Approach

```go
// With sqlmock - verbose, mock expectations
func TestWithSqlmock(t *testing.T) {
    db, mock, _ := sqlmock.New()
    defer db.Close()

    mock.ExpectBegin()
    mock.ExpectExec("INSERT INTO `users`").
        WithArgs("Alice", "alice@example.com").
        WillReturnResult(sqlmock.NewResult(1, 1))
    mock.ExpectCommit()

    gormDB, _ := gorm.Open(mysql.New(mysql.Config{
        Conn: db,
        SkipInitializeWithVersion: true,
    }), &gorm.Config{})

    // Test
    user := &User{Name: "Alice", Email: "alice@example.com"}
    err := gormDB.Create(user).Error

    assert.NoError(t, err)
    assert.NoError(t, mock.ExpectationsWereMet())
}
```

### SQLExec Approach

```go
// With SQLExec - simple, real data
func TestWithSQLExec(t *testing.T) {
    gormDB := testutil.NewTestGORM(t)
    gormDB.AutoMigrate(&User{})

    // Test - actual INSERT happens
    user := &User{Name: "Alice", Email: "alice@example.com"}
    err := gormDB.Create(user).Error

    assert.NoError(t, err)
    assert.NotZero(t, user.ID)  // Real auto-increment ID

    // Can query back - real data exists
    var found User
    gormDB.First(&found, user.ID)
    assert.Equal(t, "Alice", found.Name)
}
```

---

## Checklist: Correct Configuration

Before running tests, verify:

- [ ] `api.NewDB(nil)` called to create database
- [ ] `memory.NewMVCCDataSource()` with correct config:
  - [ ] `Type: domain.DataSourceTypeMemory`
  - [ ] `Name: "default"`
  - [ ] `Writable: true`
- [ ] `memDS.Connect(context.Background())` called
- [ ] `db.RegisterDataSource("default", memDS)` called
- [ ] `gorm.Config{SkipDefaultTransaction: true}` set
- [ ] `db.Close()` in cleanup

---

## Full Configuration Reference

```go
// Correct and complete setup
func SetupTestDB() (*gorm.DB, *api.DB) {
    // 1. Create SQLExec DB
    db, err := api.NewDB(nil)
    if err != nil {
        panic(err)
    }

    // 2. Configure memory data source
    config := &domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,  // Required
        Name:     "default",                    // Must be "default"
        Writable: true,                          // Required for DML
    }

    // 3. Create data source
    memDS := memory.NewMVCCDataSource(config)

    // 4. Connect (required!)
    if err := memDS.Connect(context.Background()); err != nil {
        panic(err)
    }

    // 5. Register (required!)
    db.RegisterDataSource("default", memDS)

    // 6. Create GORM dialector
    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(db.Session()),
        &gorm.Config{
            SkipDefaultTransaction: true,  // Recommended
        },
    )
    if err != nil {
        panic(err)
    }

    return gormDB, db
}
```
