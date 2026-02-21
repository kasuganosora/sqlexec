# GORM Driver

SQLExec provides a complete GORM Dialector implementation, allowing you to use the GORM ORM framework just like you would with MySQL.

## Installation

```bash
go get github.com/kasuganosora/sqlexec
go get gorm.io/gorm
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/kasuganosora/sqlexec/pkg/api"
    sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "gorm.io/gorm"
)

type User struct {
    ID   uint   `gorm:"primaryKey;autoIncrement"`
    Name string `gorm:"size:100"`
    Age  int
}

func main() {
    // 1. Create SQLExec database
    db, err := api.NewDB(nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 2. Register in-memory data source
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type: domain.DataSourceTypeMemory, Name: "primary", Writable: true,
    })
    if err := memDS.Connect(context.Background()); err != nil {
        log.Fatal(err)
    }
    if err := db.RegisterDataSource("primary", memDS); err != nil {
        log.Fatal(err)
    }

    // 3. Create GORM connection
    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(db.Session()),
        &gorm.Config{SkipDefaultTransaction: true},
    )
    if err != nil {
        log.Fatal(err)
    }

    // 4. Auto-migrate tables
    gormDB.AutoMigrate(&User{})

    // 5. CRUD operations
    gormDB.Create(&User{Name: "Alice", Age: 30})
    gormDB.Create(&User{Name: "Bob", Age: 25})

    var users []User
    gormDB.Where("age > ?", 20).Order("name").Find(&users)
    for _, u := range users {
        fmt.Printf("%s: %d\n", u.Name, u.Age)
    }
}
```

## API Reference

### NewDialector

```go
func NewDialector(session *api.Session) gorm.Dialector
```

Creates a GORM Dialector. Pass in a SQLExec Session as the underlying connection.

### OpenDB

```go
func OpenDB(session *api.Session) *sql.DB
```

Gets a standard `database/sql` `*sql.DB` object for scenarios that require the native SQL interface.

### CloseDB

```go
dialector := sqlexecgorm.NewDialector(session)
// ... when done
dialector.(*sqlexecgorm.Dialector).CloseDB()
```

## CRUD Operations

### Create

```go
// Single record
gormDB.Create(&User{Name: "Alice", Age: 30})

// Batch
users := []User{
    {Name: "Bob", Age: 25},
    {Name: "Charlie", Age: 35},
}
gormDB.Create(&users)
```

### Read

```go
// Find all
var users []User
gormDB.Find(&users)

// Conditional query
gormDB.Where("age > ? AND name LIKE ?", 20, "A%").Find(&users)

// Single record
var user User
gormDB.First(&user, 1)              // By primary key
gormDB.Where("name = ?", "Alice").First(&user)  // By condition
```

### Update

```go
// Single field
gormDB.Model(&user).Update("age", 31)

// Multiple fields
gormDB.Model(&user).Updates(User{Name: "Alice2", Age: 31})

// Batch
gormDB.Where("age < ?", 18).Updates(map[string]interface{}{"status": "minor"})
```

### Delete

```go
gormDB.Delete(&user, 1)                          // By primary key
gormDB.Where("age < ?", 18).Delete(&User{})      // By condition
```

## AutoMigrate

```go
type Product struct {
    ID    uint    `gorm:"primaryKey;autoIncrement"`
    Name  string  `gorm:"size:200;not null"`
    Price float64 `gorm:"default:0"`
    Stock int
}

// Automatically create or update table schema
gormDB.AutoMigrate(&Product{})
```

Supported GORM tags: `primaryKey`, `autoIncrement`, `size`, `not null`, `default`, `unique`.

## Using as SQL Mock for Testing

SQLExec can be used as a sqlmock alternative for unit testing, with the advantage of executing real SQL instead of mocking:

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

func SetupTestDB(t *testing.T) *gorm.DB {
    t.Helper()

    // 1. Create database
    db, err := api.NewDB(nil)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { db.Close() })

    // 2. Configure memory data source (CRITICAL!)
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,  // Must be memory
        Name:     "mydb",                       // Can be any name
        Writable: true,                          // Must be true
    })

    // 3. Connect data source (REQUIRED!)
    if err := memDS.Connect(context.Background()); err != nil {
        t.Fatal(err)
    }

    // 4. Register data source (REQUIRED!)
    if err := db.RegisterDataSource("mydb", memDS); err != nil {
        t.Fatal(err)
    }

    // 5. Set as default (optional, first registered becomes default automatically)
    if err := db.SetDefaultDataSource("mydb"); err != nil {
        t.Fatal(err)
    }

    // 6. Create GORM connection
    session := db.Session()
    t.Cleanup(func() { session.Close() })

    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(session),
        &gorm.Config{SkipDefaultTransaction: true},
    )
    if err != nil {
        t.Fatal(err)
    }

    return gormDB
}
```

For a complete testing guide, see [Embedded Testing Best Practices](testing.md).

## Notes

| Item | Description |
|------|-------------|
| SkipDefaultTransaction | Recommended to set to `true` to avoid unnecessary transaction overhead |
| Data Source Type | Requires a writable data source (e.g., Memory) |
| Data Source Name | Can be any string, recommend using meaningful names (e.g., "mydb", "primary") |
| Default Data Source | First registered data source becomes default automatically, or use `SetDefaultDataSource()` |
| Writable | Must be set to `true` for INSERT/UPDATE/DELETE operations |
| Connect() | Must call `memDS.Connect()` to connect the data source |
| RegisterDataSource | Must call `db.RegisterDataSource()` to register the data source |
| Associations | Preload/Association depends on underlying SQL support |
| Type Mapping | GORM data types are automatically mapped to SQLExec-supported types |
| Bool Type | Fully supported, TRUE/FALSE is correctly converted to Go bool |

## Data Type Mapping

| GORM Type | SQLExec Type |
|-----------|-------------|
| bool | BOOLEAN |
| int/int8/.../int64 | BIGINT |
| uint/uint8/.../uint64 | BIGINT UNSIGNED |
| float32/float64 | DOUBLE |
| string | VARCHAR(size) or TEXT |
| time.Time | DATETIME |
| []byte | BLOB |
