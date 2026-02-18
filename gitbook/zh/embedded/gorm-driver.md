# GORM 驱动

SQLExec 提供了完整的 GORM Dialector 实现，可以像操作 MySQL 一样使用 GORM ORM 框架。

## 安装

```bash
go get github.com/kasuganosora/sqlexec
go get gorm.io/gorm
```

## 快速开始

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
    // 1. 创建 SQLExec 数据库
    db, err := api.NewDB(nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 2. 注册内存数据源
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type: domain.DataSourceTypeMemory, Name: "default", Writable: true,
    })
    memDS.Connect(context.Background())
    db.RegisterDataSource("default", memDS)

    // 3. 创建 GORM 连接
    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(db.Session()),
        &gorm.Config{SkipDefaultTransaction: true},
    )
    if err != nil {
        log.Fatal(err)
    }

    // 4. 自动建表
    gormDB.AutoMigrate(&User{})

    // 5. CRUD 操作
    gormDB.Create(&User{Name: "Alice", Age: 30})
    gormDB.Create(&User{Name: "Bob", Age: 25})

    var users []User
    gormDB.Where("age > ?", 20).Order("name").Find(&users)
    for _, u := range users {
        fmt.Printf("%s: %d\n", u.Name, u.Age)
    }
}
```

## API 参考

### NewDialector

```go
func NewDialector(session *api.Session) gorm.Dialector
```

创建 GORM Dialector。传入 SQLExec Session 作为底层连接。

### OpenDB

```go
func OpenDB(session *api.Session) *sql.DB
```

获取标准 `database/sql` 的 `*sql.DB` 对象，用于需要原生 SQL 接口的场景。

### CloseDB

```go
dialector := sqlexecgorm.NewDialector(session)
// ... 使用完毕后
dialector.(*sqlexecgorm.Dialector).CloseDB()
```

## CRUD 操作

### 创建

```go
// 单条
gormDB.Create(&User{Name: "Alice", Age: 30})

// 批量
users := []User{
    {Name: "Bob", Age: 25},
    {Name: "Charlie", Age: 35},
}
gormDB.Create(&users)
```

### 查询

```go
// 查全部
var users []User
gormDB.Find(&users)

// 条件查询
gormDB.Where("age > ? AND name LIKE ?", 20, "A%").Find(&users)

// 单条
var user User
gormDB.First(&user, 1)              // 按主键
gormDB.Where("name = ?", "Alice").First(&user)  // 按条件
```

### 更新

```go
// 单字段
gormDB.Model(&user).Update("age", 31)

// 多字段
gormDB.Model(&user).Updates(User{Name: "Alice2", Age: 31})

// 批量
gormDB.Where("age < ?", 18).Updates(map[string]interface{}{"status": "minor"})
```

### 删除

```go
gormDB.Delete(&user, 1)                          // 按主键
gormDB.Where("age < ?", 18).Delete(&User{})      // 按条件
```

## AutoMigrate

```go
type Product struct {
    ID    uint    `gorm:"primaryKey;autoIncrement"`
    Name  string  `gorm:"size:200;not null"`
    Price float64 `gorm:"default:0"`
    Stock int
}

// 自动创建或更新表结构
gormDB.AutoMigrate(&Product{})
```

支持的 GORM 标签：`primaryKey`、`autoIncrement`、`size`、`not null`、`default`。

## 作为 SQL Mock 使用 / Using as SQL Mock

SQLExec 可以作为 sqlmock 的替代品用于单元测试，优势是执行真实的 SQL 而不是模拟：

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

    // 1. 创建数据库
    db, err := api.NewDB(nil)
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { db.Close() })

    // 2. 配置内存数据源（关键配置！）
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,  // 必须是 memory
        Name:     "default",                    // 必须是 "default"
        Writable: true,                          // 必须是 true
    })

    // 3. 连接数据源（必须！）
    memDS.Connect(context.Background())

    // 4. 注册数据源（必须！）
    db.RegisterDataSource("default", memDS)

    // 5. 创建 GORM 连接
    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(db.Session()),
        &gorm.Config{SkipDefaultTransaction: true},
    )
    if err != nil {
        t.Fatal(err)
    }

    return gormDB
}
```

完整测试指南请参考 [TESTING_WITH_GORM.md](../../docs/TESTING_WITH_GORM.md)。

## 注意事项

| 项目 | 说明 |
|------|------|
| SkipDefaultTransaction | 建议设为 `true`，避免不必要的事务开销 |
| 数据源类型 | 需要使用支持写入的数据源（如 Memory） |
| 数据源名称 | 必须是 `"default"` |
| Writable | 必须设为 `true` 才能执行 INSERT/UPDATE/DELETE |
| Connect() | 必须调用 `memDS.Connect()` 连接数据源 |
| RegisterDataSource | 必须调用 `db.RegisterDataSource()` 注册数据源 |
| 关联操作 | Preload/Association 依赖底层 SQL 支持情况 |
| 类型映射 | GORM 数据类型自动映射为 SQLExec 支持的类型 |
| Bool 类型 | 已支持，TRUE/FALSE 会正确转换为 Go bool |

## 数据类型映射

| GORM 类型 | SQLExec 类型 |
|-----------|-------------|
| bool | BOOLEAN |
| int/int8/.../int64 | BIGINT |
| uint/uint8/.../uint64 | BIGINT UNSIGNED |
| float32/float64 | DOUBLE |
| string | VARCHAR(size) 或 TEXT |
| time.Time | DATETIME |
| []byte | BLOB |
