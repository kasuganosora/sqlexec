# 测试最佳实践

本文档介绍如何在使用 SQLExec 嵌入式模式时编写隔离性良好的单元测试。

## 测试隔离原则

良好的测试应该满足：
1. **独立性**：每个测试不依赖其他测试的结果
2. **可重复性**：多次运行结果一致
3. **并行安全**：可以与其他测试同时运行

## 方案一：每个测试独立数据源（推荐）

这是最干净的方案，每个测试都有自己独立的数据库实例。内存数据源创建表的开销极小（微秒级），
即使有大量测试也不会成为瓶颈：

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

// SetupTestDB 创建一个完全独立的测试数据库
func SetupTestDB(t *testing.T) *api.DB {
    // 使用唯一名称，确保完全隔离
    dsName := "test_" + uuid.New().String()[:8]

    db, err := api.NewDB(nil) // nil 使用默认配置
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
    // 设置默认数据源，否则 session 找不到数据源
    if err := db.SetDefaultDataSource(dsName); err != nil {
        t.Fatalf("failed to set default datasource: %v", err)
    }

    // 初始化表结构
    initTestSchema(t, db)

    // 自动清理
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

// 使用示例
func TestUserOperations(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    defer session.Close()

    // 插入测试数据
    session.Execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")

    // 测试查询
    rows, err := session.QueryAll("SELECT * FROM users WHERE id = 1")
    if err != nil {
        t.Fatal(err)
    }

    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}

// 并行测试示例
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
        tt := tt // 捕获循环变量
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel() // 可以安全地并行

            db := SetupTestDB(t)
            session := db.Session()
            defer session.Close()

            // 每个测试都有独立的数据库
            session.Execute("INSERT INTO users VALUES (?, ?, ?)",
                tt.id, "User"+tt.name, "user"+tt.name+"@example.com")
        })
    }
}
```

### 优点
- 完全隔离，测试之间互不影响
- 可以安全地并行运行测试
- 实现简单，易于理解
- 内存引擎开销极小，适合绝大多数场景

### 缺点
- 每个测试都要创建表结构（但对于内存数据源，开销可忽略不计）

## 方案二：共享 DB + 数据清理

当测试数量很大且表结构复杂时，可以共享 DB 实例，但每个测试前清理数据：

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

// SetupSharedTestDB 获取共享的测试数据库
func SetupSharedTestDB(t *testing.T) *api.Session {
    // 只创建一次 DB
    sharedDBOnce.Do(func() {
        sharedDB = createSharedDB()
    })

    session := sharedDB.Session()

    // 清理所有表的数据
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

    // 创建所有表结构（只执行一次）
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
    tables := []string{"orders", "users"} // 注意顺序，先清理有外键依赖的表
    for _, table := range tables {
        _, err := session.Execute("TRUNCATE TABLE " + table)
        if err != nil {
            t.Fatalf("failed to truncate %s: %v", table, err)
        }
    }
}

// 使用示例
func TestWithSharedDB(t *testing.T) {
    session := SetupSharedTestDB(t)

    // 数据库已清空，可以开始测试
    session.Execute("INSERT INTO users VALUES (1, 'Bob', 'bob@example.com')")

    rows, _ := session.QueryAll("SELECT * FROM users")
    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}
```

### 优点
- 表结构只创建一次
- 适合表结构非常复杂的场景

### 缺点
- 不能使用 `t.Parallel()` 并行测试
- 需要维护表列表用于清理

## 方案三：GORM 集成测试

如果你的项目通过 GORM 使用 SQLExec，推荐使用 `TestDB` 封装模式，同时持有
`api.DB`、`gorm.DB` 和 `api.Session`，兼顾 ORM 和原始 SQL 两种用法：

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

// TestDB 封装 api.DB + gorm.DB + api.Session
type TestDB struct {
    DB      *api.DB
    GormDB  *gorm.DB
    Session *api.Session
}

// NewIsolatedTestDB 创建完全隔离的测试数据库（推荐用于并行测试）
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
    db.SetDefaultDataSource(dsName) // 关键：设置默认数据源

    session := db.Session()

    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(session),
        &gorm.Config{SkipDefaultTransaction: true}, // 推荐：提升性能
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

// Close 关闭测试数据库
func (t *TestDB) Close() {
    if t.Session != nil {
        t.Session.Close()
    }
    if t.DB != nil {
        t.DB.Close()
    }
}

// AutoMigrate 自动迁移模型
func (t *TestDB) AutoMigrate(models ...interface{}) error {
    return t.GormDB.AutoMigrate(models...)
}

// TruncateTables 清空指定表的数据
func (t *TestDB) TruncateTables(tables ...string) error {
    for _, table := range tables {
        if _, err := t.Session.Execute("TRUNCATE TABLE " + table); err != nil {
            return err
        }
    }
    return nil
}
```

使用示例：

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
    DeletedAt gorm.DeletedAt `gorm:"index"` // 软删除
    CreatedAt time.Time
}

func TestGORMCreateAndFind(t *testing.T) {
    testDB, err := NewIsolatedTestDB(t)
    if err != nil {
        t.Fatal(err)
    }

    // 自动创建表结构
    testDB.AutoMigrate(&User{})

    // 使用 GORM 创建记录
    user := &User{Name: "Alice", Email: "alice@example.com"}
    if err := testDB.GormDB.Create(user).Error; err != nil {
        t.Fatal(err)
    }

    // 使用 GORM 查询（自动处理 soft delete: WHERE deleted_at IS NULL）
    var found User
    err = testDB.GormDB.Where("email = ?", "alice@example.com").First(&found).Error
    if err != nil {
        t.Fatal(err)
    }

    if found.Name != "Alice" {
        t.Errorf("expected Alice, got %s", found.Name)
    }

    // 也可以直接用原始 SQL
    rows, _ := testDB.Session.QueryAll("SELECT * FROM users")
    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}
```

### 优点
- 同时支持 GORM 和原始 SQL
- `t.Cleanup` 自动清理
- 接口参数 `interface{ Cleanup(func()) }` 比 `*testing.T` 更灵活
- 可以使用 `AutoMigrate` 自动创建/同步表结构

### 关键配置说明
- **`SkipDefaultTransaction: true`**：GORM 默认对每个操作开启事务，关闭可显著提升性能
- **`SetDefaultDataSource(dsName)`**：使用唯一数据源名时必须调用，否则 session 找不到数据源

## 方案对比

| 方案 | 隔离级别 | 性能 | 并行安全 | 复杂度 | 适用场景 |
|------|----------|------|----------|--------|----------|
| 方案一：独立数据源 | 完全隔离 | 快 | 是 | 低 | 默认推荐，适合绝大多数场景 |
| 方案二：共享+清理 | 数据隔离 | 快 | 否 | 中 | 表结构极复杂、串行测试 |
| 方案三：GORM 集成 | 完全隔离 | 快 | 是 | 中 | 使用 GORM 的项目 |

## 推荐选择

1. **原始 SQL 项目**：使用方案一（独立数据源），简单可靠
2. **GORM 项目**：使用方案三（GORM 集成），同时支持 ORM 和原始 SQL
3. **复杂 schema + 串行测试**：使用方案二（共享+清理）

## 常见错误

### 错误1：Connect() 返回值未检查

```go
// 错误：忽略了 Connect 的 error 返回值
memDS.Connect(context.Background())

// 正确：检查错误
if err := memDS.Connect(context.Background()); err != nil {
    t.Fatalf("failed to connect: %v", err)
}
```

### 错误2：使用唯一数据源名却没有设置默认数据源

```go
// 错误：使用唯一名称但没有设置默认数据源，session 找不到数据
dsName := "test_" + uuid.New().String()[:8]
db.RegisterDataSource(dsName, memDS)
session := db.Session() // session 不知道使用哪个数据源！

// 正确：设置默认数据源
db.RegisterDataSource(dsName, memDS)
db.SetDefaultDataSource(dsName)  // 必须调用
session := db.Session()
```

### 错误3：Session 没有 UseDataSource 方法

```go
// 错误：Session 没有 UseDataSource 方法
session.UseDataSource("another_ds")  // 编译错误!

// 正确方式1：创建新会话时指定
session := db.SessionWithOptions(&api.SessionOptions{
    DataSourceName: "another_ds",
})

// 正确方式2：执行 USE 语句
session.Execute("USE another_ds")
```

### 错误4：忘记清理资源

```go
// 错误：没有清理
func TestBad(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    // 没有 defer session.Close()
}

// 正确：使用 Cleanup 自动清理
func TestGood(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    t.Cleanup(func() {
        session.Close()
    })
}
```

### 错误5：测试间数据污染

```go
// 错误：共享变量导致污染
var sharedRows []domain.Row

func Test1(t *testing.T) {
    sharedRows, _ = session.QueryAll("SELECT * FROM users")
}

func Test2(t *testing.T) {
    // 可能读到 Test1 的数据！
    if len(sharedRows) > 0 { ... }
}

// 正确：每个测试独立获取数据
func TestGood(t *testing.T) {
    db := SetupTestDB(t)
    session := db.Session()
    defer session.Close()

    rows, _ := session.QueryAll("SELECT * FROM users")
}
```

## 完整测试示例

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

    // 准备测试数据
    session.Execute("INSERT INTO users (name, email) VALUES (?, ?)", "Bob", "bob@example.com")

    // 测试查询
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

    // 插入第一个用户
    session.Execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")

    // 尝试插入相同邮箱，应该失败
    _, err := session.Execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice2", "alice@example.com")
    if err == nil {
        t.Error("expected error for duplicate email, got nil")
    }
}
```

## 下一步

- [DB 与 Session](db-and-session.md) -- 了解 DB 和 Session 的详细用法
- [查询与结果](query-and-result.md) -- 查询执行和结果处理
- [事务管理](transactions.md) -- 事务操作和隔离级别控制
