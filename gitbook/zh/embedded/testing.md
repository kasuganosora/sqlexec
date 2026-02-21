# 测试最佳实践

本文档介绍如何在使用 SQLExec 嵌入式模式时编写隔离性良好的单元测试。

## 测试隔离原则

良好的测试应该满足：
1. **独立性**：每个测试不依赖其他测试的结果
2. **可重复性**：多次运行结果一致
3. **并行安全**：可以与其他测试同时运行

## 方案一：每个测试独立数据源（推荐）

这是最干净的方案，每个测试都有自己独立的数据库实例：

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
    memDS.Connect(context.Background())
    if err := db.RegisterDataSource(dsName, memDS); err != nil {
        t.Fatalf("failed to register datasource: %v", err)
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

### 缺点
- 每个测试都要创建表结构，稍慢
- 如果测试数量很大，可能影响性能

## 方案二：共享 DB + 数据清理（高性能）

当测试数量很大时，可以共享 DB 实例，但每个测试前清理数据：

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
    memDS.Connect(context.Background())
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
- 性能更高，表结构只创建一次
- 适合大量测试场景

### 缺点
- 不能使用 `t.Parallel()` 并行测试
- 需要维护表列表用于清理

## 方案三：使用唯一测试数据

另一种思路是所有测试使用同一个 DB，但通过唯一标识符区分数据：

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
        // 清理本测试的数据
        session.Execute("DELETE FROM users WHERE test_id = ?", uniqueID)
        session.Close()
    })

    return session, uniqueID
}

// 使用示例
func TestWithUniqueData(t *testing.T) {
    session, testID := SetupTestWithUniqueData(t)

    // 使用 testID 区分数据
    session.Execute("INSERT INTO users (id, name, email, test_id) VALUES (?, ?, ?, ?)",
        1, "Alice", "alice@example.com", testID)

    // 只查询本测试的数据
    rows, _ := session.QueryAll("SELECT * FROM users WHERE test_id = ?", testID)
    if len(rows) != 1 {
        t.Errorf("expected 1 row, got %d", len(rows))
    }
}
```

### 优点
- 可以并行运行测试
- 不需要每次创建表结构

### 缺点
- 表结构需要额外的 `test_id` 字段
- 清理逻辑更复杂

## 方案对比

| 方案 | 隔离级别 | 性能 | 并行安全 | 复杂度 | 适用场景 |
|------|----------|------|----------|--------|----------|
| 方案一：独立数据源 | 完全隔离 | 较慢 | 是 | 低 | 少量测试、需要绝对隔离 |
| 方案二：共享+清理 | 数据隔离 | 快 | 否 | 中 | 大量测试、串行执行 |
| 方案三：唯一数据 | 数据隔离 | 中 | 是 | 高 | 需要并行、表结构可控 |

## 推荐选择

1. **测试数量 < 50**：使用方案一（独立数据源），简单可靠
2. **测试数量 > 50**：使用方案二（共享+清理），性能更好
3. **需要并行测试**：使用方案一或方案三

## 常见错误

### 错误1：Session 没有 UseDataSource 方法

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

### 错误2：忘记清理资源

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

### 错误3：测试间数据污染

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
