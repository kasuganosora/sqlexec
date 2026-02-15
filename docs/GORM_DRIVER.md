# GORM 驱动

> `pkg/api/gorm/` — 让 GORM 直接在进程内执行 SQL，跳过网络层。

---

## 1. 定位与使用场景

SQLExec 可以作为独立的 MySQL 兼容服务器运行，也可以作为库嵌入到 Go 应用中。当嵌入使用时，应用代码通过 `pkg/api` 包的 `Session.Query()` / `Session.Execute()` 直接发起查询，**不经过 TCP 连接**。

`pkg/api/gorm/` 包在此基础上提供了 GORM 适配层：应用代码可以使用 GORM 的全部 API（模型定义、关联、Scope 等），而底层的 SQL 执行自动路由到 SQLExec 的解析器和优化器，全程在进程内完成。

```
┌─────────────────────────────┐
│  应用代码                     │
│  gormDB.Create(&user)        │
└──────────┬──────────────────┘
           │ GORM 生成 SQL
           ▼
┌─────────────────────────────┐
│  gorm.Dialector (dialect.go) │
│  ConnPool = *sql.DB          │
└──────────┬──────────────────┘
           │ database/sql 标准接口
           ▼
┌─────────────────────────────┐
│  driver.go                   │
│  conn.ExecContext / QueryContext │
└──────────┬──────────────────┘
           │ 调用 Session.Execute / Session.Query
           ▼
┌─────────────────────────────┐
│  api.Session                 │
│  → Parser → Optimizer → Executor │
└─────────────────────────────┘
```

典型使用场景：
- 想用 GORM 的模型/关联/Scope 等生态，但数据源是内存表、CSV、JSON、Parquet 等非传统数据库
- 单元测试中需要一个零依赖的嵌入式 SQL 数据库
- 应用内部需要对结构化数据做 SQL 查询，不想启动额外的数据库进程

---

## 2. 快速上手

### 2.1 最小示例

```go
package main

import (
    "fmt"
    "log"

    "github.com/kasuganosora/sqlexec/pkg/api"
    sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "gorm.io/gorm"
)

type User struct {
    ID   uint   `gorm:"primaryKey"`
    Name string `gorm:"size:100"`
    Age  int
}

func main() {
    // 1. 创建 sqlexec DB 并注册内存数据源
    db, _ := api.NewDB(nil)
    defer db.Close()

    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type: domain.DataSourceTypeMemory, Name: "default", Writable: true,
    })
    db.RegisterDataSource("default", memDS)

    // 2. 创建 GORM DB
    gormDB, err := gorm.Open(
        sqlexecgorm.NewDialector(db.Session()),
        &gorm.Config{SkipDefaultTransaction: true},
    )
    if err != nil {
        log.Fatal(err)
    }

    // 3. 建表
    gormDB.AutoMigrate(&User{})

    // 4. CRUD
    gormDB.Create(&User{Name: "Alice", Age: 30})

    var users []User
    gormDB.Where("age > ?", 18).Find(&users)
    fmt.Printf("found %d users\n", len(users))
}
```

### 2.2 使用其他数据源

GORM 驱动对数据源类型透明 — 只要通过 `api.DB` 注册了数据源，GORM 就能查询它：

```go
// CSV 数据源
csvDS := csv.NewCSVAdapter(config, "data/products.csv")
db.RegisterDataSource("csv", csvDS)

// JSON 数据源
jsonDS := json.NewJSONAdapter(config, "data/users.json")
db.RegisterDataSource("json", jsonDS)

// Slice 数据源（Go 切片直接当表用）
sliceDS, _ := slice.NewFactory().Create(config)
db.RegisterDataSource("slice", sliceDS)
```

### 2.3 混合使用 GORM 与 Session

同一个 Session 可以同时被 GORM 和原生 API 使用：

```go
session := db.Session()
gormDB, _ := gorm.Open(sqlexecgorm.NewDialector(session), &gorm.Config{})

// 简单 CRUD 用 GORM
gormDB.Create(&User{Name: "Bob"})

// 复杂查询用 Session
q, _ := session.Query(`
    SELECT u.name, COUNT(o.id) as order_count
    FROM users u JOIN orders o ON u.id = o.user_id
    GROUP BY u.name HAVING order_count > ?
`, 5)
defer q.Close()
```

---

## 3. 架构原理

### 3.1 核心组件

| 文件 | 组件 | 职责 |
|------|------|------|
| `driver.go` | `connector`, `conn`, `resultRows`, `execResult`, `noopTx` | 实现 `database/sql/driver` 接口，将 SQL 路由到 `api.Session` |
| `dialect.go` | `Dialector` | 实现 `gorm.Dialector` 接口，在 `Initialize` 中创建 ConnPool 并注册回调 |
| `migrator.go` | `Migrator` | 实现 `gorm.Migrator` 接口，将 DDL 操作转为 SQL 并通过 Session 执行 |

### 3.2 database/sql/driver 桥接

GORM 通过 `database/sql` 的标准接口执行 SQL。传统数据库驱动（如 `go-sql-driver/mysql`）在这一层建立 TCP 连接。SQLExec 的驱动则**完全在进程内**：

```
driver.Connector                     ← sql.OpenDB(connector)
  └→ Connect() → driver.Conn        ← 包装 *api.Session
       ├→ QueryContext()             ← 调用 session.Query()
       ├→ ExecContext()              ← 调用 session.Execute()
       └→ Begin() → driver.Tx       ← no-op（MVCC 保证语句原子性）
```

**关键设计决策**：

1. **实现 `QueryerContext` 和 `ExecerContext`**：`database/sql` 检测到这两个接口后会跳过 `Prepare` 路径，减少一次间接调用。
2. **自动路由**：`ExecContext` 检测到 SELECT/SHOW/DESCRIBE 语句时自动转发到 `session.Query()`，兼容 `gormDB.Exec("SELECT ...")` 的用法。
3. **值类型转换**：`driver.Value` 只允许 `nil | int64 | float64 | bool | string | []byte | time.Time` 七种类型。`toDriverValue()` 负责将 SQLExec 内部的 `int`、`int32`、`float32`、`uint` 等类型转换为合法的 `driver.Value`。

### 3.3 Dialector 初始化流程

```go
func (d *Dialector) Initialize(db *gorm.DB) error {
    // 1. 通过自定义 driver.Connector 创建 *sql.DB
    d.sqlDB = sql.OpenDB(&connector{session: d.Session})

    // 2. 将 *sql.DB 设为 GORM 的连接池
    db.ConnPool = d.sqlDB

    // 3. 注册 GORM 默认回调（Create, Query, Update, Delete, Row, Raw）
    callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
}
```

这三步缺一不可：
- 没有步骤 1，GORM 没有 ConnPool，所有 CRUD 操作都会 panic
- 没有步骤 3，GORM 没有注册任何回调，SQL 不会被构建和执行

### 3.4 SQL 执行路径（以 Create 为例）

```
gormDB.Create(&user)
  │
  ├→ [gorm:begin_transaction]   Begin() → noopTx（no-op）
  ├→ [gorm:before_create]       BeforeCreate hook
  ├→ [gorm:create]              构建 INSERT SQL + 调用 ConnPool.ExecContext()
  │     │
  │     ├→ *sql.DB.ExecContext()
  │     ├→ conn.ExecContext(ctx, "INSERT INTO `users` ...", args)
  │     ├→ session.Execute("INSERT INTO `users` ...", args)
  │     ├→ bindParams() → 内联参数
  │     ├→ Parser → Optimizer → Executor
  │     └→ 返回 driver.Result{RowsAffected, LastInsertID}
  │
  ├→ [gorm:after_create]        AfterCreate hook
  └→ [gorm:commit_transaction]  Commit() → noopTx（no-op）
```

### 3.5 事务语义

当前实现使用 **no-op 事务**：`Begin()` 返回一个空的 `driver.Tx`，`Commit()` 和 `Rollback()` 都是 no-op。

这在 SQLExec 中是安全的，因为：
- MVCC 引擎保证每条 DML 语句的原子性
- 建议设置 `gorm.Config{SkipDefaultTransaction: true}` 跳过 GORM 默认的自动事务包装，避免不必要的 Begin/Commit 开销

> **注意**：跨多条语句的事务隔离（如"读 A → 改 B → 提交"保证一致性）需要 SQLExec 的 api.Transaction 完善后才能支持。

---

## 4. Migrator（DDL 迁移）

Migrator 将 GORM 的 DDL 操作转为 SQL 字符串，通过 `Session.Execute()` 执行。它**不经过 database/sql 层**，而是直接调用 Session API。

### 4.1 支持的操作

| 方法 | 实现方式 |
|------|---------|
| `AutoMigrate` | 解析 GORM schema → 生成 CREATE TABLE IF NOT EXISTS → Session.Execute |
| `CreateTable` | 同上 |
| `DropTable` | DROP TABLE IF EXISTS |
| `HasTable` | 查询 information_schema.tables |
| `HasColumn` | 查询 information_schema.columns |
| `AddColumn` | ALTER TABLE ADD COLUMN（从 schema 推导类型） |
| `AlterColumn` | ALTER TABLE MODIFY COLUMN（从 schema 推导类型） |
| `DropColumn` | ALTER TABLE DROP COLUMN |
| `RenameColumn` | ALTER TABLE RENAME COLUMN |
| `CreateIndex` | CREATE INDEX（从 schema 推导列列表） |
| `DropIndex` | DROP INDEX |
| `HasIndex` | 查询 information_schema.statistics |
| `GetTables` | 查询 information_schema.tables |
| `CurrentDatabase` | SELECT DATABASE() |

### 4.2 表名解析

`getTableName()` 按以下优先级解析表名：

1. 如果参数是 `string`，直接返回
2. 如果参数是 `*schema.Schema`，使用 `.Table` 字段
3. 使用 GORM 的 `schema.Parse()` 解析模型结构体（正确应用 NamingStrategy，如 `User` → `users`）
4. 最后回退到类型名的小写形式

### 4.3 列类型映射

`DataTypeOf()` 将 GORM 的 schema 字段类型映射到 SQL 类型：

| GORM DataType | SQL Type |
|---------------|----------|
| `Bool` | `BOOLEAN` |
| `Int/Uint` (size <= 8) | `TINYINT` |
| `Int/Uint` (size <= 16) | `SMALLINT` |
| `Int/Uint` (size <= 32) | `INT` |
| `Int/Uint` (size > 32) | `BIGINT` |
| `Float` (size <= 32) | `FLOAT` |
| `Float` (size > 32) | `DOUBLE` |
| `String` (size > 0) | `VARCHAR(size)` |
| `String` (无 size) | `TEXT` |
| `Time` | `TIMESTAMP` |
| `Bytes` | `BLOB` |
| 其他 | `VARCHAR(255)` |

---

## 5. database/sql 直接使用

GORM 驱动的底层 `database/sql/driver` 也可以直接使用，不需要 GORM：

```go
import sqlexecgorm "github.com/kasuganosora/sqlexec/pkg/api/gorm"

// 方式 1：OpenDB 便捷函数
sqlDB := sqlexecgorm.OpenDB(session)
defer sqlDB.Close()

rows, _ := sqlDB.Query("SELECT * FROM users WHERE age > ?", 18)
defer rows.Close()

// 方式 2：使用 Connector
connector := sqlexecgorm.NewConnector(session)
sqlDB := sql.OpenDB(connector)
```

这使得任何基于 `database/sql` 的 Go 库（如 `sqlx`、`squirrel`）都可以使用 SQLExec 作为后端。

---

## 6. 注意事项与限制

### 6.1 推荐配置

```go
gormDB, _ := gorm.Open(sqlexecgorm.NewDialector(session), &gorm.Config{
    SkipDefaultTransaction: true,  // 跳过自动事务（推荐）
})
```

### 6.2 当前限制

| 限制 | 说明 |
|------|------|
| 事务隔离 | 当前为 no-op 事务，单语句原子但不支持跨语句事务隔离 |
| Preload | GORM 的 Preload/Joins 预加载依赖子查询，复杂关联可能需要手动 JOIN |
| 回调 | GORM 的 Hook（BeforeCreate 等）正常工作，但 GORM 的关联自动创建可能有限 |
| ColumnTypes | `Migrator.ColumnTypes()` 返回空，不影响普通使用 |
| CreateConstraint | 外键约束不支持（SQLExec 内存引擎不支持外键） |

### 6.3 性能

由于跳过了网络层（TCP 连接、协议编解码、序列化/反序列化），嵌入式使用的查询延迟显著低于网络访问：

- 网络模式：SQL → MySQL 协议编码 → TCP → 协议解码 → Parser → Optimizer → Executor → 协议编码 → TCP → 协议解码
- 嵌入模式：SQL → Parser → Optimizer → Executor

`database/sql/driver` 层的开销极小 — `conn.QueryContext` 和 `conn.ExecContext` 只做参数类型转换后直接调用 Session 方法。
