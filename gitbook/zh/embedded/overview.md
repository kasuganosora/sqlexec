# 嵌入式使用概述

SQLExec 不仅可以作为独立服务器运行，还可以作为 **进程内嵌入式 Go 库** 直接集成到你的应用程序中。嵌入式模式无需任何外部依赖，无需网络连接，所有 SQL 解析、优化和执行都在同一进程内完成。

## 最小示例

只需 5 行核心代码，即可在 Go 程序中拥有完整的 SQL 引擎：

```go
db, _ := api.NewDB(nil)                                          // 1. 创建数据库实例
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{Writable: true})
memDS.Connect(context.Background())
db.RegisterDataSource("default", memDS)                           // 2. 注册数据源
session := db.Session()                                           // 3. 创建会话
session.Execute("CREATE TABLE t (id INT, name VARCHAR(50))")      // 4. 执行 DDL
rows, _ := session.QueryAll("SELECT * FROM t")                   // 5. 查询数据
```

## 嵌入式 vs 独立服务器

| 特性 | 嵌入式库 | 独立服务器 |
|------|---------|-----------|
| 部署方式 | Go 包导入，编译到应用中 | 独立进程，通过网络连接 |
| 网络依赖 | 无需网络 | 需要 TCP 连接 |
| 延迟 | 微秒级（进程内函数调用） | 毫秒级（网络往返） |
| 并发访问 | 同进程多 goroutine | 多客户端通过协议连接 |
| 协议支持 | Go API 直接调用 | MySQL 协议 / HTTP REST / MCP |
| 适用语言 | 仅 Go | 任意支持 MySQL 协议的语言 |
| 资源隔离 | 共享应用进程资源 | 独立进程资源 |
| 数据持久化 | 由数据源决定（内存/文件） | 由数据源决定 |

## 典型使用场景

### 单元测试

在测试中使用内存数据源，无需启动外部数据库服务，测试运行快速且隔离：

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

### CLI 工具

构建无需安装数据库的命令行数据处理工具：

```go
// 将 CSV 文件加载为数据源，直接用 SQL 分析
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

### 本地数据分析

直接对 Go 数据结构执行 SQL 查询：

```go
users := []User{{ID: 1, Name: "Alice", Age: 30}, {ID: 2, Name: "Bob", Age: 25}}
adapter, _ := slice.FromStructSlice(&users, "users", slice.WithWritable(true))
db.RegisterDataSource("users", adapter)

session.Execute("USE users")
rows, _ := session.QueryAll("SELECT name, age FROM users WHERE age > 20 ORDER BY age DESC")
```

### 应用内 SQL 引擎

为应用程序提供 SQL 查询能力，例如用户自定义报表、数据过滤等：

```go
// 应用内部使用 SQL 作为数据查询 DSL
func (app *App) RunReport(sqlQuery string) ([]map[string]interface{}, error) {
    session := app.db.Session()
    defer session.Close()
    return session.QueryAll(sqlQuery)
}
```

### ETL 管道

在数据管道中使用 SQL 进行数据转换：

```go
// 从 JSON 数据源读取，通过 SQL 转换后写入内存表
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

## 核心包结构

| 包路径 | 说明 |
|-------|------|
| `pkg/api` | 核心 API 入口 -- `DB`（数据库实例）、`Session`（会话）、`Query`（查询结果）、`Result`（执行结果）、`Transaction`（事务） |
| `pkg/resource/memory` | 内存数据源实现，支持完整的 MVCC 多版本并发控制 |
| `pkg/resource/domain` | 数据源接口定义 -- `DataSource`、`Row`、`ColumnInfo`、`TableInfo` 等 |
| `pkg/resource/slice` | Slice 适配器 -- 将 Go 的 `[]struct` 或 `[]map[string]any` 直接作为 SQL 表 |
| `pkg/api/gorm` | GORM ORM 驱动 -- 通过标准 GORM 接口访问 SQLExec |
| `pkg/parser` | SQL 解析器 |
| `pkg/optimizer` | 查询优化器 |

## 下一步

- [DB 与 Session](db-and-session.md) -- 数据库实例和会话管理
- [查询与结果](query-and-result.md) -- 执行查询和处理结果
- [事务管理](transactions.md) -- 事务操作和隔离级别
- [GORM 驱动](gorm-driver.md) -- 通过 GORM ORM 使用 SQLExec
- [Slice 适配器](slice-adapter.md) -- 直接对 Go 数据结构执行 SQL
- [测试最佳实践](testing.md) -- 编写隔离性良好的单元测试
