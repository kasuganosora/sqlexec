# DB 与 Session

`DB` 是 SQLExec 嵌入式使用的核心入口，负责管理数据源和创建会话。`Session` 代表一个数据库会话（类似 MySQL 的连接），提供查询、执行和事务操作。

## 导入

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)
```

## 创建 DB 实例

使用 `api.NewDB(config)` 创建数据库实例：

```go
// 使用默认配置
db, err := api.NewDB(nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

```go
// 使用自定义配置
db, err := api.NewDB(&api.DBConfig{
    CacheEnabled:          true,
    CacheSize:             2000,
    CacheTTL:              600,  // 秒
    DefaultLogger:         api.NewDefaultLogger(api.LogDebug),
    DebugMode:             true,
    QueryTimeout:          30 * time.Second,
    UseEnhancedOptimizer:  true,
})
```

### DBConfig 配置字段

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `CacheEnabled` | `bool` | `true` | 是否启用查询缓存 |
| `CacheSize` | `int` | `1000` | 缓存最大条目数 |
| `CacheTTL` | `int` | `300` | 缓存过期时间（秒） |
| `DefaultLogger` | `Logger` | `LogInfo` 级别 | 日志实现 |
| `DebugMode` | `bool` | `false` | 调试模式 |
| `QueryTimeout` | `time.Duration` | `0`（无限制） | 全局查询超时时间 |
| `UseEnhancedOptimizer` | `bool` | `true` | 是否使用增强查询优化器 |

## 数据源管理

DB 实例通过数据源管理方法来注册、查询和管理不同的数据源。

### RegisterDataSource -- 注册数据源

```go
// 创建内存数据源
memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
    Type:     domain.DataSourceTypeMemory,
    Name:     "default",
    Writable: true,
})
memDS.Connect(context.Background())

// 注册到 DB
err := db.RegisterDataSource("default", memDS)
if err != nil {
    log.Fatal(err)
}
```

注意：第一个注册的数据源会自动成为默认数据源。

### SetDefaultDataSource -- 设置默认数据源

```go
// 注册多个数据源后，切换默认数据源
db.RegisterDataSource("ds1", memDS1)
db.RegisterDataSource("ds2", memDS2)

err := db.SetDefaultDataSource("ds2")
```

### GetDataSource -- 获取数据源

```go
ds, err := db.GetDataSource("default")
if err != nil {
    log.Printf("数据源不存在: %v", err)
}
```

### GetDataSourceNames -- 列出所有数据源

```go
names := db.GetDataSourceNames()
for _, name := range names {
    fmt.Println("数据源:", name)
}
```

### Close -- 关闭并释放资源

```go
// 关闭 DB 实例，释放所有数据源资源
err := db.Close()
```

`Close()` 会遍历所有已注册的数据源并逐一关闭，同时清空查询缓存。

## 创建 Session

### Session -- 使用默认选项

```go
session := db.Session()
defer session.Close()
```

`db.Session()` 使用默认数据源和以下默认选项：
- 隔离级别：`IsolationRepeatableRead`
- 非只读模式
- 缓存状态继承 DB 配置

### SessionWithOptions -- 使用自定义选项

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

### SessionOptions 字段

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `DataSourceName` | `string` | 默认数据源 | 指定使用的数据源名称 |
| `Isolation` | `IsolationLevel` | `IsolationRepeatableRead` | 事务隔离级别 |
| `ReadOnly` | `bool` | `false` | 是否为只读会话 |
| `CacheEnabled` | `bool` | 继承 DB 配置 | 是否启用查询缓存 |
| `QueryTimeout` | `time.Duration` | 继承 DB 配置 | 会话级查询超时，覆盖 DB 全局设置 |
| `UseEnhancedOptimizer` | `*bool` | `nil`（继承 DB 配置） | 是否使用增强优化器 |

## Session 上下文设置

Session 提供多个方法来设置会话级别的上下文信息：

### SetUser -- 设置当前用户

```go
session.SetUser("admin")
user := session.GetUser() // "admin"
```

### SetTraceID -- 设置追踪 ID

用于请求追踪和审计日志，TraceID 会传播到底层 CoreSession：

```go
session.SetTraceID("req-abc-123")
traceID := session.GetTraceID() // "req-abc-123"
```

### SetThreadID -- 设置线程 ID

用于 KILL 查询等管理操作：

```go
session.SetThreadID(42)
threadID := session.GetThreadID() // 42
```

### SetCurrentDB -- 设置当前数据库

等效于 SQL 的 `USE database_name`：

```go
session.SetCurrentDB("analytics")
currentDB := session.GetCurrentDB() // "analytics"
```

### Close -- 关闭会话

```go
err := session.Close()
```

`Close()` 会执行以下清理操作：
1. 回滚未提交的事务
2. 删除会话中创建的临时表
3. 关闭底层 CoreSession

## 日志

### Logger 接口

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

### 日志级别

| 常量 | 值 | 说明 |
|------|-----|------|
| `api.LogError` | `0` | 仅输出错误 |
| `api.LogWarn` | `1` | 输出警告和错误 |
| `api.LogInfo` | `2` | 输出信息、警告和错误 |
| `api.LogDebug` | `3` | 输出所有级别（含调试信息） |

### 使用内置日志

```go
// 创建默认日志（输出到 stdout）
logger := api.NewDefaultLogger(api.LogDebug)

// 创建带自定义输出的日志
var buf bytes.Buffer
logger := api.NewDefaultLoggerWithOutput(api.LogInfo, &buf)

// 创建空日志（禁用日志输出）
logger := api.NewNoOpLogger()
```

### 设置 DB 日志

```go
// 通过配置设置
db, _ := api.NewDB(&api.DBConfig{
    DefaultLogger: api.NewDefaultLogger(api.LogDebug),
})

// 运行时动态切换
db.SetLogger(api.NewDefaultLogger(api.LogWarn))

// 获取当前日志
currentLogger := db.GetLogger()
```

## 完整示例

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
    // 1. 创建 DB 实例
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

    // 2. 创建并注册内存数据源
    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{
        Type:     domain.DataSourceTypeMemory,
        Name:     "main",
        Writable: true,
    })
    memDS.Connect(context.Background())

    if err := db.RegisterDataSource("main", memDS); err != nil {
        log.Fatal(err)
    }

    // 3. 列出所有数据源
    fmt.Println("已注册数据源:", db.GetDataSourceNames())

    // 4. 创建会话
    session := db.Session()
    defer session.Close()

    // 5. 设置会话上下文
    session.SetUser("app_service")
    session.SetTraceID("trace-001")

    // 6. 执行 SQL
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

## 下一步

- [查询与结果](query-and-result.md) -- 深入了解查询执行和结果处理
- [事务管理](transactions.md) -- 事务操作和隔离级别控制
