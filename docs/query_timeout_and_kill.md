# 查询超时和Kill功能使用指南

## 功能概述

本项目实现了对查询的超时限制和外部Kill功能,可以有效防止死循环查询或占用过多资源的问题。

## 核心特性

1. **查询超时**: 自动在指定时间后取消超时查询
2. **Kill查询**: 支持外部主动终止查询(MySQL KILL协议兼容)
3. **查询追踪**: 全局查询注册表,追踪所有正在执行的查询
4. **灵活配置**: 支持DB级和Session级配置,Session优先级更高

## 使用方法

### 1. DB级别配置(全局默认超时)

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "time"
)

// 创建DB时设置全局查询超时
db, err := api.NewDB(&api.DBConfig{
    CacheEnabled: true,
    CacheSize:    1000,
    QueryTimeout: 30 * time.Second, // 全局默认超时30秒
})
```

### 2. Session级别配置(覆盖DB配置)

```go
// 创建Session时设置会话级超时
session := db.SessionWithOptions(&api.SessionOptions{
    DataSourceName: "default",
    QueryTimeout:   10 * time.Second, // 这个会话超时10秒
})
```

### 3. 使用默认配置(不限制超时)

```go
// 不设置QueryTimeout,默认为0表示不限制
session := db.Session()
```

### 4. Kill查询(MySQL KILL命令)

通过MySQL客户端发送KILL命令:

```sql
-- 终止指定ThreadID的查询
KILL QUERY 123;

-- 或者使用 KILL CONNECTION 终止连接
KILL CONNECTION 123;
```

也可以通过代码调用:

```go
import "github.com/kasuganosora/sqlexec/pkg/session"

// 通过ThreadID终止查询
err := session.KillQueryByThreadID(123)
if err != nil {
    // 查询不存在或其他错误
    fmt.Printf("Kill failed: %v\n", err)
}
```

## 查询ID和ThreadID

- **ThreadID**: 由服务器分配的线程ID,用于KILL查询
- **QueryID**: 查询的唯一标识符,格式为 `{ThreadID}_{timestamp}_{sequence}`

ThreadID在Session创建时自动分配:

```go
// 服务器会自动设置ThreadID
apiSession := s.db.Session()
apiSession.SetThreadID(sess.ThreadID) // 服务器内部调用
```

## 错误处理

### 超时错误

```go
result, err := session.Query("SELECT * FROM large_table")
if err != nil {
    if api.IsErrorCode(err, api.ErrCodeTimeout) {
        // 查询超时
        fmt.Println("Query timed out")
    }
}
```

### Kill错误

```go
result, err := session.Query("SELECT * FROM table")
if err != nil {
    if api.IsErrorCode(err, api.ErrCodeTimeout) {
        // 可能是超时或被Kill
        fmt.Println("Query was timed out or killed")
    }
}
```

### Kill不存在的查询

```go
err := session.KillQueryByThreadID(999)
if err != nil {
    // 查询不存在
    fmt.Printf("Kill failed: %v\n", err)
    // 返回: "query not found for thread 999"
}
```

## 配置优先级

```
Session.QueryTimeout (最高优先级)
  ↓ 未设置(为0)
DBConfig.QueryTimeout
  ↓ 未设置(为0)
不限制超时
```

## 实现细节

### 查询上下文(QueryContext)

每个查询都会创建一个查询上下文,包含:
- QueryID: 查询唯一ID
- ThreadID: 关联的线程ID
- SQL: 执行的SQL语句
- StartTime: 查询开始时间
- CancelFunc: 取消函数

### 全局查询注册表(QueryRegistry)

- 单例模式,全局唯一
- 维护所有正在执行的查询
- 支持通过ThreadID快速查找和取消查询
- 查询完成后自动清理

### Context传播

查询执行使用带取消的context:

```go
// 创建带超时的context
queryCtx, cancel, qc := s.createQueryContext(ctx, sql)

// 注册查询
registry.RegisterQuery(qc)

// 执行查询(使用带取消的context)
result, err := s.executor.ExecuteSelect(queryCtx, stmt)

// 检查错误类型
if errors.Is(err, context.DeadlineExceeded) {
    // 超时
    qc.SetTimeout()
}
if errors.Is(err, context.Canceled) {
    if qc.IsCanceled() {
        // 被Kill
    }
}
```

## 性能考虑

1. **注册表锁粒度**: 使用读写锁,读多写少场景性能好
2. **查询追踪开销**: 仅在查询开始和结束时操作注册表
3. **Context传播**: Go原生机制,开销极小
4. **内存占用**: 注册表仅存储活跃查询,查询完成后立即清理

## 兼容性

- **MySQL KILL协议**: 完全兼容MySQL的KILL命令
- **向后兼容**: 默认不启用超时,不影响现有代码
- **客户端兼容**: 标准MySQL客户端都可以发送KILL命令

## 最佳实践

1. **设置合理的超时时间**: 根据业务需求设置,一般5-60秒
2. **监控超时查询**: 定期检查全局注册表,发现长时间运行的查询
3. **Kill恶意查询**: 发现死循环或异常查询时及时Kill
4. **资源保护**: 避免查询占用过多资源

## 示例代码

### 完整示例:超时+Kill

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/session"
)

func main() {
    // 1. 创建DB并设置全局超时
    db, _ := api.NewDB(&api.DBConfig{
        QueryTimeout: 10 * time.Second,
    })

    // 2. 创建Session(覆盖DB超时)
    sess := db.SessionWithOptions(&api.SessionOptions{
        QueryTimeout: 5 * time.Second, // 5秒超时
    })

    // 3. 执行查询(会自动处理超时)
    result, err := sess.Query("SELECT * FROM users")
    if err != nil {
        if api.IsErrorCode(err, api.ErrCodeTimeout) {
            fmt.Println("查询超时或被终止")
            return
        }
        panic(err)
    }

    fmt.Printf("查询成功,返回%d行\n", result.Total)

    // 4. Kill其他查询(通过ThreadID)
    // 假设ThreadID=123
    err = session.KillQueryByThreadID(123)
    if err != nil {
        fmt.Printf("Kill失败: %v\n", err)
    }
}
```

## API参考

### 配置相关

- `DBConfig.QueryTimeout` - DB级超时配置
- `SessionOptions.QueryTimeout` - Session级超时配置

### Session方法

- `Session.SetThreadID(threadID uint32)` - 设置线程ID
- `Session.GetThreadID() uint32` - 获取线程ID

### CoreSession方法

- `CoreSession.SetQueryTimeout(timeout time.Duration)` - 设置超时
- `CoreSession.GetQueryTimeout() time.Duration` - 获取超时
- `CoreSession.SetThreadID(threadID uint32)` - 设置线程ID
- `CoreSession.GetThreadID() uint32` - 获取线程ID

### 全局注册表函数

- `session.KillQueryByThreadID(threadID uint32) error` - Kill查询
- `session.GetQueryByThreadID(threadID uint32) *QueryContext` - 获取查询
- `session.GetAllQueries() []*QueryContext` - 获取所有查询
- `session.GetGlobalQueryRegistry() *QueryRegistry` - 获取注册表

### QueryContext方法

- `IsCanceled() bool` - 检查是否被取消
- `IsTimeout() bool` - 检查是否超时
- `GetDuration() time.Duration` - 获取执行时长
- `GetStatus() QueryStatus` - 获取查询状态

### QueryRegistry方法

- `RegisterQuery(qc *QueryContext) error` - 注册查询
- `UnregisterQuery(queryID string)` - 注销查询
- `GetQuery(queryID string) *QueryContext` - 获取查询
- `GetQueryByThreadID(threadID uint32) *QueryContext` - 通过ThreadID获取
- `KillQueryByThreadID(threadID uint32) error` - Kill查询
- `GetAllQueries() []*QueryContext` - 获取所有查询
- `GetQueryCount() int` - 获取查询数量

## 注意事项

1. **ThreadID唯一性**: 每个连接的ThreadID是唯一的,用于标识查询
2. **查询追踪**: 所有查询都会被追踪,包括SELECT、INSERT、UPDATE、DELETE
3. **资源清理**: 查询完成后自动从注册表清理,无需手动管理
4. **并发安全**: 所有组件都是线程安全的,可以并发使用
5. **错误传播**: 超时和Kill错误会通过context传播到执行层

## 故障排查

### 问题: 查询没有超时

**原因**:
- QueryTimeout设置为0
- Session没有设置QueryTimeout

**解决**:
```go
// 确保设置了超时
session.SetQueryTimeout(10 * time.Second)
```

### 问题: Kill命令不生效

**原因**:
- ThreadID不正确
- 查询已经完成

**解决**:
```go
// 验证ThreadID是否正确
qc := session.GetQueryByThreadID(threadID)
if qc == nil {
    fmt.Println("Query not found")
}
```

### 问题: 查询注册表占用内存过多

**原因**: 查询完成后没有注销

**解决**: 检查defer语句是否正确
```go
// 确保defer注册注销
registry.RegisterQuery(qc)
defer registry.UnregisterQuery(qc.QueryID)
```
