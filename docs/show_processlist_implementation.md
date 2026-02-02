# SHOW PROCESSLIST 实现说明

## 概述

实现了 `SHOW PROCESSLIST` 和 `SHOW FULL PROCESSLIST` 命令，用于查看当前正在执行的查询列表。该功能与之前实现的 KILL 功能配合使用。

## 功能特性

1. **与 MySQL 兼容的字段**
   - `Id`: 连接标识符（线程ID）
   - `User`: 执行查询的用户
   - `Host`: 客户端主机和端口（格式：host:port）
   - `db`: 当前使用的数据库
   - `Command`: 命令类型（目前固定为 "Query"）
   - `Time`: 查询已执行的秒数
   - `State`: 线程状态（executing/killed/timeout）
   - `Info`: 正在执行的 SQL 语句

2. **SHOW FULL PROCESSLIST 支持**
   - `SHOW PROCESSLIST`: Info 字段截断为前 100 个字符
   - `SHOW FULL PROCESSLIST`: Info 字段显示完整的 SQL 语句

3. **查询状态**
   - `executing`: 查询正在执行
   - `killed`: 查询被 KILL 命令终止
   - `timeout`: 查询超时

## 实现架构

### 1. 解析层（parser）

**文件**: `pkg/parser/adapter.go`

- 在 `ShowStatement` 结构体中添加 `Full` 字段，用于区分 `SHOW PROCESSLIST` 和 `SHOW FULL PROCESSLIST`
- 在 `convertShowStmt` 函数中添加 `ast.ShowProcessList` 类型处理

```go
case ast.ShowProcessList:
    showStmt.Type = "PROCESSLIST"
    showStmt.Full = stmt.Full
```

### 2. 执行层（optimizer）

**文件**: `pkg/optimizer/optimized_executor.go`

- 在 `ExecuteShow` 函数中添加 `PROCESSLIST` 类型的处理
- 实现 `executeShowProcessList` 方法，返回查询列表

**避免循环依赖的设计**:

为了解决 `session` 包和 `optimizer` 包的相互依赖问题，使用了函数注入的方式：

```go
// ProcessListProvider 进程列表提供者函数类型
type ProcessListProvider func() []interface{}

var processListProvider ProcessListProvider

// RegisterProcessListProvider 注册进程列表提供者
func RegisterProcessListProvider(provider ProcessListProvider) {
    processListProvider = provider
}
```

### 3. 会话层（session）

**文件**: `pkg/session/query_registry.go`

- 扩展 `QueryContext` 结构体，添加 `User`、`Host`、`DB` 字段
- 实现 `GetProcessListForOptimizer` 函数，返回查询列表供 optimizer 使用

```go
func GetProcessListForOptimizer() []interface{} {
    registry := GetGlobalQueryRegistry()
    queries := registry.GetAllQueries()

    result := make([]interface{}, 0, len(queries))
    for _, qc := range queries {
        status := qc.GetStatus()
        result = append(result, map[string]interface{}{
            "QueryID":   status.QueryID,
            "ThreadID":  status.ThreadID,
            "SQL":        status.SQL,
            "StartTime":  status.StartTime,
            "Duration":   status.Duration,
            "Status":     status.Status,
            "User":       status.User,
            "Host":       status.Host,
            "DB":         status.DB,
        })
    }
    return result
}
```

**文件**: `pkg/session/core.go`

- 在 `createQueryContext` 方法中设置 `User`、`Host`、`DB` 字段

### 4. 服务器层（server）

**文件**: `server/server.go`

- 在 `NewServer` 函数中注册进程列表提供者

```go
optimizer.RegisterProcessListProvider(pkg_session.GetProcessListForOptimizer)
```

## 使用示例

### 查看所有正在执行的查询

```sql
SHOW PROCESSLIST;
```

输出示例：
```
+------+------+------+------+---------+------+-------+-------+---------------------------+
| Id   | User | Host | db      | Command| Time  | State | Info                      |
+------+------+------+------+---------+------+-------+-------+---------------------------+
| 1001 | user | localhost:3306 | test | Query | 5     | executing | SELECT * FROM users |
+------+------+------+------+---------+------+-------+-------+---------------------------+
```

### 查看完整的 SQL 语句

```sql
SHOW FULL PROCESSLIST;
```

当 SQL 语句超过 100 个字符时，使用 `SHOW FULL PROCESSLIST` 可以看到完整的语句。

### 与 KILL 配合使用

```sql
-- 1. 查看正在执行的查询
SHOW PROCESSLIST;

-- 2. 找到需要终止的查询的 Id（例如 1001）
-- 3. 终止该查询
KILL 1001;
```

## 测试

**文件**: `server/tests/show_processlist_test.go`

测试用例：

1. `TestShowProcessList`: 验证基本的 SHOW PROCESSLIST 功能
2. `TestShowFullProcessList`: 验证 SHOW FULL PROCESSLIST 功能
3. `TestShowProcessListFields`: 验证返回的字段类型和名称符合 MySQL 标准

运行测试：
```bash
go test ./server/tests/ -run TestShowProcessList -v
```

## 注意事项

1. **字段限制**: 当前实现只显示正在执行的查询，不显示空闲连接（状态为 Sleep）
2. **User 和 Host 默认值**: 如果未设置，使用默认值 `user` 和 `localhost:3306`
3. **查询状态**: 当查询被 KILL 或超时后，会从注册表中移除，因此可能无法在 PROCESSLIST 中看到
4. **性能**: PROCESSLIST 操作是实时的，直接从内存注册表读取，性能开销很小

## 与 MySQL 的差异

1. **不支持空闲连接**: 只显示正在执行的查询，不显示状态为 Sleep 的连接
2. **简化状态**: 状态只有 executing/killed/timeout，不包含 MySQL 的详细状态（如 Sending data、Sorting result 等）
3. **Command 字段**: 目前固定为 "Query"，不支持其他命令类型（如 Sleep、Connect 等）
