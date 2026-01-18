# MVCC (多版本并发控制) 实现指南

## 概述

本系统实现了PostgreSQL风格的MVCC（多版本并发控制）机制，提供事务隔离、并发控制和数据一致性保证。

## 核心特性

- ✅ **PG风格MVCC**: 使用xmin/xmax和事务ID
- ✅ **多隔离级别**: 支持4种标准隔离级别
- ✅ **自动降级**: 不支持MVCC的数据源自动降级
- ✅ **降级警告**: 自动提示用户降级情况
- ✅ **版本可见性**: 完整的可见性判断规则
- ✅ **事务日志**: 类似PostgreSQL的clog
- ✅ **垃圾回收**: 自动清理过期版本

## 与TiDB的差异

| 特性 | 本实现 (PG风格) | TiDB (Percolator) |
|------|-----------------|---------------------|
| **架构** | 单机 | 分布式 |
| **版本标识** | xmin/xmax (XID) | start_ts/commit_ts (TSO) |
| **可见性** | 基于快照和事务日志 | 基于时间戳范围 |
| **提交方式** | 单阶段提交 | 两阶段提交 (2PC) |
| **锁机制** | 元组锁 | Percolator乐观锁 |
| **GC策略** | 定期清理 | 基于Safe Point |

详见 [MVCC_COMPARISON.md](./MVCC_COMPARISON.md)

## 快速开始

### 1. 创建MVCC管理器

```go
import "mysql-proxy/mysql/mvcc"

// 创建MVCC管理器
config := mvcc.DefaultConfig()
mgr := mvcc.NewManager(config)

// 或者使用全局管理器
mgr := mvcc.GetGlobalManager()
```

### 2. 开始事务

```go
// 创建数据源
dataSource := mvcc.NewMemoryDataSource("my_db")

// 开始事务
txn, err := mgr.Begin(mvcc.RepeatableRead, dataSource.GetFeatures())
if err != nil {
    log.Fatal(err)
}

fmt.Printf("事务ID: %d\n", txn.XID())
fmt.Printf("隔离级别: %s\n", mvcc.IsolationLevelToString(txn.Level()))
```

### 3. 读写操作

```go
// 写入数据
err := txn.Write("user:1", map[string]interface{}{
    "name": "Alice",
    "age": 30,
})

// 读取数据
version, err := txn.Read("user:1")

// 删除数据
err = txn.Delete("user:1")
```

### 4. 提交/回滚

```go
// 提交事务
if err := mgr.Commit(txn); err != nil {
    // 回滚事务
    mgr.Rollback(txn)
    log.Fatal(err)
}

fmt.Println("事务已提交")
```

## 数据源特性

### 能力等级

数据源可以声明不同的MVCC能力:

```go
const (
    CapabilityNone          // 不支持MVCC
    CapabilityReadSnapshot  // 支持读快照
    CapabilityWriteVersion  // 支持写多版本
    CapabilityFull         // 完全支持MVCC
)
```

### 实现MVCC数据源

```go
// 实现MVCCDataSource接口
type MyDataSource struct {
    name string
    data map[string][]*mvcc.TupleVersion
}

func (ds *MyDataSource) GetFeatures() *mvcc.DataSourceFeatures {
    return &mvcc.DataSourceFeatures{
        Name:         ds.name,
        Capability:   mvcc.CapabilityFull,
        SupportsRead:  true,
        SupportsWrite: true,
        SupportsDelete:true,
        IsolationLevels: []mvcc.IsolationLevel{
            mvcc.ReadCommitted,
            mvcc.RepeatableRead,
        },
    }
}

func (ds *MyDataSource) ReadWithMVCC(key string, snapshot *mvcc.Snapshot) (*mvcc.TupleVersion, error) {
    // 实现MVCC读取逻辑
    versions := ds.data[key]
    for i := len(versions) - 1; i >= 0; i-- {
        if versions[i].IsVisibleTo(snapshot) {
            return versions[i], nil
        }
    }
    return nil, nil
}

func (ds *MyDataSource) WriteWithMVCC(key string, version *mvcc.TupleVersion) error {
    ds.data[key] = append(ds.data[key], version)
    return nil
}

// ... 实现其他接口方法
```

## 降级和警告

### 检查MVCC支持

```go
// 创建数据源注册表
registry := mvcc.NewDataSourceRegistry()

// 注册数据源
registry.Register("memory_db", memoryDS.GetFeatures())
registry.Register("flat_file", flatFileDS.GetFeatures())

// 检查MVCC支持
allSupported, unsupported := registry.CheckMVCCSupport("memory_db", "flat_file")

if !allSupported {
    fmt.Printf("不支持MVCC的数据源: %v\n", unsupported)
}
```

### 使用降级处理器

```go
// 创建降级处理器
config := mvcc.DefaultConfig()
config.EnableWarning = true
config.AutoDowngrade = true
mgr := mvcc.NewManager(config)

handler := mvcc.NewDowngradeHandler(mgr, registry)

// 查询前检查
sources := []string{"memory_db", "flat_file"}
_, err := handler.CheckBeforeQuery(sources, false) // false表示不是只读

if err != nil {
    // 处理错误或不支持的情况
    log.Fatal(err)
}

// 继续执行查询
// 如果有不支持MVCC的数据源，会自动降级
```

### 警告日志

当启用降级警告时，系统会自动输出警告：

```
[MVCC-WARN] MVCC downgrade: The following data sources do not support MVCC: [flat_file_db (capability: 0)]
[MVCC-WARN] MVCC will be disabled for this query
```

## 事务隔离级别

### 支持的隔离级别

1. **Read Uncommitted**: 读未提交 (不推荐)
2. **Read Committed**: 读已提交
3. **Repeatable Read** (默认): 可重复读
4. **Serializable**: 串行化

### 使用隔离级别

```go
// 使用Repeatable Read (默认)
txn, err := mgr.Begin(mvcc.RepeatableRead, features)

// 使用Read Committed
txn, err := mgr.Begin(mvcc.ReadCommitted, features)

// 使用Serializable
txn, err := mgr.Begin(mvcc.Serializable, features)
```

### 隔离级别行为

| 隔离级别 | 脏读 | 不可重复读 | 幻读 | 快照时间 |
|---------|------|----------|------|---------|
| Read Uncommitted | 可能 | 可能 | 可能 | 当前时间 |
| Read Committed | 不可能 | 可能 | 可能 | 每次查询获取新快照 |
| Repeatable Read | 不可能 | 不可能 | 可能 | 事务开始时获取快照 |
| Serializable | 不可能 | 不可能 | 不可能 | 事务开始时获取快照 |

## 版本可见性

### 可见性规则

PG风格的可见性判断规则:

```
版本对快照可见的条件:
1. xmin <= snapshot.xmin 或 xmin不在活跃事务列表
2. xmax == 0 或 xmax > snapshot.xmin
3. 如果xmax != 0，xmax不在活跃事务列表
```

### 示例

```go
// 创建快照
snapshot := mvcc.NewSnapshot(1, 10, []mvcc.XID{5, 8, 9}, mvcc.RepeatableRead)

// 创建版本
version := mvcc.NewTupleVersion("Alice", 3, "user:1")

// 检查可见性
visible := version.IsVisibleTo(snapshot)
if visible {
    fmt.Println("版本可见")
} else {
    fmt.Println("版本不可见")
}
```

## 性能优化

### 1. 使用合适的隔离级别

```go
// 只读查询使用Read Committed
txn, err := mgr.Begin(mvcc.ReadCommitted, features)
```

### 2. 控制事务大小

```go
// 避免大事务
txn, _ := mgr.Begin(mvcc.RepeatableRead, features)

// 只包含必要的操作
for _, item := range smallBatch {
    txn.Write(item.Key, item.Value)
}

mgr.Commit(txn)
```

### 3. 配置GC

```go
// 调整GC间隔
config := mvcc.DefaultConfig()
config.GCInterval = 10 * time.Minute  // 更长的GC间隔
config.MaxVersions = 50           // 保留更少的版本

mgr := mvcc.NewManager(config)
```

## API参考

### Manager

MVCC管理器，负责事务管理、快照管理、GC等。

```go
type Manager struct {
    // 事务管理
    Begin(level IsolationLevel, features *DataSourceFeatures) (*Transaction, error)
    Commit(txn *Transaction) error
    Rollback(txn *Transaction) error
    
    // 状态查询
    GetTransactionStatus(xid XID) (TransactionStatus, error)
    GetActiveTransactionCount() int
    GetSnapshotCount() int
    
    // 快照管理
    GetSnapshot(id string) (*Snapshot, bool)
    SaveSnapshot(id string, snapshot *Snapshot)
    RemoveSnapshot(id string)
    
    // 配置
    GetConfig() *Config
}
```

### Transaction

事务对象，提供读写操作。

```go
type Transaction struct {
    // 基本属性
    XID() XID
    Status() TransactionStatus
    Level() IsolationLevel
    IsMVCC() bool
    Snapshot() *Snapshot
    
    // 读写操作
    Read(key string) (*TupleVersion, error)
    Write(key string, data interface{}) error
    Delete(key string) error
    
    // 锁管理
    Lock(key string) error
    Unlock(key string) error
    HasLock(key string) bool
    
    // 统计
    ReadCount() int
    WriteCount() int
    CommandCount() int
    Duration() time.Duration
}
```

### Snapshot

事务快照，用于版本可见性判断。

```go
type Snapshot struct {
    Xmin() XID
    Xmax() XID
    XIP() []XID
    Level() IsolationLevel
    IsActive(xid XID) bool
}
```

### TupleVersion

行版本，包含数据和版本信息。

```go
type TupleVersion struct {
    Data interface{}
    Xmin XID
    Xmax XID
    Cmin uint32
    Cmax uint32
    CTID string
    
    // 方法
    IsAlive() bool
    IsVisibleTo(snapshot *Snapshot) bool
    MarkDeleted(xmax XID, cmax uint32)
}
```

## 最佳实践

### 1. 只读查询

```go
// 只读查询可以使用较低的隔离级别
txn, _ := mgr.Begin(mvcc.ReadCommitted, features)
defer mgr.Commit(txn) // 只读事务可以立即提交

// 执行查询
result, _ := txn.Read(key)
```

### 2. 批量写入

```go
// 批量写入使用 Repeatable Read
txn, _ := mgr.Begin(mvcc.RepeatableRead, features)

for _, item := range items {
    if err := txn.Write(item.Key, item.Value); err != nil {
        mgr.Rollback(txn)
        return err
    }
}

mgr.Commit(txn)
```

### 3. 错误处理

```go
txn, _ := mgr.Begin(mvcc.RepeatableRead, features)

// 执行操作
err := txn.Write(key, value)

// 提交或回滚
if err != nil {
    mgr.Rollback(txn)
    log.Printf("事务失败，已回滚: %v", err)
    return err
}

if err := mgr.Commit(txn); err != nil {
    log.Printf("提交失败: %v", err)
    return err
}
```

### 4. 监控和调试

```go
// 检查活跃事务数
activeCount := mgr.GetActiveTransactionCount()
fmt.Printf("活跃事务数: %d\n", activeCount)

// 检查快照数
snapshotCount := mgr.GetSnapshotCount()
fmt.Printf("快照数: %d\n", snapshotCount)

// 检查事务状态
status, err := mgr.GetTransactionStatus(xid)
fmt.Printf("事务 %d 状态: %s\n", xid, mvcc.StatusToString(status))
```

## 限制和注意事项

### 当前限制

1. **分布式场景**
   - 当前实现为单机设计
   - 分布式场景需要额外的协调机制

2. **锁机制**
   - 暂不支持等待锁
   - 暂不支持死锁检测

3. **事务ID环绕**
   - 32位XID会环绕
   - 需要实现VACUUM清理

### 性能考虑

1. **内存使用**
   - 每个版本都需要内存
   - 长事务会累积大量版本

2. **GC开销**
   - GC需要扫描所有版本
   - GC间隔需要权衡

3. **快照维护**
   - 大量快照会增加内存使用
   - 需要及时清理过期快照

## 未来改进

### 短期

1. 实现锁等待机制
2. 添加死锁检测
3. 改进GC策略
4. 添加性能监控

### 中期

1. 支持分布式MVCC
2. 实现VACUUM清理
3. 添加事务超时机制
4. 支持savepoint

### 长期

1. 支持并行事务
2. 实现事务复制
3. 添加分布式协调
4. 支持跨数据源事务

## 示例文件

- `test_mvcc.go` - MVCC功能测试
- `example_mvcc_usage.go` - 使用示例

## 相关文档

- [MVCC_COMPARISON.md](./MVCC_COMPARISON.md) - TiDB vs PG对比
- [BUILTIN_FUNCTIONS_SUMMARY.md](./BUILTIN_FUNCTIONS_SUMMARY.md) - 内置函数

## 参考资料

1. PostgreSQL MVCC Documentation: https://www.postgresql.org/docs/current/mvcc.html
2. PostgreSQL Visibility Rules: https://www.postgresql.org/docs/current/spi-visibility.html
3. TiDB Percolator Paper: https://www.usenix.org/system/conference/nsdi12
