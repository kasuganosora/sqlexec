# MVCC 集成文档

## 概述

MVCC模块已经创建完成，现在需要集成到现有的数据源管理系统和查询系统中。本文档说明了集成进度和架构设计。

## 架构设计

### 1. 依赖关系

```
┌─────────────────────────────────────────────────────────────┐
│                  MySQL 服务器层                          │
│              (mysql.Server)                           │
│                                                      │
│  ┌─────────────────────────────────────────────────┐    │
│  │              解析器层 (parser)              │    │
│  │   - QueryBuilder                           │    │
│  │   - SQLAdapter                             │    │
│  │   - HandlerChain                          │    │
│  └─────────────────────────────────────────────────┘    │
│                      │                                   │
│  ┌─────────────────────────────────────────────────┐    │
│  │         数据源管理层 (resource)               │    │
│  │   - DataSourceManager                       │    │
│  │   - DataSource接口                         │    │
│  │   - MVCCAdapter (新增)                     │    │
│  │   - 事务上下文管理 (新增)                  │    │
│  └─────────────────────────────────────────────────┘    │
│                      │                                   │
│  ┌─────────────────────────────────────────────────┐    │
│  │          MVCC核心层 (mvcc)                  │    │
│  │   - Manager (事务管理器)                   │    │
│  │   - Transaction (事务)                      │    │
│  │   - Snapshot (快照)                         │    │
│  │   - TupleVersion (版本)                    │    │
│  │   - VisibilityChecker (可见性检查器)         │    │
│  │   - DowngradeHandler (降级处理器)            │    │
│  └─────────────────────────────────────────────────┘    │
│                      │                                   │
│  ┌─────────────────────────────────────────────────┐    │
│  │        底层数据源实现层                     │    │
│  │   - MemoryDataSource (支持MVCC)               │    │
│  │   - MySQLDataSource (非MVCC)                │    │
│  │   - CSVDataSource (非MVCC)                  │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

### 2. 核心接口

#### DataSource接口扩展

原有的`DataSource`接口保持不变，新增了`MVCCDataSource`接口：

```go
// MVCCDataSource MVCC数据源接口（可选）
type MVCCDataSource interface {
    DataSource
    
    // SupportMVCC 是否支持MVCC
    SupportMVCC() bool
    
    // BeginTransaction 开始事务
    BeginTransaction(ctx context.Context) (interface{}, error)
    
    // CommitTransaction 提交事务
    CommitTransaction(ctx context.Context, txn interface{}) error
    
    // RollbackTransaction 回滚事务
    RollbackTransaction(ctx context.Context, txn interface{}) error
    
    // QueryWithTransaction 使用事务查询
    QueryWithTransaction(ctx context.Context, txn interface{}, 
        tableName string, options *QueryOptions) (*QueryResult, error)
    
    // InsertWithTransaction 使用事务插入
    InsertWithTransaction(ctx context.Context, txn interface{}, 
        tableName string, rows []Row, options *InsertOptions) (int64, error)
    
    // UpdateWithTransaction 使用事务更新
    UpdateWithTransaction(ctx context.Context, txn interface{}, 
        tableName string, filters []Filter, updates Row, 
        options *UpdateOptions) (int64, error)
    
    // DeleteWithTransaction 使用事务删除
    DeleteWithTransaction(ctx context.Context, txn interface{}, 
        tableName string, filters []Filter, 
        options *DeleteOptions) (int64, error)
}

// TransactionOptions 事务选项
type TransactionOptions struct {
    IsolationLevel string `json:"isolation_level,omitempty"` 
    ReadOnly       bool   `json:"read_only,omitempty"`
}
```

#### MVCCAdapter 适配器

`MVCCAdapter`实现了`MVCCDataSource`接口，作为MVCC系统和数据源系统的桥梁：

```go
type MVCCAdapter struct {
    inner      mvcc.MVCCDataSource
    config     *DataSourceConfig
    manager     *mvcc.Manager
    registry    *mvcc.DataSourceRegistry
    downgrader  *mvcc.DowngradeHandler
    mu          sync.RWMutex
    connected   bool
}
```

**核心功能**：
- 实现了`DataSource`接口，可以注册到`DataSourceManager`
- 实现了`MVCCDataSource`接口，提供事务API
- 集成`DowngradeHandler`，自动处理MVCC能力检测和降级
- 支持事务上下文传递（通过context）

#### 事务上下文管理

使用context传递事务信息：

```go
// contextKey 事务上下文Key
type contextKey int

const (
    keyTransaction contextKey = iota
)

// withTransaction 将事务添加到context
func withTransaction(ctx context.Context, txn *mvcc.Transaction) context.Context {
    return context.WithValue(ctx, keyTransaction, txn)
}

// getTransactionFromContext 从context获取事务
func getTransactionFromContext(ctx context.Context) *mvcc.Transaction {
    txn, _ := ctx.Value(keyTransaction).(*mvcc.Transaction)
    return txn
}
```

### 3. SQL解析扩展

新增了事务SQL类型的支持：

```go
const (
    SQLTypeBegin    SQLType = "BEGIN"
    SQLTypeCommit   SQLType = "COMMIT"
    SQLTypeRollback SQLType = "ROLLBACK"
)

type SQLStatement struct {
    Type      SQLType             `json:"type"`
    RawSQL    string              `json:"raw_sql"`
    Select    *SelectStatement    `json:"select,omitempty"`
    Insert    *InsertStatement    `json:"insert,omitempty"`
    Update    *UpdateStatement    `json:"update,omitempty"`
    Delete    *DeleteStatement    `json:"delete,omitempty"`
    Create    *CreateStatement    `json:"create,omitempty"`
    Drop      *DropStatement      `json:"drop,omitempty"`
    Alter     *AlterStatement     `json:"alter,omitempty"`
    Begin     *TransactionStatement `json:"begin,omitempty"`      // 新增
    Commit    *TransactionStatement `json:"commit,omitempty"`     // 新增
    Rollback  *TransactionStatement `json:"rollback,omitempty"`   // 新增
}

type TransactionStatement struct {
    Level string `json:"level,omitempty"` // 隔离级别
}
```

## 集成状态

### ✅ 已完成

1. **MVCC数据源适配器** (`mvcc_adapter.go`)
   - ✅ 实现了`DataSource`接口
   - ✅ 实现了`MVCCDataSource`接口
   - ✅ 集成了`DowngradeHandler`
   - ✅ 支持事务上下文传递

2. **DataSource接口扩展** (`source.go`)
   - ✅ 新增了`MVCCDataSource`接口
   - ✅ 新增了`TransactionOptions`类型
   - ✅ 向后兼容，不影响现有代码

3. **SQL解析扩展** (`types.go`)
   - ✅ 新增了`SQLTypeBegin`、`SQLTypeCommit`、`SQLTypeRollback`
   - ✅ 新增了`TransactionStatement`类型
   - ✅ 扩展了`SQLStatement`结构

### 🚧 待完成

1. **QueryBuilder事务集成**
   - ⚠️ 需要添加事务处理逻辑
   - ⚠️ 需要处理BEGIN/COMMIT/ROLLBACK语句

2. **MySQL服务器集成**
   - ⚠️ 需要在session中管理事务状态
   - ⚠️ 需要调用MVCC API

3. **适配器实现细节**
   - ⚠️ `executeSelect`等方法需要实际实现
   - ⚠️ 需要连接到底层数据源

4. **测试验证**
   - ⚠️ 需要端到端测试
   - ⚠️ 需要降级场景测试

## 使用示例

### 1. 创建MVCC数据源

```go
import (
    "mysql-proxy/mysql/resource"
    "mysql-proxy/mysql/mvcc"
)

// 创建内部MVCC数据源
innerDS := mvcc.NewMemoryDataSource("my_db")

// 创建配置
config := &resource.DataSourceConfig{
    Type:     "memory",
    Name:     "my_db",
    Options:   make(map[string]interface{}),
}

// 创建适配器
mvccDS, err := resource.NewMVCCAdapter(innerDS, config)
if err != nil {
    log.Fatal(err)
}

// 注册到数据源管理器
dsMgr := resource.NewDataSourceManager()
if err := dsMgr.Register("my_db", mvccDS); err != nil {
    log.Fatal(err)
}
```

### 2. 使用MVCC数据源

```go
// 获取数据源
ds, err := dsMgr.Get("my_db")
if err != nil {
    log.Fatal(err)
}

// 检查是否支持MVCC
if mvccDS, ok := ds.(resource.MVCCDataSource); ok {
    if mvccDS.SupportMVCC() {
        // 开始事务
        ctx := context.Background()
        txn, err := mvccDS.BeginTransaction(ctx)
        if err != nil {
            log.Fatal(err)
        }
        
        // 使用事务查询
        result, err := mvccDS.QueryWithTransaction(ctx, txn, "users", &resource.QueryOptions{})
        if err != nil {
            // 回滚
            mvccDS.RollbackTransaction(ctx, txn)
            log.Fatal(err)
        }
        
        // 提交事务
        if err := mvccDS.CommitTransaction(ctx, txn); err != nil {
            log.Fatal(err)
        }
    }
}
```

### 3. 通过SQL使用事务

```go
// TODO: 需要实现BEGIN/COMMIT/ROLLBACK的SQL处理
sql := `
    BEGIN;
    INSERT INTO users (name, age) VALUES ('Alice', 30);
    UPDATE users SET age = 31 WHERE name = 'Alice';
    COMMIT;
`

result, err := queryBuilder.BuildAndExecute(ctx, sql)
```

## 降级机制

### 查询前检查

```go
// 只读查询可以降级
supportsMVCC, err := handler.CheckBeforeQuery(
    []string{"my_db"}, // 数据源列表
    true,               // readOnly=true
)
if err != nil {
    // 处理错误
}

if !supportsMVCC {
    // 数据源不支持MVCC，但允许降级
    // 输出警告：[MVCC-WARN] Data sources do not support MVCC
    // 继续执行普通查询
}
```

### 写入前检查

```go
// 写入操作要求MVCC支持
supportsMVCC, err := handler.CheckBeforeWrite(
    []string{"my_db"}, // 数据源列表
)
if err != nil {
    // 数据源不支持MVCC且不允许降级
    return err
}

if !supportsMVCC {
    return fmt.Errorf("write operation requires MVCC support")
}
```

## 性能考虑

### 1. MVCC开销

- **内存开销**: 每个数据源维护多个版本
- **CPU开销**: 版本可见性判断
- **GC开销**: 定期清理过期版本

### 2. 优化策略

- 使用`CapabilityReadSnapshot`而非`CapabilityFull`可减少开销
- 调整`GCAgeThreshold`优化内存使用
- 使用`RepeatableRead`而非`Serializable`平衡一致性和性能

### 3. 降级配置

```go
config := mvcc.DefaultConfig()
config.EnableWarning = true    // 启用警告
config.AutoDowngrade = true    // 允许自动降级
config.GCInterval = 5 * time.Minute
config.GCAgeThreshold = 1 * time.Hour
```

## 后续工作

1. **完善QueryBuilder集成**
   - 添加BEGIN/COMMIT/ROLLBACK处理
   - 集成事务上下文管理

2. **MySQL服务器集成**
   - Session管理事务状态
   - 支持SET TRANSACTION ISOLATION LEVEL

3. **适配器实现**
   - 实现executeSelect等方法的实际逻辑
   - 连接到底层数据源

4. **完整测试**
   - 端到端测试
   - 性能测试
   - 降级场景测试

5. **文档完善**
   - API文档
   - 最佳实践
   - 故障排查
