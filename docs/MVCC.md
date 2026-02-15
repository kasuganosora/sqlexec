# MVCC 多版本并发控制 -- 技术原理

## 1. MVCC 概述

### 1.1 什么是 MVCC

MVCC（Multi-Version Concurrency Control，多版本并发控制）是一种数据库并发控制机制。其核心思想是：**每次数据修改都不直接覆盖原有数据，而是创建一个新版本**，从而使得读操作和写操作可以并发执行而不互相阻塞。

SQLExec 项目采用了 **PostgreSQL 风格** 的 MVCC 实现，包含两个层次：

- **`pkg/mvcc/`** -- 核心 MVCC 抽象层，定义了事务 ID（XID）、快照（Snapshot）、行版本（TupleVersion）、提交日志（CommitLog）等基础概念，提供通用的可见性判断规则。
- **`pkg/resource/memory/`** -- 内存存储引擎层，基于 Copy-on-Write（写时复制）实现了表级和行级的多版本管理，是 MVCC 在实际存储中的落地。

### 1.2 为什么需要 MVCC

传统的锁机制在并发场景下有明显的缺陷：读操作会阻塞写操作，写操作也会阻塞读操作。MVCC 通过多版本机制解决了这一问题：

- **读不阻塞写**：读操作访问历史版本快照，无需等待写操作完成。
- **写不阻塞读**：写操作创建新版本，不影响正在读取旧版本的事务。
- **一致性读**：每个事务看到的数据是一个一致的快照视图。

### 1.3 整体架构

```
                    +-----------------------+
                    |     API Layer         |
                    |  Session / Transaction|
                    +----------+------------+
                               |
                    +----------v------------+
                    |   pkg/mvcc (抽象层)    |
                    |  Manager / Snapshot   |
                    |  TupleVersion / CLog  |
                    +----------+------------+
                               |
                    +----------v------------+
                    | pkg/resource/memory   |
                    |   MVCCDataSource      |
                    |  TableVersions / COW  |
                    |  BufferPool / Paging  |
                    +-----------------------+
```

---

## 2. 事务生命周期

### 2.1 事务 ID (XID)

事务 ID 是 MVCC 的基石。SQLExec 采用 PostgreSQL 风格的 32 位无符号整数作为事务 ID：

```go
// pkg/mvcc/types.go
type XID uint32

const XIDNone XID = 0        // 空事务 ID
const XIDBootstrap XID = 1   // 引导事务 ID
const XIDMax = XID(4294967295) // 最大事务 ID（2^32 - 1）
```

XID 是单调递增的。当 XID 达到最大值时会发生**环绕**（wrap-around），回到 `XIDBootstrap`：

```go
// pkg/mvcc/types.go
func NextXID(current XID) XID {
    if current == XID(XIDMax) {
        return XIDBootstrap // 环绕
    }
    return current + 1
}
```

为了防止环绕导致的数据可见性混乱，系统在 XID 接近阈值时会输出警告：

```go
// pkg/mvcc/manager.go
if current >= uint32(XIDMax) - m.config.XIDWrapThreshold {
    m.warning("XID approaching wrap-around, consider vacuum")
}
```

默认的环绕警告阈值 `XIDWrapThreshold` 为 100000。

### 2.2 BEGIN -- 开始事务

事务的开始由 `Manager.Begin()` 方法完成。该方法执行以下步骤：

1. **检查活跃事务数**是否超过上限（默认 `MaxActiveTxns = 10000`）。
2. **检查数据源的 MVCC 能力**，若不支持则降级到非 MVCC 事务。
3. **生成新的 XID**（原子递增）。
4. **创建快照**：记录当前所有活跃事务的 XID 列表。
5. **创建事务对象**，初始化命令列表和读写集合。

```go
// pkg/mvcc/manager.go
func (m *Manager) Begin(level IsolationLevel, features *DataSourceFeatures) (*Transaction, error) {
    // ... 前置检查 ...

    xid := m.nextXID()

    // 收集当前活跃事务
    xip := make([]XID, 0, len(m.transactions))
    for txnXID := range m.transactions {
        xip = append(xip, txnXID)
    }
    snapshot := NewSnapshot(m.xid, xid, xip, level)

    txn := &Transaction{
        xid:      xid,
        snapshot: snapshot,
        status:   TxnStatusInProgress,
        level:    level,
        mvcc:     true,
        commands: make([]Command, 0),
        reads:    make(map[string]bool),
        writes:   make(map[string]*TupleVersion),
        locks:    make(map[string]bool),
    }

    m.transactions[xid] = txn
    return txn, nil
}
```

在内存存储引擎层，`MVCCDataSource.BeginTx()` 会为每张表创建一个 **COW（Copy-on-Write）快照结构**，此时并不复制数据，仅记录引用：

```go
// pkg/resource/memory/transaction.go
func (m *MVCCDataSource) BeginTx(ctx context.Context, readOnly bool) (int64, error) {
    txnID := m.nextTxID
    m.nextTxID++

    tableSnapshots := make(map[string]*COWTableSnapshot)
    for tableName := range m.tables {
        tableSnapshots[tableName] = &COWTableSnapshot{
            tableName: tableName,
            copied:    false,     // 延迟复制
            baseData:  nil,       // 按需加载
        }
    }

    snapshot := &Snapshot{
        txnID:          txnID,
        startVer:       m.currentVer,
        tableSnapshots: tableSnapshots,
    }

    m.snapshots[txnID] = snapshot
    m.activeTxns[txnID] = txn
    return txnID, nil
}
```

### 2.3 COMMIT -- 提交事务

提交事务时，系统按以下顺序执行：

**MVCC 抽象层（`Manager.Commit`）**：

1. 逐一执行事务中记录的所有 `Command`（包括 `WriteCommand`、`DeleteCommand`、`UpdateCommand`）。
2. 若某个命令执行失败，则**逆序回滚**所有已执行的命令，事务状态设为 `Aborted`。
3. 成功后，将状态设为 `Committed`，写入提交日志（CommitLog），从活跃事务表中移除。

```go
// pkg/mvcc/manager.go
func (m *Manager) Commit(txn *Transaction) error {
    // 应用所有命令
    for _, cmd := range txn.commands {
        if err := cmd.Apply(); err != nil {
            // 逆序回滚
            for i := len(txn.commands) - 1; i >= 0; i-- {
                txn.commands[i].Rollback()
            }
            txn.status = TxnStatusAborted
            return err
        }
    }
    txn.status = TxnStatusCommitted
    m.clog.SetStatus(txn.xid, TxnStatusCommitted)
    delete(m.transactions, txn.xid)
    return nil
}
```

**内存存储引擎层（`MVCCDataSource.CommitTx`）**：

1. 只读事务直接清理快照返回。
2. 写事务遍历每张表的 COW 快照，若存在行级修改（`rowCopies` 或 `deletedRows` 不为空），则**合并基础数据与修改**，创建新的表版本。
3. 递增全局版本号 `currentVer`，将新版本数据写入 `TableVersions.versions` 映射。
4. 清理快照和活跃事务记录。
5. 触发 `gcOldVersions()` 进行垃圾回收。

### 2.4 ROLLBACK -- 回滚事务

回滚事务的过程如下：

**MVCC 抽象层**：逆序调用每个 `Command` 的 `Rollback()` 方法。对于 `UpdateCommand`，回滚时会恢复旧版本的 `Xmax` 和 `Expired` 字段：

```go
// pkg/mvcc/transaction.go
func (cmd *UpdateCommand) Rollback() error {
    if !cmd.applied { return nil }
    if cmd.oldVersion != nil {
        cmd.oldVersion.Expired = false
        cmd.oldVersion.Xmax = 0
    }
    return nil
}
```

**内存存储引擎层**：由于采用 COW 策略，回滚操作非常轻量 -- 只需删除快照和事务记录，事务中的修改自然被丢弃，无需恢复任何数据：

```go
// pkg/resource/memory/transaction.go
func (m *MVCCDataSource) RollbackTx(ctx context.Context, txnID int64) error {
    // COW 回滚只需删除快照
    delete(m.activeTxns, txnID)
    delete(m.snapshots, txnID)
    m.gcOldVersions()
    return nil
}
```

### 2.5 事务状态机

事务有三种状态，由 `TransactionStatus` 表示：

```
                  Commit
InProgress (0) ──────────> Committed (1)
      │
      │  Rollback
      └──────────────────> Aborted (2)
```

| 状态 | 值 | 含义 |
|------|-----|------|
| `TxnStatusInProgress` | 0 | 事务正在执行中 |
| `TxnStatusCommitted` | 1 | 事务已成功提交 |
| `TxnStatusAborted` | 2 | 事务已回滚 |

---

## 3. 版本链

### 3.1 MVCC 抽象层 -- TupleVersion

在 `pkg/mvcc/types.go` 中，`TupleVersion` 表示一行数据的某个版本，采用 PostgreSQL 风格的元组头部信息：

```go
type TupleVersion struct {
    Data      interface{} // 行数据
    Xmin      XID         // 创建该版本的事务 ID
    Xmax      XID         // 删除该版本的事务 ID（0 表示未删除）
    Cmin      uint32      // 创建时的命令序号
    Cmax      uint32      // 删除时的命令序号
    CTID      string      // 行标识符
    Expired   bool        // 是否已过期
    CreatedAt time.Time   // 创建时间
}
```

核心字段解读：

- **Xmin**：创建该版本的事务 ID。只有当该事务已提交，其他事务才能看到此版本。
- **Xmax**：删除或更新该版本的事务 ID。当 `Xmax = 0` 时表示该版本尚未被删除。
- **CTID**：行物理标识符，格式为 `ctid:<纳秒时间戳>`。

在 `MVCCDataSource`（`pkg/mvcc/datasource.go`）中，版本链存储在 `map[string][]*TupleVersion` 结构中，每个 key 对应一个版本切片，新版本追加到切片尾部：

```go
type MemoryDataSource struct {
    data map[string][]*TupleVersion // key -> [v1, v2, v3, ...]
}
```

读取时，从后向前遍历版本切片，找到第一个对当前快照可见的版本：

```go
func (ds *MemoryDataSource) ReadWithMVCC(key string, snapshot *Snapshot) (*TupleVersion, error) {
    versions := ds.data[key]
    for i := len(versions) - 1; i >= 0; i-- {
        if versions[i].IsVisibleTo(snapshot) {
            return versions[i], nil
        }
    }
    return nil, fmt.Errorf("no visible version for key: %s", key)
}
```

### 3.2 内存存储引擎 -- 表级多版本

在 `pkg/resource/memory/` 中，多版本管理以**表**为粒度。每张表维护一个版本映射：

```go
// pkg/resource/memory/types.go
type TableVersions struct {
    versions map[int64]*TableData // version -> 表数据
    latest   int64                // 最新版本号
}

type TableData struct {
    version   int64              // 版本号
    createdAt time.Time          // 创建时间
    schema    *domain.TableInfo  // 表结构
    rows      *PagedRows         // 分页行数据
}
```

每次数据变更（INSERT / UPDATE / DELETE）都会递增全局版本号 `currentVer`，创建一个新的 `TableData` 对象并将其加入 `TableVersions.versions` 映射。旧版本不会立即删除，而是保留给仍在使用该版本的活跃事务。

### 3.3 行级 Copy-on-Write

为了避免每次写操作都完整复制整张表的数据，内存存储引擎采用了**行级 COW（Copy-on-Write）** 策略。`COWTableSnapshot` 结构记录了事务内的行级修改：

```go
// pkg/resource/memory/types.go
type COWTableSnapshot struct {
    tableName     string
    copied        bool                   // 是否已创建修改副本
    baseData      *TableData             // 基础数据引用
    modifiedData  *TableData             // 修改后的元数据
    rowLocks      map[int64]bool         // 被修改的行 ID
    rowCopies     map[int64]domain.Row   // 修改后的行数据
    deletedRows   map[int64]bool         // 被删除的行 ID
    insertedCount int64                  // 新插入的行数
}
```

工作流程：

1. **读取操作**：若事务未修改该表，直接读取主版本数据（零拷贝）。若有修改，则合并基础数据和行级修改后返回。
2. **写入操作**：首次写入时调用 `ensureCopied()` 初始化 COW 结构（仅复制 schema，不复制行数据）。之后的修改只记录在 `rowCopies`/`deletedRows` 中。
3. **提交时**：将 `baseData` 与 `rowCopies`/`deletedRows` 合并，生成新的完整版本。

这种设计使得只读事务的开销几乎为零，写事务也只在提交时才产生完整的数据复制。

---

## 4. 快照隔离

### 4.1 快照结构

快照是 MVCC 可见性判断的核心，采用 PostgreSQL 风格的三元组表示：

```go
// pkg/mvcc/types.go
type Snapshot struct {
    xmin    XID            // 最小活跃事务 ID（快照创建时）
    xmax    XID            // 最大已分配事务 ID + 1
    xip     []XID          // 快照创建时仍在活跃的事务列表
    level   IsolationLevel // 隔离级别
    created time.Time      // 创建时间
}
```

快照的含义：

- **xmin**：所有 XID < xmin 的事务都已完成（提交或回滚），其提交的数据对当前事务可见。
- **xmax**：所有 XID >= xmax 的事务都是在快照之后开始的，其数据不可见。
- **xip**：在 [xmin, xmax) 范围内仍在执行中的事务列表，这些事务的数据不可见。

### 4.2 可见性规则

`TupleVersion.IsVisibleTo()` 实现了 PostgreSQL 风格的可见性判断：

```go
// pkg/mvcc/types.go
func (v *TupleVersion) IsVisibleTo(snapshot *Snapshot) bool {
    // 已过期的版本不可见
    if v.Expired {
        return false
    }

    // 规则 1：xmin 必须在快照可见范围内
    if v.Xmin > snapshot.Xmin() {
        if snapshot.IsActive(v.Xmin) {
            return false // 创建者仍在活跃中，不可见
        }
    }

    // 规则 2：xmax 为 0（未删除）或删除者尚未提交
    if v.Xmax != 0 {
        if v.Xmax <= snapshot.Xmin() {
            return false // 删除者已提交，该版本不可见
        }
        if snapshot.IsActive(v.Xmax) {
            return false // 删除者仍活跃，不可见（保守策略）
        }
    }

    return true
}
```

可见性判断流程图：

```
版本 v 对快照 s 是否可见？

    v.Expired == true ? ──yes──> 不可见
         │ no
    v.Xmin > s.xmin ? ──yes──> v.Xmin 在 s.xip 中？ ──yes──> 不可见
         │ no                        │ no
         v                          v
    v.Xmax == 0 ? ──yes──────────────────────────────> 可见
         │ no
    v.Xmax <= s.xmin ? ──yes──> 不可见
         │ no
    v.Xmax 在 s.xip 中？ ──yes──> 不可见
         │ no
         v
       可见
```

### 4.3 批量可见性检查

`VisibilityChecker` 提供了批量可见性检查和过滤功能，避免逐个调用的开销：

```go
// pkg/mvcc/types.go
type VisibilityChecker struct{}

// 批量检查
func (vc *VisibilityChecker) CheckBatch(versions []*TupleVersion, snapshot *Snapshot) []bool

// 过滤可见版本
func (vc *VisibilityChecker) FilterVisible(versions []*TupleVersion, snapshot *Snapshot) []*TupleVersion
```

### 4.4 内存引擎的快照实现

在内存存储引擎中，快照基于版本号（`startVer`）实现。事务在 `BeginTx` 时记录当前的全局版本号 `currentVer` 作为快照起始版本。查询时，事务通过 COW 快照读取与自己快照版本一致的数据，实现一致性读取：

```go
// pkg/resource/memory/query.go
if hasTxn {
    snapshot, ok := m.snapshots[txnID]
    if ok {
        cowSnapshot := snapshot.tableSnapshots[tableName]
        tableData = cowSnapshot.getTableData(tableVer)
    }
}
```

---

## 5. 隔离级别

SQLExec 支持 SQL 标准定义的四种隔离级别：

```go
// pkg/mvcc/types.go
const (
    ReadUncommitted IsolationLevel = 0
    ReadCommitted   IsolationLevel = 1
    RepeatableRead  IsolationLevel = 2
    Serializable    IsolationLevel = 3
)
```

API 层同样定义了对应的隔离级别常量：

```go
// pkg/api/session_types.go
const (
    IsolationReadUncommitted IsolationLevel = iota
    IsolationReadCommitted
    IsolationRepeatableRead
    IsolationSerializable
)
```

### 5.1 READ UNCOMMITTED（读未提交）

最低隔离级别。允许读取其他事务尚未提交的数据（脏读）。在 MVCC 实现中，该级别的快照不排除活跃事务的修改。

### 5.2 READ COMMITTED（读已提交）

每次读操作都获取最新的已提交快照。可以避免脏读，但可能出现不可重复读。在 MVCC 框架中，该级别的事务每次查询时刷新快照。

### 5.3 REPEATABLE READ（可重复读）

**默认隔离级别**。事务在开始时获取一次快照，此后所有读操作都基于同一个快照。这确保了：

- 不会出现脏读。
- 同一查询在事务内多次执行返回相同结果（可重复读）。
- 可能出现幻读（但在 MVCC 实现中，由于快照的存在，幻读被有效避免）。

```go
// pkg/mvcc/types.go -- 默认隔离级别
func IsolationLevelFromString(s string) IsolationLevel {
    // ...
    default:
        return RepeatableRead // 默认
}
```

### 5.4 SERIALIZABLE（可序列化）

最高隔离级别。保证事务的执行效果等同于串行执行。在当前实现中，该级别在快照基础上通过读写集合（`reads` 和 `writes` 字段）进行冲突检测。事务对象记录了所有读写操作的 key：

```go
// pkg/mvcc/transaction.go
type Transaction struct {
    reads  map[string]bool           // 读集合
    writes map[string]*TupleVersion  // 写集合
    locks  map[string]bool           // 锁集合
}
```

### 5.5 隔离级别的设置

用户可以通过 API 层设置会话的隔离级别：

```go
// pkg/api/session_transaction.go
func (s *Session) SetIsolationLevel(level IsolationLevel) {
    s.options.Isolation = level
}
```

也可以在 SQL 层面通过 `BEGIN` 语句指定隔离级别。解析器支持以下语法：

```go
// pkg/parser/types.go
type TransactionStatement struct {
    Level string // READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
}
```

---

## 6. 垃圾回收

随着事务不断执行，系统中会累积大量历史版本和过期的提交日志。垃圾回收（GC）机制负责清理这些不再需要的数据。

### 6.1 GC 配置

GC 相关的配置项定义在 `Config` 结构中：

```go
// pkg/mvcc/manager.go
type Config struct {
    GCInterval       time.Duration // GC 执行间隔，默认 5 分钟
    GCAgeThreshold   time.Duration // 版本保留时间，默认 1 小时
    XIDWrapThreshold uint32        // XID 环绕警告阈值，默认 100000
    MaxActiveTxns    int           // 最大活跃事务数，默认 10000
}
```

这些配置也可以通过应用全局配置文件设置：

```go
// pkg/config/config.go
type MVCCConfig struct {
    EnableWarning    bool          `json:"enable_warning"`
    AutoDowngrade    bool          `json:"auto_downgrade"`
    GCInterval       time.Duration `json:"gc_interval"`
    GCAgeThreshold   time.Duration `json:"gc_age_threshold"`
    XIDWrapThreshold uint32        `json:"xid_wrap_threshold"`
    MaxActiveTxns    int           `json:"max_active_txns"`
}
```

### 6.2 MVCC 抽象层的 GC

Manager 在启动时会创建一个后台 goroutine 定期执行 GC：

```go
// pkg/mvcc/manager.go
func (m *Manager) gcLoop() {
    ticker := time.NewTicker(m.config.GCInterval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            m.GC()
        case <-m.gcStop:
            return
        }
    }
}
```

GC 执行两个操作：

1. **清理过期快照**：遍历所有缓存的快照，删除存活时间超过 `GCAgeThreshold` 的快照。

```go
func (m *Manager) gcSnapshots() {
    for xid, snapshot := range m.snapshots {
        if snapshot.Age() > m.config.GCAgeThreshold {
            delete(m.snapshots, xid)
        }
    }
}
```

2. **清理提交日志**：`CommitLog.GC()` 删除与当前 XID 差距超过 10000 的旧日志条目，并更新最小 XID 水位线。

```go
// pkg/mvcc/clog.go
func (l *CommitLog) GC(currentXID XID) {
    for xid := range l.entries {
        if xid < currentXID - 10000 {
            delete(l.entries, xid)
        }
    }
    l.updateOldest()
}
```

### 6.3 内存存储引擎的 GC

内存存储引擎在每次事务提交或回滚后都会调用 `gcOldVersions()` 清理不再需要的表版本：

```go
// pkg/resource/memory/mvcc_datasource.go
func (m *MVCCDataSource) gcOldVersions() {
    // 找到所有活跃事务需要的最小版本号
    minRequiredVer := m.currentVer
    for _, snapshot := range m.snapshots {
        if snapshot.startVer < minRequiredVer {
            minRequiredVer = snapshot.startVer
        }
    }

    // 清理每张表的旧版本
    for tableName, tableVer := range m.tables {
        for ver, data := range tableVer.versions {
            // 保留最新版本和活跃事务需要的版本
            if ver < minRequiredVer && ver != tableVer.latest {
                // 释放分页行数据，回收缓冲池内存和溢出文件
                if data != nil && data.rows != nil {
                    data.rows.Release()
                }
                delete(tableVer.versions, ver)
            }
        }
        // 更新缓冲池的最新版本信息，用于淘汰优先级
        if m.bufferPool != nil {
            m.bufferPool.UpdateLatestVersion(tableName, tableVer.latest)
        }
    }
}
```

GC 的安全性保证：

- **只删除低于最小活跃事务版本的旧数据**，确保不会影响正在执行的事务。
- **始终保留最新版本**（`tableVer.latest`），即使没有活跃事务也不会被清理。
- **释放分页数据和缓冲池资源**，包括溢出到磁盘的临时文件。

### 6.4 提交日志的 SLRU 缓存

为了加速提交日志的查询，系统提供了一个 SLRU（Simple LRU）缓存：

```go
// pkg/mvcc/clog.go
type SLRU struct {
    size    int
    entries map[XID]TransactionStatus
    keys    []XID
}
```

当缓存满时，最旧的条目会被淘汰。这避免了提交日志无限增长占用内存。

---

## 7. 与内存存储引擎的集成

### 7.1 MVCCDataSource 概览

`MVCCDataSource` 是内存存储引擎的核心结构，定义在 `pkg/resource/memory/mvcc_datasource.go`：

```go
type MVCCDataSource struct {
    config     *domain.DataSourceConfig
    connected  bool

    indexManager *IndexManager   // 索引管理
    queryPlanner *QueryPlanner   // 查询优化
    bufferPool   *BufferPool     // 缓冲池（虚拟内存分页）

    nextTxID   int64             // 下一个事务 ID
    currentVer int64             // 当前全局版本号
    snapshots  map[int64]*Snapshot    // 活跃快照
    activeTxns map[int64]*Transaction // 活跃事务

    tables     map[string]*TableVersions // 表数据（多版本）
    tempTables map[string]bool           // 临时表
}
```

`MVCCDataSource` 同时实现了 `DataSource` 和 `TransactionalDataSource` 接口，并通过 `SupportsMVCC()` 方法声明其 MVCC 能力。

### 7.2 事务上下文传递

事务 ID 通过 Go 的 `context.Context` 进行传递。每个带事务的操作都会检查 context 中是否包含事务 ID：

```go
// pkg/resource/memory/context.go
type TransactionIDKey struct{}

func GetTransactionID(ctx context.Context) (int64, bool) {
    txnID, ok := ctx.Value(TransactionIDKey{}).(int64)
    return txnID, ok
}

func SetTransactionID(ctx context.Context, txnID int64) context.Context {
    return context.WithValue(ctx, TransactionIDKey{}, txnID)
}
```

在 INSERT / UPDATE / DELETE 操作中，系统首先检查是否在事务内：

```go
txnID, hasTxn := GetTransactionID(ctx)
if hasTxn {
    // 使用 COW 快照进行操作
} else {
    // 非事务模式，直接创建新版本
}
```

### 7.3 非事务模式的版本管理

即使没有显式事务，每次写操作也会创建新版本（自动提交模式）：

```go
// 非事务 INSERT 示例
m.currentVer++
newRows := append(existingRows, insertedRows...)
versionData := &TableData{
    version: m.currentVer,
    rows:    NewPagedRows(m.bufferPool, newRows, 0, tableName, m.currentVer),
}
tableVer.versions[m.currentVer] = versionData
tableVer.latest = m.currentVer
```

这种设计使得即使没有事务，数据修改操作也是原子的。

### 7.4 BufferPool 与分页存储

#### 7.4.1 设计目标

内存存储引擎将所有行数据保存在进程内存中。当数据量超过物理内存时，系统需要一种机制将冷数据透明地溢出到磁盘，并在需要时重新加载。`BufferPool` 就是这一机制的核心，它借鉴了传统数据库的缓冲区管理思想，但针对内存引擎的特点做了简化：

- **透明溢出**：上层代码无需关心数据是在内存还是磁盘，通过 Pin/Unpin 协议自动管理。
- **MVCC 感知淘汰**：优先淘汰旧版本的页面，保留最新版本在内存中。
- **无锁快速路径**：Pin 操作在页面已在内存时只需一次原子操作，避免锁竞争。

#### 7.4.2 核心数据结构

**PageID** -- 页面的唯一标识，由表名、版本号和页内索引组成：

```go
// pkg/resource/memory/paging.go
type PageID struct {
    Table   string  // 表名
    Version int64   // MVCC 版本号
    Index   int     // 页索引（0, 1, 2, ...）
}
```

**RowPage** -- 最小的淘汰单元，持有一个行切片：

```go
// pkg/resource/memory/paging.go
type RowPage struct {
    id        PageID
    rows      []domain.Row // nil 表示已被淘汰到磁盘
    rowCount  int          // 行数（淘汰后仍准确）
    sizeBytes int64        // 估算的内存占用（字节）
    onDisk    bool         // 是否已序列化到磁盘
    diskPath  string       // 溢出文件路径
    pinCount  int32        // 原子计数：> 0 表示正在使用，不可淘汰
    mu        sync.Mutex   // 保护 rows/onDisk/diskPath 的修改
}
```

**PagedRows** -- 替代 `[]domain.Row`，将行数据分割为多个 RowPage：

```go
// pkg/resource/memory/paged_rows.go
type PagedRows struct {
    pool      *BufferPool
    pages     []*RowPage
    totalRows int
    pageSize  int  // 每页行数（默认 4096）
}
```

**BufferPool** -- 全局缓冲池，管理所有 RowPage 的内存生命周期：

```go
// pkg/resource/memory/buffer_pool.go
type BufferPool struct {
    maxMemory      int64              // 内存上限（字节）
    usedMemory     int64              // 当前使用量（原子计数）
    spillDir       string             // 溢出文件目录
    pageSize       int                // 页大小
    evictInterval  time.Duration      // 后台淘汰间隔
    lru            *lruQueue          // LRU 队列
    latestVersions map[string]int64   // 表名 -> 最新版本号（淘汰优先级）
    stopCh         chan struct{}       // 停止信号
    disabled       bool               // 直通模式（不做内存管理）
}
```

#### 7.4.3 配置与初始化

```go
type PagingConfig struct {
    Enabled       bool          // 是否启用（false 则进入直通模式）
    MaxMemoryMB   int           // 内存上限，0 = 自动检测
    PageSize      int           // 每页行数，默认 4096
    SpillDir      string        // 溢出目录，默认 $TMPDIR/sqlexec-spill
    EvictInterval time.Duration // 后台淘汰间隔，默认 5 秒
}
```

自动内存检测策略：当 `MaxMemoryMB = 0` 时，使用 `runtime.MemStats.Sys * 70%` 作为上限，最低保证 64 MB。

#### 7.4.4 Pin/Unpin 协议

Pin/Unpin 是缓冲池的核心访问协议，借鉴自经典数据库的 Buffer Manager 设计：

```
Pin(page) → rows:
    1. 原子递增 pinCount（防止被淘汰）
    2. 快速路径：如果 page.rows != nil，直接返回（无锁）
    3. 慢速路径：加锁，从磁盘加载 rows，更新内存计数
    4. 返回 rows 引用

Unpin(page):
    1. 原子递减 pinCount
    2. 当 pinCount 降为 0 时，将 page 加入 LRU 队列尾部（最近使用）
```

**快速路径优化**：在正常查询场景中，页面几乎总是在内存中。此时 Pin 操作只需一次 `atomic.AddInt32` 和一次 nil 检查，**完全不获取互斥锁**，不触碰 LRU 队列。这保证了查询热路径的高性能。

**安全性保证**：`rows` 字段只在持有 `page.mu` 锁时才会被设为 nil（淘汰操作），而一旦 Pin 成功（pinCount > 0），淘汰器会跳过该页面。因此快速路径中观察到 `rows != nil` 后可以安全读取。

#### 7.4.5 页面创建与注册

当创建新的 `PagedRows` 时，行数据被均匀分割为多个 RowPage 并注册到 BufferPool：

```go
// pkg/resource/memory/paged_rows.go
func NewPagedRows(pool *BufferPool, rows []domain.Row, pageSize int, table string, version int64) *PagedRows {
    // 按 pageSize 切分 rows 为多个 RowPage
    for i := 0; i < totalRows; i += pageSize {
        page := &RowPage{
            id:        PageID{Table: table, Version: version, Index: len(pages)},
            rows:      rows[i:end],
            sizeBytes: estimatePageSize(rows[i:end]),
        }
        pool.Register(page)  // 注册到缓冲池，更新内存计数
    }
}
```

Register 操作会更新内存使用量，并在超出上限时**同步淘汰至多 4 个页面**（`maxSyncEvictions = 4`）。这限制了写操作路径上的阻塞时间，剩余的内存压力由后台淘汰器异步处理。

#### 7.4.6 内存大小估算

页面的内存占用通过**采样法**估算，避免遍历所有行带来的开销：

```go
// pkg/resource/memory/paging.go
func estimatePageSize(rows []domain.Row) int64 {
    if len(rows) <= 16 {
        // 行数 <= 16：精确计算每行
        return sum(estimateRowSize(row) for row in rows)
    }
    // 行数 > 16：采样前 16 行后外推
    avgRowSize := sum(estimateRowSize(rows[0:16])) / 16
    return avgRowSize * len(rows)
}
```

单行的内存估算考虑了所有值类型的实际开销：

| 类型 | 估算大小 |
|------|---------|
| `nil` | 0 |
| `bool` | 1 |
| `int` / `int64` / `float64` | 8 |
| `string` | `len(s) + 16`（header） |
| `[]byte` | `len(b) + 24`（slice header） |
| `time.Time` | 24 |
| `[]float32`（向量） | `len(v)*4 + 24` |
| `map[string]interface{}` | 递归估算 |

每行还包含 map 的基础开销：`64 + 8*len(row)` 字节。

#### 7.4.7 两层淘汰策略

LRU 队列采用**两层淘汰策略**，结合 MVCC 版本信息：

```
EvictCandidate(latestVersions):
    第一层：扫描 LRU 队列（从最久未使用开始），寻找旧版本的页面
        - 跳过 pinCount > 0 的页面（正在使用）
        - 跳过已淘汰的页面（rows == nil && onDisk）
        - 如果 page.Version < latestVersions[page.Table]，淘汰此页
          （旧 MVCC 版本只有 GC 前的活跃事务才需要，优先淘汰）

    第二层：如果没有旧版本页面可淘汰，退回到普通 LRU
        - 按最久未使用顺序淘汰任意 unpinned 页面
```

**为什么优先淘汰旧版本**：MVCC 会为每次写操作创建新版本。旧版本只在少数长事务中被读取，而最新版本几乎所有查询都会访问。因此旧版本是更好的淘汰候选。

#### 7.4.8 磁盘溢出与二进制编解码

淘汰操作将页面行数据序列化到磁盘：

```
TryEvict():
    1. 从 LRU 队列获取淘汰候选
    2. 加锁，二次检查（可能已被其他 goroutine 淘汰）
    3. 序列化到磁盘：page.rows → encodeRows() → 写文件
    4. 释放内存：page.rows = nil, page.onDisk = true
    5. 更新内存计数
```

溢出文件路径格式：`{spillDir}/{表名}_{版本号}_{页索引}.page`

**二进制编解码器**（`pkg/resource/memory/page_codec.go`）：

编解码器使用自定义的紧凑二进制格式，避免了 `encoding/gob` 因反射带来的性能问题（gob 对 `map[string]interface{}` 序列化 4K 行需要 20ms+，自定义编解码器快 10-50 倍）。

Wire 格式：

```
[rowCount:uint32]
for each row:
    [fieldCount:uint16]
    for each field:
        [keyLen:uint16][key:bytes]
        [typeTag:byte][value:bytes]
```

支持的类型标签：

| Tag | 类型 | 值编码 |
|-----|------|--------|
| 0 | `nil` | 无 |
| 1 | `bool` | 1 字节 |
| 2 | `int64` | 8 字节 LE |
| 3 | `float64` | 8 字节 LE（IEEE 754） |
| 4 | `string` | `[len:uint32][data:bytes]` |
| 5 | `[]byte` | `[len:uint32][data:bytes]` |
| 6 | `time.Time` | `[len:uint16][binary:bytes]` |
| 7 | `int` | 8 字节 LE（int64 编码） |
| 8 | `float32` | 4 字节 LE（IEEE 754） |
| 9 | `int32` | 4 字节 LE |

所有整数使用小端序（Little-Endian），编码函数内联友好。

#### 7.4.9 后台淘汰器

BufferPool 启动时创建一个后台 goroutine，定期检查内存使用情况：

```go
func (bp *BufferPool) backgroundEvictor() {
    ticker := time.NewTicker(bp.evictInterval) // 默认 5 秒
    for {
        select {
        case <-ticker.C:
            // 循环淘汰直到内存降到上限以下
            for usedMemory > maxMemory {
                if !bp.TryEvict() {
                    break // 没有可淘汰的页面
                }
            }
        case <-bp.stopCh:
            return
        }
    }
}
```

**两阶段淘汰协作**：

- **同步淘汰**：Register 时，如果超出内存上限，最多同步淘汰 4 个页面。这保证了写操作不会无限制阻塞。
- **异步淘汰**：后台淘汰器每 5 秒检查一次，持续淘汰直到内存回到安全线以下。

这种设计在写入密集的场景下，将淘汰成本分摊到后台，避免写入延迟尖刺。

#### 7.4.10 PagedRows 的访问接口

`PagedRows` 提供三种访问方式，适应不同使用场景：

| 方法 | 用途 | 页面管理 |
|------|------|---------|
| `Get(i)` | 按索引访问单行 | 自动 Pin/Unpin 单页 |
| `Materialize()` | 返回完整 `[]domain.Row` 切片 | 逐页 Pin → 复制 → Unpin |
| `Range(fn)` | 迭代所有行 | 逐页 Pin → 遍历 → Unpin |

`Materialize()` 是主要的兼容桥接方法 -- 已有的使用 `[]domain.Row` 的代码通过调用 `Materialize()` 获取完整行切片，无需修改。页面在复制完成后立即 Unpin，成为淘汰候选。

`Range(fn)` 是最内存高效的方式 -- 同一时刻只有一个页面被 Pin，适合大表的流式处理。

#### 7.4.11 生命周期管理

```
NewPagedRows(pool, rows)     → 创建页面，注册到 BufferPool
  ↓
query → Pin/Unpin             → 查询时按需加载/释放
  ↓
gcOldVersions()               → MVCC GC 时调用 PagedRows.Release()
  ↓
Release()                     → 注销所有页面，清理溢出文件
  ↓
BufferPool.Close()            → 停止后台淘汰器，删除溢出目录
```

`Release()` 由 MVCC 垃圾回收器在清理旧版本时调用，确保旧版本的磁盘溢出文件不会泄漏。

### 7.5 数据源能力降级

MVCC 系统设计了优雅的降级机制。当数据源不支持 MVCC 时，系统可以自动降级到非 MVCC 模式：

```go
// pkg/mvcc/manager.go
func (m *Manager) Begin(level IsolationLevel, features *DataSourceFeatures) (*Transaction, error) {
    if !m.checkMVCCCapability(features) {
        return m.beginNonMVCC(level)
    }
    // ... MVCC 事务 ...
}
```

数据源能力分为四个等级：

| 等级 | 值 | 含义 |
|------|-----|------|
| `CapabilityNone` | 0 | 不支持 MVCC |
| `CapabilityReadSnapshot` | 1 | 支持读快照 |
| `CapabilityWriteVersion` | 2 | 支持写多版本 |
| `CapabilityFull` | 3 | 完全支持 MVCC |

`DowngradeHandler` 负责在查询和写入前检查数据源能力，在配置允许时自动降级，否则返回错误。

### 7.6 与 API 层的集成

API 层通过 `Session` 和 `Transaction` 对象向用户暴露事务功能：

```go
// 开始事务
tx, err := session.Begin()

// 在事务中执行操作
result, err := tx.Query("SELECT * FROM users")

// 提交或回滚
err = tx.Commit()   // 或 tx.Rollback()
```

`Transaction` 对象内部持有 `domain.Transaction` 接口引用，该接口由 `MVCCTransaction` 实现，最终委托给 `MVCCDataSource` 的 `CommitTx` / `RollbackTx` 方法。不支持嵌套事务 -- 若在已有事务的会话中再次调用 `Begin()`，会返回错误。

---

## 8. 总结

SQLExec 的 MVCC 实现具有以下特点：

1. **PostgreSQL 风格的设计**：XID、Snapshot、TupleVersion、CommitLog 等核心概念均参照 PostgreSQL 的设计思路。
2. **两层架构**：抽象层（`pkg/mvcc/`）提供通用的 MVCC 语义，存储层（`pkg/resource/memory/`）负责实际的数据版本管理。
3. **行级 COW 优化**：避免每次写操作复制全表数据，只读事务开销几乎为零。
4. **自动 GC**：后台 goroutine 定期清理过期版本和日志，事务结束时也触发增量 GC。
5. **优雅降级**：对不支持 MVCC 的数据源，系统可自动降级到非 MVCC 模式，保持兼容性。
6. **分页虚拟内存**：通过 BufferPool 实现 Pin/Unpin 协议、两层 MVCC 感知 LRU 淘汰、自定义二进制编解码的磁盘溢出，使内存引擎能处理超过物理内存的数据集。
