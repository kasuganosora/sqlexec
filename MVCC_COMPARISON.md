# TiDB vs PostgreSQL MVCC 机制对比

## 概述

本文档对比TiDB和PostgreSQL的MVCC（多版本并发控制）实现机制，为本项目的MVCC实现提供参考。

## 核心差异

### 1. 架构差异

| 特性 | TiDB | PostgreSQL |
|------|-------|-----------|
| **架构** | 分布式 | 单机 |
| **存储** | TiKV (分布式KV存储) | 堆表 + 索引 |
| **事务模型** | 乐观事务 | 悲观/乐观混合 |
| **提交协议** | 两阶段提交 (2PC) | 单阶段提交 |
| **时钟** | 逻辑时间戳 (TSO) | 事务ID (XID) |

### 2. MVCC数据结构

#### TiDB (Percolator模型)

```
Key: Value
  ├─ data: 实际数据
  ├─ lock: 锁信息 (commit_ts, start_ts)
  └─ write: 写入记录 (start_ts, commit_ts)
```

**版本链:**
```
key@100 -> value_v3
key@80  -> value_v2
key@50  -> value_v1
```

**时间戳类型:**
- `start_ts`: 事务开始时间戳
- `commit_ts`: 事务提交时间戳
- `for_update_ts`: 悲观锁时间戳

#### PostgreSQL (Tuple模型)

```
Tuple (每行数据):
  ├─ 用户数据字段
  └─ 隐藏系统字段:
      ├─ xmin: 创建事务ID
      ├─ xmax: 删除事务ID
      ├─ cmin: 命令序号 (创建)
      └─ cmax: 命令序号 (删除)
```

**版本链:**
```
Page -> Tuple1 (xmin=100, xmax=200)
      -> Tuple2 (xmin=200, xmax=300)
      -> Tuple3 (xmin=300, xmax=0)  ← 最新版本
```

**事务ID (XID):**
- 32位递增计数器
- 环绕时需要冻结 (VACUUM)
- 存储在pg_clog中

### 3. 可见性判断

#### TiDB可见性规则

```
可见性条件:
1. commit_ts <= my_start_ts
2. 不存在未提交的锁
3. commit_ts >= min_start_ts (如果是Repeatable Read)

读取算法:
1. 从最新版本开始回溯
2. 找到第一个满足可见性的版本
3. 如果遇到锁，等待锁释放
```

**示例:**
```go
// TiDB简化版本可见性
func isVisible(commitTS, startTS, myStartTS uint64) bool {
    if commitTS > myStartTS {
        return false  // 未来事务，不可见
    }
    return true
}
```

#### PostgreSQL可见性规则

```
基本规则:
1. row.xmin <= my_xmin  AND
2. (row.xmax = 0 OR row.xmax > my_xmin)

特殊情况:
- 自身事务: cmin/cmax判断可见性
- 已提交事务: 检查pg_clog状态
- 回滚事务: 不可见
- 进行中事务: 不可见 (除非是自身)
```

**示例:**
```go
// PG简化版本可见性
func isVisible(rowXmin, rowXmax, myXmin uint32) bool {
    if rowXmin > myXmin {
        return false  // 未来事务
    }
    if rowXmax != 0 && rowXmax <= myXmin {
        return false  // 已被删除
    }
    return true
}
```

### 4. 事务流程

#### TiDB (Percolator两阶段提交)

```
1. Prewrite阶段:
   - 获取start_ts
   - 写入锁信息
   - 检测写写冲突
   
2. Commit阶段:
   - 获取commit_ts
   - 提交主键(primary key)
   - 提交其他键(secondary keys)
   
3. Cleanup阶段:
   - 清理锁信息
   - 清理旧版本

冲突处理:
- 检测到锁 -> 等待
- 写写冲突 -> 重试 (乐观)
```

**代码示例:**
```go
// TiDB事务流程
func (txn *Transaction) Commit() error {
    // 1. Prewrite
    for _, key := range txn.keys {
        if err := txn.prewrite(key); err != nil {
            return err  // 冲突，回滚
        }
    }
    
    // 2. Commit
    commitTS := tsOracle.Get()
    if err := txn.commitPrimary(commitTS); err != nil {
        return err
    }
    
    // 3. Commit secondary
    for _, key := range txn.keys[1:] {
        txn.commitSecondary(key, commitTS)
    }
    
    return nil
}
```

#### PostgreSQL (单阶段提交)

```
1. BEGIN:
   - 分配XID
   - 创建快照
   - 标记事务状态为IN_PROGRESS

2. 读写操作:
   - 写入时设置xmin
   - 删除时设置xmax
   - 根据快照判断可见性

3. COMMIT:
   - 标记事务为COMMITTED
   - 写入pg_clog
   - 释放锁

4. ROLLBACK:
   - 标记事务为ABORTED
   - 写入pg_clog
   - 释放锁

死锁检测:
- 检测锁等待环
- 中断其中一个事务
```

**代码示例:**
```go
// PG事务流程
func (txn *Transaction) Commit() error {
    // 1. 标记为已提交
    txn.status = TxnStatusCommitted
    
    // 2. 写入clog
    clog.SetStatus(txn.xid, TxnStatusCommitted)
    
    // 3. 释放所有锁
    txn.releaseLocks()
    
    return nil
}
```

### 5. 隔离级别

#### TiDB

| 隔离级别 | 实现 | 快照时间 |
|---------|------|---------|
| Read Uncommitted | 不推荐，可能读到未提交数据 | 当前时间 |
| Read Committed | 每个语句使用新快照 | 每次查询获取新TSO |
| Repeatable Read (默认) | 整个事务使用同一快照 | 事务开始时获取TSO |
| Serializable | 通过SSI检测串行化异常 | 事务开始时获取TSO |

#### PostgreSQL

| 隔离级别 | 实现 | 快照时间 |
|---------|------|---------|
| Read Uncommitted | 不推荐，行为同RC | 当前时间 |
| Read Committed | 每个语句使用新快照 | 每次查询获取新快照 |
| Repeatable Read (默认) | 整个事务使用同一快照 | 事务开始时创建快照 |
| Serializable | 通过SSI检测串行化异常 | 事务开始时创建快照 |

### 6. 垃圾回收 (GC/VACUUM)

#### TiDB

- **机制**: 定期清理过期版本
- **依据**: GC Safe Point
- **策略**: 
  - 每分钟计算一次GC Safe Point
  - 删除commit_ts < safe_point的版本
- **优势**: 分布式并行清理
- **劣势**: 需要协调GC时间点

#### PostgreSQL

- **机制**: VACUUM进程
- **依据**: 事务ID环绕点
- **策略**:
  - Autovacuum: 自动触发
  - VACUUM FULL: 重写表
  - VACUUM: 标记死元组
- **优势**: 细粒度控制
- **劣势**: 需要手动优化参数

### 7. 性能特性

| 指标 | TiDB | PostgreSQL |
|------|-------|-----------|
| **读性能** | 高 (无锁读取) | 高 (快照读) |
| **写性能** | 中 (需要2PC) | 高 (单机提交) |
| **冲突处理** | 重试 (乐观) | 等待/死锁检测 (悲观) |
| **扩展性** | 水平扩展 | 垂直扩展 |
| **延迟** | 较高 (网络往返) | 较低 (本地操作) |

### 8. 锁机制

#### TiDB (Percolator Lock)

```
Lock Structure:
├─ key: 主键
├─ primary: 指向主键
├─ start_ts: 事务开始时间
├─ for_update_ts: 悲观锁时间
└─ op: 操作类型 (Put/Del)

锁类型:
- PessimisticLock: 悲观锁 (for update)
- OptimisticLock: 乐观锁 (默认)
```

**冲突检测:**
```
写入冲突检测:
1. 检查Key是否被锁定
2. 检查是否有更新的commit_ts > start_ts
3. 如果冲突，回滚并重试
```

#### PostgreSQL (Tuple Lock)

```
Lock Table:
├─ locktag: 锁对象标识
├─ transaction: 持有锁的事务XID
└─ mode: 锁模式 (AccessShare/RowShare等)

锁模式:
- ACCESS SHARE: SELECT
- ROW SHARE: SELECT FOR SHARE
- ROW EXCLUSIVE: INSERT, UPDATE, DELETE
- SHARE: CREATE INDEX
- ...
- ACCESS EXCLUSIVE: VACUUM
```

**死锁检测:**
```
等待图检测:
1. 构建事务等待图
2. 检测是否有环
3. 如果有环，中断其中一个事务
```

## 总结

### TiDB MVCC特点

✅ **优势:**
- 分布式扩展能力
- 读写不阻塞
- 自动冲突检测和重试

❌ **劣势:**
- 两阶段提交开销大
- 网络往返延迟
- 需要TSO服务

### PostgreSQL MVCC特点

✅ **优势:**
- 实现简单直观
- 性能稳定
- 工具成熟完善

❌ **劣势:**
- 不支持分布式
- VACUUM开销大
- 事务ID环绕问题

### 本项目实现选择

基于以上分析，本项目采用**PostgreSQL风格的MVCC**，原因:

1. **简单性**: 适合当前单机架构
2. **可理解性**: 语义清晰，易于调试
3. **性能**: 单机场景下性能最优
4. **兼容性**: 类似PG，用户熟悉

**核心特性:**
- 事务ID (XID) 机制
- xmin/xmax 隐藏字段
- 快照隔离
- 可见性规则
- 自动降级机制

## 参考文献

1. TiDB Documentation: https://docs.pingcap.com/tidb/stable/
2. PostgreSQL MVCC: https://www.postgresql.org/docs/current/mvcc.html
3. Percolator Paper: https://www.usenix.org/system/conference/nsdi12
4. PG Concurrency: https://www.postgresql.org/docs/current/concurrency-control.html
