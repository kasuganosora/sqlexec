# MVCC 与事务

SQLExec 的内存存储引擎实现了 PostgreSQL 风格的多版本并发控制（MVCC），支持完整的 ACID 事务。

## 事务隔离级别

| 级别 | 脏读 | 不可重复读 | 幻读 | 说明 |
|------|------|-----------|------|------|
| READ UNCOMMITTED | 可能 | 可能 | 可能 | 最低隔离，可读未提交数据 |
| READ COMMITTED | 不可能 | 可能 | 可能 | 只读已提交数据 |
| **REPEATABLE READ** | 不可能 | 不可能 | 可能 | **默认级别**，事务内快照一致 |
| SERIALIZABLE | 不可能 | 不可能 | 不可能 | 最高隔离，完全串行化 |

## 使用事务

### SQL 方式

```sql
-- 开启事务
BEGIN;

-- 执行操作
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;

-- 提交或回滚
COMMIT;
-- 或 ROLLBACK;
```

### 设置隔离级别

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN;
-- ...
COMMIT;
```

### 嵌入式 Go API

```go
// 设置隔离级别
session.SetIsolationLevel(api.IsolationSerializable)

// 开启事务
tx, err := session.Begin()
if err != nil {
    log.Fatal(err)
}

// 执行操作
_, err = tx.Execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
if err != nil {
    tx.Rollback()
    return
}

_, err = tx.Execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
if err != nil {
    tx.Rollback()
    return
}

// 提交
tx.Commit()
```

## MVCC 工作原理

1. **快照隔离**：每个事务开始时获取一个版本快照，只能看到快照时刻已提交的数据
2. **多版本存储**：数据修改不覆盖旧版本，而是创建新版本
3. **写冲突检测**：两个事务同时修改同一行时，后提交的事务会检测到冲突
4. **垃圾回收**：不再被任何活跃事务引用的旧版本会被自动清理

## 配置

在 `config.json` 中配置 MVCC 参数：

```json
{
  "mvcc": {
    "enable_warning": true,
    "auto_downgrade": true,
    "gc_interval": "5m",
    "gc_age_threshold": "1h",
    "xid_wrap_threshold": 100000,
    "max_active_txns": 10000
  }
}
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `enable_warning` | `true` | 启用 MVCC 相关警告日志 |
| `auto_downgrade` | `true` | 数据源不支持时自动降级隔离级别 |
| `gc_interval` | `5m` | 版本垃圾回收执行间隔 |
| `gc_age_threshold` | `1h` | 旧版本保留时间，超过后可被回收 |
| `xid_wrap_threshold` | `100000` | 事务 ID 回绕警告阈值 |
| `max_active_txns` | `10000` | 最大同时活跃事务数 |

## 数据源支持情况

| 数据源 | MVCC 事务 | 说明 |
|--------|----------|------|
| Memory | 完整支持 | 原生 MVCC 实现 |
| MySQL | 使用原生事务 | MySQL 自身的 InnoDB 事务 |
| PostgreSQL | 使用原生事务 | PostgreSQL 自身的 MVCC |
| CSV/JSON/JSONL/Excel | 不支持 | 文件加载到内存，无事务 |
| HTTP | 不支持 | 远程 API，无事务语义 |
