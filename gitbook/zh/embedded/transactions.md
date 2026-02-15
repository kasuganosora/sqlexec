# 事务管理

SQLExec 的内存数据源支持完整的 MVCC（多版本并发控制）事务，提供从 READ UNCOMMITTED 到 SERIALIZABLE 的四种标准隔离级别。

## 基本用法

### 开启事务

使用 `session.Begin()` 开启一个新事务：

```go
tx, err := session.Begin()
if err != nil {
    log.Fatal(err)
}
```

注意：SQLExec 不支持嵌套事务。如果当前会话已有活跃事务，再次调用 `Begin()` 会返回错误。

### 事务内操作

事务对象 `Transaction` 提供了 `Query` 和 `Execute` 方法用于在事务内执行 SQL：

```go
// 事务内查询
query, err := tx.Query("SELECT balance FROM accounts WHERE id = ?", 1)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// 事务内执行
result, err := tx.Execute("UPDATE accounts SET balance = ? WHERE id = ?", newBalance, 1)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}
```

### 提交和回滚

```go
// 提交事务
err := tx.Commit()
if err != nil {
    log.Fatal(err)
}

// 或者回滚事务
err := tx.Rollback()
if err != nil {
    log.Fatal(err)
}
```

### Transaction 方法一览

| 方法 | 签名 | 说明 |
|------|------|------|
| `Query()` | `Query(sql string, args ...interface{}) (*Query, error)` | 事务内查询，支持参数绑定 |
| `Execute()` | `Execute(sql string, args ...interface{}) (*Result, error)` | 事务内执行 DML |
| `Commit()` | `Commit() error` | 提交事务 |
| `Rollback()` | `Rollback() error` | 回滚事务 |
| `Close()` | `Close() error` | 关闭事务（等同于 Rollback） |
| `IsActive()` | `IsActive() bool` | 检查事务是否仍然活跃 |

## 隔离级别

SQLExec 支持四种标准的事务隔离级别：

| 常量 | SQL 名称 | 说明 |
|------|---------|------|
| `api.IsolationReadUncommitted` | `READ UNCOMMITTED` | 允许读取未提交的数据（脏读） |
| `api.IsolationReadCommitted` | `READ COMMITTED` | 只读取已提交的数据，同一事务内两次读取可能不一致 |
| `api.IsolationRepeatableRead` | `REPEATABLE READ` | 同一事务内多次读取结果一致（默认级别） |
| `api.IsolationSerializable` | `SERIALIZABLE` | 最严格的隔离，事务完全串行化 |

### 设置隔离级别

隔离级别需要在开启事务之前设置：

```go
// 方法一：创建 Session 时指定
session := db.SessionWithOptions(&api.SessionOptions{
    Isolation: api.IsolationSerializable,
})

// 方法二：运行时设置（影响后续新事务）
session.SetIsolationLevel(api.IsolationReadCommitted)

// 查询当前隔离级别
level := session.IsolationLevel()
fmt.Println("当前隔离级别:", level) // 输出: READ COMMITTED
```

### 检查事务状态

```go
// 检查是否在事务中
if session.InTransaction() {
    fmt.Println("当前在事务中")
}

// 检查事务是否活跃
if tx.IsActive() {
    fmt.Println("事务仍然活跃")
}
```

## 转账示例

以下是一个经典的银行转账场景，演示事务的完整用法：

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

func main() {
    // 初始化
    db, _ := api.NewDB(nil)
    defer db.Close()

    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{Writable: true})
    memDS.Connect(context.Background())
    db.RegisterDataSource("default", memDS)

    session := db.Session()
    defer session.Close()

    // 创建账户表并初始化数据
    session.Execute(`
        CREATE TABLE accounts (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            balance FLOAT
        )
    `)
    session.Execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.00)")
    session.Execute("INSERT INTO accounts VALUES (2, 'Bob', 500.00)")

    // 执行转账
    err := transfer(session, 1, 2, 200.00)
    if err != nil {
        log.Printf("转账失败: %v", err)
    }

    // 查看转账后余额
    rows, _ := session.QueryAll("SELECT name, balance FROM accounts ORDER BY id")
    for _, row := range rows {
        fmt.Printf("%v: $%.2f\n", row["name"], row["balance"])
    }
    // 输出:
    // Alice: $800.00
    // Bob: $700.00
}

func transfer(session *api.Session, fromID, toID int, amount float64) error {
    // 开启事务
    tx, err := session.Begin()
    if err != nil {
        return fmt.Errorf("开启事务失败: %w", err)
    }

    // 确保事务最终会被处理（如果既没 Commit 也没 Rollback，Close 会自动 Rollback）
    defer tx.Close()

    // 查询转出账户余额
    query, err := tx.Query("SELECT balance FROM accounts WHERE id = ?", fromID)
    if err != nil {
        return fmt.Errorf("查询转出账户失败: %w", err)
    }

    if !query.Next() {
        return fmt.Errorf("转出账户 %d 不存在", fromID)
    }

    var balance float64
    if err := query.Scan(&balance); err != nil {
        return fmt.Errorf("读取余额失败: %w", err)
    }
    query.Close()

    // 检查余额是否充足
    if balance < amount {
        // 余额不足，事务回滚（defer tx.Close() 会处理）
        return fmt.Errorf("余额不足: 当前 $%.2f, 需要 $%.2f", balance, amount)
    }

    // 扣减转出账户
    _, err = tx.Execute(
        "UPDATE accounts SET balance = balance - ? WHERE id = ?",
        amount, fromID,
    )
    if err != nil {
        return fmt.Errorf("扣减失败: %w", err)
    }

    // 增加转入账户
    _, err = tx.Execute(
        "UPDATE accounts SET balance = balance + ? WHERE id = ?",
        amount, toID,
    )
    if err != nil {
        return fmt.Errorf("转入失败: %w", err)
    }

    // 提交事务
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("提交事务失败: %w", err)
    }

    fmt.Printf("转账成功: $%.2f 从账户 %d 到账户 %d\n", amount, fromID, toID)
    return nil
}
```

## MVCC 机制

内存数据源（`memory.MVCCDataSource`）实现了完整的 MVCC 多版本并发控制：

- **快照隔离**：每个事务在开始时获得数据的一致性快照，后续读操作基于该快照
- **非阻塞读**：读操作不会阻塞写操作，写操作不会阻塞读操作
- **写冲突检测**：当两个事务同时修改同一行时，后提交的事务会检测到冲突
- **版本链**：每次修改创建新版本，旧版本保留供活跃事务读取

```
时间线:
  TX1: Begin ──── Read(A=100) ────────────────── Read(A=100) ── Commit
  TX2: ──────── Begin ── Write(A=200) ── Commit ────────────────────────
                         │
                         └─ TX1 仍然读到 A=100（快照隔离）
```

## Session 关闭时的事务处理

当 Session 关闭时，如果存在未提交的事务，会自动回滚：

```go
session := db.Session()

tx, _ := session.Begin()
tx.Execute("INSERT INTO users VALUES (1, 'temp')")
// 未提交...

session.Close() // 自动回滚未提交的事务，INSERT 的数据不会保留
```

## 数据源事务支持情况

| 数据源类型 | 事务支持 | MVCC | 说明 |
|-----------|---------|------|------|
| Memory（内存） | 完整支持 | 支持 | 完整的 MVCC 快照隔离 |
| Slice 适配器 | 支持 | 可配置 | 通过 `WithMVCC(true)` 启用 |
| CSV | 不支持 | 不支持 | 文件数据源，只读或单次写入 |
| JSON | 不支持 | 不支持 | 文件数据源，只读或单次写入 |
| MySQL / PostgreSQL | 依赖远程数据库 | 依赖远程数据库 | 透传到远程数据库的事务机制 |

{% hint style="warning" %}
文件数据源（CSV、JSON、Excel 等）不支持事务。对这类数据源调用 `session.Begin()` 可能会返回错误或行为不可预期。建议将文件数据源的数据加载到内存数据源中进行事务操作。
{% endhint %}

## 下一步

- [GORM 驱动](gorm-driver.md) -- 通过 GORM ORM 框架使用事务
- [Slice 适配器](slice-adapter.md) -- 对 Go 数据结构进行事务操作
- [MVCC 与事务](../advanced/mvcc.md) -- 深入了解 MVCC 实现原理
