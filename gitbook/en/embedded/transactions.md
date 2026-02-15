# Transaction Management

SQLExec's in-memory data source supports full MVCC (Multi-Version Concurrency Control) transactions, providing four standard isolation levels from READ UNCOMMITTED to SERIALIZABLE.

## Basic Usage

### Starting a Transaction

Use `session.Begin()` to start a new transaction:

```go
tx, err := session.Begin()
if err != nil {
    log.Fatal(err)
}
```

Note: SQLExec does not support nested transactions. If the current session already has an active transaction, calling `Begin()` again will return an error.

### Operations Within a Transaction

The `Transaction` object provides `Query` and `Execute` methods for executing SQL within a transaction:

```go
// Query within a transaction
query, err := tx.Query("SELECT balance FROM accounts WHERE id = ?", 1)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Execute within a transaction
result, err := tx.Execute("UPDATE accounts SET balance = ? WHERE id = ?", newBalance, 1)
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}
```

### Commit and Rollback

```go
// Commit the transaction
err := tx.Commit()
if err != nil {
    log.Fatal(err)
}

// Or rollback the transaction
err := tx.Rollback()
if err != nil {
    log.Fatal(err)
}
```

### Transaction Methods Overview

| Method | Signature | Description |
|--------|-----------|-------------|
| `Query()` | `Query(sql string, args ...interface{}) (*Query, error)` | Query within a transaction, supports parameter binding |
| `Execute()` | `Execute(sql string, args ...interface{}) (*Result, error)` | Execute DML within a transaction |
| `Commit()` | `Commit() error` | Commit the transaction |
| `Rollback()` | `Rollback() error` | Rollback the transaction |
| `Close()` | `Close() error` | Close the transaction (equivalent to Rollback) |
| `IsActive()` | `IsActive() bool` | Check if the transaction is still active |

## Isolation Levels

SQLExec supports four standard transaction isolation levels:

| Constant | SQL Name | Description |
|----------|----------|-------------|
| `api.IsolationReadUncommitted` | `READ UNCOMMITTED` | Allows reading uncommitted data (dirty reads) |
| `api.IsolationReadCommitted` | `READ COMMITTED` | Only reads committed data; two reads within the same transaction may return different results |
| `api.IsolationRepeatableRead` | `REPEATABLE READ` | Multiple reads within the same transaction return consistent results (default level) |
| `api.IsolationSerializable` | `SERIALIZABLE` | Strictest isolation; transactions are fully serialized |

### Setting the Isolation Level

The isolation level must be set before starting a transaction:

```go
// Method 1: Specify when creating a Session
session := db.SessionWithOptions(&api.SessionOptions{
    Isolation: api.IsolationSerializable,
})

// Method 2: Set at runtime (affects subsequent new transactions)
session.SetIsolationLevel(api.IsolationReadCommitted)

// Query the current isolation level
level := session.IsolationLevel()
fmt.Println("Current isolation level:", level) // Output: READ COMMITTED
```

### Checking Transaction Status

```go
// Check if currently in a transaction
if session.InTransaction() {
    fmt.Println("Currently in a transaction")
}

// Check if the transaction is active
if tx.IsActive() {
    fmt.Println("Transaction is still active")
}
```

## Transfer Example

The following is a classic bank transfer scenario demonstrating the complete usage of transactions:

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
    // Initialize
    db, _ := api.NewDB(nil)
    defer db.Close()

    memDS := memory.NewMVCCDataSource(&domain.DataSourceConfig{Writable: true})
    memDS.Connect(context.Background())
    db.RegisterDataSource("default", memDS)

    session := db.Session()
    defer session.Close()

    // Create accounts table and initialize data
    session.Execute(`
        CREATE TABLE accounts (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            balance FLOAT
        )
    `)
    session.Execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.00)")
    session.Execute("INSERT INTO accounts VALUES (2, 'Bob', 500.00)")

    // Perform transfer
    err := transfer(session, 1, 2, 200.00)
    if err != nil {
        log.Printf("Transfer failed: %v", err)
    }

    // Check balances after transfer
    rows, _ := session.QueryAll("SELECT name, balance FROM accounts ORDER BY id")
    for _, row := range rows {
        fmt.Printf("%v: $%.2f\n", row["name"], row["balance"])
    }
    // Output:
    // Alice: $800.00
    // Bob: $700.00
}

func transfer(session *api.Session, fromID, toID int, amount float64) error {
    // Start transaction
    tx, err := session.Begin()
    if err != nil {
        return fmt.Errorf("failed to start transaction: %w", err)
    }

    // Ensure the transaction is eventually handled (Close will auto-rollback if neither Commit nor Rollback was called)
    defer tx.Close()

    // Query the source account balance
    query, err := tx.Query("SELECT balance FROM accounts WHERE id = ?", fromID)
    if err != nil {
        return fmt.Errorf("failed to query source account: %w", err)
    }

    if !query.Next() {
        return fmt.Errorf("source account %d does not exist", fromID)
    }

    var balance float64
    if err := query.Scan(&balance); err != nil {
        return fmt.Errorf("failed to read balance: %w", err)
    }
    query.Close()

    // Check if balance is sufficient
    if balance < amount {
        // Insufficient balance, transaction will be rolled back (defer tx.Close() handles it)
        return fmt.Errorf("insufficient balance: current $%.2f, required $%.2f", balance, amount)
    }

    // Deduct from source account
    _, err = tx.Execute(
        "UPDATE accounts SET balance = balance - ? WHERE id = ?",
        amount, fromID,
    )
    if err != nil {
        return fmt.Errorf("deduction failed: %w", err)
    }

    // Add to destination account
    _, err = tx.Execute(
        "UPDATE accounts SET balance = balance + ? WHERE id = ?",
        amount, toID,
    )
    if err != nil {
        return fmt.Errorf("deposit failed: %w", err)
    }

    // Commit transaction
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    fmt.Printf("Transfer successful: $%.2f from account %d to account %d\n", amount, fromID, toID)
    return nil
}
```

## MVCC Mechanism

The in-memory data source (`memory.MVCCDataSource`) implements full MVCC multi-version concurrency control:

- **Snapshot Isolation**: Each transaction obtains a consistent snapshot of the data when it begins; subsequent read operations are based on this snapshot
- **Non-blocking Reads**: Read operations do not block write operations, and write operations do not block read operations
- **Write Conflict Detection**: When two transactions modify the same row concurrently, the later commit will detect the conflict
- **Version Chain**: Each modification creates a new version, with old versions retained for active transactions to read

```
Timeline:
  TX1: Begin ──── Read(A=100) ────────────────── Read(A=100) ── Commit
  TX2: ──────── Begin ── Write(A=200) ── Commit ────────────────────────
                         │
                         └─ TX1 still reads A=100 (snapshot isolation)
```

## Transaction Handling on Session Close

When a Session is closed, any uncommitted transactions are automatically rolled back:

```go
session := db.Session()

tx, _ := session.Begin()
tx.Execute("INSERT INTO users VALUES (1, 'temp')")
// Not committed...

session.Close() // Automatically rolls back the uncommitted transaction; the INSERT data will not be retained
```

## Data Source Transaction Support

| Data Source Type | Transaction Support | MVCC | Description |
|-----------------|-------------------|------|-------------|
| Memory | Full support | Supported | Complete MVCC snapshot isolation |
| Slice Adapter | Supported | Configurable | Enable with `WithMVCC(true)` |
| CSV | Not supported | Not supported | File data source, read-only or single write |
| JSON | Not supported | Not supported | File data source, read-only or single write |
| MySQL / PostgreSQL | Depends on remote database | Depends on remote database | Transparently passes through to the remote database's transaction mechanism |

{% hint style="warning" %}
File-based data sources (CSV, JSON, Excel, etc.) do not support transactions. Calling `session.Begin()` on such data sources may return an error or exhibit unpredictable behavior. It is recommended to load file data source contents into an in-memory data source for transactional operations.
{% endhint %}

## Next Steps

- [GORM Driver](gorm-driver.md) -- Use transactions through the GORM ORM framework
- [Slice Adapter](slice-adapter.md) -- Perform transactional operations on Go data structures
- [MVCC and Transactions](../advanced/mvcc.md) -- Deep dive into MVCC implementation details
