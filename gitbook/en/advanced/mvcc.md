# MVCC and Transactions

SQLExec's in-memory storage engine implements PostgreSQL-style Multi-Version Concurrency Control (MVCC), supporting full ACID transactions.

## Transaction Isolation Levels

| Level | Dirty Read | Non-Repeatable Read | Phantom Read | Description |
|------|------|-----------|------|------|
| READ UNCOMMITTED | Possible | Possible | Possible | Lowest isolation, can read uncommitted data |
| READ COMMITTED | Impossible | Possible | Possible | Only reads committed data |
| **REPEATABLE READ** | Impossible | Impossible | Possible | **Default level**, snapshot consistency within a transaction |
| SERIALIZABLE | Impossible | Impossible | Impossible | Highest isolation, fully serialized |

## Using Transactions

### SQL

```sql
-- Begin a transaction
BEGIN;

-- Execute operations
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;

-- Commit or rollback
COMMIT;
-- Or ROLLBACK;
```

### Setting the Isolation Level

```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN;
-- ...
COMMIT;
```

### Embedded Go API

```go
// Set the isolation level
session.SetIsolationLevel(api.IsolationSerializable)

// Begin a transaction
tx, err := session.Begin()
if err != nil {
    log.Fatal(err)
}

// Execute operations
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

// Commit
tx.Commit()
```

## How MVCC Works

1. **Snapshot Isolation**: Each transaction obtains a version snapshot at its start and can only see data that was committed at that snapshot point in time
2. **Multi-Version Storage**: Data modifications do not overwrite old versions; instead, new versions are created
3. **Write Conflict Detection**: When two transactions modify the same row concurrently, the later commit detects the conflict
4. **Garbage Collection**: Old versions no longer referenced by any active transaction are automatically cleaned up

## Configuration

Configure MVCC parameters in `config.json`:

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

| Parameter | Default | Description |
|------|--------|------|
| `enable_warning` | `true` | Enable MVCC-related warning logs |
| `auto_downgrade` | `true` | Automatically downgrade isolation level when data source does not support it |
| `gc_interval` | `5m` | Interval between version garbage collection runs |
| `gc_age_threshold` | `1h` | Retention time for old versions; versions older than this may be collected |
| `xid_wrap_threshold` | `100000` | Transaction ID wraparound warning threshold |
| `max_active_txns` | `10000` | Maximum number of concurrently active transactions |

## Data Source Support

| Data Source | MVCC Transactions | Description |
|--------|----------|------|
| Memory | Full support | Native MVCC implementation |
| MySQL | Uses native transactions | MySQL's own InnoDB transactions |
| PostgreSQL | Uses native transactions | PostgreSQL's own MVCC |
| CSV/JSON/JSONL/Excel | Not supported | Files loaded into memory, no transactions |
| HTTP | Not supported | Remote API, no transaction semantics |
