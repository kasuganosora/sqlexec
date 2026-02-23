# Memory Data Source

Memory is the default data source for SQLExec. All data is stored in memory with full read/write operations and MVCC transaction support. It serves as the core engine of SQLExec — all file-based data sources (CSV, JSON, Excel, etc.) are built on top of the Memory data source.

Suitable for temporary data processing, testing environments, embedded applications, and scenarios that do not require persistence. For persistence, use it with [XML Persistence Storage](xml-persistence.md).

## Basic Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | Data source name, used as database identifier (`USE <name>` to switch) |
| `type` | string | Yes | Fixed value `memory` |
| `writable` | bool | No | Always supports read/write, default `true` |

## Configuration Examples

### datasources.json

```json
{
  "datasources": [
    {
      "name": "default",
      "type": "memory",
      "writable": true
    }
  ]
}
```

### Embedded Mode

```go
package main

import (
    "fmt"
    "github.com/kasuganosora/sqlexec/pkg/api"
)

func main() {
    // Create database instance (automatically creates a default memory data source)
    db, _ := api.NewDB(nil)
    session := db.Session()
    defer session.Close()

    // Create table
    session.Execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), email VARCHAR(200))")

    // Insert data
    session.Execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
    session.Execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")

    // Query data
    result, _ := session.Query("SELECT * FROM users WHERE id = 1")
    fmt.Println(result)
}
```

## Supported Data Types

The Memory data source supports the following SQL data types:

| Category | Data Types | Description |
|----------|-----------|-------------|
| Integer | `INT`, `INTEGER`, `TINYINT`, `SMALLINT`, `MEDIUMINT`, `BIGINT` | Integer types |
| Floating Point | `FLOAT`, `DOUBLE`, `DECIMAL`, `NUMERIC` | Floating-point and fixed-point numbers |
| String | `VARCHAR(n)`, `CHAR(n)`, `TEXT` | String types |
| Boolean | `BOOLEAN`, `BOOL` | Boolean values |
| Date/Time | `DATE`, `DATETIME`, `TIMESTAMP`, `TIME` | Date and time types |
| Binary | `BLOB` | Binary data |
| Vector | `VECTOR(dim)` | Vector type, `dim` is the dimension count |

## Features

The Memory data source supports all SQL features of SQLExec.

### DDL Operations

```sql
-- Create table (with constraints)
CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) DEFAULT 0.00,
    category VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP
);

-- Create temporary table (automatically deleted when session ends)
CREATE TEMPORARY TABLE temp_results (
    id INT,
    score FLOAT
);

-- Drop table
DROP TABLE products;

-- Truncate table data (preserves table structure)
TRUNCATE TABLE products;
```

### Constraint Support

| Constraint | Syntax | Description |
|-----------|--------|-------------|
| Primary Key | `PRIMARY KEY` | Uniquely identifies rows, NULL not allowed |
| Auto Increment | `AUTO_INCREMENT` | Automatically incrementing integer column |
| Not Null | `NOT NULL` | Column value cannot be NULL |
| Unique | `UNIQUE` | Column values must be unique |
| Default | `DEFAULT value` | Uses default value when not specified on insert |

```sql
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(200) NOT NULL,
    status INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### DML Operations

```sql
-- Insert
INSERT INTO products (name, price, category) VALUES ('Laptop', 5999.00, 'Electronics');

-- Batch insert
INSERT INTO products (name, price, category) VALUES
    ('Mouse', 99.00, 'Accessories'),
    ('Keyboard', 299.00, 'Accessories'),
    ('Monitor', 1999.00, 'Electronics');

-- Query (supports WHERE, ORDER BY, LIMIT, GROUP BY, HAVING)
SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM products
WHERE price > 100
GROUP BY category
HAVING cnt > 1
ORDER BY avg_price DESC
LIMIT 10;

-- Subquery
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);

-- Update
UPDATE products SET price = 5499.00 WHERE id = 1;

-- Delete
DELETE FROM products WHERE category = 'Discontinued';
```

### Index Support

The Memory data source supports multiple index types that can significantly improve query performance:

| Index Type | Creation Syntax | Use Case |
|-----------|----------------|----------|
| B-Tree | `CREATE INDEX` | Equality queries, range queries, sorting |
| Hash | `CREATE HASH INDEX` | Equality queries (faster) |
| Unique | `CREATE UNIQUE INDEX` | Equality queries + uniqueness constraint |
| Fulltext | `CREATE FULLTEXT INDEX` | Full-text search (supports Chinese segmentation) |
| Vector | `CREATE VECTOR INDEX` | Vector similarity search |

```sql
-- Create B-Tree index (default type)
CREATE INDEX idx_name ON products (name);

-- Create Hash index
CREATE HASH INDEX idx_id ON products (id);

-- Create unique index
CREATE UNIQUE INDEX idx_email ON users (email);

-- Create fulltext index (supports BM25 scoring and Chinese Jieba segmentation)
CREATE FULLTEXT INDEX idx_desc ON products (description);

-- Create vector index (supports HNSW, IVF-Flat algorithms)
CREATE VECTOR INDEX idx_embedding ON documents (embedding);

-- Drop index
DROP INDEX idx_name ON products;
```

#### Full-Text Search

Fulltext indexes use the BM25 scoring algorithm with Chinese Jieba segmentation support:

```sql
-- Full-text search
SELECT * FROM articles WHERE MATCH(content) AGAINST('database performance optimization');
```

#### Vector Search

Vector indexes support multiple distance metrics and index algorithms:

```sql
-- Create vector column
CREATE TABLE documents (
    id INT PRIMARY KEY,
    title TEXT,
    embedding VECTOR(768)
);

-- Vector similarity search (cosine distance)
SELECT id, title, VEC_COSINE_DISTANCE(embedding, '[0.1, 0.2, ...]') AS distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

### Transaction Support

The Memory data source implements full transaction support based on MVCC (Multi-Version Concurrency Control) with snapshot isolation.

#### Transaction Isolation Levels

| Isolation Level | Description |
|----------------|-------------|
| `READ UNCOMMITTED` | Allows reading uncommitted data |
| `READ COMMITTED` | Only reads committed data |
| `REPEATABLE READ` | Default level, repeatable reads within a transaction |
| `SERIALIZABLE` | Highest isolation level, fully serialized |

```sql
-- Set isolation level
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

-- Begin transaction
BEGIN;

INSERT INTO accounts (name, balance) VALUES ('Savings Account', 10000.00);
UPDATE accounts SET balance = balance - 500 WHERE id = 1;

-- Commit transaction
COMMIT;

-- Or rollback transaction
-- ROLLBACK;
```

#### How MVCC Works

```
Transaction A (write)                   Transaction B (read)
    |                                      |
    |-- BEGIN                              |-- BEGIN
    |-- UPDATE products ...                |
    |   (creates new version v2)           |-- SELECT * FROM products
    |                                      |   (reads snapshot version v1, unaffected)
    |-- COMMIT                             |
    |   (v2 visible to new transactions)   |-- SELECT * FROM products
    |                                      |   (still reads v1, repeatable read)
                                           |-- COMMIT
```

- Read operations do not block write operations, and vice versa.
- Each transaction sees a snapshot of the data as it was when the transaction began.
- Old version data is automatically cleaned up by GC — no manual maintenance required.

## Cross-Data Source JOIN

The Memory data source can participate in cross-data source JOIN queries with MySQL, PostgreSQL, and other external data sources.

```sql
-- Join memory table orders with MySQL table users
SELECT u.name, o.order_no, o.amount
FROM mysql_db.users u
JOIN memory.orders o ON u.id = o.user_id
WHERE u.status = 'active'    -- Pushed down to MySQL
  AND o.amount > 100;        -- Filtered in memory data source
```

Execution flow:

1. MySQL executes: `SELECT id, name FROM users WHERE status = 'active'`
2. Memory data source filters: `SELECT * FROM orders WHERE amount > 100`
3. SQLExec performs the JOIN locally
4. Returns final results

## Persistence

The Memory data source does not persist data by default. To persist table data to disk, use the XML persistence engine:

```sql
-- Create a persistent table
CREATE TABLE important_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    value TEXT
) ENGINE=xml;

-- Data is automatically written to disk after each DML operation
-- Automatically restored via USE command after restart
```

For details, see [XML Persistence Storage](xml-persistence.md).

## Notes

- All data is stored in memory by default; data is lost when the process exits. Use `ENGINE=xml` for persistence.
- Suitable for small to medium data volumes; use MySQL or PostgreSQL for large datasets.
- Writable by default; no additional configuration needed.
- MVCC transactions support concurrent reads and writes; read operations do not block write operations.
- Supports temporary tables (`CREATE TEMPORARY TABLE`) that are automatically deleted when the session ends.
- The Memory data source is the underlying engine for all file-based data sources (CSV, JSON, Excel, Parquet, JSONL).
