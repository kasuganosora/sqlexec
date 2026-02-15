# Query and Result

Session provides various methods for executing SQL queries and commands. Query operations return result sets, while execution operations return the number of affected rows. All methods support parameterized queries with `?` placeholders.

## Import

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)
```

## Query Methods

### Query -- Iterator Query

`session.Query(sql, args...)` executes SELECT / SHOW / DESCRIBE statements and returns a `*Query` iterator object:

```go
query, err := session.Query("SELECT id, name, age FROM users WHERE age > ?", 20)
if err != nil {
    log.Fatal(err)
}
defer query.Close()

for query.Next() {
    row := query.Row()
    fmt.Printf("ID=%v, Name=%v, Age=%v\n", row["id"], row["name"], row["age"])
}

if query.Err() != nil {
    log.Fatal(query.Err())
}
```

### Query Object Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `Next()` | `Next() bool` | Advances to the next row, returns `true` if data is available |
| `Row()` | `Row() domain.Row` | Gets the current row (as `map[string]interface{}`) |
| `Scan()` | `Scan(dest ...interface{}) error` | Scans the current row into variables in column order |
| `Columns()` | `Columns() []domain.ColumnInfo` | Gets column information (name, type, etc.) |
| `RowsCount()` | `RowsCount() int` | Gets the total number of rows in the result set |
| `Close()` | `Close() error` | Closes the query and releases resources |
| `Err()` | `Err() error` | Gets any error that occurred during query execution |
| `Iter()` | `Iter(fn func(row domain.Row) error) error` | Iterates over all rows (callback style) |

#### Scan Example

`Scan` scans values into Go variables in column order with automatic type conversion:

```go
query, err := session.Query("SELECT id, name, age FROM users WHERE id = ?", 1)
if err != nil {
    log.Fatal(err)
}
defer query.Close()

if query.Next() {
    var id int64
    var name string
    var age int
    if err := query.Scan(&id, &name, &age); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("ID: %d, Name: %s, Age: %d\n", id, name, age)
}
```

#### Columns Example

```go
query, err := session.Query("SELECT * FROM users")
if err != nil {
    log.Fatal(err)
}
defer query.Close()

cols := query.Columns()
for _, col := range cols {
    fmt.Printf("Column: %s, Type: %s\n", col.Name, col.Type)
}
```

#### Iter Example

`Iter` provides callback-style iteration over all rows and automatically calls `Close()` when finished:

```go
query, err := session.Query("SELECT name, age FROM users")
if err != nil {
    log.Fatal(err)
}

err = query.Iter(func(row domain.Row) error {
    fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
    return nil // Returning a non-nil error aborts the iteration
})
if err != nil {
    log.Fatal(err)
}
```

### QueryAll -- Get All Rows

`session.QueryAll(sql, args...)` executes a query and returns all rows at once:

```go
rows, err := session.QueryAll("SELECT * FROM users WHERE age > ?", 20)
if err != nil {
    log.Fatal(err)
}

for _, row := range rows {
    fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
}
```

The return type is `[]domain.Row`, where `domain.Row` is a type alias for `map[string]interface{}`.

### QueryOne -- Get a Single Row

`session.QueryOne(sql, args...)` executes a query and returns the first row. Returns an error if no matching rows are found:

```go
row, err := session.QueryOne("SELECT name, age FROM users WHERE id = ?", 1)
if err != nil {
    if api.IsErrorCode(err, api.ErrCodeInternal) {
        fmt.Println("User not found")
    } else {
        log.Fatal(err)
    }
    return
}

fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
```

## Execution Methods

### Execute -- Execute DML/DDL

`session.Execute(sql, args...)` is used for executing INSERT, UPDATE, DELETE, and DDL (CREATE/ALTER/DROP) statements:

```go
// CREATE TABLE
result, err := session.Execute(`
    CREATE TABLE users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100),
        email VARCHAR(200),
        age INT
    )
`)

// INSERT
result, err := session.Execute(
    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
    "Alice", "alice@example.com", 30,
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Rows affected: %d, Last insert ID: %d\n", result.RowsAffected, result.LastInsertID)

// UPDATE
result, err = session.Execute(
    "UPDATE users SET age = ? WHERE name = ?",
    31, "Alice",
)
fmt.Printf("Updated %d rows\n", result.RowsAffected)

// DELETE
result, err = session.Execute(
    "DELETE FROM users WHERE age < ?",
    18,
)
fmt.Printf("Deleted %d rows\n", result.RowsAffected)
```

### Result Object

| Field/Method | Type | Description |
|-------------|------|-------------|
| `RowsAffected` | `int64` | Number of affected rows |
| `LastInsertID` | `int64` | Auto-increment ID of the last inserted row (INSERT only) |
| `Err()` | `error` | Gets the execution error |

### Execute vs Query

| Operation | Method | Description |
|-----------|--------|-------------|
| `SELECT` | `Query()` / `QueryAll()` / `QueryOne()` | Returns a result set |
| `SHOW` | `Query()` | Returns a result set |
| `DESCRIBE` | `Query()` | Returns a result set |
| `INSERT` | `Execute()` | Returns RowsAffected and LastInsertID |
| `UPDATE` | `Execute()` | Returns RowsAffected |
| `DELETE` | `Execute()` | Returns RowsAffected |
| `CREATE TABLE/INDEX` | `Execute()` | DDL operation |
| `ALTER TABLE` | `Execute()` | DDL operation |
| `DROP TABLE/INDEX` | `Execute()` | DDL operation |
| `USE` | `Execute()` | Switch database |
| `EXPLAIN` | `Explain()` | Returns query plan text |

Using `Execute()` for SELECT/SHOW/DESCRIBE statements will return an error.

## Explain -- Query Plan

`session.Explain(sql, args...)` returns the execution plan for a query:

```go
plan, err := session.Explain("SELECT * FROM users WHERE age > ? ORDER BY name", 20)
if err != nil {
    log.Fatal(err)
}
fmt.Println(plan)
```

Example output:

```
Query Execution Plan
====================

SQL: SELECT * FROM users WHERE age > 20 ORDER BY name

PhysicalSort
  ├── Sort Keys: [name ASC]
  └── PhysicalFilter
      ├── Condition: age > 20
      └── PhysicalTableScan
          └── Table: users
```

Note: `Explain` only supports SELECT statements.

## Parameterized Queries

All query methods support `?` placeholders for parameter binding to prevent SQL injection:

```go
// Single parameter
rows, _ := session.QueryAll("SELECT * FROM users WHERE id = ?", 1)

// Multiple parameters
rows, _ = session.QueryAll(
    "SELECT * FROM users WHERE age > ? AND name LIKE ?",
    20, "%alice%",
)

// Execute also supports parameters
result, _ := session.Execute(
    "INSERT INTO users (name, age) VALUES (?, ?)",
    "Charlie", 35,
)

// Explain supports parameters too
plan, _ := session.Explain("SELECT * FROM users WHERE id = ?", 42)
```

Parameters are safely bound into the SQL statement, with string values automatically quoted and escaped.

## Error Handling

### Error Type

All errors in SQLExec are wrapped as the `api.Error` type, which includes an error code, message, and call stack:

```go
result, err := session.Execute("SELECT * FROM nonexistent_table")
if err != nil {
    // Type assertion to get detailed information
    if apiErr, ok := err.(*api.Error); ok {
        fmt.Printf("Error code: %s\n", apiErr.Code)
        fmt.Printf("Message: %s\n", apiErr.Message)
        fmt.Printf("Stack trace:\n")
        for _, frame := range apiErr.StackTrace() {
            fmt.Println(frame)
        }
        if apiErr.Cause != nil {
            fmt.Printf("Cause: %v\n", apiErr.Cause)
        }
    }
}
```

### Error Code Constants

| Error Code | Constant | Description |
|------------|----------|-------------|
| `DS_NOT_FOUND` | `api.ErrCodeDSNotFound` | Data source not found |
| `DS_ALREADY_EXISTS` | `api.ErrCodeDSAlreadyExists` | Data source already exists |
| `TABLE_NOT_FOUND` | `api.ErrCodeTableNotFound` | Table not found |
| `COLUMN_NOT_FOUND` | `api.ErrCodeColumnNotFound` | Column not found |
| `SYNTAX_ERROR` | `api.ErrCodeSyntax` | SQL syntax error |
| `CONSTRAINT` | `api.ErrCodeConstraint` | Constraint violation |
| `TRANSACTION` | `api.ErrCodeTransaction` | Transaction error |
| `TIMEOUT` | `api.ErrCodeTimeout` | Query timeout |
| `QUERY_KILLED` | `api.ErrCodeQueryKilled` | Query was killed |
| `INVALID_PARAM` | `api.ErrCodeInvalidParam` | Invalid parameter |
| `NOT_SUPPORTED` | `api.ErrCodeNotSupported` | Unsupported operation |
| `CLOSED` | `api.ErrCodeClosed` | Resource already closed |
| `INTERNAL` | `api.ErrCodeInternal` | Internal error |

### Error Checking Utility Functions

```go
// Check for a specific error code
if api.IsErrorCode(err, api.ErrCodeTableNotFound) {
    fmt.Println("Table does not exist, needs to be created first")
}

// Get the error code
code := api.GetErrorCode(err)

// Get the error message
msg := api.GetErrorMessage(err)
```

## Complete Example

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

    // Create table
    _, err := session.Execute(`
        CREATE TABLE employees (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100),
            department VARCHAR(50),
            salary FLOAT
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Batch insert
    _, err = session.Execute(`
        INSERT INTO employees (name, department, salary) VALUES
        ('Alice', 'Engineering', 85000),
        ('Bob', 'Marketing', 72000),
        ('Charlie', 'Engineering', 92000),
        ('Diana', 'Marketing', 68000),
        ('Eve', 'Engineering', 88000)
    `)
    if err != nil {
        log.Fatal(err)
    }

    // QueryAll: Query all engineering department employees
    fmt.Println("=== Engineering Department ===")
    rows, err := session.QueryAll(
        "SELECT name, salary FROM employees WHERE department = ? ORDER BY salary DESC",
        "Engineering",
    )
    if err != nil {
        log.Fatal(err)
    }
    for _, row := range rows {
        fmt.Printf("  %v: $%.0f\n", row["name"], row["salary"])
    }

    // QueryOne: Query the highest salary
    fmt.Println("\n=== Highest Salary ===")
    top, err := session.QueryOne("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 1")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("  %v: $%.0f\n", top["name"], top["salary"])

    // Query + Scan: Use iterator with type scanning
    fmt.Println("\n=== Department Statistics ===")
    query, err := session.Query(`
        SELECT department, COUNT(*) as cnt, AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer query.Close()

    for query.Next() {
        var dept string
        var count int64
        var avgSalary float64
        if err := query.Scan(&dept, &count, &avgSalary); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("  %s: %d employees, average salary $%.0f\n", dept, count, avgSalary)
    }

    // Execute: Update operation
    result, err := session.Execute(
        "UPDATE employees SET salary = salary * 1.1 WHERE department = ?",
        "Engineering",
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("\nGave %d engineers a 10%% raise\n", result.RowsAffected)

    // Explain: View query plan
    fmt.Println("\n=== Query Plan ===")
    plan, err := session.Explain("SELECT * FROM employees WHERE department = ? AND salary > ?", "Engineering", 80000)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(plan)
}
```

## Next Steps

- [Transaction Management](transactions.md) -- Learn about transaction operations and isolation levels
- [GORM Driver](gorm-driver.md) -- Use SQLExec through an ORM framework
