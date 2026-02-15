# Slice Adapter

The Slice adapter converts Go struct slices or map slices directly into SQL-queryable tables, without needing to create files or define tables.

## Use Cases

- Execute SQL queries on in-memory Go data
- Quickly build test data in unit tests
- Expose application runtime data as a SQL interface
- Intermediate data processing in ETL pipelines

## Creating from Struct Slices

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/slice"

type Employee struct {
    ID     int    `db:"id"`
    Name   string `db:"name"`
    Age    int    `json:"age"`
    Salary float64
    Skip   string `db:"-"`   // Skip this field
}

employees := []Employee{
    {ID: 1, Name: "Alice", Age: 30, Salary: 8000},
    {ID: 2, Name: "Bob", Age: 25, Salary: 6000},
    {ID: 3, Name: "Charlie", Age: 35, Salary: 10000},
}

// Create adapter (pass a pointer to support writes)
adapter, err := slice.FromStructSlice(
    &employees,
    "employees",
    slice.WithWritable(true),
    slice.WithMVCC(true),
    slice.WithDatabaseName("hr"),
)
```

### Struct Tag Rules

| Priority | Tag | Example | Description |
|----------|-----|---------|-------------|
| 1 | `db` | `db:"user_name"` | Highest priority |
| 2 | `json` | `json:"userName"` | Second priority |
| 3 | Field name | `Name` | Default: uses the field name |
| - | `db:"-"` | `db:"-"` | Skip this field |

## Creating from Map Slices

```go
data := &[]map[string]any{
    {"id": 1, "name": "Product A", "price": 99.9},
    {"id": 2, "name": "Product B", "price": 199.9},
}

adapter, err := slice.FromMapSlice(
    data,
    "products",
    slice.WithWritable(true),
)
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithWritable(bool)` | Enable write support | `false` |
| `WithMVCC(bool)` | Enable MVCC transaction support | `false` |
| `WithDatabaseName(string)` | Set the database name | `""` |

## Register and Query

```go
// Register with the database
db.RegisterDataSource("hr", adapter)

// Query
session := db.Session()
session.Execute("USE hr")

rows, _ := session.QueryAll("SELECT name, salary FROM employees WHERE age > 25 ORDER BY salary DESC")
for _, row := range rows {
    fmt.Printf("%s: %.0f\n", row["name"], row["salary"])
}
// Output:
// Charlie: 10000
// Alice: 8000
```

## Write Operations

When `WithWritable(true)` is set, INSERT / UPDATE / DELETE are supported:

```go
// Insert
session.Execute("INSERT INTO employees (id, name, age, salary) VALUES (4, 'Diana', 28, 7000)")

// Update
session.Execute("UPDATE employees SET salary = 9000 WHERE name = 'Alice'")

// Delete
session.Execute("DELETE FROM employees WHERE id = 4")
```

## Syncing Back to Original Data

SQL operations modify an in-memory copy. Use `SyncToOriginal()` to write changes back to the original Go slice:

```go
// After performing some SQL modifications...
err := adapter.SyncToOriginal()
if err != nil {
    log.Fatal(err)
}

// The employees slice is now updated
fmt.Println(employees[0].Salary) // 9000 (modified by UPDATE)
```

## Reloading from External Changes

When external code modifies the original slice, call `Reload()` to refresh the in-memory table:

```go
// External code modified employees...
employees = append(employees, Employee{ID: 5, Name: "Eve", Age: 22, Salary: 5000})

// Reload into the SQL table
err := adapter.Reload()
```

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/slice"
)

type Task struct {
    ID       int    `db:"id"`
    Title    string `db:"title"`
    Done     bool   `db:"done"`
    Priority int    `db:"priority"`
}

func main() {
    tasks := []Task{
        {1, "Write docs", false, 1},
        {2, "Fix bug", true, 2},
        {3, "Add tests", false, 3},
    }

    adapter, _ := slice.FromStructSlice(&tasks, "tasks",
        slice.WithWritable(true),
    )
    adapter.Connect(context.Background())

    db, _ := api.NewDB(nil)
    defer db.Close()
    db.RegisterDataSource("todo", adapter)

    session := db.Session()
    defer session.Close()
    session.Execute("USE todo")

    // Query incomplete tasks
    rows, _ := session.QueryAll("SELECT title, priority FROM tasks WHERE done = false ORDER BY priority")
    for _, row := range rows {
        fmt.Printf("[P%v] %v\n", row["priority"], row["title"])
    }

    // Mark as done
    session.Execute("UPDATE tasks SET done = true WHERE id = 1")

    // Sync back to Go slice
    adapter.SyncToOriginal()
    fmt.Println(tasks[0].Done) // true
}
```
