# 查询与结果

Session 提供了多种方法来执行 SQL 查询和命令。查询类操作返回结果集，执行类操作返回受影响的行数。所有方法都支持 `?` 占位符的参数化查询。

## 导入

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)
```

## 查询方法

### Query -- 迭代器查询

`session.Query(sql, args...)` 执行 SELECT / SHOW / DESCRIBE 语句，返回 `*Query` 迭代器对象：

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

### Query 对象方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `Next()` | `Next() bool` | 移动到下一行，有数据返回 `true` |
| `Row()` | `Row() domain.Row` | 获取当前行（`map[string]interface{}` 形式） |
| `Scan()` | `Scan(dest ...interface{}) error` | 将当前行按列顺序扫描到变量 |
| `Columns()` | `Columns() []domain.ColumnInfo` | 获取列信息（名称、类型等） |
| `RowsCount()` | `RowsCount() int` | 获取结果集总行数 |
| `Close()` | `Close() error` | 关闭查询，释放资源 |
| `Err()` | `Err() error` | 获取查询执行过程中的错误 |
| `Iter()` | `Iter(fn func(row domain.Row) error) error` | 遍历所有行（回调方式） |

#### Scan 示例

`Scan` 按列顺序将值扫描到 Go 变量中，自动进行类型转换：

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

#### Columns 示例

```go
query, err := session.Query("SELECT * FROM users")
if err != nil {
    log.Fatal(err)
}
defer query.Close()

cols := query.Columns()
for _, col := range cols {
    fmt.Printf("列名: %s, 类型: %s\n", col.Name, col.Type)
}
```

#### Iter 示例

`Iter` 提供回调方式遍历所有行，遍历结束后自动调用 `Close()`：

```go
query, err := session.Query("SELECT name, age FROM users")
if err != nil {
    log.Fatal(err)
}

err = query.Iter(func(row domain.Row) error {
    fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
    return nil // 返回非 nil 错误会中止遍历
})
if err != nil {
    log.Fatal(err)
}
```

### QueryAll -- 获取全部行

`session.QueryAll(sql, args...)` 执行查询并一次性返回所有行：

```go
rows, err := session.QueryAll("SELECT * FROM users WHERE age > ?", 20)
if err != nil {
    log.Fatal(err)
}

for _, row := range rows {
    fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
}
```

返回类型为 `[]domain.Row`，其中 `domain.Row` 是 `map[string]interface{}` 的类型别名。

### QueryOne -- 获取单行

`session.QueryOne(sql, args...)` 执行查询并返回第一行。如果没有匹配的行，返回错误：

```go
row, err := session.QueryOne("SELECT name, age FROM users WHERE id = ?", 1)
if err != nil {
    if api.IsErrorCode(err, api.ErrCodeInternal) {
        fmt.Println("用户不存在")
    } else {
        log.Fatal(err)
    }
    return
}

fmt.Printf("Name: %v, Age: %v\n", row["name"], row["age"])
```

## 执行方法

### Execute -- 执行 DML/DDL

`session.Execute(sql, args...)` 用于执行 INSERT、UPDATE、DELETE 和 DDL（CREATE/ALTER/DROP）语句：

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
fmt.Printf("影响行数: %d, 自增ID: %d\n", result.RowsAffected, result.LastInsertID)

// UPDATE
result, err = session.Execute(
    "UPDATE users SET age = ? WHERE name = ?",
    31, "Alice",
)
fmt.Printf("更新了 %d 行\n", result.RowsAffected)

// DELETE
result, err = session.Execute(
    "DELETE FROM users WHERE age < ?",
    18,
)
fmt.Printf("删除了 %d 行\n", result.RowsAffected)
```

### Result 对象

| 字段/方法 | 类型 | 说明 |
|----------|------|------|
| `RowsAffected` | `int64` | 受影响的行数 |
| `LastInsertID` | `int64` | 最后插入行的自增 ID（仅 INSERT） |
| `Err()` | `error` | 获取执行错误 |

### Execute 与 Query 的区别

| 操作 | 使用方法 | 说明 |
|------|---------|------|
| `SELECT` | `Query()` / `QueryAll()` / `QueryOne()` | 返回结果集 |
| `SHOW` | `Query()` | 返回结果集 |
| `DESCRIBE` | `Query()` | 返回结果集 |
| `INSERT` | `Execute()` | 返回 RowsAffected 和 LastInsertID |
| `UPDATE` | `Execute()` | 返回 RowsAffected |
| `DELETE` | `Execute()` | 返回 RowsAffected |
| `CREATE TABLE/INDEX` | `Execute()` | DDL 操作 |
| `ALTER TABLE` | `Execute()` | DDL 操作 |
| `DROP TABLE/INDEX` | `Execute()` | DDL 操作 |
| `USE` | `Execute()` | 切换数据库 |
| `EXPLAIN` | `Explain()` | 返回查询计划文本 |

对 SELECT/SHOW/DESCRIBE 误用 `Execute()` 会返回错误提示。

## Explain -- 查询计划

`session.Explain(sql, args...)` 返回查询的执行计划：

```go
plan, err := session.Explain("SELECT * FROM users WHERE age > ? ORDER BY name", 20)
if err != nil {
    log.Fatal(err)
}
fmt.Println(plan)
```

输出示例：

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

注意：`Explain` 仅支持 SELECT 语句。

## 参数化查询

所有查询方法都支持 `?` 占位符进行参数绑定，防止 SQL 注入：

```go
// 单个参数
rows, _ := session.QueryAll("SELECT * FROM users WHERE id = ?", 1)

// 多个参数
rows, _ = session.QueryAll(
    "SELECT * FROM users WHERE age > ? AND name LIKE ?",
    20, "%alice%",
)

// Execute 同样支持
result, _ := session.Execute(
    "INSERT INTO users (name, age) VALUES (?, ?)",
    "Charlie", 35,
)

// Explain 也支持
plan, _ := session.Explain("SELECT * FROM users WHERE id = ?", 42)
```

参数会被安全地绑定到 SQL 中，字符串值会自动添加引号和转义。

## 错误处理

### Error 类型

SQLExec 的所有错误都封装为 `api.Error` 类型，包含错误码、消息和调用堆栈：

```go
result, err := session.Execute("SELECT * FROM nonexistent_table")
if err != nil {
    // 类型断言获取详细信息
    if apiErr, ok := err.(*api.Error); ok {
        fmt.Printf("错误码: %s\n", apiErr.Code)
        fmt.Printf("消息: %s\n", apiErr.Message)
        fmt.Printf("堆栈:\n")
        for _, frame := range apiErr.StackTrace() {
            fmt.Println(frame)
        }
        if apiErr.Cause != nil {
            fmt.Printf("原因: %v\n", apiErr.Cause)
        }
    }
}
```

### 错误码常量

| 错误码 | 常量 | 说明 |
|--------|------|------|
| `DS_NOT_FOUND` | `api.ErrCodeDSNotFound` | 数据源不存在 |
| `DS_ALREADY_EXISTS` | `api.ErrCodeDSAlreadyExists` | 数据源已存在 |
| `TABLE_NOT_FOUND` | `api.ErrCodeTableNotFound` | 表不存在 |
| `COLUMN_NOT_FOUND` | `api.ErrCodeColumnNotFound` | 列不存在 |
| `SYNTAX_ERROR` | `api.ErrCodeSyntax` | SQL 语法错误 |
| `CONSTRAINT` | `api.ErrCodeConstraint` | 约束冲突 |
| `TRANSACTION` | `api.ErrCodeTransaction` | 事务错误 |
| `TIMEOUT` | `api.ErrCodeTimeout` | 查询超时 |
| `QUERY_KILLED` | `api.ErrCodeQueryKilled` | 查询被终止 |
| `INVALID_PARAM` | `api.ErrCodeInvalidParam` | 无效参数 |
| `NOT_SUPPORTED` | `api.ErrCodeNotSupported` | 不支持的操作 |
| `CLOSED` | `api.ErrCodeClosed` | 资源已关闭 |
| `INTERNAL` | `api.ErrCodeInternal` | 内部错误 |

### 错误检查工具函数

```go
// 检查是否为特定错误码
if api.IsErrorCode(err, api.ErrCodeTableNotFound) {
    fmt.Println("表不存在，需要先创建")
}

// 获取错误码
code := api.GetErrorCode(err)

// 获取错误消息
msg := api.GetErrorMessage(err)
```

## 完整示例

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

    // 创建表
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

    // 批量插入
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

    // QueryAll: 查询所有工程部员工
    fmt.Println("=== 工程部员工 ===")
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

    // QueryOne: 查询最高薪资
    fmt.Println("\n=== 最高薪资 ===")
    top, err := session.QueryOne("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 1")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("  %v: $%.0f\n", top["name"], top["salary"])

    // Query + Scan: 使用迭代器和类型扫描
    fmt.Println("\n=== 部门统计 ===")
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
        fmt.Printf("  %s: %d人, 平均薪资 $%.0f\n", dept, count, avgSalary)
    }

    // Execute: 更新操作
    result, err := session.Execute(
        "UPDATE employees SET salary = salary * 1.1 WHERE department = ?",
        "Engineering",
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("\n已为 %d 名工程师加薪 10%%\n", result.RowsAffected)

    // Explain: 查看查询计划
    fmt.Println("\n=== 查询计划 ===")
    plan, err := session.Explain("SELECT * FROM employees WHERE department = ? AND salary > ?", "Engineering", 80000)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(plan)
}
```

## 下一步

- [事务管理](transactions.md) -- 了解事务操作和隔离级别
- [GORM 驱动](gorm-driver.md) -- 通过 ORM 框架使用 SQLExec
