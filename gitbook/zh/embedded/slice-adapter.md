# Slice 适配器

Slice 适配器可以将 Go 结构体切片或 map 切片直接转换为 SQL 可查询的表，无需创建文件或建表。

## 适用场景

- 对内存中的 Go 数据执行 SQL 查询
- 单元测试中快速构建测试数据
- 将应用运行时数据暴露为 SQL 接口
- ETL 管道中的中间数据处理

## 从结构体切片创建

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/slice"

type Employee struct {
    ID     int    `db:"id"`
    Name   string `db:"name"`
    Age    int    `json:"age"`
    Salary float64
    Skip   string `db:"-"`   // 跳过此字段
}

employees := []Employee{
    {ID: 1, Name: "Alice", Age: 30, Salary: 8000},
    {ID: 2, Name: "Bob", Age: 25, Salary: 6000},
    {ID: 3, Name: "Charlie", Age: 35, Salary: 10000},
}

// 创建适配器（传入指针以支持写入）
adapter, err := slice.FromStructSlice(
    &employees,
    "employees",
    slice.WithWritable(true),
    slice.WithMVCC(true),
    slice.WithDatabaseName("hr"),
)
```

### Struct Tag 规则

| 优先级 | Tag | 示例 | 说明 |
|--------|-----|------|------|
| 1 | `db` | `db:"user_name"` | 最高优先级 |
| 2 | `json` | `json:"userName"` | 次优先级 |
| 3 | 字段名 | `Name` | 默认使用字段名 |
| - | `db:"-"` | `db:"-"` | 跳过该字段 |

## 从 Map 切片创建

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

## 配置选项

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `WithWritable(bool)` | 启用写入支持 | `false` |
| `WithMVCC(bool)` | 启用 MVCC 事务支持 | `false` |
| `WithDatabaseName(string)` | 设置数据库名称 | `""` |

## 注册并查询

```go
// 注册到数据库
db.RegisterDataSource("hr", adapter)

// 查询
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

## 写入操作

当 `WithWritable(true)` 时，支持 INSERT / UPDATE / DELETE：

```go
// 插入
session.Execute("INSERT INTO employees (id, name, age, salary) VALUES (4, 'Diana', 28, 7000)")

// 更新
session.Execute("UPDATE employees SET salary = 9000 WHERE name = 'Alice'")

// 删除
session.Execute("DELETE FROM employees WHERE id = 4")
```

## 同步回原始数据

SQL 操作修改的是内存中的副本。使用 `SyncToOriginal()` 将变更写回原始 Go 切片：

```go
// 执行一些 SQL 修改后...
err := adapter.SyncToOriginal()
if err != nil {
    log.Fatal(err)
}

// 现在 employees 切片已更新
fmt.Println(employees[0].Salary) // 9000（已被 UPDATE 修改）
```

## 从外部重新加载

当外部代码修改了原始切片后，调用 `Reload()` 刷新内存表：

```go
// 外部代码修改了 employees...
employees = append(employees, Employee{ID: 5, Name: "Eve", Age: 22, Salary: 5000})

// 重新加载到 SQL 表
err := adapter.Reload()
```

## 完整示例

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

    // 查询未完成的任务
    rows, _ := session.QueryAll("SELECT title, priority FROM tasks WHERE done = false ORDER BY priority")
    for _, row := range rows {
        fmt.Printf("[P%v] %v\n", row["priority"], row["title"])
    }

    // 标记完成
    session.Execute("UPDATE tasks SET done = true WHERE id = 1")

    // 同步回 Go 切片
    adapter.SyncToOriginal()
    fmt.Println(tasks[0].Done) // true
}
```
