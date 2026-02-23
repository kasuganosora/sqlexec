# Slice 切片数据源

Slice 数据源将 Go 的 `[]map[string]any` 或 `[]struct` 切片包装为 SQL 可查询的数据表。无需创建文件或手动建表，直接将程序内存中的数据结构暴露为 SQL 接口。

适用于对应用运行时数据执行 SQL 查询、单元测试快速构建数据、ETL 管道中的中间数据处理等场景。

> 完整的使用指南请参考 [嵌入式使用 - Slice 适配器](../embedded/slice-adapter.md)。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称 |
| `type` | string | 是 | 固定值 `slice` |
| `writable` | bool | 否 | 是否允许写入，默认 `true` |

## 选项

| 选项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `data` | `interface{}` | 是 | 原始数据（`[]map[string]any` 或 `[]struct`），推荐传指针 |
| `table_name` | string | 是 | 表名 |
| `database_name` | string | 否 | 数据库名，默认 `"default"` |
| `writable` | bool | 否 | 是否可写，默认 `true`（非指针数据自动设为 `false`） |
| `mvcc_supported` | bool | 否 | 是否启用 MVCC 事务，默认 `true` |

## 快速开始

### 从结构体切片创建

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/slice"

type User struct {
    ID   int    `db:"id"`
    Name string `db:"name"`
    Age  int    `json:"age"`
}

users := []User{
    {ID: 1, Name: "Alice", Age: 30},
    {ID: 2, Name: "Bob", Age: 25},
    {ID: 3, Name: "Charlie", Age: 35},
}

// 创建适配器（传入指针以支持写入和同步）
adapter, err := slice.FromStructSlice(
    &users,
    "users",
    slice.WithWritable(true),
    slice.WithMVCC(true),
)
```

### 从 Map 切片创建

```go
data := &[]map[string]any{
    {"id": 1, "name": "产品A", "price": 99.9},
    {"id": 2, "name": "产品B", "price": 199.9},
}

adapter, err := slice.FromMapSlice(data, "products",
    slice.WithWritable(true),
)
```

### Struct Tag 映射规则

| 优先级 | Tag | 示例 | 说明 |
|--------|-----|------|------|
| 1 | `db` | `db:"user_name"` | 最高优先级 |
| 2 | `json` | `json:"userName"` | 次优先级 |
| 3 | 字段名 | `Name` | 默认使用字段名 |
| - | `db:"-"` | `db:"-"` | 跳过该字段 |

### 类型映射

| Go 类型 | SQL 类型 |
|---------|----------|
| `int`, `int64` 等 | `INT` |
| `float32`, `float64` | `FLOAT` |
| `bool` | `BOOLEAN` |
| `string` | `TEXT` |
| `time.Time` | `DATETIME` |
| `[]byte` | `BLOB` |

## 注册并查询

```go
import "github.com/kasuganosora/sqlexec/pkg/api"

db, _ := api.NewDB(nil)
db.RegisterDataSource("hr", adapter)

session := db.Session()
defer session.Close()
session.Execute("USE hr")

// 标准 SQL 查询
rows, _ := session.QueryAll("SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC")
for _, row := range rows {
    fmt.Printf("%s: %v\n", row["name"], row["age"])
}
```

## 写入与同步

当 `WithWritable(true)` 且传入指针数据时，支持 SQL 写入：

```go
// SQL 写入操作
session.Execute("INSERT INTO users (id, name, age) VALUES (4, 'Diana', 28)")
session.Execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
session.Execute("DELETE FROM users WHERE id = 4")

// 同步回原始 Go 切片
adapter.SyncToOriginal()
fmt.Println(users[0].Age) // 31（已被 UPDATE 修改）
```

当外部代码修改了原始切片后，调用 `Reload()` 刷新内存表：

```go
users = append(users, User{ID: 5, Name: "Eve", Age: 22})
adapter.Reload()
```

## 通过工厂创建

Slice 数据源也支持通过 Factory 模式创建：

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/slice"

factory := slice.NewFactory()
ds, err := factory.Create(&domain.DataSourceConfig{
    Type:     "slice",
    Name:     "mydata",
    Writable: true,
    Options: map[string]interface{}{
        "data":          &mySlice,
        "table_name":    "records",
        "database_name": "app",
    },
})
```

## 注意事项

- 如需写入操作和 `SyncToOriginal()`，必须传入指针（如 `&users`）。非指针数据自动设为只读。
- 数据在创建适配器时一次性加载到底层 Memory 引擎，后续 SQL 操作在内存中执行。
- `SyncToOriginal()` 将内存中的修改写回原始 Go 切片。不调用此方法，原始数据不会变化。
- `Reload()` 从原始切片重新加载数据到内存表，会覆盖未同步的 SQL 修改。
- Slice 数据源基于 Memory 引擎，支持完整的 SQL 功能（WHERE、JOIN、GROUP BY、ORDER BY 等）和 MVCC 事务。
