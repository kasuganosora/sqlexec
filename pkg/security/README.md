# Security Package - SQL Escape

本包提供 SQL 参数化查询工具，帮助开发者安全地构造 SQL 查询语句。

## 背景

SQL 注入是数据库应用中最常见的安全漏洞之一。传统的防御方式包括：
1. **参数化查询（推荐）** - 使用预编译语句和参数绑定
2. **输入转义** - 正确转义特殊字符

本包提供**输入转义工具**，类似于 TiDB 的 `sqlescape` 包。

## 重要说明

⚠️ **此工具不能阻止所有类型的 SQL 注入攻击！**

安全性取决于开发者的正确使用：
- 建议优先使用数据库的参数化查询（预编译语句）
- 此工具主要用于动态 SQL 构建场景
- 开发者仍需确保编写安全的 SQL

## API 参考

### `EscapeSQL(sql string, args ...interface{}) (string, error)`

将参数安全地转义并插入到 SQL 字符串中。

**格式说明符：**
- `%?` - 自动类型转换的参数
- `%%` - 输出 %
- `%n` - 标识符（表名、列名），自动用反引号包裹

**示例：**

```go
// 基本使用
query, err := EscapeSQL("SELECT * FROM %n WHERE id = %?", "users", 123)
// 结果: "SELECT * FROM `users` WHERE id = 123"

// 多个参数
query, err := EscapeSQL(
    "INSERT INTO %n (name, age) VALUES (%?, %?)",
    "users", "Alice", 30,
)
// 结果: "INSERT INTO `users` (name, age) VALUES ('Alice', 30)"

// 字符串自动转义
query, err := EscapeSQL(
    "SELECT * FROM %n WHERE name = %?",
    "users", "O'Reilly",
)
// 结果: "SELECT * FROM `users` WHERE name = 'O\\'Reilly'"

// NULL 值
query, err := EscapeSQL(
    "SELECT * FROM %n WHERE deleted_at IS %?",
    "users", nil,
)
// 结果: "SELECT * FROM `users` WHERE deleted_at IS NULL"

// 数组类型
query, err := EscapeSQL(
    "SELECT * FROM %n WHERE id IN (%?)",
    "users", []int{1, 2, 3},
)
// 结果: "SELECT * FROM `users` WHERE id IN (1,2,3)"

// 时间类型
query, err := EscapeSQL(
    "SELECT * FROM %n WHERE created_at = %?",
    "logs", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
)
// 结果: "SELECT * FROM `logs` WHERE created_at = '2024-01-15 10:30:00'"
```

### `MustEscapeSQL(sql string, args ...interface{}) string`

`EscapeSQL` 的便捷版本，遇到错误会 panic。

适用于参数类型在编译时已知安全的场景。

```go
query := MustEscapeSQL("SELECT * FROM %n WHERE id = %?", "users", 123)
```

### `FormatSQL(w io.Writer, sql string, args ...interface{}) error`

`EscapeSQL` 的 Writer 版本，适用于构建复杂 SQL。

```go
var buf strings.Builder

// 构建复杂查询
FormatSQL(&buf, "SELECT * FROM %n WHERE ", "users")
FormatSQL(&buf, "%n = %?", "status", "active")
FormatSQL(&buf, " AND %n > %?", "age", 18)

fmt.Println(buf.String())
// 输出: SELECT * FROM `users` WHERE `status` = 'active' AND `age` > 18
```

### `MustFormatSQL(w *strings.Builder, sql string, args ...interface{})`

`FormatSQL` 的便捷版本，使用 `strings.Builder`，遇到错误会 panic。

```go
var buf strings.Builder
MustFormatSQL(&buf, "SELECT * FROM %n WHERE id = %?", "users", 123)
```

## 支持的类型

### 标识符 (`%n`)
- `string` - 标识符名称（表名、列名等）

### 参数 (`%?`)
- `int`, `int8`, `int16`, `int32`, `int64`
- `uint`, `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `bool` - 转换为 1 (true) 或 0 (false)
- `string` - 自动转义特殊字符
- `[]byte` - 二进制数据（以 `_binary'...'` 格式）
- `time.Time` - 时间格式 `'YYYY-MM-DD HH:MM:SS.FFFFFF'`
- `json.RawMessage` - JSON 数据
- `[]string`, `[]int`, `[]int64` - 数组类型（逗号分隔）
- `nil` - 转换为 `NULL`

## 转义规则

字符串和二进制数据会转义以下特殊字符（使用反斜杠）：

| 字符 | 转义后 |
|------|---------|
| `\0`  | `\\0`   |
| `\n`  | `\\n`   |
| `\r`  | `\\r`   |
| `\\`   | `\\\\`  |
| `'`   | `\\'`   |
| `"`   | `\\"`   |
| `\x1a` | `\\Z`   |

标识符中的反引号会转义为 `` ` `` -> `` `` ``。

## 错误处理

`EscapeSQL` 和 `FormatSQL` 可能返回以下错误：

1. **参数不足** - 格式说明符多于提供的参数
2. **参数过多** - 提供的参数多于格式说明符
3. **类型错误** - 标识符必须是字符串类型
4. **不支持的类型** - 参数类型无法转换为 SQL 值

## 最佳实践

### 1. 优先使用参数化查询

```go
// ✅ 推荐：使用数据库参数绑定
db.Query("SELECT * FROM users WHERE id = ?", 123)

// ⚠️  可接受：使用 EscapeSQL
query, _ := EscapeSQL("SELECT * FROM %n WHERE id = %?", "users", 123)
db.Exec(query)
```

### 2. 仅在动态 SQL 构建时使用此工具

```go
// 适用于表名或列名动态变化的场景
func QueryTable(tableName string, id int) (string, error) {
    return EscapeSQL("SELECT * FROM %n WHERE id = %?", tableName, id)
}
```

### 3. 避免直接拼接用户输入

```go
// ❌ 危险：直接拼接
query := fmt.Sprintf("SELECT * FROM users WHERE name = '%s'", userInput)

// ✅ 安全：使用 EscapeSQL
query, _ := EscapeSQL("SELECT * FROM %n WHERE name = %?", "users", userInput)
```

## 与其他数据库的兼容性

此工具使用 MySQL 的转义规则（与 TiDB 一致）：

- 字符串用单引号包裹
- 标识符用反引号包裹
- 使用反斜杠转义特殊字符

其他数据库可能需要不同的转义规则。

## 性能考虑

- `EscapeSQL` 会预分配足够的缓冲区空间
- `FormatSQL` 允许流式构建，适合复杂 SQL
- 避免在循环中重复构建相同的 SQL

## 参考实现

本包参考了 TiDB 的 `sqlescape` 实现：
- https://github.com/pingcap/tidb/tree/master/pkg/util/sqlescape

## License

同主项目。
