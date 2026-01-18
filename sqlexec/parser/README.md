# SQL 解析适配器

## 概述

SQL 解析适配器是一个基于 TiDB Parser 的 SQL 解析和执行层，它能够：

1. 解析各种 SQL 语句（SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER）
2. 将 SQL 转换为结构化的中间表示
3. 将解析后的语句转换为数据源操作
4. 支持复杂的查询条件、JOIN、排序等

## 架构

```
SQL 字符串
    ↓
TiDB Parser (AST)
    ↓
SQLAdapter (适配器层)
    ↓
SQLStatement (中间表示)
    ↓
QueryBuilder (构建器)
    ↓
DataSource 操作
```

## 核心组件

### 1. SQLAdapter (`adapter.go`)

负责将 TiDB AST 转换为自定义的 SQLStatement 结构体。

```go
adapter := parser.NewSQLAdapter()
result, err := adapter.Parse("SELECT * FROM users WHERE age > 25")
```

**支持的功能**：
- SELECT 语句：列选择、WHERE 条件、ORDER BY、LIMIT、JOIN、GROUP BY、HAVING
- INSERT 语句：单行/多行插入
- UPDATE 语句：单表更新、条件更新
- DELETE 语句：条件删除
- DDL 语句：CREATE TABLE、DROP TABLE、ALTER TABLE

### 2. QueryBuilder (`builder.go`)

负责将解析后的 SQLStatement 转换为数据源操作。

```go
builder := parser.NewQueryBuilder(dataSource)
result, err := builder.BuildAndExecute(ctx, "SELECT * FROM users")
```

### 3. 数据结构 (`types.go`)

定义了 SQL 解析相关的所有数据结构。

## 使用示例

### 基本用法

```go
package main

import (
    "context"
    "mysql-proxy/mysql/parser"
    "mysql-proxy/mysql/resource"
)

func main() {
    // 1. 创建数据源
    config := &resource.DataSourceConfig{
        Type: resource.DataSourceTypeMemory,
        Name: "test",
    }
    dataSource, _ := resource.CreateDataSource(config)
    dataSource.Connect(context.Background())

    // 2. 创建查询构建器
    builder := parser.NewQueryBuilder(dataSource)

    // 3. 执行 SQL
    result, err := builder.BuildAndExecute(
        context.Background(),
        "SELECT * FROM users WHERE age > 25",
    )

    // 4. 处理结果
    if err == nil {
        fmt.Printf("Found %d rows\n", len(result.Rows))
    }
}
```

### 直接使用适配器

```go
// 创建适配器
adapter := parser.NewSQLAdapter()

// 解析 SQL
result, err := adapter.Parse("SELECT id, name FROM users WHERE age > 25")
if err != nil {
    log.Fatal(err)
}

// 访问解析后的数据
if result.Statement.Type == parser.SQLTypeSelect {
    selectStmt := result.Statement.Select
    fmt.Printf("Table: %s\n", selectStmt.From)
    fmt.Printf("Columns: %v\n", selectStmt.Columns)
    if selectStmt.Where != nil {
        fmt.Printf("Where: %v\n", selectStmt.Where)
    }
}
```

### 批量解析

```go
// 解析多条 SQL 语句
results, err := adapter.ParseMulti(`
    CREATE TABLE users (id INT, name VARCHAR(255));
    INSERT INTO users (name) VALUES ('Alice');
    SELECT * FROM users;
`)

for _, result := range results {
    if result.Success {
        fmt.Printf("Statement type: %s\n", result.Statement.Type)
    }
}
```

## SQL 语句支持

### SELECT

支持以下特性：
- 列选择（`SELECT id, name FROM users`）
- 通配符（`SELECT * FROM users`）
- WHERE 条件（`WHERE age > 25 AND status = 'active'`）
- ORDER BY（`ORDER BY created_at DESC`）
- LIMIT/OFFSET（`LIMIT 10 OFFSET 20`）
- JOIN（`JOIN orders ON users.id = orders.user_id`）
- GROUP BY（`GROUP BY category`）
- HAVING（`HAVING COUNT(*) > 5`）
- 聚合函数（`COUNT`, `SUM`, `AVG`, `MAX`, `MIN`）
- DISTINCT（`SELECT DISTINCT name FROM users`）

### INSERT

支持以下特性：
- 单行插入（`INSERT INTO users (name) VALUES ('Alice')`）
- 批量插入（`INSERT INTO users (name) VALUES ('Alice'), ('Bob')`）
- 指定列（`INSERT INTO users (name, age) VALUES ('Alice', 25)`）

### UPDATE

支持以下特性：
- 单字段更新（`UPDATE users SET age = 26`）
- 多字段更新（`UPDATE users SET age = 26, status = 'active'`）
- 条件更新（`WHERE id = 1`）
- ORDER BY（`ORDER BY created_at DESC`）
- LIMIT（`LIMIT 10`）

### DELETE

支持以下特性：
- 条件删除（`DELETE FROM users WHERE age < 18`）
- ORDER BY（`ORDER BY created_at DESC`）
- LIMIT（`LIMIT 10`）

### DDL

支持以下特性：
- CREATE TABLE（`CREATE TABLE users (id INT, name VARCHAR(255))`）
- DROP TABLE（`DROP TABLE users`）
- DROP TABLE IF EXISTS（`DROP TABLE IF EXISTS users`）
- ALTER TABLE（基础支持）

## 测试

### 运行单元测试

```bash
go test -v ./mysql/parser/...
```

### 运行集成示例

```bash
go run example_sql_adapter.go
```

### 运行性能测试

```bash
go test -bench=. -benchmem ./mysql/parser/...
```

## 数据结构

### SQLStatement

所有 SQL 语句的通用结构。

```go
type SQLStatement struct {
    Type      SQLType
    RawSQL    string
    Select    *SelectStatement
    Insert    *InsertStatement
    Update    *UpdateStatement
    Delete    *DeleteStatement
    Create    *CreateStatement
    Drop      *DropStatement
    Alter     *AlterStatement
}
```

### SelectStatement

```go
type SelectStatement struct {
    Distinct   bool
    Columns    []SelectColumn
    From       string
    Joins      []JoinInfo
    Where      *Expression
    GroupBy    []string
    Having     *Expression
    OrderBy    []OrderByItem
    Limit      *int64
    Offset     *int64
}
```

### Expression

表达式树结构，支持复杂的条件表达式。

```go
type Expression struct {
    Type      ExprType
    Column    string
    Value     interface{}
    Operator  string
    Left      *Expression
    Right     *Expression
    Args      []Expression
    Function  string
}
```

## 性能特性

- 基于 TiDB Parser，性能优异
- 零内存分配的解析过程（大部分）
- 支持批量解析
- 解析结果可序列化为 JSON

## 扩展性

### 添加新的 SQL 类型支持

1. 在 `types.go` 中定义新的 SQLType
2. 在 `adapter.go` 中实现对应的转换函数
3. 在 `builder.go` 中实现执行逻辑
4. 添加测试用例

### 添加新的表达式类型

在 `convertExpression` 函数中添加新的 case 分支：

```go
case *ast.YourExprType:
    expr.Type = "YOUR_TYPE"
    // 处理逻辑
```

## 限制

当前版本的限制：

1. 子查询支持有限（仅能解析，不能完全执行）
2. 窗口函数支持有限（仅能解析）
3. CTE (WITH 子句) 支持有限（仅能解析）
4. 存储过程不支持
5. 复杂的 ALTER TABLE 操作支持有限

## 后续计划

- [ ] 完善子查询执行
- [ ] 添加窗口函数支持
- [ ] 实现 CTE 支持
- [ ] 优化表达式求值
- [ ] 添加查询优化器
- [ ] 支持更多数据类型

## 参考资料

- [TiDB Parser 文档](https://github.com/pingcap/tidb/tree/master/pkg/parser)
- [MySQL 5.7 参考手册](https://dev.mysql.com/doc/refman/5.7/en/)
- [TIDB_INTEGRATION.md](../../TIDB_INTEGRATION.md) - TiDB 集成研究报告
