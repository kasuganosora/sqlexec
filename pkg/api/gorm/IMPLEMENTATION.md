# GORM 驱动实现说明

## 架构概述

这个 GORM 驱动是一个适配器，它将 `pkg/api` 中的 `Session` 封装为 GORM 的 Dialector，让您可以在 GORM 中使用 sqlexec 的 SQL 执行引擎。

## 核心组件

### 1. Dialector (`dialect.go`)

Dialector 是 GORM 驱动的核心，实现了 `gorm.Dialector` 接口：

```go
type Dialector struct {
    Session      *api.Session      // sqlexec 会话
    SQLParser    func(sql string) (string, []interface{}, error) // SQL 解析器
    CachedDB     *sql.DB          // GORM 使用的数据库连接
    Initialized  bool
}
```

**关键方法：**
- `Name()` - 返回 "sqlexec"
- `Initialize(*gorm.DB)` - 初始化 GORM DB
- `Migrator(*gorm.DB)` - 返回 Migrator 实例
- `DataTypeOf(*schema.Field)` - 类型映射
- `BindVarTo(writer, stmt, v)` - 参数绑定
- `QuoteTo(writer, str)` - 标识符引用
- `Explain(sql, vars...)` - SQL 解释

### 2. Migrator (`migrator.go`)

Migrator 实现了 GORM 的 Migrator 接口，将迁移操作委托给 sqlexec：

**支持的操作：**
- `HasTable(value)` - 检查表是否存在
- `CreateTable(value)` - 创建表
- `DropTable(value)` - 删除表
- `RenameTable(old, new)` - 重命名表
- `GetTables()` - 获取所有表
- `AddColumn(value, field)` - 添加列
- `DropColumn(value, name)` - 删除列
- `AlterColumn(value, field)` - 修改列
- `RenameColumn(value, old, new)` - 重命名列
- `ColumnTypes(value)` - 获取列类型
- `CreateConstraint(value, name)` - 创建约束
- `DropConstraint(value, name)` - 删除约束
- `HasConstraint(value, name)` - 检查约束
- `CreateIndex(value, name)` - 创建索引
- `DropIndex(value, name)` - 删除索引
- `HasIndex(value, name)` - 检查索引
- `RenameIndex(value, old, new)` - 重命名索引

**限制：**
- `AutoMigrate()` - 不支持，需要手动创建表
- 复杂的迁移操作需要手动实现

## 工作原理

### SQL 执行流程

1. **用户调用 GORM API**
   ```go
   db.Create(&User{Name: "John"})
   ```

2. **GORM 构建 SQL**
   ```sql
   INSERT INTO users (name) VALUES (?)
   ```

3. **Dialector 接收 SQL**
   ```go
   d.Session.Execute(sql, params...)
   ```

4. **sqlexec Session 执行 SQL**
   - 通过 sqlexec 的 Session API 执行
   - 利用 sqlexec 的优化器、缓存等功能
   - 返回结果给 GORM

### 事务处理

GORM 的事务通过 sqlexec Session 的事务功能实现：

```go
// GORM 调用
tx := db.Begin()

// sqlexec 执行
session.Begin()
```

### 查询结果处理

1. GORM 调用 `Find()`
2. Dialector 调用 `Session.Query()`
3. Session 返回 `*api.Query`
4. Dialector 将 Query 结果映射到 GORM 期望的格式

## 使用模式

### 模式 1: 纯 GORM 使用

```go
// 创建 GORM DB
gormDB := gorm.Open(gorm.NewDialector(session), &gorm.Config{})

// 使用 GORM ORM 功能
var users []User
gormDB.Find(&users)
```

### 模式 2: 纯 sqlexec 使用

```go
// 直接使用 sqlexec Session
query, err := session.Query("SELECT * FROM users WHERE age > ?", 18)
if err != nil {
    log.Fatal(err)
}

for query.Next() {
    row := query.Row()
    // 处理行数据
}
```

### 模式 3: 混合使用

```go
// 简单操作使用 GORM
gormDB.Create(&User{Name: "John"})

// 复杂查询使用 sqlexec
query, err := session.Query("SELECT u.*, o.order_date FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > ?", 18)
if err != nil {
    log.Fatal(err)
}

for query.Next() {
    // 处理结果
}
```

## 优势

1. **无缝集成**: 可以在现有 sqlexec 应用中逐步引入 GORM
2. **保持 sqlexec 优势**: 继续使用 sqlexec 的 SQL 优化器
3. **灵活选择**: 根据场景选择 GORM 或 sqlexec API
4. **平滑迁移**: 不需要重写代码，可以逐步迁移
5. **生态兼容**: 兼容 GORM 插件和工具

## 限制

1. **AutoMigrate**: sqlexec 不支持自动迁移，需要手动创建表
2. **关联**: HasOne, HasMany 等关联功能需要手动实现
3. **复杂查询**: 某些 GORM 功能可能不完全支持
4. **钩子**: BeforeCreate, AfterUpdate 等钩子需要手动处理

## 扩展点

### 自定义 SQL 解析器

如果需要将 GORM 的 SQL 转换为 sqlexec 特定格式：

```go
dialector := gorm.NewDialector(session)

dialector.SetSQLParser(func(sql string) (string, []interface{}, error) {
    // 自定义 SQL 转换逻辑
    return sql, params, nil
})
```

### 自定义类型映射

修改 `DataTypeOf` 方法来支持特定的数据类型：

```go
func (d *Dialector) DataTypeOf(field *schema.Field) string {
    // 自定义类型映射逻辑
    switch field.DataType {
    // ...
    }
}
```

### 自定义子句构建器

注册自定义子句构建器来支持特定的 SQL 语法：

```go
func (d *Dialector) Initialize(db *gorm.DB) error {
    d.Initialized = true
    
    // 注册自定义子句构建器
    db.ClauseBuilders["LIMIT"] = MyCustomLimitBuilder
    
    return nil
}
```

## 测试

所有示例都在 `examples.go` 中，可以运行：

```go
package main

import (
    "github.com/kasuganosora/sqlexec/pkg/api/gorm"
)

func main() {
    gorm.ExampleBasicUsage()
    // gorm.ExampleQueryWithConditions()
    // gorm.ExamplePagination()
    // gorm.ExampleTransaction()
    // ...
}
```

## 性能建议

1. **连接池**: sqlexec Session 会管理连接，GORM 不会创建额外连接
2. **查询缓存**: 利用 sqlexec 的查询缓存
3. **批量操作**: 尽量使用批量操作
4. **索引**: 在底层表中创建合适的索引
5. **事务**: 合理使用事务，避免过多小事务

## 故障排除

### 问题：Create/Update/Delete 不生效

**原因**: sqlexec Session 可能未正确配置或数据源未注册

**解决**:
```go
// 检查 Session 状态
if session.Err() != nil {
    log.Fatal("Session error:", session.Err())
}

// 检查数据源
ds, err := db.GetDataSource("default")
if err != nil {
    log.Fatal("Datasource error:", err)
}
```

### 问题：查询结果为空

**原因**: SQL 语法问题或表不存在

**解决**:
```go
// 开启调试模式
db := api.NewDB(&api.DBConfig{
    DebugMode: true,
})

// 查看实际执行的 SQL
var users []User
result := gormDB.Find(&users)
fmt.Printf("SQL: %s\n", result.Statement.SQL.String())
```

## 参考

- [GORM 官方文档](https://gorm.io/docs/)
- [sqlexec API 文档](../README.md)
- [使用示例](examples.go)
