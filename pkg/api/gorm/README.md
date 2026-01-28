# GORM 驱动集成

本包提供了将 sqlexec 的 SQL 执行器封装为 GORM Dialector，让您可以在 GORM 中使用 sqlexec 的 SQL 执行引擎。

## 概述

这个 GORM 驱动是一个适配器，它：
- 封装了 `pkg/api` 中的 `Session` 作为 GORM 的数据库驱动
- 将 GORM 的 SQL 操作委托给 sqlexec 的 SQL 执行器
- 支持基本的 CRUD 操作、事务管理、查询等

## 快速开始

### 1. 创建 sqlexec 数据库和会话

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// 创建数据库
db, err := api.NewDB(&api.DBConfig{
    DebugMode: true,
})
if err != nil {
    log.Fatal(err)
}

// 注册内存数据源
config := &domain.DataSourceConfig{
    Type:     domain.DataSourceTypeMemory,
    Name:     "default",
    Writable: true,
}
memoryDS := memory.NewMVCCDataSource(config)
err = db.RegisterDataSource("default", memoryDS)
if err != nil {
    log.Fatal(err)
}

// 创建会话
session := db.Session()
```

### 2. 使用 GORM 与 sqlexec 驱动

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "gorm.io/gorm"
)

// 创建 sqlexec 会话
session := db.Session()

// 创建 GORM 驱动
dialector := gorm.NewDialector(session)

// 使用 GORM
gormDB, err := gorm.Open(dialector, &gorm.Config{})
if err != nil {
    log.Fatal(err)
}

// 现在可以使用 GORM 的所有功能
type User struct {
    gorm.Model
    ID   uint
    Name  string
    Email string
}

// 创建记录
result := gormDB.Create(&User{Name: "John", Email: "john@example.com"})

// 查询记录
var users []User
gormDB.Find(&users)

// 更新记录
gormDB.Model(&User{}).Where("id = ?", 1).Update("name", "Jane")

// 删除记录
gormDB.Delete(&User{}, 1)
```

## 支持的功能

### ✅ 内存数据源 (Memory) - 完全支持

内存数据源是 sqlexec 的默认数据源，支持以下所有功能：

- **表管理**: 创建表、删除表、清空表
- **CRUD 操作**: Create, Read, Update, Delete
- **条件查询**: Where, Or, Not
- **更新操作**: Update, Updates
- **删除操作**: Delete
- **事务管理**: Begin, Commit, Rollback
- **Where 条件**: Where, Or, Not
- **排序**: Order
- **限制/分页**: Limit, Offset
- **聚合**: Count, Group, Having
- **批量操作**: 批量创建、更新、删除
- **原生 SQL**: 支持 `Exec` 和 `Raw` 方法执行原生 SQL
- **复杂查询**: 支持 JOIN、子查询等复杂查询

### ⚠️ Slice 数据源 - 根据配置支持

Slice 数据源基于 Go 的 slice 数据，功能取决于配置：

- **只读模式**: 查询、条件查询、排序、分页、聚合
- **可写模式**: 除只读模式功能外，还支持创建、更新、删除操作
- **限制**: 不支持事务、不支持修改表结构

### 📝 文件数据源 (CSV/JSON/Excel) - 只读模式

文件数据源主要用于读取和查询外部文件：

- **支持的操作**:
  - 查询数据
  - 条件查询
  - 排序
  - 分页
  - 聚合
  - 原生 SQL 查询 (SELECT)

- **不支持的操作**:
  - 创建表
  - 删除表
  - 插入数据
  - 更新数据
  - 删除数据
  - 事务
  - 任何写入操作

### ❌ GORM 高级特性

以下 GORM 高级特性的支持情况：

- **自动迁移 (AutoMigrate)**: ✅ **已支持** - 可以自动创建表和添加缺失的列
- **关联**: ✅ **已支持** - GORM 会自动生成 JOIN 查询，无需特殊处理
- **钩子**: ⚠️ **部分支持** - BeforeCreate, AfterUpdate 等钩子可以在业务逻辑中手动实现

## 支持的数据源

### 内存数据源 (Memory)

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "gorm.io/gorm"
)

// 创建数据库
db, err := api.NewDB(&api.DBConfig{
    DebugMode: true,
})

// 创建内存数据源配置
config := &domain.DataSourceConfig{
    Type:     domain.DataSourceTypeMemory,
    Name:     "memory",
    Writable: true,
}

// 创建并注册内存数据源
memoryDS := memory.NewMVCCDataSource(config)
err = db.RegisterDataSource("memory", memoryDS)
if err != nil {
    log.Fatal(err)
}

// 创建会话
session := db.Session()

// 创建 GORM DB
gormDB, err := gorm.Open(gorm.NewDialector(session), &gorm.Config{})
if err != nil {
    log.Fatal(err)
}

// 使用 GORM 操作内存数据
type Product struct {
    ID    uint
    Name  string
    Price float64
}

product := Product{Name: "Laptop", Price: 999.99}
gormDB.Create(&product)
```

### CSV 数据源

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/csv"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "gorm.io/gorm"
)

// 创建数据库
db, err := api.NewDB(&api.DBConfig{
    DebugMode: true,
})

// 创建 CSV 数据源配置
config := &domain.DataSourceConfig{
    Type:     domain.DataSourceTypeCSV,
    Name:     "csv",
    Writable: false, // CSV 默认只读
    Options: map[string]interface{}{
        "delimiter": ",",
        "header":    true,
    },
}

// 创建并注册 CSV 数据源
csvDS := csv.NewCSVAdapter(config, "data/products.csv")
err = db.RegisterDataSource("csv", csvDS)
if err != nil {
    log.Fatal(err)
}

// 创建会话
session := db.Session()

// 创建 GORM DB
gormDB, err := gorm.Open(gorm.NewDialector(session), &gorm.Config{})
if err != nil {
    log.Fatal(err)
}

// 使用 GORM 查询 CSV 数据
type Product struct {
    ID    uint
    Name  string
    Price float64
}

var products []Product
gormDB.Find(&products)
```

### JSON 数据源

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/json"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "gorm.io/gorm"
)

// 创建数据库
db, err := api.NewDB(&api.DBConfig{
    DebugMode: true,
})

// 创建 JSON 数据源配置
config := &domain.DataSourceConfig{
    Type:     domain.DataSourceTypeJSON,
    Name:     "json",
    Writable: false, // JSON 默认只读
    Options: map[string]interface{}{
        "array_root": "", // 空字符串表示根是数组
    },
}

// 创建并注册 JSON 数据源
jsonDS := json.NewJSONAdapter(config, "data/users.json")
err = db.RegisterDataSource("json", jsonDS)
if err != nil {
    log.Fatal(err)
}

// 创建会话
session := db.Session()

// 创建 GORM DB
gormDB, err := gorm.Open(gorm.NewDialector(session), &gorm.Config{})
if err != nil {
    log.Fatal(err)
}

// 使用 GORM 查询 JSON 数据
type User struct {
    ID    uint
    Name  string
    Email string
}

var users []User
gormDB.Find(&users)
```

### Slice 数据源

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/api/gorm"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/slice"
    "gorm.io/gorm"
)

// 创建数据库
db, err := api.NewDB(&api.DBConfig{
    DebugMode: true,
})

// 准备数据
data := []map[string]interface{}{
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30},
    {"id": 3, "name": "Charlie", "age": 28},
}

// 创建 Slice 数据源配置
config := &domain.DataSourceConfig{
    Type:    "slice",
    Name:    "slice",
    Writable: false,
    Options: map[string]interface{}{
        "data":           data,
        "table_name":     "people",
        "database_name":  "default",
        "mvcc_supported": false,
    },
}

// 使用工厂创建 Slice 数据源
sliceFactory := slice.NewFactory()
sliceDS, err := sliceFactory.Create(config)
if err != nil {
    log.Fatal(err)
}

// 注册数据源
err = db.RegisterDataSource("slice", sliceDS)
if err != nil {
    log.Fatal(err)
}

// 创建会话
session := db.Session()

// 创建 GORM DB
gormDB, err := gorm.Open(gorm.NewDialector(session), &gorm.Config{})
if err != nil {
    log.Fatal(err)
}

// 使用 GORM 查询 Slice 数据
type Person struct {
    ID   uint
    Name string
    Age  int
}

var people []Person
gormDB.Find(&people)
```

## 使用示例

### 基本 CRUD 操作

```go
// 创建
user := User{Name: "John", Age: 30}
result := db.Create(&user)

// 查询所有
var users []User
db.Find(&users)

// 查询单个
var user User
db.First(&user, 1)

// 更新
db.Model(&user).Update("age", 31)

// 删除
db.Delete(&user)
```

### 条件查询

```go
// WHERE 条件
db.Where("name = ?", "John").Find(&users)

// 多个条件
db.Where("name = ? AND age > ?", "John", 25).Find(&users)

// OR 条件
db.Where("name = ? OR age > ?", "John", 25).Find(&users)

// IN 条件
db.Where("id IN ?", []int{1, 2, 3}).Find(&users)

// LIKE 条件
db.Where("name LIKE ?", "%John%").Find(&users)
```

### 事务处理

```go
// 开始事务
tx := db.Begin()

// 执行操作
if err := tx.Create(&User{Name: "John"}).Error; err != nil {
    tx.Rollback()
    return err
}

if err := tx.Create(&User{Name: "Jane"}).Error; err != nil {
    tx.Rollback()
    return err
}

// 提交事务
tx.Commit()
```

### 原生 SQL 查询

```go
// 使用 Exec 执行原生 SQL
db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))")

// 使用 Raw 执行查询并扫描
var result []map[string]interface{}
db.Raw("SELECT * FROM users WHERE age > ?", 18).Scan(&result)
```

### 使用 sqlexec 的 Query API

```go
// 从 GORM 的 DB 获取 sqlexec Session
sqlexecDB := db.Session()

// 直接使用 sqlexec 的查询功能
query, err := sqlexecDB.Query("SELECT * FROM users WHERE age > ?", 18)
if err != nil {
    log.Fatal(err)
}

// 遍历结果
for query.Next() {
    row := query.Row()
    var name string
    var age int
    row["name"] = &name
    row["age"] = &age
    
    fmt.Printf("User: %s, Age: %d\n", name, age)
}

query.Close()
```

## 性能考虑

1. **连接池**: sqlexec Session 会管理连接池，GORM 不会创建额外的连接
2. **查询缓存**: sqlexec 内置查询缓存，可以利用缓存提高性能
3. **批量操作**: 尽可能使用批量操作而不是单条操作
4. **索引**: 在底层表中创建合适的索引以提高查询性能

## 错误处理

```go
// 处理 GORM 错误
result := db.Create(&user)
if result.Error != nil {
    // result.Error 包含了 sqlexec 的错误信息
    log.Printf("Create failed: %v", result.Error)
}

// 处理事务错误
tx := db.Begin()
if err := tx.Create(&user).Error; err != nil {
    tx.Rollback()
    log.Printf("Transaction failed: %v", err)
    return err
}
tx.Commit()
```

## 限制和注意事项

1. **表结构**: 需要手动创建表结构，不支持 AutoMigrate
2. **类型映射**: GORM 类型到 sqlexec 类型的映射是基本的，可能需要调整
3. **复杂查询**: 某些复杂的 GORM 查询可能需要优化
4. **性能**: 大量数据操作时，考虑使用 sqlexec 的批量操作 API
5. **并发**: sqlexec Session 是并发安全的，可以在多个 goroutine 中使用

## 与标准 GORM 驱动的区别

| 特性 | 标准 GORM 驱动 | sqlexec GORM 驱动 |
|------|---------------------|-------------------|
| SQL 执行 | 数据库驱动直接执行 | 委托给 sqlexec 执行器 |
| 性能优化 | 针对特定数据库优化 | 通用优化，可自定义 |
| 功能支持 | 完整的数据库功能 | 依赖 sqlexec 的功能 |
| 可扩展性 | 有限 | 高度可扩展 |
| 使用场景 | 直接操作数据库 | 构建在 sqlexec 之上的应用 |

## 高级用法

### 自定义 SQL 解析器

如果需要将 GORM 的 SQL 转换为 sqlexec 可识别的格式，可以设置自定义解析器：

```go
dialector := gorm.NewDialector(session)

// 设置自定义 SQL 解析器
dialector.SetSQLParser(func(sql string) (string, []interface{}, error) {
    // 将 GORM 的 SQL 转换为 sqlexec 格式
    // 返回转换后的 SQL 和参数
    return sql, params, nil
})
```

### 与 sqlexec Query API 混合使用

```go
// 可以在同一个应用中混合使用 GORM 和 sqlexec
session := db.Session()

// 使用 GORM 处理 CRUD
gormDB := gorm.Open(gorm.NewDialector(session), &gorm.Config{})

// 使用 sqlexec 处理复杂查询
result, err := session.Query("SELECT * FROM users WHERE age > ? ORDER BY name LIMIT 10", 18)
if err != nil {
    log.Fatal(err)
}
```

## 总结

这个 GORM 驱动让您可以：
1. 使用 GORM 的 ORM 功能和查询构建器
2. 利用 sqlexec 的 SQL 执行引擎和优化
3. 在现有 sqlexec 应用中引入 GORM，无需重写代码
4. 逐步从 sqlexec 迁移到 GORM，或两者混合使用

这个适配器是连接 sqlexec 和 GORM 的桥梁，让您可以充分利用两个生态系统的优势。
