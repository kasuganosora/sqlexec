# 数据源接口

数据源接口提供了一个统一、可扩展的抽象层，用于支持多种数据源的读取、写入和删除操作。

## 特性

- **统一接口**: 通过 `DataSource` 接口提供一致的数据访问方式
- **多种数据源**: 支持内存、MySQL 等多种数据源类型
- **CRUD 操作**: 支持完整的增删改查操作
- **可扩展**: 易于添加新的数据源类型
- **连接管理**: 内置数据源管理器，支持多数据源管理

## 支持的数据源类型

### 1. 内存数据源 (MemorySource)

适用于测试、缓存等场景，数据存储在内存中。

```go
config := &resource.DataSourceConfig{
    Type: resource.DataSourceTypeMemory,
    Name: "my_memory_db",
}
```

### 2. MySQL 数据源 (MySQLSource)

连接到 MySQL/MariaDB 数据库。

```go
config := &resource.DataSourceConfig{
    Type:     resource.DataSourceTypeMySQL,
    Name:     "my_mysql_db",
    Host:     "localhost",
    Port:     3306,
    Username: "root",
    Password: "password",
    Database: "mydb",
}
```

### 3. 扩展新的数据源类型

实现 `DataSourceFactory` 和 `DataSource` 接口，然后注册工厂：

```go
// 1. 实现 DataSourceFactory 接口
type MyCustomFactory struct{}

func (f *MyCustomFactory) GetType() resource.DataSourceType {
    return "my_custom"
}

func (f *MyCustomFactory) Create(config *resource.DataSourceConfig) (resource.DataSource, error) {
    return &MyCustomSource{config: config}, nil
}

// 2. 实现 DataSource 接口
type MyCustomSource struct {
    config    *resource.DataSourceConfig
    // 其他字段...
}

// 实现 DataSource 接口的所有方法...

// 3. 注册工厂
func init() {
    resource.RegisterFactory(&MyCustomFactory{})
}
```

## 基本用法

### 1. 创建数据源

```go
import "mysql-proxy/mysql/resource"

// 创建配置
config := &resource.DataSourceConfig{
    Type: resource.DataSourceTypeMemory,
    Name: "test_db",
}

// 创建数据源
ds, err := resource.CreateDataSource(config)
if err != nil {
    log.Fatal(err)
}

// 连接数据源
if err := ds.Connect(context.Background()); err != nil {
    log.Fatal(err)
}
defer ds.Close(context.Background())
```

### 2. 创建表

```go
tableInfo := &resource.TableInfo{
    Name: "users",
    Columns: []resource.ColumnInfo{
        {Name: "id", Type: "int", Nullable: false, Primary: true},
        {Name: "name", Type: "varchar", Nullable: false},
        {Name: "email", Type: "varchar", Nullable: false},
        {Name: "age", Type: "int", Nullable: true},
    },
}

if err := ds.CreateTable(ctx, tableInfo); err != nil {
    log.Fatal(err)
}
```

### 3. 插入数据

```go
rows := []resource.Row{
    {"name": "Alice", "email": "alice@example.com", "age": 25},
    {"name": "Bob", "email": "bob@example.com", "age": 30},
}

inserted, err := ds.Insert(ctx, "users", rows, nil)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("插入了 %d 行数据\n", inserted)
```

### 4. 查询数据

```go
result, err := ds.Query(ctx, "users", &resource.QueryOptions{
    Filters: []resource.Filter{
        {Field: "age", Operator: ">=", Value: 25},
    },
    OrderBy: "age",
    Order:   "ASC",
    Limit:   10,
})
if err != nil {
    log.Fatal(err)
}

fmt.Printf("查询到 %d 行数据:\n", len(result.Rows))
for _, row := range result.Rows {
    fmt.Printf("  Name: %v, Age: %v\n", row["name"], row["age"])
}
```

### 5. 更新数据

```go
updates := resource.Row{"age": 31}
updated, err := ds.Update(ctx, "users",
    []resource.Filter{{Field: "name", Operator: "=", Value: "Bob"}},
    updates, nil)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("更新了 %d 行数据\n", updated)
```

### 6. 删除数据

```go
deleted, err := ds.Delete(ctx, "users",
    []resource.Filter{{Field: "age", Operator: "<", Value: 25}},
    nil)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("删除了 %d 行数据\n", deleted)
```

## 数据源管理器

`DataSourceManager` 提供了统一管理多个数据源的能力：

```go
import "mysql-proxy/mysql/resource"

// 创建管理器
manager := resource.NewDataSourceManager()

// 创建并注册数据源
config1 := &resource.DataSourceConfig{
    Type: resource.DataSourceTypeMemory,
    Name: "memory_db",
}

if err := manager.CreateAndRegister(ctx, "memory", config1); err != nil {
    log.Fatal(err)
}
defer manager.CloseAll(ctx)

// 设置默认数据源
manager.SetDefault("memory")

// 列出所有数据源
sources := manager.List()
fmt.Println("已注册的数据源:", sources)

// 获取数据源状态
status := manager.GetStatus()
for name, connected := range status {
    fmt.Printf("  %s: %v\n", name, connected)
}

// 使用指定数据源查询
result, err := manager.Query(ctx, "memory", "users", nil)
```

## 接口定义

### DataSource 接口

```go
type DataSource interface {
    // 连接数据源
    Connect(ctx context.Context) error
    
    // 关闭连接
    Close(ctx context.Context) error
    
    // 检查是否已连接
    IsConnected() bool
    
    // 获取数据源配置
    GetConfig() *DataSourceConfig
    
    // 获取所有表
    GetTables(ctx context.Context) ([]string, error)
    
    // 获取表信息
    GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)
    
    // 查询数据
    Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)
    
    // 插入数据
    Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error)
    
    // 更新数据
    Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)
    
    // 删除数据
    Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error)
    
    // 创建表
    CreateTable(ctx context.Context, tableInfo *TableInfo) error
    
    // 删除表
    DropTable(ctx context.Context, tableName string) error
    
    // 清空表
    TruncateTable(ctx context.Context, tableName string) error
    
    // 执行自定义SQL
    Execute(ctx context.Context, sql string) (*QueryResult, error)
}
```

### 数据结构

#### DataSourceConfig
```go
type DataSourceConfig struct {
    Type     DataSourceType         // 数据源类型
    Name     string                 // 数据源名称
    Host     string                 // 主机地址（可选）
    Port     int                    // 端口号（可选）
    Username string                 // 用户名（可选）
    Password string                 // 密码（可选）
    Database string                 // 数据库名（可选）
    Options  map[string]interface{} // 其他选项（可选）
}
```

#### QueryOptions
```go
type QueryOptions struct {
    Filters   []Filter // 查询过滤器
    OrderBy   string   // 排序字段
    Order     string   // 排序方向 (ASC, DESC)
    Limit     int      // 限制返回行数
    Offset    int      // 偏移量
}
```

#### Filter
```go
type Filter struct {
    Field    string      // 字段名
    Operator string      // 操作符 (=, !=, >, <, >=, <=, LIKE, IN)
    Value    interface{} // 值
}
```

## 高级用法

### 事务支持

某些数据源（如 MySQL）支持事务操作：

```go
// MySQL 数据源可以通过 Execute 方法执行事务SQL
ds.Execute(ctx, "BEGIN TRANSACTION")
ds.Execute(ctx, "INSERT INTO users VALUES (...)")
ds.Execute(ctx, "COMMIT")
```

### 批量操作

```go
// 批量插入
rows := make([]resource.Row, 1000)
for i := 0; i < 1000; i++ {
    rows[i] = resource.Row{
        "name": fmt.Sprintf("User%d", i),
        "email": fmt.Sprintf("user%d@example.com", i),
    }
}
inserted, err := ds.Insert(ctx, "users", rows, nil)
```

### 过滤器组合

```go
// 多条件过滤
result, err := ds.Query(ctx, "users", &resource.QueryOptions{
    Filters: []resource.Filter{
        {Field: "age", Operator: ">=", Value: 18},
        {Field: "age", Operator: "<=", Value: 60},
        {Field: "status", Operator: "=", Value: "active"},
    },
    OrderBy: "created_at",
    Order:   "DESC",
})
```

## 测试

运行测试：

```bash
go test ./mysql/resource/...
```

运行示例：

```bash
go test ./mysql/resource/... -run=Example
```

## 注意事项

1. **连接管理**: 始终记得在使用完数据源后关闭连接
2. **并发安全**: 所有数据源实现都是并发安全的
3. **错误处理**: 务必检查所有操作的错误
4. **资源清理**: 使用 defer 确保资源被正确释放
5. **性能考虑**: 批量操作比单条操作效率更高

## 未来计划

- [ ] 添加 PostgreSQL 数据源支持
- [ ] 添加 SQLite 数据源支持
- [ ] 支持分布式数据源
- [ ] 添加数据源健康检查
- [ ] 支持数据源热切换
- [ ] 添加查询缓存
- [ ] 支持读写分离

## 许可证

MIT License
