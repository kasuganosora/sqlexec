# pkg/resource DDD 重构总结

## 重构完成情况

本次重构按照 DDD (Domain-Driven Design) 设计模式，成功将 `pkg/resource` 模块重构为清晰的分层架构，实现了数据源模块化，内存模块作为所有模块的基座。

## 新的目录结构

```
pkg/resource/
├── domain/                    # 领域层 - 核心接口和模型
│   ├── datasource.go         # DataSource 接口定义
│   ├── factory.go            # DataSourceFactory 接口定义
│   ├── models.go             # 领域模型（DataSourceConfig, TableInfo, Row, QueryResult, Filter等）
│   └── errors.go             # 领域错误定义
│
├── memory/                    # 内存模块 - 作为基座
│   ├── datasource.go         # MemorySource 实现
│   ├── factory.go            # MemoryFactory
│   ├── mvcc.go               # MVCC 支持
│   ├── constraints.go        # 约束管理（唯一约束、外键约束）
│   └── index.go              # 索引实现
│
├── sql/                       # SQL 数据源基类
│   ├── datasource.go         # SQLDataSource 基类
│   ├── builder.go            # SQL 构建器
│   ├── tx.go                 # 事务支持
│   ├── statement_cache.go    # 语句缓存
│   └── slow_query.go         # 慢查询日志
│
├── file/                      # 文件数据源基类
│   ├── datasource.go         # FileDataSource 基类
│   ├── parser.go             # 文件解析器接口
│   ├── schema.go             # Schema 推断
│   └── util.go               # 文件操作工具
│
├── mysql/                     # MySQL 模块
│   ├── datasource.go         # MySQLSource 实现
│   └── factory.go            # MySQLFactory
│
├── sqlite/                    # SQLite 模块
│   ├── datasource.go         # SQLiteSource 实现
│   └── factory.go            # SQLiteFactory
│
├── csv/                       # CSV 模块
│   ├── datasource.go         # CSVSource 实现
│   └── factory.go            # CSVFactory
│
├── json/                      # JSON 模块
│   ├── datasource.go         # JSONSource 实现
│   └── factory.go            # JSONFactory
│
├── excel/                     # Excel 模块
│   ├── datasource.go         # ExcelSource 实现（占位符，待实现）
│   └── factory.go            # ExcelFactory
│
├── parquet/                   # Parquet 模块
│   ├── datasource.go         # ParquetSource 实现（占位符，待实现）
│   └── factory.go            # ParquetFactory
│
├── infrastructure/            # 基础设施层
│   ├── cache/
│   │   └── query_cache.go     # 查询缓存实现
│   ├── pool/
│   │   └── connection_pool.go # 连接池实现
│   └── errors/
│       └── errors.go          # 基础设施错误
│
├── application/               # 应用层
│   ├── manager.go            # DataSourceManager
│   └── registry.go           # 工厂注册表
│
├── util/                      # 通用工具
│   ├── string.go             # 字符串工具
│   ├── compare.go            # 比较工具
│   ├── filter.go             # 过滤器工具
│   ├── order.go              # 排序工具
│   └── pagination.go         # 分页工具
│
├── compat.go                  # 向后兼容层
└── REFACTOR_PLAN.md          # 重构计划文档
```

## 核心改进

### 1. 清晰的分层架构

- **Domain Layer (领域层)**: 定义核心业务接口和数据模型，不依赖任何具体实现
- **Infrastructure Layer (基础设施层)**: 提供缓存、连接池、错误处理等跨领域支持
- **Application Layer (应用层)**: 提供数据源管理、工厂注册、协调和编排功能

### 2. 数据源模块化

每个数据源类型独立为一个模块：
- MySQL: `mysql/`
- SQLite: `sqlite/`
- CSV: `csv/`
- JSON: `json/`
- Excel: `excel/`
- Parquet: `parquet/`

每个模块包含：
- `datasource.go` - 数据源实现
- `factory.go` - 工厂实现

### 3. 基类抽象

创建了两个重要的基类：
- **SQL 基类** (`sql/`): 提供所有 SQL 数据源的通用实现（MySQL、SQLite 等）
- **File 基类** (`file/`): 提供所有文件数据源的通用实现（CSV、JSON、Excel、Parquet 等）

### 4. 内存模块作为基座

内存模块 (`memory/`) 提供：
- 完整的 CRUD 操作实现
- MVCC 支持（多版本并发控制）
- 约束管理（唯一约束、外键约束）
- 索引功能（B树索引、哈希索引）

内存模块可以作为：
- **缓存层**：其他数据源可以使用内存模块作为缓存
- **聚合层**：用于数据聚合和计算
- **测试 Mock**：用于单元测试

### 5. 向后兼容

创建了 `compat.go` 文件，提供向后兼容性：
- 重新导出所有旧 API
- 现有代码无需修改即可继续工作
- 新代码可以使用新的模块化结构

## 关键文件说明

### Domain 层

- `domain/datasource.go`: 定义 `DataSource`、`TransactionalDataSource`、`Transaction` 接口
- `domain/factory.go`: 定义 `DataSourceFactory` 接口
- `domain/models.go`: 定义所有领域模型（DataSourceConfig、TableInfo、Row 等）
- `domain/errors.go`: 定义领域错误类型

### Memory 模块

- `memory/datasource.go`: 实现内存数据源的核心功能
- `memory/constraints.go`: 实现约束管理（唯一约束、外键约束）
- `memory/index.go`: 实现索引管理（B树索引、哈希索引）
- `memory/mvcc.go`: 提供 MVCC 支持（占位符，需要外部依赖）
- `memory/factory.go`: 实现工厂接口

### SQL 基类

- `sql/datasource.go`: SQL 数据源基类，提供通用实现
- `sql/builder.go`: SQL 查询构建器
- `sql/tx.go`: 事务支持
- `sql/statement_cache.go`: 语句缓存
- `sql/slow_query.go`: 慢查询日志

### File 基类

- `file/datasource.go`: 文件数据源基类，提供通用实现
- `file/parser.go`: 文件解析器接口
- `file/schema.go`: Schema 推断器
- `file/util.go`: 文件操作工具函数

### 具体数据源模块

每个模块（mysql、sqlite、csv、json、excel、parquet）都包含：
- `datasource.go`: 实现特定数据源逻辑
- `factory.go`: 实现工厂接口

### Application 层

- `application/registry.go`: 工厂注册表，管理所有数据源工厂
- `application/manager.go`: 数据源管理器，管理数据源实例

### Infrastructure 层

- `infrastructure/cache/query_cache.go`: 查询缓存实现
- `infrastructure/pool/connection_pool.go`: 连接池实现
- `infrastructure/errors/errors.go`: 基础设施错误

### Util 层

- `util/string.go`: 字符串工具函数
- `util/compare.go`: 比较工具函数
- `util/filter.go`: 过滤器工具函数
- `util/order.go`: 排序工具函数
- `util/pagination.go`: 分页工具函数

## 使用示例

### 使用新的模块化结构

```go
import (
    "your-project/pkg/resource/domain"
    "your-project/pkg/resource/mysql"
    "your-project/pkg/resource/csv"
    "your-project/pkg/resource/application"
)

// 注册工厂
registry := application.GetRegistry()
registry.Register(mysql.NewMySQLFactory())
registry.Register(csv.NewCSVFactory())

// 创建数据源管理器
manager := application.NewDataSourceManager()

// 创建 MySQL 数据源
mysqlConfig := &domain.DataSourceConfig{
    Type:     domain.DataSourceTypeMySQL,
    Name:     "mysql_db",
    Host:     "localhost",
    Port:     3306,
    Username: "root",
    Password: "password",
    Database: "test",
    Writable: true,
}

err := manager.CreateAndRegister(ctx, "mysql", mysqlConfig)

// 创建 CSV 数据源
csvConfig := &domain.DataSourceConfig{
    Type:     domain.DataSourceTypeCSV,
    Name:     "data.csv",
    Writable: false,
    Options: map[string]interface{}{
        "delimiter": ",",
        "header":    true,
    },
}

err = manager.CreateAndRegister(ctx, "csv", csvConfig)

// 查询数据
result, err := manager.Query(ctx, "mysql", "users", &domain.QueryOptions{
    Filters: []domain.Filter{
        {
            Field:    "age",
            Operator: ">",
            Value:    18,
        },
    },
    OrderBy: "name",
    Order:   "ASC",
})
```

### 使用向后兼容层

```go
import "your-project/pkg/resource"

// 使用旧 API（仍然有效）
config := &resource.DataSourceConfig{
    Type:     resource.DataSourceTypeMySQL,
    Name:     "mysql_db",
    // ...
}

ds, err := resource.CreateDataSource(config)
err = ds.Connect(context.Background())

result, err := ds.Query(context.Background(), "users", &resource.QueryOptions{
    Filters: []resource.Filter{
        {
            Field:    "age",
            Operator: ">",
            Value:    18,
        },
    },
})
```

## 迁移指南

### 对于新代码

推荐使用新的模块化结构：

1. 导入 `your-project/pkg/resource/domain` 使用领域模型
2. 导入具体的数据源模块（`mysql`、`csv` 等）使用数据源实现
3. 导入 `your-project/pkg/resource/application` 使用数据源管理器

### 对于现有代码

现有代码无需修改，`compat.go` 提供了向后兼容性。可以逐步迁移到新的模块化结构。

## 后续工作

1. **完善 Excel 模块**: 实现 Excel 文件的完整解析功能
2. **完善 Parquet 模块**: 实现 Parquet 文件的完整解析功能
3. **完善 MVCC 支持**: 完成内存模块的 MVCC 实现
4. **添加单元测试**: 为每个模块添加完整的单元测试
5. **性能优化**: 对性能热点进行优化
6. **文档完善**: 添加更多使用示例和 API 文档

## 总结

本次重构成功实现了以下目标：

1. **清晰的分层架构**: Domain、Application、Infrastructure 分层清晰
2. **数据源模块化**: 一种数据源一个子目录（模块）
3. **内存作为基座**: 内存模块作为所有其他模块的基座
4. **高内聚低耦合**: 每个模块职责单一，依赖关系清晰
5. **易于扩展**: 添加新数据源只需创建新模块
6. **向后兼容**: 现有代码无需修改即可继续工作
