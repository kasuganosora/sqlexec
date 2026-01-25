# pkg/resource DDD 重构计划

## 当前结构分析

当前 `pkg/resource` 目录包含以下文件：
- `source.go` - 核心接口定义（DataSource, DataSourceFactory等）
- `manager.go` - 数据源管理器
- `base.go` - 基础数据源类
- `memory_source.go` - 内存数据源实现
- `mysql_source.go` - MySQL数据源实现
- `sqlite_source.go` - SQLite数据源实现
- `csv_source.go` - CSV文件数据源实现
- `excel_source.go` - Excel文件数据源实现
- `json_source.go` - JSON文件数据源实现
- `parquet_source.go` - Parquet文件数据源实现
- `cache.go` - 缓存实现
- `connection_pool.go` - 连接池实现
- `errors.go` - 错误定义
- `index.go` - 索引实现
- `util.go` - 工具函数
- `memory_mvcc.go` - MVCC支持

## 重构目标

按照 DDD (Domain-Driven Design) 模式重构，实现：
1. **清晰的分层架构**：Domain, Application, Infrastructure 分层
2. **数据源模块化**：一种数据源一个子目录（模块）
3. **内存作为基座**：内存模块作为所有其他模块的基座
4. **高内聚低耦合**：每个模块职责单一，依赖关系清晰

## 新的目录结构

```
pkg/resource/
├── domain/                    # 领域层 - 核心接口和模型
│   ├── datasource.go         # DataSource 接口定义
│   ├── factory.go            # DataSourceFactory 接口定义
│   ├── models.go             # 领域模型（DataSourceConfig, TableInfo, Row, QueryResult, Filter等）
│   ├── repository.go         # Repository 接口定义（可选）
│   └── errors.go             # 领域错误定义
│
├── memory/                    # 内存模块 - 作为基座
│   ├── datasource.go         # MemorySource 实现
│   ├── factory.go            # MemoryFactory
│   ├── mvcc.go               # MVCC 支持
│   ├── constraints.go        # 约束管理（唯一约束、外键约束）
│   ├── index.go              # 索引实现
│   └── util.go               # 内存模块工具函数
│
├── sql/                       # SQL 数据源基类
│   ├── datasource.go         # SQLDataSource 基类
│   ├── builder.go            # SQL 构建器
│   ├── tx.go                 # 事务支持
│   ├── cache.go              # 查询缓存
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
│   ├── factory.go            # MySQLFactory
│   ├── driver.go             # MySQL 驱动封装
│   └── converter.go          # 类型转换
│
├── sqlite/                    # SQLite 模块
│   ├── datasource.go         # SQLiteSource 实现
│   ├── factory.go            # SQLiteFactory
│   └── driver.go             # SQLite 驱动封装
│
├── csv/                       # CSV 模块
│   ├── datasource.go         # CSVSource 实现
│   ├── factory.go            # CSVFactory
│   ├── parser.go             # CSV 解析器
│   ├── writer.go             # CSV 写入器
│   └── schema.go             # Schema 推断
│
├── json/                      # JSON 模块
│   ├── datasource.go         # JSONSource 实现
│   ├── factory.go            # JSONFactory
│   ├── parser.go             # JSON 解析器
│   └── schema.go             # Schema 推断
│
├── excel/                     # Excel 模块
│   ├── datasource.go         # ExcelSource 实现
│   ├── factory.go            # ExcelFactory
│   ├── parser.go             # Excel 解析器
│   └── schema.go             # Schema 推断
│
├── parquet/                   # Parquet 模块
│   ├── datasource.go         # ParquetSource 实现
│   ├── factory.go            # ParquetFactory
│   ├── parser.go             # Parquet 解析器
│   └── schema.go             # Schema 推断
│
├── infrastructure/            # 基础设施层
│   ├── cache/
│   │   └── query_cache.go   # 查询缓存实现
│   ├── pool/
│   │   └── connection_pool.go # 连接池实现
│   ├── index/
│   │   └── index.go          # 通用索引实现
│   └── errors/
│       └── errors.go         # 基础设施错误
│
├── application/               # 应用层
│   ├── manager.go            # DataSourceManager
│   ├── registry.go           # 工厂注册表
│   └── service.go            # 应用服务
│
└── util/                      # 通用工具
    ├── filter.go             # 过滤器工具
    ├── order.go              # 排序工具
    ├── pagination.go         # 分页工具
    └── convert.go            # 类型转换工具
```

## 分层职责

### Domain Layer (领域层)
- 定义核心业务接口：`DataSource`, `DataSourceFactory`
- 定义领域模型：`DataSourceConfig`, `TableInfo`, `Row`, `QueryResult`, `Filter` 等
- 定义领域错误
- **不依赖任何具体实现**

### Memory Module (内存模块)
- 实现 `DataSource` 接口的内存版本
- 提供 MVCC 支持（多版本并发控制）
- 实现约束管理（唯一约束、外键约束）
- 实现索引功能
- **作为其他模块的基座**：
  - 可以作为缓存层
  - 可以作为测试的 mock 实现
  - 可以用于数据聚合和计算

### SQL Module (SQL 数据源基类)
- 抽象 SQL 数据源的通用实现
- 提供 SQL 构建器
- 提供事务支持
- 提供查询缓存和慢查询日志
- MySQL 和 SQLite 模块继承此基类

### File Module (文件数据源基类)
- 抽象文件数据源的通用实现
- 提供 Schema 推断接口
- 提供文件解析器接口
- CSV, JSON, Excel, Parquet 模块继承此基类

### Specific Data Source Modules (具体数据源模块)
每个模块独立实现：
- MySQL: 继承 SQL 基类，实现 MySQL 特定逻辑
- SQLite: 继承 SQL 基类，实现 SQLite 特定逻辑
- CSV: 继承 File 基类，实现 CSV 特定逻辑
- JSON: 继承 File 基类，实现 JSON 特定逻辑
- Excel: 继承 File 基类，实现 Excel 特定逻辑
- Parquet: 继承 File 基类，实现 Parquet 特定逻辑

### Infrastructure Layer (基础设施层)
- 提供跨领域的基础设施支持
- 缓存、连接池、索引等

### Application Layer (应用层)
- DataSourceManager：管理所有数据源实例
- Factory Registry：注册和查找数据源工厂
- 提供协调和编排功能

## 依赖关系

```
domain (领域层)
  ↑
  ├─ memory (内存模块)
  │   ↓
  │   infrastructure/
  │
  ├─ sql (SQL基类)
  │   ├─ mysql
  │   ├─ sqlite
  │   ↓
  │   infrastructure/
  │
  └─ file (文件基类)
      ├─ csv
      ├─ json
      ├─ excel
      └─ parquet
      ↓
      infrastructure/

application (应用层)
  ↑
  ├─ memory
  ├─ sql (mysql, sqlite)
  └─ file (csv, json, excel, parquet)
```

## 重构步骤

### 第一阶段：基础设施
1. 创建新的目录结构
2. 提取 domain 层
3. 创建 infrastructure 层

### 第二阶段：基座模块
1. 重构 memory 模块
2. 创建 sql 基类
3. 创建 file 基类

### 第三阶段：具体模块
1. 重构 mysql 模块
2. 重构 sqlite 模块
3. 重构 csv 模块
4. 重构 json 模块
5. 重构 excel 模块
6. 重构 parquet 模块

### 第四阶段：应用层
1. 重构 manager
2. 创建 registry
3. 确保所有模块正确注册

### 第五阶段：向后兼容
1. 创建兼容性适配器
2. 更新导入路径
3. 编写迁移文档

## 内存模块作为基座的设计

### 1. 基座功能
内存模块提供以下核心功能：

```go
// memory/datasource.go
type MemorySource struct {
    // 实现 DataSource 接口
    // 提供完整的 CRUD 操作
    // 支持 MVCC
    // 支持约束和索引
}
```

### 2. 与其他模块的集成

#### 2.1 作为缓存层
```go
// mysql/datasource.go
type MySQLSource struct {
    sql.SQLDataSource
    cache *memory.MemorySource  // 使用内存模块作为缓存
}
```

#### 2.2 作为聚合层
```go
// application/service.go
func (s *Service) QueryAcrossSources(ctx context.Context, sources []string) (*QueryResult, error) {
    // 使用内存模块聚合多个数据源的结果
    mem := memory.NewMemorySource(config)
    for _, src := range sources {
        result := s.querySource(ctx, src)
        mem.Insert(ctx, "result", result.Rows, nil)
    }
    return mem.Query(ctx, "result", options)
}
```

#### 2.3 作为测试 Mock
```go
// 测试时使用内存模块替代真实数据源
func TestService(t *testing.T) {
    mem := memory.NewMemorySource(config)
    // 设置测试数据
    // 测试服务逻辑
}
```

### 3. 内存模块的核心特性

- **MVCC 支持**：多版本并发控制，支持事务快照
- **约束管理**：唯一约束、外键约束、检查约束
- **索引支持**：B树索引、哈希索引
- **类型系统**：与 SQL 数据源兼容的类型系统
- **事务支持**：ACID 特性

## 向后兼容策略

### 1. 保留旧的导入路径
创建兼容层文件，导出旧的 API：

```go
// pkg/resource/source.go (兼容层)
package resource

import (
    "your-project/pkg/resource/domain"
    "your-project/pkg/resource/memory"
    "your-project/pkg/resource/mysql"
    // ...
)

// 重新导出类型和常量
type DataSource = domain.DataSource
type DataSourceConfig = domain.DataSourceConfig
// ...

// 重新导出工厂注册函数
func RegisterFactory(factory domain.DataSourceFactory) {
    application.GetRegistry().Register(factory)
}

func CreateDataSource(config *domain.DataSourceConfig) (domain.DataSource, error) {
    return application.GetRegistry().Create(config)
}
```

### 2. 渐进式迁移
- 不破坏现有代码
- 新代码可以使用新的模块化结构
- 提供迁移指南

## 优势

1. **清晰的职责划分**：每个模块职责单一
2. **高内聚低耦合**：模块之间依赖关系清晰
3. **易于扩展**：添加新数据源只需创建新模块
4. **易于测试**：可以独立测试每个模块
5. **代码复用**：基类提供通用实现
6. **性能优化**：内存模块可作为缓存和聚合层
7. **团队协作**：不同团队可以独立开发不同模块

## 注意事项

1. **循环依赖**：确保没有循环依赖
2. **接口稳定性**：domain 层接口定义后尽量不修改
3. **性能考虑**：抽象层不应引入过多性能开销
4. **向后兼容**：确保现有代码可以继续工作
