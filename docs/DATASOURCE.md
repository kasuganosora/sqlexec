# 数据源架构技术原理

本文档描述 SQLExec 项目中数据源（DataSource）子系统的架构设计与实现原理。数据源子系统采用接口抽象 + 工厂注册表 + 方言分离的分层架构，使得 SQLExec 能够以统一的 SQL 协议访问 MySQL、PostgreSQL、HTTP 远程 API、文件（CSV/JSON/Excel/Parquet）以及通过插件动态加载的第三方数据源。

---

## 目录

1. [数据源接口](#1-数据源接口)
2. [工厂与注册表](#2-工厂与注册表)
3. [DataSourceManager](#3-datasourcemanager)
4. [SQL 数据源共享层](#4-sql-数据源共享层)
5. [MySQL 数据源](#5-mysql-数据源)
6. [PostgreSQL 数据源](#6-postgresql-数据源)
7. [HTTP 数据源](#7-http-数据源)
8. [文件数据源](#8-文件数据源)
9. [插件系统](#9-插件系统)
10. [配置加载](#10-配置加载)

---

## 1. 数据源接口

### 1.1 DataSource 接口

所有数据源必须实现 `domain.DataSource` 接口，定义于 `pkg/resource/domain/datasource.go`。该接口是整个数据源子系统的核心抽象，涵盖生命周期管理、元数据查询、CRUD 操作和原始 SQL 执行：

```go
type DataSource interface {
    // 生命周期
    Connect(ctx context.Context) error
    Close(ctx context.Context) error
    IsConnected() bool
    IsWritable() bool
    GetConfig() *DataSourceConfig

    // 元数据
    GetTables(ctx context.Context) ([]string, error)
    GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)

    // CRUD
    Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)
    Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error)
    Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)
    Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error)

    // DDL
    CreateTable(ctx context.Context, tableInfo *TableInfo) error
    DropTable(ctx context.Context, tableName string) error
    TruncateTable(ctx context.Context, tableName string) error

    // 原始 SQL
    Execute(ctx context.Context, sql string) (*QueryResult, error)
}
```

**设计要点：**

- 所有方法均接受 `context.Context`，支持超时、取消和链路追踪。
- `Query` 通过 `QueryOptions` 传入过滤器、排序、分页和列裁剪，各数据源自行将其转换为原生查询。
- `Filter` 支持嵌套逻辑（AND/OR），既可通过 `SubFilters` 字段递归，也可通过 `Value` 字段传入 `[]Filter` 实现。
- `IsWritable()` 由 `DataSourceConfig.Writable` 字段控制，写操作会在执行前检查此标志。

### 1.2 TransactionalDataSource 接口

对于支持事务的数据源，定义了扩展接口：

```go
type TransactionalDataSource interface {
    DataSource
    BeginTransaction(ctx context.Context, options *TransactionOptions) (Transaction, error)
}
```

`TransactionOptions` 支持设置隔离级别（READ UNCOMMITTED / READ COMMITTED / REPEATABLE READ / SERIALIZABLE）和只读事务标志。`Transaction` 接口提供 `Commit`、`Rollback`、`Execute`、`Query`、`Insert`、`Update`、`Delete` 方法。

### 1.3 核心领域模型

定义于 `pkg/resource/domain/models.go`：

| 类型 | 说明 |
|------|------|
| `DataSourceType` | 字符串枚举：`memory`、`mysql`、`postgresql`、`sqlite`、`csv`、`excel`、`json`、`parquet`、`http` |
| `DataSourceConfig` | 数据源配置：`Type`、`Name`、`Host`、`Port`、`Username`、`Password`、`Database`、`Writable`、`Options` |
| `TableInfo` | 表元数据：`Name`、`Schema`、`Columns`、`Temporary`、`Atts`、`Charset`、`Collation` |
| `ColumnInfo` | 列元数据：`Name`、`Type`、`Nullable`、`Primary`、`Default`、`Unique`、`AutoIncrement`、`ForeignKey`、`IsGenerated`、`VectorDim` 等 |
| `Row` | `map[string]interface{}` 类型别名，表示一行数据 |
| `QueryResult` | 查询结果：`Columns`、`Rows`、`Total` |
| `Filter` | 查询过滤器：支持 `Field/Operator/Value` 简单过滤和 `Logic/SubFilters` 嵌套逻辑 |

`Options` 字段为 `map[string]interface{}` 类型，各类型数据源从中解析自身专属配置（如 SQL 连接池参数、HTTP 认证信息、CSV 分隔符等）。

---

## 2. 工厂与注册表

### 2.1 DataSourceFactory 接口

定义于 `pkg/resource/domain/factory.go`，每种数据源类型提供一个工厂实现：

```go
type DataSourceFactory interface {
    Create(config *DataSourceConfig) (DataSource, error)
    GetType() DataSourceType
}
```

工厂的职责是：接收通用的 `DataSourceConfig`，解析其 `Options` 字段中的类型专属配置，创建并返回对应的 `DataSource` 实例。工厂不负责连接数据源，连接由上层管理器调用 `Connect()` 完成。

### 2.2 Registry 注册表

定义于 `pkg/resource/application/registry.go`，`Registry` 是一个线程安全的工厂注册表：

```go
type Registry struct {
    factories map[domain.DataSourceType]domain.DataSourceFactory
    mu        sync.RWMutex
}
```

**核心操作：**

| 方法 | 说明 |
|------|------|
| `Register(factory)` | 注册工厂，同一类型不可重复注册 |
| `Unregister(type)` | 注销工厂 |
| `Get(type)` | 获取指定类型的工厂 |
| `Create(config)` | 根据 `config.Type` 查找工厂并调用 `Create`，一步完成 |
| `List()` | 列出所有已注册的数据源类型 |
| `Exists(type)` | 检查某类型工厂是否已注册 |

**全局注册表：** 模块提供全局单例 `globalRegistry` 和便捷函数 `RegisterFactory`、`GetFactory`、`CreateDataSource`、`GetSupportedTypes`，供不需要自定义注册表的场景使用。

### 2.3 工厂注册流程

在 `server/server.go` 的 `NewServer()` 中，服务器启动时按以下顺序注册工厂：

```go
dsManager.GetRegistry().Register(httpds.NewHTTPFactory())
dsManager.GetRegistry().Register(mysqlds.NewMySQLFactory())
dsManager.GetRegistry().Register(pgds.NewPostgreSQLFactory())
```

注册完成后，从 `datasources.json` 加载配置并通过 `dsManager.CreateFromConfig()` 创建数据源实例。插件系统扫描 `datasource/` 目录后也会向同一注册表注册额外的工厂。

---

## 3. DataSourceManager

定义于 `pkg/resource/application/manager.go`，`DataSourceManager` 是数据源的编排和路由中心：

```go
type DataSourceManager struct {
    sources      map[string]domain.DataSource   // name -> DataSource
    registry     *Registry                       // 工厂注册表
    defaultDS    string                          // 默认数据源名称
    enabledTypes map[domain.DataSourceType]bool  // 启用的数据源类型白名单
    mu           sync.RWMutex
}
```

### 3.1 核心职责

**数据源生命周期管理：**
- `Register(name, ds)` -- 注册已创建的数据源实例。第一个注册的数据源自动成为默认数据源。
- `Unregister(name)` -- 注销数据源，自动调用 `Close()` 释放资源。若注销的是默认数据源，自动选择另一个作为新默认值。
- `ConnectAll(ctx)` -- 连接所有尚未连接的数据源。
- `CloseAll(ctx)` -- 关闭所有数据源，容忍个别关闭失败。

**创建与注册一体化：**
- `CreateFromConfig(config)` -- 检查类型白名单，调用 `Registry.Create()` 创建数据源。
- `CreateAndRegister(ctx, name, config)` -- 创建 + 连接 + 注册，若连接或注册失败自动清理。

**路由与代理：**
- `Get(name)` / `GetDefault()` -- 获取指定或默认数据源。
- 提供 `Query`、`Insert`、`Update`、`Delete`、`CreateTable`、`DropTable`、`TruncateTable`、`Execute` 等便捷方法，接受 `dsName` 参数，内部先通过 `Get()` 获取数据源再委派调用。

**状态监控：**
- `GetStatus()` -- 返回 `map[string]bool`，每个数据源名称对应其连接状态。
- `GetDefaultName()` -- 返回当前默认数据源名称。

### 3.2 类型白名单

通过 `SetEnabledTypes(types)` 设置启用的数据源类型。设置后，`CreateFromConfig()` 会拒绝创建未启用类型的数据源。若未设置白名单（`enabledTypes` 为空），则所有类型均允许。

---

## 4. SQL 数据源共享层

MySQL 和 PostgreSQL 数据源共享一套位于 `server/datasource/sql/` 的基础设施，通过策略模式（Dialect 接口）消除数据库差异。

### 4.1 Dialect 接口

定义于 `server/datasource/sql/dialect.go`：

```go
type Dialect interface {
    DriverName() string                                          // "mysql" 或 "postgres"
    BuildDSN(dsCfg, sqlCfg) (string, error)                     // 构建连接字符串
    QuoteIdentifier(name string) string                          // 标识符引用：`name` 或 "name"
    Placeholder(n int) string                                    // 参数占位符：? 或 $N
    GetTablesQuery() string                                      // 列出所有用户表的 SQL
    GetTableInfoQuery() string                                   // 获取列元数据的 SQL
    MapColumnType(dbTypeName string, scanType) string            // 数据库类型 -> 领域类型映射
    GetDatabaseName(dsCfg, sqlCfg) string                        // 虚拟数据库名
}
```

### 4.2 SQLConfig 共享配置

定义于 `server/datasource/sql/config.go`，从 `DataSourceConfig.Options` 中通过 JSON 序列化/反序列化提取：

```go
type SQLConfig struct {
    MaxOpenConns    int    // 最大打开连接数（默认 25）
    MaxIdleConns    int    // 最大空闲连接数（默认 5）
    ConnMaxLifetime int    // 连接最大生命周期秒数（默认 300）
    ConnMaxIdleTime int    // 连接最大空闲时间秒数（默认 60）
    SSLMode         string // TLS 模式（默认 "disable"）
    SSLCert/SSLKey/SSLRootCert string
    Charset         string // MySQL 字符集（默认 "utf8mb4"）
    Collation       string // MySQL 排序规则（默认 "utf8mb4_unicode_ci"）
    ParseTime       *bool  // MySQL 时间解析（默认 true）
    Schema          string // PostgreSQL search_path（默认 "public"）
    ConnectTimeout  int    // 连接超时秒数（默认 10）
}
```

### 4.3 SQLCommonDataSource

定义于 `server/datasource/sql/common.go`，是 MySQL 和 PostgreSQL 数据源的共享基类：

```go
type SQLCommonDataSource struct {
    mu        sync.RWMutex
    config    *domain.DataSourceConfig
    sqlCfg    *SQLConfig
    dialect   Dialect
    db        *sql.DB
    connected bool
}
```

**连接流程 (`Connect`)：**
1. 调用 `dialect.BuildDSN()` 构建连接字符串。
2. 调用 `sql.Open(dialect.DriverName(), dsn)` 打开数据库。
3. 配置连接池参数：`SetMaxOpenConns`、`SetMaxIdleConns`、`SetConnMaxLifetime`、`SetConnMaxIdleTime`。
4. 调用 `db.PingContext()` 验证连通性，超时由 `ConnectTimeout` 控制。

**查询流程 (`Query`)：**
1. 调用 `BuildSelectSQL(dialect, tableName, options, paramOffset)` 生成 SELECT 语句和参数列表。
2. 执行 `db.QueryContext(ctx, sql, params...)`。
3. 调用 `ScanRows(rows, dialect)` 将 `*sql.Rows` 转换为 `[]domain.Row` 和 `[]domain.ColumnInfo`。

**写操作（`Insert`/`Update`/`Delete`）：**
- 写操作前先检查 `config.Writable`，若为 false 返回 `ErrReadOnly`。
- 分别调用 `BuildInsertSQL`、`BuildUpdateSQL`、`BuildDeleteSQL` 生成 SQL，执行后返回 `RowsAffected`。

**Execute（原始 SQL）：**
- 通过前缀匹配（SELECT/SHOW/DESCRIBE/EXPLAIN/WITH）判断是否为查询语句。
- 查询语句走 `QueryContext` 路径，DML/DDL 走 `ExecContext` 路径（同样检查 `Writable`）。

### 4.4 SQL Builder

定义于 `server/datasource/sql/builder.go`，提供四个核心构建函数：

| 函数 | 说明 |
|------|------|
| `BuildSelectSQL(d, table, options, offset)` | 根据 `QueryOptions` 构建 SELECT 语句，支持列裁剪、WHERE、ORDER BY、LIMIT/OFFSET |
| `BuildWhereClause(d, filters, offset)` | 将 `[]Filter` 递归构建为 WHERE 子句，支持 `=`、`!=`、`>`、`<`、`>=`、`<=`、`LIKE`、`IN`、`BETWEEN`、`IS NULL`、`IS NOT NULL` |
| `BuildInsertSQL(d, table, rows)` | 构建批量 INSERT 语句，列名按字母排序保证确定性 |
| `BuildUpdateSQL(d, table, filters, updates)` | 构建 UPDATE SET ... WHERE ... 语句 |
| `BuildDeleteSQL(d, table, filters)` | 构建 DELETE FROM ... WHERE ... 语句 |

所有 Builder 函数通过 `d.Placeholder(n)` 和 `d.QuoteIdentifier(name)` 实现方言无关。`paramOffset` 参数使得 PostgreSQL 的 `$N` 占位符可以正确编号。

### 4.5 Scanner

定义于 `server/datasource/sql/scanner.go`，`ScanRows` 函数将 `*sql.Rows` 转换为领域模型：

1. 通过 `rows.ColumnTypes()` 获取列类型元数据。
2. 对每列调用 `dialect.MapColumnType()` 将数据库类型映射为领域类型。
3. 扫描每行数据，调用 `normalizeValue()` 将 `[]byte` 转为 `string`、`time.Time` 格式化为 `"2006-01-02 15:04:05"` 等标准形式。

---

## 5. MySQL 数据源

位于 `server/datasource/mysql/`，由三个文件组成。

### 5.1 MySQLDialect

实现 `Dialect` 接口的 MySQL 特化：

- **DriverName:** `"mysql"`
- **BuildDSN:** 使用 `github.com/go-sql-driver/mysql` 的 `Config` 结构体构建 DSN。默认端口 3306，配置 `AllowNativePasswords`、`Collation`、`Charset`、`ParseTime`、`Timeout`。TLS 模式根据 `SSLMode` 映射为 `"true"`/`"skip-verify"`/`"false"`。
- **QuoteIdentifier:** 反引号包裹 `` `name` ``，内部反引号转义为 ```` `` ````。
- **Placeholder:** 始终返回 `"?"`（MySQL 使用位置占位符）。
- **GetTablesQuery:** 查询 `INFORMATION_SCHEMA.TABLES`，过滤 `TABLE_SCHEMA = DATABASE()` 且 `TABLE_TYPE = 'BASE TABLE'`。
- **GetTableInfoQuery:** 查询 `INFORMATION_SCHEMA.COLUMNS`，返回 `COLUMN_NAME`、`COLUMN_TYPE`、`IS_NULLABLE`、`COLUMN_KEY`、`COLUMN_DEFAULT`、`EXTRA`，按 `ORDINAL_POSITION` 排序。`COLUMN_KEY = 'PRI'` 标识主键，`EXTRA` 中包含 `auto_increment` 标识自增列。
- **MapColumnType:** 将 MySQL 类型映射为领域类型。特殊处理 `tinyint(1)` 映射为 `"bool"`。整数类族（tinyint/smallint/mediumint/int/bigint）统一映射为 `"int"`，浮点类族映射为 `"float64"`，字符串类族映射为 `"string"`，时间类族根据精度映射为 `"date"`/`"time"`/`"datetime"`。

### 5.2 MySQLDataSource

嵌入 `SQLCommonDataSource`，在构造时注入 `MySQLDialect`：

```go
type MySQLDataSource struct {
    *sqlcommon.SQLCommonDataSource
}
```

所有 DataSource 接口方法均由 `SQLCommonDataSource` 提供，MySQL 无需覆盖任何方法。

### 5.3 MySQLFactory

工厂实现：

```go
func (f *MySQLFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
    sqlCfg, err := sqlcommon.ParseSQLConfig(config)
    // ...
    return NewMySQLDataSource(config, sqlCfg)
}
```

---

## 6. PostgreSQL 数据源

位于 `server/datasource/postgresql/`，结构与 MySQL 完全对称。

### 6.1 PostgreSQLDialect

实现 `Dialect` 接口的 PostgreSQL 特化：

- **DriverName:** `"postgres"`
- **BuildDSN:** 构建 key=value 格式的连接字符串（`host=... port=... user=... password=... dbname=... sslmode=...`）。默认端口 5432。支持 `search_path`、`connect_timeout`、`sslcert`/`sslkey`/`sslrootcert` 参数。
- **QuoteIdentifier:** 双引号包裹 `"name"`，内部双引号转义为 `""`。
- **Placeholder:** 返回 `"$N"`（PostgreSQL 使用编号占位符），例如 `$1`、`$2`、`$3`。
- **GetTablesQuery:** 查询 `information_schema.tables`，过滤 `table_schema = current_schema()` 且 `table_type = 'BASE TABLE'`。
- **GetTableInfoQuery:** 通过三表 JOIN 检测主键：
  - `information_schema.columns` (c) -- 列元数据
  - `information_schema.table_constraints` (tc) -- 约束类型过滤 `PRIMARY KEY`
  - `information_schema.key_column_usage` (kcu) -- 约束关联到具体列

  当 `kcu.column_name IS NOT NULL` 时标记 `column_key = 'PRI'`。使用 `$1` 占位符传入表名。PostgreSQL 使用 `data_type` 而非 MySQL 的 `column_type`，`parseColumnInfo` 会自动适配。

- **MapColumnType:** PostgreSQL 类型映射覆盖范围广泛：
  - 整数：smallint/integer/bigint/serial/bigserial 等 -> `"int"`
  - 浮点：real/float4 -> `"float64"`，double precision/numeric/decimal/money -> `"float64"`
  - 布尔：boolean/bool -> `"bool"`
  - 字符串：varchar/char/text/citext/bytea/json/jsonb/uuid/inet/xml/geometry 类型等 -> `"string"`
  - 时间：date -> `"date"`，time/timetz -> `"time"`，timestamp/timestamptz -> `"datetime"`

### 6.2 PostgreSQLDataSource

嵌入 `SQLCommonDataSource`，导入 `github.com/lib/pq` 驱动（通过 `_ "github.com/lib/pq"` 匿名导入注册驱动）：

```go
type PostgreSQLDataSource struct {
    *sqlcommon.SQLCommonDataSource
}
```

### 6.3 PostgreSQLFactory

与 MySQL 工厂对称：解析 `SQLConfig`，创建 `PostgreSQLDataSource`。

---

## 7. HTTP 数据源

位于 `server/datasource/http/`，实现了将远程 HTTP API 适配为 `DataSource` 接口的能力。

### 7.1 架构概览

```
DataSourceConfig
    |
    v
HTTPConfig（解析 Options）
    |
    v
HTTPClient（封装 HTTP 请求）
    |
    v
HTTPDataSource（实现 DataSource 接口）
```

### 7.2 HTTPConfig

从 `DataSourceConfig.Options` 解析，定义于 `server/datasource/http/config.go`：

```go
type HTTPConfig struct {
    BasePath     string            // URL 基础路径前缀
    Paths        *PathsConfig      // 各操作的路径模板
    AuthType     string            // 认证类型：bearer / basic / api_key / ""
    AuthToken    string            // Bearer token
    APIKeyHeader string            // API Key 头名（默认 "X-API-Key"）
    APIKeyValue  string            // API Key 值
    TimeoutMs    int               // 请求超时毫秒数（默认 30000）
    RetryCount   int               // 重试次数（默认 0）
    RetryDelayMs int               // 重试间隔毫秒数（默认 1000）
    TLSSkipVerify bool             // 跳过 TLS 证书验证
    TLSCACert    string            // 自定义 CA 证书路径
    Headers      map[string]string // 自定义请求头（支持模板渲染）
    Database     string            // SQL 中的虚拟数据库名
    TableAlias   map[string]string // SQL 表名 -> HTTP 端表名映射
    ACL          *ACLConfig        // 访问控制列表
}
```

**默认路径模板：**

| 操作 | 默认路径 | HTTP 方法 |
|------|----------|-----------|
| 表列表 | `/_schema/tables` | GET |
| 表结构 | `/_schema/tables/{table}` | GET |
| 查询 | `/_query/{table}` | POST |
| 插入 | `/_insert/{table}` | POST |
| 更新 | `/_update/{table}` | POST |
| 删除 | `/_delete/{table}` | POST |
| 健康检查 | `/_health` | GET |

`{table}` 会在请求时替换为实际的表名。

### 7.3 表名别名（TableAlias）

通过 `table_alias` 配置，可以将 SQL 中的表名映射为远程 API 使用的不同表名：

```json
{
  "table_alias": {
    "users": "api_users",
    "orders": "api_orders"
  }
}
```

`ResolveTableName(sqlTable)` 在发送请求前将 SQL 表名转换为 HTTP 端表名。`GetTables()` 返回表列表时会反向映射回 SQL 表名。

### 7.4 认证模式

`HTTPClient.setAuth()` 支持三种认证方式：

| AuthType | 行为 |
|----------|------|
| `bearer` | 设置 `Authorization: Bearer <auth_token>` 头 |
| `basic` | 使用 `DataSourceConfig.Username/Password` 进行 HTTP Basic Auth |
| `api_key` | 设置 `<api_key_header>: <api_key_value>` 头 |

### 7.5 模板头与签名

自定义 `Headers` 支持 `{{变量}}` 和 `{{函数(参数)}}` 模板语法，定义于 `server/datasource/http/template.go`。

**可用变量：**

| 变量 | 说明 |
|------|------|
| `timestamp` | Unix 时间戳（秒） |
| `timestamp_ms` | Unix 时间戳（毫秒） |
| `uuid` | 随机 UUID |
| `nonce` | 8 字节随机十六进制串 |
| `date` | 当前日期 `YYYY-MM-DD` |
| `datetime` | UTC 时间 ISO 8601 格式 |
| `method` | HTTP 方法 |
| `path` | 请求路径 |
| `body` | 请求体 JSON 字符串 |
| `auth_token` | 配置中的 auth_token |

**可用函数：**

| 函数 | 说明 |
|------|------|
| `hmac_sha256(key, data)` | HMAC-SHA256 签名 |
| `hmac_md5(key, data)` | HMAC-MD5 签名 |
| `md5(data)` | MD5 哈希 |
| `sha256(data)` | SHA256 哈希 |
| `base64(data)` | Base64 编码 |
| `upper(data)` | 转大写 |
| `lower(data)` | 转小写 |

函数参数支持 `+` 拼接变量和字面字符串，例如：
```
{{hmac_sha256(auth_token, method + path + timestamp)}}
```

### 7.6 ACL 访问控制

`ACLConfig` 提供两级访问控制：

```go
type ACLConfig struct {
    AllowedUsers []string            // 白名单用户列表
    Permissions  map[string][]string // 用户 -> 允许的操作列表
}
```

`CheckACL(user, operation)` 的检查逻辑：
1. 若 `AllowedUsers` 非空，用户必须在白名单中，否则拒绝。
2. 若 `Permissions` 非空且用户有对应条目，操作必须在用户的权限列表中（或包含 `"ALL"`/`"ALL PRIVILEGES"`）。
3. 若用户在 `AllowedUsers` 中但不在 `Permissions` 中，默认允许所有操作。

ACL 检查在 `Query` 方法中通过 `QueryOptions.User` 字段触发。

### 7.7 重试与错误处理

- `doRequest` 方法支持自动重试，最多 `RetryCount + 1` 次尝试。
- 仅在 5xx 错误或连接错误时重试，4xx 错误不重试。
- 重试间隔由 `RetryDelayMs` 控制。
- 错误响应解析为 `HTTPError` 结构体，包含 `StatusCode`、`Code`、`Message`。

### 7.8 不支持的操作

HTTP 数据源不支持 DDL 操作（`CreateTable`、`DropTable`、`TruncateTable`）和原始 SQL 执行（`Execute`），调用时返回 `ErrUnsupportedOperation`。

---

## 8. 文件数据源

CSV、JSON、Excel、Parquet 四种文件数据源采用统一的适配器模式：将文件内容加载到 MVCC 内存数据源（`memory.MVCCDataSource`），以内存表的形式提供查询能力。

### 8.1 通用架构

```
文件 (CSV/JSON/Excel/Parquet)
    |  Connect() 时读取
    v
MVCCDataSource（内存表）
    |  Query/Insert/Update/Delete 委派
    v
DataSource 接口
    |  Close() 时写回（若 writable）
    v
文件（原子写回）
```

每种适配器都继承（嵌入）`memory.MVCCDataSource`，并覆盖 `Connect()` 和 `Close()` 方法以实现文件加载和写回。CRUD 操作全部委派给内存数据源。

### 8.2 CSV 数据源

- **工厂：** `CSVFactory`，文件路径从 `config.Database` 或 `options["path"]` 获取。
- **配置选项：** `delimiter`（分隔符，默认 `,`）、`header`（是否有表头，默认 `true`）、`writable`。
- **加载流程：**
  1. 使用 `encoding/csv.Reader` 读取所有行。
  2. 若有表头，第一行作为列名；否则自动生成 `column_1`、`column_2` 等。
  3. 采样前 100 行推断每列类型（int64/float64/bool/string），选择出现最多的类型。
  4. 数据加载到名为 `"csv_data"` 的内存表中。
- **写回：** 可写模式下，`Close()` 时将内存中最新数据写回原始 CSV 文件。

### 8.3 JSON 数据源

- **工厂：** `JSONFactory`，文件路径同 CSV。
- **配置选项：** `array_root`（JSON 数组所在的根键名）、`writable`。
- **加载流程：**
  1. 读取整个 JSON 文件，解析为 `interface{}`。
  2. 若配置了 `array_root`，从根对象的指定键获取数组；否则直接解析为数组。
  3. 采样前 100 行推断列类型，JSON 数字区分整数和浮点。
  4. 数据加载到名为 `"json_data"` 的内存表中。
- **写回：** 使用原子写入策略：先写临时文件，再 `os.Rename` 替换原文件，避免写入中途失败导致数据丢失。

### 8.4 Excel 数据源

- **工厂：** `ExcelFactory`，文件路径同上。
- **配置选项：** `sheet_name`（工作表名，默认使用第一个工作表）、`writable`。
- **加载流程：**
  1. 使用 `github.com/xuri/excelize/v2` 打开 Excel 文件。
  2. 第一行作为列头，后续行作为数据。
  3. 采样推断列类型。
  4. 以工作表名作为内存表名。
- **写回：** 安全写回策略：在同一 Excel 文件中创建临时工作表写入数据，成功后删除旧表并重命名临时表，最后 `SaveAs` 保存。

### 8.5 Parquet 数据源

- **工厂：** `ParquetFactory`。
- **状态：** 当前为简化实现（占位符），返回固定的测试数据。完整实现需集成 Apache Arrow/Parquet 库。
- **配置选项：** `table_name`（内存表名，默认 `"parquet_data"`）、`writable`（默认 `false`）。

### 8.6 文件数据源的限制

所有文件数据源不支持 DDL 操作（`CreateTable`、`DropTable`）和原始 SQL 执行（`Execute`），调用时返回 `ErrReadOnly` 或 `ErrUnsupportedOperation`。

---

## 9. 插件系统

位于 `pkg/plugin/`，允许通过动态链接库（.so / .dll）加载第三方数据源。

### 9.1 架构组件

| 组件 | 说明 |
|------|------|
| `PluginLoader` | 接口，定义 `Load(path)` 和 `SupportedExtension()` 方法 |
| `GoPluginLoader` | Linux/macOS 实现，加载 `.so` 文件 |
| `DLLPluginLoader` | Windows 实现，加载 `.dll` 文件 |
| `unsupportedLoader` | 其他平台的占位实现 |
| `PluginManager` | 管理插件扫描、加载和数据源创建的编排器 |

平台选择通过 Go 的 `//go:build` 标签在编译时确定：
- `linux || darwin || freebsd` -> `GoPluginLoader`
- `windows` -> `DLLPluginLoader`
- 其他平台 -> `unsupportedLoader`

### 9.2 Go Plugin（.so）加载

`GoPluginLoader` 使用 Go 标准库 `plugin` 包：

1. 调用 `plugin.Open(path)` 加载 `.so` 文件。
2. 查找 `NewFactory` 符号，类型必须为 `*func() domain.DataSourceFactory`。
3. 调用 `(*factoryFn)()` 获取工厂实例。
4. 可选查找 `PluginVersion`（`*string`）和 `PluginDescription`（`*string`）符号获取元数据。

**插件开发约定：**
```go
// 插件必须导出以下变量
var NewFactory func() domain.DataSourceFactory = func() domain.DataSourceFactory {
    return &MyCustomFactory{}
}

// 可选元数据
var PluginVersion string = "1.0.0"
var PluginDescription string = "My custom datasource"
```

### 9.3 DLL Plugin（.dll）加载

`DLLPluginLoader` 使用 `syscall.LoadDLL`，通过 JSON-RPC 协议与 DLL 通信：

**DLL 必须导出三个函数：**

| 导出函数 | 签名 | 说明 |
|----------|------|------|
| `PluginGetInfo` | `() -> *char` | 返回 JSON 格式的插件信息 |
| `PluginHandleRequest` | `(*char) -> *char` | 接收 JSON-RPC 请求，返回 JSON-RPC 响应 |
| `PluginFreeString` | `(*char) -> void` | 释放由 DLL 分配的字符串内存 |

**JSON-RPC 请求格式：**
```json
{
  "method": "query",
  "id": "datasource_instance_name",
  "params": { "table": "users", "options": {...} }
}
```

**JSON-RPC 响应格式：**
```json
{
  "result": { "columns": [...], "rows": [...], "total": 42 },
  "error": ""
}
```

`DLLDataSourceFactory` 和 `DLLDataSource` 将 `DataSource` 接口的每个方法转换为对应的 JSON-RPC 调用（`create`、`connect`、`close`、`get_tables`、`get_table_info`、`query`、`insert`、`update`、`delete`、`create_table`、`drop_table`、`truncate_table`、`execute`），C 字符串通过 `cStringToGoString` 读取（最大 1MB 安全限制），读取后调用 `PluginFreeString` 释放内存。

### 9.4 PluginManager 工作流

定义于 `pkg/plugin/manager.go`：

```
ScanAndLoad(pluginDir)
    |
    |--> 遍历目录中匹配扩展名的文件
    |       |
    |       |--> LoadPlugin(path)
    |       |       |
    |       |       |--> loader.Load(path) -> factory + info
    |       |       |--> registry.Register(factory)
    |       |       |--> 记录 PluginInfo
    |       |
    |       v
    |--> createDatasourcesFromConfig()
            |
            |--> config_schema.LoadDatasources(configDir)
            |--> 筛选 type 匹配已加载插件的配置
            |--> CreateFromConfig + Connect + Register
```

在 `server/server.go` 中，插件加载发生在内置工厂注册和 `datasources.json` 加载之后：
```go
pluginDir := filepath.Join(dataDir, "datasource")
pluginMgr := plugin.NewPluginManager(registry, dsManager, configDir)
pluginMgr.ScanAndLoad(pluginDir)
```

---

## 10. 配置加载

### 10.1 datasources.json 格式

数据源配置文件位于服务器启动目录下的 `datasources.json`，格式为 JSON 数组：

```json
[
  {
    "type": "mysql",
    "name": "my_mysql",
    "host": "127.0.0.1",
    "port": 3306,
    "username": "root",
    "password": "secret",
    "database": "mydb",
    "writable": true,
    "options": {
      "max_open_conns": 50,
      "charset": "utf8mb4",
      "ssl_mode": "disable"
    }
  },
  {
    "type": "postgresql",
    "name": "my_pg",
    "host": "127.0.0.1",
    "port": 5432,
    "username": "postgres",
    "password": "secret",
    "database": "mydb",
    "writable": true,
    "options": {
      "schema": "public",
      "ssl_mode": "require"
    }
  },
  {
    "type": "http",
    "name": "remote_api",
    "host": "https://api.example.com",
    "writable": false,
    "options": {
      "auth_type": "bearer",
      "auth_token": "my-secret-token",
      "timeout_ms": 10000,
      "table_alias": {
        "users": "api_users"
      },
      "acl": {
        "allowed_users": ["admin", "reader"],
        "permissions": {
          "reader": ["SELECT"]
        }
      }
    }
  },
  {
    "type": "csv",
    "name": "my_csv",
    "database": "/path/to/data.csv",
    "writable": false,
    "options": {
      "delimiter": ",",
      "header": true
    }
  }
]
```

### 10.2 加载流程

`config_schema.LoadDatasources(configDir)` 定义于 `pkg/config_schema/json_persistence.go`：

1. 拼接路径：`filepath.Join(configDir, "datasources.json")`。
2. 读取文件内容，若文件不存在返回空数组（不报错）。
3. JSON 反序列化为 `[]domain.DataSourceConfig`。
4. 使用 `sync.Mutex` 保护并发访问。

### 10.3 服务器启动流程

`server/server.go` 中 `NewServer()` 的完整数据源初始化流程：

```
1. 创建 API DB 实例
2. 创建 MVCC 内存数据源 "default"，连接并注册
3. 注册内置工厂：
   - HTTPFactory
   - MySQLFactory
   - PostgreSQLFactory
4. 加载 datasources.json：
   - 遍历配置数组
   - 对每个配置调用 dsManager.CreateFromConfig()
   - 特殊处理：type=memory 时 fallback 到直接创建 MVCCDataSource
   - 连接数据源 ds.Connect(ctx)
   - 注册到 dsManager.Register(name, ds)
5. 扫描 datasource/ 目录加载插件：
   - 创建 PluginManager
   - ScanAndLoad(pluginDir)
   - 插件工厂注册到同一 Registry
   - 自动为匹配插件类型的 datasources.json 配置创建数据源
```

所有数据源（内置 + 配置文件 + 插件）最终统一注册在 `DataSourceManager` 中，通过名称路由访问，实现了对上层 SQL 引擎完全透明的多数据源管理。
