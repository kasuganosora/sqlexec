# 自定义数据源

通过实现 `DataSource` 和 `DataSourceFactory` 接口，可以将任意数据后端接入 SQLExec。

## 概述

自定义数据源需要实现两个接口：

1. **DataSource** — 数据源本身，提供连接、查询、写入等能力
2. **DataSourceFactory** — 工厂，负责根据配置创建数据源实例

## 步骤一：定义类型常量

在 `pkg/resource/domain/models.go` 中新增：

```go
const DataSourceTypeRedis DataSourceType = "redis"
```

## 步骤二：实现 DataSource 接口

```go
package redis

import (
    "context"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

type RedisDataSource struct {
    config    *domain.DataSourceConfig
    connected bool
    // ... 你的连接客户端
}

// 生命周期
func (ds *RedisDataSource) Connect(ctx context.Context) error {
    // 连接到 Redis
    ds.connected = true
    return nil
}

func (ds *RedisDataSource) Close(ctx context.Context) error {
    ds.connected = false
    return nil
}

func (ds *RedisDataSource) IsConnected() bool {
    return ds.connected
}

func (ds *RedisDataSource) IsWritable() bool {
    return ds.config.Writable
}

func (ds *RedisDataSource) GetConfig() *domain.DataSourceConfig {
    return ds.config
}

// 元数据
func (ds *RedisDataSource) GetTables(ctx context.Context) ([]string, error) {
    // 返回可查询的"表"列表
    return []string{"redis_data"}, nil
}

func (ds *RedisDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
    return &domain.TableInfo{
        Name: tableName,
        Columns: []domain.ColumnInfo{
            {Name: "key", Type: "string"},
            {Name: "value", Type: "string"},
            {Name: "ttl", Type: "int64"},
        },
    }, nil
}

// 查询
func (ds *RedisDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
    // 将 QueryOptions 中的 filters 转换为 Redis 操作
    // 返回查询结果
    return &domain.QueryResult{
        Columns: []domain.ColumnInfo{...},
        Rows:    []domain.Row{...},
        Total:   0,
    }, nil
}

// 写入操作
func (ds *RedisDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
    // 实现插入逻辑
    return int64(len(rows)), nil
}

func (ds *RedisDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
    return 0, nil
}

func (ds *RedisDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
    return 0, nil
}

// DDL（通常不支持）
func (ds *RedisDataSource) CreateTable(ctx context.Context, info *domain.TableInfo) error {
    return domain.NewErrUnsupportedOperation("redis", "create table")
}

func (ds *RedisDataSource) DropTable(ctx context.Context, name string) error {
    return domain.NewErrUnsupportedOperation("redis", "drop table")
}

func (ds *RedisDataSource) TruncateTable(ctx context.Context, name string) error {
    return domain.NewErrUnsupportedOperation("redis", "truncate table")
}

func (ds *RedisDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
    return nil, domain.NewErrUnsupportedOperation("redis", "execute SQL")
}
```

## 步骤三：实现工厂

```go
type RedisFactory struct{}

func NewRedisFactory() *RedisFactory {
    return &RedisFactory{}
}

func (f *RedisFactory) GetType() domain.DataSourceType {
    return domain.DataSourceTypeRedis
}

func (f *RedisFactory) Create(config *domain.DataSourceConfig) (domain.DataSource, error) {
    if config == nil {
        return nil, fmt.Errorf("redis factory: config cannot be nil")
    }
    return &RedisDataSource{config: config}, nil
}
```

## 步骤四：注册

在 `pkg/resource/registry.go` 的 `init()` 中注册：

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/redis"

func init() {
    registry := application.GetRegistry()
    // ... 其他注册
    registry.Register(redis.NewRedisFactory())
}
```

## 使用

注册后即可通过配置或代码使用：

```json
{
  "type": "redis",
  "name": "cache",
  "host": "127.0.0.1",
  "port": 6379,
  "writable": true
}
```

```sql
USE cache;
SELECT * FROM redis_data WHERE key LIKE 'user:*';
```

## 快捷方式：嵌入 MVCCDataSource

对于文件型数据源，可以嵌入 `memory.MVCCDataSource`，只需实现加载和写回逻辑：

```go
type MyFileAdapter struct {
    *memory.MVCCDataSource
    filePath string
}

func (a *MyFileAdapter) Connect(ctx context.Context) error {
    // 1. 读取文件
    // 2. 解析为 []domain.Row
    // 3. 调用 a.LoadTable("my_data", tableInfo, rows)
    // 4. 调用 a.MVCCDataSource.Connect(ctx)
    return nil
}
```

这种方式自动获得完整的 SQL 查询能力、MVCC 事务、索引支持，无需自己实现 Query/Insert/Update/Delete。CSV、JSON、JSONL、Excel、Parquet 数据源都采用这种模式。
