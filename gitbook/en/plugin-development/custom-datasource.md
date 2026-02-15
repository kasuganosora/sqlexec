# Custom Data Source

By implementing the `DataSource` and `DataSourceFactory` interfaces, you can connect any data backend to SQLExec.

## Overview

A custom data source requires implementing two interfaces:

1. **DataSource** -- The data source itself, providing connection, query, write, and other capabilities
2. **DataSourceFactory** -- The factory, responsible for creating data source instances from configuration

## Step 1: Define the Type Constant

Add the following in `pkg/resource/domain/models.go`:

```go
const DataSourceTypeRedis DataSourceType = "redis"
```

## Step 2: Implement the DataSource Interface

```go
package redis

import (
    "context"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

type RedisDataSource struct {
    config    *domain.DataSourceConfig
    connected bool
    // ... your connection client
}

// Lifecycle
func (ds *RedisDataSource) Connect(ctx context.Context) error {
    // Connect to Redis
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

// Metadata
func (ds *RedisDataSource) GetTables(ctx context.Context) ([]string, error) {
    // Return the list of queryable "tables"
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

// Query
func (ds *RedisDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
    // Convert filters in QueryOptions to Redis operations
    // Return query results
    return &domain.QueryResult{
        Columns: []domain.ColumnInfo{...},
        Rows:    []domain.Row{...},
        Total:   0,
    }, nil
}

// Write operations
func (ds *RedisDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
    // Implement insert logic
    return int64(len(rows)), nil
}

func (ds *RedisDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
    return 0, nil
}

func (ds *RedisDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
    return 0, nil
}

// DDL (typically not supported)
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

## Step 3: Implement the Factory

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

## Step 4: Register

Register in the `init()` function of `pkg/resource/registry.go`:

```go
import "github.com/kasuganosora/sqlexec/pkg/resource/redis"

func init() {
    registry := application.GetRegistry()
    // ... other registrations
    registry.Register(redis.NewRedisFactory())
}
```

## Usage

Once registered, it can be used via configuration or code:

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

## Shortcut: Embedding MVCCDataSource

For file-based data sources, you can embed `memory.MVCCDataSource` and only implement the load and write-back logic:

```go
type MyFileAdapter struct {
    *memory.MVCCDataSource
    filePath string
}

func (a *MyFileAdapter) Connect(ctx context.Context) error {
    // 1. Read the file
    // 2. Parse into []domain.Row
    // 3. Call a.LoadTable("my_data", tableInfo, rows)
    // 4. Call a.MVCCDataSource.Connect(ctx)
    return nil
}
```

This approach automatically provides full SQL query capability, MVCC transactions, and index support without implementing Query/Insert/Update/Delete yourself. The CSV, JSON, JSONL, Excel, and Parquet data sources all use this pattern.
