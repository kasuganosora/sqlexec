# Badger KV 持久化存储

Badger 数据源基于 [Badger](https://github.com/dgraph-io/badger) 嵌入式 KV 数据库实现，提供磁盘持久化的数据存储。数据以 LSM-Tree 结构存储在本地磁盘，进程重启后数据不会丢失。

Badger 数据源通常不需要直接使用，而是作为 [Hybrid 混合数据源](hybrid.md) 的后端存储自动管理。如需直接使用，适用于需要纯持久化、无内存缓存的嵌入式场景。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称，作为数据库标识符（`USE <name>` 切换） |
| `type` | string | 是 | 固定值 `badger` |
| `writable` | bool | 否 | 始终支持读写，默认 `true` |

## 存储选项

通过 `options` 字段可以配置存储参数：

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `data_dir` | `""` | 数据文件存储目录，为空时使用内存模式 |
| `in_memory` | `false` | 是否使用纯内存模式（不写入磁盘） |
| `sync_writes` | `false` | 是否每次写入都同步到磁盘（开启后更安全但更慢） |
| `value_threshold` | `1024` | 超过此大小（字节）的值存入 Value Log |
| `num_memtables` | `5` | 内存表数量 |
| `base_table_size` | `2097152` | LSM 基础表大小（2MB） |
| `compression` | `1` | 压缩算法：`0`=无，`1`=Snappy，`2`=ZSTD |

## 配置示例

### 嵌入模式

```go
package main

import (
    "context"
    "fmt"

    "github.com/kasuganosora/sqlexec/pkg/resource/badger"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func main() {
    // 创建 Badger 数据源
    ds := badger.NewBadgerDataSource(&domain.DataSourceConfig{
        Type:     "badger",
        Name:     "persistent",
        Writable: true,
        Options: map[string]interface{}{
            "data_dir": "./badger_data",
        },
    })

    ctx := context.Background()
    ds.Connect(ctx)
    defer ds.Close(ctx)

    // 创建表
    ds.CreateTable(ctx, &domain.TableInfo{
        Name: "users",
        Columns: []domain.ColumnInfo{
            {Name: "id", Type: "INT", Primary: true, AutoIncrement: true},
            {Name: "name", Type: "VARCHAR", Nullable: false},
            {Name: "email", Type: "VARCHAR", Nullable: true},
        },
    })

    // 插入数据
    ds.Insert(ctx, "users", []domain.Row{
        {"name": "张三", "email": "zhangsan@example.com"},
        {"name": "李四", "email": "lisi@example.com"},
    }, nil)

    // 查询数据
    result, _ := ds.Query(ctx, "users", &domain.QueryOptions{})
    fmt.Printf("共 %d 条记录\n", result.Total)
}
```

### 自定义存储配置

```go
cfg := badger.DefaultDataSourceConfig("./data")
cfg.SyncWrites = true    // 每次写入同步磁盘
cfg.Compression = 2      // 使用 ZSTD 压缩

ds := badger.NewBadgerDataSourceWithConfig(domainCfg, cfg)
```

## 功能特性

### DDL 操作

支持 `CreateTable`、`DropTable`、`TruncateTable`：

- 建表时自动为 `AUTO_INCREMENT` 列初始化序列号。
- `TRUNCATE` 会清除所有行和索引，并重置自增序列。
- `DROP TABLE` 会删除表元数据、所有行数据和索引。

### DML 操作

支持 `Insert`、`Query`、`Update`、`Delete`：

- 查询支持过滤条件（`=`、`!=`、`>`、`>=`、`<`、`<=`、`LIKE`、`IN`）。
- 查询支持 `ORDER BY` 排序和 `LIMIT` / `OFFSET` 分页。
- 支持 `AND` / `OR` 组合过滤条件。
- 自增主键在插入时自动生成。

### 索引

- 自动为 `PRIMARY KEY` 和 `UNIQUE` 列维护索引。
- 索引与行数据一同持久化在 Badger 中。

### 数据持久化

- 数据以 KV 形式存储在 Badger LSM-Tree 中。
- 支持 Snappy 和 ZSTD 压缩。
- 支持数据加密（通过 `EncryptionKey` 配置）。
- 进程重启后自动加载表元数据和数据。

## 与 Memory 数据源的对比

| 特性 | Memory | Badger |
|------|--------|--------|
| 持久化 | 否（进程退出丢失） | 是（磁盘存储） |
| 查询性能 | 极快（纯内存） | 快（LSM + 缓存） |
| MVCC 事务 | 完整支持 | 基础事务 |
| 索引类型 | B-Tree、Hash、Fulltext、Vector | Primary、Unique |
| 全文搜索 | 支持 | 不支持 |
| 向量搜索 | 支持 | 不支持 |
| SQL 执行 | 完整 SQL | 通过上层路由 |
| 适用场景 | 临时数据、高性能查询 | 需要持久化的数据 |

## 注意事项

- Badger 数据源不支持直接执行原始 SQL（`Execute` 方法），查询需通过 `Query` 等接口方法调用。
- 如需同时享受内存查询性能和磁盘持久化，推荐使用 [Hybrid 混合数据源](hybrid.md)。
- `data_dir` 为空时自动进入内存模式，行为类似 Memory 但不支持 MVCC 和高级索引。
- 生产环境建议开启 `sync_writes` 以确保数据安全。
- 大量数据写入后建议定期执行 Badger GC（通过维护 API）。
