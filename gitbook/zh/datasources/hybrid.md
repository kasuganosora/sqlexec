# Hybrid 混合数据源

Hybrid 混合数据源将 Memory 内存引擎与 Badger KV 持久化存储相结合，实现**按表配置持久化策略**。默认所有表运行在内存中（高性能），需要持久化的表可以单独配置写入磁盘。

适用于既需要高性能内存查询，又需要部分数据持久化的嵌入式应用场景。

## 基本配置

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 数据源名称 |
| `type` | string | 是 | 固定值 `hybrid` |
| `writable` | bool | 否 | 默认 `true` |

## 混合存储选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `data_dir` | `""` | Badger 持久化数据目录 |
| `default_persistent` | `false` | 新表是否默认持久化 |
| `enable_badger` | `true` | 是否启用 Badger 后端 |

### 缓存配置

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `cache.enabled` | `true` | 是否为持久化表启用内存缓存 |
| `cache.max_size_mb` | `256` | 缓存最大内存（MB） |
| `cache.eviction_policy` | `lru` | 淘汰策略：`lru`、`lfu` |

## 配置示例

### 嵌入模式

```go
package main

import (
    "context"
    "fmt"

    "github.com/kasuganosora/sqlexec/pkg/api"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/hybrid"
)

func main() {
    // 创建 Hybrid 数据源
    ds := hybrid.NewHybridDataSource(
        &domain.DataSourceConfig{
            Type:     "hybrid",
            Name:     "mydb",
            Writable: true,
        },
        &hybrid.HybridDataSourceConfig{
            DataDir:           "./data",
            DefaultPersistent: false,   // 新表默认不持久化
            EnableBadger:      true,
        },
    )

    ctx := context.Background()
    ds.Connect(ctx)
    defer ds.Close(ctx)

    // 注册到 DB
    db, _ := api.NewDB(nil)
    db.RegisterDataSource("mydb", ds)

    session := db.Session()
    defer session.Close()
    session.Execute("USE mydb")

    // 创建普通内存表（不持久化）
    session.Execute("CREATE TABLE cache_data (id INT, value TEXT)")

    // 对特定表启用持久化
    ds.EnablePersistence(ctx, "important_data",
        hybrid.WithSyncOnWrite(true),    // 每次写入同步磁盘
        hybrid.WithCacheInMemory(true),  // 同时保留内存缓存
    )

    fmt.Println("Hybrid 数据源已就绪")
}
```

## 工作原理

### 路由架构

```
SQL 操作
    ↓
HybridDataSource（路由器）
    ↓
┌─────────────────┬──────────────────┐
│  内存表          │  持久化表         │
│  Memory Engine  │  Badger Backend  │
│  (默认)         │  (按表配置)       │
└─────────────────┴──────────────────┘
```

- **路由器**根据每张表的持久化配置决定操作发往哪个后端。
- 默认所有表使用 Memory 引擎（快速查询、MVCC 事务）。
- 通过 `EnablePersistence()` 可将指定表切换到 Badger 后端。
- 支持**双写模式**：同时写入内存和磁盘，读取走内存（最高性能 + 持久化保障）。

### 读写路由规则

| 表配置 | 读操作 | 写操作 |
|--------|--------|--------|
| 纯内存（默认） | Memory | Memory |
| 持久化 | Badger | Badger |
| 双写（CacheInMemory） | Memory（优先） | Memory + Badger |

## 持久化控制

### 启用持久化

```go
// 基本启用
ds.EnablePersistence(ctx, "orders")

// 带选项启用
ds.EnablePersistence(ctx, "orders",
    hybrid.WithSyncOnWrite(true),    // 同步写入磁盘
    hybrid.WithCacheInMemory(true),  // 保留内存缓存
)
```

### 禁用持久化

```go
// 禁用后数据保留在内存中
ds.DisablePersistence(ctx, "orders")
```

### 查询持久化状态

```go
config, _ := ds.GetPersistenceConfig("orders")
if config.Persistent {
    fmt.Printf("表 %s 已持久化\n", config.TableName)
}

// 列出所有持久化表
tables := ds.ListPersistentTables()
```

## 数据迁移

### 内存迁移到磁盘

将已有的内存表迁移到 Badger 持久化存储：

```go
// 迁移过程：
// 1. 读取内存表的 schema 和数据
// 2. 在 Badger 中创建表并写入数据
// 3. 更新路由配置
// 4. 从内存中删除原表
err := ds.MigrateToPersistent(ctx, "orders")
```

### 磁盘加载到内存

将持久化表加载到内存以提高查询性能：

```go
// 加载过程：
// 1. 读取 Badger 中的 schema 和数据
// 2. 在内存中创建表并写入数据
// 3. 更新路由配置为纯内存
err := ds.LoadToMemory(ctx, "orders")
```

## 事务支持

Hybrid 数据源支持事务，事务协调 Memory 和 Badger 两个后端：

```go
txn, err := ds.BeginTransaction(ctx, nil)
if err != nil {
    panic(err)
}

// 在事务中执行操作
txn.Insert(ctx, "orders", rows, nil)
txn.Update(ctx, "orders", filters, updates, nil)

// 提交（先提交 Memory，再同步 Badger）
err = txn.Commit(ctx)
// 或回滚
// err = txn.Rollback(ctx)
```

## 统计信息

```go
stats := ds.Stats()
fmt.Printf("总读取: %d (内存: %d, Badger: %d)\n",
    stats.TotalReads, stats.MemoryReads, stats.BadgerReads)
fmt.Printf("总写入: %d (内存: %d, Badger: %d)\n",
    stats.TotalWrites, stats.MemoryWrites, stats.BadgerWrites)
```

## 与其他持久化方案的对比

| 特性 | Hybrid | XML 持久化 | 纯 Badger |
|------|--------|-----------|----------|
| 持久化粒度 | 按表配置 | 按表（ENGINE=xml） | 全部持久化 |
| 内存查询 | 支持（路由到 Memory） | 支持（内存引擎） | 不支持 |
| 高级 SQL | 完整支持 | 完整支持 | 基础操作 |
| MVCC 事务 | 支持 | 支持 | 基础事务 |
| 配置方式 | Go API | SQL (ENGINE=xml) | Go API |
| 运行时切换 | 支持（Enable/Disable） | 不支持 | 不支持 |
| 数据迁移 | 内置（Migrate/Load） | 不支持 | 不支持 |
| 适用场景 | 嵌入式应用 | 独立服务器/嵌入式 | 纯持久化需求 |

## 注意事项

- Hybrid 数据源目前仅支持嵌入模式（Go API），不支持通过 `datasources.json` 配置。
- `EnablePersistence` 和 `DisablePersistence` 仅更改路由配置，不自动迁移数据。如需迁移数据，请使用 `MigrateToPersistent` 或 `LoadToMemory`。
- 双写模式下，如果 Badger 写入失败，Memory 写入不会回滚（最终一致性）。
- 持久化表的配置本身也存储在 Badger 中，重启后自动恢复。
- 内存表支持所有 SQL 功能（MVCC、索引、向量搜索等），持久化表仅支持基础 CRUD。
