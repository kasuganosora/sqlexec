# Badger 持久化存储设计

## 概述

为 sqlexec 添加基于 [Badger](https://github.com/dgraph-io/badger) 的持久化存储能力。设计目标：

1. **默认不持久化** - 保持当前内存存储的默认行为
2. **表级控制** - 用户可以为特定表启用/禁用持久化
3. **透明切换** - 对上层 API 透明，无需修改业务代码
4. **ACID 保证** - 利用 Badger 的事务特性保证数据一致性

---

## 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      API Layer (Session)                     │
├─────────────────────────────────────────────────────────────┤
│                   HybridDataSource                           │
│  ┌─────────────────────┐  ┌─────────────────────────────┐   │
│  │   TableConfigManager │  │         Router              │   │
│  │  - 持久化表列表        │  │  - 根据 TableConfig 路由    │   │
│  │  - 默认持久化策略      │  │  - 读写分离/合并           │   │
│  └─────────────────────┘  └─────────────────────────────┘   │
│           │                           │                      │
│           ▼                           ▼                      │
│  ┌─────────────────┐        ┌─────────────────────────┐     │
│  │ MemoryDataSource │        │   BadgerDataSource      │     │
│  │ (现有实现)        │        │   (新增持久化实现)        │     │
│  │ - MVCC          │        │   - 基于 Badger DB       │     │
│  │ - Copy-on-Write │        │   - 事务支持             │     │
│  └─────────────────┘        └─────────────────────────┘     │
│                                        │                      │
│                                        ▼                      │
│                              ┌─────────────────┐             │
│                              │   Badger DB     │             │
│                              │   (磁盘文件)     │             │
│                              └─────────────────┘             │
└─────────────────────────────────────────────────────────────┘
```

### 核心组件

#### 1. HybridDataSource

混合数据源，协调内存和持久化存储：

```go
// HybridDataSource 混合数据源
type HybridDataSource struct {
    config    *domain.DataSourceConfig
    connected bool
    mu        sync.RWMutex
    
    // 子数据源
    memory   *memory.MVCCDataSource  // 内存存储
    badger   *BadgerDataSource       // 持久化存储 (可选)
    
    // 表配置管理
    tableConfig *TableConfigManager
    
    // 路由器
    router *DataSourceRouter
}
```

#### 2. TableConfigManager

管理每个表的持久化配置：

```go
// TableConfig 表级配置
type TableConfig struct {
    TableName      string `json:"table_name"`
    Persistent     bool   `json:"persistent"`       // 是否持久化
    SyncOnWrite    bool   `json:"sync_on_write"`    // 写入时同步到磁盘
    CacheInMemory  bool   `json:"cache_in_memory"`  // 是否同时在内存中缓存
}

// TableConfigManager 表配置管理器
type TableConfigManager struct {
    mu          sync.RWMutex
    configs     map[string]*TableConfig  // table_name -> config
    defaultPersistent bool               // 默认持久化策略 (默认 false)
    
    // 持久化配置本身也需要持久化
    badger      *BadgerDataSource
}
```

#### 3. BadgerDataSource

基于 Badger 的持久化数据源：

```go
// BadgerDataSource Badger 持久化数据源
type BadgerDataSource struct {
    config    *domain.DataSourceConfig
    db        *badger.DB
    connected bool
    mu        sync.RWMutex
    
    // 表元数据缓存
    tables    map[string]*domain.TableInfo
    
    // 索引管理
    indexManager *BadgerIndexManager
    
    // 数据目录
    dataDir  string
}
```

#### 4. DataSourceRouter

数据源路由器，决定操作发往哪个数据源：

```go
// DataSourceRouter 数据源路由器
type DataSourceRouter struct {
    memory      *memory.MVCCDataSource
    badger      *BadgerDataSource
    tableConfig *TableConfigManager
}

// RouteDecision 路由决策
type RouteDecision int

const (
    RouteMemoryOnly RouteDecision = iota  // 仅内存
    RouteBadgerOnly                       // 仅持久化
    RouteBoth                             // 双写/双读
)

// Decide 根据表名和操作类型决定路由
func (r *DataSourceRouter) Decide(tableName string, op OperationType) RouteDecision
```

---

## Key-Value 映射设计

### Key 前缀规范

```
前缀          用途                          示例
────────────────────────────────────────────────────────
meta:         元数据                        meta:config
table:        表结构信息                     table:users
row:          行数据                         row:users:000001
idx:          索引数据                       idx:users:email:test@example.com
seq:          序列号 (自增ID)                 seq:users:id
txn:          事务状态                       txn:12345
config:       表配置                         config:users
```

### 行数据 Key 设计

```
row:{table_name}:{primary_key}

示例:
row:users:user_001
row:orders:order_12345
```

### 索引 Key 设计

```
idx:{table_name}:{column_name}:{value}

示例:
idx:users:email:test@example.com -> ["user_001"]
idx:users:status:active -> ["user_001", "user_002", "user_003"]
idx:orders:user_id:user_001 -> ["order_12345", "order_12346"]
```

### 序列号 Key 设计

```
seq:{table_name}:{column_name}

示例:
seq:users:id -> 1001 (下一个自增ID)
```

---

## API 设计

### 1. 创建带持久化的数据源

```go
// 创建混合数据源
func NewHybridDataSource(config *HybridDataSourceConfig) (*HybridDataSource, error) {
    // 配置示例
    config := &HybridDataSourceConfig{
        DataDir:           "./data",           // 持久化数据目录
        DefaultPersistent: false,              // 默认不持久化
        EnableBadger:      true,               // 启用 Badger
        BadgerOptions:     badger.DefaultOptions("./data"),
    }
}
```

### 2. 表级持久化控制

```go
// 为特定表启用持久化
func (ds *HybridDataSource) EnablePersistence(ctx context.Context, tableName string, opts ...PersistenceOption) error

// 为特定表禁用持久化 (数据保留在内存中)
func (ds *HybridDataSource) DisablePersistence(ctx context.Context, tableName string) error

// 获取表的持久化状态
func (ds *HybridDataSource) GetPersistenceConfig(tableName string) (*TableConfig, bool)

// 列出所有持久化表
func (ds *HybridDataSource) ListPersistentTables() []string
```

### 3. 数据迁移

```go
// 将内存表迁移到持久化存储
func (ds *HybridDataSource) MigrateToPersistent(ctx context.Context, tableName string) error

// 将持久化表加载到内存
func (ds *HybridDataSource) LoadToMemory(ctx context.Context, tableName string) error

// 同步内存和持久化数据
func (ds *HybridDataSource) SyncTable(ctx context.Context, tableName string) error
```

---

## 配置选项

### HybridDataSourceConfig

```go
type HybridDataSourceConfig struct {
    // 数据目录
    DataDir string `json:"data_dir"`
    
    // 默认持久化策略 (默认 false)
    DefaultPersistent bool `json:"default_persistent"`
    
    // 是否启用 Badger (默认 true)
    EnableBadger bool `json:"enable_badger"`
    
    // Badger 配置
    BadgerOptions badger.Options `json:"-"`
    
    // 内存缓存配置
    CacheConfig *CacheConfig `json:"cache_config"`
}

type CacheConfig struct {
    // 是否缓存持久化表到内存
    Enabled bool `json:"enabled"`
    
    // 最大缓存大小 (MB)
    MaxSizeMB int `json:"max_size_mb"`
    
    // 缓存淘汰策略
    EvictionPolicy string `json:"eviction_policy"` // "lru", "lfu"
}
```

### PersistenceOption

```go
type PersistenceOption func(*TableConfig)

// 写入时同步到磁盘
func WithSyncOnWrite(sync bool) PersistenceOption

// 同时在内存中缓存
func WithCacheInMemory(cache bool) PersistenceOption
```

---

## 数据流

### 写入流程

```
┌──────────────────────────────────────────────────────────┐
│                      INSERT/UPDATE                        │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
               ┌─────────────────────┐
               │  TableConfigManager │
               │  查询表配置          │
               └─────────┬───────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
          ▼                             ▼
   ┌─────────────┐              ┌─────────────┐
   │ 内存表      │              │ 持久化表     │
   └──────┬──────┘              └──────┬──────┘
          │                             │
          ▼                             ▼
   ┌─────────────┐              ┌─────────────┐
   │ MemoryDS    │              │ BadgerDS    │
   │ 写入内存     │              │ 写入磁盘     │
   └─────────────┘              │ (事务保证)   │
                                └─────────────┘
```

### 读取流程

```
┌──────────────────────────────────────────────────────────┐
│                        SELECT                             │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
               ┌─────────────────────┐
               │  TableConfigManager │
               │  查询表配置          │
               └─────────┬───────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
          ▼                             ▼
   ┌─────────────┐              ┌─────────────┐
   │ 内存表      │              │ 持久化表     │
   └──────┬──────┘              └──────┬──────┘
          │                             │
          ▼                             ▼
   ┌─────────────┐              ┌─────────────┐
   │ MemoryDS    │              │ BadgerDS    │
   │ 从内存读取   │              │ 从磁盘读取   │
   └─────────────┘              └─────────────┘
```

---

## 实现计划

### Phase 1: 基础框架 (Week 1)

1. 创建 `pkg/resource/badger/` 目录结构
2. 实现 `BadgerDataSource` 基础接口
3. 实现 Key-Value 映射和序列化
4. 单元测试

### Phase 2: 表配置管理 (Week 1-2)

1. 实现 `TableConfigManager`
2. 实现 `HybridDataSource` 框架
3. 实现 `DataSourceRouter`
4. 集成测试

### Phase 3: 完整 CRUD (Week 2-3)

1. 完整实现 Query/Insert/Update/Delete
2. 索引支持
3. 事务支持
4. 性能测试

### Phase 4: 高级特性 (Week 3-4)

1. 数据迁移工具
2. 备份/恢复
3. 压缩和垃圾回收
4. 文档完善

---

## 文件结构

```
pkg/resource/
├── domain/              # 现有接口定义
├── memory/              # 现有内存实现
├── badger/              # 新增: Badger 持久化实现
│   ├── datasource.go    # BadgerDataSource 实现
│   ├── types.go         # 类型定义
│   ├── key_encoding.go  # Key 编码/解码
│   ├── row_codec.go     # 行数据序列化
│   ├── index.go         # 索引管理
│   ├── transaction.go   # 事务支持
│   └── datasource_test.go
├── hybrid/              # 新增: 混合数据源
│   ├── datasource.go    # HybridDataSource 实现
│   ├── table_config.go  # 表配置管理
│   ├── router.go        # 数据源路由
│   └── datasource_test.go
└── integration_test.go  # 集成测试
```

---

## 注意事项

### 1. 性能考虑

- Badger 适合写多读少场景，对于热点读数据考虑启用内存缓存
- 批量写入使用 Badger 的事务批处理
- 索引更新采用异步策略

### 2. 兼容性

- 保持现有 `MemoryDataSource` API 不变
- `HybridDataSource` 实现相同的 `TransactionalDataSource` 接口
- 向后兼容现有配置

### 3. 错误处理

- 持久化失败时回滚内存操作
- 提供详细错误日志
- 支持手动恢复

### 4. 资源管理

- 合理设置 Badger 的内存限制
- 定期执行压缩和垃圾回收
- 监控磁盘空间使用
