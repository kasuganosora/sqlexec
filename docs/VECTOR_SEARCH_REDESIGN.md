# SQLExec 向量搜索独立层设计

## 设计原则

1. **存储层无关**: 向量索引不入侵存储层，CSV/JSON/API 等存储保持只读
2. **按需构建**: 从任何 DataSource 读取数据构建内存/磁盘索引
3. **独立管理**: 向量索引有自己的生命周期管理
4. **统一接口**: 无论底层是 CSV、MySQL 还是 API，向量搜索接口一致

---

## 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      SQL Layer (Parser)                      │
│         CREATE VECTOR INDEX / ORDER BY VEC_...              │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   Optimizer Layer                            │
│  VectorIndexAdvisor ──→ VectorScan Plan ──→ Cost Estimation │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  Vector Search Layer                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         Vector Index Manager (pkg/vector/)          │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  │   │
│  │  │ HNSW Index   │  │ IVF Index    │  │ Flat     │  │   │
│  │  │ (in-memory)  │  │ (in-memory)  │  │ (brute)  │  │   │
│  │  └──────────────┘  └──────────────┘  └──────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
│                          │                                  │
│  ┌───────────────────────▼────────────────────────────┐    │
│  │       Vector Index Builder (增量/全量构建)          │    │
│  │  - 从 DataSource 读取数据                          │    │
│  │  - 异步构建索引                                    │    │
│  │  - 增量更新支持                                    │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────┬───────────────────────────────────┘
                          │ 读取原始数据
┌─────────────────────────▼───────────────────────────────────┐
│                   Data Access Layer                          │
│              DataService / DataSourceManager                 │
│     CSV    JSON    MySQL    API    Memory    Parquet        │
└─────────────────────────────────────────────────────────────┘
```

---

## 1. 向量搜索层 (pkg/vector/)

这是一个全新的独立层，专门处理向量索引和搜索。

### 1.1 核心接口

**文件**: `pkg/vector/interfaces.go`

```go
package vector

import (
    "context"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Index 向量索引接口
type Index interface {
    // Search 近似最近邻搜索
    // query: 查询向量 []float32 或 SparseVector
    // k: 返回最相似的 k 个结果
    // filter: 可选的预过滤条件（行ID过滤）
    // 返回: 搜索结果列表
    Search(ctx context.Context, query interface{}, k int, filter *SearchFilter) (*SearchResult, error)
    
    // GetStats 获取索引统计信息
    GetStats() IndexStats
    
    // Close 关闭索引，释放资源
    Close() error
}

// SearchResult 搜索结果
type SearchResult struct {
    IDs         []int64      // 行ID/文档ID
    Distances   []float32    // 距离值
    Vectors     [][]float32  // 可选：返回原始向量
    TotalCount  int          // 搜索结果总数
}

// SearchFilter 搜索过滤器
type SearchFilter struct {
    // 行ID白名单（为空时不过滤）
    RowIDs []int64
    // 元数据过滤条件（可选）
    MetadataFilter map[string]interface{}
}

// IndexStats 索引统计
type IndexStats struct {
    Type           IndexType
    MetricType     MetricType
    Dimension      int
    TotalVectors   int64
    IndexSize      int64    // 索引占用的字节数
    IndexStatus    IndexStatus
    LastUpdated    int64    // Unix timestamp
}

// IndexStatus 索引状态
type IndexStatus int

const (
    IndexStatusUninitialized IndexStatus = iota
    IndexStatusBuilding
    IndexStatusReady
    IndexStatusFailed
    IndexStatusOutdated  // 数据已变更，需要更新
)

// IndexType 索引类型
type IndexType string

const (
    IndexHNSW    IndexType = "hnsw"
    IndexIVFFlat IndexType = "ivf_flat"
    IndexIVFPQ   IndexType = "ivf_pq"
    IndexIVFSQ8  IndexType = "ivf_sq8"
    IndexFlat    IndexType = "flat"     // 暴力搜索
    IndexDiskANN IndexType = "diskann"  // 磁盘索引（大规模数据）
)

// MetricType 距离度量类型
type MetricType string

const (
    MetricCosine  MetricType = "cosine"
    MetricL2      MetricType = "l2"
    MetricIP      MetricType = "ip"          // Inner Product
    MetricHamming MetricType = "hamming"     // 二值向量
    MetricJaccard MetricType = "jaccard"     // 二值向量
)

// IndexConfig 索引配置
type IndexConfig struct {
    Type       IndexType
    MetricType MetricType
    Dimension  int
    
    // HNSW 参数
    HNSW *HNSWParams
    
    // IVF 参数
    IVF *IVFParams
    
    // 磁盘索引参数
    Disk *DiskParams
}

// HNSWParams HNSW 参数
type HNSWParams struct {
    M              int     // 图中每个节点的最大邻居数，默认 16
    EfConstruction int     // 构建时的探索因子，默认 200
    EfSearch       int     // 搜索时的探索因子，默认 64
}

// IVFParams IVF 参数
type IVFParams struct {
    NList  int     // 聚类中心数，默认 100
    NProbe int     // 搜索时探测的聚类数，默认 10
    // PQ 参数
    M      int     // 子向量数
    NBits  int     // 每个子向量的编码位数
}

// DiskParams 磁盘索引参数
type DiskParams struct {
    StoragePath string  // 索引存储路径
    MaxMemory   int64   // 最大内存使用
}
```

### 1.2 向量索引管理器

**文件**: `pkg/vector/manager.go`

```go
package vector

import (
    "context"
    "fmt"
    "sync"
    
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IndexManager 向量索引管理器
// 独立管理所有向量索引，与存储层解耦
type IndexManager struct {
    // 索引存储
    indexes map[IndexKey]*ManagedIndex
    
    // 索引构建器
    builder *IndexBuilder
    
    // 数据源访问接口
    dataSourceAccessor DataSourceAccessor
    
    // 配置
    config *ManagerConfig
    
    mu sync.RWMutex
}

// IndexKey 索引唯一标识
type IndexKey struct {
    DataSourceName string  // 数据源名称
    TableName      string  // 表名
    ColumnName     string  // 向量列名
}

func (k IndexKey) String() string {
    return fmt.Sprintf("%s.%s.%s", k.DataSourceName, k.TableName, k.ColumnName)
}

// ManagedIndex 被管理的索引
type ManagedIndex struct {
    Key        IndexKey
    Index      Index
    Config     *IndexConfig
    Status     IndexStatus
    Progress   float64       // 构建进度 0-100
    Error      error         // 最后错误
    CreatedAt  int64
    UpdatedAt  int64
    
    // 数据源信息（用于增量更新）
    DataSourceVersion string  // 数据版本（如文件修改时间、数据条数等）
}

// ManagerConfig 管理器配置
type ManagerConfig struct {
    // 默认索引类型
    DefaultIndexType IndexType
    
    // 最大内存使用（字节）
    MaxMemoryUsage int64
    
    // 自动构建阈值（数据量小于此值时自动构建）
    AutoBuildThreshold int64
    
    // 索引存储路径（用于磁盘索引）
    StoragePath string
    
    // 是否启用异步构建
    AsyncBuild bool
}

// DataSourceAccessor 数据源访问接口
// 通过这个接口从任何 DataSource 读取数据，不依赖具体存储实现
type DataSourceAccessor interface {
    // ReadVectorData 读取向量数据
    // 从指定数据源读取向量列的数据
    ReadVectorData(
        ctx context.Context,
        dataSourceName string,
        tableName string,
        columnName string,
        rowIDColumn string,  // 行ID列名（如 "id" 或 "__rowid__"）
    ) (*VectorData, error)
    
    // GetDataVersion 获取数据版本（用于判断是否需要重建索引）
    GetDataVersion(
        ctx context.Context,
        dataSourceName string,
        tableName string,
    ) (string, error)
    
    // GetRowCount 获取数据行数
    GetRowCount(
        ctx context.Context,
        dataSourceName string,
        tableName string,
    ) (int64, error)
}

// VectorData 向量数据集
type VectorData struct {
    RowIDs   []int64       // 行ID列表
    Vectors  [][]float32   // 向量列表（与RowIDs一一对应）
    Metadata []map[string]interface{} // 可选元数据
}

// NewIndexManager 创建索引管理器
func NewIndexManager(
    accessor DataSourceAccessor,
    config *ManagerConfig,
) *IndexManager {
    if config == nil {
        config = DefaultManagerConfig()
    }
    
    return &IndexManager{
        indexes:            make(map[IndexKey]*ManagedIndex),
        builder:            NewIndexBuilder(),
        dataSourceAccessor: accessor,
        config:             config,
    }
}

// DefaultManagerConfig 默认配置
func DefaultManagerConfig() *ManagerConfig {
    return &ManagerConfig{
        DefaultIndexType:   IndexHNSW,
        MaxMemoryUsage:     1024 * 1024 * 1024,  // 1GB
        AutoBuildThreshold: 100000,
        StoragePath:        "./vector_indexes",
        AsyncBuild:         true,
    }
}

// CreateIndex 创建向量索引
func (m *IndexManager) CreateIndex(
    ctx context.Context,
    key IndexKey,
    config *IndexConfig,
    rowIDColumn string,
) (*ManagedIndex, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // 检查是否已存在
    if existing, ok := m.indexes[key]; ok {
        return existing, fmt.Errorf("index already exists: %s", key)
    }
    
    // 创建管理对象
    managed := &ManagedIndex{
        Key:       key,
        Config:    config,
        Status:    IndexStatusUninitialized,
        CreatedAt: time.Now().Unix(),
    }
    
    m.indexes[key] = managed
    
    // 构建索引
    if m.config.AsyncBuild {
        go m.buildIndexAsync(managed, rowIDColumn)
    } else {
        err := m.buildIndexSync(ctx, managed, rowIDColumn)
        if err != nil {
            managed.Status = IndexStatusFailed
            managed.Error = err
            return nil, err
        }
    }
    
    return managed, nil
}

// buildIndexSync 同步构建索引
func (m *IndexManager) buildIndexSync(
    ctx context.Context,
    managed *ManagedIndex,
    rowIDColumn string,
) error {
    managed.Status = IndexStatusBuilding
    
    // 1. 从数据源读取数据
    vectorData, err := m.dataSourceAccessor.ReadVectorData(
        ctx,
        managed.Key.DataSourceName,
        managed.Key.TableName,
        managed.Key.ColumnName,
        rowIDColumn,
    )
    if err != nil {
        return fmt.Errorf("read vector data failed: %w", err)
    }
    
    // 2. 获取数据版本
    version, err := m.dataSourceAccessor.GetDataVersion(
        ctx,
        managed.Key.DataSourceName,
        managed.Key.TableName,
    )
    if err != nil {
        version = fmt.Sprintf("%d", time.Now().Unix())
    }
    managed.DataSourceVersion = version
    
    // 3. 构建索引
    index, err := m.builder.Build(ctx, vectorData, managed.Config)
    if err != nil {
        return fmt.Errorf("build index failed: %w", err)
    }
    
    managed.Index = index
    managed.Status = IndexStatusReady
    managed.Progress = 100
    managed.UpdatedAt = time.Now().Unix()
    
    return nil
}

// GetIndex 获取索引
func (m *IndexManager) GetIndex(key IndexKey) (Index, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    managed, ok := m.indexes[key]
    if !ok {
        return nil, fmt.Errorf("index not found: %s", key)
    }
    
    if managed.Status != IndexStatusReady {
        return nil, fmt.Errorf("index not ready: %s, status=%d", key, managed.Status)
    }
    
    return managed.Index, nil
}

// Search 执行搜索
func (m *IndexManager) Search(
    ctx context.Context,
    key IndexKey,
    query []float32,
    k int,
    filter *SearchFilter,
) (*SearchResult, error) {
    index, err := m.GetIndex(key)
    if err != nil {
        return nil, err
    }
    
    return index.Search(ctx, query, k, filter)
}

// DropIndex 删除索引
func (m *IndexManager) DropIndex(key IndexKey) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    managed, ok := m.indexes[key]
    if !ok {
        return fmt.Errorf("index not found: %s", key)
    }
    
    if managed.Index != nil {
        managed.Index.Close()
    }
    
    delete(m.indexes, key)
    return nil
}

// ListIndexes 列出所有索引
func (m *IndexManager) ListIndexes() []*ManagedIndex {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    result := make([]*ManagedIndex, 0, len(m.indexes))
    for _, managed := range m.indexes {
        result = append(result, managed)
    }
    return result
}

// CheckAndRebuild 检查并重建过期的索引
func (m *IndexManager) CheckAndRebuild(ctx context.Context, rowIDColumn string) error {
    m.mu.Lock()
    indexes := make([]*ManagedIndex, 0, len(m.indexes))
    for _, managed := range m.indexes {
        indexes = append(indexes, managed)
    }
    m.mu.Unlock()
    
    for _, managed := range indexes {
        currentVersion, err := m.dataSourceAccessor.GetDataVersion(
            ctx,
            managed.Key.DataSourceName,
            managed.Key.TableName,
        )
        if err != nil {
            continue
        }
        
        if currentVersion != managed.DataSourceVersion {
            // 数据已变更，需要重建
            managed.Status = IndexStatusOutdated
            if m.config.AsyncBuild {
                go m.buildIndexAsync(managed, rowIDColumn)
            } else {
                m.buildIndexSync(ctx, managed, rowIDColumn)
            }
        }
    }
    
    return nil
}
```

### 1.3 索引构建器

**文件**: `pkg/vector/builder.go`

```go
package vector

import (
    "context"
    "fmt"
)

// IndexBuilder 向量索引构建器
type IndexBuilder struct{}

// NewIndexBuilder 创建构建器
func NewIndexBuilder() *IndexBuilder {
    return &IndexBuilder{}
}

// Build 构建索引
func (b *IndexBuilder) Build(
    ctx context.Context,
    data *VectorData,
    config *IndexConfig,
) (Index, error) {
    switch config.Type {
    case IndexHNSW:
        return b.buildHNSW(ctx, data, config)
    case IndexIVFFlat:
        return b.buildIVF(ctx, data, config, false)
    case IndexIVFPQ:
        return b.buildIVF(ctx, data, config, true)
    case IndexIVFSQ8:
        return b.buildIVFSQ8(ctx, data, config)
    case IndexFlat:
        return b.buildFlat(ctx, data, config)
    default:
        return nil, fmt.Errorf("unsupported index type: %s", config.Type)
    }
}

// buildHNSW 构建 HNSW 索引
func (b *IndexBuilder) buildHNSW(
    ctx context.Context,
    data *VectorData,
    config *IndexConfig,
) (Index, error) {
    params := config.HNSW
    if params == nil {
        params = &HNSWParams{M: 16, EfConstruction: 200, EfSearch: 64}
    }
    
    index := NewHNSWIndex(config.Dimension, config.MetricType, params)
    
    // 批量添加向量
    for i, vec := range data.Vectors {
        if err := index.Add(data.RowIDs[i], vec); err != nil {
            return nil, fmt.Errorf("add vector failed at %d: %w", i, err)
        }
        
        // 检查上下文取消
        if i%1000 == 0 {
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            default:
            }
        }
    }
    
    return index, nil
}

// buildIVF 构建 IVF 索引
func (b *IndexBuilder) buildIVF(
    ctx context.Context,
    data *VectorData,
    config *IndexConfig,
    usePQ bool,
) (Index, error) {
    params := config.IVF
    if params == nil {
        params = &IVFParams{NList: 100, NProbe: 10}
    }
    
    index := NewIVFIndex(config.Dimension, config.MetricType, params, usePQ)
    
    // 训练索引（如果数据量足够）
    if len(data.Vectors) > params.NList {
        if err := index.Train(data.Vectors); err != nil {
            return nil, fmt.Errorf("train index failed: %w", err)
        }
    }
    
    // 添加向量
    for i, vec := range data.Vectors {
        if err := index.Add(data.RowIDs[i], vec); err != nil {
            return nil, fmt.Errorf("add vector failed at %d: %w", i, err)
        }
    }
    
    return index, nil
}

// buildIVFSQ8 构建 IVF_SQ8 索引
func (b *IndexBuilder) buildIVFSQ8(
    ctx context.Context,
    data *VectorData,
    config *IndexConfig,
) (Index, error) {
    // 类似 IVF，但使用标量量化
    return b.buildIVF(ctx, data, config, false)
}

// buildFlat 构建暴力搜索索引
func (b *IndexBuilder) buildFlat(
    ctx context.Context,
    data *VectorData,
    config *IndexConfig,
) (Index, error) {
    index := NewFlatIndex(config.Dimension, config.MetricType)
    
    for i, vec := range data.Vectors {
        if err := index.Add(data.RowIDs[i], vec); err != nil {
            return nil, err
        }
    }
    
    return index, nil
}
```

### 1.4 数据源访问实现

**文件**: `pkg/vector/datasource_accessor.go`

```go
package vector

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/resource/application"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// DefaultDataSourceAccessor 默认数据源访问实现
type DefaultDataSourceAccessor struct {
    manager *application.DataSourceManager
}

// NewDefaultDataSourceAccessor 创建默认访问器
func NewDefaultDataSourceAccessor(manager *application.DataSourceManager) *DefaultDataSourceAccessor {
    return &DefaultDataSourceAccessor{manager: manager}
}

// ReadVectorData 从数据源读取向量数据
func (a *DefaultDataSourceAccessor) ReadVectorData(
    ctx context.Context,
    dataSourceName string,
    tableName string,
    columnName string,
    rowIDColumn string,
) (*VectorData, error) {
    // 获取数据源
    ds, err := a.manager.Get(dataSourceName)
    if err != nil {
        return nil, fmt.Errorf("get data source failed: %w", err)
    }
    
    // 查询所有数据（只读取必要的列）
    queryOptions := &domain.QueryOptions{
        SelectAll: false,
    }
    
    // 如果没有指定行ID列，使用数据源默认方式生成
    columns := []string{columnName}
    if rowIDColumn != "" {
        columns = append(columns, rowIDColumn)
    }
    
    result, err := ds.Query(ctx, tableName, queryOptions)
    if err != nil {
        return nil, fmt.Errorf("query data failed: %w", err)
    }
    
    // 转换为向量数据
    vectorData := &VectorData{
        RowIDs:  make([]int64, 0, len(result.Rows)),
        Vectors: make([][]float32, 0, len(result.Rows)),
    }
    
    for i, row := range result.Rows {
        // 获取向量值
        vecValue, ok := row[columnName]
        if !ok {
            continue  // 跳过没有向量的行
        }
        
        vec, err := convertToFloatSlice(vecValue)
        if err != nil {
            continue  // 跳过无效的向量
        }
        
        // 获取行ID
        var rowID int64 = int64(i)  // 默认使用行号
        if rowIDColumn != "" {
            if idValue, ok := row[rowIDColumn]; ok {
                rowID = convertToInt64(idValue)
            }
        }
        
        vectorData.RowIDs = append(vectorData.RowIDs, rowID)
        vectorData.Vectors = append(vectorData.Vectors, vec)
    }
    
    return vectorData, nil
}

// GetDataVersion 获取数据版本
func (a *DefaultDataSourceAccessor) GetDataVersion(
    ctx context.Context,
    dataSourceName string,
    tableName string,
) (string, error) {
    ds, err := a.manager.Get(dataSourceName)
    if err != nil {
        return "", err
    }
    
    // 获取表信息
    tableInfo, err := ds.GetTableInfo(ctx, tableName)
    if err != nil {
        return "", err
    }
    
    // 获取行数作为版本标识
    rowCount, err := a.GetRowCount(ctx, dataSourceName, tableName)
    if err != nil {
        return "", err
    }
    
    // 组合版本信息
    version := fmt.Sprintf("%s_%d_%d", tableName, len(tableInfo.Columns), rowCount)
    return version, nil
}

// GetRowCount 获取数据行数
func (a *DefaultDataSourceAccessor) GetRowCount(
    ctx context.Context,
    dataSourceName string,
    tableName string,
) (int64, error) {
    ds, err := a.manager.Get(dataSourceName)
    if err != nil {
        return 0, err
    }
    
    result, err := ds.Query(ctx, tableName, &domain.QueryOptions{Limit: 0})
    if err != nil {
        return 0, err
    }
    
    return result.Total, nil
}

// 辅助函数：转换值为 float32 切片
func convertToFloatSlice(v interface{}) ([]float32, error) {
    switch val := v.(type) {
    case []float32:
        return val, nil
    case []float64:
        result := make([]float32, len(val))
        for i, f := range val {
            result[i] = float32(f)
        }
        return result, nil
    case []interface{}:
        result := make([]float32, len(val))
        for i, item := range val {
            switch n := item.(type) {
            case float32:
                result[i] = n
            case float64:
                result[i] = float32(n)
            case int:
                result[i] = float32(n)
            default:
                return nil, fmt.Errorf("invalid vector element type: %T", item)
            }
        }
        return result, nil
    case string:
        // 尝试解析 JSON 字符串 [1.0, 2.0, 3.0]
        return parseVectorString(val)
    default:
        return nil, fmt.Errorf("cannot convert %T to float32 slice", v)
    }
}

// 辅助函数：解析向量字符串
func parseVectorString(s string) ([]float32, error) {
    // 实现 JSON 解析或其他格式解析
    // ...
}

func convertToInt64(v interface{}) int64 {
    switch n := v.(type) {
    case int64:
        return n
    case int:
        return int64(n)
    case int32:
        return int64(n)
    case float64:
        return int64(n)
    case float32:
        return int64(n)
    default:
        return 0
    }
}
```

---

## 2. HNSW 索引实现

**文件**: `pkg/vector/hnsw.go`

```go
package vector

import (
    "container/heap"
    "context"
    "math"
    "math/rand"
    "sync"
)

// HNSWIndex HNSW 索引实现
type HNSWIndex struct {
    dimension   int
    metricType  MetricType
    params      *HNSWParams
    
    // 索引数据
    nodes       map[int64]*HNSWNode
    entryPoint  int64
    maxLevel    int
    
    // 并发控制
    mu          sync.RWMutex
}

// HNSWNode HNSW 节点
type HNSWNode struct {
    ID        int64
    Vector    []float32
    Level     int
    Neighbors [][]int64  // 每层的邻居
}

// NewHNSWIndex 创建 HNSW 索引
func NewHNSWIndex(
    dimension int,
    metricType MetricType,
    params *HNSWParams,
) *HNSWIndex {
    if params == nil {
        params = &HNSWParams{M: 16, EfConstruction: 200, EfSearch: 64}
    }
    
    return &HNSWIndex{
        dimension:  dimension,
        metricType: metricType,
        params:     params,
        nodes:      make(map[int64]*HNSWNode),
    }
}

// Add 添加向量
func (idx *HNSWIndex) Add(id int64, vector []float32) error {
    if len(vector) != idx.dimension {
        return fmt.Errorf("dimension mismatch: expected %d, got %d", idx.dimension, len(vector))
    }
    
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    // 计算节点层数
    level := idx.randomLevel()
    
    node := &HNSWNode{
        ID:        id,
        Vector:    make([]float32, len(vector)),
        Level:     level,
        Neighbors: make([][]int64, level+1),
    }
    copy(node.Vector, vector)
    
    // 空索引直接作为入口点
    if len(idx.nodes) == 0 {
        idx.nodes[id] = node
        idx.entryPoint = id
        idx.maxLevel = level
        return nil
    }
    
    // 插入节点
    currEp := idx.entryPoint
    currDist := idx.distance(idx.nodes[currEp].Vector, vector)
    
    // 从顶层开始逐层搜索
    for l := idx.maxLevel; l > level; l-- {
        changed := true
        for changed {
            changed = false
            if l >= len(idx.nodes[currEp].Neighbors) {
                break
            }
            for _, neighborID := range idx.nodes[currEp].Neighbors[l] {
                if neighbor, ok := idx.nodes[neighborID]; ok {
                    dist := idx.distance(neighbor.Vector, vector)
                    if dist < currDist {
                        currEp = neighborID
                        currDist = dist
                        changed = true
                    }
                }
            }
        }
    }
    
    // 在 level 及以下层构建连接
    for l := min(level, idx.maxLevel); l >= 0; l-- {
        // 搜索最近邻居
        candidates := idx.searchLayer(vector, currEp, idx.params.EfConstruction, l)
        
        // 选择 M 个邻居
        neighbors := idx.selectNeighbors(candidates, idx.params.M)
        node.Neighbors[l] = neighbors
        
        // 双向连接
        for _, neighborID := range neighbors {
            if neighbor, ok := idx.nodes[neighborID]; ok {
                idx.addBidirectionalConnection(neighbor, node.ID, l)
            }
        }
    }
    
    idx.nodes[id] = node
    
    // 更新入口点
    if level > idx.maxLevel {
        idx.maxLevel = level
        idx.entryPoint = id
    }
    
    return nil
}

// Search 搜索最近邻
func (idx *HNSWIndex) Search(
    ctx context.Context,
    query interface{},
    k int,
    filter *SearchFilter,
) (*SearchResult, error) {
    queryVec, ok := query.([]float32)
    if !ok {
        return nil, fmt.Errorf("query must be []float32")
    }
    
    if len(queryVec) != idx.dimension {
        return nil, fmt.Errorf("dimension mismatch")
    }
    
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    if len(idx.nodes) == 0 {
        return &SearchResult{IDs: []int64{}, Distances: []float32{}}, nil
    }
    
    // 获取候选行ID（如果有预过滤）
    allowedIDs := make(map[int64]bool)
    if filter != nil && len(filter.RowIDs) > 0 {
        for _, id := range filter.RowIDs {
            allowedIDs[id] = true
        }
    }
    
    // 从入口点开始搜索
    currEp := idx.entryPoint
    currDist := idx.distance(idx.nodes[currEp].Vector, queryVec)
    
    // 从顶层开始逐层搜索
    for l := idx.maxLevel; l > 0; l-- {
        changed := true
        for changed {
            changed = false
            if l >= len(idx.nodes[currEp].Neighbors) {
                break
            }
            for _, neighborID := range idx.nodes[currEp].Neighbors[l] {
                // 应用过滤
                if len(allowedIDs) > 0 && !allowedIDs[neighborID] {
                    continue
                }
                
                if neighbor, ok := idx.nodes[neighborID]; ok {
                    dist := idx.distance(neighbor.Vector, queryVec)
                    if dist < currDist {
                        currEp = neighborID
                        currDist = dist
                        changed = true
                    }
                }
            }
        }
    }
    
    // 在最底层进行 efSearch 搜索
    candidates := idx.searchLayerWithFilter(queryVec, currEp, idx.params.EfSearch, 0, allowedIDs)
    
    // 取前 k 个
    if len(candidates) > k {
        candidates = candidates[:k]
    }
    
    result := &SearchResult{
        IDs:        make([]int64, len(candidates)),
        Distances:  make([]float32, len(candidates)),
        TotalCount: len(candidates),
    }
    
    for i, c := range candidates {
        result.IDs[i] = c.ID
        result.Distances[i] = float32(c.Distance)
    }
    
    return result, nil
}

// GetStats 获取统计信息
func (idx *HNSWIndex) GetStats() IndexStats {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    return IndexStats{
        Type:         IndexHNSW,
        MetricType:   idx.metricType,
        Dimension:    idx.dimension,
        TotalVectors: int64(len(idx.nodes)),
    }
}

// Close 关闭索引
func (idx *HNSWIndex) Close() error {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    idx.nodes = nil
    return nil
}

// 距离计算
func (idx *HNSWIndex) distance(v1, v2 []float32) float64 {
    switch idx.metricType {
    case MetricCosine:
        return cosineDistance(v1, v2)
    case MetricL2:
        return l2Distance(v1, v2)
    case MetricIP:
        return -innerProduct(v1, v2)
    default:
        return l2Distance(v1, v2)
    }
}

// randomLevel 随机层数
func (idx *HNSWIndex) randomLevel() int {
    level := 0
    for rand.Float64() < 0.5 && level < 16 {
        level++
    }
    return level
}

// searchLayer 在某一层搜索
func (idx *HNSWIndex) searchLayer(
    query []float32,
    entryPoint int64,
    ef int,
    level int,
) []NodeDistance {
    // 实现贪婪搜索
    // ...
}

// searchLayerWithFilter 带过滤的层搜索
func (idx *HNSWIndex) searchLayerWithFilter(
    query []float32,
    entryPoint int64,
    ef int,
    level int,
    allowedIDs map[int64]bool,
) []NodeDistance {
    // 实现带过滤的搜索
    // ...
}

// selectNeighbors 选择邻居
func (idx *HNSWIndex) selectNeighbors(candidates []NodeDistance, m int) []int64 {
    // 实现邻居选择算法
    // ...
}

// addBidirectionalConnection 添加双向连接
func (idx *HNSWIndex) addBidirectionalConnection(node *HNSWNode, newNeighborID int64, level int) {
    // 添加连接并维护最大连接数
    // ...
}

// 距离计算辅助函数
func cosineDistance(v1, v2 []float32) float64 {
    var dot, norm1, norm2 float64
    for i := 0; i < len(v1); i++ {
        dot += float64(v1[i] * v2[i])
        norm1 += float64(v1[i] * v1[i])
        norm2 += float64(v2[i] * v2[i])
    }
    if norm1 == 0 || norm2 == 0 {
        return 1.0
    }
    return 1.0 - dot/(math.Sqrt(norm1)*math.Sqrt(norm2))
}

func l2Distance(v1, v2 []float32) float64 {
    var sum float64
    for i := 0; i < len(v1); i++ {
        diff := float64(v1[i] - v2[i])
        sum += diff * diff
    }
    return math.Sqrt(sum)
}

func innerProduct(v1, v2 []float32) float64 {
    var sum float64
    for i := 0; i < len(v1); i++ {
        sum += float64(v1[i] * v2[i])
    }
    return sum
}
```

---

## 3. 优化器集成

### 3.1 向量搜索优化规则

**文件**: `pkg/optimizer/vector_search_optimizer.go`

```go
package optimizer

import (
    "github.com/kasuganosora/sqlexec/pkg/parser"
    "github.com/kasuganosora/sqlexec/pkg/vector"
)

// VectorSearchOptimizer 向量搜索优化器
type VectorSearchOptimizer struct {
    indexManager *vector.IndexManager
}

// NewVectorSearchOptimizer 创建优化器
func NewVectorSearchOptimizer(manager *vector.IndexManager) *VectorSearchOptimizer {
    return &VectorSearchOptimizer{indexManager: manager}
}

// Optimize 优化向量搜索查询
// 检测 ORDER BY VEC_... LIMIT 模式并转换为 VectorScan
func (o *VectorSearchOptimizer) Optimize(node LogicalOperator) (LogicalOperator, error) {
    // 查找 Sort + Limit 模式
    sortOp, ok := node.(*LogicalSort)
    if !ok {
        return node, nil
    }
    
    limitOp, ok := sortOp.Children[0].(*LogicalLimit)
    if !ok {
        return node, nil
    }
    
    // 检查 ORDER BY 是否为向量距离函数
    if len(sortOp.OrderBy) != 1 {
        return node, nil
    }
    
    orderExpr := sortOp.OrderBy[0].Expr
    if orderExpr == nil || orderExpr.Type != parser.ExprTypeFunction {
        return node, nil
    }
    
    var metricType vector.MetricType
    switch orderExpr.Function {
    case "VEC_COSINE_DISTANCE":
        metricType = vector.MetricCosine
    case "VEC_L2_DISTANCE":
        metricType = vector.MetricL2
    case "VEC_INNER_PRODUCT":
        metricType = vector.MetricIP
    default:
        return node, nil
    }
    
    // 获取列名和查询向量
    columnName := orderExpr.Args[0].Column
    queryVector := extractVectorFromExpr(orderExpr.Args[1])
    
    if queryVector == nil {
        return node, nil
    }
    
    // 检查是否有可用的向量索引
    indexKey := vector.IndexKey{
        DataSourceName: sortOp.DataSource,  // 需要从上下文中获取
        TableName:      sortOp.TableName,
        ColumnName:     columnName,
    }
    
    idx, err := o.indexManager.GetIndex(indexKey)
    if err != nil {
        // 没有索引，保持原计划（将执行暴力搜索）
        return node, nil
    }
    
    // 检查索引的距离度量是否匹配
    stats := idx.GetStats()
    if stats.MetricType != metricType {
        // 距离度量不匹配，不能使用索引
        return node, nil
    }
    
    // 创建向量扫描算子
    vectorScan := &LogicalVectorScan{
        TableName:    sortOp.TableName,
        ColumnName:   columnName,
        QueryVector:  queryVector,
        K:            int(limitOp.Limit),
        MetricType:   metricType,
        IndexKey:     indexKey,
        PreFilters:   extractFilters(limitOp.Children[0]),
    }
    
    return vectorScan, nil
}
```

### 3.2 向量扫描逻辑算子

**文件**: `pkg/optimizer/logical_vector_scan.go`

```go
package optimizer

import (
    "github.com/kasuganosora/sqlexec/pkg/parser"
    "github.com/kasuganosora/sqlexec/pkg/vector"
)

// LogicalVectorScan 向量索引扫描
type LogicalVectorScan struct {
    baseLogicalOperator
    
    TableName    string
    ColumnName   string
    QueryVector  []float32
    K            int
    MetricType   vector.MetricType
    IndexKey     vector.IndexKey
    PreFilters   []*parser.Expression  // 预过滤条件
}

// NewLogicalVectorScan 创建向量扫描算子
func NewLogicalVectorScan(
    tableName, columnName string,
    queryVector []float32,
    k int,
    metricType vector.MetricType,
) *LogicalVectorScan {
    return &LogicalVectorScan{
        TableName:   tableName,
        ColumnName:  columnName,
        QueryVector: queryVector,
        K:           k,
        MetricType:  metricType,
    }
}

// GetChildren 获取子节点
func (op *LogicalVectorScan) GetChildren() []LogicalOperator {
    return nil  // 向量扫描是叶子节点
}

// SetChildren 设置子节点
func (op *LogicalVectorScan) SetChildren(children ...LogicalOperator) {
    // 无子节点
}

// EstimateCost 估算成本
func (op *LogicalVectorScan) EstimateCost() float64 {
    // 向量索引搜索成本远低于全表扫描
    // 成本 = 索引搜索成本 + 获取行数据成本
    return float64(op.K) * 10  // 基准成本
}
```

---

## 4. 执行器集成

### 4.1 向量搜索算子

**文件**: `pkg/executor/operators/vector_search.go`

```go
package operators

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/vector"
)

// VectorSearchOperator 向量搜索算子
type VectorSearchOperator struct {
    *BaseOperator
    config *plan.VectorSearchPlan
}

// VectorSearchPlan 向量搜索计划配置
// 定义在 pkg/optimizer/plan/vector.go
type VectorSearchPlan struct {
    TableName    string
    ColumnName   string
    QueryVector  []float32
    K            int
    MetricType   string
    IndexKey     vector.IndexKey
    PreFilters   []domain.Filter
    DistanceCol  string  // 距离列别名
}

// NewVectorSearchOperator 创建算子
func NewVectorSearchOperator(
    p *plan.Plan,
    das dataaccess.Service,
) (*VectorSearchOperator, error) {
    config, ok := p.Config.(*plan.VectorSearchPlan)
    if !ok {
        return nil, fmt.Errorf("invalid config type")
    }
    
    return &VectorSearchOperator{
        BaseOperator: NewBaseOperator(p, das),
        config:       config,
    }, nil
}

// Execute 执行向量搜索
func (op *VectorSearchOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
    // 1. 从上下文获取 IndexManager（通过依赖注入）
    indexManager := op.GetVectorIndexManager()  // 需要在 BaseOperator 中实现
    
    // 2. 执行向量搜索
    metricType := vector.MetricType(op.config.MetricType)
    filter := &vector.SearchFilter{
        RowIDs: op.getPreFilteredRowIDs(ctx),
    }
    
    searchResult, err := indexManager.Search(
        ctx,
        op.config.IndexKey,
        op.config.QueryVector,
        op.config.K,
        filter,
    )
    if err != nil {
        return nil, fmt.Errorf("vector search failed: %w", err)
    }
    
    // 3. 根据 RowIDs 获取完整行数据
    rows, err := op.fetchRowsByIDs(ctx, searchResult.IDs)
    if err != nil {
        return nil, fmt.Errorf("fetch rows failed: %w", err)
    }
    
    // 4. 添加距离列
    if op.config.DistanceCol != "" {
        for i, row := range rows {
            row[op.config.DistanceCol] = searchResult.Distances[i]
        }
    }
    
    return &domain.QueryResult{
        Columns: op.buildColumns(),
        Rows:    rows,
        Total:   int64(len(rows)),
    }, nil
}

// getPreFilteredRowIDs 获取预过滤的行ID
func (op *VectorSearchOperator) getPreFilteredRowIDs(ctx context.Context) []int64 {
    if len(op.config.PreFilters) == 0 {
        return nil
    }
    
    // 执行预过滤查询，获取符合条件的行ID
    // 返回 nil 表示不过滤
    return nil
}

// fetchRowsByIDs 根据ID获取行数据
func (op *VectorSearchOperator) fetchRowsByIDs(
    ctx context.Context,
    ids []int64,
) ([]domain.Row, error) {
    // 使用 DataService 根据 ID 列表查询数据
    // 实现批量查询优化
    result := make([]domain.Row, 0, len(ids))
    
    for _, id := range ids {
        // 查询单行数据
        filter := domain.Filter{
            Field:    "id",  // 或使用配置的主键列
            Operator: "=",
            Value:    id,
        }
        
        rows, _, err := op.dataAccessService.Filter(ctx, op.config.TableName, filter, 0, 1)
        if err != nil {
            continue
        }
        
        if len(rows) > 0 {
            result = append(result, rows[0])
        }
    }
    
    return result, nil
}

// buildColumns 构建输出列信息
func (op *VectorSearchOperator) buildColumns() []domain.ColumnInfo {
    // 获取表信息，构建列信息
    // 如果指定了距离列别名，添加距离列
    return nil
}
```

---

## 5. 使用示例

### 5.1 初始化向量搜索层

```go
import (
    "github.com/kasuganosora/sqlexec/pkg/resource/application"
    "github.com/kasuganosora/sqlexec/pkg/vector"
)

// 1. 创建数据源管理器
dsManager := application.NewDataSourceManager()

// 2. 注册数据源（CSV、JSON、MySQL等）
dsManager.Register("articles_csv", csvDataSource)
dsManager.Register("products_api", apiDataSource)

// 3. 创建数据源访问器
accessor := vector.NewDefaultDataSourceAccessor(dsManager)

// 4. 创建向量索引管理器
indexManager := vector.NewIndexManager(accessor, &vector.ManagerConfig{
    DefaultIndexType:   vector.IndexHNSW,
    AsyncBuild:         true,
    MaxMemoryUsage:     2 * 1024 * 1024 * 1024,  // 2GB
})

// 5. 注册到优化器和执行器
optimizer.RegisterVectorIndexManager(indexManager)
executor.RegisterVectorIndexManager(indexManager)
```

### 5.2 创建向量索引

```go
// 从 CSV 数据源创建向量索引
indexKey := vector.IndexKey{
    DataSourceName: "articles_csv",
    TableName:      "articles",
    ColumnName:     "embedding",
}

config := &vector.IndexConfig{
    Type:       vector.IndexHNSW,
    MetricType: vector.MetricCosine,
    Dimension:  768,
    HNSW: &vector.HNSWParams{
        M:              16,
        EfConstruction: 200,
    },
}

// 创建索引（异步构建）
managedIndex, err := indexManager.CreateIndex(ctx, indexKey, config, "id")
if err != nil {
    log.Fatal(err)
}

// 等待构建完成
for managedIndex.Status != vector.IndexStatusReady {
    time.Sleep(100 * time.Millisecond)
    fmt.Printf("Building... %.1f%%\n", managedIndex.Progress)
}
```

### 5.3 执行向量搜索

```go
// 方式1: 直接使用 IndexManager
queryVector := []float32{0.1, 0.2, 0.3, /* ... 768维 ... */}

result, err := indexManager.Search(ctx, indexKey, queryVector, 10, nil)
if err != nil {
    log.Fatal(err)
}

for i, id := range result.IDs {
    fmt.Printf("ID: %d, Distance: %.4f\n", id, result.Distances[i])
}

// 方式2: 使用 SQL
rows, err := db.Query(`
    SELECT id, title, VEC_COSINE_DISTANCE(embedding, ?) AS distance
    FROM articles
    ORDER BY distance
    LIMIT 10
`, queryVector)
```

---

## 6. 总结

### 优势

1. **存储无关**: 不修改 CSV/JSON/API 等存储层
2. **按需构建**: 从任何数据源读取数据构建内存索引
3. **独立管理**: 向量索引有自己的生命周期
4. **统一接口**: 无论底层存储是什么，向量搜索接口一致
5. **增量更新**: 支持检测数据变化并重建索引

### 文件结构

```
pkg/vector/
├── interfaces.go           # 核心接口定义
├── manager.go              # 索引管理器
├── builder.go              # 索引构建器
├── hnsw.go                 # HNSW 索引实现
├── ivf.go                  # IVF 索引实现
├── flat.go                 # 暴力搜索索引
├── distance.go             # 距离计算函数
├── datasource_accessor.go  # 数据源访问实现
└── utils.go                # 工具函数
```

### 集成点

1. **Parser**: 解析 `CREATE VECTOR INDEX` 和向量函数
2. **Optimizer**: 识别向量搜索模式，选择使用索引或暴力搜索
3. **Executor**: 执行向量搜索算子，从索引获取结果后查询完整数据
4. **API**: 提供 SQL 接口和编程接口两种方式
