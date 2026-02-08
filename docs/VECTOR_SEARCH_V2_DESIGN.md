# SQLExec 向量搜索 V2 设计

## 设计原则

1. **复用现有层**: 扩展 `pkg/dataaccess` 而不是新建独立层
2. **开闭原则**: 新增算法/索引类型只添加新文件，不修改旧代码
3. **插件化架构**: 距离计算、索引类型、搜索策略都是可插拔的
4. **向后兼容**: 不影响现有 DataAccess 接口的使用

---

## 架构图

```
pkg/dataaccess/
├── service.go                    # 现有 Service 接口
├── manager.go                    # 现有 Manager
├── router.go                     # 现有 Router
│
├── index/                        # 索引子包（新增）
│   ├── registry.go               # 索引注册中心
│   ├── builder.go                # 索引构建器接口
│   ├── manager.go                # 索引管理器（管理所有索引类型）
│   │
│   ├── distance/                 # 距离计算（模块化）
│   │   ├── interface.go          # DistanceFunc 接口
│   │   ├── cosine.go             # 余弦距离
│   │   ├── l2.go                 # 欧氏距离
│   │   ├── inner_product.go      # 内积
│   │   ├── hamming.go            # 汉明距离
│   │   └── registry.go           # 距离函数注册中心
│   │
│   ├── types/                    # 索引类型实现（每个类型独立文件）
│   │   ├── interface.go          # VectorIndex 接口
│   │   ├── hnsw/                 # HNSW 索引
│   │   │   ├── index.go          # HNSW 实现
│   │   │   ├── node.go           # 节点结构
│   │   │   └── params.go         # 参数配置
│   │   │
│   │   ├── ivf/                  # IVF 索引
│   │   │   ├── index.go          # IVF 实现
│   │   │   ├── kmeans.go         # K-Means 聚类
│   │   │   └── params.go         # 参数配置
│   │   │
│   │   ├── ivfpq/                # IVF-PQ 索引
│   │   ├── ivfsq8/               # IVF-SQ8 索引
│   │   └── flat/                 # 暴力搜索
│   │       └── index.go
│   │
│   └── search/                   # 搜索执行层
│       ├── interface.go          # Searcher 接口
│       ├── ann_searcher.go       # ANN 搜索器
│       └── brute_force.go        # 暴力搜索器
│
└── vector_service.go             # 扩展 Service 支持向量搜索
```

---

## 1. 索引注册中心（开闭原则核心）

**文件**: `pkg/dataaccess/index/registry.go`

```go
package index

import (
    "context"
    "fmt"
    "sync"
)

// IndexType 索引类型标识
type IndexType string

const (
    TypeHNSW    IndexType = "hnsw"
    TypeIVFFlat IndexType = "ivf_flat"
    TypeIVFPQ   IndexType = "ivf_pq"
    TypeIVFSQ8  IndexType = "ivf_sq8"
    TypeFlat    IndexType = "flat"
)

// Factory 索引工厂函数
type Factory func(config *Config) (VectorIndex, error)

// Registry 索引注册中心
// 使用注册模式实现开闭原则：新增索引类型只需注册，无需修改现有代码
type Registry struct {
    factories map[IndexType]Factory
    mu        sync.RWMutex
}

var globalRegistry = &Registry{
    factories: make(map[IndexType]Factory),
}

// Register 注册索引工厂
// 示例：在 hnsw/index.go 的 init() 中调用
func Register(indexType IndexType, factory Factory) {
    globalRegistry.mu.Lock()
    defer globalRegistry.mu.Unlock()
    
    if _, exists := globalRegistry.factories[indexType]; exists {
        panic(fmt.Sprintf("index type %s already registered", indexType))
    }
    
    globalRegistry.factories[indexType] = factory
}

// Create 创建索引实例
func Create(indexType IndexType, config *Config) (VectorIndex, error) {
    globalRegistry.mu.RLock()
    defer globalRegistry.mu.RUnlock()
    
    factory, ok := globalRegistry.factories[indexType]
    if !ok {
        return nil, fmt.Errorf("unknown index type: %s", indexType)
    }
    
    return factory(config)
}

// ListRegisteredTypes 列出已注册的索引类型
func ListRegisteredTypes() []IndexType {
    globalRegistry.mu.RLock()
    defer globalRegistry.mu.RUnlock()
    
    types := make([]IndexType, 0, len(globalRegistry.factories))
    for t := range globalRegistry.factories {
        types = append(types, t)
    }
    return types
}

// IsRegistered 检查索引类型是否已注册
func IsRegistered(indexType IndexType) bool {
    globalRegistry.mu.RLock()
    defer globalRegistry.mu.RUnlock()
    
    _, ok := globalRegistry.factories[indexType]
    return ok
}
```

---

## 2. 距离计算模块化（每个算法独立文件）

### 2.1 接口定义

**文件**: `pkg/dataaccess/index/distance/interface.go`

```go
package distance

// Func 距离函数接口
type Func interface {
    // Name 距离函数名称
    Name() string
    
    // Compute 计算两个向量的距离
    // v1, v2: 输入向量
    // 返回距离值（越小越相似，内积除外）
    Compute(v1, v2 []float32) float32
    
    // IsSimilarity 是否为相似度度量（值越大越相似）
    IsSimilarity() bool
}

// BinaryFunc 二值向量距离函数接口
type BinaryFunc interface {
    Func
    ComputeBinary(v1, v2 []byte) float32
}

// RegistryKey 获取注册表的 key
func RegistryKey(name string) string {
    return name
}
```

### 2.2 余弦距离

**文件**: `pkg/dataaccess/index/distance/cosine.go`

```go
package distance

import (
    "math"
    "sync"
)

func init() {
    Register("cosine", &Cosine{})
    Register("cos", &Cosine{}) // 别名
}

// Cosine 余弦距离实现
type Cosine struct{}

// Name 距离函数名称
func (c *Cosine) Name() string {
    return "cosine"
}

// Compute 计算余弦距离
// 距离 = 1 - 余弦相似度
func (c *Cosine) Compute(v1, v2 []float32) float32 {
    if len(v1) != len(v2) {
        return math.MaxFloat32
    }
    
    var dot, norm1, norm2 float64
    for i := 0; i < len(v1); i++ {
        dot += float64(v1[i] * v2[i])
        norm1 += float64(v1[i] * v1[i])
        norm2 += float64(v2[i] * v2[i])
    }
    
    if norm1 == 0 || norm2 == 0 {
        return 1.0  // 零向量距离最大
    }
    
    similarity := dot / (math.Sqrt(norm1) * math.Sqrt(norm2))
    return float32(1.0 - similarity)
}

// IsSimilarity 是否为相似度度量
func (c *Cosine) IsSimilarity() bool {
    return false  // 距离度量，越小越相似
}

// CosineSimilarity 余弦相似度（值越大越相似）
type CosineSimilarity struct{}

func init() {
    Register("cosine_similarity", &CosineSimilarity{})
}

func (c *CosineSimilarity) Name() string {
    return "cosine_similarity"
}

func (c *CosineSimilarity) Compute(v1, v2 []float32) float32 {
    cos := &Cosine{}
    return 1.0 - cos.Compute(v1, v2)  // 相似度 = 1 - 距离
}

func (c *CosineSimilarity) IsSimilarity() bool {
    return true
}
```

### 2.3 欧氏距离

**文件**: `pkg/dataaccess/index/distance/l2.go`

```go
package distance

import (
    "math"
)

func init() {
    Register("l2", &L2{})
    Register("euclidean", &L2{}) // 别名
}

// L2 欧氏距离（L2范数）
type L2 struct{}

// Name 距离函数名称
func (l *L2) Name() string {
    return "l2"
}

// Compute 计算欧氏距离
func (l *L2) Compute(v1, v2 []float32) float32 {
    if len(v1) != len(v2) {
        return math.MaxFloat32
    }
    
    var sum float64
    for i := 0; i < len(v1); i++ {
        diff := float64(v1[i] - v2[i])
        sum += diff * diff
    }
    
    return float32(math.Sqrt(sum))
}

// IsSimilarity 是否为相似度度量
func (l *L2) IsSimilarity() bool {
    return false
}

// L2Squared 欧氏距离平方（避免开方运算，更快）
type L2Squared struct{}

func init() {
    Register("l2_squared", &L2Squared{})
}

func (l *L2Squared) Name() string {
    return "l2_squared"
}

func (l *L2Squared) Compute(v1, v2 []float32) float32 {
    if len(v1) != len(v2) {
        return math.MaxFloat32
    }
    
    var sum float64
    for i := 0; i < len(v1); i++ {
        diff := float64(v1[i] - v2[i])
        sum += diff * diff
    }
    
    return float32(sum)  // 不开方
}

func (l *L2Squared) IsSimilarity() bool {
    return false
}
```

### 2.4 内积

**文件**: `pkg/dataaccess/index/distance/inner_product.go`

```go
package distance

func init() {
    Register("ip", &InnerProduct{})
    Register("inner_product", &InnerProduct{})
    Register("dot", &InnerProduct{})
}

// InnerProduct 内积（点积）
// 注意：内积越大越相似，通常取负值作为距离
type InnerProduct struct{}

func (i *InnerProduct) Name() string {
    return "inner_product"
}

func (i *InnerProduct) Compute(v1, v2 []float32) float32 {
    if len(v1) != len(v2) {
        return 0
    }
    
    var sum float32
    for j := 0; j < len(v1); j++ {
        sum += v1[j] * v2[j]
    }
    
    return -sum  // 取负，使越大越相似转为越小越相似
}

func (i *InnerProduct) IsSimilarity() bool {
    return false  // 返回的是负内积，作为距离使用
}

// InnerProductRaw 原始内积（正值，越大越相似）
type InnerProductRaw struct{}

func init() {
    Register("inner_product_raw", &InnerProductRaw{})
}

func (i *InnerProductRaw) Name() string {
    return "inner_product_raw"
}

func (i *InnerProductRaw) Compute(v1, v2 []float32) float32 {
    if len(v1) != len(v2) {
        return 0
    }
    
    var sum float32
    for j := 0; j < len(v1); j++ {
        sum += v1[j] * v2[j]
    }
    
    return sum
}

func (i *InnerProductRaw) IsSimilarity() bool {
    return true  // 原始内积，越大越相似
}
```

### 2.5 距离函数注册中心

**文件**: `pkg/dataaccess/index/distance/registry.go`

```go
package distance

import (
    "fmt"
    "sync"
)

var (
    registry = make(map[string]Func)
    mu       sync.RWMutex
)

// Register 注册距离函数
// 在各自的 init() 中调用
func Register(name string, fn Func) {
    mu.Lock()
    defer mu.Unlock()
    
    if _, exists := registry[name]; exists {
        panic(fmt.Sprintf("distance function %s already registered", name))
    }
    
    registry[name] = fn
}

// Get 获取距离函数
func Get(name string) (Func, error) {
    mu.RLock()
    defer mu.RUnlock()
    
    fn, ok := registry[name]
    if !ok {
        return nil, fmt.Errorf("unknown distance function: %s", name)
    }
    
    return fn, nil
}

// MustGet 获取距离函数（不存在则 panic）
func MustGet(name string) Func {
    fn, err := Get(name)
    if err != nil {
        panic(err)
    }
    return fn
}

// ListAll 列出所有可用的距离函数
func ListAll() []string {
    mu.RLock()
    defer mu.RUnlock()
    
    names := make([]string, 0, len(registry))
    for name := range registry {
        names = append(names, name)
    }
    return names
}

// Exists 检查距离函数是否存在
func Exists(name string) bool {
    mu.RLock()
    defer mu.RUnlock()
    
    _, ok := registry[name]
    return ok
}
```

---

## 3. 索引类型实现（可插拔）

### 3.1 索引接口

**文件**: `pkg/dataaccess/index/types/interface.go`

```go
package types

import (
    "context"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/distance"
)

// VectorIndex 向量索引接口
type VectorIndex interface {
    // Add 添加向量
    Add(id int64, vector []float32) error
    
    // BatchAdd 批量添加向量
    BatchAdd(ids []int64, vectors [][]float32) error
    
    // Search 搜索最近邻
    // k: 返回最相似的 k 个结果
    // filter: 可选的 ID 过滤器
    Search(ctx context.Context, query []float32, k int, filter *SearchFilter) (*SearchResult, error)
    
    // Delete 删除向量
    Delete(id int64) error
    
    // GetStats 获取索引统计信息
    GetStats() Stats
    
    // Close 关闭索引
    Close() error
}

// SearchResult 搜索结果
type SearchResult struct {
    IDs       []int64
    Distances []float32
}

// SearchFilter 搜索过滤器
type SearchFilter struct {
    IDs []int64  // 允许的行ID列表，nil 表示不过滤
}

// Stats 索引统计
type Stats struct {
    Type         string
    DistanceType string
    Dimension    int
    Count        int64
    MemorySize   int64
}

// Config 索引配置
type Config struct {
    Dimension    int
    DistanceFunc distance.Func
    MetricType   string  // 用于标识
    
    // 类型特定参数（由具体索引类型定义）
    Params interface{}
}
```

### 3.2 HNSW 索引实现

**文件**: `pkg/dataaccess/index/types/hnsw/index.go`

```go
package hnsw

import (
    "context"
    "fmt"
    "math"
    "math/rand"
    "sync"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index"
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/distance"
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/types"
)

func init() {
    // 注册 HNSW 索引工厂
    index.Register(index.TypeHNSW, func(config *types.Config) (types.VectorIndex, error) {
        params, ok := config.Params.(*Params)
        if !ok {
            params = DefaultParams()
        }
        return New(config.Dimension, config.DistanceFunc, params), nil
    })
}

// HNSW HNSW 索引实现
type HNSW struct {
    dimension    int
    distanceFunc distance.Func
    params       *Params
    
    nodes        map[int64]*Node
    entryPoint   int64
    maxLevel     int
    
    mu           sync.RWMutex
}

// New 创建 HNSW 索引
func New(dimension int, distFn distance.Func, params *Params) *HNSW {
    if params == nil {
        params = DefaultParams()
    }
    
    return &HNSW{
        dimension:    dimension,
        distanceFunc: distFn,
        params:       params,
        nodes:        make(map[int64]*Node),
    }
}

// Add 添加向量
func (h *HNSW) Add(id int64, vector []float32) error {
    if len(vector) != h.dimension {
        return fmt.Errorf("dimension mismatch: expected %d, got %d", h.dimension, len(vector))
    }
    
    h.mu.Lock()
    defer h.mu.Unlock()
    
    level := h.randomLevel()
    node := NewNode(id, vector, level)
    
    if len(h.nodes) == 0 {
        h.nodes[id] = node
        h.entryPoint = id
        h.maxLevel = level
        return nil
    }
    
    // 构建连接...
    h.insertNode(node)
    
    return nil
}

// BatchAdd 批量添加
func (h *HNSW) BatchAdd(ids []int64, vectors [][]float32) error {
    // 批量优化实现...
    for i, id := range ids {
        if err := h.Add(id, vectors[i]); err != nil {
            return err
        }
    }
    return nil
}

// Search 搜索
func (h *HNSW) Search(
    ctx context.Context,
    query []float32,
    k int,
    filter *types.SearchFilter,
) (*types.SearchResult, error) {
    h.mu.RLock()
    defer h.mu.RUnlock()
    
    if len(h.nodes) == 0 {
        return &types.SearchResult{}, nil
    }
    
    // 实现搜索逻辑...
    return &types.SearchResult{}, nil
}

// Delete 删除
func (h *HNSW) Delete(id int64) error {
    h.mu.Lock()
    defer h.mu.Unlock()
    
    delete(h.nodes, id)
    return nil
}

// GetStats 获取统计
func (h *HNSW) GetStats() types.Stats {
    h.mu.RLock()
    defer h.mu.RUnlock()
    
    return types.Stats{
        Type:         "hnsw",
        DistanceType: h.distanceFunc.Name(),
        Dimension:    h.dimension,
        Count:        int64(len(h.nodes)),
    }
}

// Close 关闭
func (h *HNSW) Close() error {
    h.mu.Lock()
    defer h.mu.Unlock()
    
    h.nodes = nil
    return nil
}

// 辅助方法...
func (h *HNSW) randomLevel() int {
    level := 0
    for rand.Float64() < h.params.LevelProbability && level < h.params.MaxLevel {
        level++
    }
    return level
}

func (h *HNSW) insertNode(node *Node) {
    // 插入逻辑...
}
```

### 3.3 HNSW 参数

**文件**: `pkg/dataaccess/index/types/hnsw/params.go`

```go
package hnsw

// Params HNSW 参数
type Params struct {
    M                  int     // 每层最大邻居数
    EfConstruction     int     // 构建时的探索因子
    EfSearch          int     // 搜索时的探索因子
    MaxLevel          int     // 最大层数
    LevelProbability  float64 // 层概率
}

// DefaultParams 默认参数
func DefaultParams() *Params {
    return &Params{
        M:                 16,
        EfConstruction:    200,
        EfSearch:         64,
        MaxLevel:         16,
        LevelProbability: 0.5,
    }
}

// WithM 设置 M
func (p *Params) WithM(m int) *Params {
    p.M = m
    return p
}

// WithEfConstruction 设置 EfConstruction
func (p *Params) WithEfConstruction(ef int) *Params {
    p.EfConstruction = ef
    return p
}
```

### 3.4 HNSW 节点

**文件**: `pkg/dataaccess/index/types/hnsw/node.go`

```go
package hnsw

// Node HNSW 节点
type Node struct {
    ID        int64
    Vector    []float32
    Level     int
    Neighbors [][]int64  // 每层的邻居节点ID
}

// NewNode 创建节点
func NewNode(id int64, vector []float32, level int) *Node {
    return &Node{
        ID:        id,
        Vector:    vector,
        Level:     level,
        Neighbors: make([][]int64, level+1),
    }
}

// GetNeighbors 获取指定层的邻居
func (n *Node) GetNeighbors(level int) []int64 {
    if level < 0 || level >= len(n.Neighbors) {
        return nil
    }
    return n.Neighbors[level]
}

// AddNeighbor 添加邻居
func (n *Node) AddNeighbor(level int, neighborID int64) {
    if level < 0 || level >= len(n.Neighbors) {
        return
    }
    n.Neighbors[level] = append(n.Neighbors[level], neighborID)
}
```

---

## 4. 索引构建器

**文件**: `pkg/dataaccess/index/builder.go`

```go
package index

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/distance"
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/types"
)

// BuildRequest 构建请求
type BuildRequest struct {
    Type       IndexType
    Dimension  int
    Metric     string  // 距离度量名称
    Params     interface{}
    
    // 数据源
    DataLoader DataLoader
}

// DataLoader 数据加载接口
type DataLoader interface {
    // Load 加载数据
    // 返回：ID列表、向量列表
    Load(ctx context.Context) ([]int64, [][]float32, error)
    
    // Count 预估数据量
    Count() int64
}

// Builder 索引构建器
type Builder struct{}

// NewBuilder 创建构建器
func NewBuilder() *Builder {
    return &Builder{}
}

// Build 构建索引
func (b *Builder) Build(ctx context.Context, req *BuildRequest) (types.VectorIndex, error) {
    // 1. 获取距离函数
    distFn, err := distance.Get(req.Metric)
    if err != nil {
        return nil, fmt.Errorf("get distance function failed: %w", err)
    }
    
    // 2. 创建索引配置
    config := &types.Config{
        Dimension:    req.Dimension,
        DistanceFunc: distFn,
        MetricType:   req.Metric,
        Params:       req.Params,
    }
    
    // 3. 创建空索引
    idx, err := Create(req.Type, config)
    if err != nil {
        return nil, fmt.Errorf("create index failed: %w", err)
    }
    
    // 4. 加载数据
    ids, vectors, err := req.DataLoader.Load(ctx)
    if err != nil {
        return nil, fmt.Errorf("load data failed: %w", err)
    }
    
    // 5. 批量添加
    if err := idx.BatchAdd(ids, vectors); err != nil {
        return nil, fmt.Errorf("batch add failed: %w", err)
    }
    
    return idx, nil
}
```

---

## 5. 扩展 Service 支持向量搜索

**文件**: `pkg/dataaccess/vector_service.go`

```go
package dataaccess

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index"
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/types"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VectorService 向量搜索服务扩展
type VectorService struct {
    base      Service  // 基础数据访问服务
    idxManager *IndexManager
}

// VectorSearchOptions 向量搜索选项
type VectorSearchOptions struct {
    TableName    string
    ColumnName   string
    QueryVector  []float32
    K            int
    Metric       string           // 距离度量
    IndexType    index.IndexType  // 使用的索引类型（可选）
    Filter       *domain.Filter   // 预过滤条件（可选）
    ReturnDist   bool             // 是否返回距离
}

// VectorSearchResult 向量搜索结果
type VectorSearchResult struct {
    Rows      []domain.Row
    Distances []float32
    Total     int64
}

// NewVectorService 创建向量服务
func NewVectorService(base Service) *VectorService {
    return &VectorService{
        base:       base,
        idxManager: NewIndexManager(),
    }
}

// VectorSearch 向量搜索
func (vs *VectorService) VectorSearch(
    ctx context.Context,
    opts *VectorSearchOptions,
) (*VectorSearchResult, error) {
    // 1. 获取或创建索引
    idxKey := IndexKey{
        Table:  opts.TableName,
        Column: opts.ColumnName,
    }
    
    idx, err := vs.idxManager.GetOrCreate(ctx, idxKey, func() (*IndexMeta, error) {
        return vs.buildIndex(ctx, opts)
    })
    if err != nil {
        return nil, fmt.Errorf("get or create index failed: %w", err)
    }
    
    // 2. 执行搜索
    // 如果有预过滤，先获取候选ID
    var filter *types.SearchFilter
    if opts.Filter != nil {
        ids, err := vs.getFilteredIDs(ctx, opts.TableName, opts.Filter)
        if err != nil {
            return nil, err
        }
        filter = &types.SearchFilter{IDs: ids}
    }
    
    searchResult, err := idx.Search(ctx, opts.QueryVector, opts.K, filter)
    if err != nil {
        return nil, fmt.Errorf("vector search failed: %w", err)
    }
    
    // 3. 获取完整行数据
    rows, err := vs.getRowsByIDs(ctx, opts.TableName, searchResult.IDs)
    if err != nil {
        return nil, fmt.Errorf("get rows failed: %w", err)
    }
    
    return &VectorSearchResult{
        Rows:      rows,
        Distances: searchResult.Distances,
        Total:     int64(len(rows)),
    }, nil
}

// buildIndex 构建索引
func (vs *VectorService) buildIndex(
    ctx context.Context,
    opts *VectorSearchOptions,
) (*IndexMeta, error) {
    // 1. 获取表信息
    tableInfo, err := vs.base.GetTableInfo(ctx, opts.TableName)
    if err != nil {
        return nil, err
    }
    
    // 2. 查找向量列
    var vectorCol *domain.ColumnInfo
    for _, col := range tableInfo.Columns {
        if col.Name == opts.ColumnName {
            vectorCol = &col
            break
        }
    }
    if vectorCol == nil {
        return nil, fmt.Errorf("column not found: %s", opts.ColumnName)
    }
    
    // 3. 获取维度（从列类型或属性中解析）
    dimension := extractDimension(vectorCol)
    if dimension == 0 {
        return nil, fmt.Errorf("cannot determine vector dimension")
    }
    
    // 4. 创建数据加载器
    loader := &tableDataLoader{
        service:  vs.base,
        table:    opts.TableName,
        column:   opts.ColumnName,
    }
    
    // 5. 构建索引
    builder := index.NewBuilder()
    idx, err := builder.Build(ctx, &index.BuildRequest{
        Type:       opts.IndexType,
        Dimension:  dimension,
        Metric:     opts.Metric,
        Params:     nil, // 使用默认参数
        DataLoader: loader,
    })
    if err != nil {
        return nil, err
    }
    
    return &IndexMeta{
        Index:     idx,
        Dimension: dimension,
        Metric:    opts.Metric,
    }, nil
}

// getFilteredIDs 获取过滤后的行ID
func (vs *VectorService) getFilteredIDs(
    ctx context.Context,
    tableName string,
    filter *domain.Filter,
) ([]int64, error) {
    // 使用基础服务过滤数据
    rows, _, err := vs.base.Filter(ctx, tableName, *filter, 0, 0)
    if err != nil {
        return nil, err
    }
    
    // 提取ID（假设每行有 id 列或使用行号）
    ids := make([]int64, len(rows))
    for i, row := range rows {
        if id, ok := row["id"]; ok {
            ids[i] = convertToInt64(id)
        } else {
            ids[i] = int64(i)
        }
    }
    
    return ids, nil
}

// getRowsByIDs 根据ID获取行
func (vs *VectorService) getRowsByIDs(
    ctx context.Context,
    tableName string,
    ids []int64,
) ([]domain.Row, error) {
    rows := make([]domain.Row, 0, len(ids))
    
    for _, id := range ids {
        // 使用 Filter 查询单行
        filter := domain.Filter{
            Field:    "id",
            Operator: "=",
            Value:    id,
        }
        
        result, _, err := vs.base.Filter(ctx, tableName, filter, 0, 1)
        if err != nil {
            continue
        }
        
        if len(result) > 0 {
            rows = append(rows, result[0])
        }
    }
    
    return rows, nil
}

// tableDataLoader 表数据加载器
type tableDataLoader struct {
    service Service
    table   string
    column  string
}

func (l *tableDataLoader) Load(ctx context.Context) ([]int64, [][]float32, error) {
    result, err := l.service.Query(ctx, l.table, &QueryOptions{
        SelectColumns: []string{l.column, "id"},
    })
    if err != nil {
        return nil, nil, err
    }
    
    ids := make([]int64, 0, len(result.Rows))
    vectors := make([][]float32, 0, len(result.Rows))
    
    for i, row := range result.Rows {
        vecValue, ok := row[l.column]
        if !ok {
            continue
        }
        
        vec, err := convertToFloatSlice(vecValue)
        if err != nil {
            continue
        }
        
        var id int64 = int64(i)
        if idValue, ok := row["id"]; ok {
            id = convertToInt64(idValue)
        }
        
        ids = append(ids, id)
        vectors = append(vectors, vec)
    }
    
    return ids, vectors, nil
}

func (l *tableDataLoader) Count() int64 {
    // 估算数据量
    return 0
}
```

---

## 6. 如何扩展（新增索引类型示例）

假设要新增 `DiskANN` 索引类型：

### 步骤 1: 创建目录和文件

```
pkg/dataaccess/index/types/diskann/
├── index.go    # 实现 VectorIndex 接口
├── params.go   # 参数定义
└── init.go     # 注册到 Registry
```

### 步骤 2: 实现索引

**文件**: `pkg/dataaccess/index/types/diskann/index.go`

```go
package diskann

import (
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index"
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/types"
)

// DiskANN DiskANN 索引
type DiskANN struct {
    // 实现...
}

// New 创建 DiskANN 索引
func New(dimension int, distFn distance.Func, params *Params) *DiskANN {
    // 实现...
}

// 实现 VectorIndex 接口的所有方法...
func (d *DiskANN) Add(id int64, vector []float32) error { return nil }
func (d *DiskANN) BatchAdd(ids []int64, vectors [][]float32) error { return nil }
func (d *DiskANN) Search(ctx context.Context, query []float32, k int, filter *types.SearchFilter) (*types.SearchResult, error) {
    return nil, nil
}
func (d *DiskANN) Delete(id int64) error { return nil }
func (d *DiskANN) GetStats() types.Stats { return types.Stats{} }
func (d *DiskANN) Close() error { return nil }
```

### 步骤 3: 注册（关键！）

**文件**: `pkg/dataaccess/index/types/diskann/init.go`

```go
package diskann

import (
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index"
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/types"
)

// 在包初始化时自动注册
func init() {
    index.Register("diskann", func(config *types.Config) (types.VectorIndex, error) {
        params, ok := config.Params.(*Params)
        if !ok {
            params = DefaultParams()
        }
        return New(config.Dimension, config.DistanceFunc, params), nil
    })
}
```

### 完成！

无需修改任何已有代码，新的 `DiskANN` 索引类型已经可用：

```go
idx, err := index.Create("diskann", &types.Config{
    Dimension:    768,
    DistanceFunc: distance.MustGet("cosine"),
    Params:       diskann.DefaultParams(),
})
```

---

## 7. 如何新增距离算法

**文件**: `pkg/dataaccess/index/distance/manhattan.go`

```go
package distance

func init() {
    Register("manhattan", &Manhattan{})
    Register("l1", &Manhattan{}) // 别名
}

// Manhattan 曼哈顿距离（L1范数）
type Manhattan struct{}

func (m *Manhattan) Name() string {
    return "manhattan"
}

func (m *Manhattan) Compute(v1, v2 []float32) float32 {
    var sum float64
    for i := 0; i < len(v1); i++ {
        sum += math.Abs(float64(v1[i] - v2[i]))
    }
    return float32(sum)
}

func (m *Manhattan) IsSimilarity() bool {
    return false
}
```

无需修改其他文件，新的距离函数立即可用。

---

## 8. 总结

### 核心优势

| 特性 | 说明 |
|------|------|
| **开闭原则** | 新增索引类型/距离算法只添加文件，不修改旧代码 |
| **模块化** | 每个距离算法、索引类型都是独立模块 |
| **复用性** | 复用 `pkg/dataaccess` 基础设施 |
| **可测试** | 每个模块可独立测试 |
| **易扩展** | 通过 `init()` 注册新组件 |

### 文件结构

```
pkg/dataaccess/
├── service.go                    # 现有
├── vector_service.go             # 新增：向量搜索扩展
│
└── index/                        # 新增：索引子包
    ├── registry.go               # 索引类型注册中心
    ├── builder.go                # 索引构建器
    ├── manager.go                # 索引管理器
    │
    ├── distance/                 # 距离计算模块
    │   ├── interface.go          # 接口
    │   ├── registry.go           # 注册中心
    │   ├── cosine.go             # 余弦（独立文件）
    │   ├── l2.go                 # 欧氏（独立文件）
    │   ├── inner_product.go      # 内积（独立文件）
    │   └── ...                   # 新增算法只需加文件
    │
    └── types/                    # 索引类型模块
        ├── interface.go          # 接口
        ├── hnsw/                 # HNSW（独立目录）
        ├── ivf/                  # IVF（独立目录）
        ├── flat/                 # Flat（独立目录）
        └── ...                   # 新增类型只需加目录
```

### 使用方式

```go
// 1. 自动注册所有索引类型和距离函数（通过 init）
import _ "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/types/hnsw"
import _ "github.com/kasuganosora/sqlexec/pkg/dataaccess/index/types/ivf"

// 2. 创建向量服务
baseService := dataaccess.NewDataService(dataSource)
vectorService := dataaccess.NewVectorService(baseService)

// 3. 执行向量搜索
result, err := vectorService.VectorSearch(ctx, &dataaccess.VectorSearchOptions{
    TableName:   "articles",
    ColumnName:  "embedding",
    QueryVector: []float32{0.1, 0.2, ...},
    K:           10,
    Metric:      "cosine",
    IndexType:   "hnsw",
})
```
