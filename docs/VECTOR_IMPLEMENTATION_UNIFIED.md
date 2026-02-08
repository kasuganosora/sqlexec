# SQLExec 向量搜索统一实施计划（修正版）

## 问题修正声明

本版本修正以下 P0 级问题：
1. ✅ 统一类型定义（单一定义源）
2. ✅ 索引接口与现有 `Index` 接口统一
3. ✅ 距离函数单一实现
4. ✅ 完整成本模型集成
5. ✅ 完整物理计划定义

---

## 1. 统一类型定义

### 1.1 扩展 Domain 类型（唯一定义源）

**文件**: `pkg/resource/domain/types_index.go`（新增）

```go
package domain

// IndexType 统一索引类型（扩展自 models.go 中的定义）
type IndexType string

const (
    // 传统索引类型
    IndexTypeBTree    IndexType = "btree"
    IndexTypeHash     IndexType = "hash"
    IndexTypeFullText IndexType = "fulltext"
    
    // ========== 新增：向量索引类型 ==========
    IndexTypeVectorHNSW    IndexType = "vector_hnsw"
    IndexTypeVectorIVFFlat IndexType = "vector_ivf_flat"
    IndexTypeVectorFlat    IndexType = "vector_flat"
)

// IsVectorIndex 检查是否为向量索引
func (t IndexType) IsVectorIndex() bool {
    switch t {
    case IndexTypeVectorHNSW, IndexTypeVectorIVFFlat, IndexTypeVectorFlat:
        return true
    default:
        return false
    }
}

// VectorMetricType 向量距离度量类型
type VectorMetricType string

const (
    VectorMetricCosine  VectorMetricType = "cosine"
    VectorMetricL2      VectorMetricType = "l2"
    VectorMetricIP      VectorMetricType = "inner_product"
    VectorMetricHamming VectorMetricType = "hamming"
)

// VectorDataType 向量数据类型
type VectorDataType string

const (
    VectorDataTypeFloat32  VectorDataType = "float32"
    VectorDataTypeFloat16  VectorDataType = "float16"
    VectorDataTypeBFloat16 VectorDataType = "bfloat16"
    VectorDataTypeInt8     VectorDataType = "int8"
    VectorDataTypeBinary   VectorDataType = "binary"
)
```

### 1.2 扩展 ColumnInfo（修改 models.go）

**文件**: `pkg/resource/domain/models.go`

```go
// ColumnInfo 在原有基础上扩展
type ColumnInfo struct {
    Name         string           `json:"name"`
    Type         string           `json:"type"`
    Nullable     bool             `json:"nullable"`
    Primary      bool             `json:"primary"`
    Default      string           `json:"default,omitempty"`
    Unique       bool             `json:"unique,omitempty"`
    AutoIncrement bool            `json:"auto_increment,omitempty"`
    ForeignKey   *ForeignKeyInfo  `json:"foreign_key,omitempty"`
    IsGenerated  bool             `json:"is_generated,omitempty"`
    GeneratedType string          `json:"generated_type,omitempty"`
    GeneratedExpr string          `json:"generated_expr,omitempty"`
    GeneratedDepends []string     `json:"generated_depends,omitempty"`
    
    // ========== 新增：向量类型字段 ==========
    VectorDim    int              `json:"vector_dim,omitempty"`
    VectorType   VectorDataType   `json:"vector_type,omitempty"`
}

// IsVectorType 检查是否为向量列
func (c ColumnInfo) IsVectorType() bool {
    return c.VectorDim > 0
}
```

### 1.3 扩展 Index 定义（修改 models.go）

```go
// Index 在原有基础上扩展
type Index struct {
    Name     string      `json:"name"`
    Table    string      `json:"table"`
    Columns  []string    `json:"columns"`
    Type     IndexType   `json:"type"`
    Unique   bool        `json:"unique"`
    Primary  bool        `json:"primary"`
    Enabled  bool        `json:"enabled"`
    
    // ========== 新增：向量索引扩展信息 ==========
    VectorConfig *VectorIndexConfig `json:"vector_config,omitempty"`
}

// VectorIndexConfig 向量索引配置
type VectorIndexConfig struct {
    MetricType VectorMetricType       `json:"metric_type"`
    Dimension  int                    `json:"dimension"`
    Params     map[string]interface{} `json:"params,omitempty"`
}
```

---

## 2. 统一索引接口（继承现有 Index 接口）

### 2.1 现有 Index 接口回顾

**文件**: `pkg/resource/memory/index.go`

```go
// 现有接口 - 保持不变
type Index interface {
    Insert(key interface{}, rowIDs []int64) error
    Delete(key interface{}) error
    Find(key interface{}) ([]int64, bool)
    FindRange(min, max interface{}) ([]int64, error)
    GetIndexInfo() *IndexInfo
}
```

### 2.2 向量索引扩展接口

**文件**: `pkg/resource/index/vector.go`（新增，与 memory/index 平级）

```go
package index

import (
    "context"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VectorIndex 向量索引接口
// 独立于传统 Index 接口，但在 IndexManager 中统一管理
type VectorIndex interface {
    // Build 构建索引（从数据源加载数据）
    Build(ctx context.Context, loader DataLoader) error
    
    // Search 近似最近邻搜索
    // query: []float32
    // k: 返回最相似的 k 个结果
    // filter: 可选的行ID过滤
    Search(ctx context.Context, query []float32, k int, filter RowIDFilter) (*SearchResult, error)
    
    // Insert 增量插入（单条）
    Insert(id int64, vector []float32) error
    
    // Delete 删除
    Delete(id int64) error
    
    // GetConfig 获取索引配置
    GetConfig() *domain.VectorIndexConfig
    
    // Stats 获取统计信息
    Stats() Stats
    
    // Close 关闭索引
    Close() error
}

// DataLoader 数据加载接口
type DataLoader interface {
    Load(ctx context.Context) ([]VectorRecord, error)
    Count() int64
}

// VectorRecord 向量记录
type VectorRecord struct {
    ID     int64
    Vector []float32
}

// RowIDFilter 行ID过滤器
type RowIDFilter struct {
    IDs []int64  // nil 表示不过滤
}

// SearchResult 搜索结果
type SearchResult struct {
    IDs       []int64
    Distances []float32
}

// Stats 统计信息
type Stats struct {
    Type       domain.IndexType
    Metric     domain.VectorMetricType
    Dimension  int
    Count      int64
    MemorySize int64
}
```

---

## 3. 统一距离函数（单一实现）

### 3.1 距离函数接口与注册

**文件**: `pkg/resource/index/distance/distance.go`（唯一定义源）

```go
package distance

import (
    "fmt"
    "math"
    "sync"
)

// Function 距离函数接口
type Function interface {
    Name() string
    Compute(v1, v2 []float32) float32
}

// Registry 全局注册中心
var registry = &Registry{
    functions: make(map[string]Function),
}

type Registry struct {
    mu        sync.RWMutex
    functions map[string]Function
}

// Register 注册距离函数
func Register(name string, fn Function) {
    registry.mu.Lock()
    defer registry.mu.Unlock()
    
    if _, exists := registry.functions[name]; exists {
        panic(fmt.Sprintf("distance function %s already registered", name))
    }
    registry.functions[name] = fn
}

// Get 获取距离函数
func Get(name string) (Function, error) {
    registry.mu.RLock()
    defer registry.mu.RUnlock()
    
    fn, ok := registry.functions[name]
    if !ok {
        return nil, fmt.Errorf("unknown distance function: %s", name)
    }
    return fn, nil
}

// MustGet 必须获取
func MustGet(name string) Function {
    fn, err := Get(name)
    if err != nil {
        panic(err)
    }
    return fn
}

// Cosine 余弦距离
func Cosine(v1, v2 []float32) float32 {
    return MustGet("cosine").Compute(v1, v2)
}

// L2 欧氏距离
func L2(v1, v2 []float32) float32 {
    return MustGet("l2").Compute(v1, v2)
}

// InnerProduct 内积（返回负值，使越大越相似转为越小越相似）
func InnerProduct(v1, v2 []float32) float32 {
    return MustGet("inner_product").Compute(v1, v2)
}
```

### 3.2 具体实现（独立文件）

**文件**: `pkg/resource/index/distance/cosine.go`

```go
package distance

import "math"

func init() {
    Register("cosine", cosine{})
}

type cosine struct{}

func (c cosine) Name() string { return "cosine" }

func (c cosine) Compute(v1, v2 []float32) float32 {
    var dot, norm1, norm2 float64
    for i := 0; i < len(v1); i++ {
        dot += float64(v1[i] * v2[i])
        norm1 += float64(v1[i] * v1[i])
        norm2 += float64(v2[i] * v2[i])
    }
    
    if norm1 == 0 || norm2 == 0 {
        return 1.0
    }
    
    similarity := dot / (math.Sqrt(norm1) * math.Sqrt(norm2))
    return float32(1.0 - similarity)
}
```

**文件**: `pkg/resource/index/distance/l2.go`

```go
package distance

import "math"

func init() {
    Register("l2", l2{})
}

type l2 struct{}

func (l l2) Name() string { return "l2" }

func (l l2) Compute(v1, v2 []float32) float32 {
    var sum float64
    for i := 0; i < len(v1); i++ {
        diff := float64(v1[i] - v2[i])
        sum += diff * diff
    }
    return float32(math.Sqrt(sum))
}
```

**文件**: `pkg/resource/index/distance/ip.go`

```go
package distance

func init() {
    Register("inner_product", ip{})
}

type ip struct{}

func (i ip) Name() string { return "inner_product" }

func (i ip) Compute(v1, v2 []float32) float32 {
    var sum float32
    for j := 0; j < len(v1); j++ {
        sum += v1[j] * v2[j]
    }
    return -sum  // 取负转为距离
}
```

### 3.3 SQL 函数注册（复用 distance 包）

**文件**: `pkg/builtin/vector_functions.go`（复用，不重复实现）

```go
package builtin

import (
    "github.com/kasuganosora/sqlexec/pkg/resource/index/distance"
)

func init() {
    // VEC_COSINE_DISTANCE - 复用 distance 包
    RegisterGlobal(&FunctionInfo{
        Name: "VEC_COSINE_DISTANCE",
        Type: FunctionTypeScalar,
        Signatures: []FunctionSignature{
            {ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
        },
        Handler: func(args []interface{}) (interface{}, error) {
            v1 := toFloatSlice(args[0])
            v2 := toFloatSlice(args[1])
            return distance.Cosine(v1, v2), nil
        },
        Category: "vector",
    })
    
    // VEC_L2_DISTANCE - 复用 distance 包
    RegisterGlobal(&FunctionInfo{
        Name: "VEC_L2_DISTANCE",
        Type: FunctionTypeScalar,
        Signatures: []FunctionSignature{
            {ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
        },
        Handler: func(args []interface{}) (interface{}, error) {
            v1 := toFloatSlice(args[0])
            v2 := toFloatSlice(args[1])
            return distance.L2(v1, v2), nil
        },
        Category: "vector",
    })
    
    // VEC_INNER_PRODUCT - 复用 distance 包
    RegisterGlobal(&FunctionInfo{
        Name: "VEC_INNER_PRODUCT",
        Type: FunctionTypeScalar,
        Signatures: []FunctionSignature{
            {ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
        },
        Handler: func(args []interface{}) (interface{}, error) {
            v1 := toFloatSlice(args[0])
            v2 := toFloatSlice(args[1])
            return distance.InnerProduct(v1, v2), nil
        },
        Category: "vector",
    })
}

func toFloatSlice(v interface{}) []float32 {
    // 类型转换逻辑...
    switch val := v.(type) {
    case []float32:
        return val
    case []float64:
        result := make([]float32, len(val))
        for i, f := range val {
            result[i] = float32(f)
        }
        return result
    // ... 其他类型
    default:
        return nil
    }
}
```

---

## 4. 成本模型集成（完整实现）

### 4.1 扩展 CostModel 接口

**文件**: `pkg/optimizer/cost/interfaces.go`（扩展）

```go
package cost

import "github.com/kasuganosora/sqlexec/pkg/resource/domain"

// CostModel 扩展现有接口
type CostModel interface {
    // 现有方法
    ScanCost(tableName string, rowCount int64, useIndex bool) float64
    FilterCost(inputRows int64, selectivity float64, filters []interface{}) float64
    JoinCost(left, right interface{}, joinType JoinType, conditions []*parser.Expression) float64
    AggregateCost(inputRows int64, groupByCols, aggFuncs int) float64
    ProjectCost(inputRows int64, projCols int) float64
    SortCost(inputRows int64, sortCols int) float64
    
    // ========== 新增：向量搜索成本 ==========
    VectorSearchCost(indexType domain.IndexType, rowCount int64, k int) float64
}
```

### 4.2 成本模型实现

**文件**: `pkg/optimizer/cost/adaptive_model.go`（扩展）

```go
package cost

import "github.com/kasuganosora/sqlexec/pkg/resource/domain"

// AdaptiveCostModel 扩展实现
func (m *AdaptiveCostModel) VectorSearchCost(
    indexType domain.IndexType,
    rowCount int64,
    k int,
) float64 {
    // 基础成本
    baseCost := float64(k) * 10
    
    // 根据索引类型调整
    switch indexType {
    case domain.IndexTypeVectorHNSW:
        // HNSW: O(log N) 复杂度
        logFactor := math.Log10(float64(rowCount)+1) * 2
        return baseCost * logFactor
        
    case domain.IndexTypeVectorIVFFlat:
        // IVF: O(N/nlist) 复杂度
        nlist := 100.0  // 默认聚类数
        scanFactor := float64(rowCount) / nlist / 1000
        return baseCost * math.Max(1, scanFactor)
        
    case domain.IndexTypeVectorFlat:
        // 暴力搜索: O(N)
        scanFactor := float64(rowCount) / 1000
        return baseCost * math.Max(1, scanFactor)
        
    default:
        return baseCost
    }
}
```

### 4.3 向量搜索成本估算

**文件**: `pkg/optimizer/vector_cost.go`（新增）

```go
package optimizer

import (
    "github.com/kasuganosora/sqlexec/pkg/optimizer/cost"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// EstimateVectorScanCost 估算向量扫描成本
func EstimateVectorScanCost(
    costModel cost.CostModel,
    tableInfo *domain.TableInfo,
    indexType domain.IndexType,
    k int,
) float64 {
    // 估算行数
    rowCount := estimateRowCount(tableInfo)
    
    // 使用成本模型
    return costModel.VectorSearchCost(indexType, rowCount, k)
}

// estimateRowCount 估算表行数
func estimateRowCount(tableInfo *domain.TableInfo) int64 {
    // 从统计信息或元数据获取
    if stats := tableInfo.Stats; stats != nil {
        return stats.RowCount
    }
    return 10000  // 默认值
}
```

---

## 5. 物理计划完整定义

### 5.1 向量搜索物理算子

**文件**: `pkg/optimizer/physical/vector_scan.go`（新增）

```go
package physical

import (
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/optimizer"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VectorScan 向量扫描物理算子
type VectorScan struct {
    basePhysicalOperator
    
    TableName   string
    ColumnName  string
    QueryVector []float32
    K           int
    Metric      domain.VectorMetricType
    IndexType   domain.IndexType
    
    estimatedCost float64
}

// NewVectorScan 创建向量扫描算子
func NewVectorScan(
    tableName, columnName string,
    queryVector []float32,
    k int,
    metric domain.VectorMetricType,
    indexType domain.IndexType,
) *VectorScan {
    return &VectorScan{
        TableName:   tableName,
        ColumnName:  columnName,
        QueryVector: queryVector,
        K:           k,
        Metric:      metric,
        IndexType:   indexType,
    }
}

// Cost 返回执行成本
func (v *VectorScan) Cost() float64 {
    return v.estimatedCost
}

// SetCost 设置成本（由优化器计算）
func (v *VectorScan) SetCost(cost float64) {
    v.estimatedCost = cost
}

// Explain 返回计划说明
func (v *VectorScan) Explain() string {
    return fmt.Sprintf("VectorScan[%s.%s, %s, k=%d, cost=%.2f]",
        v.TableName, v.ColumnName, v.Metric, v.K, v.estimatedCost)
}

// Schema 返回输出列
func (v *VectorScan) Schema() []optimizer.ColumnInfo {
    return []optimizer.ColumnInfo{
        {Name: "id", Type: "BIGINT"},
        {Name: "distance", Type: "FLOAT"},
    }
}
```

### 5.2 物理计划配置

**文件**: `pkg/optimizer/plan/vector.go`（新增）

```go
package plan

import "github.com/kasuganosora/sqlexec/pkg/resource/domain"

// VectorScanConfig 向量扫描配置
type VectorScanConfig struct {
    TableName   string
    ColumnName  string
    QueryVector []float32
    K           int
    Metric      domain.VectorMetricType
    IndexType   domain.IndexType
    Filter      *FilterConfig  // 可选的过滤条件
}

// FilterConfig 过滤配置
type FilterConfig struct {
    Column   string
    Operator string
    Value    interface{}
}

// PlanType 扩展
const (
    // 现有类型...
    TypeVectorScan PlanType = "VectorScan"
)
```

---

## 6. 执行器算子实现

### 6.1 向量搜索执行算子

**文件**: `pkg/executor/operators/vector_scan.go`（新增）

```go
package operators

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/index"
)

// VectorScanOperator 向量扫描算子
type VectorScanOperator struct {
    *BaseOperator
    config    *plan.VectorScanConfig
    idxMgr    *index.Manager  // 通过依赖注入获取
}

// NewVectorScanOperator 创建算子
func NewVectorScanOperator(
    p *plan.Plan,
    das dataaccess.Service,
    idxMgr *index.Manager,
) (*VectorScanOperator, error) {
    config, ok := p.Config.(*plan.VectorScanConfig)
    if !ok {
        return nil, fmt.Errorf("invalid config type")
    }
    
    return &VectorScanOperator{
        BaseOperator: NewBaseOperator(p, das),
        config:       config,
        idxMgr:       idxMgr,
    }, nil
}

// Execute 执行向量搜索
func (v *VectorScanOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
    fmt.Printf("[EXECUTOR] VectorScan: table=%s, column=%s, k=%d\n",
        v.config.TableName, v.config.ColumnName, v.config.K)
    
    // 1. 获取向量索引
    idxKey := index.Key{
        Table:  v.config.TableName,
        Column: v.config.ColumnName,
    }
    
    vectorIdx, err := v.idxMgr.GetOrBuild(ctx, idxKey, v.buildIndex)
    if err != nil {
        return nil, fmt.Errorf("get or build index failed: %w", err)
    }
    
    // 2. 执行搜索
    result, err := vectorIdx.Search(ctx, v.config.QueryVector, v.config.K, nil)
    if err != nil {
        return nil, fmt.Errorf("vector search failed: %w", err)
    }
    
    // 3. 获取完整行数据
    rows, err := v.fetchRowsByIDs(ctx, result.IDs)
    if err != nil {
        return nil, fmt.Errorf("fetch rows failed: %w", err)
    }
    
    // 4. 添加距离列
    for i, row := range rows {
        row["_distance"] = result.Distances[i]
    }
    
    return &domain.QueryResult{
        Rows: rows,
    }, nil
}

// buildIndex 构建索引回调
func (v *VectorScanOperator) buildIndex(ctx context.Context) (index.VectorIndex, error) {
    // 从数据源加载数据并构建索引
    // 具体实现...
    return nil, nil
}

// fetchRowsByIDs 根据ID获取行
func (v *VectorScanOperator) fetchRowsByIDs(ctx context.Context, ids []int64) ([]domain.Row, error) {
    rows := make([]domain.Row, 0, len(ids))
    
    for _, id := range ids {
        // 使用 DataService 查询单行
        filter := domain.Filter{
            Field:    "id",
            Operator: "=",
            Value:    id,
        }
        
        result, _, err := v.dataAccessService.Filter(ctx, v.config.TableName, filter, 0, 1)
        if err != nil || len(result) == 0 {
            continue
        }
        
        rows = append(rows, result[0])
    }
    
    return rows, nil
}
```

---

## 7. 清晰的实施阶段

### 阶段 1: 基础类型（Week 1）
- [x] 统一类型定义（`domain/types_index.go`）
- [x] 扩展 `ColumnInfo` 和 `Index`
- [ ] 单元测试

### 阶段 2: 距离函数（Week 1-2）
- [ ] 创建 `pkg/resource/index/distance/` 目录
- [ ] 实现 cosine、l2、inner_product
- [ ] 注册 SQL 函数
- [ ] 单元测试

### 阶段 3: 向量索引接口（Week 2-3）
- [ ] 定义 `VectorIndex` 接口
- [ ] 实现 HNSW 索引
- [ ] 实现 Flat 索引（暴力搜索）
- [ ] 单元测试

### 阶段 4: 索引管理器（Week 3-4）
- [ ] 实现 `IndexManager`
- [ ] 数据源加载集成
- [ ] 缓存和版本管理
- [ ] 集成测试

### 阶段 5: 成本模型（Week 4-5）
- [ ] 扩展 `CostModel` 接口
- [ ] 实现 `VectorSearchCost`
- [ ] 成本估算测试

### 阶段 6: 优化器集成（Week 5-6）
- [ ] 实现 `VectorScan` 物理算子
- [ ] 实现优化规则（Sort+Limit → VectorScan）
- [ ] 计划生成测试

### 阶段 7: 执行器集成（Week 6-7）
- [ ] 实现 `VectorScanOperator`
- [ ] 依赖注入（IndexManager）
- [ ] 端到端测试

### 阶段 8: SQL 解析（Week 7-8）
- [ ] 扩展 Parser 支持 VECTOR 类型
- [ ] 扩展 Parser 支持向量索引语法
- [ ] 集成测试

### 阶段 9: 完整测试（Week 8-10）
- [ ] 单元测试覆盖率 > 80%
- [ ] 性能测试
- [ ] 文档完善

---

## 8. 文件依赖图

```
pkg/resource/domain/
├── models.go                    # 修改：扩展 ColumnInfo, Index
└── types_index.go               # 新增：统一类型定义

pkg/resource/index/
├── distance/
│   ├── distance.go              # 新增：接口与注册中心
│   ├── cosine.go                # 新增
│   ├── l2.go                    # 新增
│   └── ip.go                    # 新增
│
├── types/
│   ├── hnsw.go                  # 新增：HNSW 实现
│   └── flat.go                  # 新增：暴力搜索实现
│
├── interface.go                 # 新增：VectorIndex 接口
├── manager.go                   # 新增：索引管理器
└── registry.go                  # 新增：索引类型注册

pkg/builtin/
└── vector_functions.go          # 新增：复用 distance 包

pkg/optimizer/cost/
├── interfaces.go                # 修改：添加 VectorSearchCost
└── adaptive_model.go            # 修改：实现成本计算

pkg/optimizer/physical/
└── vector_scan.go               # 新增：物理算子

pkg/optimizer/plan/
└── vector.go                    # 新增：VectorScanConfig

pkg/executor/operators/
└── vector_scan.go               # 新增：执行算子
```

---

## 9. 关键集成点

### 9.1 与现有 DataAccess 集成

```go
// pkg/dataaccess/service.go 扩展

// Service 扩展接口
type Service interface {
    // 现有方法...
    Query(ctx context.Context, tableName string, options *QueryOptions) (*domain.QueryResult, error)
    Filter(ctx context.Context, tableName string, filter domain.Filter, offset, limit int) ([]domain.Row, int64, error)
    
    // 新增：向量搜索方法
    VectorSearch(ctx context.Context, opts *VectorSearchOptions) (*VectorSearchResult, error)
}
```

### 9.2 与现有 Optimizer 集成

```go
// pkg/optimizer/enhanced_optimizer.go 扩展

// EnhancedOptimizer 扩展
func (o *EnhancedOptimizer) optimizeVectorSearch(plan LogicalPlan) (LogicalPlan, error) {
    // 检测向量搜索模式
    // 应用 VectorScan 优化
}
```

### 9.3 与现有 Executor 集成

```go
// pkg/executor/executor.go 扩展

// 注册向量搜索算子
func init() {
    RegisterOperator(plan.TypeVectorScan, func(p *plan.Plan, das dataaccess.Service) (Operator, error) {
        return operators.NewVectorScanOperator(p, das, globalIndexManager)
    })
}
```

---

## 10. 验收标准

### 功能验收
- [ ] `CREATE TABLE t (v VECTOR(768))` 成功
- [ ] `CREATE VECTOR INDEX idx ON t(v) USING HNSW` 成功
- [ ] `SELECT * FROM t ORDER BY VEC_COSINE_DISTANCE(v, [...]) LIMIT 10` 成功
- [ ] 支持 CSV、Memory、MySQL 数据源

### 代码质量
- [ ] 类型定义单一来源（domain 包）
- [ ] 距离函数单一实现（index/distance 包）
- [ ] 新增索引类型零侵入（通过 Register）
- [ ] 测试覆盖率 > 80%

### 性能验收
- [ ] 100万向量 HNSW 查询 P99 < 10ms
- [ ] 召回率 > 95%
- [ ] 索引构建速度 > 5000 向量/秒
