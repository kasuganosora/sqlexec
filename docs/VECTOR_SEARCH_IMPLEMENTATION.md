# SQLExec 向量搜索完整实施文档

## 目录
1. [架构概述](#1-架构概述)
2. [实施阶段](#2-实施阶段)
3. [详细设计](#3-详细设计)
4. [代码实现](#4-代码实现)
5. [测试验收](#5-测试验收)

---

## 1. 架构概述

### 1.1 设计原则

- **复用现有层**: 扩展 `pkg/dataaccess` 和 `pkg/resource/memory`，不创建新目录
- **开闭原则**: 新增索引类型/距离算法只添加代码，不修改现有逻辑
- **类型统一**: 所有类型定义在现有文件中扩展，避免重复定义
- **接口一致**: 完全匹配现有接口签名

### 1.2 架构图

```
┌─────────────────────────────────────────────────────────────┐
│  SQL Layer (Parser)                                         │
│  CREATE TABLE (VECTOR) / CREATE VECTOR INDEX                │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│  Optimizer Layer                                            │
│  VectorIndexRule → VectorScan Plan → CostModel              │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│  Executor Layer                                             │
│  VectorScanOperator → IndexManager → VectorIndex            │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│  Resource Layer (pkg/resource/memory/)                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ HNSW Index  │  │ Flat Index  │  │ Distance    │         │
│  │ (新增)      │  │ (新增)      │  │ (新增)      │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│  IndexManager (扩展支持向量索引)                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│  DataAccess Layer                                           │
│  CSV / JSON / Memory / MySQL / API                          │
└─────────────────────────────────────────────────────────────┘
```

### 1.3 文件变更总览

**修改现有文件（7个）**:
| 文件 | 变更 |
|------|------|
| `pkg/resource/memory/index.go` | 扩展 IndexType，添加向量类型常量 |
| `pkg/resource/memory/index_manager.go` | 添加 vectorIndexes 映射和方法 |
| `pkg/resource/domain/models.go` | 扩展 ColumnInfo（VectorDim/VectorType）|
| `pkg/optimizer/cost/interfaces.go` | 添加 VectorSearchCost 方法 |
| `pkg/optimizer/cost/adaptive_model.go` | 实现 VectorSearchCost |
| `pkg/optimizer/plan/types.go` | 添加 TypeVectorScan |
| `pkg/executor/executor.go` | 修改 buildOperator 和 BaseExecutor |

**新增文件（6个）**:
| 文件 | 说明 |
|------|------|
| `pkg/resource/memory/distance.go` | 距离函数注册中心和实现 |
| `pkg/resource/memory/vector_index.go` | VectorIndex 接口定义 |
| `pkg/resource/memory/hnsw_index.go` | HNSW 索引实现 |
| `pkg/resource/memory/flat_index.go` | 暴力搜索实现 |
| `pkg/optimizer/plan/vector_scan.go` | VectorScanConfig |
| `pkg/executor/operators/vector_scan.go` | 向量扫描执行算子 |

---

## 2. 实施阶段

### Phase 1: 基础类型扩展（Week 1）

**目标**: 扩展类型定义，确保编译通过

**任务清单**:
- [ ] 修改 `pkg/resource/memory/index.go` - 添加 IndexTypeVectorHNSW 等常量
- [ ] 修改 `pkg/resource/domain/models.go` - 扩展 ColumnInfo 和 Index
- [ ] 运行 `go build ./pkg/resource/...` 验证

**关键代码**:
```go
// pkg/resource/memory/index.go
const (
    IndexTypeBTree    IndexType = "btree"
    IndexTypeHash     IndexType = "hash"
    IndexTypeFullText IndexType = "fulltext"
    // 新增
    IndexTypeVectorHNSW    IndexType = "hnsw"
    IndexTypeVectorIVFFlat IndexType = "ivf_flat"
    IndexTypeVectorFlat    IndexType = "flat"
)
```

### Phase 2: 距离函数实现（Week 1-2）

**目标**: 实现模块化距离计算

**任务清单**:
- [ ] 创建 `pkg/resource/memory/distance.go`
- [ ] 实现 Cosine、L2、InnerProduct
- [ ] 注册到全局注册中心
- [ ] 单元测试

**关键代码**:
```go
// pkg/resource/memory/distance.go
func init() {
    RegisterDistance("cosine", &cosineDistance{})
    RegisterDistance("l2", &l2Distance{})
    RegisterDistance("inner_product", &innerProductDistance{})
}
```

### Phase 3: 向量索引接口与实现（Week 2-3）

**目标**: 实现 HNSW 和 Flat 索引

**任务清单**:
- [ ] 创建 `pkg/resource/memory/vector_index.go` - 接口定义
- [ ] 创建 `pkg/resource/memory/hnsw_index.go` - HNSW 实现
- [ ] 创建 `pkg/resource/memory/flat_index.go` - 暴力搜索实现
- [ ] 扩展 `pkg/resource/memory/index_manager.go` - 支持向量索引
- [ ] 集成测试

### Phase 4: 成本模型扩展（Week 3-4）

**目标**: 优化器支持向量搜索成本估算

**任务清单**:
- [ ] 修改 `pkg/optimizer/cost/interfaces.go` - 添加 VectorSearchCost
- [ ] 修改 `pkg/optimizer/cost/adaptive_model.go` - 实现成本计算
- [ ] 添加 `pkg/optimizer/plan/vector_scan.go` - VectorScanConfig
- [ ] 修改 `pkg/optimizer/plan/types.go` - 添加 TypeVectorScan

**关键代码**:
```go
// pkg/optimizer/cost/interfaces.go
type CostModel interface {
    // 现有方法...
    VectorSearchCost(indexType IndexType, rowCount int64, k int) float64
}

// pkg/optimizer/plan/types.go
const (
    TypeTableScan  PlanType = "TableScan"
    TypeHashJoin   PlanType = "HashJoin"
    TypeSort       PlanType = "Sort"
    TypeAggregate  PlanType = "Aggregate"
    TypeProjection PlanType = "Projection"
    TypeSelection  PlanType = "Selection"
    TypeLimit      PlanType = "Limit"
    TypeInsert     PlanType = "Insert"
    TypeUpdate     PlanType = "Update"
    TypeDelete     PlanType = "Delete"
    TypeUnion      PlanType = "Union"
    // ========== 新增 ==========
    TypeVectorScan PlanType = "VectorScan"
)
```

### Phase 5: 执行器集成（Week 4-5）

**目标**: 实现向量搜索执行算子

**任务清单**:
- [ ] 创建 `pkg/executor/operators/vector_scan.go`
- [ ] 修改 `pkg/executor/executor.go` - 添加 switch-case
- [ ] 扩展 BaseExecutor 添加 IndexManager
- [ ] 端到端测试

**关键代码**:
```go
// pkg/executor/executor.go
func (e *BaseExecutor) buildOperator(p *plan.Plan) (operators.Operator, error) {
    switch p.Type {
    // ... 现有 case ...
    case plan.TypeVectorScan:
        return operators.NewVectorScanOperator(p, e.dataAccessService, e.indexManager)
    }
}
```

### Phase 6: SQL 解析扩展（Week 5-6）

**目标**: 支持 VECTOR 类型和向量索引语法

**任务清单**:
- [ ] 修改 `pkg/parser/adapter.go` - 解析 VECTOR 类型
- [ ] 修改 `pkg/parser/types.go` - 添加向量语句类型
- [ ] 添加内置函数 VEC_COSINE_DISTANCE 等
- [ ] 集成测试

### Phase 7: 优化器规则（Week 6-7）

**目标**: 自动识别向量搜索并优化

**任务清单**:
- [ ] 创建 `pkg/optimizer/rules_vector.go`
- [ ] 实现 VectorIndexRule（Sort+Limit → VectorScan）
- [ ] 注册到优化器规则链
- [ ] 性能测试

### Phase 8: 测试与优化（Week 7-8）

**目标**: 完整测试覆盖，性能达标

**任务清单**:
- [ ] 单元测试覆盖率 > 80%
- [ ] 性能测试：100万向量 HNSW 查询 P99 < 10ms
- [ ] 召回率测试：> 95%
- [ ] 文档完善

---

## 3. 详细设计

### 3.1 类型定义（统一扩展）

**pkg/resource/memory/index.go**:
```go
// IndexType 扩展
type IndexType string

const (
    IndexTypeBTree    IndexType = "btree"
    IndexTypeHash     IndexType = "hash"
    IndexTypeFullText IndexType = "fulltext"
    IndexTypeVectorHNSW    IndexType = "vector_hnsw"
    IndexTypeVectorIVFFlat IndexType = "vector_ivf_flat"
    IndexTypeVectorFlat    IndexType = "vector_flat"
)

func (t IndexType) IsVectorIndex() bool {
    switch t {
    case IndexTypeVectorHNSW, IndexTypeVectorIVFFlat, IndexTypeVectorFlat:
        return true
    default:
        return false
    }
}

// VectorMetricType 距离度量
type VectorMetricType string

const (
    VectorMetricCosine  VectorMetricType = "cosine"
    VectorMetricL2      VectorMetricType = "l2"
    VectorMetricIP      VectorMetricType = "inner_product"
)

// VectorIndexConfig 索引配置
type VectorIndexConfig struct {
    MetricType VectorMetricType       `json:"metric_type"`
    Dimension  int                    `json:"dimension"`
    Params     map[string]interface{} `json:"params,omitempty"`
}
```

**pkg/resource/domain/models.go**:
```go
// ColumnInfo 扩展
type ColumnInfo struct {
    // ... 现有字段 ...
    VectorDim  int    `json:"vector_dim,omitempty"`
    VectorType string `json:"vector_type,omitempty"`
}

func (c ColumnInfo) IsVectorType() bool {
    return c.VectorDim > 0
}

// Index 扩展
type Index struct {
    // ... 现有字段 ...
    VectorConfig *VectorIndexConfig `json:"vector_config,omitempty"`
}
```

### 3.2 距离函数（模块化）

**pkg/resource/memory/distance.go**:
```go
package memory

import (
    "fmt"
    "math"
    "sync"
)

// DistanceFunc 接口
type DistanceFunc interface {
    Name() string
    Compute(v1, v2 []float32) float32
}

// 注册中心
var distanceRegistry = struct {
    mu    sync.RWMutex
    funcs map[string]DistanceFunc
}{funcs: make(map[string]DistanceFunc)}

func RegisterDistance(name string, fn DistanceFunc) {
    distanceRegistry.mu.Lock()
    defer distanceRegistry.mu.Unlock()
    distanceRegistry.funcs[name] = fn
}

func GetDistance(name string) (DistanceFunc, error) {
    distanceRegistry.mu.RLock()
    defer distanceRegistry.mu.RUnlock()
    fn, ok := distanceRegistry.funcs[name]
    if !ok {
        return nil, fmt.Errorf("unknown distance function: %s", name)
    }
    return fn, nil
}

// 快捷方式
func CosineDistance(v1, v2 []float32) float32 {
    return MustGetDistance("cosine").Compute(v1, v2)
}

// 具体实现
type cosineDistance struct{}

func (c *cosineDistance) Name() string { return "cosine" }

func (c *cosineDistance) Compute(v1, v2 []float32) float32 {
    var dot, norm1, norm2 float64
    for i := 0; i < len(v1); i++ {
        dot += float64(v1[i] * v2[i])
        norm1 += float64(v1[i] * v1[i])
        norm2 += float64(v2[i] * v2[i])
    }
    if norm1 == 0 || norm2 == 0 {
        return 1.0
    }
    return float32(1.0 - dot/(math.Sqrt(norm1)*math.Sqrt(norm2)))
}

func init() {
    RegisterDistance("cosine", &cosineDistance{})
    // ... 其他距离函数
}
```

### 3.3 向量索引接口

**pkg/resource/memory/vector_index.go**:
```go
package memory

import "context"

type VectorIndex interface {
    Build(ctx context.Context, loader VectorDataLoader) error
    Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error)
    Insert(id int64, vector []float32) error
    Delete(id int64) error
    GetConfig() *VectorIndexConfig
    Stats() VectorIndexStats
    Close() error
}

type VectorDataLoader interface {
    Load(ctx context.Context) ([]VectorRecord, error)
    Count() int64
}

type VectorRecord struct {
    ID     int64
    Vector []float32
}

type VectorFilter struct {
    IDs []int64
}

type VectorSearchResult struct {
    IDs       []int64
    Distances []float32
}

type VectorIndexStats struct {
    Type       IndexType
    Metric     VectorMetricType
    Dimension  int
    Count      int64
    MemorySize int64
}
```

### 3.4 IndexManager 扩展

**pkg/resource/memory/index_manager.go**:
```go
// TableIndexes 扩展现有结构
type TableIndexes struct {
    tableName     string
    indexes       map[string]Index       // indexName -> Index（传统索引）
    columnMap     map[string]Index       // columnName -> Index（传统索引快速查找）
    vectorIndexes map[string]VectorIndex // columnName -> VectorIndex（新增：向量索引）
    mu            sync.RWMutex
}

// CreateVectorIndex 新增方法
func (m *IndexManager) CreateVectorIndex(
    tableName, columnName string,
    metricType VectorMetricType,
    indexType IndexType,
    dimension int,
    params map[string]interface{},
) (VectorIndex, error) {
    // 实现...
}

// GetVectorIndex 新增方法
func (m *IndexManager) GetVectorIndex(tableName, columnName string) (VectorIndex, error) {
    // 实现...
}
```

---

## 4. 代码实现

### 4.1 HNSW 索引（简化版）

```go
// pkg/resource/memory/hnsw_index.go
package memory

import (
    "context"
    "fmt"
    "sort"
    "sync"
)

type HNSWIndex struct {
    columnName string
    config     *VectorIndexConfig
    distFunc   DistanceFunc
    vectors    map[int64][]float32
    mu         sync.RWMutex
}

func NewHNSWIndex(columnName string, config *VectorIndexConfig) (*HNSWIndex, error) {
    distFunc, err := GetDistance(string(config.MetricType))
    if err != nil {
        return nil, err
    }
    return &HNSWIndex{
        columnName: columnName,
        config:     config,
        distFunc:   distFunc,
        vectors:    make(map[int64][]float32),
    }, nil
}

func (h *HNSWIndex) Build(ctx context.Context, loader VectorDataLoader) error {
    records, err := loader.Load(ctx)
    if err != nil {
        return err
    }
    h.mu.Lock()
    defer h.mu.Unlock()
    for _, rec := range records {
        vec := make([]float32, len(rec.Vector))
        copy(vec, rec.Vector)
        h.vectors[rec.ID] = vec
    }
    return nil
}

func (h *HNSWIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
    h.mu.RLock()
    defer h.mu.RUnlock()
    
    type idDist struct {
        id   int64
        dist float32
    }
    
    candidates := make([]idDist, 0, len(h.vectors))
    for id, vec := range h.vectors {
        if filter != nil && len(filter.IDs) > 0 {
            found := false
            for _, fid := range filter.IDs {
                if fid == id {
                    found = true
                    break
                }
            }
            if !found {
                continue
            }
        }
        dist := h.distFunc.Compute(query, vec)
        candidates = append(candidates, idDist{id, dist})
    }
    
    sort.Slice(candidates, func(i, j int) bool {
        return candidates[i].dist < candidates[j].dist
    })
    
    if len(candidates) > k {
        candidates = candidates[:k]
    }
    
    result := &VectorSearchResult{
        IDs:       make([]int64, len(candidates)),
        Distances: make([]float32, len(candidates)),
    }
    for i, c := range candidates {
        result.IDs[i] = c.id
        result.Distances[i] = c.dist
    }
    return result, nil
}

// 其他方法...（Insert, Delete, Stats, Close）
```

### 4.2 执行算子

```go
// pkg/executor/operators/vector_scan.go
package operators

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

type VectorScanOperator struct {
    *BaseOperator
    config *plan.VectorScanConfig
    idxMgr *memory.IndexManager
}

func NewVectorScanOperator(p *plan.Plan, das dataaccess.Service, idxMgr *memory.IndexManager) (*VectorScanOperator, error) {
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

func (v *VectorScanOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
    vectorIdx, err := v.idxMgr.GetVectorIndex(v.config.TableName, v.config.ColumnName)
    if err != nil {
        return nil, err
    }
    
    result, err := vectorIdx.Search(ctx, v.config.QueryVector, v.config.K, nil)
    if err != nil {
        return nil, err
    }
    
    rows, err := v.fetchRowsByIDs(ctx, result.IDs)
    if err != nil {
        return nil, err
    }
    
    for i, row := range rows {
        row["_distance"] = result.Distances[i]
    }
    
    return &domain.QueryResult{Rows: rows}, nil
}

func (v *VectorScanOperator) fetchRowsByIDs(ctx context.Context, ids []int64) ([]domain.Row, error) {
    rows := make([]domain.Row, 0, len(ids))
    for _, id := range ids {
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

## 5. 测试验收

### 5.1 功能测试

```go
// 测试向量索引创建和搜索
func TestVectorIndex(t *testing.T) {
    idxMgr := memory.NewIndexManager()
    
    // 创建索引
    idx, err := idxMgr.CreateVectorIndex("articles", "embedding",
        memory.VectorMetricCosine, memory.IndexTypeVectorHNSW, 768, nil)
    require.NoError(t, err)
    
    // 添加向量
    for i := 0; i < 100; i++ {
        vec := randomVector(768)
        err := idx.Insert(int64(i), vec)
        require.NoError(t, err)
    }
    
    // 搜索
    query := randomVector(768)
    result, err := idx.Search(context.Background(), query, 10, nil)
    require.NoError(t, err)
    require.Len(t, result.IDs, 10)
}
```

### 5.2 性能测试

| 指标 | 目标值 | 测试方法 |
|------|-------|---------|
| 索引构建 | > 5000 向量/秒 | 100万向量构建时间 |
| HNSW 查询 P99 | < 10ms | 100万向量，k=10 |
| Flat 查询 P99 | < 100ms | 10万向量，k=10 |
| 召回率 | > 95% | 与暴力搜索对比 |

### 5.3 集成测试

```sql
-- 测试 SQL
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    embedding VECTOR(768)
);

CREATE VECTOR INDEX idx_embedding ON articles(embedding) 
    USING HNSW WITH (metric = 'cosine');

SELECT * FROM articles 
ORDER BY VEC_COSINE_DISTANCE(embedding, '[0.1, 0.2, ...]')
LIMIT 10;
```

---

## 附录：快速参考

### A. 类型对照表

| 概念 | 类型定义位置 | 值 |
|------|------------|-----|
| 向量索引类型 | `pkg/resource/memory/index.go` | IndexTypeVectorHNSW |
| 距离度量 | `pkg/resource/memory/index.go` | VectorMetricCosine |
| 向量维度 | `pkg/resource/domain/models.go` | ColumnInfo.VectorDim |

### B. 文件路径速查

```
pkg/resource/memory/
├── index.go              # IndexType 扩展
├── index_manager.go      # CreateVectorIndex / GetVectorIndex
├── distance.go           # 距离函数
├── vector_index.go       # VectorIndex 接口
├── hnsw_index.go         # HNSW 实现
└── flat_index.go         # Flat 实现

pkg/optimizer/
├── cost/interfaces.go    # VectorSearchCost
├── cost/adaptive_model.go # 成本实现
├── plan/types.go         # TypeVectorScan
└── plan/vector_scan.go   # VectorScanConfig

pkg/executor/
├── executor.go           # buildOperator switch-case
└── operators/vector_scan.go # 执行算子
```

### C. 编译检查命令

```bash
# 逐层编译检查
go build ./pkg/resource/memory/...
go build ./pkg/resource/domain/...
go build ./pkg/optimizer/...
go build ./pkg/executor/...
go build ./...

# 测试
go test ./pkg/resource/memory/... -v
go test ./pkg/optimizer/... -v
go test ./pkg/executor/... -v
```

---

**文档版本**: v3.0-COMPLETE  
**最后更新**: 2024-01-08  
**状态**: 可直接按阶段实施
