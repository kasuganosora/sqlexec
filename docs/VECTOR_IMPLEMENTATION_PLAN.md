# SQLExec 向量处理能力实施计划

## 概述

本文档描述了为 SQLExec 项目增加完整向量处理能力的实施计划。参考 Milvus 的向量处理实现和 TiDB 的向量索引语法，为 SQLExec 添加高性能的向量数据存储、索引和查询能力。

## 目标

1. **向量数据类型支持**: 支持固定维度和稀疏向量
2. **向量索引**: 实现 HNSW、IVF 等近似最近邻(ANN)索引
3. **距离计算**: 支持余弦距离、L2 距离等常用距离度量
4. **向量搜索优化**: 优化器支持向量索引选择和查询优化
5. **SQL 语法兼容**: 参考 TiDB 语法，提供直观的向量操作语句

---

## 1. 数据类型层 (pkg/types/)

### 1.1 向量类型定义

**文件**: `pkg/types/vector.go`

```go
// VectorType 向量类型
 type VectorType int
 
 const (
     VectorTypeFloat32 VectorType = iota  // 浮点向量
     VectorTypeFloat16                    // Float16 压缩向量
     VectorTypeBFloat16                   // BFloat16 压缩向量
     VectorTypeBinary                     // 二值向量
     VectorTypeInt8                       // Int8 向量
     VectorTypeSparse                     // 稀疏向量
 )

// Vector 向量接口
type Vector interface {
    Dim() int                    // 返回维度
    Type() VectorType            // 返回向量类型
    Serialize() []byte           // 序列化
    Deserialize([]byte) error    // 反序列化
    ToFloat32() []float32        // 转换为float32切片
}

// FloatVector 浮点向量实现
type FloatVector struct {
    Data []float32
}

// SparseVector 稀疏向量实现
type SparseVector struct {
    Dim      int
    Indices  []uint32   // 非零位置
    Values   []float32  // 非零值
}

// VectorDistance 距离函数类型
type VectorDistance int

const (
    DistanceCosine VectorDistance = iota  // 余弦距离
    DistanceL2                            // 欧氏距离(L2)
    DistanceInnerProduct                  // 内积
    DistanceHamming                       // 汉明距离(二值向量)
    DistanceJaccard                       // Jaccard距离
)
```

### 1.2 列类型扩展

**修改文件**: `pkg/resource/domain/models.go`

```go
// ColumnInfo 扩展列信息
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
    
    // 向量类型特有属性
    VectorDim    int              `json:"vector_dim,omitempty"`      // 向量维度
    VectorType   VectorDataType   `json:"vector_type,omitempty"`     // 向量数据类型
}

// VectorDataType 向量数据类型
type VectorDataType string

const (
    VectorDataTypeFloat32  VectorDataType = "float32"
    VectorDataTypeFloat16  VectorDataType = "float16"
    VectorDataTypeBFloat16 VectorDataType = "bfloat16"
    VectorDataTypeBinary   VectorDataType = "binary"
    VectorDataTypeInt8     VectorDataType = "int8"
    VectorDataTypeSparse   VectorDataType = "sparse"
)
```

---

## 2. SQL 解析层 (pkg/parser/)

### 2.1 VECTOR 类型解析

**修改文件**: `pkg/parser/adapter.go`

添加 VECTOR 类型识别：

```go
// isVectorType 检查是否为向量类型
func (a *SQLAdapter) isVectorType(tp string) bool {
    upper := strings.ToUpper(tp)
    return strings.HasPrefix(upper, "VECTOR") || 
           strings.HasPrefix(upper, "FLOATVECTOR") ||
           strings.HasPrefix(upper, "SPARSEVECTOR")
}

// parseVectorType 解析向量类型定义
// 支持: VECTOR(768), VECTOR(768, FLOAT32), SPARSEVECTOR
func (a *SQLAdapter) parseVectorType(tp string) (dim int, vtype VectorDataType, err error) {
    // 实现解析逻辑
}
```

### 2.2 向量索引语句解析

**修改文件**: `pkg/parser/types.go`

```go
// CreateVectorIndexStatement 创建向量索引语句
type CreateVectorIndexStatement struct {
    IndexName    string
    TableName    string
    ColumnName   string
    DistanceFunc string       // VEC_COSINE_DISTANCE, VEC_L2_DISTANCE
    IndexType    string       // HNSW, IVF_FLAT, IVF_PQ, etc.
    Params       map[string]interface{}  // 索引参数
}

// 支持语句:
// CREATE VECTOR INDEX idx_embedding ON foo ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;
// ALTER TABLE foo ADD VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(embedding)));
```

**修改文件**: `pkg/parser/adapter.go` - 添加 convertCreateVectorIndexStmt

### 2.3 向量函数解析

**修改文件**: `pkg/parser/expr_converter.go`

```go
// VectorFunction 向量函数类型
const (
    VectorFuncCosineDistance = "VEC_COSINE_DISTANCE"
    VectorFuncL2Distance     = "VEC_L2_DISTANCE"
    VectorFuncInnerProduct   = "VEC_INNER_PRODUCT"
)

// isVectorFunction 检查是否为向量距离函数
func (a *SQLAdapter) isVectorFunction(funcName string) bool {
    upper := strings.ToUpper(funcName)
    return upper == VectorFuncCosineDistance || 
           upper == VectorFuncL2Distance ||
           upper == VectorFuncInnerProduct
}
```

---

## 3. 内置函数层 (pkg/builtin/)

### 3.1 向量距离函数

**文件**: `pkg/builtin/vector_functions.go`

```go
package builtin

import (
    "math"
    "github.com/kasuganosora/sqlexec/pkg/types"
)

func init() {
    // 注册向量函数
    vectorFunctions := []*FunctionInfo{
        {
            Name: "VEC_COSINE_DISTANCE",
            Type: FunctionTypeScalar,
            Signatures: []FunctionSignature{
                {Name: "VEC_COSINE_DISTANCE", ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
            },
            Handler:     vecCosineDistance,
            Description: "计算两个向量的余弦距离",
            Example:     "VEC_COSINE_DISTANCE(embedding, '[1,2,3]')",
            Category:    "vector",
        },
        {
            Name: "VEC_L2_DISTANCE",
            Type: FunctionTypeScalar,
            Signatures: []FunctionSignature{
                {Name: "VEC_L2_DISTANCE", ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
            },
            Handler:     vecL2Distance,
            Description: "计算两个向量的欧氏距离(L2)",
            Example:     "VEC_L2_DISTANCE(embedding, '[1,2,3]')",
            Category:    "vector",
        },
        {
            Name: "VEC_INNER_PRODUCT",
            Type: FunctionTypeScalar,
            Signatures: []FunctionSignature{
                {Name: "VEC_INNER_PRODUCT", ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
            },
            Handler:     vecInnerProduct,
            Description: "计算两个向量的内积",
            Example:     "VEC_INNER_PRODUCT(embedding, '[1,2,3]')",
            Category:    "vector",
        },
    }
    
    for _, fn := range vectorFunctions {
        RegisterGlobal(fn)
    }
}

// vecCosineDistance 余弦距离计算
// 距离 = 1 - 余弦相似度
func vecCosineDistance(args []interface{}) (interface{}, error) {
    if len(args) != 2 {
        return nil, fmt.Errorf("VEC_COSINE_DISTANCE requires exactly 2 arguments")
    }
    
    vec1, err := toFloatVector(args[0])
    if err != nil {
        return nil, err
    }
    
    vec2, err := toFloatVector(args[1])
    if err != nil {
        return nil, err
    }
    
    if len(vec1) != len(vec2) {
        return nil, fmt.Errorf("vector dimensions do not match: %d vs %d", len(vec1), len(vec2))
    }
    
    var dotProduct, norm1, norm2 float64
    for i := 0; i < len(vec1); i++ {
        dotProduct += float64(vec1[i] * vec2[i])
        norm1 += float64(vec1[i] * vec1[i])
        norm2 += float64(vec2[i] * vec2[i])
    }
    
    if norm1 == 0 || norm2 == 0 {
        return 1.0, nil  // 零向量返回最大距离
    }
    
    similarity := dotProduct / (math.Sqrt(norm1) * math.Sqrt(norm2))
    distance := 1.0 - similarity
    
    return distance, nil
}

// vecL2Distance 欧氏距离计算
func vecL2Distance(args []interface{}) (interface{}, error) {
    // 实现...
}

// vecInnerProduct 内积计算
func vecInnerProduct(args []interface{}) (interface{}, error) {
    // 实现...
}

// parseVectorLiteral 解析向量字面量
// 支持格式: '[1,2,3]', '[1.0, 2.0, 3.0]'
func parseVectorLiteral(s string) ([]float32, error) {
    // 实现解析逻辑
}
```

---

## 4. 索引层 (pkg/resource/memory/)

### 4.1 向量索引接口

**文件**: `pkg/resource/memory/vector_index.go`

```go
package memory

// VectorIndex 向量索引接口
type VectorIndex interface {
    Index
    
    // Search 近似最近邻搜索
    // query: 查询向量
    // k: 返回最相似的k个结果
    // 返回: (行ID列表, 距离列表)
    Search(query interface{}, k int) ([]int64, []float32, error)
    
    // GetMetricType 返回距离度量类型
    GetMetricType() VectorMetricType
    
    // GetIndexType 返回索引类型
    GetIndexType() VectorIndexType
}

// VectorMetricType 向量距离度量类型
type VectorMetricType string

const (
    MetricCosine     VectorMetricType = "cosine"
    MetricL2         VectorMetricType = "l2"
    MetricIP         VectorMetricType = "ip"          // Inner Product
    MetricHamming    VectorMetricType = "hamming"
    MetricJaccard    VectorMetricType = "jaccard"
)

// VectorIndexType 向量索引类型
type VectorIndexType string

const (
    VectorIndexHNSW    VectorIndexType = "hnsw"
    VectorIndexIVFFlat VectorIndexType = "ivf_flat"
    VectorIndexIVFPQ   VectorIndexType = "ivf_pq"
    VectorIndexIVFSQ8  VectorIndexType = "ivf_sq8"
    VectorIndexFlat    VectorIndexType = "flat"
    VectorIndexSparse  VectorIndexType = "sparse"
)

// VectorIndexParams 向量索引参数
type VectorIndexParams struct {
    // HNSW 参数
    M              int     // 图中每个节点的最大邻居数
    EfConstruction int     // 构建时的探索因子
    EfSearch       int     // 搜索时的探索因子
    
    // IVF 参数
    NList          int     // 聚类中心数
    NProbe         int     // 搜索时探测的聚类数
    
    // PQ 参数
    M_PQ           int     // 子向量数
    NBits          int     // 每个子向量的编码位数
}
```

### 4.2 HNSW 索引实现

**文件**: `pkg/resource/memory/hnsw_index.go`

```go
package memory

// HNSWIndex HNSW 索引实现
// 参考 Milvus 的 HNSW 实现
type HNSWIndex struct {
    info        *IndexInfo
    metricType  VectorMetricType
    
    // HNSW 参数
    M              int
    efConstruction int
    efSearch       int
    
    // 索引数据
    nodes       map[int64]*HNSWNode  // 节点ID -> 节点
    entryPoint  int64                // 入口节点
    maxLevel    int                  // 最大层数
    
    // 维度信息
    dim         int
    
    mu          sync.RWMutex
}

// HNSWNode HNSW 图节点
type HNSWNode struct {
    ID       int64
    Vector   []float32
    Level    int
    Neighbors [][]int64  // 每层邻居
}

// NewHNSWIndex 创建 HNSW 索引
func NewHNSWIndex(
    tableName, 
    columnName string, 
    dim int, 
    metricType VectorMetricType,
    M, efConstruction int,
) *HNSWIndex {
    return &HNSWIndex{
        info: &IndexInfo{
            Name:      fmt.Sprintf("idx_hnsw_%s_%s", tableName, columnName),
            TableName: tableName,
            Column:    columnName,
            Type:      IndexTypeVector,
            Unique:    false,
        },
        metricType:     metricType,
        M:              M,
        efConstruction: efConstruction,
        efSearch:       efConstruction, // 默认使用相同值
        nodes:          make(map[int64]*HNSWNode),
        dim:            dim,
    }
}

// Insert 插入向量
func (idx *HNSWIndex) Insert(key interface{}, rowIDs []int64) error {
    vec, ok := key.([]float32)
    if !ok {
        return fmt.Errorf("HNSW index requires float32 slice")
    }
    
    if len(vec) != idx.dim {
        return fmt.Errorf("vector dimension mismatch: expected %d, got %d", idx.dim, len(vec))
    }
    
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    for _, rowID := range rowIDs {
        // 计算节点层数
        level := idx.randomLevel()
        
        node := &HNSWNode{
            ID:        rowID,
            Vector:    make([]float32, len(vec)),
            Level:     level,
            Neighbors: make([][]int64, level+1),
        }
        copy(node.Vector, vec)
        
        // 插入到图中
        idx.insertNode(node)
    }
    
    return nil
}

// Search 搜索最近邻
func (idx *HNSWIndex) Search(query interface{}, k int) ([]int64, []float32, error) {
    vec, ok := query.([]float32)
    if !ok {
        return nil, nil, fmt.Errorf("query must be float32 slice")
    }
    
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    if len(idx.nodes) == 0 {
        return []int64{}, []float32{}, nil
    }
    
    // 从入口点开始搜索
    currEp := idx.entryPoint
    currDist := idx.distance(idx.nodes[currEp].Vector, vec)
    
    // 从顶层开始逐层搜索
    for level := idx.maxLevel; level > 0; level-- {
        changed := true
        for changed {
            changed = false
            node := idx.nodes[currEp]
            if level >= len(node.Neighbors) {
                continue
            }
            for _, neighborID := range node.Neighbors[level] {
                if neighbor, exists := idx.nodes[neighborID]; exists {
                    dist := idx.distance(neighbor.Vector, vec)
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
    candidates := idx.searchLayer(vec, currEp, idx.efSearch, 0)
    
    // 取前k个
    if len(candidates) > k {
        candidates = candidates[:k]
    }
    
    var ids []int64
    var distances []float32
    for _, c := range candidates {
        ids = append(ids, c.ID)
        distances = append(distances, float32(c.Distance))
    }
    
    return ids, distances, nil
}

// distance 计算距离
func (idx *HNSWIndex) distance(v1, v2 []float32) float64 {
    switch idx.metricType {
    case MetricCosine:
        return cosineDistance(v1, v2)
    case MetricL2:
        return l2Distance(v1, v2)
    case MetricIP:
        return -innerProduct(v1, v2)  // 内积越大越相似，取负转为距离
    default:
        return l2Distance(v1, v2)
    }
}

// randomLevel 随机生成层数
func (idx *HNSWIndex) randomLevel() int {
    // 使用指数分布，参考原始 HNSW 论文
    level := 0
    for rand.Float64() < 0.5 && level < 16 {
        level++
    }
    return level
}

// 其他辅助方法...
```

### 4.3 IVF 索引实现

**文件**: `pkg/resource/memory/ivf_index.go`

```go
package memory

// IVFIndex 倒排文件索引实现
// 使用 K-Means 聚类构建倒排索引
type IVFIndex struct {
    info       *IndexInfo
    metricType VectorMetricType
    
    // IVF 参数
    nlist      int     // 聚类中心数
    nprobe     int     // 搜索时探测的聚类数
    
    // 聚类数据
    centroids  [][]float32          // 聚类中心
    invertedLists [][]int64         // 倒排列表
    vectors    map[int64][]float32  // 向量数据
    
    dim        int
    trained    bool
    
    mu         sync.RWMutex
}

// NewIVFIndex 创建 IVF 索引
func NewIVFIndex(
    tableName, 
    columnName string,
    dim int,
    metricType VectorMetricType,
    nlist int,
) *IVFIndex {
    return &IVFIndex{
        info: &IndexInfo{
            Name:      fmt.Sprintf("idx_ivf_%s_%s", tableName, columnName),
            TableName: tableName,
            Column:    columnName,
            Type:      IndexTypeVector,
        },
        metricType: metricType,
        nlist:      nlist,
        nprobe:     max(nlist/10, 1),  // 默认探测 10%
        centroids:  make([][]float32, nlist),
        invertedLists: make([][]int64, nlist),
        vectors:    make(map[int64][]float32),
        dim:        dim,
    }
}

// Train 训练聚类中心
func (idx *IVFIndex) Train(vectors [][]float32) error {
    // 使用 K-Means++ 初始化
    // 实现聚类逻辑
}

// Search 搜索
func (idx *IVFIndex) Search(query interface{}, k int) ([]int64, []float32, error) {
    // 1. 找到最近的 nprobe 个聚类中心
    // 2. 在这些聚类中搜索最近的 k 个向量
}
```

### 4.4 索引管理器扩展

**修改文件**: `pkg/resource/memory/index_manager.go`

```go
// CreateVectorIndex 创建向量索引
func (m *IndexManager) CreateVectorIndex(
    tableName, 
    columnName string,
    indexType VectorIndexType,
    metricType VectorMetricType,
    dim int,
    params VectorIndexParams,
) (VectorIndex, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    key := m.indexKey(tableName, columnName)
    
    var index VectorIndex
    switch indexType {
    case VectorIndexHNSW:
        index = NewHNSWIndex(tableName, columnName, dim, metricType, params.M, params.EfConstruction)
    case VectorIndexIVFFlat:
        index = NewIVFIndex(tableName, columnName, dim, metricType, params.NList)
    // 其他索引类型...
    default:
        return nil, fmt.Errorf("unsupported vector index type: %s", indexType)
    }
    
    m.vectorIndexes[key] = index
    return index, nil
}
```

---

## 5. 优化器层 (pkg/optimizer/)

### 5.1 向量索引支持

**文件**: `pkg/optimizer/vector_index_support.go`

```go
package optimizer

// VectorIndexSupport 向量索引支持
type VectorIndexSupport struct {
    // 向量索引成本因子
    HNSWSearchCostFactor    float64
    IVFSearchCostFactor     float64
    BruteForceCostFactor    float64
}

// NewVectorIndexSupport 创建向量索引支持
func NewVectorIndexSupport() *VectorIndexSupport {
    return &VectorIndexSupport{
        HNSWSearchCostFactor: 0.01,   // HNSW 搜索成本很低
        IVFSearchCostFactor:  0.05,   // IVF 搜索成本较低
        BruteForceCostFactor: 1.0,    // 暴力搜索成本基准
    }
}

// IsVectorSearchQuery 检查是否为向量搜索查询
// 模式: ORDER BY VEC_COSINE_DISTANCE(...) LIMIT n
func (vis *VectorIndexSupport) IsVectorSearchQuery(
    orderBy []OrderByItem,
    limit *int64,
) (bool, string, VectorMetricType) {
    if limit == nil || *limit <= 0 {
        return false, "", ""
    }
    
    if len(orderBy) != 1 {
        return false, "", ""
    }
    
    // 检查 ORDER BY 是否为向量距离函数
    expr := orderBy[0].Expr
    if expr == nil || expr.Type != ExprTypeFunction {
        return false, "", ""
    }
    
    switch expr.Function {
    case "VEC_COSINE_DISTANCE":
        return true, expr.Args[0].Column, MetricCosine
    case "VEC_L2_DISTANCE":
        return true, expr.Args[0].Column, MetricL2
    case "VEC_INNER_PRODUCT":
        return true, expr.Args[0].Column, MetricIP
    default:
        return false, "", ""
    }
}

// EstimateVectorSearchCost 估算向量搜索成本
func (vis *VectorIndexSupport) EstimateVectorSearchCost(
    rowCount int64,
    limit int64,
    indexType VectorIndexType,
) float64 {
    var factor float64
    switch indexType {
    case VectorIndexHNSW:
        factor = vis.HNSWSearchCostFactor
    case VectorIndexIVFFlat, VectorIndexIVFPQ, VectorIndexIVFSQ8:
        factor = vis.IVFSearchCostFactor
    default:
        factor = vis.BruteForceCostFactor
    }
    
    // 成本 = 因子 * (数据集大小因子) * log(返回结果数)
    datasetFactor := math.Log10(float64(rowCount)+1) / 10
    return factor * datasetFactor * math.Log10(float64(limit)+1)
}

// GetCompatibleIndex 获取兼容的向量索引
func (vis *VectorIndexSupport) GetCompatibleIndex(
    indexes []VectorIndex,
    columnName string,
    metricType VectorMetricType,
) VectorIndex {
    for _, idx := range indexes {
        if idx.GetIndexInfo().Column == columnName && 
           idx.GetMetricType() == metricType {
            return idx
        }
    }
    return nil
}
```

### 5.2 逻辑算子 - 向量扫描

**文件**: `pkg/optimizer/logical_vector_scan.go`

```go
package optimizer

// LogicalVectorScan 向量索引扫描逻辑算子
type LogicalVectorScan struct {
    TableName    string
    ColumnName   string
    QueryVector  interface{}      // 查询向量
    K            int              // 返回最相似的K个
    DistanceFunc string           // 距离函数
    MetricType   VectorMetricType
    IndexType    VectorIndexType
    
    // 过滤条件（支持预过滤）
    PreFilters   []*Expression
}

// NewLogicalVectorScan 创建向量扫描算子
func NewLogicalVectorScan(
    tableName, columnName string,
    queryVector interface{},
    k int,
    metricType VectorMetricType,
) *LogicalVectorScan {
    return &LogicalVectorScan{
        TableName:    tableName,
        ColumnName:   columnName,
        QueryVector:  queryVector,
        K:            k,
        MetricType:   metricType,
    }
}
```

### 5.3 物理算子 - 向量搜索

**文件**: `pkg/optimizer/physical/vector_search.go`

```go
package physical

// PhysicalVectorSearch 向量搜索物理算子
type PhysicalVectorSearch struct {
    basePhysicalOperator
    
    TableName    string
    ColumnName   string
    QueryVector  interface{}
    K            int
    MetricType   VectorMetricType
    IndexType    VectorIndexType
    
    // 成本
    estimatedCost float64
}

// NewPhysicalVectorSearch 创建向量搜索算子
func NewPhysicalVectorSearch(
    tableName, columnName string,
    queryVector interface{},
    k int,
    metricType VectorMetricType,
    indexType VectorIndexType,
) *PhysicalVectorSearch {
    return &PhysicalVectorSearch{
        TableName:   tableName,
        ColumnName:  columnName,
        QueryVector: queryVector,
        K:           k,
        MetricType:  metricType,
        IndexType:   indexType,
    }
}

// Cost 返回执行成本
func (op *PhysicalVectorSearch) Cost() float64 {
    return op.estimatedCost
}

// Explain 返回计划说明
func (op *PhysicalVectorSearch) Explain() string {
    return fmt.Sprintf("VectorSearch[%s.%s, %s, k=%d]",
        op.TableName, op.ColumnName, op.MetricType, op.K)
}
```

### 5.4 向量索引选择器

**修改文件**: `pkg/optimizer/index/selector.go`

```go
// SelectVectorIndex 选择向量索引
func (s *IndexSelector) SelectVectorIndex(
    tableName string,
    vectorScan *LogicalVectorScan,
    availableIndexes []domain.Index,
) (*domain.Index, error) {
    // 查找兼容的向量索引
    var candidates []*domain.Index
    
    for _, idx := range availableIndexes {
        if idx.Type != domain.IndexTypeVector {
            continue
        }
        
        // 检查列是否匹配
        if len(idx.Columns) != 1 || idx.Columns[0] != vectorScan.ColumnName {
            continue
        }
        
        // 检查距离函数是否匹配
        if idx.VectorMetricType != vectorScan.MetricType {
            continue
        }
        
        candidates = append(candidates, idx)
    }
    
    if len(candidates) == 0 {
        return nil, nil  // 没有合适的索引
    }
    
    // 根据索引类型选择最优索引
    // HNSW > IVF_PQ > IVF_SQ8 > IVF_FLAT
    priority := map[string]int{
        "hnsw":     4,
        "ivf_pq":   3,
        "ivf_sq8":  2,
        "ivf_flat": 1,
    }
    
    best := candidates[0]
    bestPriority := priority[best.IndexType]
    
    for _, idx := range candidates[1:] {
        if p := priority[idx.IndexType]; p > bestPriority {
            best = idx
            bestPriority = p
        }
    }
    
    return best, nil
}
```

### 5.5 优化规则 - 向量索引替换

**文件**: `pkg/optimizer/rules_vector.go`

```go
package optimizer

// VectorIndexRewriteRule 向量索引重写规则
// 将 ORDER BY distance LIMIT k 转换为 VectorScan
type VectorIndexRewriteRule struct {
    vectorSupport *VectorIndexSupport
}

// NewVectorIndexRewriteRule 创建规则
func NewVectorIndexRewriteRule() *VectorIndexRewriteRule {
    return &VectorIndexRewriteRule{
        vectorSupport: NewVectorIndexSupport(),
    }
}

// Apply 应用规则
func (r *VectorIndexRewriteRule) Apply(node LogicalOperator) (LogicalOperator, bool) {
    // 查找 Sort + Limit 模式
    sortOp, ok := node.(*LogicalSort)
    if !ok {
        return node, false
    }
    
    // 检查是否为向量搜索模式
    isVector, columnName, metricType := r.vectorSupport.IsVectorSearchQuery(
        sortOp.OrderBy, 
        sortOp.Limit,
    )
    if !isVector {
        return node, false
    }
    
    // 获取查询向量（从 ORDER BY 表达式中提取）
    queryVector := r.extractQueryVector(sortOp.OrderBy[0].Expr)
    
    // 创建向量扫描算子
    vectorScan := NewLogicalVectorScan(
        sortOp.TableName,
        columnName,
        queryVector,
        int(*sortOp.Limit),
        metricType,
    )
    
    return vectorScan, true
}

// extractQueryVector 从表达式中提取查询向量
func (r *VectorIndexRewriteRule) extractQueryVector(expr *Expression) interface{} {
    if expr == nil || len(expr.Args) < 2 {
        return nil
    }
    
    // 第二个参数是查询向量
    queryArg := expr.Args[1]
    if queryArg.Type == ExprTypeValue {
        return queryArg.Value
    }
    
    return nil
}
```

---

## 6. 执行器层 (pkg/executor/)

### 6.1 向量搜索算子

**文件**: `pkg/executor/operators/vector_search.go`

```go
package operators

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// VectorSearchOperator 向量搜索算子
type VectorSearchOperator struct {
    *BaseOperator
    config *plan.VectorSearchConfig
}

// VectorSearchConfig 向量搜索配置
// 定义在 pkg/optimizer/plan/vector.go
type VectorSearchConfig struct {
    TableName    string
    ColumnName   string
    QueryVector  []float32
    K            int
    MetricType   string
    IndexType    string
    
    // 可选的预过滤条件
    PreFilters   []domain.Filter
}

// NewVectorSearchOperator 创建向量搜索算子
func NewVectorSearchOperator(
    p *plan.Plan, 
    das dataaccess.Service,
) (*VectorSearchOperator, error) {
    config, ok := p.Config.(*plan.VectorSearchConfig)
    if !ok {
        return nil, fmt.Errorf("invalid config type for VectorSearch")
    }
    
    base := NewBaseOperator(p, das)
    return &VectorSearchOperator{
        BaseOperator: base,
        config:       config,
    }, nil
}

// Execute 执行向量搜索
func (op *VectorSearchOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
    fmt.Printf("  [EXECUTOR] VectorSearch: 表=%s, 列=%s, k=%d\n",
        op.config.TableName, op.config.ColumnName, op.config.K)
    
    // 1. 获取向量索引
    index, err := op.getVectorIndex()
    if err != nil {
        // 没有索引时回退到暴力搜索
        return op.bruteForceSearch(ctx)
    }
    
    // 2. 执行 ANN 搜索
    rowIDs, distances, err := index.Search(op.config.QueryVector, op.config.K)
    if err != nil {
        return nil, fmt.Errorf("vector search failed: %w", err)
    }
    
    // 3. 根据 rowIDs 获取完整行数据
    result, err := op.fetchRowsByIDs(ctx, rowIDs, distances)
    if err != nil {
        return nil, fmt.Errorf("fetch rows failed: %w", err)
    }
    
    return result, nil
}

// getVectorIndex 获取向量索引
func (op *VectorSearchOperator) getVectorIndex() (memory.VectorIndex, error) {
    // 从数据源获取索引管理器
    // 返回对应的向量索引
}

// bruteForceSearch 暴力搜索（无索引时回退）
func (op *VectorSearchOperator) bruteForceSearch(
    ctx context.Context,
) (*domain.QueryResult, error) {
    // 1. 获取所有数据
    // 2. 计算每行与查询向量的距离
    // 3. 排序取前K个
}

// fetchRowsByIDs 根据ID获取行数据
func (op *VectorSearchOperator) fetchRowsByIDs(
    ctx context.Context,
    rowIDs []int64,
    distances []float32,
) (*domain.QueryResult, error) {
    // 实现根据ID列表获取行数据
    // 并将距离添加到结果中
}
```

---

## 7. 信息模式扩展 (pkg/information_schema/)

### 7.1 向量索引信息表

**文件**: `pkg/information_schema/vector_indexes.go`

```go
package information_schema

// VectorIndexInfo 向量索引信息
type VectorIndexInfo struct {
    Database       string
    Table          string
    IndexName      string
    ColumnName     string
    IndexType      string  // HNSW, IVF_FLAT, etc.
    MetricType     string  // COSINE, L2, IP
    Status         string  // BUILDING, READY, FAILED
    RowsIndexed    int64
    RowsTotal      int64
    BuildProgress  float64 // 0-100
    Dimension      int
    Params         string  // JSON格式的参数
}

// VectorIndexesTable 向量索引信息表
var VectorIndexesTable = &VirtualTable{
    Name:    "VECTOR_INDEXES",
    Columns: []string{
        "TIDB_DATABASE", "TIDB_TABLE", "TABLE_ID", 
        "COLUMN_NAME", "INDEX_NAME", "INDEX_TYPE",
        "METRIC_TYPE", "STATUS", "ROWS_INDEXED", 
        "ROWS_TOTAL", "BUILD_PROGRESS", "DIMENSION", "PARAMS",
    },
    FetchFunc: fetchVectorIndexes,
}

func fetchVectorIndexes(ctx context.Context, schema string) ([]Row, error) {
    // 实现获取向量索引信息的逻辑
}
```

---

## 8. DDL 支持

### 8.1 创建向量索引

**SQL 语法**（参考 TiDB）：

```sql
-- 建表时创建向量索引
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    embedding VECTOR(768),
    VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(embedding))) USING HNSW
);

-- 为现有表添加向量索引
CREATE VECTOR INDEX idx_embedding ON articles ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;

-- 或者使用 ALTER TABLE
ALTER TABLE articles ADD VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(embedding))) USING HNSW;

-- 指定参数
CREATE VECTOR INDEX idx_embedding ON articles ((VEC_COSINE_DISTANCE(embedding))) 
    USING HNSW 
    WITH (M=16, EF_CONSTRUCTION=200);
```

### 8.2 向量搜索查询

```sql
-- 基本向量搜索（使用索引）
SELECT * FROM articles
ORDER BY VEC_COSINE_DISTANCE(embedding, '[1, 2, 3, ...]')
LIMIT 10;

-- 带预过滤的向量搜索（先过滤再搜索）
SELECT * FROM articles
WHERE category = 'tech'
ORDER BY VEC_COSINE_DISTANCE(embedding, '[1, 2, 3, ...]')
LIMIT 10;

-- 后过滤模式（先搜索再过滤）
SELECT * FROM (
    SELECT * FROM articles
    ORDER BY VEC_COSINE_DISTANCE(embedding, '[1, 2, 3, ...]')
    LIMIT 100
) t
WHERE category = 'tech'
LIMIT 10;

-- 返回距离值
SELECT id, title, VEC_COSINE_DISTANCE(embedding, '[1, 2, 3, ...]') AS distance
FROM articles
ORDER BY distance
LIMIT 10;
```

---

## 9. 实施路线图

### 阶段 1: 基础类型和函数（2周）

- [ ] 实现向量类型定义 (`pkg/types/vector.go`)
- [ ] 扩展 ColumnInfo 支持向量属性
- [ ] 实现向量距离函数 (`pkg/builtin/vector_functions.go`)
- [ ] 添加 SQL 解析支持

### 阶段 2: HNSW 索引实现（2周）

- [ ] 实现向量索引接口
- [ ] 实现 HNSW 索引 (`pkg/resource/memory/hnsw_index.go`)
- [ ] 扩展索引管理器支持向量索引
- [ ] 添加索引单元测试

### 阶段 3: 优化器支持（2周）

- [ ] 实现向量索引支持 (`pkg/optimizer/vector_index_support.go`)
- [ ] 实现向量扫描逻辑算子
- [ ] 实现向量搜索物理算子
- [ ] 实现向量索引重写规则
- [ ] 扩展索引选择器

### 阶段 4: 执行器支持（1周）

- [ ] 实现向量搜索算子 (`pkg/executor/operators/vector_search.go`)
- [ ] 注册向量搜索计划类型
- [ ] 集成到执行器

### 阶段 5: DDL 和元数据（1周）

- [ ] 实现 CREATE VECTOR INDEX 语句解析
- [ ] 实现 ALTER TABLE ADD VECTOR INDEX
- [ ] 添加 VECTOR_INDEXES 信息表
- [ ] 实现索引构建进度追踪

### 阶段 6: 测试和优化（2周）

- [ ] 单元测试（向量函数、索引、搜索）
- [ ] 集成测试（端到端向量搜索）
- [ ] 性能测试和优化
- [ ] 文档编写

---

## 10. 使用示例

### 10.1 创建向量表

```go
// 使用 API 创建带向量列的表
db := api.Open(":memory:")

_, err := db.Exec(`
    CREATE TABLE documents (
        id INT PRIMARY KEY,
        title VARCHAR(255),
        content TEXT,
        embedding VECTOR(768)
    )
`)
```

### 10.2 插入向量数据

```go
// 插入向量数据
embedding := []float32{0.1, 0.2, 0.3, /* ... 768维 ... */}

_, err := db.Exec(
    "INSERT INTO documents (id, title, content, embedding) VALUES (?, ?, ?, ?)",
    1, "Hello World", "This is a test", embedding,
)
```

### 10.3 创建向量索引

```go
// 创建 HNSW 向量索引
_, err := db.Exec(`
    CREATE VECTOR INDEX idx_embedding ON documents (
        (VEC_COSINE_DISTANCE(embedding))
    ) USING HNSW
`)
```

### 10.4 执行向量搜索

```go
// 向量搜索
queryVector := []float32{0.15, 0.25, 0.35, /* ... */}

rows, err := db.Query(`
    SELECT id, title, VEC_COSINE_DISTANCE(embedding, ?) AS distance
    FROM documents
    ORDER BY distance
    LIMIT 10
`, queryVector)

for rows.Next() {
    var id int
    var title string
    var distance float64
    rows.Scan(&id, &title, &distance)
    fmt.Printf("ID: %d, Title: %s, Distance: %.4f\n", id, title, distance)
}
```

---

## 11. 性能目标

| 指标 | 目标 |
|------|------|
| 向量索引构建速度 | 10,000 向量/秒 (768维) |
| HNSW 查询延迟 (P99) | < 10ms (100万向量) |
| IVF 查询延迟 (P99) | < 20ms (1000万向量) |
| 召回率 | > 95% (HNSW), > 90% (IVF) |
| 内存占用 | < 2x 原始数据大小 |

---

## 12. 注意事项

### 12.1 向量类型约束

1. **固定维度**: 创建表时必须指定向量维度
2. **数据类型**: 支持 FLOAT32 (默认), FLOAT16, BFLOAT16, INT8
3. **NULL 值**: 向量列支持 NULL，但索引会跳过 NULL 值

### 12.2 向量索引约束

1. **单列索引**: 向量索引只能基于单一向量列创建
2. **距离函数**: 创建索引时必须指定距离函数，查询时必须使用相同的距离函数
3. **维度匹配**: 查询向量的维度必须与列定义和索引一致
4. **唯一性**: 向量索引不能作为主键或唯一索引

### 12.3 查询优化建议

1. **使用 LIMIT**: 始终使用 LIMIT 限制返回结果数
2. **预过滤**: 尽可能使用预过滤减少搜索空间
3. **索引选择**: HNSW 适合高召回率场景，IVF 适合大规模数据

---

## 13. 参考资料

1. **Milvus 向量索引实现**:
   - `milvus/client/index/hnsw.go`
   - `milvus/client/index/ivf.go`
   - `milvus/client/entity/vectors.go`

2. **TiDB 向量搜索文档**:
   - 向量索引语法参考
   - 距离函数定义

3. **HNSW 论文**: 
   - "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs"

4. **IVF 索引参考**:
   - Faiss 库实现
   - "Billion-scale similarity search with GPUs"
