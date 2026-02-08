# SQLExec 向量搜索实施路线图

## 版本信息
- **版本**: v1.0
- **状态**: 待实施
- **预计工期**: 8-10 周
- **风险等级**: 中等

---

## 1. 现状分析

### 1.1 现有架构回顾

```
┌─────────────────────────────────────────────────────────────┐
│                     Server Layer                             │
│  MySQL Protocol Handler → Query Parser (TiDB Parser)        │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   Optimizer Layer                            │
│  SQL Converter → Logical Plan → Physical Plan → Cost Model  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   Executor Layer                             │
│  Plan → Operators (Scan, Filter, Join, Aggregate)           │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  DataAccess Layer                            │
│  Service → Manager → Router → DataSource (CSV/MySQL/Memory) │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  Resource Layer                              │
│  DataSource Interface → CSV / JSON / Memory / MySQL / API   │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 需要集成的点

| 层级 | 现有文件 | 修改类型 | 说明 |
|------|---------|---------|------|
| Domain | `pkg/resource/domain/models.go` | 扩展 | 添加向量类型和索引定义 |
| Parser | `pkg/parser/adapter.go` | 扩展 | 解析 VECTOR 类型和索引 |
| BuiltIn | `pkg/builtin/functions.go` | 扩展 | 添加向量距离函数 |
| DataAccess | `pkg/dataaccess/` | 新增 | 向量搜索扩展模块 |
| Optimizer | `pkg/optimizer/rules.go` | 扩展 | 向量索引重写规则 |
| Executor | `pkg/executor/operators/` | 新增 | 向量搜索算子 |
| InfoSchema | `pkg/information_schema/` | 扩展 | 向量索引信息表 |

---

## 2. 详细实施步骤

### 阶段 1: 基础类型扩展（Week 1）

#### 2.1.1 扩展 Column 类型定义

**文件**: `pkg/resource/domain/models.go`

```go
// 添加在文件末尾

// VectorDataType 向量数据类型
type VectorDataType string

const (
    VectorDataTypeFloat32  VectorDataType = "FLOAT32"
    VectorDataTypeFloat16  VectorDataType = "FLOAT16"
    VectorDataTypeBFloat16 VectorDataType = "BFLOAT16"
    VectorDataTypeInt8     VectorDataType = "INT8"
    VectorDataTypeBinary   VectorDataType = "BINARY"
    VectorDataTypeSparse   VectorDataType = "SPARSE"
)

// IsVectorType 检查是否为向量类型
func (c ColumnInfo) IsVectorType() bool {
    return c.VectorDim > 0
}

// ColumnInfo 扩展现有结构
// 在 ColumnInfo 中添加以下字段：
// VectorDim    int              `json:"vector_dim,omitempty"`      // 向量维度
// VectorType   VectorDataType   `json:"vector_type,omitempty"`     // 向量数据类型
```

**修改点**:
```go
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
    
    // ========== 新增字段 ==========
    VectorDim    int              `json:"vector_dim,omitempty"`      // 向量维度
    VectorType   VectorDataType   `json:"vector_type,omitempty"`     // 向量数据类型
}
```

#### 2.1.2 扩展索引类型

**文件**: `pkg/resource/domain/models.go`

```go
// IndexType 扩展索引类型
// 在 IndexType 常量中添加：
const (
    IndexTypeBTree    IndexType = "btree"
    IndexTypeHash     IndexType = "hash"
    IndexTypeFullText IndexType = "fulltext"
    // ========== 新增 ==========
    IndexTypeVector   IndexType = "vector"  // 向量索引
)

// VectorIndexInfo 向量索引扩展信息
type VectorIndexInfo struct {
    MetricType string                 `json:"metric_type"`  // cosine, l2, ip
    IndexAlgo  string                 `json:"index_algo"`   // hnsw, ivf_flat, flat
    Dimension  int                    `json:"dimension"`
    Params     map[string]interface{} `json:"params"`       // 算法特定参数
}

// Index 扩展现有结构
type Index struct {
    Name     string      `json:"name"`
    Table    string      `json:"table"`
    Columns  []string    `json:"columns"`
    Type     IndexType   `json:"type"`
    Unique   bool        `json:"unique"`
    Primary  bool        `json:"primary"`
    Enabled  bool        `json:"enabled"`
    
    // ========== 新增字段 ==========
    VectorInfo *VectorIndexInfo `json:"vector_info,omitempty"`  // 向量索引特有信息
}
```

**实施检查点**:
- [ ] 修改 `pkg/resource/domain/models.go`
- [ ] 运行 `go build ./pkg/resource/domain/` 确认无编译错误
- [ ] 运行 `go test ./pkg/resource/domain/...` 确认测试通过

---

### 阶段 2: 距离函数实现（Week 1-2）

#### 2.2.1 创建距离函数模块

**目录结构**:
```
pkg/builtin/vector/
├── distance/
│   ├── interface.go      # 接口定义
│   ├── registry.go       # 注册中心
│   ├── cosine.go         # 余弦距离
│   ├── l2.go             # 欧氏距离
│   ├── inner_product.go  # 内积
│   └── init.go           # 自动注册
```

**文件**: `pkg/builtin/vector/distance/interface.go`

```go
package distance

// Func 距离函数接口
type Func interface {
    Name() string
    Compute(v1, v2 []float32) float32
    IsSimilarity() bool  // true = 越大越相似, false = 越小越相似
}

// BatchFunc 批量距离计算接口（性能优化）
type BatchFunc interface {
    Func
    BatchCompute(query []float32, vectors [][]float32) []float32
}
```

**文件**: `pkg/builtin/vector/distance/registry.go`

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

// MustGet 必须获取（不存在则 panic）
func MustGet(name string) Func {
    fn, err := Get(name)
    if err != nil {
        panic(err)
    }
    return fn
}

// ListAll 列出所有
func ListAll() []string {
    mu.RLock()
    defer mu.RUnlock()
    
    names := make([]string, 0, len(registry))
    for name := range registry {
        names = append(names, name)
    }
    return names
}
```

**文件**: `pkg/builtin/vector/distance/cosine.go`

```go
package distance

import (
    "math"
)

func init() {
    Register("cosine", &cosine{})
    Register("cos", &cosine{}) // 别名
}

type cosine struct{}

func (c *cosine) Name() string { return "cosine" }

func (c *cosine) Compute(v1, v2 []float32) float32 {
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
        return 1.0
    }
    
    similarity := dot / (math.Sqrt(norm1) * math.Sqrt(norm2))
    return float32(1.0 - similarity)  // 转为距离
}

func (c *cosine) IsSimilarity() bool { return false }
```

**文件**: `pkg/builtin/vector/distance/l2.go`

```go
package distance

import (
    "math"
)

func init() {
    Register("l2", &l2{})
    Register("euclidean", &l2{})
}

type l2 struct{}

func (l *l2) Name() string { return "l2" }

func (l *l2) Compute(v1, v2 []float32) float32 {
    var sum float64
    for i := 0; i < len(v1); i++ {
        diff := float64(v1[i] - v2[i])
        sum += diff * diff
    }
    return float32(math.Sqrt(sum))
}

func (l *l2) IsSimilarity() bool { return false }
```

**文件**: `pkg/builtin/vector/distance/inner_product.go`

```go
package distance

func init() {
    Register("ip", &innerProduct{})
    Register("inner_product", &innerProduct{})
}

type innerProduct struct{}

func (ip *innerProduct) Name() string { return "inner_product" }

func (ip *innerProduct) Compute(v1, v2 []float32) float32 {
    var sum float32
    for i := 0; i < len(v1); i++ {
        sum += v1[i] * v2[i]
    }
    return -sum  // 取负转为距离
}

func (ip *innerProduct) IsSimilarity() bool { return false }
```

**文件**: `pkg/builtin/vector/distance/init.go`

```go
package distance

// 确保所有距离函数在导入时注册
// 实际注册在各算法的 init() 中
```

#### 2.2.2 集成到 Builtin 注册中心

**文件**: `pkg/builtin/functions.go`

```go
// 添加导入
import "github.com/kasuganosora/sqlexec/pkg/builtin/vector/distance"

// 在 init() 中初始化向量函数
func init() {
    initVectorFunctions()
}

func initVectorFunctions() {
    // VEC_COSINE_DISTANCE
    RegisterGlobal(&FunctionInfo{
        Name: "VEC_COSINE_DISTANCE",
        Type: FunctionTypeScalar,
        Signatures: []FunctionSignature{
            {Name: "VEC_COSINE_DISTANCE", ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
        },
        Handler: func(args []interface{}) (interface{}, error) {
            v1, err := toFloatSlice(args[0])
            if err != nil {
                return nil, err
            }
            v2, err := toFloatSlice(args[1])
            if err != nil {
                return nil, err
            }
            return distance.MustGet("cosine").Compute(v1, v2), nil
        },
        Description: "计算余弦距离",
        Category:    "vector",
    })
    
    // VEC_L2_DISTANCE
    RegisterGlobal(&FunctionInfo{
        Name: "VEC_L2_DISTANCE",
        Type: FunctionTypeScalar,
        Signatures: []FunctionSignature{
            {Name: "VEC_L2_DISTANCE", ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
        },
        Handler: func(args []interface{}) (interface{}, error) {
            v1, _ := toFloatSlice(args[0])
            v2, _ := toFloatSlice(args[1])
            return distance.MustGet("l2").Compute(v1, v2), nil
        },
        Description: "计算欧氏距离",
        Category:    "vector",
    })
    
    // VEC_INNER_PRODUCT
    RegisterGlobal(&FunctionInfo{
        Name: "VEC_INNER_PRODUCT",
        Type: FunctionTypeScalar,
        Signatures: []FunctionSignature{
            {Name: "VEC_INNER_PRODUCT", ReturnType: "float", ParamTypes: []string{"vector", "vector"}},
        },
        Handler: func(args []interface{}) (interface{}, error) {
            v1, _ := toFloatSlice(args[0])
            v2, _ := toFloatSlice(args[1])
            return distance.MustGet("ip").Compute(v1, v2), nil
        },
        Description: "计算内积",
        Category:    "vector",
    })
}

// 辅助函数
func toFloatSlice(v interface{}) ([]float32, error) {
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
    default:
        return nil, fmt.Errorf("cannot convert %T to float32 slice", v)
    }
}
```

**实施检查点**:
- [ ] 创建 `pkg/builtin/vector/distance/` 目录
- [ ] 实现所有距离函数
- [ ] 运行 `go test ./pkg/builtin/...`
- [ ] 验证距离计算正确性

---

### 阶段 3: SQL 解析扩展（Week 2-3）

#### 2.3.1 扩展 Parser 类型定义

**文件**: `pkg/parser/types.go`

```go
// SQLType 扩展
const (
    // ... 现有类型 ...
    
    // ========== 新增 ==========
    SQLTypeCreateVectorIndex SQLType = "CREATE_VECTOR_INDEX"
    SQLTypeDropVectorIndex   SQLType = "DROP_VECTOR_INDEX"
)

// CreateVectorIndexStatement 创建向量索引语句
type CreateVectorIndexStatement struct {
    IndexName    string
    TableName    string
    ColumnName   string
    MetricType   string  // COSINE, L2, IP
    IndexAlgo    string  // HNSW, IVF_FLAT, etc.
    Params       map[string]interface{}
    IfNotExists  bool
}

// DropVectorIndexStatement 删除向量索引语句
type DropVectorIndexStatement struct {
    IndexName string
    TableName string
    IfExists  bool
}
```

#### 2.3.2 扩展 SQLAdapter

**文件**: `pkg/parser/adapter.go`

```go
// convertToStatement 扩展 switch 语句
case *ast.CreateIndexStmt:
    // 检查是否为向量索引
    if stmt.IndexOption != nil && stmt.IndexOption.Tp == ast.IndexTypeVector {
        vectorStmt, err := a.convertCreateVectorIndexStmt(stmtNode)
        if err != nil {
            return nil, err
        }
        stmt.Type = SQLTypeCreateVectorIndex
        stmt.CreateVectorIndex = vectorStmt
    } else {
        // 原有逻辑
    }

// convertCreateVectorIndexStmt 转换创建向量索引语句
func (a *SQLAdapter) convertCreateVectorIndexStmt(stmt *ast.CreateIndexStmt) (*CreateVectorIndexStatement, error) {
    result := &CreateVectorIndexStatement{
        IndexName: stmt.IndexName.String(),
        TableName: stmt.Table.Name.String(),
        Params:    make(map[string]interface{}),
    }
    
    // 提取列名
    if lenStmt.IndexPartSpecifications > 0 {
        spec := stmt.IndexPartSpecifications[0]
        if expr, ok := spec.Expr.(*ast.FuncCallExpr); ok {
            // 解析 VEC_COSINE_DISTANCE(column) 格式
            result.MetricType = extractMetricType(expr.FnName.String())
            if len(expr.Args) > 0 {
                if col, ok := expr.Args[0].(*ast.ColumnNameExpr); ok {
                    result.ColumnName = col.Name.Name.String()
                }
            }
        }
    }
    
    // 提取索引算法（USING HNSW）
    if stmt.IndexOption != nil {
        result.IndexAlgo = stmt.IndexOption.Tp.String()
    }
    
    return result, nil
}

// extractMetricType 提取距离度量类型
func extractMetricType(funcName string) string {
    switch strings.ToUpper(funcName) {
    case "VEC_COSINE_DISTANCE":
        return "COSINE"
    case "VEC_L2_DISTANCE":
        return "L2"
    case "VEC_INNER_PRODUCT":
        return "IP"
    default:
        return "COSINE"
    }
}
```

#### 2.3.3 VECTOR 类型解析

**文件**: `pkg/parser/adapter.go`

```go
// convertCreateTableStmt 扩展列类型解析
func (a *SQLAdapter) convertColumnDef(col *ast.ColumnDef) (ColumnInfo, error) {
    colInfo := ColumnInfo{
        Name:     col.Name.Name.String(),
        Type:     simplifyTypeName(col.Tp.String()),
        Nullable: true,
    }
    
    // 检查是否为向量类型
    if isVectorType(col.Tp) {
        dim, vtype, err := parseVectorType(col.Tp)
        if err != nil {
            return colInfo, err
        }
        colInfo.VectorDim = dim
        colInfo.VectorType = vtype
    }
    
    // ... 原有逻辑 ...
}

// isVectorType 检查是否为向量类型
func isVectorType(tp *types.FieldType) bool {
    return tp.Tp == mysql.TypeVector || strings.HasPrefix(tp.String(), "VECTOR")
}

// parseVectorType 解析向量类型
// VECTOR(768) -> dim=768, type=FLOAT32
// VECTOR(768, FLOAT16) -> dim=768, type=FLOAT16
func parseVectorType(tp *types.FieldType) (int, domain.VectorDataType, error) {
    // 解析维度
    if len(tp.Elems) > 0 {
        dim, err := strconv.Atoi(tp.Elems[0])
        if err != nil {
            return 0, "", err
        }
        
        vtype := domain.VectorDataTypeFloat32
        if len(tp.Elems) > 1 {
            vtype = domain.VectorDataType(tp.Elems[1])
        }
        
        return dim, vtype, nil
    }
    
    return 0, "", fmt.Errorf("invalid vector type")
}
```

**实施检查点**:
- [ ] 扩展 `pkg/parser/types.go`
- [ ] 扩展 `pkg/parser/adapter.go`
- [ ] 添加测试用例
- [ ] 运行 `go test ./pkg/parser/...`

---

### 阶段 4: DataAccess 向量扩展（Week 3-5）

#### 2.4.1 索引注册中心

**文件**: `pkg/dataaccess/index/registry.go`

```go
package index

import (
    "fmt"
    "sync"
)

// Type 索引类型
type Type string

const (
    TypeHNSW    Type = "hnsw"
    TypeIVFFlat Type = "ivf_flat"
    TypeFlat    Type = "flat"
)

// Factory 工厂函数
type Factory func(config *Config) (VectorIndex, error)

var (
    factories = make(map[Type]Factory)
    mu        sync.RWMutex
)

// Register 注册索引类型
func Register(t Type, f Factory) {
    mu.Lock()
    defer mu.Unlock()
    
    if _, exists := factories[t]; exists {
        panic(fmt.Sprintf("index type %s already registered", t))
    }
    factories[t] = f
}

// Create 创建索引
func Create(t Type, config *Config) (VectorIndex, error) {
    mu.RLock()
    defer mu.RUnlock()
    
    f, ok := factories[t]
    if !ok {
        return nil, fmt.Errorf("unknown index type: %s", t)
    }
    return f(config)
}
```

#### 2.4.2 索引接口定义

**文件**: `pkg/dataaccess/index/interface.go`

```go
package index

import (
    "context"
    
    "github.com/kasuganosora/sqlexec/pkg/builtin/vector/distance"
)

// VectorIndex 向量索引接口
type VectorIndex interface {
    // Add 添加向量
    Add(id int64, vector []float32) error
    
    // BatchAdd 批量添加
    BatchAdd(ids []int64, vectors [][]float32) error
    
    // Search 搜索最近邻
    Search(ctx context.Context, query []float32, k int, filter *Filter) (*Result, error)
    
    // Delete 删除
    Delete(id int64) error
    
    // Stats 统计信息
    Stats() Stats
    
    // Close 关闭
    Close() error
}

// Filter 搜索过滤器
type Filter struct {
    IDs []int64  // 允许的行ID列表
}

// Result 搜索结果
type Result struct {
    IDs       []int64
    Distances []float32
}

// Stats 统计信息
type Stats struct {
    Type       Type
    Distance   string
    Dimension  int
    Count      int64
    MemorySize int64
}

// Config 索引配置
type Config struct {
    Dimension  int
    DistanceFn distance.Func
    MetricType string
    Params     interface{}
}
```

#### 2.4.3 HNSW 实现

**文件**: `pkg/dataaccess/index/types/hnsw/hnsw.go`

```go
package hnsw

import (
    "context"
    "math"
    "math/rand"
    "sync"
    
    "github.com/kasuganosora/sqlexec/pkg/builtin/vector/distance"
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index"
)

func init() {
    index.Register(index.TypeHNSW, func(config *index.Config) (index.VectorIndex, error) {
        params := DefaultParams()
        if config.Params != nil {
            if p, ok := config.Params.(*Params); ok {
                params = p
            }
        }
        return New(config.Dimension, config.DistanceFn, params), nil
    })
}

// HNSW 索引
type HNSW struct {
    dimension int
    distFn    distance.Func
    params    *Params
    
    nodes     map[int64]*Node
    entryPoint int64
    maxLevel   int
    
    mu        sync.RWMutex
}

// New 创建 HNSW
func New(dimension int, distFn distance.Func, params *Params) *HNSW {
    return &HNSW{
        dimension: dimension,
        distFn:    distFn,
        params:    params,
        nodes:     make(map[int64]*Node),
    }
}

// Add 添加向量
func (h *HNSW) Add(id int64, vector []float32) error {
    if len(vector) != h.dimension {
        return fmt.Errorf("dimension mismatch")
    }
    
    h.mu.Lock()
    defer h.mu.Unlock()
    
    level := h.randomLevel()
    node := NewNode(id, vector, level)
    
    // 插入逻辑...
    h.nodes[id] = node
    
    return nil
}

// BatchAdd 批量添加
func (h *HNSW) BatchAdd(ids []int64, vectors [][]float32) error {
    for i, id := range ids {
        if err := h.Add(id, vectors[i]); err != nil {
            return err
        }
    }
    return nil
}

// Search 搜索
func (h *HNSW) Search(ctx context.Context, query []float32, k int, filter *index.Filter) (*index.Result, error) {
    // 实现搜索逻辑
    return &index.Result{}, nil
}

// Delete 删除
func (h *HNSW) Delete(id int64) error {
    h.mu.Lock()
    defer h.mu.Unlock()
    delete(h.nodes, id)
    return nil
}

// Stats 统计
func (h *HNSW) Stats() index.Stats {
    h.mu.RLock()
    defer h.mu.RUnlock()
    
    return index.Stats{
        Type:      index.TypeHNSW,
        Distance:  h.distFn.Name(),
        Dimension: h.dimension,
        Count:     int64(len(h.nodes)),
    }
}

// Close 关闭
func (h *HNSW) Close() error {
    h.mu.Lock()
    defer h.mu.Unlock()
    h.nodes = nil
    return nil
}

// randomLevel 随机层数
func (h *HNSW) randomLevel() int {
    level := 0
    for rand.Float64() < h.params.LevelProbability && level < h.params.MaxLevel {
        level++
    }
    return level
}
```

#### 2.4.4 向量搜索服务扩展

**文件**: `pkg/dataaccess/vector_service.go`

```go
package dataaccess

import (
    "context"
    "fmt"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess/index"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VectorSearchOptions 向量搜索选项
type VectorSearchOptions struct {
    TableName   string
    ColumnName  string
    QueryVector []float32
    K           int
    Metric      string
    IndexType   index.Type
    Filter      *domain.Filter
}

// VectorSearchResult 结果
type VectorSearchResult struct {
    Rows      []domain.Row
    Distances []float32
}

// VectorService 向量搜索服务
type VectorService struct {
    base   Service
    idxMgr *IndexManager
}

// NewVectorService 创建服务
func NewVectorService(base Service) *VectorService {
    return &VectorService{
        base:   base,
        idxMgr: NewIndexManager(),
    }
}

// Search 向量搜索
func (vs *VectorService) Search(ctx context.Context, opts *VectorSearchOptions) (*VectorSearchResult, error) {
    // 获取或构建索引
    idxKey := IndexKey{Table: opts.TableName, Column: opts.ColumnName}
    idx, err := vs.idxMgr.GetOrBuild(ctx, idxKey, func() (*IndexMeta, error) {
        return vs.buildIndex(ctx, opts)
    })
    if err != nil {
        return nil, err
    }
    
    // 执行搜索
    result, err := idx.Search(ctx, opts.QueryVector, opts.K, nil)
    if err != nil {
        return nil, err
    }
    
    // 获取行数据
    rows, err := vs.getRowsByIDs(ctx, opts.TableName, result.IDs)
    if err != nil {
        return nil, err
    }
    
    return &VectorSearchResult{
        Rows:      rows,
        Distances: result.Distances,
    }, nil
}

// buildIndex 构建索引
func (vs *VectorService) buildIndex(ctx context.Context, opts *VectorSearchOptions) (*IndexMeta, error) {
    // 读取表数据
    result, err := vs.base.Query(ctx, opts.TableName, &QueryOptions{
        SelectColumns: []string{opts.ColumnName, "id"},
    })
    if err != nil {
        return nil, err
    }
    
    // 提取向量和ID
    var ids []int64
    var vectors [][]float32
    
    for _, row := range result.Rows {
        vec, err := extractVector(row[opts.ColumnName])
        if err != nil {
            continue
        }
        
        id := extractID(row["id"])
        
        ids = append(ids, id)
        vectors = append(vectors, vec)
    }
    
    // 获取距离函数
    distFn, err := distance.Get(opts.Metric)
    if err != nil {
        return nil, err
    }
    
    // 创建索引
    idxConfig := &index.Config{
        Dimension:  len(vectors[0]),
        DistanceFn: distFn,
        MetricType: opts.Metric,
    }
    
    idx, err := index.Create(opts.IndexType, idxConfig)
    if err != nil {
        return nil, err
    }
    
    // 批量添加
    if err := idx.BatchAdd(ids, vectors); err != nil {
        return nil, err
    }
    
    return &IndexMeta{
        Index:     idx,
        Dimension: len(vectors[0]),
        Metric:    opts.Metric,
    }, nil
}
```

**实施检查点**:
- [ ] 创建 `pkg/dataaccess/index/` 目录结构
- [ ] 实现 HNSW 和 Flat 索引
- [ ] 实现 VectorService
- [ ] 集成测试

---

### 阶段 5: 优化器集成（Week 5-7）

参考 `pkg/optimizer/spatial_index_support.go` 的实现模式。

#### 2.5.1 向量索引支持模块

**文件**: `pkg/optimizer/vector_index_support.go`

```go
package optimizer

import (
    "github.com/kasuganosora/sqlexec/pkg/parser"
)

// VectorIndexSupport 向量索引支持
type VectorIndexSupport struct{}

// IsVectorSearch 检查是否为向量搜索模式
// 模式: ORDER BY VEC_xxx_DISTANCE(...) LIMIT n
func (v *VectorIndexSupport) IsVectorSearch(stmt *SQLStatement) (bool, *VectorSearchInfo) {
    if stmt.Select == nil || stmt.Select.Limit == nil {
        return false, nil
    }
    
    if len(stmt.Select.OrderBy) != 1 {
        return false, nil
    }
    
    orderExpr := stmt.Select.OrderBy[0].Expr
    if orderExpr == nil || orderExpr.Type != parser.ExprTypeFunction {
        return false, nil
    }
    
    // 检查是否为距离函数
    var metric string
    switch orderExpr.Function {
    case "VEC_COSINE_DISTANCE":
        metric = "cosine"
    case "VEC_L2_DISTANCE":
        metric = "l2"
    case "VEC_INNER_PRODUCT":
        metric = "ip"
    default:
        return false, nil
    }
    
    // 提取列名和查询向量
    if len(orderExpr.Args) < 2 {
        return false, nil
    }
    
    columnName := orderExpr.Args[0].Column
    queryVector := extractVector(orderExpr.Args[1])
    
    return true, &VectorSearchInfo{
        TableName:   stmt.Select.From,
        ColumnName:  columnName,
        QueryVector: queryVector,
        K:           int(*stmt.Select.Limit),
        Metric:      metric,
    }
}

// VectorSearchInfo 向量搜索信息
type VectorSearchInfo struct {
    TableName   string
    ColumnName  string
    QueryVector []float32
    K           int
    Metric      string
}
```

#### 2.5.2 向量扫描算子

**文件**: `pkg/optimizer/logical_vector_scan.go`

```go
package optimizer

// LogicalVectorScan 向量扫描逻辑算子
type LogicalVectorScan struct {
    baseLogicalOperator
    
    TableName   string
    ColumnName  string
    QueryVector []float32
    K           int
    Metric      string
}

// EstimateCost 估算成本
func (l *LogicalVectorScan) EstimateCost() float64 {
    // 向量索引搜索成本：O(log N) 或 O(1)
    return float64(l.K) * 10
}
```

#### 2.5.3 优化规则

**文件**: `pkg/optimizer/rules_vector.go`

```go
package optimizer

// VectorIndexRewrite 向量索引重写规则
func (r *VectorIndexRewrite) Apply(node LogicalOperator) (LogicalOperator, bool) {
    // 查找 Sort + Limit 模式
    sortOp, ok := node.(*LogicalSort)
    if !ok {
        return node, false
    }
    
    // 检查是否为向量搜索
    support := &VectorIndexSupport{}
    isVector, info := support.IsVectorSearch(&SQLStatement{
        Select: &SelectStatement{
            OrderBy: sortOp.OrderBy,
            Limit:   sortOp.Limit,
        },
    })
    
    if !isVector {
        return node, false
    }
    
    // 替换为向量扫描
    return &LogicalVectorScan{
        TableName:   info.TableName,
        ColumnName:  info.ColumnName,
        QueryVector: info.QueryVector,
        K:           info.K,
        Metric:      info.Metric,
    }, true
}
```

**实施检查点**:
- [ ] 实现向量索引支持
- [ ] 添加优化规则
- [ ] 测试优化器改写

---

### 阶段 6: 执行器集成（Week 7-8）

#### 2.6.1 向量搜索算子

**文件**: `pkg/executor/operators/vector_search.go`

```go
package operators

import (
    "context"
    
    "github.com/kasuganosora/sqlexec/pkg/dataaccess"
    "github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
    "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VectorSearchOperator 向量搜索算子
type VectorSearchOperator struct {
    *BaseOperator
    config *plan.VectorSearchPlan
}

// Execute 执行
func (v *VectorSearchOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
    // 从上下文获取 VectorService
    vectorSvc := v.GetVectorService()
    
    result, err := vectorSvc.Search(ctx, &dataaccess.VectorSearchOptions{
        TableName:   v.config.TableName,
        ColumnName:  v.config.ColumnName,
        QueryVector: v.config.QueryVector,
        K:           v.config.K,
        Metric:      v.config.Metric,
    })
    if err != nil {
        return nil, err
    }
    
    return &domain.QueryResult{
        Rows: result.Rows,
    }, nil
}
```

---

### 阶段 7: 测试与优化（Week 8-10）

#### 2.7.1 测试策略

```
1. 单元测试
   - pkg/builtin/vector/distance/... - 距离函数正确性
   - pkg/dataaccess/index/types/... - 索引正确性
   - pkg/optimizer/... - 优化规则

2. 集成测试
   - 端到端向量搜索
   - 不同数据源（CSV、Memory）
   - 不同索引类型（HNSW、Flat）

3. 性能测试
   - 索引构建速度
   - 查询延迟（P50、P95、P99）
   - 召回率测试
```

#### 2.7.2 性能基准

| 指标 | 目标值 |
|------|-------|
| 索引构建 | 10,000 向量/秒 |
| HNSW 查询 P99 | < 5ms (100万向量) |
| Flat 查询 P99 | < 100ms (10万向量) |
| 召回率 | > 95% |

---

## 3. 风险与缓解

### 风险 1: 索引接口不一致

**风险**: 现有内存索引与向量索引接口不统一

**缓解**:
```go
// 保持 VectorIndex 接口独立，不强制统一
// 内存索引 (BTree/Hash) 保持现有接口
// 向量索引使用新的 VectorIndex 接口
// 在 DataAccess 层做适配
```

### 风险 2: 内存占用过高

**风险**: 大维度向量索引占用过多内存

**缓解**:
- 支持磁盘索引（DiskANN）
- 向量量化（Float16、Int8）
- 索引分片

### 风险 3: 数据源变更检测

**风险**: CSV/JSON 文件变更后索引过期

**缓解**:
```go
// 使用文件修改时间或数据哈希作为版本
func getDataVersion(ds DataSource, table string) string {
    // 对于文件：mtime + size
    // 对于数据库：row count + max(id)
}
```

---

## 4. 验收标准

### 功能验收

- [ ] 支持 `CREATE TABLE ... (embedding VECTOR(768))`
- [ ] 支持 `CREATE VECTOR INDEX ... USING HNSW`
- [ ] 支持 `SELECT ... ORDER BY VEC_COSINE_DISTANCE(embedding, ...) LIMIT 10`
- [ ] 支持 CSV、JSON、Memory 数据源的向量搜索

### 性能验收

- [ ] 100万向量 HNSW 查询 P99 < 10ms
- [ ] 召回率 > 95%

### 代码验收

- [ ] 新增索引类型无需修改现有代码（通过 Register）
- [ ] 新增距离算法无需修改现有代码（通过 Register）
- [ ] 测试覆盖率 > 80%

---

## 5. 附录：完整文件清单

### 新增文件

```
pkg/resource/domain/models.go                          # 扩展（已有文件修改）
pkg/builtin/vector/distance/interface.go              # 新增
pkg/builtin/vector/distance/registry.go               # 新增
pkg/builtin/vector/distance/cosine.go                 # 新增
pkg/builtin/vector/distance/l2.go                     # 新增
pkg/builtin/vector/distance/inner_product.go          # 新增
pkg/parser/types.go                                   # 扩展
pkg/parser/adapter.go                                 # 扩展
pkg/dataaccess/index/registry.go                      # 新增
pkg/dataaccess/index/interface.go                     # 新增
pkg/dataaccess/index/types/hnsw/hnsw.go               # 新增
pkg/dataaccess/index/types/hnsw/params.go             # 新增
pkg/dataaccess/index/types/hnsw/node.go               # 新增
pkg/dataaccess/index/types/flat/flat.go               # 新增
pkg/dataaccess/vector_service.go                      # 新增
pkg/dataaccess/index_manager.go                       # 新增
pkg/optimizer/vector_index_support.go                 # 新增
pkg/optimizer/logical_vector_scan.go                  # 新增
pkg/optimizer/rules_vector.go                         # 新增
pkg/executor/operators/vector_search.go               # 新增
```

### 修改文件

```
pkg/builtin/functions.go                              # 添加向量函数
pkg/optimizer/rules.go                                # 注册向量优化规则
```
