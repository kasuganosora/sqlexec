# SQLExec 向量搜索系统技术原理

## 目录

1. [概述](#概述)
2. [向量数据模型](#向量数据模型)
3. [距离度量](#距离度量)
4. [向量索引类型](#向量索引类型)
5. [SQL 接口](#sql-接口)
6. [索引构建与查询流程](#索引构建与查询流程)
7. [与查询优化器的集成](#与查询优化器的集成)
8. [架构总览](#架构总览)

---

## 概述

SQLExec 内置了一套完整的向量搜索引擎，支持高维向量的存储、索引和近似最近邻（ANN）检索。该系统覆盖了从 SQL 解析、向量列定义、索引创建到查询优化器自动改写的全链路，用户可通过标准 SQL 语法完成向量搜索操作。

核心能力：
- 支持 `VECTOR(dim)` 列类型定义任意维度的 float32 向量
- 提供 10 种向量索引类型，涵盖精确搜索和多种近似搜索算法
- 支持余弦距离、L2 距离、内积三种距离度量
- 查询优化器自动将 `ORDER BY vec_distance(...) LIMIT k` 改写为 VectorScan 算子

---

## 向量数据模型

### 列定义

向量列通过 `ColumnInfo` 结构体中的两个专用字段描述：

```go
// 文件: pkg/resource/domain/models.go

type ColumnInfo struct {
    Name         string           `json:"name"`
    Type         string           `json:"type"`
    // ... 其他字段 ...

    // Vector Columns 支持
    VectorDim  int    `json:"vector_dim,omitempty"`   // 向量维度
    VectorType string `json:"vector_type,omitempty"`  // 向量类型（如 "float32"）
}

// IsVectorType 检查是否为向量类型
func (c ColumnInfo) IsVectorType() bool {
    return c.VectorDim > 0
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `VectorDim` | `int` | 向量维度，例如 128、768、1536 |
| `VectorType` | `string` | 向量元素类型，当前统一为 `"float32"` |

当 `VectorDim > 0` 时，该列被识别为向量列。解析器在遇到 `VECTOR(dim)` 类型时，自动设置 `Type = "VECTOR"`、`VectorDim = dim`、`VectorType = "float32"`。

### 向量索引配置

向量索引通过独立的 `VectorIndexConfig` 结构体配置：

```go
// 文件: pkg/resource/memory/index.go

type VectorIndexConfig struct {
    MetricType VectorMetricType       `json:"metric_type"`  // 距离度量类型
    Dimension  int                    `json:"dimension"`     // 向量维度
    Params     map[string]interface{} `json:"params,omitempty"` // 索引参数
}
```

同时，`domain.VectorIndexConfig` 和 `domain.Index` 中也保存了向量索引的元数据，使得索引信息可在 Schema 层面被持久化和查询。

### 向量记录与搜索结果

```go
// 文件: pkg/resource/memory/vector_index.go

// VectorRecord 向量记录
type VectorRecord struct {
    ID     int64
    Vector []float32
}

// VectorSearchResult 向量搜索结果
type VectorSearchResult struct {
    IDs       []int64
    Distances []float32
}
```

所有向量在内存中以 `[]float32` 格式存储，搜索结果返回 ID 列表和对应的距离值，按距离升序排列。

---

## 距离度量

系统采用注册中心模式管理距离函数，支持运行时扩展。

### 距离函数接口

```go
// 文件: pkg/resource/memory/distance.go

type DistanceFunc interface {
    Name() string
    Compute(v1, v2 []float32) float32
}
```

### 内置度量类型

| 度量类型 | 注册名 | 常量 | 公式 | 特点 |
|---------|--------|------|------|------|
| 余弦距离 | `cosine` | `VectorMetricCosine` | `1 - (v1 . v2) / (||v1|| * ||v2||)` | 值域 [0, 2]，0 表示完全相同 |
| L2 距离 | `l2` | `VectorMetricL2` | `sqrt(sum((v1[i] - v2[i])^2))` | 欧几里得距离，值域 [0, +inf) |
| 内积距离 | `inner_product` | `VectorMetricIP` | `-sum(v1[i] * v2[i])` | 返回负内积，使得距离越小相似度越高 |

### 余弦距离实现细节

余弦距离的计算经过了 SIMD 友好的循环展开优化，每次处理 4 个元素：

```go
// 文件: pkg/resource/memory/distance.go

func (c *cosineDistance) Compute(v1, v2 []float32) float32 {
    n := len(v1)
    var dot, norm1, norm2 float32

    // 4 路展开，帮助编译器进行向量化优化
    i := 0
    for ; i <= n-4; i += 4 {
        a0, a1, a2, a3 := v1[i], v1[i+1], v1[i+2], v1[i+3]
        b0, b1, b2, b3 := v2[i], v2[i+1], v2[i+2], v2[i+3]
        dot += a0*b0 + a1*b1 + a2*b2 + a3*b3
        norm1 += a0*a0 + a1*a1 + a2*a2 + a3*a3
        norm2 += b0*b0 + b1*b1 + b2*b2 + b3*b3
    }
    // 处理剩余元素
    for ; i < n; i++ {
        dot += v1[i] * v2[i]
        norm1 += v1[i] * v1[i]
        norm2 += v2[i] * v2[i]
    }

    if norm1 == 0 || norm2 == 0 {
        return 1.0
    }
    return 1.0 - dot/float32(math.Sqrt(float64(norm1)*float64(norm2)))
}
```

### 注册机制

距离函数在 `init()` 中自动注册到全局注册中心，也支持用户自定义注册：

```go
func init() {
    RegisterDistance("cosine", &cosineDistance{})
    RegisterDistance("l2", &l2Distance{})
    RegisterDistance("inner_product", &innerProductDistance{})
}
```

---

## 向量索引类型

系统定义了统一的 `VectorIndex` 接口，所有索引类型均实现该接口：

```go
// 文件: pkg/resource/memory/vector_index.go

type VectorIndex interface {
    Build(ctx context.Context, loader VectorDataLoader) error
    Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error)
    Insert(id int64, vector []float32) error
    Delete(id int64) error
    GetConfig() *VectorIndexConfig
    Stats() VectorIndexStats
    Close() error
}
```

### 索引类型总览

| 索引类型 | 常量 | 实现文件 | 搜索方式 | 内存占用 | 适用场景 |
|---------|------|---------|---------|---------|---------|
| Flat | `vector_flat` | `flat_index.go` | 精确暴力搜索 | 高（原始向量） | 小数据集、精度基准 |
| HNSW | `vector_hnsw` | `hnsw_index.go` | 图搜索 | 高（原始向量+图） | 通用场景、高召回率 |
| HNSW-SQ | `vector_hnsw_sq` | `hnsw_sq_index.go` | 图搜索+标量量化 | 中（int8向量+图） | 内存受限、精度可接受 |
| HNSW-PQ | `vector_hnsw_pq` | `hnsw_pq_index.go` | 图搜索+乘积量化 | 低（PQ编码+图） | 大数据集、内存敏感 |
| HNSW-PRQ | `vector_hnsw_prq` | `hnsw_prq_index.go` | 图搜索+残差乘积量化 | 低（粗码+残差PQ+图） | 大数据集、高精度要求 |
| IVF-Flat | `vector_ivf_flat` | `ivf_flat_index.go` | 倒排索引+精确搜索 | 高（原始向量+聚类） | 中等数据集 |
| IVF-SQ8 | `vector_ivf_sq8` | `ivf_sq8_index.go` | 倒排索引+标量量化 | 中 | 大数据集、内存受限 |
| IVF-PQ | `vector_ivf_pq` | `ivf_pq_index.go` | 倒排索引+乘积量化 | 低 | 超大数据集 |
| IVF-RaBitQ | `vector_ivf_rabitq` | `ivf_rabitq_index.go` | 倒排索引+二进制量化 | 极低（1 bit/维度） | 超大数据集、极致压缩 |
| AISAQ | `vector_aisaq` | `aisaq_index.go` | Vamana图+自适应量化 | 中 | 自适应场景 |

### 4.1 Flat 索引（精确搜索）

Flat 索引是最简单的索引类型，存储所有原始向量，查询时遍历计算距离并排序取 Top-K：

```go
// 文件: pkg/resource/memory/flat_index.go

func (f *FlatIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
    candidates := make([]idDist, 0, len(f.vectors))
    for id, vec := range f.vectors {
        dist := f.distFunc.Compute(query, vec)
        candidates = append(candidates, idDist{id, dist})
    }
    sort.Slice(candidates, func(i, j int) bool {
        return candidates[i].dist < candidates[j].dist
    })
    if len(candidates) > k {
        candidates = candidates[:k]
    }
    // ... 构建结果 ...
}
```

**时间复杂度**: O(n * d)，其中 n 为向量数量，d 为维度。
**空间复杂度**: O(n * d * 4) 字节（float32）。

### 4.2 HNSW 索引

HNSW（Hierarchical Navigable Small World）是基于论文 *Malkov & Yashunin, 2016* 实现的多层图索引。

#### 核心数据结构

```go
// 文件: pkg/resource/memory/hnsw_index.go

type HNSWIndex struct {
    vectors    map[int64][]float32        // 向量存储
    layers     []map[int64][]int64        // 多层图：layers[level][nodeID] = 邻居ID列表
    nodeLevel  map[int64]int              // 节点所在最高层级
    entryPoint int64                       // 全局入口点
    entryLevel int                         // 入口点层级
}
```

#### 默认参数

```go
var DefaultHNSWParams = HNSWParams{
    M:              16,     // 每个节点每层最大邻居数
    EFConstruction: 200,    // 构建时的搜索宽度
    EFSearch:       256,    // 查询时的搜索宽度
    ML:             0.25,   // 层级生成因子 (~1/ln(16))
}
```

#### 层级生成

每个新节点的层级通过几何分布随机生成：

```go
func (h *HNSWIndex) randomLevel() int {
    level := 0
    for h.rng.Float64() < DefaultHNSWParams.ML && level < 16 {
        level++
    }
    return level
}
```

概率 `ML = 0.25` 意味着约 75% 的节点仅在第 0 层，约 18.75% 在第 0-1 层，以此类推。这确保了高层的稀疏性，实现了 O(log n) 的搜索路径。

#### 插入算法

插入过程分为两个阶段：

**阶段一 -- 贪心下降**: 从入口点所在的最高层开始，沿图边贪心地找到离插入向量最近的节点，逐层向下直到插入节点所在的层。

**阶段二 -- 连接构建**: 从插入节点所在层到第 0 层，在每层执行 Beam Search 找到 `efConstruction` 个候选邻居，使用启发式邻居选择算法选出最终邻居，并建立双向连接。

```go
func (h *HNSWIndex) insertInternal(id int64, vector []float32) {
    level := h.randomLevel()

    // 阶段1: 从顶层贪心下降到 level+1
    ep := h.entryPoint
    for l := h.entryLevel; l > level; l-- {
        ep = h.greedyClosest(vector, ep, l)
    }

    // 阶段2: 从 min(level, epLevel) 到第 0 层，搜索并连接
    for l := topInsertLevel; l >= 0; l-- {
        candidates := h.searchLevel(vector, ep, DefaultHNSWParams.EFConstruction, l)
        neighbors := h.selectNeighbors(vector, candidates, maxConn)

        // 建立双向连接
        h.layers[l][id] = neighbors
        for _, neighborID := range neighbors {
            nNeighbors := h.layers[l][neighborID]
            nNeighbors = append(nNeighbors, id)
            if len(nNeighbors) > maxConn {
                nNeighbors = h.pruneNeighbors(neighborID, nNeighbors, maxConn)
            }
            h.layers[l][neighborID] = nNeighbors
        }
    }
}
```

#### 启发式邻居选择

HNSW 使用论文中的 Algorithm 4 进行邻居选择。核心思想是：优先选择那些不仅距离查询点近，而且彼此之间距离也足够远的邻居，以确保图的多样性和连通性。

```go
func (h *HNSWIndex) selectNeighbors(query []float32, candidates []hnswCandidate, m int) []int64 {
    selected := make([]int64, 0, m)
    selectedVecs := make([][]float32, 0, m)

    for _, c := range candidates {
        if len(selected) >= m {
            break
        }
        cVec := h.vectors[c.id]

        // 检查候选是否比所有已选邻居更近
        good := true
        for _, sVec := range selectedVecs {
            if h.distFunc.Compute(cVec, sVec) < c.dist {
                good = false
                break
            }
        }
        if good {
            selected = append(selected, c.id)
            selectedVecs = append(selectedVecs, cVec)
        }
    }
    // 如果启发式没有填满，用最近的候选补充
    // ...
}
```

#### 搜索算法

搜索同样分为两个阶段：

```go
func (h *HNSWIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
    ef := DefaultHNSWParams.EFSearch
    if k > ef {
        ef = k
    }

    ep := h.entryPoint

    // 阶段1: 从顶层到第 1 层贪心下降
    for l := h.entryLevel; l >= 1; l-- {
        ep = h.greedyClosest(query, ep, l)
    }

    // 阶段2: 在第 0 层进行 Beam Search，探索 ef 个候选
    candidates := h.searchLevel(query, ep, ef, 0)

    // 过滤并返回 Top-K
    // ...
}
```

**连接数规则**: 第 0 层使用 `Mmax0 = 2*M`，上层使用 `M`。这是因为第 0 层存储所有节点，需要更多连接保证召回率。

### 4.3 HNSW-SQ 索引（标量量化）

HNSW-SQ 将 float32 向量量化为 int8，每个维度的存储从 4 字节降低到 1 字节，压缩比约 4:1。

#### 量化原理

```go
// 文件: pkg/resource/memory/hnsw_sq_index.go

// 训练：计算每个维度的 min/max，得到 scale 和 shift
func (h *HNSWSQIndex) trainScalarQuantizer(records []VectorRecord) error {
    for d := 0; d < dimension; d++ {
        h.scale[d] = 127.0 / (maxs[d] - mins[d])
        h.shift[d] = (maxs[d] + mins[d]) / 2.0
    }
}

// 量化: float32 -> int8
func (h *HNSWSQIndex) quantize(vec []float32) []int8 {
    quantized := make([]int8, len(vec))
    for d := 0; d < len(vec); d++ {
        val := (vec[d] - h.shift[d]) * h.scale[d]
        // 裁剪到 [-128, 127]
        quantized[d] = int8(clamp(val, -128, 127))
    }
    return quantized
}

// 反量化: int8 -> float32
func (h *HNSWSQIndex) dequantize(vec []int8) []float32 {
    dequantized := make([]float32, len(vec))
    for d := 0; d < len(vec); d++ {
        dequantized[d] = float32(vec[d])/h.scale[d] + h.shift[d]
    }
    return dequantized
}
```

图结构保持标准 HNSW 不变，但距离计算使用反量化后的近似值。

### 4.4 HNSW-PQ 索引（乘积量化）

HNSW-PQ 使用乘积量化（Product Quantization）将高维向量分割为多个子向量，每个子向量用 K-Means 聚类得到的码本 ID 表示。

#### PQ 编码过程

```
原始向量 (D 维 float32) -> 分割为 M 个子向量 -> 每个子向量量化为 1 字节码本ID
存储: D * 4 bytes -> M bytes   (压缩比: 4D/M)
```

```go
// 文件: pkg/resource/memory/hnsw_pq_index.go

// 训练乘积量化器：对每个子空间执行 K-Means
func (h *HNSWPQIndex) trainProductQuantizer(records []VectorRecord) error {
    for subq := 0; subq < nsubq; subq++ {
        // 每个子量化器独立执行 K-Means，迭代 20 轮
        err := h.subqKmeans(records, subq, subDim, ksubq)
    }
}

// 编码：将向量映射为 PQ 码
func (h *HNSWPQIndex) encode(vec []float32) []int8 {
    code := make([]int8, nsubq)
    for subq := 0; subq < nsubq; subq++ {
        subvec := vec[subq*subDim : (subq+1)*subDim]
        // 找到最近的质心
        bestCentroid := argmin(distance(subvec, codebook[subq]))
        code[subq] = int8(bestCentroid)
    }
    return code
}

// 近似距离计算
func (h *HNSWPQIndex) computeApproxDistance(query []float32, code []int8) float32 {
    distance := float32(0)
    for subq := 0; subq < nsubq; subq++ {
        subvec := query[subq*subDim : (subq+1)*subDim]
        centroid := codebooks[subq][code[subq]]
        distance += l2(subvec, centroid)
    }
    return distance
}
```

### 4.5 HNSW-PRQ 索引（残差乘积量化）

HNSW-PRQ 是两级量化方案：

1. **粗量化**: 使用 K-Means 将向量分配到 `Kcoarse`（默认 64）个聚类中心
2. **残差 PQ**: 对残差（向量 - 聚类中心）执行乘积量化

```
编码: 粗码(1 byte) + 残差PQ码(M bytes)
近似距离 = distance(query, coarseCodebook[coarseCode] + decodePQ(residualCode))
```

这种两级结构比单层 PQ 有更低的量化误差，特别适合数据分布不均匀的场景。

### 4.6 IVF-Flat 索引

IVF（Inverted File）使用 K-Means 将向量空间划分为 `nlist` 个 Voronoi 区域，搜索时只在最近的 `nprobe` 个区域内执行精确搜索。

```go
// 文件: pkg/resource/memory/ivf_flat_index.go

var DefaultIVFFlatParams = IVFFlatParams{
    Nlist:  128,  // 聚类数量
    Nprobe: 32,   // 搜索时检查的聚类数量
}

func (i *IVFFlatIndex) Search(ctx context.Context, query []float32, k int, filter *VectorFilter) (*VectorSearchResult, error) {
    // 阶段1: 找到 nprobe 个最近的聚类中心
    clusterDists := make([]clusterDist, i.nlist)
    for j, center := range i.centroids {
        dist := i.distFunc.Compute(query, center)
        clusterDists[j] = clusterDist{clusterID: j, distance: dist}
    }
    sort.Slice(clusterDists, ...)

    // 阶段2: 在选中的聚类中精确搜索
    for j := 0; j < nprobe; j++ {
        clusterID := clusterDists[j].clusterID
        for _, rec := range i.vectorsByCluster[clusterID] {
            dist := i.distFunc.Compute(query, rec.Vector)
            candidates = append(candidates, ...)
        }
    }
    // 排序返回 Top-K
}
```

`nprobe/nlist` 的比值控制搜索范围：比值越大，召回率越高但延迟也越高。

### 4.7 IVF-RaBitQ 索引

基于 SIGMOD 2024 论文 *"RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound"* 实现。将 D 维 float32 向量量化为 D 比特的二进制串，每个 uint64 存储 64 个维度。

```go
// 文件: pkg/resource/memory/ivf_rabitq_index.go

type IVFRabitQIndex struct {
    quantizedVectors map[int64][]uint64  // D/64 个 uint64
    projectionMatrix [][]float32          // Johnson-Lindenstrauss 随机投影矩阵
    // ...
}
```

使用 Johnson-Lindenstrauss 变换进行随机投影后取符号位，实现了极致的压缩比（float32 -> 1 bit/维度，压缩 32 倍）。

### 4.8 AISAQ 索引

AISAQ（Adaptive Index Scalar Quantization）基于 Vamana 图结构和自适应标量量化：

```go
// 文件: pkg/resource/memory/aisaq_index.go

type AISAQIndex struct {
    graph map[int64]*vamanaNode  // Vamana 图结构
    quantizedVectors map[int64][]int8  // 自适应标量量化
}

var DefaultAISAQParams = AISAQParams{
    MaxDegree:      56,   // 最大出度
    SearchListSize: 100,  // 搜索列表大小
}
```

Vamana 图是一种单层图结构（相比 HNSW 的多层），具有更简单的实现和更可控的内存占用。

---

## SQL 接口

### 5.1 创建含向量列的表

```sql
-- 创建包含 768 维向量列的表
CREATE TABLE articles (
    id INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    embedding VECTOR(768)
);

-- 创建多向量列的表
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(128),
    image_feature VECTOR(512),
    text_feature VECTOR(1536)
);
```

解析器通过 `isVectorType()` 函数识别 `VECTOR(dim)` 类型，支持 `VECTOR(dim)` 和 `ARRAY<FLOAT, dim>` 两种语法格式：

```go
// 文件: pkg/parser/adapter.go

func isVectorType(typeStr string) bool {
    upperType := strings.ToUpper(typeStr)
    return strings.HasPrefix(upperType, "VECTOR") || strings.HasPrefix(upperType, "ARRAY<")
}
```

### 5.2 创建向量索引

系统支持两种向量索引创建语法：

#### 标准语法（WITH 子句）

```sql
-- 使用 HNSW 索引 + 余弦距离
CREATE VECTOR INDEX idx_embedding ON articles(embedding)
    USING HNSW
    WITH (metric='cosine', dim=768, M=16, ef=200);

-- 使用 IVF-Flat 索引 + L2 距离
CREATE VECTOR INDEX idx_features ON products(image_feature)
    USING IVF_FLAT
    WITH (metric='l2', dim=512, nlist=128, nprobe=32);

-- 使用 HNSW-PQ 索引 + 内积
CREATE VECTOR INDEX idx_text ON products(text_feature)
    USING HNSW_PQ
    WITH (metric='inner_product', dim=1536, m=8, nbits=8);
```

`WITH` 子句在解析器层面被预处理为 TiDB 兼容的 `COMMENT` 子句：

```go
// 文件: pkg/parser/parser.go

// WITH (metric='cosine', dim=768) -> COMMENT 'metric=cosine, dim=768'
func preprocessWithClause(sql string) string {
    // 查找 WITH (...) 并转换为 COMMENT '...'
}
```

#### TiDB 兼容语法

```sql
-- TiDB 风格的向量索引创建
CREATE VECTOR INDEX idx_emb ON articles((VEC_COSINE_DISTANCE(embedding)));

-- 指定使用 HNSW
CREATE VECTOR INDEX idx_emb ON articles((VEC_COSINE_DISTANCE(embedding))) USING HNSW;

-- 带维度参数
CREATE VECTOR INDEX idx_emb ON articles((VEC_COSINE_DISTANCE(embedding)))
    COMMENT 'dim=768, M=8';
```

该语法从距离函数名推导度量类型：`VEC_COSINE_DISTANCE` -> `cosine`，`VEC_L2_DISTANCE` -> `l2`，`VEC_INNER_PRODUCT` -> `inner_product`。

#### 支持的 USING 子句值

| USING 值 | 对应索引类型 |
|----------|------------|
| `HNSW` | HNSW |
| `FLAT` | Flat（暴力搜索） |
| `IVF_FLAT` | IVF-Flat |
| `IVF_SQ8` | IVF-SQ8 |
| `IVF_PQ` | IVF-PQ |
| `HNSW_SQ` | HNSW-SQ |
| `HNSW_PQ` | HNSW-PQ |
| `IVF_RABITQ` | IVF-RaBitQ |
| `HNSW_PRQ` | HNSW-PRQ |
| `AISAQ` | AISAQ |

### 5.3 向量相似搜索查询

```sql
-- 使用余弦距离搜索最相似的 10 条文章
SELECT id, title, vec_cosine_distance(embedding, '[0.1, 0.2, ..., 0.8]') AS distance
FROM articles
ORDER BY vec_cosine_distance(embedding, '[0.1, 0.2, ..., 0.8]') ASC
LIMIT 10;

-- 使用 L2 距离搜索
SELECT id, name, vec_l2_distance(image_feature, '[0.5, 0.3, ...]') AS dist
FROM products
ORDER BY vec_l2_distance(image_feature, '[0.5, 0.3, ...]')
LIMIT 5;

-- 使用内积搜索（降序，因为内积越大越相似）
SELECT id, title, vec_inner_product(embedding, '[0.1, 0.2, ...]') AS score
FROM articles
ORDER BY vec_inner_product(embedding, '[0.1, 0.2, ...]')
LIMIT 20;

-- 通用距离函数（默认使用余弦距离）
SELECT *, vec_distance(embedding, '[0.1, 0.2, ...]') AS dist
FROM articles
ORDER BY vec_distance(embedding, '[0.1, 0.2, ...]')
LIMIT 10;
```

### 5.4 内置向量函数

| 函数名 | 参数 | 返回值 | 说明 |
|-------|------|--------|------|
| `vec_cosine_distance(col, vec)` | 向量列, 查询向量 | `float` | 余弦距离 |
| `vec_l2_distance(col, vec)` | 向量列, 查询向量 | `float` | L2/欧几里得距离 |
| `vec_inner_product(col, vec)` | 向量列, 查询向量 | `float` | 内积值 |
| `vec_distance(col, vec)` | 向量列, 查询向量 | `float` | 通用距离（默认余弦） |

向量参数支持 JSON 数组字符串格式 `'[0.1, 0.2, 0.3]'`，也支持 `[]float64`、`[]float32`、`[]interface{}` 类型。

---

## 索引构建与查询流程

### 6.1 索引构建流程

```
SQL: CREATE VECTOR INDEX idx ON tbl(col) USING HNSW WITH (metric='cosine', dim=768)
                    |
                    v
            [SQL 解析器 (adapter.go)]
            preprocessWithClause() -> 转换 WITH 为 COMMENT
            convertCreateIndexStmt() -> CreateIndexStatement
                    |
                    v
            [查询构建器 (builder.go)]
            executeCreateVectorIndex()
                    |
                    v
            [索引管理器 (index_manager.go)]
            CreateVectorIndex(tableName, columnName, metricType, indexType, dimension, params)
                    |
                    v
            [工厂方法] 根据 indexType 选择:
            NewHNSWIndex() / NewIVFFlatIndex() / NewHNSWPQIndex() / ...
                    |
                    v
            [VectorIndex.Build()] 加载数据并构建索引
            - HNSW: 逐个插入节点，构建多层图
            - IVF: K-Means 聚类
            - PQ/SQ: 训练量化器 -> 编码 -> 构建图/倒排
```

### 6.2 查询执行流程

```
SQL: SELECT * FROM articles ORDER BY vec_cosine_distance(embedding, '[...]') LIMIT 10
                    |
                    v
            [SQL 解析器] -> SelectStatement
            识别 ORDER BY 中的 vec_cosine_distance 函数
            解析查询向量 '[0.1, 0.2, ...]' -> []float32
                    |
                    v
            [查询优化器 (rules_vector.go)]
            VectorIndexRule.Match():
              - 检测 Sort 节点 + Limit 节点
              - 检测 ORDER BY 中的向量距离函数
            VectorIndexRule.Apply():
              - 提取列名、查询向量、度量类型
              - 创建 LogicalVectorScan 节点替换 Sort+Limit
                    |
                    v
            [物理计划 (vector_scan.go)]
            NewVectorScanPlan() -> Plan{Type: TypeVectorScan}
                    |
                    v
            [执行器 (operators/vector_scan.go)]
            VectorScanOperator.Execute():
              1. idxMgr.GetVectorIndex(tableName, columnName)
              2. vectorIdx.Search(ctx, queryVector, k, nil)
              3. fetchRowsByIDs(ctx, result.IDs)
              4. 为每行添加 _distance 列
              5. 返回 QueryResult
```

### 6.3 HNSW 搜索过程详解

```
查询: k=10, ef=256, 向量维度 768

Step 1: 确定入口点 (entryPoint) 和最高层级 (entryLevel)

Step 2: 贪心下降 (顶层 -> 第 1 层)
  Layer 3: ep -> 贪心找最近 -> ep'
  Layer 2: ep' -> 贪心找最近 -> ep''
  Layer 1: ep'' -> 贪心找最近 -> ep'''

Step 3: Beam Search (第 0 层)
  从 ep''' 出发，使用优先队列探索 ef=256 个候选
  |-- 弹出最近候选 -> 扩展其邻居
  |-- 每个邻居计算距离 -> 插入已排序结果
  |-- 当结果集满 ef 且最近候选 > 最远结果时停止

Step 4: 过滤 + 取 Top-K
  从 256 个候选中取距离最小的 10 个
  返回 {IDs: [...], Distances: [...]}
```

### 6.4 过滤搜索

VectorFilter 支持基于 ID 集合的预过滤。当存在过滤条件时，HNSW 会自适应调整搜索宽度：

```go
// 文件: pkg/resource/memory/hnsw_index.go

if filterSet != nil {
    ratio := float64(len(h.vectors)) / float64(len(filterSet))
    adjustedEf := int(float64(ef) * ratio)
    if adjustedEf > len(h.vectors) {
        adjustedEf = len(h.vectors)
    }
    if adjustedEf > ef {
        ef = adjustedEf
    }
}
```

当过滤比例较高（大部分向量被过滤掉）时，搜索宽度按比例放大以保证召回率。

---

## 与查询优化器的集成

### 7.1 VectorIndexRule 优化规则

查询优化器通过 `VectorIndexRule` 自动识别向量搜索模式，将传统的 Sort + Limit 查询计划改写为专用的 VectorScan 节点。

#### 匹配条件

```go
// 文件: pkg/optimizer/rules_vector.go

func (r *VectorIndexRule) Match(plan LogicalPlan) bool {
    // 条件 1: 节点是 LogicalSort
    sort, ok := plan.(*LogicalSort)
    if !ok {
        return false
    }

    // 条件 2: 存在 LIMIT 子节点
    hasLimit := false
    children := sort.Children()
    if len(children) > 0 {
        if _, ok := children[0].(*LogicalLimit); ok {
            hasLimit = true
        }
    }

    // 条件 3: ORDER BY 包含向量距离函数
    hasVectorFunc := false
    for _, item := range sort.OrderBy() {
        if isVectorDistanceFunction(&item.Expr) {
            hasVectorFunc = true
            break
        }
    }

    return hasLimit && hasVectorFunc
}
```

可识别的向量距离函数：`vec_cosine_distance`、`vec_l2_distance`、`vec_inner_product`、`vec_distance`。

#### 计划改写

当匹配成功时，优化器将整个 Sort -> Limit -> DataSource 子树替换为单个 `LogicalVectorScan` 节点：

```
改写前:                          改写后:
LogicalSort                      LogicalVectorScan
  |-- ORDER BY vec_cosine_...      TableName: "articles"
  |                                ColumnName: "embedding"
  +-- LogicalLimit                 QueryVector: [0.1, 0.2, ...]
        |-- LIMIT 10               K: 10
        |                          MetricType: "cosine"
        +-- LogicalDataSource
              |-- articles
```

#### 基数估算

VectorScan 节点的基数估算直接返回 K 值，这为上层的 Join 选择和排序消除提供了准确的信息：

```go
func (l *LogicalVectorScan) EstimateCardinality() int64 {
    return int64(l.K)
}
```

### 7.2 VectorScan 执行算子

`VectorScanOperator` 负责将逻辑计划转化为实际的向量搜索操作：

```go
// 文件: pkg/executor/operators/vector_scan.go

func (v *VectorScanOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
    // 1. 通过 IndexManager 获取向量索引
    vectorIdx, err := v.idxMgr.GetVectorIndex(v.config.TableName, v.config.ColumnName)

    // 2. 执行向量搜索（调用具体索引的 Search 方法）
    result, err := vectorIdx.Search(ctx, v.config.QueryVector, v.config.K, nil)

    // 3. 根据返回的 ID 列表获取完整行数据
    rows, err := v.fetchRowsByIDs(ctx, result.IDs)

    // 4. 为每一行附加 _distance 列
    for i, row := range rows {
        row["_distance"] = result.Distances[i]
    }

    return &domain.QueryResult{
        Columns: columns,
        Rows:    rows,
        Total:   int64(len(rows)),
    }, nil
}
```

结果中自动包含 `_distance` 列，包含每行与查询向量的距离值。

---

## 架构总览

```
                    SQL 输入
                       |
            +----------+----------+
            |    SQL 解析器        |
            |  (parser/adapter.go)|
            |  - VECTOR(dim) 类型 |
            |  - CREATE VECTOR IDX|
            |  - WITH 子句预处理   |
            +----------+----------+
                       |
            +----------+----------+
            |    查询优化器        |
            | (optimizer/         |
            |  rules_vector.go)   |
            |  - VectorIndexRule  |
            |  - Sort+Limit 改写  |
            +----------+----------+
                       |
            +----------+----------+
            |    执行器            |
            | (executor/operators/|
            |  vector_scan.go)    |
            |  - VectorScanOp     |
            +----------+----------+
                       |
            +----------+----------+
            |    索引管理器        |
            | (memory/            |
            |  index_manager.go)  |
            |  - 创建/获取/删除    |
            +----------+----------+
                       |
         +------+------+------+------+
         |      |      |      |      |
       Flat   HNSW  IVF-*  HNSW-* AISAQ
         |      |      |      |      |
         v      v      v      v      v
    +----+------+------+------+------+----+
    |        距离度量注册中心               |
    |  (memory/distance.go)               |
    |  cosine | l2 | inner_product        |
    +-----------------------------------------+
```

### 代码目录结构

```
pkg/
  resource/
    domain/
      models.go              # ColumnInfo.VectorDim/VectorType, VectorIndexConfig
    memory/
      vector_index.go        # VectorIndex 接口定义
      index.go               # IndexType 常量, VectorMetricType, VectorIndexConfig
      index_manager.go       # IndexManager (创建/管理向量索引)
      distance.go            # 距离函数接口与实现 (cosine, l2, inner_product)
      flat_index.go          # Flat 暴力搜索索引
      hnsw_index.go          # HNSW 索引 (标准实现)
      hnsw_index_improved.go # HNSW 改进版 (参考 Milvus)
      hnsw_sq_index.go       # HNSW-SQ (标量量化)
      hnsw_pq_index.go       # HNSW-PQ (乘积量化)
      hnsw_prq_index.go      # HNSW-PRQ (残差乘积量化)
      ivf_flat_index.go      # IVF-Flat
      ivf_sq8_index.go       # IVF-SQ8
      ivf_pq_index.go        # IVF-PQ
      ivf_rabitq_index.go    # IVF-RaBitQ (SIGMOD 2024)
      aisaq_index.go         # AISAQ (Vamana 图 + 自适应量化)
  parser/
    types.go                 # ColumnInfo, CreateIndexStatement (向量字段)
    adapter.go               # SQL 解析 (VECTOR 类型, CREATE VECTOR INDEX)
    parser.go                # preprocessWithClause (WITH -> COMMENT)
  builtin/
    vector_functions.go      # vec_cosine_distance, vec_l2_distance, vec_inner_product
  optimizer/
    rules_vector.go          # VectorIndexRule, LogicalVectorScan
    plan/
      vector_scan.go         # VectorScanConfig, NewVectorScanPlan
  executor/
    operators/
      vector_scan.go         # VectorScanOperator
```
