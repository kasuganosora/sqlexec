# 向量搜索实施启动指南

## ✅ 前置检查

### 文档状态确认
- [x] 所有 P0/P1 问题已修复
- [x] 类型定义统一
- [x] 接口与现有代码兼容
- [x] 目录结构符合现有规范

### 环境准备
```bash
# 确认 Go 版本
go version  # >= 1.21

# 确认项目可编译
cd d:\code\db
go build ./...

# 运行现有测试（确保基准通过）
go test ./pkg/resource/memory/... -v
go test ./pkg/optimizer/... -v
go test ./pkg/executor/... -v
```

---

## 🚀 第一步：基础类型扩展（Week 1 Day 1）

### 任务清单
1. [ ] 修改 `pkg/resource/memory/index.go` - 添加向量类型常量
2. [ ] 修改 `pkg/resource/domain/models.go` - 扩展 ColumnInfo 和 Index
3. [ ] 编译验证
4. [ ] 提交代码

### 详细步骤

#### Step 1: 修改 pkg/resource/memory/index.go

```bash
# 备份原文件
copy pkg\resource\memory\index.go pkg\resource\memory\index.go.bak
```

添加以下内容（在文件末尾）：

```go
// ========== 向量索引类型（新增）==========

const (
    IndexTypeVectorHNSW    IndexType = "hnsw"
    IndexTypeVectorIVFFlat IndexType = "ivf_flat"
    IndexTypeVectorFlat    IndexType = "flat"
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
)

// VectorDataType 向量数据类型
type VectorDataType string

const (
    VectorDataTypeFloat32  VectorDataType = "float32"
    VectorDataTypeFloat16  VectorDataType = "float16"
    VectorDataTypeBFloat16 VectorDataType = "bfloat16"
    VectorDataTypeInt8     VectorDataType = "int8"
)

// VectorIndexConfig 向量索引配置
type VectorIndexConfig struct {
    MetricType VectorMetricType       `json:"metric_type"`
    Dimension  int                    `json:"dimension"`
    Params     map[string]interface{} `json:"params,omitempty"`
}
```

#### Step 2: 修改 pkg/resource/domain/models.go

找到 `ColumnInfo` 结构，添加：

```go
type ColumnInfo struct {
    // ... 现有字段 ...
    
    // ========== 新增：向量类型字段 ==========
    VectorDim  int    `json:"vector_dim,omitempty"`      // 向量维度
    VectorType string `json:"vector_type,omitempty"`     // 向量数据类型
}

// IsVectorType 检查是否为向量列（新增方法）
func (c ColumnInfo) IsVectorType() bool {
    return c.VectorDim > 0
}
```

找到 `Index` 结构，添加：

```go
type Index struct {
    // ... 现有字段 ...
    
    // ========== 新增：向量索引扩展信息 ==========
    VectorConfig *VectorIndexConfig `json:"vector_config,omitempty"`
}
```

注意：需要在文件顶部添加 import：
```go
import (
    // ... 现有 imports ...
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
)
```

#### Step 3: 编译验证

```bash
cd d:\code\db

# 编译 resource 包
go build ./pkg/resource/memory/...
go build ./pkg/resource/domain/...

# 如果报错，检查：
# 1. import 路径是否正确
# 2. 类型是否循环依赖
# 3. 字段名是否冲突
```

#### Step 4: 验证代码

创建一个临时测试文件验证类型：

```go
// pkg/resource/memory/index_test.go（临时）
package memory

import "testing"

func TestVectorTypes(t *testing.T) {
    // 验证常量
    if IndexTypeVectorHNSW != "hnsw" {
        t.Error("IndexTypeVectorHNSW value mismatch")
    }
    
    // 验证 IsVectorIndex
    if !IndexTypeVectorHNSW.IsVectorIndex() {
        t.Error("IsVectorIndex should return true for HNSW")
    }
    
    if IndexTypeBTree.IsVectorIndex() {
        t.Error("IsVectorIndex should return false for BTree")
    }
    
    // 验证配置
    config := VectorIndexConfig{
        MetricType: VectorMetricCosine,
        Dimension:  768,
    }
    if config.Dimension != 768 {
        t.Error("VectorIndexConfig.Dimension mismatch")
    }
}
```

运行测试：
```bash
go test ./pkg/resource/memory/... -run TestVectorTypes -v
```

#### Step 5: 提交代码

```bash
# 添加修改的文件
git add pkg/resource/memory/index.go
git add pkg/resource/domain/models.go

# 提交（不要使用 git commit -a，避免提交临时文件）
git commit -m "feat(vector): add vector index types and config

- Add IndexTypeVectorHNSW, IndexTypeVectorIVFFlat, IndexTypeVectorFlat
- Add VectorMetricType (cosine, l2, inner_product)
- Add VectorDataType (float32, float16, bfloat16, int8)
- Extend ColumnInfo with VectorDim and VectorType
- Extend Index with VectorConfig
- Add IsVectorIndex() and IsVectorType() methods"
```

---

## ⚠️ 关键注意事项

### 1. 不要修改的字段
- 不要删除或重命名 `TableIndexes.indexes` 和 `columnMap`
- 这两个字段用途不同：
  - `indexes`: indexName -> Index
  - `columnMap`: columnName -> Index (快速查找)

### 2. 循环依赖检查
如果在 `domain` 包引用 `memory` 包导致循环依赖，解决方案：

```go
// 方案：在 domain 包中重新定义类型，不依赖 memory 包

// pkg/resource/domain/types.go
type VectorMetricType string

const (
    VectorMetricCosine VectorMetricType = "cosine"
    VectorMetricL2     VectorMetricType = "l2"
)

// 使用时在 memory 包中转换
func toMemoryMetricType(t domain.VectorMetricType) VectorMetricType {
    return VectorMetricType(t)
}
```

### 3. 编译错误快速排查

| 错误 | 原因 | 解决 |
|------|------|------|
| `undefined: VectorMetricType` | 未添加类型定义 | 检查 index.go 是否保存 |
| `redeclared in this block` | IndexType 重复定义 | 检查是否与其他文件冲突 |
| `import cycle` | 循环依赖 | domain 包不要 import memory 包 |
| `undefined: IsVectorIndex` | 方法未添加 | 检查是否在 IndexType 上定义 |

---

## 📊 每日检查点

### Day 1 完成标准
- [ ] `go build ./pkg/resource/memory/...` 通过
- [ ] `go build ./pkg/resource/domain/...` 通过
- [ ] 类型测试通过
- [ ] 代码已提交

### 下一步预告
**Week 1 Day 2**: 实现距离函数模块
- 创建 `pkg/resource/memory/distance.go`
- 实现 Cosine、L2、InnerProduct
- 注册到 DistanceRegistry

---

## 🔧 常用命令速查

```bash
# 编译检查
go build ./pkg/resource/...
go build ./pkg/optimizer/...
go build ./pkg/executor/...
go build ./...

# 测试
go test ./pkg/resource/memory/... -v
go test ./pkg/resource/memory/... -run TestXXX -v

# 查看编译错误详情
go build -v ./pkg/resource/memory/... 2>&1

# 格式化代码
go fmt ./pkg/resource/memory/...

# 代码检查
go vet ./pkg/resource/memory/...
```

---

## 📞 问题反馈

如果在实施过程中遇到以下问题：
1. 编译错误无法解决
2. 类型定义冲突
3. 循环依赖

请提供：
- 完整的错误信息
- 相关文件内容
- 已尝试的解决方案

祝实施顺利！🎉
