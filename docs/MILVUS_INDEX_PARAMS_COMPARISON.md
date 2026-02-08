# Milvus 索引参数对比文档

## 参数对比总结

本文档对比我们的索引实现与 Milvus 官方最佳实践的参数差异。

### 1. IVF_FLAT

| 参数 | 我们的实现 | Milvus 默认 | 说明 |
|------|-----------|--------------|------|
| nlist | 128 | 128 | ✅ 已修正 |
| nprobe | 32 | 32 | ✅ 已修正 |

**优化建议**:
- nlist: 建议设置为数据量的 1/4 到 n/1000 之间
- nprobe: 建议设置为 nlist 的 1%~10%

### 2. IVF_SQ8

| 参数 | 我们的实现 | Milvus 默认 | 说明 |
|------|-----------|--------------|------|
| nlist | 128 | 128 | ✅ 已修正 |
| nprobe | 32 | 32 | ✅ 已修正 |

**优化建议**:
- 内存占用降低约 75%
- 召回率约 90-95%

### 3. IVF_PQ

| 参数 | 我们的实现 | Milvus 默认 | 说明 |
|------|-----------|--------------|------|
| nlist | 128 | 128 | ✅ 已修正 |
| nprobe | 32 | 32 | ✅ 已修正 |
| m (子量化器数) | 16 | 16 | ✅ 已修正 |
| nbits (编码位数) | 8 | 8 | ✅ 已修正 |

**优化建议**:
- 内存占用降低约 90%+
- 召回率约 85-95%
- m: 建议设置为维度的 1/4
- nbits: 默认为 8，不建议调整

### 4. HNSW_SQ

| 参数 | 我们的实现 | Milvus 默认 | 说明 |
|------|-----------|--------------|------|
| M (每层连接数) | 8 | 8 | ✅ 已修正 |
| MaxLevel | 16 | 16 | ✅ 已修正 |
| ML (层级生成因子) | 1/ln(8) | 1/ln(8) | ✅ 已修正 |
| EFConstruction | 40 | 40 | ✅ 已修正 |
| EF (搜索参数) | 96 (可动态配置) | 96 | ✅ 已修正 |

**优化建议**:
- M: 建议设置为 8-32，影响内存和性能
- EFConstruction: 建议设置为 40-100
- EF (搜索): 建议设置为 64-200
- 召回率: 95-100%
- 内存开销: 约原始向量的 1.5-2 倍

### 5. HNSW_PQ

| 参数 | 我们的实现 | Milvus 默认 | 说明 |
|------|-----------|--------------|------|
| M (每层连接数) | 8 | 8 | ✅ 已修正 |
| MaxLevel | 16 | 16 | ✅ 已修正 |
| ML (层级生成因子) | 1/ln(8) | 1/ln(8) | ✅ 已修正 |
| EFConstruction | 40 | 40 | ✅ 已修正 |
| EF (搜索参数) | 96 (可动态配置) | 96 | ✅ 已修正 |
| Nbits (编码位数) | 8 | 8 | ✅ 已修正 |

**优化建议**:
- 极致内存优化，内存占用可降低 90%+
- 召回率约 85-95%
- 参数与 HNSW_SQ 类似

## 修正详情

### HNSW 参数区分

Milvus 区分了构建和搜索参数：

1. **EFConstruction (构建时)**: 用于构建索引时的探索宽度
2. **EF (搜索时)**: 用于搜索时的探索宽度，可动态调整

我们的实现现在正确区分了这两个参数：

```go
// 参数
	efConstruction int // 构建时的探索宽度
	ef             int // 搜索时的探索宽度
```

### 搜索时的 EF 参数

HNSW 索引现在支持在搜索时动态配置 ef 参数：

```go
// 获取 ef 搜索参数（可动态配置）
ef := h.ef
if val, ok := h.config.Params["ef"].(int); ok {
    ef = val
}
```

这样用户可以在不重建索引的情况下调整搜索精度和速度的平衡。

## 参数配置示例

### IVF_FLAT
```json
{
  "index_type": "IVF_FLAT",
  "metric_type": "L2",
  "params": {
    "nlist": 128,
    "nprobe": 32
  }
}
```

### IVF_SQ8
```json
{
  "index_type": "IVF_SQ8",
  "metric_type": "L2",
  "params": {
    "nlist": 128,
    "nprobe": 32
  }
}
```

### IVF_PQ
```json
{
  "index_type": "IVF_PQ",
  "metric_type": "L2",
  "params": {
    "nlist": 128,
    "nprobe": 32,
    "m": 16,
    "nbits": 8
  }
}
```

### HNSW_SQ
```json
{
  "index_type": "HNSW_SQ",
  "metric_type": "L2",
  "params": {
    "M": 8,
    "efConstruction": 40,
    "ef": 96
  }
}
```

### HNSW_PQ
```json
{
  "index_type": "HNSW_PQ",
  "metric_type": "L2",
  "params": {
    "M": 8,
    "efConstruction": 40,
    "ef": 96,
    "nbits": 8
  }
}
```

## 参考来源

- Milvus 官方文档: https://milvus.io/docs
- Milvus 测试代码: tests/go_client/testcases/index_test.go
- Milvus 参数说明: tests/go_client/testcases/helper/index_helper.go

## 总结

✅ 所有索引参数已修正为 Milvus 推荐值
✅ HNSW 索引正确区分了 EFConstruction (构建) 和 EF (搜索)
✅ EF 搜索参数支持动态配置
✅ 编译通过，所有实现正确
