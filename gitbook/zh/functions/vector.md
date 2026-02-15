# 向量函数

SQLExec 内置了向量距离计算函数，支持多种距离度量方式，可用于向量相似性搜索、推荐系统、RAG（检索增强生成）等场景。

## 向量格式

向量以字符串形式表示，使用 JSON 数组语法：

```
'[0.1, 0.2, 0.3, 0.4]'
```

参与计算的两个向量必须具有相同的维度。

## 函数列表

| 函数 | 说明 | 返回值范围 | 示例 |
|------|------|------------|------|
| `VEC_COSINE_DISTANCE(v1, v2)` | 计算余弦距离 | 0.0 ~ 2.0 | `SELECT VEC_COSINE_DISTANCE('[1,0]', '[0,1]');` -- `1.0` |
| `VEC_L2_DISTANCE(v1, v2)` | 计算欧氏距离（L2） | 0.0 ~ +inf | `SELECT VEC_L2_DISTANCE('[0,0]', '[3,4]');` -- `5.0` |
| `VEC_INNER_PRODUCT(v1, v2)` | 计算内积（点积） | -inf ~ +inf | `SELECT VEC_INNER_PRODUCT('[1,2]', '[3,4]');` -- `11.0` |
| `VEC_DISTANCE(v1, v2)` | 计算默认距离（等同于 L2 距离） | 0.0 ~ +inf | `SELECT VEC_DISTANCE('[0,0]', '[3,4]');` -- `5.0` |

## 距离度量说明

### 余弦距离（Cosine Distance）

余弦距离 = 1 - 余弦相似度。衡量两个向量方向的差异，不受向量长度影响。

- **值为 0**：两个向量方向完全一致
- **值为 1**：两个向量正交（无关）
- **值为 2**：两个向量方向完全相反

适用于文本语义搜索、文档相似度等不关心向量幅度的场景。

```sql
SELECT VEC_COSINE_DISTANCE('[1,1]', '[1,1]');    -- 0.0（完全一致）
SELECT VEC_COSINE_DISTANCE('[1,0]', '[0,1]');    -- 1.0（正交）
SELECT VEC_COSINE_DISTANCE('[1,0]', '[-1,0]');   -- 2.0（完全相反）
```

### 欧氏距离（L2 Distance）

欧氏距离是两个向量在空间中的直线距离。值越小表示越相似。

适用于需要考虑向量幅度差异的场景，如图像特征匹配。

```sql
SELECT VEC_L2_DISTANCE('[0,0,0]', '[1,2,2]');   -- 3.0
SELECT VEC_L2_DISTANCE('[1,1]', '[4,5]');        -- 5.0
```

### 内积（Inner Product）

内积（点积）是两个向量对应元素乘积之和。值越大表示越相似（对于归一化向量）。

适用于已归一化的向量，常见于最大内积搜索（MIPS）场景。

```sql
SELECT VEC_INNER_PRODUCT('[1,2,3]', '[4,5,6]'); -- 32.0
SELECT VEC_INNER_PRODUCT('[0.5,0.5]', '[1,0]'); -- 0.5
```

## 使用示例

### 基本向量搜索

```sql
-- 使用余弦距离进行语义搜索
SELECT id, title,
       VEC_COSINE_DISTANCE(embedding, '[0.12, -0.34, 0.56, 0.78, -0.91]') AS distance
FROM documents
ORDER BY distance
LIMIT 10;
```

### K 近邻搜索（KNN）

```sql
-- 查找最相似的 10 条记录
SELECT id, content,
       VEC_L2_DISTANCE(embedding, '[0.1, 0.2, 0.3, 0.4, 0.5]') AS distance
FROM docs
ORDER BY distance
LIMIT 10;
```

### RAG 检索增强生成

```sql
-- 根据用户问题的向量表示检索相关文档
SELECT id, content, source,
       VEC_COSINE_DISTANCE(embedding, '[0.15, -0.28, 0.44, ...]') AS score
FROM knowledge_base
ORDER BY score
LIMIT 5;
```

### 推荐系统

```sql
-- 查找与目标商品最相似的商品
SELECT p.id, p.name, p.category,
       VEC_COSINE_DISTANCE(p.feature_vector, t.feature_vector) AS distance
FROM products p, products t
WHERE t.id = 1001 AND p.id != 1001
ORDER BY distance
LIMIT 20;
```

### 配合过滤条件

```sql
-- 在特定类别中进行向量搜索
SELECT id, title,
       VEC_COSINE_DISTANCE(embedding, '[0.12, -0.34, 0.56, ...]') AS distance
FROM articles
WHERE category = 'technology'
  AND published = true
ORDER BY distance
LIMIT 10;
```

### 内积搜索

```sql
-- 使用内积进行最大相似度搜索（向量已归一化）
SELECT id, title,
       VEC_INNER_PRODUCT(embedding, '[0.12, -0.34, 0.56, ...]') AS score
FROM documents
ORDER BY score DESC
LIMIT 10;
```

## 距离度量选择指南

| 度量 | 函数 | 适用场景 | 排序方向 |
|------|------|----------|----------|
| 余弦距离 | `VEC_COSINE_DISTANCE` | 文本语义搜索、不关心幅度 | 升序（越小越相似） |
| 欧氏距离 | `VEC_L2_DISTANCE` | 图像特征、空间位置 | 升序（越小越相似） |
| 内积 | `VEC_INNER_PRODUCT` | 归一化向量、MIPS | 降序（越大越相似） |
| 默认距离 | `VEC_DISTANCE` | 通用场景 | 升序（越小越相似） |

## 性能建议

- 对于大规模向量数据，建议创建向量索引（如 HNSW 索引）以加速搜索。
- 向量维度越高，计算开销越大。常见维度范围为 128 ~ 1536。
- 在查询中先使用标量条件过滤，再进行向量距离计算，可以显著提升性能。
- 若向量已归一化，余弦距离和内积的排序结果等价，可选择计算更快的方式。
