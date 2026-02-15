# 向量搜索

SQLExec 支持高维向量的近似最近邻搜索（Approximate Nearest Neighbor, ANN），适用于语义搜索、推荐系统、图像检索等 AI 应用场景。

## 向量列类型

使用 `VECTOR(dim)` 类型定义向量列，其中 `dim` 为向量维度。

```sql
CREATE TABLE documents (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    content TEXT,
    embedding VECTOR(768)
);
```

## 向量索引类型

SQLExec 提供 10 种向量索引类型，覆盖从精确搜索到极致压缩的各种场景：

| 索引类型 | 说明 | 精度 | 速度 | 内存占用 | 适用场景 |
|---------|------|------|------|---------|---------|
| Flat | 暴力搜索，逐一比较 | 完美 | 慢 | 高 | 小数据集、精度基准 |
| IVF-Flat | IVF 聚类 + 精确距离 | 好 | 快 | 中 | 中等规模数据集 |
| IVF-SQ8 | IVF + 标量量化 | 较好 | 快 | 较低 | 内存受限场景 |
| IVF-PQ | IVF + 乘积量化 | 中等 | 快 | 低 | 大规模数据集 |
| **HNSW** | 层次导航小世界图 | **优秀** | **快** | 中 | **推荐，通用场景** |
| HNSW-SQ | HNSW + 标量量化 | 好 | 快 | 较低 | 兼顾精度与内存 |
| HNSW-PQ | HNSW + 乘积量化 | 较好 | 快 | 低 | 大规模高维数据 |
| HNSW-PRQ | HNSW + 渐进残差量化 | 好 | 快 | 低 | 高压缩比场景 |
| IVF-RabitQ | IVF + RabitQ 量化 | 较好 | 快 | 低 | 超大规模数据集 |
| AISAQ | 非对称 ISAQ | 较好 | 快 | 极低 | 极端内存受限场景 |

> **推荐**：大多数场景下建议使用 **HNSW** 索引，它在精度和速度之间取得了最佳平衡。

## 创建向量索引

```sql
CREATE VECTOR INDEX idx_embedding ON documents(embedding)
    USING HNSW
    WITH (
        metric = 'cosine',
        dim = 768,
        m = 16,
        ef_construction = 200
    );
```

### 索引参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `metric` | 距离度量方式 | `cosine` |
| `dim` | 向量维度 | 与列定义一致 |
| `m` | HNSW 每个节点的最大连接数 | `16` |
| `ef_construction` | HNSW 构建时的搜索宽度 | `200` |
| `nprobe` | IVF 搜索时的聚类探测数 | `10` |
| `pq_m` | PQ 乘积量化的子空间数 | `8` |

## 距离度量

SQLExec 支持 3 种距离度量方式：

| 度量方式 | 函数 | 值域 | 说明 |
|---------|------|------|------|
| Cosine | `vec_cosine_distance(a, b)` | [0, 2] | 余弦距离，适用于归一化向量，值越小越相似 |
| L2 / Euclidean | `vec_l2_distance(a, b)` | [0, +∞) | 欧氏距离，适用于原始向量 |
| Inner Product | `vec_ip_distance(a, b)` | (-∞, +∞) | 内积距离，值越大越相似 |

## Top-K 查询

使用 `ORDER BY` 距离函数配合 `LIMIT` 实现 Top-K 近邻搜索：

```sql
SELECT id, title,
       vec_cosine_distance(embedding, '[0.1, 0.2, ..., 0.5]') AS distance
FROM documents
ORDER BY vec_cosine_distance(embedding, '[0.1, 0.2, ..., 0.5]')
LIMIT 10;
```

## 混合搜索

将向量搜索与全文搜索的分数结合，实现更精准的语义+关键词混合检索：

```sql
SELECT id, title,
       vec_cosine_distance(embedding, '[0.1, 0.2, ..., 0.5]') AS vec_score,
       MATCH(content) AGAINST('机器学习') AS text_score,
       vec_cosine_distance(embedding, '[0.1, 0.2, ..., 0.5]') * 0.7
           + (1 - MATCH(content) AGAINST('机器学习')) * 0.3 AS combined_score
FROM documents
WHERE MATCH(content) AGAINST('机器学习')
ORDER BY combined_score
LIMIT 10;
```

## 完整示例

以下是一个完整的向量搜索流程：

```sql
-- 1. 创建包含向量列的表
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    content TEXT,
    embedding VECTOR(384)
);

-- 2. 插入数据（向量通常由外部模型生成）
INSERT INTO articles (id, title, content, embedding) VALUES
(1, '深度学习入门', '神经网络是深度学习的基础...', '[0.12, 0.45, ..., 0.78]'),
(2, '自然语言处理', 'NLP 是 AI 的重要分支...', '[0.34, 0.67, ..., 0.91]'),
(3, '计算机视觉', '图像识别技术广泛应用...', '[0.56, 0.23, ..., 0.44]');

-- 3. 创建 HNSW 向量索引
CREATE VECTOR INDEX idx_article_emb ON articles(embedding)
    USING HNSW
    WITH (metric = 'cosine', m = 16, ef_construction = 200);

-- 4. 搜索最相似的文章
SELECT id, title,
       vec_cosine_distance(embedding, '[0.13, 0.44, ..., 0.80]') AS distance
FROM articles
ORDER BY vec_cosine_distance(embedding, '[0.13, 0.44, ..., 0.80]')
LIMIT 5;
```
