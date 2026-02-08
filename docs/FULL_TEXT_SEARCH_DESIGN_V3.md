# 全文搜索设计方案 v3（融合 Milvus + pgsearch + 本项目）

## 设计理念

融合三大优势：
- **Milvus**: 自动 BM25 稀疏向量生成 + 混合搜索（RRF）
- **pgsearch**: 丰富查询语法（13 种）+ SQL 接口 + 多类型字段
- **本项目**: 10 种向量索引（HNSW/IVF 等）+ 多语言支持

## 核心架构

```
SQL 接口 (@@ 操作符)
    ↓
SQL 解析（函数式配置）
    ↓
┌────────┬──────────────┬──────────┐
│ 全文   │ 向量         │ 混合    │
│ 引擎   │ 引擎         │ 引擎     │
│ (BM25) │ (Vector)     │ (Hybrid) │
└────────┴──────────────┴──────────┘
    ↓
统一存储层（倒排 + 向量表）
```

## 核心特性

### 1. 自动向量生成（Milvus 特性）

```go
// 自动将文档转换为稀疏向量
func (bm25 *BM25Function) AutoConvert(doc Document) (SparseVector, error) {
    // 1. 分词
    tokens := bm25.Analyzer.Analyze(doc.Text, doc.Language)
    
    // 2. 更新统计
    bm25.UpdateVocabulary(tokens)
    
    // 3. 自动计算 BM25 分数
    sparse := make(SparseVector)
    for _, token := range tokens {
        termID := bm25.vocab.GetOrCreateTermID(token.Text)
        tf := countTermFreq(tokens, token.Text)
        
        idf := bm25.vocab.GetIDF(termID)
        docLength := len(tokens)
        
        // BM25 公式
        numerator := float64(tf * (bm25.k1 + 1))
        denominator := float64(tf) + bm25.k1*(1-bm25.b+bm25.b*float64(docLength)/bm25.stats.AvgDocLength)
        
        sparse[termID] = idf * (numerator / denominator)
    }
    
    return sparse, nil
}
```

### 2. 混合搜索（Milvus + 本项目）

```go
// RRF 融合算法
func (he *HybridEngine) RRF(ftResults, vecResults []SearchResult, k int) []HybridResult {
    scores := make(map[int64]float64)
    
    // 合并结果
    for i, result := range ftResults {
        scores[result.DocID] += 1.0 / float64(k + i + 1)
    }
    
    for i, result := range vecResults {
        scores[result.DocID] += 1.0 / float64(k + i + 1)
    }
    
    // 排序
    ranked := make([]HybridResult, 0, len(scores))
    for docID, score := range scores {
        ranked = append(ranked, HybridResult{
            DocID:      docID,
            HybridScore: score,
            FTScore:    he.getFTScore(docID, ftResults),
            VecScore:   he.getVecScore(docID, vecResults),
        })
    }
    
    sort.Slice(ranked, func(i, j int) bool {
        return ranked[i].HybridScore > ranked[j].HybridScore
    })
    
    return ranked
}

// 加权融合
func (he *HybridEngine) WeightedFusion(ftResults, vecResults []SearchResult) []HybridResult {
    merged := make(map[int64]*HybridResult)
    
    // 归一化
    ftMaxScore := ftResults[0].Score
    for _, result := range ftResults {
        merged[result.DocID] = &HybridResult{
            DocID:    result.DocID,
            FTScore:  result.Score / ftMaxScore,
            VecScore: 0,
        }
    }
    
    vecMaxScore := vecResults[0].Score
    for _, result := range vecResults {
        if r, exists := merged[result.DocID]; exists {
            r.VecScore = result.Score / vecMaxScore
        } else {
            merged[result.DocID] = &HybridResult{
                DocID:    result.DocID,
                FTScore:  0,
                VecScore: result.Score / vecMaxScore,
            }
        }
    }
    
    // 计算混合分数
    results := make([]HybridResult, 0, len(merged))
    for _, r := range merged {
        r.HybridScore = r.FTScore*he.FTWeight + r.VecScore*he.VecWeight
        results = append(results, *r)
    }
    
    sort.Slice(results, func(i, j int) bool {
        return results[i].HybridScore > results[j].HybridScore
    })
    
    return results
}
```

### 3. 13 种查询类型（pgsearch 特性）

1. **TermQuery**: `field:term`
2. **PhraseQuery**: `field:"phrase"`
3. **PhrasePrefix**: `phrase_prefix(field, ['terms'])`
4. **FuzzyQuery**: `fuzzy_term(field, term, distance=2)`
5. **RegexQuery**: `regex(field, pattern)`
6. **RangeQuery**: `field:[1 TO 4]`
7. **BooleanQuery**: `must + must_not + should`
8. **DisjunctionMax**: `disjunction_max([q1, q2])`
9. **ConstScore**: `const_score(query, score)`
10. **Empty**: `empty()`
11. **TermSet**: `term_set([term1, term2])`
12. **TokenizerTerms**: `tokenizer_terms(field, text, 'OR')`
13. **Boost**: `boost(query, 2)`

### 4. 函数式配置接口（pgsearch 特性）

```go
// 字段配置
fulltext_field('name', 
    fast=>FALSE, 
    tokenizer=>fulltext_tokenizer('jieba', hmm=>TRUE))

// 分词器配置
fulltext_tokenizer('jieba', 
    hmm=>TRUE, 
    search=>TRUE, 
    dict=>'user.dict',
    stopword=>'stop_words.txt')

// 查询配置
fulltext_config('field:term^2 AND field2:term3')

// 布尔查询
fulltext_boolean(
    must => ARRAY[fulltext_term('field', 'value')],
    should => ARRAY[fulltext_term('field', 'value')],
    must_not => ARRAY[fulltext_term('field', 'value')]
)
```

### 5. 6 种分词器（pgsearch + 本项目）

| 分词器 | 语言 | 特性 |
|-------|------|------|
| **Jieba** | 中文 | HMM、搜索模式、自定义词典、停用词 |
| **Ngram** | 中英混合 | min_gram, max_gram, prefix_only |
| **Chinese Compatible** | 中文 | 单字 + 连续词，简单快速 |
| **Lindera** | CJK | 中日韩分词，支持 IPADIC、KoDic |
| **English Stem** | 英文 | 词干提取（run → run） |
| **Default** | 通用 | 基础空格分词 |

### 6. 多类型字段索引（pgsearch 特性）

| 字段类型 | 数据类型 | 特殊功能 |
|---------|---------|---------|
| **文本** | VARCHAR, TEXT, VARCHAR[], TEXT[] | 分词、BM25 评分、位置信息 |
| **数值** | INT, BIGINT, FLOAT, DOUBLE, NUMERIC | 快速过滤、范围查询 |
| **布尔** | BOOLEAN, BOOLEAN[] | 快速过滤 |
| **日期时间** | DATE, TIMESTAMP, TIMESTAMPTZ, TIME | 日期范围查询 |
| **JSON** | JSON, JSONB | 嵌套字段搜索、展开点（`metadata.color`） |
| **向量** | ARRAY(FLOAT) | 密集向量，用于混合搜索 |

## SQL 接口设计

### 创建索引

```sql
-- 标准语法
CREATE FULLTEXT INDEX search_idx 
ON articles(content)
WITH (
    tokenizer = 'jieba',
    language = 'zh',
    k1 = 1.2,
    b = 0.75,
    enable_hybrid = TRUE,
    fusion_strategy = 'rrf',
    ft_weight = 0.7,
    vec_weight = 0.3
);

-- 函数式配置（推荐）
CALL create_fulltext_index(
    index_name => 'search_idx',
    table_name => 'articles',
    text_fields => 
        fulltext_field('content', 
                      tokenizer=>fulltext_tokenizer('jieba', hmm=>TRUE)),
    enable_hybrid => TRUE,
    fusion_strategy => 'rrf',
    ft_weight => 0.7,
    vec_weight => 0.3
);
```

### 搜索查询

```sql
-- 基础全文搜索
SELECT *, content @@ fulltext_config('关键词') AS bm25_score
FROM articles
ORDER BY bm25_score DESC
LIMIT 10;

-- 混合搜索（全文 + 向量）
SELECT * FROM articles
WHERE content @@ fulltext_config('关键词', enable_hybrid=>TRUE)
ORDER BY hybrid_score DESC
LIMIT 10;

-- 指定字段
SELECT *, description @@ fulltext_config('description:关键词') AS score
FROM products
ORDER BY score DESC;

-- 邻近性搜索
SELECT * FROM articles
WHERE content @@ fulltext_config('content:"人工智能 机器学习"~2')
ORDER BY bm25_score DESC;

-- 模糊搜索
SELECT * FROM articles
WHERE content @@ fulltext_fuzzy('content', '机器学习', distance=>2)
ORDER BY bm25_score DESC;

-- 正则表达式
SELECT * FROM articles
WHERE content @@ fulltext_regex('content', '(AI|人工智能|机器学习)')
ORDER BY bm25_score DESC;

-- 布尔组合
SELECT * FROM articles
WHERE content @@ fulltext_boolean(
    must => ARRAY[fulltext_term('category', '科技')],
    should => ARRAY[
        fulltext_term('content', '人工智能'),
        fulltext_term('content', '机器学习')
    ]
)
ORDER BY bm25_score DESC;

-- 高级查询：混合 + 提升 + 范围
SELECT * FROM articles
WHERE content @@ fulltext_boolean(
    should => ARRAY[
        fulltext_term('content', '人工智能^2'),
        fulltext_term('content', '机器学习^1.5')
    ],
    must => ARRAY[
        fulltext_range('publish_date', '2023-01-01', '2024-12-31')
    ],
    enable_hybrid => TRUE
)
ORDER BY hybrid_score DESC
LIMIT 20;
```

## 实施计划

### 阶段 1：基础功能（2-3 周）
- 数据库表结构
- Jieba, Ngram, Default 分词器
- BM25 自动转换
- 倒排索引实现
- 词项查询、布尔查询

### 阶段 2：高级查询（2 周）
- 13 种查询类型完整实现
- 函数式配置接口
- DAAT_MAXSCORE/WAND 算法
- 高亮显示

### 阶段 3：混合搜索（2 周）
- 复用现有向量引擎
- RRF 融合算法
- 加权融合算法
- 自动向量生成

### 阶段 4：多类型字段（1 周）
- 数值、布尔、日期时间字段
- JSON 字段支持
- 范围查询

### 阶段 5：高级功能（1 周）
- Lindera 分词器（CJK）
- 词干提取器
- 查询解释

### 阶段 6：集成测试（1 周）
- 单元测试
- 集成测试
- 性能基准测试
- 与现有系统集成

## 特性对比表

| 特性 | Milvus | pgsearch | 本项目 | v3 融合方案 |
|-----|---------|---------|--------|-------------|
| **自动 BM25 转换** | ✅ | ❌ | ❌ | ✅ |
| **混合搜索** | ✅ | ❌ | ❌ | ✅ |
| **查询类型** | 3 | 13 | 1 | 13 |
| **SQL 接口** | ❌ | ✅ | ✅ | ✅ |
| **函数式配置** | ❌ | ✅ | ✅ | ✅ |
| **分词器** | Standard | 6 | 1 | 6 |
| **多类型字段** | 仅文本 | 文本/数值/布尔/日期/JSON | 仅文本 | 文本/数值/布尔/日期/JSON/向量 |
| **向量索引** | 1 | 0 | 10 | 10 |
| **高亮显示** | ❌ | ❌ | ❌ | ✅ |
| **模糊搜索** | ❌ | ✅ | ❌ | ✅ |
| **正则表达式** | ❌ | ✅ | ❌ | ✅ |
| **邻近性搜索** | ❌ | ✅ | ❌ | ✅ |
| **提升排名** | ❌ | ✅ | ❌ | ✅ |
| **中文优化** | 中 | 强 | 中 | 强 |

## 总结

v3 方案融合了三大优势：

1. **Milvus**：自动 BM25 转换 + 混合搜索（RRF）
2. **pgsearch**：13 种查询类型 + 函数式配置 + 多类型字段
3. **本项目**：10 种向量索引 + 多语言支持

**核心创新**：
- 自动将文档转换为稀疏向量（无需手动）
- 混合搜索（全文 + 向量）支持 RRF 和加权融合
- 13 种查询类型满足各种搜索需求
- 标准的 SQL 接口，易于使用
- 函数式配置接口，灵活强大

**适用场景**：
- RAG（检索增强生成）
- 电商搜索
- 文档管理
- 知识库搜索
- 内容推荐系统

**实施周期**：9-10 周
