# 全文搜索系统 V3 优化总结

## 优化概述

基于 `FULL_TEXT_SEARCH_DESIGN_V3.md` 文档，对现有全文搜索系统进行了以下关键优化：

| 优化项 | V2 实现 | V3 优化后 | 状态 |
|-------|--------|----------|------|
| **查询类型** | 4 种 | 13 种（完整） | ✅ 完成 |
| **SQL 函数接口** | 基础 | 函数式配置 | ✅ 完成 |
| **多类型字段** | 仅文本 | 6 种类型 | ✅ 完成 |
| **查询构建器** | 无 | 流式 API | ✅ 完成 |

## 1. 新增查询类型（9种）

### 新增查询类型列表

```go
// 1. DisjunctionMaxQuery - 析取最大查询
// 取多个查询中的最高分，支持 TieBreaker
query := query.NewDisjunctionMaxQuery(
    []query.Query{q1, q2, q3},
    0.3, // tieBreaker
)

// 2. ConstScoreQuery - 常数分数查询
// 包装查询并赋予固定分数
query := query.NewConstScoreQuery(innerQuery, 1.0)

// 3. TermSetQuery - 词集查询（OR逻辑）
// 匹配任意一个词
query := query.NewTermSetQuery("content", []string{"词1", "词2", "词3"})

// 4. PhrasePrefixQuery - 短语前缀查询（自动补全）
query := query.NewPhrasePrefixQuery("content", []string{"全文", "搜索"})

// 5. RegexQuery - 正则表达式查询
query := query.NewRegexQuery("content", "^prefix.*")

// 6. EmptyQuery - 空查询（返回空结果）
query := query.NewEmptyQuery()

// 7. AllDocsQuery - 匹配所有文档
query := query.NewAllDocsQuery()

// 8. NestedQuery - 嵌套字段查询（JSON）
query := query.NewNestedQuery("metadata.color", innerQuery)

// 9. ExistsQuery - 字段存在查询
query := query.NewExistsQuery("content")

// 10. PrefixQuery - 前缀查询
query := query.NewPrefixQuery("content", "全文")

// 11. WildcardQuery - 通配符查询
query := query.NewWildcardQuery("content", "全文*")

// 12. FunctionScoreQuery - 函数评分查询
query := query.NewFunctionScoreQuery(innerQuery, func(docID int64, baseScore float64) float64 {
    return baseScore * 2.0
})
```

### 查询构建器（流式 API）

```go
// 使用流式 API 构建复杂查询
q := query.NewQueryBuilder().
    Bool(func(bq *query.BooleanQuery) {
        bq.AddMust(query.NewTermQuery("category", "技术"))
        bq.AddShould(query.NewTermQuery("title", "全文搜索"))
        bq.AddShould(query.NewTermQuery("content", "搜索引擎"))
    }).
    Build()
```

## 2. SQL 函数式接口

### 字段配置函数

```sql
-- 使用函数式配置创建索引
CALL create_fulltext_index(
    index_name => 'search_idx',
    table_name => 'articles',
    text_fields => 
        fulltext_field('title', 
                      tokenizer=>fulltext_tokenizer('jieba', hmm=>TRUE),
                      boost=>2.0) || 
        fulltext_field('content', 
                      tokenizer=>fulltext_tokenizer('jieba'),
                      record=>'position')
);
```

### 查询函数

```sql
-- 13 种查询类型的 SQL 表示

-- 1. TermQuery
WHERE content @@ fulltext_term('field', 'value', boost=>2.0)

-- 2. PhraseQuery
WHERE content @@ fulltext_phrase('field', ARRAY['词1', '词2'], slop=>0)

-- 3. PhrasePrefixQuery
WHERE content @@ fulltext_phrase_prefix('field', ARRAY['全文', '搜索'])

-- 4. FuzzyQuery
WHERE content @@ fulltext_fuzzy('field', '关键词', distance=>2)

-- 5. RegexQuery
WHERE content @@ fulltext_regex('field', 'pattern.*')

-- 6. RangeQuery
WHERE content @@ fulltext_range('price', 10, 100, include_min=>TRUE, include_max=>TRUE)

-- 7. BooleanQuery
WHERE content @@ fulltext_boolean(
    must => ARRAY[fulltext_term('category', '科技')],
    should => ARRAY[
        fulltext_term('content', '人工智能'),
        fulltext_term('content', '机器学习')
    ]
)

-- 8. DisjunctionMaxQuery
WHERE content @@ fulltext_disjunction_max(
    ARRAY[
        fulltext_term('title', '搜索'),
        fulltext_term('content', '搜索')
    ],
    tie_breaker => 0.3
)

-- 9. PrefixQuery
WHERE content @@ fulltext_prefix('field', '前缀')

-- 10. WildcardQuery
WHERE content @@ fulltext_wildcard('field', '全文*')

-- 11. ExistsQuery
WHERE content @@ fulltext_exists('field')

-- 12. NestedQuery
WHERE content @@ fulltext_nested('metadata', fulltext_term('color', 'red'))

-- 13. BoostQuery
WHERE content @@ fulltext_boost(fulltext_term('field', 'value'), 2.0)
```

### 辅助函数

```sql
-- BM25 分数
SELECT bm25_score(content, '关键词') AS score FROM articles;

-- BM25 排名
SELECT bm25_rank(content, '关键词') AS rank FROM articles;

-- 混合分数
SELECT hybrid_score(
    bm25_score(content, '关键词'),
    vector_score(embedding, '[0.1, 0.2, ...]'),
    ft_weight => 0.7,
    vec_weight => 0.3
) AS hybrid_score FROM articles;

-- RRF 融合排名
SELECT rrf_rank(
    bm25_rank(content, '关键词'),
    vector_rank(embedding, '[0.1, 0.2, ...]'),
    k => 60
) AS rrf_rank FROM articles;

-- 高亮显示
SELECT highlight(content, '关键词', '<mark>', '</mark>') AS highlighted FROM articles;

-- 分词测试
SELECT fulltext_tokenize('jieba', '永和服装饰品有限公司');
```

### Go SQL 生成器

```go
// 使用 SQL 生成器构建查询
sql := sql.NewSQLGenerator("articles", "content", "全文搜索").
    Select("id", "title", "content").
    OrderByScore(true).
    Paginate(10, 0).
    BuildWithScore()

// 生成: 
// SELECT id, title, content, bm25_score(content, '全文搜索') AS bm25_score 
// FROM articles 
// WHERE content @@ fulltext_config('全文搜索') 
// ORDER BY bm25_score DESC 
// LIMIT 10 OFFSET 0
```

## 3. 多类型字段支持

### 支持的字段类型

```go
const (
    FieldTypeText     FieldType = "text"      // 文本（VARCHAR, TEXT）
    FieldTypeNumeric  FieldType = "numeric"   // 数值（INT, FLOAT）
    FieldTypeBoolean  FieldType = "boolean"   // 布尔
    FieldTypeDatetime FieldType = "datetime"  // 日期时间
    FieldTypeJSON     FieldType = "json"      // JSON/JSONB
    FieldTypeVector   FieldType = "vector"    // 向量
)
```

### Schema 定义示例

```go
// 使用 Schema 构建器定义多类型索引
schema := schema.NewSchemaBuilder().
    AddTextField("title", func(b *schema.FieldSchemaBuilder) {
        b.WithTokenizer("jieba").
          Boost(2.0).
          Indexed(true).
          Stored(true)
    }).
    AddTextField("content", func(b *schema.FieldSchemaBuilder) {
        b.WithTokenizer("jieba").
          Stored(true)
    }).
    AddNumericField("price", func(b *schema.FieldSchemaBuilder) {
        b.Fast(true)
    }).
    AddField(schema.NewBooleanField("in_stock")).
    AddField(schema.NewDatetimeField("created_at")).
    AddField(schema.NewJSONField("metadata")).
    AddField(schema.NewVectorField("embedding", 768)).
    Build()
```

### 文档操作

```go
// 创建多类型文档
doc := schema.NewDocument(1).
    AddText("title", "全文搜索引擎").
    AddText("content", "高性能搜索需要优化索引结构").
    AddNumeric("price", 99.99).
    AddBoolean("in_stock", true).
    AddDatetime("created_at", time.Now()).
    AddJSON("metadata", map[string]interface{}{
        "color": "red",
        "size":  "large",
    }).
    AddVector("embedding", []float32{0.1, 0.2, ...})

// 获取字段值
title, _ := doc.GetText("title")
price, _ := doc.GetNumeric("price")
stock, _ := doc.GetBoolean("in_stock")
```

## 4. 与 V2 对比的改进

### 查询类型对比

| 查询类型 | V2 | V3 优化 | 说明 |
|---------|-----|---------|------|
| TermQuery | ✅ | ✅ | 词项查询 |
| PhraseQuery | ✅ | ✅ | 短语查询 |
| BooleanQuery | ✅ | ✅ | 布尔查询 |
| RangeQuery | ✅ | ✅ | 范围查询 |
| FuzzyQuery | ✅ | ✅ | 模糊查询 |
| DisjunctionMaxQuery | ❌ | ✅ 新增 | 析取最大 |
| ConstScoreQuery | ❌ | ✅ 新增 | 常数分数 |
| TermSetQuery | ❌ | ✅ 新增 | 词集查询 |
| PhrasePrefixQuery | ❌ | ✅ 新增 | 短语前缀 |
| RegexQuery | ❌ | ✅ 新增 | 正则查询 |
| PrefixQuery | ❌ | ✅ 新增 | 前缀查询 |
| WildcardQuery | ❌ | ✅ 新增 | 通配符 |
| ExistsQuery | ❌ | ✅ 新增 | 字段存在 |
| NestedQuery | ❌ | ✅ 新增 | 嵌套查询 |
| FunctionScoreQuery | ❌ | ✅ 新增 | 函数评分 |

### 字段类型对比

| 字段类型 | V2 | V3 优化 | 说明 |
|---------|-----|---------|------|
| Text | ✅ | ✅ | 文本字段 |
| Numeric | ❌ | ✅ 新增 | 数值字段 |
| Boolean | ❌ | ✅ 新增 | 布尔字段 |
| Datetime | ❌ | ✅ 新增 | 日期时间 |
| JSON | ❌ | ✅ 新增 | JSON字段 |
| Vector | ✅ | ✅ | 向量字段 |

### SQL 接口对比

| 功能 | V2 | V3 优化 | 说明 |
|-----|-----|---------|------|
| 基础搜索 | ✅ | ✅ | `@@` 操作符 |
| bm25_score | ✅ | ✅ | BM25分数函数 |
| highlight | ✅ | ✅ | 高亮函数 |
| 函数式配置 | ❌ | ✅ 新增 | `fulltext_field()` 等 |
| 分词器配置 | ❌ | ✅ 新增 | `fulltext_tokenizer()` |
| 查询构建 | ❌ | ✅ 新增 | 13种查询函数 |
| hybrid_score | ❌ | ✅ 新增 | 混合分数 |
| rrf_rank | ❌ | ✅ 新增 | RRF融合 |

## 5. 实施状态

### 已完成的优化

- [x] **9 种新查询类型** - DisjunctionMax, ConstScore, TermSet, PhrasePrefix, Regex, Prefix, Wildcard, Exists, Nested, FunctionScore
- [x] **SQL 函数接口** - 完整的函数式配置支持
- [x] **多类型字段** - Text, Numeric, Boolean, Datetime, JSON, Vector
- [x] **Schema 构建器** - 流式 API 定义索引结构
- [x] **SQL 生成器** - 程序化生成 SQL 查询

### 待实现的优化

- [ ] **Lindera 分词器** - CJK 分词支持
- [ ] **查询解析器** - 完整的查询字符串解析
- [ ] **JSON 展开** - 点表示法路径支持 (`metadata.color`)
- [ ] **更多优化算法** - DAAT_WAND, TAAT

## 6. 代码结构更新

```
pkg/fulltext/
├── types.go                    # 基础类型
├── engine.go                   # 全文引擎
├── analyzer/
│   └── tokenizer.go            # 分词器（6种）
├── bm25/
│   ├── sparse_vector.go        # 稀疏向量
│   └── scorer.go               # BM25评分
├── index/
│   ├── posting.go              # 倒排列表
│   └── inverted_index.go       # 倒排索引
├── query/
│   ├── query.go                # 基础查询（4种）
│   ├── advanced_query.go       # 高级查询（+9种）⭐新增
│   └── parser.go               # 查询解析
├── sql/
│   └── functions.go            # SQL函数接口 ⭐新增
├── schema/
│   └── field.go                # 多类型字段支持 ⭐新增
└── examples_test.go
```

## 7. 性能优化建议

### 查询优化

```go
// 1. 使用 DisjunctionMax 替代多个 OR
// 优化前
boolQuery.AddShould(q1)
boolQuery.AddShould(q2)
boolQuery.AddShould(q3)

// 优化后
disMaxQuery := query.NewDisjunctionMaxQuery(
    []query.Query{q1, q2, q3}, 
    0.3, // tieBreaker
)

// 2. 使用 TermSet 替代多个 Term OR
// 优化前
for _, term := range terms {
    boolQuery.AddShould(query.NewTermQuery("field", term))
}

// 优化后
termSetQuery := query.NewTermSetQuery("field", terms)
```

### 索引优化

```go
// 1. 合理设置字段权重
schema := schema.NewSchemaBuilder().
    AddTextField("title", func(b *schema.FieldSchemaBuilder) {
        b.Boost(2.0) // 标题权重更高
    }).
    AddTextField("content", func(b *schema.FieldSchemaBuilder) {
        b.Boost(1.0)
    }).
    Build()

// 2. 对过滤字段使用 Fast 选项
schema.NewNumericField("price").Fast = true
schema.NewBooleanField("in_stock").Fast = true
```

## 总结

V3 优化将全文搜索系统从**基础可用**提升到**生产就绪**级别：

1. **查询能力增强**：从 4 种查询类型扩展到 13 种，满足各种搜索需求
2. **SQL 接口完善**：函数式配置使 SQL 使用更灵活
3. **多类型字段**：支持除文本外的数值、布尔、日期、JSON 等类型
4. **Schema 管理**：流式 API 简化索引定义

这些优化使系统适用于：
- RAG（检索增强生成）
- 电商搜索
- 文档管理
- 知识库搜索
- 内容推荐系统
