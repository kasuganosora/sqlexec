# 全文搜索 SQL 接口文档

本文档描述如何在 SQL 层使用全文搜索功能。

## 1. 创建全文索引

### 1.1 基础用法

```sql
-- 创建基础全文索引（使用默认配置）
CREATE FULLTEXT INDEX idx_ft_content ON articles(content);

-- 指定分词器
CREATE FULLTEXT INDEX idx_ft_content ON articles(content)
WITH TOKENIZER = 'jieba';

-- 指定分词器和BM25参数
CREATE FULLTEXT INDEX idx_ft_content ON articles(content)
WITH (
    TOKENIZER = 'jieba',
    BM25_K1 = 1.5,
    BM25_B = 0.75
);

-- 多字段全文索引
CREATE FULLTEXT INDEX idx_ft_multi ON articles(title, content, tags)
WITH (
    TOKENIZER = 'jieba',
    FIELD_WEIGHTS = '{"title": 3.0, "content": 1.0, "tags": 2.0}'
);
```

### 1.2 高级配置

```sql
-- N-gram 分词器（适合中英文混合）
CREATE FULLTEXT INDEX idx_ft_content ON articles(content)
WITH (
    TOKENIZER = 'ngram',
    TOKENIZER_OPTIONS = '{"min_gram": 2, "max_gram": 3, "prefix_only": false}'
);

-- 英文分词器（带词干提取）
CREATE FULLTEXT INDEX idx_ft_content ON articles(content)
WITH TOKENIZER = 'english';
```

## 2. 全文搜索语法

### 2.1 基础搜索

```sql
-- 简单搜索（默认返回按BM25分数排序）
SELECT * FROM articles 
WHERE content @@ '全文搜索'
ORDER BY bm25_score DESC
LIMIT 10;

-- 获取BM25分数
SELECT 
    id, title, content,
    bm25_score(content, '全文搜索') as relevance
FROM articles 
WHERE content @@ '全文搜索'
ORDER BY relevance DESC;
```

### 2.2 短语搜索

```sql
-- 精确短语匹配
SELECT * FROM articles 
WHERE content @@ '"全文搜索引擎"'
ORDER BY bm25_score DESC;

-- 邻近搜索（允许2个词的距离）
SELECT * FROM articles 
WHERE content @@ '"全文 引擎"~2'
ORDER BY bm25_score DESC;
```

### 2.3 布尔查询

```sql
-- AND 查询（必须同时包含）
SELECT * FROM articles 
WHERE content @@ '数据库 AND 优化'
ORDER BY bm25_score DESC;

-- OR 查询
SELECT * FROM articles 
WHERE content @@ 'MySQL OR PostgreSQL'
ORDER BY bm25_score DESC;

-- NOT 查询
SELECT * FROM articles 
WHERE content @@ '数据库 NOT MySQL'
ORDER BY bm25_score DESC;

-- 组合查询
SELECT * FROM articles 
WHERE content @@ '(数据库 OR 存储) AND 优化 NOT MongoDB'
ORDER BY bm25_score DESC;

-- +/- 语法
SELECT * FROM articles 
WHERE content @@ '+必须包含 -必须排除 可选包含'
ORDER BY bm25_score DESC;
```

### 2.4 字段限定搜索

```sql
-- 只在title字段搜索
SELECT * FROM articles 
WHERE content @@ 'title:数据库优化'
ORDER BY bm25_score DESC;

-- 多字段搜索
SELECT * FROM articles 
WHERE content @@ 'title:数据库 OR content:优化'
ORDER BY bm25_score DESC;
```

### 2.5 模糊搜索

```sql
-- 模糊搜索（编辑距离默认2）
SELECT * FROM articles 
WHERE content @@ '数据库~'
ORDER BY bm25_score DESC;

-- 指定编辑距离
SELECT * FROM articles 
WHERE content @@ '数据库~1'
ORDER BY bm25_score DESC;
```

### 2.6 权重提升

```sql
-- 提升关键词权重
SELECT * FROM articles 
WHERE content @@ '数据库^2.0 优化^1.5 索引'
ORDER BY bm25_score DESC;
```

## 3. 高亮显示

```sql
-- 基础高亮
SELECT 
    id, title,
    highlight(content, '全文搜索', '<mark>', '</mark>') as highlighted
FROM articles 
WHERE content @@ '全文搜索'
ORDER BY bm25_score DESC;

-- 多片段高亮
SELECT 
    id, title,
    highlight_multi(
        content, 
        '全文搜索', 
        '<mark>', '</mark>',
        3,           -- 最大片段数
        150          -- 每个片段长度
    ) as fragments
FROM articles 
WHERE content @@ '全文搜索';
```

## 4. 混合搜索（全文 + 向量）

### 4.1 使用 RRF 融合

```sql
-- RRF (Reciprocal Rank Fusion) 融合
SELECT * FROM articles 
WHERE content @@ '数据库优化'
ORDER BY rrf_rank(
    bm25_rank(content, '数据库优化'),
    vector_rank(embedding, '[0.1, 0.2, ...]')
) ASC
LIMIT 10;
```

### 4.2 使用加权融合

```sql
-- 加权融合（70%全文 + 30%向量）
SELECT 
    id, title, content,
    hybrid_score(
        bm25_score(content, '数据库优化') * 0.7 +
        vector_score(embedding, '[0.1, 0.2, ...]') * 0.3
    ) as hybrid_score
FROM articles 
WHERE content @@ '数据库优化'
ORDER BY hybrid_score DESC
LIMIT 10;
```

### 4.3 混合搜索配置

```sql
-- 设置混合搜索权重
SET hybrid_search_fulltext_weight = 0.6;
SET hybrid_search_vector_weight = 0.4;
SET hybrid_search_rrf_k = 60;

-- 使用配置进行搜索
SELECT * FROM articles 
WHERE content @@ '数据库优化'
ORDER BY hybrid_rank(content, embedding, '数据库优化') ASC
LIMIT 10;
```

## 5. 管理函数

### 5.1 索引管理

```sql
-- 查看全文索引信息
SELECT * FROM information_schema.fulltext_indexes 
WHERE table_name = 'articles';

-- 查看索引统计信息
SELECT * FROM fulltext_index_stats('idx_ft_content');

-- 重建全文索引
CALL rebuild_fulltext_index('idx_ft_content');

-- 删除全文索引
DROP FULLTEXT INDEX idx_ft_content ON articles;
```

### 5.2 分词测试

```sql
-- 查看分词结果
SELECT fulltext_tokenize(
    'jieba', 
    '全文搜索引擎是信息检索的核心组件'
);

-- 查看分词结果（带配置）
SELECT fulltext_tokenize(
    'ngram', 
    '全文搜索',
    '{"min_gram": 2, "max_gram": 3}'
);
```

### 5.3 索引状态

```sql
-- 查看索引文档数量
SELECT fulltext_doc_count('idx_ft_content');

-- 查看索引词汇数量
SELECT fulltext_term_count('idx_ft_content');

-- 查看词频统计
SELECT * FROM fulltext_term_stats('idx_ft_content') 
ORDER BY doc_freq DESC 
LIMIT 20;
```

## 6. 高级用法

### 6.1 分页搜索

```sql
-- 分页查询
SELECT * FROM articles 
WHERE content @@ '全文搜索'
ORDER BY bm25_score DESC
LIMIT 10 OFFSET 20;

-- 使用游标分页
SELECT * FROM articles 
WHERE content @@ '全文搜索' 
  AND bm25_score(content, '全文搜索') < 0.5  -- 上一页最后一条的分数
ORDER BY bm25_score DESC
LIMIT 10;
```

### 6.2 过滤器组合

```sql
-- 全文搜索 + 普通条件过滤
SELECT * FROM articles 
WHERE content @@ '全文搜索'
  AND category = '技术'
  AND created_at > '2024-01-01'
ORDER BY bm25_score DESC
LIMIT 10;

-- 全文搜索 + 范围过滤
SELECT * FROM articles 
WHERE content @@ '数据库优化'
  AND rating BETWEEN 4 AND 5
ORDER BY bm25_score DESC;
```

### 6.3 聚合统计

```sql
-- 搜索结果聚合
SELECT 
    category,
    COUNT(*) as count,
    AVG(bm25_score(content, '全文搜索')) as avg_score
FROM articles 
WHERE content @@ '全文搜索'
GROUP BY category
ORDER BY count DESC;
```

### 6.4 相关推荐

```sql
-- 根据文档内容找相似文档
SELECT 
    b.id, b.title,
    bm25_score(b.content, a.content) as similarity
FROM articles a
JOIN articles b ON a.id != b.id
WHERE a.id = 1
ORDER BY similarity DESC
LIMIT 5;
```

## 7. 性能优化提示

### 7.1 索引优化

```sql
-- 为常用查询创建复合索引
CREATE FULLTEXT INDEX idx_ft_title_content ON articles(title, content);

-- 使用前缀索引加速查询
CREATE FULLTEXT INDEX idx_ft_content_prefix ON articles(content)
WITH (
    TOKENIZER = 'ngram',
    TOKENIZER_OPTIONS = '{"prefix_only": true, "min_gram": 2, "max_gram": 4}'
);
```

### 7.2 查询优化

```sql
-- 使用更精确的查询词
SELECT * FROM articles 
WHERE content @@ '"全文搜索引擎"'  -- 使用短语而非单个词
ORDER BY bm25_score DESC
LIMIT 10;

-- 限制搜索范围
SELECT * FROM articles 
WHERE content @@ '数据库优化'
  AND id BETWEEN 1000 AND 2000  -- 限制文档范围
ORDER BY bm25_score DESC
LIMIT 10;
```

## 8. Go API 使用示例

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/kasuganosora/sqlexec/pkg/resource/memory"
    "github.com/kasuganosora/sqlexec/pkg/fulltext"
)

func main() {
    // 创建索引管理器
    indexManager := memory.NewIndexManager()
    
    // 创建高级全文索引
    ftIndex, err := indexManager.CreateAdvancedFullTextIndex(
        "articles", "content",
        &memory.AdvancedFullTextIndexConfig{
            TokenizerType: fulltext.TokenizerTypeJieba,
            BM25Params: fulltext.BM25Params{
                K1: 1.2,
                B:  0.75,
            },
        },
    )
    if err != nil {
        log.Fatal(err)
    }
    
    // 索引文档
    documents := map[int64]string{
        1: "全文搜索引擎是信息检索的核心组件",
        2: "高性能搜索需要优化索引结构",
        3: "倒排索引是搜索引擎的基础数据结构",
    }
    
    for docID, content := range documents {
        if err := ftIndex.Insert(content, []int64{docID}); err != nil {
            log.Printf("Index error: %v", err)
        }
    }
    
    // 搜索
    results, err := ftIndex.Search("搜索引擎", 10)
    if err != nil {
        log.Fatal(err)
    }
    
    // 打印结果
    for _, r := range results {
        fmt.Printf("Doc %d: score=%.4f\n", r.DocID, r.Score)
    }
    
    // 带高亮的搜索
    highlightedResults, err := ftIndex.SearchWithHighlight(
        "搜索引擎", 10, "<mark>", "</mark>",
    )
    if err != nil {
        log.Fatal(err)
    }
    
    for _, r := range highlightedResults {
        fmt.Printf("Doc %d: highlights=%v\n", r.DocID, r.Highlights)
    }
}
```

## 9. 配置参数

| 参数 | 说明 | 默认值 | 范围 |
|-----|------|-------|-----|
| `BM25_K1` | 词频饱和参数 | 1.2 | 1.0-2.0 |
| `BM25_B` | 长度归一化参数 | 0.75 | 0.0-1.0 |
| `TOKENIZER` | 分词器类型 | `standard` | `jieba`/`ngram`/`english`/`standard` |
| `FIELD_WEIGHTS` | 字段权重 | `{}` | JSON对象 |
| `STOP_WORDS` | 停用词列表 | `default` | `default`/`none`/自定义 |
| `MIN_TOKEN_LEN` | 最小词长度 | 2 | >=1 |
| `MAX_TOKEN_LEN` | 最大词长度 | 100 | >=10 |

## 10. 故障排除

### 10.1 搜索结果为空

```sql
-- 检查索引是否存在
SHOW INDEX FROM articles WHERE Index_type = 'FULLTEXT';

-- 检查分词结果
SELECT fulltext_tokenize('jieba', '测试文本');

-- 检查索引统计
SELECT fulltext_doc_count('idx_ft_content');
```

### 10.2 搜索性能慢

```sql
-- 检查索引使用情况
EXPLAIN SELECT * FROM articles WHERE content @@ '关键词';

-- 检查索引大小
SELECT fulltext_index_size('idx_ft_content');

-- 重建索引
CALL rebuild_fulltext_index('idx_ft_content');
```
