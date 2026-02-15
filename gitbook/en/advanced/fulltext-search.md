# Full-Text Search

SQLExec includes a built-in full-text search engine based on the BM25 algorithm, using inverted indexes for efficient text retrieval with native Chinese tokenization support.

## Creating a Full-Text Index

```sql
CREATE FULLTEXT INDEX idx_content ON articles(content);
```

Specifying a tokenizer:

```sql
CREATE FULLTEXT INDEX idx_content ON articles(content)
    WITH (tokenizer = 'jieba');
```

## Basic Search

```sql
SELECT * FROM articles
WHERE MATCH(content) AGAINST('数据库');
```

### Boolean Mode

Boolean mode supports more precise search control:

```sql
SELECT * FROM articles
WHERE MATCH(content) AGAINST('数据库 AND 优化' IN BOOLEAN MODE);
```

## Tokenizers

SQLExec provides 4 tokenizers for different languages and scenarios:

| Tokenizer | Description | Use Case |
|--------|------|---------|
| Standard | Unicode standard tokenization, splitting by spaces and punctuation | General text, mixed languages |
| English | English tokenization with stemming support | English text |
| Ngram | N-gram splitting, suitable for CJK characters | Chinese/Japanese/Korean text, no dictionary required |
| Jieba | Jieba Chinese tokenizer, dictionary-based precise segmentation | Chinese text (recommended) |

How to specify a tokenizer:

```sql
-- Use the Jieba tokenizer (recommended for Chinese)
CREATE FULLTEXT INDEX idx_cn ON articles(content)
    WITH (tokenizer = 'jieba');

-- Use the Ngram tokenizer
CREATE FULLTEXT INDEX idx_ngram ON articles(content)
    WITH (tokenizer = 'ngram');

-- Use the English tokenizer
CREATE FULLTEXT INDEX idx_en ON articles(content)
    WITH (tokenizer = 'english');
```

## BM25 Parameters

The BM25 algorithm has two key tunable parameters:

| Parameter | Default | Description |
|------|--------|------|
| `K1` | 1.2 | Term frequency saturation parameter; higher values increase the impact of term frequency |
| `B` | 0.75 | Document length normalization parameter; higher values penalize longer documents more heavily |

```sql
CREATE FULLTEXT INDEX idx_content ON articles(content)
    WITH (tokenizer = 'jieba', k1 = 1.5, b = 0.5);
```

## Query Types

Full-text search supports a rich query syntax:

| Query Type | Syntax Example | Description |
|---------|---------|------|
| Term query | `'数据库'` | Matches documents containing the term |
| Phrase query | `'"分布式数据库"'` | Exact match of the complete phrase |
| Boolean query | `'数据库 AND 优化'` | Combine multiple conditions with AND, OR |
| Fuzzy query | `'databse~1'` | Fuzzy match with edit distance of 1 |
| Wildcard query | `'data*'` | Prefix match, matching words starting with data |
| Range query | `'[alpha TO omega]'` | Matches terms within a lexicographic range |

### Boolean Query Examples

```sql
-- AND: contains both keywords
SELECT * FROM articles
WHERE MATCH(content) AGAINST('数据库 AND 索引' IN BOOLEAN MODE);

-- OR: contains either keyword
SELECT * FROM articles
WHERE MATCH(content) AGAINST('MySQL OR PostgreSQL' IN BOOLEAN MODE);

-- Combined usage
SELECT * FROM articles
WHERE MATCH(content) AGAINST('(数据库 OR 存储) AND 优化' IN BOOLEAN MODE);
```

## Complete Example

```sql
-- 1. Create the articles table
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    content TEXT
);

-- 2. Insert Chinese documents
INSERT INTO articles (id, title, content) VALUES
(1, '数据库索引原理', '数据库索引是提高查询性能的关键技术，B+树索引是最常用的索引结构。'),
(2, '全文搜索引擎', '全文搜索引擎使用倒排索引来快速定位包含特定词语的文档。'),
(3, '分布式系统设计', '分布式数据库需要处理数据分片、副本同步和一致性等核心问题。');

-- 3. Create a full-text index (using the Jieba tokenizer)
CREATE FULLTEXT INDEX idx_article_content ON articles(content)
    WITH (tokenizer = 'jieba');

-- 4. Search for articles containing "数据库"
SELECT id, title,
       MATCH(content) AGAINST('数据库') AS score
FROM articles
WHERE MATCH(content) AGAINST('数据库')
ORDER BY score DESC;

-- 5. Boolean mode search
SELECT id, title
FROM articles
WHERE MATCH(content) AGAINST('数据库 AND 索引' IN BOOLEAN MODE);
```

## Chinese Search Example

The Jieba tokenizer can accurately segment Chinese text:

```sql
-- Create a table and insert Chinese content
CREATE TABLE news (
    id INT PRIMARY KEY,
    headline VARCHAR(500),
    body TEXT
);

INSERT INTO news (id, headline, body) VALUES
(1, '人工智能助力医疗诊断', '近年来，深度学习技术在医学影像分析领域取得了突破性进展，辅助医生进行更精准的疾病诊断。'),
(2, '新能源汽车市场持续增长', '电动汽车销量连续三年保持高速增长，充电基础设施建设也在加速推进。');

-- Create a Jieba full-text index
CREATE FULLTEXT INDEX idx_news_body ON news(body)
    WITH (tokenizer = 'jieba');

-- Search for news related to deep learning
SELECT id, headline
FROM news
WHERE MATCH(body) AGAINST('深度学习');
```
