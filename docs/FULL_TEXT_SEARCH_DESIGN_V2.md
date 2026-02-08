# 全文搜索设计方案 v2（参考 pgsearch）

## 概述

参考 AnalyticDB PostgreSQL 版的 pgsearch 插件，基于高性能全文检索引擎 Tantivy，实现更强大的 BM25 全文搜索功能。支持中文、日文、韩文等多语言，提供丰富的查询语法和函数式配置接口。

## 核心特性

### 1. 基于 Tantivy 引擎
- 高性能全文检索引擎（Rust 实现）
- 零拷贝序列化，高性能
- 支持增量更新

### 2. 多种分词器
- **Jieba**: 中文分词，支持自定义词典和停用词
- **Ngram**: n-gram 分词，适用于中英文混合场景
- **Chinese Compatible**: 中文兼容分词
- **Lindera**: 支持中日韩（CJK）分词
- **English Stem**: 英文词干提取
- **Default**: 基础空格分词

### 3. 多类型字段索引
- **文本字段**: VARCHAR, TEXT
- **数值字段**: INT, BIGINT, FLOAT, DOUBLE
- **布尔字段**: BOOLEAN
- **日期时间字段**: DATE, TIMESTAMP
- **JSON 字段**: JSON, JSONB（支持嵌套）

### 4. 丰富的查询语法
- 字段指定检索
- 邻近性操作符 (~)
- 短语检索和前缀匹配
- 模糊检索（Levenshtein 距离）
- 正则表达式检索
- 布尔组合查询
- 提升排名 (^)

### 5. 函数式配置
- `fulltext_field()`: 字段配置
- `fulltext_tokenizer()`: 分词器配置
- `fulltext_config()`: 查询配置

## 数据结构设计

### 1. 表结构（SQL）

```sql
-- 全文索引元数据表
CREATE TABLE fulltext_index_metadata (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    index_name VARCHAR(255) NOT NULL UNIQUE,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) DEFAULT 'public',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_table (table_name, schema_name)
) ENGINE=InnoDB;

-- 字段配置表
CREATE TABLE fulltext_field_config (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    index_id BIGINT NOT NULL,
    field_name VARCHAR(255) NOT NULL,
    field_type VARCHAR(50) NOT NULL, -- text, numeric, boolean, datetime, json
    is_fast BOOLEAN DEFAULT FALSE,
    tokenizer_config JSON,
    record_type VARCHAR(20) DEFAULT 'position', -- raw, freq, position
    FOREIGN KEY (index_id) REFERENCES fulltext_index_metadata(id) ON DELETE CASCADE,
    UNIQUE KEY uk_index_field (index_id, field_name)
) ENGINE=InnoDB;

-- BM25 统计信息表
CREATE TABLE fulltext_bm25_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    index_id BIGINT NOT NULL,
    avg_doc_length FLOAT NOT NULL,
    total_docs INT NOT NULL,
    k1 FLOAT DEFAULT 1.2,
    b FLOAT DEFAULT 0.75,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (index_id) REFERENCES fulltext_index_metadata(id) ON DELETE CASCADE,
    UNIQUE KEY uk_index (index_id)
) ENGINE=InnoDB;

-- 词汇表
CREATE TABLE fulltext_vocabulary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    index_id BIGINT NOT NULL,
    term VARCHAR(255) NOT NULL,
    df INT DEFAULT 0, -- 文档频率
    idf FLOAT DEFAULT 0, -- 逆文档频率
    total_tf INT DEFAULT 0, -- 总词频
    language VARCHAR(10) DEFAULT 'zh',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (index_id) REFERENCES fulltext_index_metadata(id) ON DELETE CASCADE,
    UNIQUE KEY uk_index_term (index_id, term, language),
    INDEX idx_term (term, language)
) ENGINE=InnoDB;

-- 倒排索引表（存储稀疏向量）
CREATE TABLE fulltext_inverted_index (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    index_id BIGINT NOT NULL,
    term_id INT NOT NULL,
    doc_id BIGINT NOT NULL,
    score FLOAT NOT NULL, -- BM25 分数
    frequency INT NOT NULL, -- 词频
    positions JSON, -- 位置信息 [pos1, pos2, ...]
    field_name VARCHAR(255) NOT NULL, -- 字段名
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (index_id) REFERENCES fulltext_index_metadata(id) ON DELETE CASCADE,
    FOREIGN KEY (term_id) REFERENCES fulltext_vocabulary(id) ON DELETE CASCADE,
    INDEX idx_term (index_id, term_id),
    INDEX idx_doc (index_id, doc_id)
) ENGINE=InnoDB;
```

### 2. 核心数据结构（Go）

```go
// FulltextIndex 全文索引
type FulltextIndex struct {
    ID          int64
    Name        string
    TableName   string
    SchemaName  string
    
    // 字段配置
    FieldConfigs map[string]*FieldConfig
    
    // 倒排索引
    InvertedIndex *InvertedIndex
    
    // 词汇表
    Vocabulary *Vocabulary
    
    // BM25 统计
    BM25Stats *BM25Stats
    
    // 分析器
    Analyzer MultiLanguageAnalyzer
}

// FieldConfig 字段配置
type FieldConfig struct {
    Name           string
    Type           string // text, numeric, boolean, datetime, json
    IsFast         bool   // 快速访问
    TokenizerConfig *TokenizerConfig
    RecordType     string // raw, freq, position
}

// TokenizerConfig 分词器配置
type TokenizerConfig struct {
    Type         string // jieba, ngram, lindera, en_stem, default, raw
    MinGram      int    // ngram 最小长度
    MaxGram      int    // ngram 最大长度
    PrefixOnly   bool   // ngram 是否只生成前缀
    SearchMode   bool   // jieba 搜索模式
    HMM          bool   // jieba HMM 辅助分词
    DictPath     string // 自定义词典路径
    StopWordsPath string // 停用词路径
    Lowercase    bool   // 转小写
    RemoveLong   int    // 移除长于该长度的词
    Stemmer      string // 词干提取器（仅英文）
}

// BM25Stats BM25 统计信息
type BM25Stats struct {
    IndexID      int64
    AvgDocLength float64
    TotalDocs    int
    K1           float64
    B            float64
}

// PostingsList 倒排链
type PostingsList struct {
    DocID     int64
    Score     float64
    Frequency int
    Positions []int
    FieldName string
    Next      *PostingsList
}

// Query 查询接口
type Query interface {
    Execute(index *FulltextIndex) []SearchResult
}

// BooleanQuery 布尔查询
type BooleanQuery struct {
    Must     []Query
    MustNot  []Query
    Should   []Query
    MinimumShouldMatch int
}

// TermQuery 词项查询
type TermQuery struct {
    Field  string
    Value  string
    Boost  float64
}

// PhraseQuery 短语查询
type PhraseQuery struct {
    Field    string
    Phrases  []string
    Slop     int
    Boost    float64
}

// FuzzyQuery 模糊查询
type FuzzyQuery struct {
    Field            string
    Value            string
    Distance         int
    TranspositionCostOne bool
    Prefix           bool
    Boost            float64
}

// RangeQuery 范围查询
type RangeQuery struct {
    Field      string
    MinValue   interface{}
    MaxValue   interface{}
    IncludeMin bool
    IncludeMax bool
    Boost      float64
}

// RegexQuery 正则表达式查询
type RegexQuery struct {
    Field   string
    Pattern string
    Boost   float64
}

// SearchResult 搜索结果
type SearchResult struct {
    DocID      int64
    Score      float64
    Highlights []string // 高亮文本
}

// Highlighter 高亮器
type Highlighter struct {
    PreTag  string
    PostTag string
    FragmentLen int
    NumFragments int
}
```

## SQL 接口设计

### 1. 创建全文索引

```sql
-- 方式1：使用 JSON 配置
CALL create_fulltext_index(
    index_name => 'search_idx',
    table_name => 'mock_items',
    schema_name => 'public',
    text_fields => '{
        "description": {
            "fast": false,
            "fieldnorms": true,
            "tokenizer": {"type": "jieba", "hmm": true, "search": true}
        },
        "category": {
            "tokenizer": {"type": "ngram", "min_gram": 2, "max_gram": 3}
        }
    }',
    numeric_fields => '{
        "rating": {"fast": true}
    }',
    boolean_fields => '{
        "in_stock": {"fast": true}
    }',
    datetime_fields => '{
        "created_at": {"fast": true}
    }',
    json_fields => '{
        "metadata": {
            "fast": true,
            "expand_dots": true,
            "tokenizer": {"type": "en_stem"}
        }
    }'
);

-- 方式2：使用函数式配置（推荐）
CALL create_fulltext_index(
    index_name => 'search_idx',
    table_name => 'mock_items',
    schema_name => 'public',
    text_fields => 
        fulltext_field('description', fast=>FALSE, record=>'position', 
                      tokenizer=>fulltext_tokenizer('jieba', hmm=>TRUE, search=>TRUE)) || 
        fulltext_field('category'),
    numeric_fields => 
        fulltext_field('rating', fast=>TRUE),
    boolean_fields => 
        fulltext_field('in_stock', fast=>TRUE),
    datetime_fields => 
        fulltext_field('created_at', fast=>TRUE),
    json_fields => 
        fulltext_field('metadata', fast=>TRUE, expand_dots=>TRUE,
                      tokenizer=>fulltext_tokenizer('en_stem'))
);
```

### 2. 全文搜索语法

```sql
-- 基础全文搜索
SELECT * FROM mock_items
WHERE description @@ fulltext_config('socks')
ORDER BY bm25_score DESC
LIMIT 10;

-- 获取 BM25 分数
SELECT *, description @@ fulltext_config('socks') AS bm25_score
FROM mock_items
ORDER BY bm25_score DESC
LIMIT 10;

-- 指定字段搜索
SELECT * FROM mock_items
WHERE description @@ fulltext_config('description:socks')
ORDER BY bm25_score DESC;

-- JSON 字段搜索
SELECT * FROM mock_items
WHERE metadata @@ fulltext_config('metadata.color:white')
ORDER BY bm25_score DESC;

-- 邻近性搜索（slop）
SELECT * FROM mock_items
WHERE description @@ fulltext_config('description:"ergonomic keyboard"~1')
ORDER BY bm25_score DESC;

-- 布尔组合
SELECT * FROM mock_items
WHERE description @@ fulltext_config('description:keyboard OR category:toy')
ORDER BY bm25_score DESC;

-- 提升排名
SELECT * FROM mock_items
WHERE description @@ fulltext_config('description:keyboard^2 OR category:electronics^3')
ORDER BY bm25_score DESC;

-- 范围查询
SELECT * FROM mock_items
WHERE description @@ fulltext_config('description:socks AND rating:[1 TO 4]')
ORDER BY bm25_score DESC;

-- 模糊查询
SELECT * FROM mock_items
WHERE description @@ fulltext_fuzzy('description', 'wow', distance=>2, prefix=>TRUE)
ORDER BY bm25_score DESC;

-- 正则表达式
SELECT * FROM mock_items
WHERE description @@ fulltext_regex('description', '(glass|screen|like|cloth|phone)')
ORDER BY bm25_score DESC;

-- 高级布尔查询
SELECT * FROM mock_items
WHERE description @@ fulltext_boolean(
    must => ARRAY[fulltext_term('rating', '4')],
    must_not => ARRAY[fulltext_term('description', 'writer')],
    should => ARRAY[
        fulltext_term('description', 'socks'),
        fulltext_phrase_prefix('description', ARRAY['book'])
    ]
)
ORDER BY bm25_score DESC;
```

### 3. 管理函数

```sql
-- 查看索引配置
SELECT * FROM fulltext_index_metadata WHERE index_name = 'search_idx';

-- 查看字段配置
SELECT * FROM fulltext_field_config WHERE index_id = 1;

-- 查看分词效果
SELECT fulltext_tokenize(
    fulltext_tokenizer('jieba', hmm=>FALSE, search=>FALSE),
    '永和服装饰品有限公司'
);

-- 重建索引
CALL rebuild_fulltext_index('search_idx');

-- 删除索引
DROP INDEX search_idx;
```

## 与 Milvus 对比

| 特性 | Milvus | pgsearch | 本设计 |
|-----|---------|---------|--------|
| **底层引擎** | Tantivy | Tantivy | 可选 Tantivy 或自研 |
| **分词器** | Standard | Jieba, Ngram, Lindera 等 | Jieba, Ngram, Lindera 等 |
| **多类型字段** | 仅文本 | 文本/数值/布尔/日期/JSON | 文本/数值/布尔/日期/JSON |
| **查询语法** | 专用 API | @@@ 操作符 | @@ 操作符 + 函数 |
| **高亮显示** | ✅ | ❌ | ✅ |
| **模糊搜索** | ❌ | ✅ | ✅ |
| **正则表达式** | ❌ | ✅ | ✅ |
| **邻近性搜索** | ❌ | ✅ | ✅ |
| **混合搜索** | ✅ | ❌ | ✅ |

## 实施计划

### 阶段 1：基础功能（2-3 周）
1. 数据库表结构
2. 基础分词器（Jieba, Ngram, Default）
3. BM25 评分算法
4. 倒排索引实现
5. 基础查询（词项查询、布尔查询）

### 阶段 2：高级查询（2 周）
1. 短语查询
2. 模糊查询
3. 正则表达式查询
4. 范围查询
5. 提升排名

### 阶段 3：性能优化（2 周）
1. DAAT_MAXSCORE 算法
2. DAAT_WAND 算法
3. 缓存机制
4. 并发控制（分片）

### 阶段 4：高级功能（2 周）
1. 高亮显示
2. 多类型字段支持（数值、布尔、日期、JSON）
3. Lindera 分词器（CJK）
4. 词干提取器

### 阶段 5：集成测试（1 周）
1. 单元测试
2. 集成测试
3. 性能基准测试
4. 与现有系统集成

## 总结

本设计参考了 pgsearch 的优秀实现，提供了：

1. **更强大的分词器支持**：6 种分词器，特别优化中文分词
2. **更丰富的查询语法**：13 种查询类型，满足各种搜索需求
3. **更多类型的字段索引**：支持文本、数值、布尔、日期、JSON
4. **函数式配置接口**：`fulltext_field()`, `fulltext_tokenizer()`, `fulltext_config()`
5. **高性能优化算法**：DAAT_MAXSCORE, DAAT_WAND

相比 Milvus，本设计在以下方面更强：
- **SQL 接口**：标准的 SQL 语法，易于使用
- **查询语法**：更丰富的查询类型
- **字段类型**：支持更多类型
- **高亮显示**：内置高亮功能

下一步：开始实施阶段 1 的基础功能。
