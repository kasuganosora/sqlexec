# SQLExec 全文搜索系统技术原理

## 目录

1. [全文搜索架构](#1-全文搜索架构)
2. [分词器系统](#2-分词器系统)
3. [倒排索引](#3-倒排索引)
4. [BM25 评分算法](#4-bm25-评分算法)
5. [SQL 接口](#5-sql-接口)
6. [查询处理流程](#6-查询处理流程)
7. [性能优化](#7-性能优化)

---

## 1. 全文搜索架构

### 1.1 整体架构

SQLExec 的全文搜索系统采用分层架构设计，由六个核心子包组成：

```
pkg/fulltext/
├── engine.go              # 搜索引擎核心：索引、搜索、高亮入口
├── hybrid_search.go       # 混合搜索引擎（BM25 + 向量融合）
├── types.go               # 公共类型定义与配置
├── analyzer/              # 分词器系统
│   ├── tokenizer.go       # 分词器接口、标准/Ngram/英文分词器
│   └── jieba.go           # 基于 gojieba 的中文分词器
├── bm25/                  # BM25 评分系统
│   ├── scorer.go          # BM25 评分器与集合统计
│   └── sparse_vector.go   # 稀疏向量（文档/查询的向量化表示）
├── index/                 # 倒排索引
│   ├── inverted_index.go  # 倒排索引核心：文档存储、搜索、TopK
│   └── posting.go         # 倒排列表、跳表、迭代器
├── query/                 # 查询系统
│   ├── parser.go          # 查询解析器（布尔语法、短语、模糊等）
│   ├── query.go           # 查询类型：Term/Phrase/Boolean/Range/Fuzzy
│   └── advanced_query.go  # 高级查询：DisjunctionMax/Regex/Wildcard/Nested
├── schema/                # Schema 定义
│   └── field.go           # 字段类型、Schema 构建器
└── sql/                   # SQL 函数接口
    └── functions.go       # SQL 函数生成器（MATCH AGAINST、BM25 评分等）
```

### 1.2 核心组件关系

系统的核心数据流如下：

```
用户查询 (SQL / API)
       │
       v
  ┌─────────┐    分词     ┌───────────┐
  │  Engine  │ ──────────> │  Analyzer  │  (Standard / Jieba / Ngram / English)
  └─────────┘             └───────────┘
       │                        │
       │  查询向量              │ Token 列表
       v                        v
  ┌─────────────┐        ┌──────────────┐
  │ InvertedIndex│ <───── │ AddDocument  │  (建立倒排索引)
  └─────────────┘        └──────────────┘
       │                        │
       │  候选文档              │ 倒排列表 + BM25 预计算
       v                        v
  ┌─────────┐            ┌────────────┐
  │  Scorer  │ ─────────> │ SparseVector│  (文档向量 / 查询向量)
  └─────────┘            └────────────┘
       │
       v
  排序后的搜索结果 (DocID, Score, Document)
```

### 1.3 Engine 核心结构

`Engine` 是全文搜索的入口，封装了分词器、评分器和倒排索引：

```go
// Engine 全文搜索引擎
type Engine struct {
    config      *Config
    tokenizer   analyzer.Tokenizer      // 分词器（可切换）
    scorer      *bm25.Scorer            // BM25 评分器
    invertedIdx *index.InvertedIndex    // 倒排索引
    vocabulary  *Vocabulary             // 词汇表（term <-> ID 双向映射）
    mu          sync.RWMutex            // 读写锁
}
```

`Engine` 对外提供的主要方法：

| 方法 | 说明 |
|------|------|
| `IndexDocument(doc)` | 索引一个文档：分词 -> 建立倒排索引 -> 计算 BM25 向量 |
| `Search(query, topK)` | BM25 向量搜索，返回 TopK 结果 |
| `SearchBM25(query, topK)` | 使用 `TokenizeForSearch` 的搜索模式 |
| `SearchPhrase(phrase, slop, topK)` | 短语搜索（支持 slop 容差） |
| `SearchWithQuery(q, topK)` | 使用 `Query` 对象执行复杂查询 |
| `SearchWithHighlight(query, topK, pre, post)` | 带高亮标记的搜索 |
| `DeleteDocument(docID)` | 从索引中删除文档 |

### 1.4 配置体系

```go
// Config 全文搜索配置
type Config struct {
    BM25Params   BM25Params  // BM25 参数 (K1, B)
    StopWords    []string    // 停用词列表
    MinTokenLen  int         // 最小分词长度（默认 2）
    MaxTokenLen  int         // 最大分词长度（默认 100）
}

// 默认 BM25 参数
var DefaultBM25Params = BM25Params{
    K1: 1.2,   // 词频饱和参数
    B:  0.75,  // 长度归一化参数
}
```

---

## 2. 分词器系统

### 2.1 分词器接口

所有分词器实现统一的 `Tokenizer` 接口：

```go
// Tokenizer 分词器接口
type Tokenizer interface {
    Tokenize(text string) ([]Token, error)           // 标准分词（用于索引）
    TokenizeForSearch(text string) ([]Token, error)   // 搜索模式分词（可能更细粒度）
}

// Token 分词结果
type Token struct {
    Text     string  // 词文本（已归一化）
    Position int     // 在文档中的位置序号
    Start    int     // 起始字节偏移
    End      int     // 结束字节偏移
    Type     string  // 词性标注（如 "word", "keyword"）
}
```

### 2.2 分词器类型

系统通过 `TokenizerFactory` 工厂方法支持以下分词器：

| 分词器类型 | 常量 | 适用场景 |
|-----------|------|---------|
| Standard | `TokenizerTypeStandard` | 通用分词，按 Unicode 字符类别切分 |
| English | `TokenizerTypeEnglish` | 英文分词，带词干提取（Porter Stemmer 简化版） |
| Jieba | `TokenizerTypeJieba` | 中文分词，基于 gojieba |
| N-gram | `TokenizerTypeNgram` | N-gram 分词，适合模糊匹配与 CJK 文本 |
| Raw | `TokenizerTypeRaw` | 原始文本，不做分词 |

### 2.3 标准分词器 (StandardTokenizer)

标准分词器基于 Unicode 字符类别进行切分，支持字母、数字、CJK 字符：

```go
func (t *StandardTokenizer) isTokenChar(r rune) bool {
    // 字母、数字、汉字都是有效字符
    return unicode.IsLetter(r) || unicode.IsNumber(r) || (r >= 0x4E00 && r <= 0x9FFF)
}
```

处理流程：
1. 遍历 Unicode rune 序列，遇到非 token 字符时切分
2. 小写归一化 (`Lowercase`)
3. 停用词过滤 (`StopWords`)
4. 最小/最大长度过滤 (`MinTokenLen` / `MaxTokenLen`)

### 2.4 中文分词器 (JiebaTokenizer)

中文分词器基于 [gojieba](https://github.com/yanyiwu/gojieba) 库实现，这是结巴分词的 Go 语言绑定（通过 CGO 调用 C++ 实现的 cppjieba）。

```go
type JiebaTokenizer struct {
    *BaseTokenizer
    segmenter  *gojieba.Jieba   // gojieba 分词器实例
    useHMM     bool              // 是否启用 HMM 新词发现
    searchMode bool              // 是否使用搜索模式
    mu         sync.RWMutex
}
```

#### 分词模式

Jieba 分词器支持四种分词模式：

| 模式 | 方法 | 说明 |
|------|------|------|
| 精确模式 | `Tokenize()` / `segmenter.Cut()` | 最精确的切分，适合文本分析 |
| 搜索模式 | `TokenizeForSearch()` / `segmenter.CutForSearch()` | 在精确模式基础上对长词再切分，适合搜索 |
| 全模式 | `CutAll()` / `segmenter.CutAll()` | 把所有可能的分词结果都列出来 |
| 索引模式 | `CutForIndex()` | 精确分词 + 长词前缀生成（2字/3字前缀） |

示例：对"永和服装饰品有限公司"进行分词

- **精确模式**: `["永和", "服装", "饰品", "有限公司"]`
- **搜索模式**: `["永和", "服装", "饰品", "有限", "公司", "有限公司"]`
- **全模式**: `["永和", "服装", "饰品", "有限", "有限公司", "公司"]`

#### HMM 新词发现

Jieba 内置基于隐马尔可夫模型（HMM）的新词识别机制。当 `useHMM=true` 时，对于词典中没有的词，会通过 HMM 模型进行识别：

```go
// 启用 HMM
words := j.segmenter.Cut(text, true)   // useHMM=true

// 禁用 HMM
words := j.segmenter.Cut(text, false)  // useHMM=false
```

#### 动态词典管理

支持运行时动态添加/删除词汇：

```go
// 添加词
tokenizer.AddWord("深度学习")
tokenizer.AddWordEx("机器学习", 20000, "n")  // 带词频和词性

// 删除词
tokenizer.RemoveWord("临时词")

// 检查词是否在词典中
exists := tokenizer.HasWord("人工智能")
```

#### 词性标注与关键词提取

```go
// 词性标注
tokens, _ := tokenizer.Tag("我爱北京天安门")
// 返回: [{Text:"我" Type:"r"}, {Text:"爱" Type:"v"}, {Text:"北京" Type:"ns"}, ...]

// 关键词提取 (TF-IDF)
keywords, _ := tokenizer.Extract("全文搜索引擎是信息检索的核心组件", 5)
```

#### 停用词

系统内置中英文停用词列表：

```go
// 默认中文停用词
var DefaultChineseStopWords = []string{
    "的", "了", "和", "是", "在", "有", "我", "他", "她", "它",
    "们", "这", "那", "之", "与", "或", "及", "等", "对", "为",
}
```

可通过 `MergeStopWords()` 合并多个停用词列表。

### 2.5 N-gram 分词器

N-gram 分词器将文本切分为固定长度的字符片段，特别适合 CJK 语言和模糊匹配场景：

```go
type NgramTokenizer struct {
    *BaseTokenizer
    MinGram    int    // 最小 gram（默认 2）
    MaxGram    int    // 最大 gram（默认 3）
    PrefixOnly bool   // 仅前缀模式
}
```

对于中文文本"搜索引擎"，bigram (2-gram) 分词结果为：`["搜索", "索引", "引擎"]`。

### 2.6 英文分词器

英文分词器在标准分词基础上增加了词干提取（Porter Stemmer 简化版）：

```go
func (t *EnglishTokenizer) stem(word string) string {
    suffixes := []string{"ing", "edly", "edly", "ed", "ly", "s", "es", "ies"}
    for _, suffix := range suffixes {
        if strings.HasSuffix(word, suffix) && len(word) > len(suffix)+2 {
            return word[:len(word)-len(suffix)]
        }
    }
    return word
}
```

例如：`"running"` -> `"runn"`, `"quickly"` -> `"quick"`

---

## 3. 倒排索引

### 3.1 倒排索引结构

倒排索引是全文搜索的核心数据结构，将"词项"映射到"包含该词的文档列表"：

```go
type InvertedIndex struct {
    postings   map[int64]*PostingsList          // termID -> 倒排列表
    docStore   map[int64]*Document              // docID -> 文档存储
    docVectors map[int64]*bm25.SparseVector     // docID -> BM25 稀疏向量
    stats      *bm25.CollectionStats            // 集合统计信息
    scorer     *bm25.Scorer                     // BM25 评分器
    mu         sync.RWMutex                     // 读写锁
}
```

### 3.2 词项标识 (Term ID)

系统使用 FNV-1a 64 位哈希函数将词文本映射为整数 ID，避免字符串比较的开销：

```go
func hashString(s string) int64 {
    h := uint64(14695981039346656037)  // FNV offset basis
    for i := 0; i < len(s); i++ {
        h ^= uint64(s[i])
        h *= 1099511628211             // FNV prime
    }
    return int64(h)
}
```

该哈希函数在 `engine`、`index`、`query` 三个包中保持一致。

### 3.3 倒排列表 (PostingsList)

每个词项对应一个 `PostingsList`，包含该词出现的所有文档信息：

```go
type PostingsList struct {
    TermID   int64
    Postings []Posting     // 按 DocID 排序的倒排项列表
    DocCount int64         // 文档数量
    MaxScore float64       // 最大 BM25 分数（MAXSCORE 优化用）
    SkipList *SkipList     // 跳表（快速跳转）
}

type Posting struct {
    DocID     int64       // 文档 ID
    Frequency int         // 词频 (TF)
    Positions []int       // 位置信息（支持短语查询）
    BM25Score float64     // 预计算的 BM25 分数
}
```

倒排列表的关键特性：

1. **按 DocID 有序**: 使用二分查找插入和查找，`AddPosting` 通过 `sort.Search` 维护有序性
2. **预计算 BM25**: 索引时即计算每个 `(term, doc)` 对的 BM25 分数，搜索时直接使用
3. **位置信息**: 记录词在文档中的位置序号，支持短语查询和邻近查询
4. **跳表加速**: 每 64 个文档添加一个跳点，加速长列表的遍历

### 3.4 跳表 (SkipList)

跳表是对长倒排列表的加速结构：

```go
type SkipList struct {
    Points []SkipPoint
}

type SkipPoint struct {
    DocID    int64     // 跳点对应的文档 ID
    Index    int       // 在 Postings 数组中的位置
    MaxScore float64   // 到该跳点为止的最大分数
}
```

每 64 个倒排项自动添加一个跳点。查找时先通过跳表二分查找定位最近的跳点，再线性扫描：

```go
func (sl *SkipList) FindNearestSkipPoint(docID int64) int {
    idx := sort.Search(len(sl.Points), func(i int) bool {
        return sl.Points[i].DocID >= docID
    })
    if idx > 0 {
        return sl.Points[idx-1].Index
    }
    return -1
}
```

### 3.5 文档索引流程

文档索引是构建倒排索引的核心过程：

```
IndexDocument(doc)
    │
    v
1. 分词: tokenizer.Tokenize(doc.Content) -> []Token
    │
    v
2. 统计词频和位置:
   termFreqs[termID]++
   termPositions[termID] = append(..., token.Position)
    │
    v
3. 计算文档 BM25 向量（快照模式，避免频繁加锁）:
   vector = scorer.ComputeDocumentVectorWithStats(termFreqs, docLen, totalDocs, avgDocLen, stats)
    │
    v
4. 更新倒排列表:
   对每个 termID:
     - 创建或获取 PostingsList
     - 添加 Posting{DocID, Frequency, Positions, BM25Score}
     - 更新 stats.DocFreqs[termID]++
    │
    v
5. 更新集合统计:
   stats.AddDocStats(docLength)
```

### 3.6 记录类型 (RecordType)

系统定义了四种记录粒度，在存储空间和查询能力之间取得平衡：

| RecordType | 存储内容 | 用途 |
|-----------|---------|------|
| `basic` | 仅文档 ID | 最小存储，仅支持布尔查询 |
| `freq` | 文档 ID + 词频 | 支持 TF-IDF 评分 |
| `position` | 文档 ID + 词频 + 位置 | 支持短语查询和邻近查询 |
| `score` | 文档 ID + BM25 分数 | 预计算分数，搜索最快 |

当前实现中，所有文档默认使用 `position` 级别存储，同时预计算 BM25 分数。

---

## 4. BM25 评分算法

### 4.1 BM25 公式

BM25 (Best Matching 25) 是信息检索领域最经典的相关性评分算法。系统实现了完整的 BM25 公式：

对于查询 Q 中的每个词项 t，其对文档 D 的评分为：

```
Score(Q, D) = SUM_t [ IDF(t) * TF(t, D) ]
```

其中：

**IDF（逆文档频率）**：衡量词项的稀有程度

```
IDF(t) = log((N - df(t) + 0.5) / (df(t) + 0.5))
```

- `N`: 文档集合总数
- `df(t)`: 包含词项 t 的文档数

**TF（词频分数）**：衡量词项在文档中的重要性（带饱和效应）

```
TF(t, D) = (f(t,D) * (k1 + 1)) / (f(t,D) + k1 * (1 - b + b * |D| / avgdl))
```

- `f(t, D)`: 词项 t 在文档 D 中的出现次数
- `|D|`: 文档 D 的长度（词数）
- `avgdl`: 集合中文档的平均长度
- `k1`: 词频饱和参数（默认 1.2），控制词频增长的速度
- `b`: 长度归一化参数（默认 0.75），控制文档长度的影响

### 4.2 评分器实现

```go
type Scorer struct {
    params Params
    stats  *CollectionStats
}

// 计算单个词的 BM25 分数
func (s *Scorer) Score(termID int64, freq int, docLength int) float64 {
    idf := s.CalculateIDF(termID)
    tf := s.CalculateTF(freq, docLength)
    return idf * tf
}
```

IDF 计算：

```go
func (s *Scorer) CalculateIDF(termID int64) float64 {
    df := s.stats.GetDocFreq(termID)
    N := float64(s.stats.GetTotalDocs())
    if df == 0 || N == 0 {
        return 0
    }
    return math.Log((N - float64(df) + 0.5) / (float64(df) + 0.5))
}
```

TF 计算：

```go
func (s *Scorer) CalculateTF(freq int, docLength int) float64 {
    k1, b := s.params.K1, s.params.B
    avgdl := s.stats.GetAvgDocLength()
    if avgdl == 0 {
        avgdl = 1
    }
    numerator := float64(freq) * (k1 + 1)
    denominator := float64(freq) + k1*(1-b+b*float64(docLength)/avgdl)
    return numerator / denominator
}
```

### 4.3 集合统计信息

`CollectionStats` 维护了全局统计信息，所有操作都是线程安全的：

```go
type CollectionStats struct {
    TotalDocs      int64               // 文档总数
    TotalDocLength int64               // 所有文档长度之和
    AvgDocLength   float64             // 平均文档长度（自动更新）
    DocFreqs       map[int64]int64     // termID -> 包含该词的文档数
    mu             sync.RWMutex
}
```

每次添加/删除文档时自动更新 `AvgDocLength`：

```go
func (s *CollectionStats) AddDocStats(docLength int64) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.TotalDocs++
    s.TotalDocLength += docLength
    s.UpdateAvgDocLength()  // AvgDocLength = TotalDocLength / TotalDocs
}
```

### 4.4 稀疏向量表示

每个文档被表示为一个 BM25 稀疏向量，其中每个维度对应一个词项，权重为该词项的 BM25 分数：

```go
type SparseVector struct {
    Terms map[int64]float64   // termID -> BM25 权重
    Norm  float64             // 向量 L2 范数（缓存）
}
```

支持的操作：

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| `Set(termID, weight)` | O(1) | 设置词项权重 |
| `Get(termID)` | O(1) | 获取词项权重 |
| `DotProduct(other)` | O(min(n,m)) | 点积（遍历较小向量） |
| `CosineSimilarity(other)` | O(min(n,m)) | 余弦相似度 |
| `Normalize()` | O(n) | L2 归一化 |

搜索时，查询也被向量化，最终通过向量点积计算相关性分数：

```go
// 构建查询向量
queryVector := bm25.NewSparseVector()
for _, token := range tokens {
    termID := hashString(token.Text)
    if weight, exists := queryVector.Get(termID); exists {
        queryVector.Set(termID, weight+1.0)
    } else {
        queryVector.Set(termID, 1.0)
    }
}
queryVector.Normalize()

// 搜索：查询向量 x 文档向量 = 相关性分数
results := invertedIdx.SearchTopK(queryVector, topK)
```

### 4.5 参数调优指南

| 参数 | 范围 | 默认值 | 调大效果 | 调小效果 |
|------|------|--------|---------|---------|
| `k1` | 1.2-2.0 | 1.2 | 更重视高频词 | 词频饱和更快 |
| `b` | 0-1 | 0.75 | 更多惩罚长文档 | 减少长度影响 |

当 `b=0` 时，文档长度不影响评分；当 `b=1` 时，完全按比例归一化。

---

## 5. SQL 接口

### 5.1 CREATE FULLTEXT INDEX

通过查询优化器的 `FullTextIndexSupport` 组件，系统支持生成全文索引的 DDL：

```sql
-- 单列全文索引
CREATE FULLTEXT INDEX ft_articles_content ON articles(content)

-- 多列复合全文索引
CREATE FULLTEXT INDEX ft_articles_title_content ON articles(title, content)
```

对应的 Go 生成方法：

```go
fts := NewFullTextIndexSupport()
ddl := fts.GetFullTextIndexDDL("articles", []string{"title", "content"}, "")
// 输出: CREATE FULLTEXT INDEX ft_articles_title_content ON articles(title, content)
```

支持的列类型：`CHAR`, `VARCHAR`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT`, `TINYTEXT`, `CHARACTER`, `NCHAR`, `NVARCHAR`, `NTEXT`

### 5.2 MATCH AGAINST 查询语法

系统识别标准的 `MATCH ... AGAINST` 语法：

```sql
-- 基本全文搜索
SELECT * FROM articles
WHERE MATCH(content) AGAINST('数据库 索引 优化')

-- 带 BM25 评分排序
SELECT *, bm25_score(content, '搜索引擎') AS score
FROM articles
WHERE MATCH(content) AGAINST('搜索引擎')
ORDER BY score DESC
LIMIT 10

-- 多列搜索
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('全文搜索')
```

### 5.3 全文查询函数 (sql 包)

`sql` 包提供了一套 SQL 函数生成器，用于程序化构建全文查询：

#### 字段配置

```go
// 配置全文索引字段
field := sql.NewFulltextField("content").
    WithTokenizer("jieba").
    WithRecord("position").
    WithBoost(2.0).
    WithFast(false)

// 生成: fulltext_field('content', fast=>false, tokenizer=>'jieba', record=>'position', boost=>2.00)
field.ToSQL()
```

#### 分词器配置

```go
// Jieba 分词器
tokenizer := sql.NewFulltextTokenizer("jieba").
    WithHMM(true).
    WithSearchMode(true)
// 生成: fulltext_tokenizer('jieba', hmm=>true, search=>true)

// N-gram 分词器
tokenizer := sql.NewFulltextTokenizer("ngram").
    WithNgram(2, 3, false)
// 生成: fulltext_tokenizer('ngram', min_gram=>2, max_gram=>3, prefix_only=>false)
```

#### 查询函数

```go
// 词项查询
sql.FulltextTerm("content", "数据库", 2.0)
// 生成: fulltext_term('content', '数据库', boost=>2.00)

// 短语查询
sql.FulltextPhrase("content", []string{"全文", "搜索"}, 0, 1.0)
// 生成: fulltext_phrase('content', ARRAY['全文', '搜索'], slop=>0, boost=>1.00)

// 模糊查询
sql.FulltextFuzzy("content", "databas", 2, 1.0)
// 生成: fulltext_fuzzy('content', 'databas', distance=>2, boost=>1.00)

// 正则查询
sql.FulltextRegex("email", "^user@.*\\.com$", 1.0)

// 范围查询
sql.FulltextRange("date", "2024-01-01", "2024-12-31", true, true, 1.0)

// 前缀查询
sql.FulltextPrefix("title", "数据", 1.0)

// 通配符查询
sql.FulltextWildcard("title", "数据*", 1.0)
```

#### 布尔查询

```go
clause := sql.BooleanClause{
    Must:    []string{sql.FulltextTerm("content", "数据库", 1.0)},
    Should:  []string{sql.FulltextTerm("content", "索引", 1.0)},
    MustNot: []string{sql.FulltextTerm("content", "删除", 1.0)},
}
sql.FulltextBooleanSQL(clause, 1, 1.0)
```

#### 评分与排名函数

```go
// BM25 评分
sql.BM25Score("content", "关键词")
// 生成: bm25_score(content, '关键词')

// BM25 排名
sql.BM25Rank("content", "关键词")
// 生成: bm25_rank(content, '关键词')

// 混合评分
sql.HybridScore("ft_score", "vec_score", 0.7, 0.3)
// 生成: hybrid_score(ft_score, vec_score, ft_weight=>0.70, vec_weight=>0.30)

// RRF 融合排名
sql.RRFRank([]string{"bm25_rank", "vector_rank"}, 60)
// 生成: rrf_rank(bm25_rank, vector_rank, k=>60)
```

#### 高亮函数

```go
// 单片段高亮
sql.Highlight("content", "搜索", "<mark>", "</mark>")
// 生成: highlight(content, '搜索', '<mark>', '</mark>')

// 多片段高亮
sql.HighlightMulti("content", "搜索", "<mark>", "</mark>", 3, 150)
// 生成: highlight_multi(content, '搜索', '<mark>', '</mark>', 3, 150)
```

#### SQL 生成器

```go
gen := sql.NewSQLGenerator("articles", "content", "全文搜索").
    Select("id", "title", "content").
    OrderByScore(true).
    Paginate(10, 0)

gen.BuildWithScore()
// 生成:
// SELECT id, title, content, bm25_score(content, '全文搜索') AS bm25_score
// FROM articles
// WHERE content @@ fulltext_config('全文搜索')
// ORDER BY bm25_score DESC
// LIMIT 10
```

---

## 6. 查询处理流程

### 6.1 查询类型体系

所有查询类型实现统一的 `Query` 接口：

```go
type Query interface {
    Execute(idx *index.InvertedIndex) []SearchResult
    SetBoost(boost float64)
    GetBoost() float64
}
```

查询类型层次：

```
Query (接口)
├── TermQuery         词项查询：匹配单个词项
├── PhraseQuery       短语查询：匹配有序词序列（支持 slop）
├── BooleanQuery      布尔查询：Must / Should / MustNot 组合
├── MatchAllQuery     匹配所有文档
├── MatchNoneQuery    不匹配任何文档
├── RangeQuery        范围查询：数值/日期/字符串范围
├── FuzzyQuery        模糊查询：基于编辑距离（Levenshtein）
├── RegexQuery        正则表达式查询
├── PrefixQuery       前缀查询
├── WildcardQuery     通配符查询（* 和 ?）
├── TermSetQuery      词集查询：匹配任意一个词
├── DisjunctionMaxQuery  析取最大查询：取最高分 + TieBreaker * 次高分
├── ConstScoreQuery   常数分数查询：固定分数
├── FunctionScoreQuery 函数评分查询：自定义评分函数
├── ExistsQuery       字段存在查询
├── NestedQuery       嵌套查询（JSON 字段）
└── PhrasePrefixQuery 短语前缀查询（自动补全）
```

### 6.2 查询解析器

系统提供两级解析器：

**完整解析器 (`Parser`)**: 支持丰富的查询语法

```
支持语法:
- 简单词:      keyword
- 短语:        "exact phrase"
- 字段限定:    title:keyword
- 布尔操作:    +must -must_not  OR  AND  NOT
- 模糊查询:    keyword~2
- 权重提升:    keyword^2.0
- 分组:        (word1 OR word2)
```

**简化解析器 (`SimpleQueryParser`)**: 适合用户输入

```
支持语法:
- 空格分隔:    hello world (OR 逻辑)
- 双引号短语:  "exact match"
- +必须包含:   +required
- -必须排除:   -excluded
```

### 6.3 BM25 向量搜索流程

标准搜索流程（`Engine.Search`）：

```
1. 查询分词
   queryStr -> tokenizer.Tokenize() -> []Token

2. 构建查询向量
   for each token:
       termID = hashString(token.Text)
       queryVector[termID] += 1.0
   queryVector.Normalize()   // L2 归一化

3. 执行 TopK 搜索（DAAT + 二分查找）
   invertedIdx.SearchTopK(queryVector, topK)

4. 返回排序后的结果
   []SearchResult{DocID, Score, Doc}
```

### 6.4 布尔查询执行

`BooleanQuery` 支持 Must（AND）、Should（OR）、MustNot（NOT）三种子句：

```go
// 执行流程:
// 1. 执行所有 Must 子查询 -> 候选文档必须同时出现在所有结果中
// 2. 执行所有 Should 子查询 -> 候选文档至少出现在 MinShouldMatch 个结果中
// 3. 执行所有 MustNot 子查询 -> 排除这些文档
// 4. 最终分数 = sum(子查询分数) * boost
```

**快速路径优化**: 当 `BooleanQuery` 仅包含 Should 子句且全部为 `TermQuery` 时，自动合并为单次向量搜索，避免 N 次全索引扫描：

```go
func (q *BooleanQuery) tryMergeTermQueries(idx *index.InvertedIndex) []SearchResult {
    queryVector := bm25.NewSparseVector()
    for _, sq := range q.Should {
        tq, ok := sq.(*TermQuery)
        if !ok {
            return nil  // 回退到通用路径
        }
        termID := hashString(tq.Term)
        queryVector.Set(termID, tq.GetBoost())
    }
    queryVector.Normalize()
    return idx.Search(queryVector)
}
```

### 6.5 短语查询

短语查询利用倒排列表中的位置信息，验证词项是否按指定顺序出现：

```
SearchPhrase("machine learning", slop=0)

1. 获取 "machine" 的倒排列表（作为起始词）
2. 对每个候选文档:
   a. 获取所有词项的位置列表
      "machine": [2, 15, 30]
      "learning": [3, 8, 31]
   b. 检查位置匹配: 对 "machine" 的每个位置 p1,
      检查 "learning" 是否有位置 p2 满足 |p2 - (p1+1)| <= slop
      -> p1=2, p2=3: |3 - 3| = 0 <= 0 => 匹配!
3. 返回匹配的文档
```

当 `slop > 0` 时，允许词项之间有一定的位置偏差，实现邻近搜索。

### 6.6 模糊查询

模糊查询基于 Levenshtein 编辑距离实现，采用空间优化的双行滚动算法：

```go
// O(min(m,n)) 空间的 Levenshtein 距离计算
func levenshteinDistance(s1, s2 string) int {
    // 确保 s1 是较短字符串
    if len(s1) > len(s2) {
        s1, s2 = s2, s1
    }
    // 双行滚动: prev[] 和 curr[]
    prev := make([]int, len(s1)+1)
    curr := make([]int, len(s1)+1)
    // ... DP 计算
    return prev[len(s1)]
}
```

---

## 7. 性能优化

### 7.1 TopK 搜索优化：DAAT + 最小堆 + 二分查找

`SearchTopK` 是搜索性能的关键路径。系统采用 DAAT（Document-At-A-Time）策略配合最小堆，实现零额外 map 分配：

```go
func (idx *InvertedIndex) SearchTopK(queryVector *bm25.SparseVector, topK int) []SearchResult {
    // 1. 按倒排列表长度升序排序（稀有词优先）
    sort.Slice(terms, func(i, j int) bool {
        return len(terms[i].postings.Postings) < len(terms[j].postings.Postings)
    })

    heap := newMinHeap(topK)

    // Phase 1: 遍历最稀有词的倒排列表，对其他词使用二分查找探测
    for _, posting := range terms[0].postings.Postings {
        docID := posting.DocID
        score := terms[0].queryWeight * posting.BM25Score
        for i := 1; i < len(terms); i++ {
            if p := terms[i].postings.FindPosting(docID); p != nil {
                score += terms[i].queryWeight * p.BM25Score
            }
        }
        heap.tryAdd(docID, score)
    }

    // Phase 2: 处理不在最稀有词中但出现在其他词中的文档
    // (仅当 Phase 1 未填满堆时执行)
    // ...
}
```

核心优化点：

- **稀有词优先**: 倒排列表最短的词先遍历，其他词用二分查找探测，大幅减少遍历量
- **最小堆维护 TopK**: O(N log K) 复杂度，堆顶为当前最小分数，只有超过堆顶的文档才入堆
- **零 map 分配**: Phase 1 不需要 map，直接遍历 + 探测
- **二分查找**: `FindPosting` 使用 `sort.Search` 在有序倒排列表上进行 O(log n) 查找

最小堆实现：

```go
type minHeap struct {
    items   []heapItem
    maxSize int
}

func (h *minHeap) tryAdd(docID int64, score float64) {
    if len(h.items) < h.maxSize {
        h.items = append(h.items, heapItem{docID: docID, score: score})
        h.up(len(h.items) - 1)
    } else if score > h.items[0].score {
        h.items[0] = heapItem{docID: docID, score: score}
        h.down(0)
    }
}
```

### 7.2 BM25 预计算

索引时一次性计算每个 `(term, doc)` 的 BM25 分数并存储在 `Posting.BM25Score` 中，搜索时直接使用，避免重复计算：

```go
// 索引时: 批量计算文档向量（快照模式）
vector := idx.scorer.ComputeDocumentVectorWithStats(
    termFreqs, len(tokens),
    totalDocs, avgDocLength,  // 快照值，避免频繁加锁
    idx.stats,
)
```

### 7.3 BooleanQuery 快速路径

当布尔查询仅包含 Should + TermQuery 子句时，系统自动合并为单次向量搜索，避免 N 次全索引扫描：

```
传统方式: 3 个 TermQuery -> 3 次全索引扫描 -> 合并结果
快速路径: 3 个 TermQuery -> 合并为 1 个查询向量 -> 1 次向量搜索
```

### 7.4 统计信息快照

`ComputeDocumentVectorWithStats` 方法在计算文档向量前读取一次全局统计信息快照（`totalDocs`, `avgDocLength`），避免对每个词项都加锁读取：

```go
func (s *Scorer) ComputeDocumentVectorWithStats(
    termFreqs map[int64]int,
    docLength int,
    totalDocs int64,       // 预获取的快照
    avgDocLength float64,  // 预获取的快照
    stats *CollectionStats,
) *SparseVector {
    // 使用快照值计算，仅 df 需要逐词查询
    for termID, freq := range termFreqs {
        df := float64(stats.GetDocFreq(termID))
        // ... 计算 IDF, TF, score
    }
}
```

### 7.5 稀疏向量点积优化

点积计算时遍历较小的向量，在较大向量中做 O(1) 查找：

```go
func (v *SparseVector) DotProduct(other *SparseVector) float64 {
    // 遍历较小的向量以提高效率
    if len(v.Terms) > len(other.Terms) {
        v, other = other, v
    }
    var result float64
    for termID, weight := range v.Terms {
        if otherWeight, ok := other.Terms[termID]; ok {
            result += weight * otherWeight
        }
    }
    return result
}
```

### 7.6 FNV-1a 哈希

使用 FNV-1a 64 位哈希替代字符串比较和 map 查找，在词项标识上提供 O(1) 的等值比较：

```go
func hashString(s string) int64 {
    h := uint64(14695981039346656037)
    for i := 0; i < len(s); i++ {
        h ^= uint64(s[i])
        h *= 1099511628211
    }
    return int64(h)
}
```

### 7.7 并发安全

系统全面使用 `sync.RWMutex` 读写锁，搜索操作获取读锁（可并发），索引/删除操作获取写锁（互斥）：

```go
// 搜索: 读锁 (多个搜索可并发)
func (e *Engine) Search(...) {
    e.mu.RLock()
    defer e.mu.RUnlock()
    // ...
}

// 索引: 写锁 (与其他操作互斥)
func (e *Engine) IndexDocument(...) {
    e.mu.Lock()
    defer e.mu.Unlock()
    // ...
}
```

### 7.8 混合搜索

`HybridEngine` 支持 BM25 全文搜索与向量搜索的融合，提供两种融合策略：

**RRF (Reciprocal Rank Fusion)**:

```
RRF_score(d) = SUM_r [ 1 / (k + rank_r(d)) ]
```

其中 `k` 是常数（默认 60），`rank_r(d)` 是文档 d 在第 r 个排名列表中的位置。

**加权融合 (Weighted Fusion)**:

```
Hybrid_score(d) = ft_weight * norm(ft_score) + vec_weight * norm(vec_score)
```

分数先归一化到 [0, 1] 范围再加权求和。

### 7.9 查询优化器集成

全文索引与查询优化器通过 `FullTextIndexSupport` 组件集成：

1. **表达式识别**: 自动识别 `MATCH ... AGAINST`、`FULLTEXT()` 函数调用、带前导通配符的 `LIKE '%term%'`
2. **索引候选提取**: 从全文查询表达式中提取列，生成 `IndexCandidate`（优先级 4，高于普通索引）
3. **收益估算**: 根据行数、搜索词长度、匹配类型计算全文索引的搜索收益
4. **DDL 生成**: 自动生成 `CREATE FULLTEXT INDEX` 建议

```go
fts := NewFullTextIndexSupport()

// 识别全文查询
fts.IsFullTextExpression("MATCH(content) AGAINST('search term')")  // true
fts.IsFullTextExpression("content LIKE '%search%'")                // true

// 提取索引候选
candidates := fts.ExtractFullTextIndexCandidates("articles", expr, columnTypes)

// 计算收益
benefit := fts.CalculateFullTextSearchBenefit(100000, 6, false)
```
