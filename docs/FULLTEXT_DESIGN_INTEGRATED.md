# 高性能全文搜索系统设计（集成版）

## 设计理念

本设计结合两个参考文档的精华，为 `pkg` 项目打造**灵魂级**全文搜索功能：

1. **高性能内核**：采用 BM25 + 稀疏向量 + 倒排索引的核心架构
2. **中文优化**：Jieba/CJK 分词器 + 停用词过滤
3. **查询优化**：DAAT_MAXSCORE + WAND 算法实现毫秒级响应
4. **无缝集成**：与现有 `pkg/resource/memory` 索引体系完美融合
5. **功能丰富**：短语搜索、模糊搜索、高亮、混合搜索（全文+向量）

---

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FullTextEngine (全文引擎)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Analyzer   │  │    BM25      │  │    Index     │  │    Query     │   │
│  │   (分词器)    │──▶│  (评分函数)   │──▶│  (倒排索引)   │──▶│  (查询器)     │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   │
├─────────────────────────────────────────────────────────────────────────────┤
│                         IndexManager (索引管理器)                            │
│                    兼容 B-Tree / Hash / Vector / FullText                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 核心组件设计

### 1. 分词器体系 (pkg/fulltext/analyzer)

```go
// Tokenizer 分词器接口
type Tokenizer interface {
    Tokenize(text string) ([]Token, error)
    TokenizeForSearch(text string) ([]Token, error)
}

// Token 分词结果
type Token struct {
    Text      string // 词文本
    Position  int    // 位置
    Start     int    // 起始偏移
    End       int    // 结束偏移
    Type      string // 词性
}

// 分词器类型
const (
    TokenizerTypeJieba     = "jieba"      // 中文分词（推荐）
    TokenizerTypeNgram     = "ngram"      // N-gram 分词
    TokenizerTypeLindera   = "lindera"    // CJK 分词
    TokenizerTypeEnglish   = "english"    // 英文词干提取
    TokenizerTypeStandard  = "standard"   // 标准分词
    TokenizerTypeRaw       = "raw"        // 不分词
)
```

**分词器实现：**

```go
// JiebaTokenizer Jieba中文分词器
type JiebaTokenizer struct {
    segmenter     *gojieba.Segmenter
    stopWords     map[string]bool
    searchMode    bool  // 搜索模式（更细粒度）
    useHMM        bool  // 使用HMM新词发现
}

func (t *JiebaTokenizer) Tokenize(text string) ([]Token, error) {
    // 使用Jieba精确模式分词
    words := t.segmenter.Cut(text, t.useHMM)
    return t.filterAndConvert(words)
}

func (t *JiebaTokenizer) TokenizeForSearch(text string) ([]Token, error) {
    // 使用Jieba搜索引擎模式（更细粒度）
    words := t.segmenter.CutForSearch(text, t.useHMM)
    return t.filterAndConvert(words)
}

// NgramTokenizer N-gram分词器（适合中英文混合）
type NgramTokenizer struct {
    minGram    int
    maxGram    int
    prefixOnly bool
}

func (t *NgramTokenizer) Tokenize(text string) ([]Token, error) {
    runes := []rune(text)
    var tokens []Token
    
    for i := 0; i < len(runes); i++ {
        for j := t.minGram; j <= t.maxGram && i+j <= len(runes); j++ {
            token := string(runes[i : i+j])
            tokens = append(tokens, Token{
                Text:     token,
                Position: i,
                Start:    i,
                End:      i + j,
            })
        }
    }
    return tokens, nil
}
```

### 2. BM25 评分系统 (pkg/fulltext/bm25)

```go
// BM25Params BM25参数
type BM25Params struct {
    K1 float64 // 词频饱和参数 (1.2-2.0)
    B  float64 // 长度归一化参数 (0-1)
}

var DefaultBM25Params = BM25Params{
    K1: 1.2,
    B:  0.75,
}

// BM25Scorer BM25评分器
type BM25Scorer struct {
    params      BM25Params
    stats       *CollectionStats
    vocabulary  *Vocabulary
}

// CollectionStats 集合统计信息
type CollectionStats struct {
    TotalDocs      int64
    AvgDocLength   float64
    DocCountByTerm map[int64]int64 // termID -> docCount (DF)
}

// SparseVector 稀疏向量 (termID -> BM25权重)
type SparseVector struct {
    Terms map[int64]float64 // termID -> weight
    Norm  float64           // 向量范数（用于余弦相似度）
}

// Score 计算文档与查询的相关性分数
func (s *BM25Scorer) Score(doc *Document, query *SparseVector) float64 {
    var score float64
    
    for termID, queryWeight := range query.Terms {
        if docWeight, exists := doc.Vector.Terms[termID]; exists {
            score += queryWeight * docWeight
        }
    }
    
    return score
}

// ComputeDocumentVector 计算文档的BM25稀疏向量
func (s *BM25Scorer) ComputeDocumentVector(tokens []Token) *SparseVector {
    // 统计词频
    termFreq := make(map[int64]int)
    docLength := len(tokens)
    
    for _, token := range tokens {
        termID := s.vocabulary.GetOrCreateID(token.Text)
        termFreq[termID]++
    }
    
    // 计算BM25权重
    vector := &SparseVector{Terms: make(map[int64]float64)}
    for termID, freq := range termFreq {
        idf := s.calculateIDF(termID)
        tf := s.calculateTF(freq, docLength)
        vector.Terms[termID] = idf * tf
    }
    
    return vector
}

// calculateIDF 计算逆文档频率
func (s *BM25Scorer) calculateIDF(termID int64) float64 {
    df := s.stats.GetDocFreq(termID)
    N := float64(s.stats.TotalDocs)
    
    // IDF = log((N - df + 0.5) / (df + 0.5))
    return math.Log((N - float64(df) + 0.5) / (float64(df) + 0.5))
}

// calculateTF 计算词频分数
func (s *BM25Scorer) calculateTF(freq, docLength int) float64 {
    k1, b := s.params.K1, s.params.B
    avgdl := s.stats.AvgDocLength
    
    // TF = (f * (k1 + 1)) / (f + k1 * (1 - b + b * |D| / avgdl))
    numerator := float64(freq * (k1 + 1))
    denominator := float64(freq) + k1*(1-b+b*float64(docLength)/avgdl)
    
    return numerator / denominator
}
```

### 3. 倒排索引 (pkg/fulltext/index)

```go
// Posting 倒排列表项
type Posting struct {
    DocID      int64
    Frequency  int       // 词频
    Positions  []int     // 位置信息（用于短语查询）
    BM25Score  float64   // 预计算的BM25分数
}

// PostingsList 倒排链表
type PostingsList struct {
    TermID      int64
    Postings    []Posting
    DocCount    int64
    MaxScore    float64 // 最大分数（用于MAXSCORE优化）
}

// InvertedIndex 内存倒排索引
type InvertedIndex struct {
    postings    map[int64]*PostingsList  // termID -> PostingsList
    docStore    map[int64]*Document      // docID -> Document
    stats       *CollectionStats
    mu          sync.RWMutex
}

// Document 文档
type Document struct {
    ID        int64
    Content   string
    Vector    *SparseVector
    Length    int
    Fields    map[string]interface{} // 其他字段
}

// AddDocument 添加文档到索引
func (idx *InvertedIndex) AddDocument(doc *Document) error {
    idx.mu.Lock()
    defer idx.mu.Unlock()
    
    // 存储文档
    idx.docStore[doc.ID] = doc
    
    // 更新倒排索引
    for termID, weight := range doc.Vector.Terms {
        postingsList, exists := idx.postings[termID]
        if !exists {
            postingsList = &PostingsList{TermID: termID}
            idx.postings[termID] = postingsList
        }
        
        posting := Posting{
            DocID:     doc.ID,
            Frequency: doc.Vector.Terms[termID], // 实际存储频率
            BM25Score: weight,
        }
        
        postingsList.Postings = append(postingsList.Postings, posting)
        postingsList.DocCount++
        
        // 更新最大分数
        if weight > postingsList.MaxScore {
            postingsList.MaxScore = weight
        }
    }
    
    // 更新统计信息
    idx.stats.TotalDocs++
    idx.updateAvgDocLength()
    
    return nil
}

// Search 基础搜索（简单合并）
func (idx *InvertedIndex) Search(queryVector *SparseVector) []SearchResult {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    scores := make(map[int64]float64)
    
    for termID, queryWeight := range queryVector.Terms {
        if postingsList, exists := idx.postings[termID]; exists {
            for _, posting := range postingsList.Postings {
                scores[posting.DocID] += queryWeight * posting.BM25Score
            }
        }
    }
    
    // 转换为结果列表
    results := make([]SearchResult, 0, len(scores))
    for docID, score := range scores {
        results = append(results, SearchResult{
            DocID: docID,
            Score: score,
        })
    }
    
    // 按分数降序排序
    sort.Slice(results, func(i, j int) bool {
        return results[i].Score > results[j].Score
    })
    
    return results
}

// SearchDAATMaxScore DAAT MAXSCORE优化搜索
func (idx *InvertedIndex) SearchDAATMaxScore(
    queryVector *SparseVector, 
    topK int,
) []SearchResult {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    
    // 获取所有查询词的倒排列表，按maxScore降序排序
    var lists []*PostingsList
    for termID := range queryVector.Terms {
        if list, exists := idx.postings[termID]; exists {
            lists = append(lists, list)
        }
    }
    
    sort.Slice(lists, func(i, j int) bool {
        return lists[i].MaxScore > lists[j].MaxScore
    })
    
    // 使用最小堆维护Top-K
    heap := NewMinHeap(topK)
    
    // 计算阈值，跳过不可能进入Top-K的文档
    threshold := 0.0
    
    // 遍历首个倒排列表（分数最高的词）
    if len(lists) == 0 {
        return nil
    }
    
    firstList := lists[0]
    for _, posting := range firstList.Postings {
        docID := posting.DocID
        score := queryVector.Terms[firstList.TermID] * posting.BM25Score
        
        // 累加其他词的分数
        for i := 1; i < len(lists); i++ {
            if p := lists[i].findPosting(docID); p != nil {
                score += queryVector.Terms[lists[i].TermID] * p.BM25Score
            }
        }
        
        // 更新堆
        if heap.Len() < topK {
            heap.Push(docID, score)
        } else if score > heap.MinScore() {
            heap.Pop()
            heap.Push(docID, score)
        }
        
        threshold = heap.MinScore()
    }
    
    return heap.ToResults()
}
```

### 4. 查询系统 (pkg/fulltext/query)

```go
// Query 查询接口
type Query interface {
    Execute(index *InvertedIndex) []SearchResult
}

// TermQuery 词项查询
type TermQuery struct {
    Field string
    Term  string
    Boost float64
}

// PhraseQuery 短语查询
type PhraseQuery struct {
    Field   string
    Phrases []string
    Slop    int // 允许的词间距
    Boost   float64
}

// BooleanQuery 布尔查询
type BooleanQuery struct {
    Must    []Query
    Should  []Query
    MustNot []Query
    MinShouldMatch int
}

// FuzzyQuery 模糊查询
type FuzzyQuery struct {
    Field    string
    Term     string
    Distance int // 编辑距离
    Boost    float64
}

// RangeQuery 范围查询
type RangeQuery struct {
    Field      string
    Min        interface{}
    Max        interface{}
    IncludeMin bool
    IncludeMax bool
}

// QueryParser 查询解析器
type QueryParser struct {
    tokenizer Tokenizer
    analyzer  *Analyzer
}

// Parse 解析查询字符串
func (p *QueryParser) Parse(queryStr string) (Query, error) {
    // 支持语法：
    // - 简单词: "keyword"
    // - 短语: "\"exact phrase\""
    // - 字段限定: "title:keyword"
    // - 布尔: "+must -must_not should"
    // - 模糊: "keyword~2"
    // - 范围: "price:[10 TO 100]"
    // - 邻近: "\"word1 word2\"~5"
    
    // 实现查询语法解析
    // ...
}
```

### 5. 全文索引实现 (pkg/resource/memory/fulltext_index.go)

```go
package memory

import (
    "github.com/kasuganosora/sqlexec/pkg/fulltext"
)

// AdvancedFullTextIndex 高级全文索引
type AdvancedFullTextIndex struct {
    info        *IndexInfo
    engine      *fulltext.Engine
    tokenizer   fulltext.Tokenizer
    bm25Params  fulltext.BM25Params
    mu          sync.RWMutex
}

// NewAdvancedFullTextIndex 创建高级全文索引
func NewAdvancedFullTextIndex(
    tableName, columnName string,
    tokenizerType string,
    bm25Params fulltext.BM25Params,
) (*AdvancedFullTextIndex, error) {
    
    // 创建分词器
    tokenizer, err := fulltext.CreateTokenizer(tokenizerType, nil)
    if err != nil {
        return nil, err
    }
    
    // 创建全文引擎
    engine := fulltext.NewEngine(&fulltext.Config{
        BM25Params: bm25Params,
        Tokenizer:  tokenizer,
    })
    
    return &AdvancedFullTextIndex{
        info: &IndexInfo{
            Name:      fmt.Sprintf("idx_ft_%s_%s", tableName, columnName),
            TableName: tableName,
            Column:    columnName,
            Type:      IndexTypeFullText,
            Unique:    false,
        },
        engine:     engine,
        tokenizer:  tokenizer,
        bm25Params: bm25Params,
    }, nil
}

// Insert 插入文档
func (idx *AdvancedFullTextIndex) Insert(key interface{}, rowIDs []int64) error {
    text, ok := key.(string)
    if !ok {
        return fmt.Errorf("full-text index requires string key, got %T", key)
    }
    
    for _, rowID := range rowIDs {
        doc := &fulltext.Document{
            ID:      rowID,
            Content: text,
        }
        
        if err := idx.engine.AddDocument(doc); err != nil {
            return err
        }
    }
    
    return nil
}

// Search 搜索文档
func (idx *AdvancedFullTextIndex) Search(query string, topK int) ([]fulltext.SearchResult, error) {
    return idx.engine.Search(query, topK)
}

// SearchWithHighlight 带高亮的搜索
func (idx *AdvancedFullTextIndex) SearchWithHighlight(
    query string, 
    topK int,
    preTag, postTag string,
) ([]fulltext.SearchResultWithHighlight, error) {
    return idx.engine.SearchWithHighlight(query, topK, preTag, postTag)
}

// GetIndexInfo 获取索引信息
func (idx *AdvancedFullTextIndex) GetIndexInfo() *IndexInfo {
    return idx.info
}

// 实现标准 Index 接口的其他方法...
func (idx *AdvancedFullTextIndex) Delete(key interface{}) error {
    // 实现删除逻辑
    return nil
}

func (idx *AdvancedFullTextIndex) Find(key interface{}) ([]int64, bool) {
    query, ok := key.(string)
    if !ok {
        return nil, false
    }
    
    results, err := idx.Search(query, 1000)
    if err != nil || len(results) == 0 {
        return nil, false
    }
    
    rowIDs := make([]int64, len(results))
    for i, r := range results {
        rowIDs[i] = r.DocID
    }
    
    return rowIDs, true
}

func (idx *AdvancedFullTextIndex) FindRange(min, max interface{}) ([]int64, error) {
    return nil, fmt.Errorf("full-text index does not support range queries")
}
```

### 6. 混合搜索（全文 + 向量）

```go
// HybridSearcher 混合搜索器
type HybridSearcher struct {
    fulltextIndex *AdvancedFullTextIndex
    vectorIndex   VectorIndex
    ftWeight      float64
    vecWeight     float64
    rrfK          int // RRF参数
}

// Search 执行混合搜索
func (hs *HybridSearcher) Search(
    textQuery string,
    vectorQuery []float32,
    topK int,
) ([]HybridResult, error) {
    
    // 并行执行全文搜索和向量搜索
    var ftResults []fulltext.SearchResult
    var vecResults []VectorSearchResult
    var ftErr, vecErr error
    
    var wg sync.WaitGroup
    wg.Add(2)
    
    go func() {
        defer wg.Done()
        ftResults, ftErr = hs.fulltextIndex.Search(textQuery, topK*2)
    }()
    
    go func() {
        defer wg.Done()
        vecResults, vecErr = hs.vectorIndex.Search(vectorQuery, topK*2)
    }()
    
    wg.Wait()
    
    if ftErr != nil {
        return nil, ftErr
    }
    if vecErr != nil {
        return nil, vecErr
    }
    
    // 使用RRF融合结果
    return hs.fuseWithRRF(ftResults, vecResults, topK)
}

// fuseWithRRF 使用RRF算法融合结果
func (hs *HybridSearcher) fuseWithRRF(
    ftResults []fulltext.SearchResult,
    vecResults []VectorSearchResult,
    topK int,
) ([]HybridResult, error) {
    
    scores := make(map[int64]float64)
    k := float64(hs.rrfK)
    
    // 融合全文搜索结果
    for rank, result := range ftResults {
        docID := result.DocID
        scores[docID] += 1.0 / (k + float64(rank+1))
    }
    
    // 融合向量搜索结果
    for rank, result := range vecResults {
        docID := result.DocID
        scores[docID] += 1.0 / (k + float64(rank+1))
    }
    
    // 排序
    results := make([]HybridResult, 0, len(scores))
    for docID, score := range scores {
        results = append(results, HybridResult{
            DocID:        docID,
            RRFScore:     score,
        })
    }
    
    sort.Slice(results, func(i, j int) bool {
        return results[i].RRFScore > results[j].RRFScore
    })
    
    if len(results) > topK {
        results = results[:topK]
    }
    
    return results, nil
}
```

---

## SQL 接口设计

### 1. 创建全文索引

```sql
-- 基础用法
CREATE FULLTEXT INDEX idx_ft_content ON articles(content);

-- 高级配置
CREATE FULLTEXT INDEX idx_ft_content ON articles(content)
WITH (
    tokenizer = 'jieba',
    tokenizer_options = '{"search_mode": true, "hmm": true}',
    bm25_k1 = 1.2,
    bm25_b = 0.75,
    stop_words = 'default'
);

-- 多字段索引
CREATE FULLTEXT INDEX idx_ft_multi ON articles(title, content, tags)
WITH (
    tokenizer = 'jieba',
    field_weights = '{"title": 3.0, "content": 1.0, "tags": 2.0}'
);
```

### 2. 全文搜索语法

```sql
-- 基础搜索
SELECT * FROM articles 
WHERE content @@ '全文搜索' 
ORDER BY bm25_score DESC 
LIMIT 10;

-- 带分数的搜索
SELECT *, bm25_score(content, '全文搜索') as score 
FROM articles 
WHERE content @@ '全文搜索'
ORDER BY score DESC;

-- 短语搜索
SELECT * FROM articles 
WHERE content @@ '"精确短语"' 
ORDER BY bm25_score DESC;

-- 邻近搜索（slop=2）
SELECT * FROM articles 
WHERE content @@ '"词A 词B"~2' 
ORDER BY bm25_score DESC;

-- 布尔搜索
SELECT * FROM articles 
WHERE content @@ '+必须包含 -必须排除 可选包含' 
ORDER BY bm25_score DESC;

-- 字段限定
SELECT * FROM articles 
WHERE content @@ 'title:关键词' 
ORDER BY bm25_score DESC;

-- 模糊搜索（编辑距离2）
SELECT * FROM articles 
WHERE content @@ '关键词~2' 
ORDER BY bm25_score DESC;

-- 带高亮的搜索
SELECT *, highlight(content, '关键词', '<mark>', '</mark>') as highlighted
FROM articles 
WHERE content @@ '关键词'
ORDER BY bm25_score DESC;
```

### 3. 混合搜索（全文 + 向量）

```sql
-- 混合搜索
SELECT * FROM articles 
WHERE content @@ '数据库优化' 
ORDER BY hybrid_score(
    bm25_score(content, '数据库优化') * 0.7 + 
    vector_score(embedding, '[0.1, 0.2, ...]') * 0.3
) DESC
LIMIT 10;

-- 使用RRF融合
SELECT * FROM articles 
WHERE content @@ '数据库优化'
ORDER BY rrf_rank(
    bm25_rank(content, '数据库优化'),
    vector_rank(embedding, '[0.1, 0.2, ...]')
) ASC
LIMIT 10;
```

---

## 性能优化策略

### 1. 查询优化算法

| 算法 | 适用场景 | 时间复杂度 | 特点 |
|-----|---------|-----------|-----|
| **DAAT_MAXSCORE** | 通用 | O(N) | 跳过不可能进入Top-K的文档 |
| **DAAT_WAND** | 小k值查询 | O(k) | 使用贪心算法快速收敛 |
| **TAAT** | 短查询 | O(M*logN) | 对每个词单独计算后合并 |

### 2. 索引压缩

```go
// 使用变长编码压缩倒排列表
func compressPostings(postings []Posting) []byte {
    // 使用PForDelta或Simple9压缩
    // 大幅减少内存占用
}

// 使用跳表加速跳转
type SkipList struct {
    Levels []*SkipLevel
}

type SkipLevel struct {
    DocID      int64
    Index      int
    MaxScore   float64
}
```

### 3. 缓存策略

```go
type FullTextCache struct {
    queryCache  *lru.Cache // 查询结果缓存
    vectorCache *lru.Cache // 向量缓存
    statsCache  *lru.Cache // 统计信息缓存
}
```

### 4. 并发控制

```go
// 分片锁减少锁竞争
type ShardedInvertedIndex struct {
    shards []*IndexShard
    shardCount int
}

type IndexShard struct {
    postings map[int64]*PostingsList
    mu       sync.RWMutex
}

func (idx *ShardedInvertedIndex) getShard(termID int64) *IndexShard {
    return idx.shards[int(termID)%idx.shardCount]
}
```

---

## 与现有系统集成

### 1. 目录结构

```
pkg/
├── fulltext/                    # 全文搜索核心包
│   ├── analyzer/               # 分词器
│   │   ├── tokenizer.go        # 分词器接口
│   │   ├── jieba.go            # Jieba分词器
│   │   ├── ngram.go            # N-gram分词器
│   │   └── english.go          # 英文分词器
│   ├── bm25/                   # BM25评分
│   │   ├── scorer.go           # 评分器
│   │   ├── sparse_vector.go    # 稀疏向量
│   │   └── stats.go            # 统计信息
│   ├── index/                  # 倒排索引
│   │   ├── inverted_index.go   # 倒排索引实现
│   │   ├── posting.go          # 倒排列表
│   │   └── skip_list.go        # 跳表优化
│   ├── query/                  # 查询系统
│   │   ├── query.go            # 查询接口
│   │   ├── parser.go           # 查询解析器
│   │   └── executor.go         # 查询执行器
│   ├── engine.go               # 全文引擎
│   └── config.go               # 配置
└── resource/memory/
    └── fulltext_index.go       # 与IndexManager集成
```

### 2. 索引管理器集成

```go
// IndexManager 扩展示例
func (m *IndexManager) CreateFullTextIndex(
    tableName, columnName string,
    tokenizerType string,
    bm25Params fulltext.BM25Params,
) (*AdvancedFullTextIndex, error) {
    
    idx, err := NewAdvancedFullTextIndex(
        tableName, columnName,
        tokenizerType, bm25Params,
    )
    if err != nil {
        return nil, err
    }
    
    // 存储索引
    tableIdxs := m.getOrCreateTableIndexes(tableName)
    tableIdxs.columnMap[columnName] = idx
    tableIdxs.indexes[idx.GetIndexInfo().Name] = idx
    
    return idx, nil
}
```

### 3. 优化器集成

```go
// 全文搜索代价估算
func EstimateFullTextCost(
    rowCount int64,
    queryTerms int,
    selectivity float64,
) float64 {
    // 倒排列表查找代价
    lookupCost := float64(queryTerms) * math.Log(float64(rowCount))
    
    // 合并代价
    mergeCost := float64(rowCount) * selectivity * 0.1
    
    // BM25计算代价
    scoreCost := float64(rowCount) * selectivity * 0.05
    
    return lookupCost + mergeCost + scoreCost
}
```

---

## 性能基准

| 指标 | 目标值 | 说明 |
|-----|-------|-----|
| **索引速度** | >10,000 docs/s | 单线程 |
| **查询延迟** | <10ms (p99) | 100万文档 |
| **并发查询** | >1000 QPS | 4核8G |
| **内存占用** | <原始文本3倍 | 含倒排索引 |
| **召回率** | >95% | 对比 Elasticsearch |

---

## 实施路线图

### 阶段1：核心框架（2周）
- [ ] 项目结构设计
- [ ] 分词器接口和Jieba实现
- [ ] BM25评分系统
- [ ] 基础倒排索引

### 阶段2：查询系统（2周）
- [ ] 查询解析器
- [ ] 布尔查询实现
- [ ] DAAT_MAXSCORE优化
- [ ] 短语查询

### 阶段3：高级功能（2周）
- [ ] 模糊搜索
- [ ] 高亮显示
- [ ] 混合搜索（全文+向量）
- [ ] SQL接口

### 阶段4：性能优化（2周）
- [ ] 索引压缩
- [ ] 并发优化
- [ ] 缓存系统
- [ ] 性能测试

---

## 总结

本设计为项目提供了：

1. **完整的全文搜索能力**：从分词到查询的全链路支持
2. **极致的中文体验**：Jieba分词 + BM25算法优化
3. **高性能架构**：DAAT_MAXSCORE + WAND + 多线程
4. **丰富的查询语法**：短语、模糊、布尔、邻近搜索
5. **无缝集成**：与现有索引系统和SQL层完美融合

这是项目的**灵魂功能**，将全文搜索能力提升到生产级水平。
