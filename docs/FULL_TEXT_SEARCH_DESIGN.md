# 全文搜索设计方案

## 概述

参考 Milvus 的全文搜索实现，设计一个支持中文等东亚语言的全文搜索方案。全文搜索使用 BM25 算法进行相关性评分，通过文本分词、稀疏向量转换和倒排索引实现高效的文本检索。

## 核心架构

### 1. 整体流程

```
原始文本 → 分词器 → BM25 函数 → 稀疏向量 → 倒排索引 → BM25 搜索 → 排序结果
```

### 2. 关键组件

#### 2.1 文本分词器（Text Analyzer）
- **作用**：将文本分割成有意义的词汇单元（tokens）
- **中文支持**：
  - 支持多种中文分词算法
  - 支持自定义词典
  - 支持停用词过滤

#### 2.2 BM25 函数
- **作用**：将分词结果转换为稀疏向量表示
- **公式**：
  ```
  BM25(D, Q) = Σ IDF(qi) × (f(qi, D) × (k1 + 1)) / (f(qi, D) + k1 × (1 - b + b × |D| / avgdl))
  ```
  - `f(qi, D)`: 术语 qi 在文档 D 中的频率
  - `|D|`: 文档 D 的长度
  - `avgdl`: 平均文档长度
  - `k1`: 词频饱和参数 [1.2, 2.0]
  - `b`: 长度归一化参数 [0, 1]

#### 2.3 稀疏向量
- **表示方式**：Map[int32]float64，key 为词汇 ID，value 为 BM25 分数
- **特点**：
  - 高维度但稀疏（大部分元素为 0）
  - 节省存储空间
  - 快速计算

#### 2.4 倒排索引（Inverted Index）
- **作用**：快速定位包含特定词汇的文档
- **结构**：
  ```
  term_id: {
    doc_id1: {position: [p1, p2, ...], frequency: n},
    doc_id2: {position: [p1, ...], frequency: m},
    ...
  }
  ```

## 数据结构设计

### 1. 表结构（SQL）

```sql
-- 全文搜索表
CREATE TABLE fulltext_documents (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    text TEXT NOT NULL,                    -- 原始文本
    language VARCHAR(10) DEFAULT 'zh',     -- 语言标识
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ft_text (text(100))          -- 索引前缀
) ENGINE=InnoDB;

-- 全文索引表（存储 BM25 稀疏向量）
CREATE TABLE fulltext_index (
    doc_id BIGINT NOT NULL,
    term_id INT NOT NULL,
    score FLOAT NOT NULL,                 -- BM25 分数
    frequency INT NOT NULL,                -- 词频
    positions BLOB,                       -- 位置信息（JSON）
    PRIMARY KEY (doc_id, term_id),
    INDEX idx_term (term_id),
    FOREIGN KEY (doc_id) REFERENCES fulltext_documents(id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- 词汇表
CREATE TABLE vocabulary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    term VARCHAR(100) NOT NULL UNIQUE,   -- 词汇
    df INT DEFAULT 0,                     -- 文档频率
    idf FLOAT DEFAULT 0,                  -- 逆文档频率
    language VARCHAR(10) DEFAULT 'zh',
    INDEX idx_term_language (term, language)
) ENGINE=InnoDB;

-- BM25 统计信息
CREATE TABLE bm25_stats (
    collection_id BIGINT PRIMARY KEY,
    avg_doc_length FLOAT NOT NULL,        -- 平均文档长度
    total_docs INT NOT NULL,              -- 总文档数
    k1 FLOAT DEFAULT 1.2,                -- BM25 k1 参数
    b FLOAT DEFAULT 0.75                 -- BM25 b 参数
) ENGINE=InnoDB;
```

### 2. 内存结构（Go）

```go
// TextAnalyzer 文本分词器接口
type TextAnalyzer interface {
    Analyze(text string, language string) ([]Token, error)
}

// Token 分词结果
type Token struct {
    Text      string
    Position  int
    Offset    int
    Type      string // 词性标记
}

// BM25Function BM25 函数
type BM25Function struct {
    k1        float64
    b         float64
    vocab     *Vocabulary
    stats     *BM25Stats
}

// SparseVector 稀疏向量
type SparseVector map[int32]float64

// InvertedIndex 倒排索引
type InvertedIndex struct {
    index map[int32]*PostingsList // term_id -> postings list
}

// PostingsList 倒排链
type PostingsList struct {
    DocID     int32
    Frequency int
    Positions []int
    BM25Score float64
    Next      *PostingsList
}

// BM25Stats BM25 统计信息
type BM25Stats struct {
    AvgDocLength float64
    TotalDocs   int
    K1          float64
    B           float64
}
```

## 分词器实现

### 1. 中文分词器

#### 1.1 Jieba 分词器（推荐）

```go
import (
    "github.com/wanglei1918/gosim/jieba"
)

type JiebaAnalyzer struct {
    jieba *jieba.Segmenter
    stopWords map[string]bool
}

func NewJiebaAnalyzer(dictPath string, hmmPath string, userDict string, stopWordsPath string) *JiebaAnalyzer {
    segmenter := jieba.NewJieba(dictPath, hmmPath, userDict, "")
    
    // 加载停用词
    stopWords := loadStopWords(stopWordsPath)
    
    return &JiebaAnalyzer{
        jieba:    segmenter,
        stopWords: stopWords,
    }
}

func (a *JiebaAnalyzer) Analyze(text string, language string) ([]Token, error) {
    if language != "zh" && language != "" {
        return nil, fmt.Errorf("unsupported language: %s", language)
    }
    
    // 使用 Jieba 分词
    words := a.jieba.CutForSearch(text, true)
    
    tokens := make([]Token, 0, len(words))
    position := 0
    
    for _, word := range words {
        // 过滤停用词和标点
        if a.stopWords[word] || len(word) <= 1 {
            continue
        }
        
        tokens = append(tokens, Token{
            Text:     word,
            Position: position,
            Offset:   0,
            Type:     "unknown",
        })
        position++
    }
    
    return tokens, nil
}
```

#### 1.2 HanLP 分词器（备选）

```go
type HanLPAnalyzer struct {
    stopWords map[string]bool
}

func NewHanLPAnalyzer(stopWordsPath string) *HanLPAnalyzer {
    stopWords := loadStopWords(stopWordsPath)
    return &HanLPAnalyzer{
        stopWords: stopWords,
    }
}

func (a *HanLPAnalyzer) Analyze(text string, language string) ([]Token, error) {
    if language != "zh" {
        return nil, fmt.Errorf("unsupported language: %s", language)
    }
    
    // 使用 HanLP 分词
    words := hanlp.Cut(text)
    
    tokens := make([]Token, 0, len(words))
    position := 0
    
    for _, word := range words {
        if a.stopWords[word] || len(word) <= 1 {
            continue
        }
        
        tokens = append(tokens, Token{
            Text:     word,
            Position: position,
            Type:     "unknown",
        })
        position++
    }
    
    return tokens, nil
}
```

### 2. 英文分词器

```go
import (
    "regexp"
    "strings"
)

type EnglishAnalyzer struct {
    stopWords map[string]bool
}

func NewEnglishAnalyzer(stopWordsPath string) *EnglishAnalyzer {
    stopWords := loadStopWords(stopWordsPath)
    return &EnglishAnalyzer{
        stopWords: stopWords,
    }
}

func (a *EnglishAnalyzer) Analyze(text string, language string) ([]Token, error) {
    if language != "en" {
        return nil, fmt.Errorf("unsupported language: %s", language)
    }
    
    // 转小写
    text = strings.ToLower(text)
    
    // 分割单词（支持连字符和撇号）
    re := regexp.MustCompile(`[a-z]+(?:'[a-z]+)?`)
    words := re.FindAllString(text, -1)
    
    tokens := make([]Token, 0, len(words))
    position := 0
    
    for _, word := range words {
        if a.stopWords[word] || len(word) <= 2 {
            continue
        }
        
        tokens = append(tokens, Token{
            Text:     word,
            Position: position,
            Type:     "unknown",
        })
        position++
    }
    
    return tokens, nil
}
```

### 3. 多语言支持

```go
type MultiLanguageAnalyzer struct {
    analyzers map[string]TextAnalyzer
    defaultLang string
}

func NewMultiLanguageAnalyzer() *MultiLanguageAnalyzer {
    return &MultiLanguageAnalyzer{
        analyzers: make(map[string]TextAnalyzer),
        defaultLang: "zh",
    }
}

func (a *MultiLanguageAnalyzer) RegisterAnalyzer(lang string, analyzer TextAnalyzer) {
    a.analyzers[lang] = analyzer
}

func (a *MultiLanguageAnalyzer) Analyze(text string, language string) ([]Token, error) {
    if language == "" {
        language = a.defaultLang
    }
    
    analyzer, ok := a.analyzers[language]
    if !ok {
        return nil, fmt.Errorf("no analyzer for language: %s", language)
    }
    
    return analyzer.Analyze(text, language)
}
```

## BM25 实现

### 1. BM25 函数

```go
type BM25Function struct {
    k1    float64
    b     float64
    vocab *Vocabulary
    stats *BM25Stats
}

func NewBM25Function(k1, b float64, vocab *Vocabulary, stats *BM25Stats) *BM25Function {
    return &BM25Function{
        k1:    k1,
        b:     b,
        vocab: vocab,
        stats: stats,
    }
}

// ConvertToSparseVector 将文档转换为稀疏向量
func (bm25 *BM25Function) ConvertToSparseVector(tokens []Token) (SparseVector, error) {
    sparse := make(SparseVector)
    
    // 统计词频
    termFreq := make(map[int32]int)
    docLength := len(tokens)
    
    for _, token := range tokens {
        termID, err := bm25.vocab.GetOrCreateTermID(token.Text)
        if err != nil {
            continue
        }
        termFreq[termID]++
    }
    
    // 计算 BM25 分数
    for termID, freq := range termFreq {
        idf := bm25.vocab.GetIDF(termID)
        
        // BM25 公式
        numerator := float64(freq * (bm25.k1 + 1))
        denominator := float64(freq) + bm25.k1*(1-bm25.b+bm25.b*float64(docLength)/bm25.stats.AvgDocLength)
        
        score := idf * (numerator / denominator)
        sparse[termID] = score
    }
    
    return sparse, nil
}

// ComputeScore 计算 BM25 分数
func (bm25 *BM25Function) ComputeScore(querySparse SparseVector, docSparse SparseVector) float64 {
    var score float64 = 0
    
    // 只计算查询词的 BM25 分数
    for termID, queryScore := range querySparse {
        if docScore, exists := docSparse[termID]; exists {
            score += queryScore * docScore
        }
    }
    
    return score
}
```

### 2. 词汇表管理

```go
type Vocabulary struct {
    mu sync.RWMutex
    db *sql.DB
    cache map[string]int32 // term -> id
}

func NewVocabulary(db *sql.DB) *Vocabulary {
    return &Vocabulary{
        db:    db,
        cache: make(map[string]int32),
    }
}

func (v *Vocabulary) GetOrCreateTermID(term string) (int32, error) {
    v.mu.RLock()
    if termID, exists := v.cache[term]; exists {
        v.mu.RUnlock()
        return termID, nil
    }
    v.mu.RUnlock()
    
    v.mu.Lock()
    defer v.mu.Unlock()
    
    // 再次检查（防止并发问题）
    if termID, exists := v.cache[term]; exists {
        return termID, nil
    }
    
    // 从数据库查询
    var termID int32
    err := v.db.QueryRow("SELECT id FROM vocabulary WHERE term = ?", term).Scan(&termID)
    if err == sql.ErrNoRows {
        // 插入新词
        result, err := v.db.Exec("INSERT INTO vocabulary (term) VALUES (?)", term)
        if err != nil {
            return 0, err
        }
        id, _ := result.LastInsertId()
        termID = int32(id)
    } else if err != nil {
        return 0, err
    }
    
    v.cache[term] = termID
    return termID, nil
}

func (v *Vocabulary) GetIDF(termID int32) float64 {
    var df int
    var totalDocs int
    
    v.db.QueryRow("SELECT df FROM vocabulary WHERE id = ?", termID).Scan(&df)
    v.db.QueryRow("SELECT total_docs FROM bm25_stats LIMIT 1").Scan(&totalDocs)
    
    if df == 0 || totalDocs == 0 {
        return 0
    }
    
    // IDF = log((N - df + 0.5) / (df + 0.5))
    return math.Log(float64(totalDocs-df+0.5) / float64(df+0.5))
}

func (v *Vocabulary) UpdateDF(termID int32) error {
    _, err := v.db.Exec("UPDATE vocabulary SET df = df + 1 WHERE id = ?", termID)
    return err
}
```

## 倒排索引实现

```go
type InvertedIndex struct {
    mu    sync.RWMutex
    index map[int32]*PostingsList // term_id -> postings list
    db    *sql.DB
}

func NewInvertedIndex(db *sql.DB) *InvertedIndex {
    return &InvertedIndex{
        index: make(map[int32]*PostingsList),
        db:    db,
    }
}

// AddDocument 添加文档到倒排索引
func (ii *InvertedIndex) AddDocument(docID int64, sparse SparseVector) error {
    ii.mu.Lock()
    defer ii.mu.Unlock()
    
    tx, err := ii.db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()
    
    for termID, score := range sparse {
        // 内存索引
        postings := ii.index[termID]
        if postings == nil {
            postings = &PostingsList{}
            ii.index[termID] = postings
        }
        
        // 添加到链表
        newPosting := &PostingsList{
            DocID:     int32(docID),
            BM25Score: score,
            Next:      postings.Next,
        }
        postings.Next = newPosting
        
        // 数据库索引
        _, err := tx.Exec(
            "INSERT INTO fulltext_index (doc_id, term_id, score) VALUES (?, ?, ?)",
            docID, termID, score,
        )
        if err != nil {
            return err
        }
        
        // 更新文档频率
        _, err = tx.Exec("UPDATE vocabulary SET df = df + 1 WHERE id = ?", termID)
        if err != nil {
            return err
        }
    }
    
    return tx.Commit()
}

// Search 搜索匹配文档
func (ii *InvertedIndex) Search(querySparse SparseVector, topK int) []SearchResult {
    ii.mu.RLock()
    defer ii.mu.RUnlock()
    
    // 收集候选文档
    candidates := make(map[int32]float64)
    
    for termID, queryScore := range querySparse {
        postings := ii.index[termID]
        if postings == nil {
            continue
        }
        
        // 遍历倒排链
        for p := postings.Next; p != nil; p = p.Next {
            score := queryScore * p.BM25Score
            candidates[p.DocID] += score
        }
    }
    
    // 排序并返回 Top-K
    results := make([]SearchResult, 0, len(candidates))
    for docID, score := range candidates {
        results = append(results, SearchResult{
            DocID: docID,
            Score: score,
        })
    }
    
    // 按分数降序排序
    sort.Slice(results, func(i, j int) bool {
        return results[i].Score > results[j].Score
    })
    
    if topK > len(results) {
        topK = len(results)
    }
    
    return results[:topK]
}

type SearchResult struct {
    DocID int32
    Score float64
}
```

## SQL 接口设计

### 1. CREATE FULLTEXT INDEX 语法

```sql
-- 创建全文索引
CREATE FULLTEXT INDEX idx_ft_text ON articles(content)
WITH (
    analyzer = 'jieba',           -- 分词器: jieba, hanlp, english
    language = 'zh',                -- 语言: zh, en
    k1 = 1.2,                     -- BM25 k1 参数
    b = 0.75                      -- BM25 b 参数
);

-- 指定自定义词典
CREATE FULLTEXT INDEX idx_ft_text ON articles(content)
WITH (
    analyzer = 'jieba',
    language = 'zh',
    user_dict = '/path/to/user.dict',
    stop_words = '/path/to/stop_words.txt'
);
```

### 2. 全文搜索语法

```sql
-- 全文搜索
SELECT * FROM articles
WHERE MATCH(content) AGAINST('搜索关键词')
ORDER BY SCORE DESC
LIMIT 10;

-- 带参数的全文搜索
SELECT * FROM articles
WHERE MATCH(content) AGAINST('搜索关键词' IN BOOLEAN MODE)
ORDER BY SCORE DESC
LIMIT 10;

-- 混合搜索（全文 + 向量）
SELECT * FROM articles
WHERE MATCH(content) AGAINST('搜索关键词')
ORDER BY (bm25_score * 0.7 + vector_score * 0.3) DESC
LIMIT 10;
```

### 3. 存储过程

```sql
-- 插入文档并更新全文索引
CREATE PROCEDURE InsertDocumentWithFulltext(
    IN p_text TEXT,
    IN p_language VARCHAR(10)
)
BEGIN
    DECLARE doc_id BIGINT;
    
    -- 插入文档
    INSERT INTO fulltext_documents (text, language)
    VALUES (p_text, p_language);
    
    SET doc_id = LAST_INSERT_ID();
    
    -- 调用 BM25 函数生成稀疏向量
    --（需要在应用层实现，这里只是示例）
    
    -- 返回文档 ID
    SELECT doc_id;
END;
```

## 性能优化

### 1. 查询优化算法

#### DAAT_MAXSCORE（推荐）
```go
func (ii *InvertedIndex) SearchDAATMaxScore(querySparse SparseVector, topK int) []SearchResult {
    // 按最大影响分数排序术语
    sortedTerms := ii.sortTermsByMaxScore(querySparse)
    
    // 维护 Top-K 结果
    heap := NewMinHeap(topK)
    
    for _, term := range sortedTerms {
        postings := ii.index[term.TermID]
        if postings == nil {
            continue
        }
        
        for p := postings.Next; p != nil; p = p.Next {
            currentScore := heap.GetMinScore()
            maxScore := term.MaxScore * p.BM25Score
            
            // 如果可能进入 Top-K，则计算完整分数
            if currentScore < 0 || maxScore > currentScore {
                score := heap.GetScore(p.DocID)
                if score >= 0 {
                    heap.Update(p.DocID, score+term.QueryScore*p.BM25Score)
                } else {
                    heap.Insert(p.DocID, term.QueryScore*p.BM25Score)
                }
            }
        }
    }
    
    return heap.GetAll()
}
```

#### DAAT_WAND
```go
func (ii *InvertedIndex) SearchDAATWAND(querySparse SparseVector, topK int) []SearchResult {
    // 实现 WAND 算法，跳过非竞争性文档
    // 适合小 k 值或短查询
    // ...
}
```

### 2. 缓存策略

```go
type FulltextCache struct {
    queryCache  *lru.Cache  // 查询结果缓存
    vocabCache  *lru.Cache  // 词汇表缓存
    indexCache  *lru.Cache  // 倒排索引缓存
}

func NewFulltextCache() *FulltextCache {
    return &FulltextCache{
        queryCache: lru.New(1000),
        vocabCache: lru.New(10000),
        indexCache: lru.New(1000),
    }
}
```

### 3. 并发控制

```go
type ConcurrentInvertedIndex struct {
    shards []*Shard
    shardCount int
}

type Shard struct {
    mu    sync.RWMutex
    index map[int32]*PostingsList
}

func NewConcurrentInvertedIndex(shardCount int) *ConcurrentInvertedIndex {
    shards := make([]*Shard, shardCount)
    for i := 0; i < shardCount; i++ {
        shards[i] = &Shard{
            index: make(map[int32]*PostingsList),
        }
    }
    return &ConcurrentInvertedIndex{
        shards:      shards,
        shardCount:  shardCount,
    }
}

func (cii *ConcurrentInvertedIndex) getShard(termID int32) *Shard {
    return cii.shards[int(termID)%cii.shardCount]
}
```

## 混合搜索方案

### 1. 全文搜索 + 向量搜索

```go
type HybridSearch struct {
    fulltextIndex *InvertedIndex
    vectorIndex   *VectorIndex
    ftWeight      float64 // 全文搜索权重
    vecWeight     float64 // 向量搜索权重
}

func (hs *HybridSearch) Search(queryText string, queryVector []float64, topK int) []HybridResult {
    // 全文搜索
    ftResults := hs.fulltextIndex.SearchText(queryText, topK*2)
    
    // 向量搜索
    vecResults := hs.vectorIndex.SearchVector(queryVector, topK*2)
    
    // 归一化分数
    ftMaxScore := ftResults[0].Score
    vecMaxScore := vecResults[0].Score
    
    // 合并结果
    merged := make(map[int32]*HybridResult)
    
    for _, ft := range ftResults {
        merged[ft.DocID] = &HybridResult{
            DocID:   ft.DocID,
            FTScore: ft.Score / ftMaxScore,
            VecScore: 0,
        }
    }
    
    for _, vec := range vecResults {
        if result, exists := merged[vec.DocID]; exists {
            result.VecScore = vec.Score / vecMaxScore
        } else {
            merged[vec.DocID] = &HybridResult{
                DocID:    vec.DocID,
                FTScore:   0,
                VecScore:  vec.Score / vecMaxScore,
            }
        }
    }
    
    // 计算混合分数
    results := make([]HybridResult, 0, len(merged))
    for _, result := range merged {
        result.HybridScore = result.FTScore*hs.ftWeight + result.VecScore*hs.vecWeight
        results = append(results, *result)
    }
    
    // 排序并返回 Top-K
    sort.Slice(results, func(i, j int) bool {
        return results[i].HybridScore > results[j].HybridScore
    })
    
    if topK > len(results) {
        topK = len(results)
    }
    
    return results[:topK]
}

type HybridResult struct {
    DocID      int32
    FTScore    float64
    VecScore   float64
    HybridScore float64
}
```

### 2. RRF（Reciprocal Rank Fusion）

```go
func RRF(results1, results2 []SearchResult, k int) []SearchResult {
    scores := make(map[int32]float64)
    
    // 合并结果集
    for i, result := range results1 {
        scores[result.DocID] += 1.0 / float64(k+i+1)
    }
    
    for i, result := range results2 {
        scores[result.DocID] += 1.0 / float64(k+i+1)
    }
    
    // 排序
    ranked := make([]SearchResult, 0, len(scores))
    for docID, score := range scores {
        ranked = append(ranked, SearchResult{
            DocID: docID,
            Score: score,
        })
    }
    
    sort.Slice(ranked, func(i, j int) bool {
        return ranked[i].Score > ranked[j].Score
    })
    
    return ranked
}
```

## 部署方案

### 1. 配置文件

```yaml
fulltext:
  analyzer:
    type: "jieba"  # jieba, hanlp, english
    language: "zh"
    user_dict: "/data/dicts/user.dict"
    stop_words: "/data/dicts/stop_words.txt"
  
  bm25:
    k1: 1.2
    b: 0.75
  
  index:
    algorithm: "daat_maxscore"  # daat_maxscore, daat_wand, taat_naive
    shard_count: 16
    
  cache:
    query_cache_size: 1000
    vocab_cache_size: 10000
    index_cache_size: 1000
```

### 2. 依赖管理

```go
// go.mod
require (
    github.com/wanglei1918/gosim v0.0.0
    github.com/yanyiwu/gojieba v1.0.0
    github.com/hankcs/HanLP v2.0.0
    github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
)
```

### 3. 初始化

```go
func InitFullTextSearch(config *FullTextConfig) (*FullTextEngine, error) {
    // 初始化数据库连接
    db, err := sql.Open("mysql", config.DSN)
    if err != nil {
        return nil, err
    }
    
    // 创建表结构
    if err := createTables(db); err != nil {
        return nil, err
    }
    
    // 初始化分词器
    analyzer := NewJiebaAnalyzer(
        config.Analyzer.UserDict,
        "",
        "",
        config.Analyzer.StopWords,
    )
    
    // 初始化 BM25 函数
    vocab := NewVocabulary(db)
    stats := loadBM25Stats(db)
    bm25 := NewBM25Function(config.BM25.K1, config.BM25.B, vocab, stats)
    
    // 初始化倒排索引
    index := NewInvertedIndex(db)
    
    return &FullTextEngine{
        analyzer: analyzer,
        bm25:     bm25,
        index:    index,
        db:       db,
        cache:    NewFulltextCache(),
    }, nil
}
```

## 总结

本设计方案提供了：

1. **完整的全文搜索支持**：支持中文、英文等多种语言
2. **高效的 BM25 算法**：使用稀疏向量和倒排索引
3. **多种查询优化算法**：DAAT_MAXSCORE, DAAT_WAND, TAAT_NAIVE
4. **混合搜索能力**：全文搜索 + 向量搜索
5. **性能优化**：缓存、分片、并发控制
6. **灵活的 SQL 接口**：符合标准的 SQL 语法

下一步：
1. 实现核心组件
2. 添加单元测试
3. 性能基准测试
4. 与现有系统集成
