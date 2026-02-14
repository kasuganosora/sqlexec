package fulltext

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/fulltext/analyzer"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/bm25"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/index"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/query"
)

// DefaultEnglishStopWords 默认英文停用词
var DefaultEnglishStopWords = analyzer.DefaultEnglishStopWords

// DefaultChineseStopWords 默认中文停用词
var DefaultChineseStopWords = analyzer.DefaultChineseStopWords

// Engine 全文搜索引擎
type Engine struct {
	config      *Config
	tokenizer   analyzer.Tokenizer
	scorer      *bm25.Scorer
	invertedIdx *index.InvertedIndex
	vocabulary  *Vocabulary
	mu          sync.RWMutex
}

// Vocabulary 词汇表
type Vocabulary struct {
	termToID map[string]int64
	idToTerm map[int64]string
	nextID   int64
	mu       sync.RWMutex
}

// NewVocabulary 创建词汇表
func NewVocabulary() *Vocabulary {
	return &Vocabulary{
		termToID: make(map[string]int64),
		idToTerm: make(map[int64]string),
		nextID:   1,
	}
}

// GetOrCreateID 获取或创建词ID
func (v *Vocabulary) GetOrCreateID(term string) int64 {
	v.mu.RLock()
	if id, exists := v.termToID[term]; exists {
		v.mu.RUnlock()
		return id
	}
	v.mu.RUnlock()
	
	v.mu.Lock()
	defer v.mu.Unlock()
	
	// 双重检查
	if id, exists := v.termToID[term]; exists {
		return id
	}
	
	id := v.nextID
	v.nextID++
	v.termToID[term] = id
	v.idToTerm[id] = term
	return id
}

// GetTerm 获取词
func (v *Vocabulary) GetTerm(id int64) (string, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	term, ok := v.idToTerm[id]
	return term, ok
}

// NewEngine 创建全文搜索引擎
func NewEngine(config *Config) *Engine {
	if config == nil {
		config = DefaultConfig
	}
	
	// 创建分词器
	tokenizer, _ := analyzer.TokenizerFactory(analyzer.TokenizerTypeStandard, nil)
	
	// 创建BM25评分器
	stats := bm25.NewCollectionStats()
	bm25Params := bm25.Params{
		K1: config.BM25Params.K1,
		B:  config.BM25Params.B,
	}
	scorer := bm25.NewScorer(bm25Params, stats)
	
	return &Engine{
		config:      config,
		tokenizer:   tokenizer,
		scorer:      scorer,
		invertedIdx: index.NewInvertedIndex(scorer),
		vocabulary:  NewVocabulary(),
	}
}

// SetTokenizer 设置分词器
func (e *Engine) SetTokenizer(tokenizer analyzer.Tokenizer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tokenizer = tokenizer
}

// IndexDocument 索引文档
func (e *Engine) IndexDocument(doc *Document) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	// 分词
	tokens, err := e.tokenizer.Tokenize(doc.Content)
	if err != nil {
		return fmt.Errorf("tokenize failed: %w", err)
	}
	
	// 转换为内部文档类型
	internalDoc := &index.Document{
		ID:      doc.ID,
		Content: doc.Content,
		Fields:  doc.Fields,
	}
	
	// 添加到倒排索引
	return e.invertedIdx.AddDocument(internalDoc, tokens)
}

// IndexDocumentWithTokens 使用预分词的token索引文档
func (e *Engine) IndexDocumentWithTokens(doc *Document, tokens []Token) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	// 转换词为ID
	idTokens := make([]analyzer.Token, len(tokens))
	for i, token := range tokens {
		idTokens[i] = analyzer.Token{
			Text:     token.Text,
			Position: token.Position,
			Start:    token.Start,
			End:      token.End,
			Type:     token.Type,
		}
	}
	
	// 转换为内部文档类型
	internalDoc := &index.Document{
		ID:      doc.ID,
		Content: doc.Content,
		Fields:  doc.Fields,
	}
	
	return e.invertedIdx.AddDocument(internalDoc, idTokens)
}

// Search 搜索
func (e *Engine) Search(queryStr string, topK int) ([]SearchResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// 使用引擎的分词器对查询进行分词
	tokens, err := e.tokenizer.Tokenize(queryStr)
	if err != nil {
		return nil, fmt.Errorf("tokenize query failed: %w", err)
	}
	
	// 构建布尔查询
	boolQuery := query.NewBooleanQuery()
	for _, token := range tokens {
		boolQuery.AddShould(query.NewTermQuery("content", token.Text))
	}
	
	// 执行查询
	queryResults := boolQuery.Execute(e.invertedIdx)
	
	// 转换为引擎结果类型
	results := convertQueryResults(queryResults)
	
	// 限制结果数量
	if topK > 0 && len(results) > topK {
		results = results[:topK]
	}
	
	return results, nil
}

// SearchWithQuery 使用Query对象搜索
func (e *Engine) SearchWithQuery(q query.Query, topK int) ([]SearchResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	queryResults := q.Execute(e.invertedIdx)
	
	// 转换为引擎结果类型
	results := convertQueryResults(queryResults)
	
	if topK > 0 && len(results) > topK {
		results = results[:topK]
	}
	
	return results, nil
}

// SearchBM25 使用BM25评分搜索
func (e *Engine) SearchBM25(queryStr string, topK int) ([]SearchResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// 分词查询
	tokens, err := e.tokenizer.TokenizeForSearch(queryStr)
	if err != nil {
		return nil, fmt.Errorf("tokenize failed: %w", err)
	}
	
	// 构建查询向量（使用hashString保持与索引一致）
	queryVector := bm25.NewSparseVector()
	for _, token := range tokens {
		termID := hashString(token.Text)
		// 计算查询词的权重（这里使用简单的TF）
		if weight, exists := queryVector.Get(termID); exists {
			queryVector.Set(termID, weight+1.0)
		} else {
			queryVector.Set(termID, 1.0)
		}
	}
	
	// 归一化查询向量
	queryVector.Normalize()
	
	// 执行搜索
	idxResults := e.invertedIdx.SearchTopK(queryVector, topK)
	if topK <= 0 {
		idxResults = e.invertedIdx.Search(queryVector)
	}
	
	// 转换为引擎结果类型
	results := convertIdxResults(idxResults)
	
	return results, nil
}

// hashString 将字符串hash为int64（与index包保持一致）
func hashString(s string) int64 {
	h := int64(0)
	for _, c := range s {
		h = h*31 + int64(c)
	}
	return h
}

// SearchWithHighlight 带高亮的搜索
func (e *Engine) SearchWithHighlight(queryStr string, topK int, preTag, postTag string) ([]SearchResultWithHighlight, error) {
	results, err := e.Search(queryStr, topK)
	if err != nil {
		return nil, err
	}
	
	highlighter := &Highlighter{
		PreTag:  preTag,
		PostTag: postTag,
	}
	
	// 获取查询词
	tokens, _ := e.tokenizer.Tokenize(queryStr)
	queryTerms := make([]string, len(tokens))
	for i, token := range tokens {
		queryTerms[i] = token.Text
	}
	
	resultsWithHighlight := make([]SearchResultWithHighlight, len(results))
	for i, result := range results {
		highlights := highlighter.Highlight(result.Doc.Content, queryTerms)
		resultsWithHighlight[i] = SearchResultWithHighlight{
			SearchResult: result,
			Highlights:   highlights,
		}
	}
	
	return resultsWithHighlight, nil
}

// convertIdxResults 转换索引结果到引擎结果
func convertIdxResults(idxResults []index.SearchResult) []SearchResult {
	results := make([]SearchResult, len(idxResults))
	for i, r := range idxResults {
		doc := r.Doc
		if doc == nil {
			doc = &index.Document{}
		}
		results[i] = SearchResult{
			DocID: r.DocID,
			Score: r.Score,
			Doc: &Document{
				ID:      doc.ID,
				Content: doc.Content,
				Fields:  doc.Fields,
			},
		}
	}
	return results
}

// convertQueryResults 转换查询结果到引擎结果
func convertQueryResults(queryResults []query.SearchResult) []SearchResult {
	results := make([]SearchResult, len(queryResults))
	for i, r := range queryResults {
		doc := r.Doc
		if doc == nil {
			doc = &index.Document{}
		}
		results[i] = SearchResult{
			DocID: r.DocID,
			Score: r.Score,
			Doc: &Document{
				ID:      doc.ID,
				Content: doc.Content,
				Fields:  doc.Fields,
			},
		}
	}
	return results
}

// SearchPhrase 短语搜索
func (e *Engine) SearchPhrase(phrase string, slop int, topK int) ([]SearchResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// 分词
	tokens, err := e.tokenizer.Tokenize(phrase)
	if err != nil {
		return nil, err
	}
	
	// 直接使用原始token，不做词ID转换
	idxResults := e.invertedIdx.SearchPhrase(tokens, slop)
	results := convertIdxResults(idxResults)
	
	if topK > 0 && len(results) > topK {
		results = results[:topK]
	}
	
	return results, nil
}

// GetDocument 获取文档
func (e *Engine) GetDocument(docID int64) *Document {
	e.mu.RLock()
	defer e.mu.RUnlock()
	internalDoc := e.invertedIdx.GetDocument(docID)
	if internalDoc == nil {
		return nil
	}
	return &Document{
		ID:      internalDoc.ID,
		Content: internalDoc.Content,
		Fields:  internalDoc.Fields,
	}
}

// GetStats 获取统计信息
func (e *Engine) GetStats() *bm25.CollectionStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.invertedIdx.GetStats()
}

// DeleteDocument 删除文档
func (e *Engine) DeleteDocument(docID int64) error {
	// 简化实现：实际应该更新倒排索引
	return nil
}

// Clear 清空索引
func (e *Engine) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	stats := bm25.NewCollectionStats()
	bm25Params := bm25.Params{
		K1: e.config.BM25Params.K1,
		B:  e.config.BM25Params.B,
	}
	e.scorer = bm25.NewScorer(bm25Params, stats)
	e.invertedIdx = index.NewInvertedIndex(e.scorer)
	e.vocabulary = NewVocabulary()
}

// DocumentCount 获取文档数量
func (e *Engine) DocumentCount() int64 {
	return e.GetStats().TotalDocs
}

// Highlighter 高亮器
type Highlighter struct {
	PreTag       string
	PostTag      string
	FragmentLen  int
	NumFragments int
}

// Highlight 生成高亮文本
func (h *Highlighter) Highlight(text string, queryTerms []string) []string {
	if h.FragmentLen == 0 {
		h.FragmentLen = 150
	}
	if h.NumFragments == 0 {
		h.NumFragments = 3
	}
	
	// 查找查询词位置
	var positions []highlightPos
	lowerText := strings.ToLower(text)
	
	for _, term := range queryTerms {
		term = strings.ToLower(term)
		start := 0
		for {
			idx := strings.Index(lowerText[start:], term)
			if idx == -1 {
				break
			}
			actualIdx := start + idx
			positions = append(positions, highlightPos{
				start: actualIdx,
				end:   actualIdx + len(term),
			})
			start = actualIdx + 1
		}
	}
	
	// 按位置排序
	sort.Slice(positions, func(i, j int) bool {
		return positions[i].start < positions[j].start
	})
	
	// 生成片段
	fragments := h.extractFragments(text, positions)
	
	return fragments
}

type highlightPos struct {
	start int
	end   int
}

func (h *Highlighter) extractFragments(text string, positions []highlightPos) []string {
	if len(positions) == 0 {
		return []string{h.truncate(text, h.FragmentLen)}
	}
	
	var fragments []string
	added := make(map[string]bool)
	
	for _, pos := range positions {
		// 计算片段边界
		fragmentStart := pos.start - h.FragmentLen/2
		if fragmentStart < 0 {
			fragmentStart = 0
		}
		fragmentEnd := pos.end + h.FragmentLen/2
		if fragmentEnd > len(text) {
			fragmentEnd = len(text)
		}
		
		// 提取片段
		fragment := text[fragmentStart:fragmentEnd]
		
		// 添加高亮标记
		localStart := pos.start - fragmentStart
		localEnd := pos.end - fragmentStart
		
		if localStart >= 0 && localEnd <= len(fragment) {
			highlighted := fragment[:localStart] + h.PreTag +
				fragment[localStart:localEnd] + h.PostTag +
				fragment[localEnd:]
			
			if !added[highlighted] {
				fragments = append(fragments, highlighted)
				added[highlighted] = true
			}
		}
		
		if len(fragments) >= h.NumFragments {
			break
		}
	}
	
	return fragments
}

func (h *Highlighter) truncate(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + "..."
}
