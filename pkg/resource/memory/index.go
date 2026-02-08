package memory

import (
	"fmt"
	"strings"
	"sync"
)

// IndexType 索引类型
type IndexType string

const (
	IndexTypeBTree           IndexType = "btree"
	IndexTypeHash            IndexType = "hash"
	IndexTypeFullText        IndexType = "fulltext"
	IndexTypeVectorFlat      IndexType = "vector_flat"
	IndexTypeVectorIVFFlat   IndexType = "vector_ivf_flat"
	IndexTypeVectorHNSW      IndexType = "vector_hnsw"
	IndexTypeVectorIVFSQ8    IndexType = "vector_ivf_sq8"
	IndexTypeVectorIVFPQ     IndexType = "vector_ivf_pq"
	IndexTypeVectorHNSWSQ    IndexType = "vector_hnsw_sq"
	IndexTypeVectorHNSWPQ    IndexType = "vector_hnsw_pq"
	IndexTypeVectorIVFRabitQ IndexType = "vector_ivf_rabitq"
	IndexTypeVectorHNSWPRQ   IndexType = "vector_hnsw_prq"
	IndexTypeVectorAISAQ     IndexType = "vector_aisaq"
)

// IsVectorIndex 检查是否为向量索引
func (t IndexType) IsVectorIndex() bool {
	switch t {
	case IndexTypeVectorHNSW, IndexTypeVectorIVFFlat, IndexTypeVectorFlat,
		IndexTypeVectorIVFSQ8, IndexTypeVectorIVFPQ,
		IndexTypeVectorHNSWSQ, IndexTypeVectorHNSWPQ,
		IndexTypeVectorIVFRabitQ, IndexTypeVectorHNSWPRQ, IndexTypeVectorAISAQ:
		return true
	default:
		return false
	}
}

// VectorMetricType 距离度量类型
type VectorMetricType string

const (
	VectorMetricCosine     VectorMetricType = "cosine"
	VectorMetricL2         VectorMetricType = "l2"
	VectorMetricIP         VectorMetricType = "inner_product"
)

// VectorIndexConfig 向量索引配置
type VectorIndexConfig struct {
	MetricType VectorMetricType       `json:"metric_type"`
	Dimension  int                    `json:"dimension"`
	Params     map[string]interface{} `json:"params,omitempty"`
}

// Index 索引接口
type Index interface {
	// Insert 插入键值
	Insert(key interface{}, rowIDs []int64) error

	// Delete 删除键值
	Delete(key interface{}) error

	// Find 查找键值对应的行ID
	Find(key interface{}) ([]int64, bool)

	// FindRange 范围查询（仅B-Tree支持）
	FindRange(min, max interface{}) ([]int64, error)

	// GetIndexInfo 获取索引信息
	GetIndexInfo() *IndexInfo
}

// IndexInfo 索引信息
type IndexInfo struct {
	Name      string
	TableName string
	Column    string
	Type      IndexType
	Unique    bool
}

// ==================== B-Tree 索引实现 ====================

// BTreeIndex B-Tree 索引
type BTreeIndex struct {
	info  *IndexInfo
	root  *BTreeNode
	height int
	mu    sync.RWMutex
	unique bool
}

// BTreeNode B-Tree 节点（简化版，使用切片实现）
// 实际实现应该使用真正的B+ Tree结构
type BTreeNode struct {
	isLeaf   bool
	keys     []interface{}
	children []*BTreeNode
	mu       sync.RWMutex
}

// NewBTreeNode 创建B-Tree节点
func NewBTreeNode(isLeaf bool) *BTreeNode {
	return &BTreeNode{
		isLeaf:   isLeaf,
		keys:     make([]interface{}, 0),
		children: make([]*BTreeNode, 0),
	}
}

// NewBTreeIndex 创建B-Tree索引
func NewBTreeIndex(tableName, columnName string, unique bool) *BTreeIndex {
	return &BTreeIndex{
		info: &IndexInfo{
			Name:      fmt.Sprintf("idx_%s_%s", tableName, columnName),
			TableName: tableName,
			Column:    columnName,
			Type:      IndexTypeBTree,
			Unique:    unique,
		},
		root:  NewBTreeNode(true),
		height: 1,
		unique: unique,
	}
}

// Insert 插入键值到B-Tree索引
func (idx *BTreeIndex) Insert(key interface{}, rowIDs []int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 简化实现：插入到root节点
	// 实际应该实现B+ Tree的插入逻辑
	idx.root.keys = append(idx.root.keys, key)
	return nil
}

// Delete 从B-Tree索引删除键值
func (idx *BTreeIndex) Delete(key interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 简化实现：从root节点删除
	// 实际应该实现B+ Tree的删除逻辑
	for i, k := range idx.root.keys {
		if k == key {
			idx.root.keys = append(idx.root.keys[:i], idx.root.keys[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("key not found: %v", key)
}

// Find 在B-Tree索引查找键值
func (idx *BTreeIndex) Find(key interface{}) ([]int64, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 简化实现：在root节点查找
	// 实际应该实现B+ Tree的查找逻辑
	for _, k := range idx.root.keys {
		if k == key {
			// 简化返回
			return []int64{1}, true
		}
	}
	return nil, false
}

// FindRange 在B-Tree索引范围查询
func (idx *BTreeIndex) FindRange(min, max interface{}) ([]int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 简化实现：在root节点范围查询
	// 实际应该实现B+ Tree的范围查询逻辑
	var results []int64
	for _, k := range idx.root.keys {
		if compareKeys(k, min) >= 0 && compareKeys(k, max) <= 0 {
			results = append(results, 1)
		}
	}
	return results, nil
}

// GetIndexInfo 获取索引信息
func (idx *BTreeIndex) GetIndexInfo() *IndexInfo {
	return idx.info
}

// ==================== Hash 索引实现 ====================

// HashIndex 哈希索引
type HashIndex struct {
	info  *IndexInfo
	data  map[interface{}][]int64
	mu    sync.RWMutex
	unique bool
}

// NewHashIndex 创建哈希索引
func NewHashIndex(tableName, columnName string, unique bool) *HashIndex {
	return &HashIndex{
		info: &IndexInfo{
			Name:      fmt.Sprintf("idx_%s_%s", tableName, columnName),
			TableName: tableName,
			Column:    columnName,
			Type:      IndexTypeHash,
			Unique:    unique,
		},
		data:  make(map[interface{}][]int64),
		unique: unique,
	}
}

// Insert 插入键值到哈希索引
func (idx *HashIndex) Insert(key interface{}, rowIDs []int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.unique {
		if _, exists := idx.data[key]; exists {
			return fmt.Errorf("duplicate key violation for unique index")
		}
		idx.data[key] = rowIDs
	} else {
		idx.data[key] = append(idx.data[key], rowIDs...)
	}

	return nil
}

// Delete 从哈希索引删除键值
func (idx *HashIndex) Delete(key interface{}) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	delete(idx.data, key)
	return nil
}

// Find 在哈希索引查找键值
func (idx *HashIndex) Find(key interface{}) ([]int64, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	rowIDs, exists := idx.data[key]
	return rowIDs, exists
}

// FindRange 哈希索引不支持范围查询
func (idx *HashIndex) FindRange(min, max interface{}) ([]int64, error) {
	return nil, fmt.Errorf("hash index does not support range queries")
}

// GetIndexInfo 获取索引信息
func (idx *HashIndex) GetIndexInfo() *IndexInfo {
	return idx.info
}

// ==================== 全文索引实现 ====================

// InvertedIndex 倒排索引
type InvertedIndex struct {
	tokens map[string][]int64
	mu     sync.RWMutex
}

// NewInvertedIndex 创建倒排索引
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		tokens: make(map[string][]int64),
	}
}

// AddDocument 添加文档到倒排索引
func (inv *InvertedIndex) AddDocument(docID int64, text string) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	// 简单分词：按空格分割
	tokens := tokenize(text)
	for _, token := range tokens {
		if token == "" {
			continue
		}
		inv.tokens[token] = append(inv.tokens[token], docID)
	}
}

// Search 在倒排索引搜索
func (inv *InvertedIndex) Search(query string) []int64 {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	queryTokens := tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	}

	// 取第一个token的结果
	results := make(map[int64]bool)
	for _, docID := range inv.tokens[queryTokens[0]] {
		results[docID] = true
	}

	// AND操作：取交集
	for _, token := range queryTokens[1:] {
		intersection := make(map[int64]bool)
		for _, docID := range inv.tokens[token] {
			if results[docID] {
				intersection[docID] = true
			}
		}
		results = intersection
	}

	// 转换为切片
	resultSlice := make([]int64, 0, len(results))
	for docID := range results {
		resultSlice = append(resultSlice, docID)
	}

	return resultSlice
}

// FullTextIndex 全文索引
type FullTextIndex struct {
	info     *IndexInfo
	inverted *InvertedIndex
	mu       sync.RWMutex
}

// NewFullTextIndex 创建全文索引
func NewFullTextIndex(tableName, columnName string) *FullTextIndex {
	return &FullTextIndex{
		info: &IndexInfo{
			Name:      fmt.Sprintf("idx_ft_%s_%s", tableName, columnName),
			TableName: tableName,
			Column:    columnName,
			Type:      IndexTypeFullText,
			Unique:    false,
		},
		inverted: NewInvertedIndex(),
	}
}

// Insert 插入文本到全文索引
func (idx *FullTextIndex) Insert(key interface{}, rowIDs []int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// key应该是字符串类型
	text, ok := key.(string)
	if !ok {
		return fmt.Errorf("full-text index requires string key, got %T", key)
	}

	for _, rowID := range rowIDs {
		idx.inverted.AddDocument(rowID, text)
	}

	return nil
}

// Delete 从全文索引删除键值
func (idx *FullTextIndex) Delete(key interface{}) error {
	// 全文索引不支持删除单个键
	// 实际实现应该维护反向映射
	return fmt.Errorf("full-text index delete not implemented")
}

// Find 在全文索引查找
func (idx *FullTextIndex) Find(key interface{}) ([]int64, bool) {
	query, ok := key.(string)
	if !ok {
		return nil, false
	}

	results := idx.inverted.Search(query)
	return results, len(results) > 0
}

// FindRange 全文索引不支持范围查询
func (idx *FullTextIndex) FindRange(min, max interface{}) ([]int64, error) {
	return nil, fmt.Errorf("full-text index does not support range queries")
}

// GetIndexInfo 获取索引信息
func (idx *FullTextIndex) GetIndexInfo() *IndexInfo {
	return idx.info
}

// ==================== 辅助函数 ====================

// tokenize 分词函数
func tokenize(text string) []string {
	// 简单实现：按空格、制表符分割
	text = strings.ToLower(text)
	text = strings.ReplaceAll(text, "\t", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	
	parts := strings.Split(text, " ")
	tokens := make([]string, 0, len(parts))
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			tokens = append(tokens, part)
		}
	}
	
	return tokens
}

// compareKeys 比较两个键（用于范围查询）
func compareKeys(a, b interface{}) int {
	switch va := a.(type) {
	case int:
		vb := b.(int)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case int64:
		vb := b.(int64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case float64:
		vb := b.(float64)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb := b.(string)
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	}
	return 0
}
