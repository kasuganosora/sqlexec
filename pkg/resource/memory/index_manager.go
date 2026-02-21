package memory

import (
	"fmt"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IndexManager 索引管理器
type IndexManager struct {
	tables map[string]*TableIndexes
	mu     sync.RWMutex
}

// TableIndexes 单个表的索引集合
type TableIndexes struct {
	tableName     string
	indexes       map[string]Index       // indexName -> Index（传统索引）
	columnMap     map[string]Index       // columnName -> Index（传统索引快速查找）
	vectorIndexes map[string]VectorIndex // columnName -> VectorIndex（向量索引）
	mu            sync.RWMutex
}

// NewIndexManager 创建索引管理器
func NewIndexManager() *IndexManager {
	return &IndexManager{
		tables: make(map[string]*TableIndexes),
		mu:     sync.RWMutex{},
	}
}

// CreateIndex creates a single-column index (convenience wrapper for CreateIndexWithColumns)
// This method is kept for internal use and testing
func (m *IndexManager) CreateIndex(tableName, columnName string, indexType IndexType, unique bool) (Index, error) {
	return m.CreateIndexWithColumns(tableName, []string{columnName}, indexType, unique)
}

// CreateIndexWithColumns creates an index on one or more columns (composite index support)
func (m *IndexManager) CreateIndexWithColumns(tableName string, columnNames []string, indexType IndexType, unique bool) (Index, error) {
	if len(columnNames) == 0 {
		return nil, fmt.Errorf("at least one column is required for index")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		tableIdxs = &TableIndexes{
			tableName:     tableName,
			indexes:       make(map[string]Index),
			columnMap:     make(map[string]Index),
			vectorIndexes: make(map[string]VectorIndex),
			mu:            sync.RWMutex{},
		}
		m.tables[tableName] = tableIdxs
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	// For single column, check if column already has index
	if len(columnNames) == 1 {
		if _, exists := tableIdxs.columnMap[columnNames[0]]; exists {
			return nil, fmt.Errorf("index already exists for column: %s", columnNames[0])
		}
	}

	// Create index based on type
	var idx Index
	switch indexType {
	case IndexTypeBTree:
		if len(columnNames) == 1 {
			idx = NewBTreeIndex(tableName, columnNames[0], unique)
		} else {
			idx = NewBTreeIndexComposite(tableName, columnNames, unique)
		}
	case IndexTypeHash:
		if len(columnNames) == 1 {
			idx = NewHashIndex(tableName, columnNames[0], unique)
		} else {
			idx = NewHashIndexComposite(tableName, columnNames, unique)
		}
	case IndexTypeFullText:
		// FullText index only supports single column
		if len(columnNames) > 1 {
			return nil, fmt.Errorf("fulltext index does not support multiple columns")
		}
		idx = NewFullTextIndex(tableName, columnNames[0])
	case IndexTypeSpatialRTree:
		// Spatial index only supports single column
		if len(columnNames) > 1 {
			return nil, fmt.Errorf("spatial index does not support multiple columns")
		}
		idx = NewRTreeIndex(tableName, columnNames[0])
	default:
		return nil, fmt.Errorf("unsupported index type: %s", indexType)
	}

	// Store index
	info := idx.GetIndexInfo()
	tableIdxs.indexes[info.Name] = idx

	// Map first column for quick lookup (backward compatibility)
	tableIdxs.columnMap[columnNames[0]] = idx

	return idx, nil
}

// CreateVectorIndex 创建向量索引
func (m *IndexManager) CreateVectorIndex(
	tableName, columnName string,
	metricType VectorMetricType,
	indexType IndexType,
	dimension int,
	params map[string]interface{},
) (VectorIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		tableIdxs = &TableIndexes{
			tableName:     tableName,
			indexes:       make(map[string]Index),
			columnMap:     make(map[string]Index),
			vectorIndexes: make(map[string]VectorIndex),
			mu:            sync.RWMutex{},
		}
		m.tables[tableName] = tableIdxs
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	// 检查列是否已有向量索引
	if _, exists := tableIdxs.vectorIndexes[columnName]; exists {
		return nil, fmt.Errorf("vector index already exists for column: %s", columnName)
	}

	// 创建索引配置
	config := &VectorIndexConfig{
		MetricType: metricType,
		Dimension:  dimension,
		Params:     params,
	}

	// 根据类型创建向量索引
	var idx VectorIndex
	var err error
	switch indexType {
	case IndexTypeVectorHNSW:
		idx, err = NewHNSWIndex(columnName, config)
	case IndexTypeVectorFlat:
		idx, err = NewFlatIndex(columnName, config)
	case IndexTypeVectorIVFFlat:
		idx, err = NewIVFFlatIndex(columnName, config)
	case IndexTypeVectorIVFSQ8:
		idx, err = NewIVFSQ8Index(columnName, config)
	case IndexTypeVectorIVFPQ:
		idx, err = NewIVFPQIndex(columnName, config)
	case IndexTypeVectorHNSWSQ:
		idx, err = NewHNSWSQIndex(columnName, config)
	case IndexTypeVectorHNSWPQ:
		idx, err = NewHNSWPQIndex(columnName, config)
	case IndexTypeVectorIVFRabitQ:
		idx, err = NewIVFRabitQIndex(columnName, config)
	case IndexTypeVectorHNSWPRQ:
		idx, err = NewHNSWPRQIndex(columnName, config)
	case IndexTypeVectorAISAQ:
		idx, err = NewAISAQIndex(columnName, config)
	default:
		return nil, fmt.Errorf("unsupported vector index type: %s", indexType)
	}

	if err != nil {
		return nil, err
	}

	// 存储索引
	tableIdxs.vectorIndexes[columnName] = idx

	return idx, nil
}

// GetVectorIndex 获取向量索引
func (m *IndexManager) GetVectorIndex(tableName, columnName string) (VectorIndex, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	tableIdxs.mu.RLock()
	defer tableIdxs.mu.RUnlock()

	idx, exists := tableIdxs.vectorIndexes[columnName]
	if !exists {
		return nil, fmt.Errorf("vector index not found for column: %s", columnName)
	}

	return idx, nil
}

// DropVectorIndex 删除向量索引
func (m *IndexManager) DropVectorIndex(tableName, columnName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	idx, exists := tableIdxs.vectorIndexes[columnName]
	if !exists {
		return fmt.Errorf("vector index not found for column: %s", columnName)
	}

	// 关闭索引
	_ = idx.Close()
	delete(tableIdxs.vectorIndexes, columnName)

	return nil
}

// GetIndex 获取指定列的索引
func (m *IndexManager) GetIndex(tableName, columnName string) (Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	tableIdxs.mu.RLock()
	defer tableIdxs.mu.RUnlock()

	idx, exists := tableIdxs.columnMap[columnName]
	if !exists {
		return nil, fmt.Errorf("index not found for column: %s", columnName)
	}

	return idx, nil
}

// DropIndex 删除索引
func (m *IndexManager) DropIndex(tableName, indexName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	delete(tableIdxs.indexes, indexName)

	// 从columnMap中移除
	for col, idx := range tableIdxs.columnMap {
		if idx.GetIndexInfo().Name == indexName {
			delete(tableIdxs.columnMap, col)
			break
		}
	}

	// 重要：只删除索引元数据，不应该影响表数据
	// 确保不会删除表本身
	if _, ok := m.tables[tableName]; !ok {
		return fmt.Errorf("unexpected: table %s was deleted", tableName)
	}

	return nil
}

// DropTableIndexes 删除表的所有索引
func (m *IndexManager) DropTableIndexes(tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return nil
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	// 关闭所有向量索引
	for _, idx := range tableIdxs.vectorIndexes {
		_ = idx.Close()
	}

	delete(m.tables, tableName)
	return nil
}

// RebuildIndex 重建索引
func (m *IndexManager) RebuildIndex(tableName string, schema *domain.TableInfo, rows []domain.Row) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return fmt.Errorf("table not found: %s", tableName)
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	// 清空所有索引
	for _, idx := range tableIdxs.indexes {
		switch idx := idx.(type) {
		case *BTreeIndex:
			// 重置B+ Tree
			idx.mu.Lock()
			idx.root = NewBTreeNode(true)
			idx.height = 1
			idx.data = make(map[interface{}][]int64)
			idx.mu.Unlock()
		case *HashIndex:
			// 重置Hash索引
			idx.mu.Lock()
			idx.data = make(map[interface{}][]int64)
			idx.mu.Unlock()
		case *FullTextIndex:
			// 重置全文索引
			idx.mu.Lock()
			idx.inverted = NewInvertedIndex()
			idx.mu.Unlock()
		case *RTreeIndex:
			// 重置R-Tree空间索引
			idx.Reset()
		}
	}

	// 重建索引
	for i, row := range rows {
		rowID := int64(i + 1)
		for columnName, value := range row {
			if idx, ok := tableIdxs.columnMap[columnName]; ok {
				_ = idx.Insert(value, []int64{rowID})
			}
		}
	}

	return nil
}

// GetTableIndexes 获取表的所有索引信息
func (m *IndexManager) GetTableIndexes(tableName string) ([]*IndexInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	tableIdxs.mu.RLock()
	defer tableIdxs.mu.RUnlock()

	infos := make([]*IndexInfo, 0, len(tableIdxs.indexes))
	for _, idx := range tableIdxs.indexes {
		infos = append(infos, idx.GetIndexInfo())
	}

	return infos, nil
}

// CreateAdvancedFullTextIndex 创建高级全文索引
func (m *IndexManager) CreateAdvancedFullTextIndex(
	tableName, columnName string,
	config *AdvancedFullTextIndexConfig,
) (*AdvancedFullTextIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		tableIdxs = &TableIndexes{
			tableName:     tableName,
			indexes:       make(map[string]Index),
			columnMap:     make(map[string]Index),
			vectorIndexes: make(map[string]VectorIndex),
			mu:            sync.RWMutex{},
		}
		m.tables[tableName] = tableIdxs
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	// 检查列是否已有索引
	if _, exists := tableIdxs.columnMap[columnName]; exists {
		return nil, fmt.Errorf("index already exists for column: %s", columnName)
	}

	// 创建高级全文索引
	idx, err := NewAdvancedFullTextIndex(tableName, columnName, config)
	if err != nil {
		return nil, err
	}

	// 存储索引
	tableIdxs.indexes[idx.GetIndexInfo().Name] = idx
	tableIdxs.columnMap[columnName] = idx

	return idx, nil
}

// GetFullTextIndex 获取全文索引
func (m *IndexManager) GetFullTextIndex(tableName, columnName string) (*AdvancedFullTextIndex, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	tableIdxs.mu.RLock()
	defer tableIdxs.mu.RUnlock()

	idx, exists := tableIdxs.columnMap[columnName]
	if !exists {
		return nil, fmt.Errorf("index not found for column: %s", columnName)
	}

	// 尝试转换为高级全文索引
	ftIdx, ok := idx.(*AdvancedFullTextIndex)
	if !ok {
		return nil, fmt.Errorf("index is not a full-text index")
	}

	return ftIdx, nil
}
