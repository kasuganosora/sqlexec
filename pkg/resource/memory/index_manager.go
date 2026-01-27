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
	tableName string
	indexes   map[string]Index   // columnName -> Index
	columnMap map[string]Index      // columnName -> Index
	mu        sync.RWMutex
}

// NewIndexManager 创建索引管理器
func NewIndexManager() *IndexManager {
	return &IndexManager{
		tables: make(map[string]*TableIndexes),
		mu:     sync.RWMutex{},
	}
}

// CreateIndex 创建索引
func (m *IndexManager) CreateIndex(tableName, columnName string, indexType IndexType, unique bool) (Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableIdxs, ok := m.tables[tableName]
	if !ok {
		tableIdxs = &TableIndexes{
			tableName: tableName,
			indexes:   make(map[string]Index),
			columnMap: make(map[string]Index),
			mu:        sync.RWMutex{},
		}
		m.tables[tableName] = tableIdxs
	}

	tableIdxs.mu.Lock()
	defer tableIdxs.mu.Unlock()

	// 检查列是否已有索引
	if _, exists := tableIdxs.columnMap[columnName]; exists {
		return nil, fmt.Errorf("index already exists for column: %s", columnName)
	}

	// 根据类型创建索引
	var idx Index
	switch indexType {
	case IndexTypeBTree:
		idx = NewBTreeIndex(tableName, columnName, unique)
	case IndexTypeHash:
		idx = NewHashIndex(tableName, columnName, unique)
	case IndexTypeFullText:
		idx = NewFullTextIndex(tableName, columnName)
	default:
		return nil, fmt.Errorf("unsupported index type: %s", indexType)
	}

	// 存储索引
	tableIdxs.indexes[idx.GetIndexInfo().Name] = idx
	tableIdxs.columnMap[columnName] = idx

	return idx, nil
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

	return nil
}

// DropTableIndexes 删除表的所有索引
func (m *IndexManager) DropTableIndexes(tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
