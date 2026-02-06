package optimizer

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// HypotheticalIndexStore 虚拟索引存储
// 内存存储，不持久化，用于 What-If 分析
type HypotheticalIndexStore struct {
	mu          sync.RWMutex
	indexes     map[string]*HypotheticalIndex    // indexID -> index
	tableMap    map[string]map[string]bool       // tableName -> indexIDs
	nextIndexID int64
}

// NewHypotheticalIndexStore 创建虚拟索引存储
func NewHypotheticalIndexStore() *HypotheticalIndexStore {
	return &HypotheticalIndexStore{
		indexes:  make(map[string]*HypotheticalIndex),
		tableMap: make(map[string]map[string]bool),
	}
}

// CreateIndex 创建虚拟索引
func (s *HypotheticalIndexStore) CreateIndex(tableName string, columns []string, isUnique, isPrimary bool) (*HypotheticalIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查是否已存在相同的索引
	indexKey := s.buildIndexKey(tableName, columns)
	if idx, exists := s.findExistingIndex(tableName, columns); exists {
		return idx, fmt.Errorf("index already exists: %s", indexKey)
	}

	// 生成唯一 ID
	indexID := fmt.Sprintf("hyp_%d", s.nextIndexID)
	s.nextIndexID++

	// 创建虚拟索引
	index := &HypotheticalIndex{
		ID:        indexID,
		TableName: tableName,
		Columns:   columns,
		IsUnique:  isUnique,
		IsPrimary: isPrimary,
		CreatedAt: time.Now(),
	}

	// 存储索引
	s.indexes[indexID] = index

	// 更新表映射
	if _, ok := s.tableMap[tableName]; !ok {
		s.tableMap[tableName] = make(map[string]bool)
	}
	s.tableMap[tableName][indexID] = true

	return index, nil
}

// GetIndex 获取虚拟索引
func (s *HypotheticalIndexStore) GetIndex(indexID string) (*HypotheticalIndex, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	index, ok := s.indexes[indexID]
	return index, ok
}

// GetTableIndexes 获取表的所有虚拟索引
func (s *HypotheticalIndexStore) GetTableIndexes(tableName string) []*HypotheticalIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var indexes []*HypotheticalIndex
	if indexIDs, ok := s.tableMap[tableName]; ok {
		for indexID := range indexIDs {
			if idx, exists := s.indexes[indexID]; exists {
				indexes = append(indexes, idx)
			}
		}
	}

	return indexes
}

// DeleteIndex 删除虚拟索引
func (s *HypotheticalIndexStore) DeleteIndex(indexID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, ok := s.indexes[indexID]
	if !ok {
		return fmt.Errorf("index not found: %s", indexID)
	}

	// 从表映射中删除
	if indexIDs, ok := s.tableMap[index.TableName]; ok {
		delete(indexIDs, indexID)
		if len(indexIDs) == 0 {
			delete(s.tableMap, index.TableName)
		}
	}

	// 删除索引
	delete(s.indexes, indexID)

	return nil
}

// DeleteTableIndexes 删除表的所有虚拟索引
func (s *HypotheticalIndexStore) DeleteTableIndexes(tableName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	if indexIDs, ok := s.tableMap[tableName]; ok {
		for indexID := range indexIDs {
			delete(s.indexes, indexID)
			count++
		}
		delete(s.tableMap, tableName)
	}

	return count
}

// UpdateStats 更新索引统计信息
func (s *HypotheticalIndexStore) UpdateStats(indexID string, stats *HypotheticalIndexStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	index, ok := s.indexes[indexID]
	if !ok {
		return fmt.Errorf("index not found: %s", indexID)
	}

	index.Stats = stats
	return nil
}

// Clear 清空所有虚拟索引
func (s *HypotheticalIndexStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.indexes = make(map[string]*HypotheticalIndex)
	s.tableMap = make(map[string]map[string]bool)
}

// Count 返回索引总数
func (s *HypotheticalIndexStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.indexes)
}

// FindIndexByColumns 根据列查找索引
func (s *HypotheticalIndexStore) FindIndexByColumns(tableName string, columns []string) (*HypotheticalIndex, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.findExistingIndex(tableName, columns)
}

// ListAllIndexes 列出所有索引
func (s *HypotheticalIndexStore) ListAllIndexes() []*HypotheticalIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()

	indexes := make([]*HypotheticalIndex, 0, len(s.indexes))
	for _, index := range s.indexes {
		indexes = append(indexes, index)
	}

	return indexes
}

// findExistingIndex 内部方法：查找是否已存在相同的索引
func (s *HypotheticalIndexStore) findExistingIndex(tableName string, columns []string) (*HypotheticalIndex, bool) {
	indexIDs, ok := s.tableMap[tableName]
	if !ok {
		return nil, false
	}

	for indexID := range indexIDs {
		index := s.indexes[indexID]
		if s.columnsEqual(index.Columns, columns) {
			return index, true
		}
	}

	return nil, false
}

// buildIndexKey 构建索引键
func (s *HypotheticalIndexStore) buildIndexKey(tableName string, columns []string) string {
	var builder strings.Builder
	builder.WriteString(tableName)
	for _, col := range columns {
		builder.WriteString(":")
		builder.WriteString(col)
	}
	return builder.String()
}

// columnsEqual 比较列数组是否相等
func (s *HypotheticalIndexStore) columnsEqual(cols1, cols2 []string) bool {
	if len(cols1) != len(cols2) {
		return false
	}
	for i := range cols1 {
		if cols1[i] != cols2[i] {
			return false
		}
	}
	return true
}
