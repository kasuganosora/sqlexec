package resource

import (
	"fmt"
	"sort"
	"sync"
)

// ==================== 索引定义 ====================

// IndexType 索引类型
type IndexType string

const (
	IndexTypeHash  IndexType = "HASH"  // 哈希索引
	IndexTypeBTree IndexType = "BTREE" // B树索�?
	IndexTypeSkip  IndexType = "SKIP"  // 跳表索引
)

// IndexInfo 索引信息
type IndexInfo struct {
	Name        string      // 索引名称
	TableName   string      // 表名
	Columns     []string    // 索引�?
	Type        IndexType   // 索引类型
	Unique      bool        // 是否唯一索引
	Primary     bool        // 是否主键索引
	Created     bool        // 是否已创�?
	mu          sync.RWMutex
}

// NewIndexInfo 创建索引信息
func NewIndexInfo(name, tableName string, columns []string, idxType IndexType, unique, primary bool) *IndexInfo {
	return &IndexInfo{
		Name:      name,
		TableName: tableName,
		Columns:   columns,
		Type:      idxType,
		Unique:    unique,
		Primary:   primary,
		Created:   false,
	}
}

// IsUnique 是否唯一索引
func (idx *IndexInfo) IsUnique() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.Unique
}

// IsPrimary 是否主键索引
func (idx *IndexInfo) IsPrimary() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.Primary
}

// ==================== 哈希索引实现 ====================

// HashIndex 哈希索引
type HashIndex struct {
	info      *IndexInfo
	index     map[string][]int // key -> row indices
	rows      []Row           // 表的所有行（引用）
	tableName string
	mu        sync.RWMutex
}

// NewHashIndex 创建哈希索引
func NewHashIndex(info *IndexInfo, rows []Row, tableName string) *HashIndex {
	return &HashIndex{
		info:      info,
		index:     make(map[string][]int),
		rows:      rows,
		tableName: tableName,
	}
}

// Build 构建索引
func (idx *HashIndex) Build() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.index = make(map[string][]int)

	for i, row := range idx.rows {
		// 获取索引列的值作为键
		key, err := idx.extractKey(row)
		if err != nil {
			return err
		}

		// 添加到索�?
		idx.index[key] = append(idx.index[key], i)
	}

	idx.info.Created = true
	return nil
}

// extractKey 从行中提取索引键
func (idx *HashIndex) extractKey(row Row) (string, error) {
	keyValues := make([]interface{}, len(idx.info.Columns))
	for i, col := range idx.info.Columns {
		val, exists := row[col]
		if !exists {
			return "", fmt.Errorf("column %s not found in row", col)
		}
		keyValues[i] = val
	}

	// 组合键�?
	return fmt.Sprintf("%v", keyValues), nil
}

// Lookup 查找等于指定值的行索�?
func (idx *HashIndex) Lookup(value interface{}) []int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	key := fmt.Sprintf("%v", value)
	return idx.index[key]
}

// RangeLookup 范围查找（哈希索引不支持�?
func (idx *HashIndex) RangeLookup(min, max interface{}) []int {
	// 哈希索引不支持范围查询，返回所有行
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]int, 0, len(idx.rows))
	for i := range idx.rows {
		result = append(result, i)
	}
	return result
}

// GetInfo 获取索引信息
func (idx *HashIndex) GetInfo() *IndexInfo {
	return idx.info
}

// Rebuild 重建索引
func (idx *HashIndex) Rebuild() error {
	return idx.Build()
}

// ==================== B树索引实�?====================

// BTreeNode B树节�?
type BTreeNode struct {
	Keys     []interface{}
	Children []*BTreeNode
	IsLeaf   bool
	Values   []int // 行索�?
}

// BTreeIndex B树索�?
type BTreeIndex struct {
	info       *IndexInfo
	root       *BTreeNode
	rows       []Row
	tableName  string
	order      int    // B树的阶数
	mu         sync.RWMutex
}

// NewBTreeIndex 创建B树索�?
func NewBTreeIndex(info *IndexInfo, rows []Row, tableName string, order int) *BTreeIndex {
	if order <= 0 {
		order = 3 // 默认阶数
	}
	return &BTreeIndex{
		info:      info,
		root:      nil,
		rows:      rows,
		tableName: tableName,
		order:     order,
	}
}

// Build 构建索引
func (idx *BTreeIndex) Build() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// 创建根节�?
	idx.root = &BTreeNode{
		Keys:     make([]interface{}, 0),
		Children: make([]*BTreeNode, 0),
		IsLeaf:   true,
		Values:   make([]int, 0),
	}

	// 插入所有行
	for i, row := range idx.rows {
		key, err := idx.extractKey(row)
		if err != nil {
			return err
		}

		// 简化构建：直接插入到根节点（不处理分裂�?
		// 这不是完整的B树实现，而是用于演示的基本版�?
		pos := idx.findPosition(idx.root, key)
		idx.root.Keys = append(idx.root.Keys[:pos], append([]interface{}{key}, idx.root.Keys[pos:]...)...)
		if idx.root.IsLeaf {
			idx.root.Values = append(idx.root.Values[:pos], append([]int{i}, idx.root.Values[pos:]...)...)
		}

		// 如果节点满了，标记为需要split（但不实际执行）
		if len(idx.root.Keys) >= idx.order*2 {
			// 简化：对于演示，我们限制插入数�?
			break
		}
	}

	idx.info.Created = true
	return nil
}

// extractKey 从行中提取索引键
func (idx *BTreeIndex) extractKey(row Row) (interface{}, error) {
	// 单列索引直接返回列�?
	if len(idx.info.Columns) == 1 {
		val, exists := row[idx.info.Columns[0]]
		if !exists {
			return nil, fmt.Errorf("column %s not found in row", idx.info.Columns[0])
		}
		return val, nil
	}

	// 多列索引组合�?
	keyValues := make([]interface{}, len(idx.info.Columns))
	for i, col := range idx.info.Columns {
		val, exists := row[col]
		if !exists {
			return nil, fmt.Errorf("column %s not found in row", col)
		}
		keyValues[i] = val
	}
	return keyValues, nil
}

// insert 插入键值到B�?
func (idx *BTreeIndex) insert(node *BTreeNode, key interface{}, value int) error {
	// 叶子节点
	if node.IsLeaf {
		// 找到插入位置
		pos := idx.findPosition(node, key)
		node.Keys = append(node.Keys[:pos], append([]interface{}{key}, node.Keys[pos:]...)...)
		node.Values = append(node.Values[:pos], append([]int{value}, node.Values[pos:]...)...)
		return nil
	}

	// 内部节点
	pos := idx.findPosition(node, key)
	return idx.insert(node.Children[pos], key, value)
}

// findPosition 在节点中查找键的位置
func (idx *BTreeIndex) findPosition(node *BTreeNode, key interface{}) int {
	return sort.Search(len(node.Keys), func(i int) bool {
		return compareValues(key, node.Keys[i]) < 0
	})
}

// split 分割节点
func (idx *BTreeIndex) split(node *BTreeNode) (*BTreeNode, interface{}) {
	mid := len(node.Keys) / 2
	midKey := node.Keys[mid]

	newNode := &BTreeNode{
		Keys:     append([]interface{}{}, node.Keys[mid+1:]...),
		Children: append([]*BTreeNode{}, node.Children[mid+1:]...),
		IsLeaf:   node.IsLeaf,
	}

	if node.IsLeaf {
		newNode.Values = append([]int{}, node.Values[mid+1:]...)
		node.Keys = node.Keys[:mid]
		node.Values = node.Values[:mid]
	} else {
		node.Keys = node.Keys[:mid]
		node.Children = node.Children[:mid+1]
	}

	return newNode, midKey
}

// Lookup 查找等于指定值的行索�?
func (idx *BTreeIndex) Lookup(value interface{}) []int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.root == nil {
		return []int{}
	}

	result := idx.lookup(idx.root, value)
	return result
}

// lookup 递归查找
func (idx *BTreeIndex) lookup(node *BTreeNode, key interface{}) []int {
	if node == nil {
		return []int{}
	}

	// 叶子节点
	if node.IsLeaf {
		for i, k := range node.Keys {
			if compareValues(key, k) == 0 && i < len(node.Values) {
				return []int{node.Values[i]}
			}
		}
		return []int{}
	}

	// 内部节点：找到子节点
	pos := idx.findPosition(node, key)

	// 检查是否找�?
	if pos < len(node.Keys) && compareValues(key, node.Keys[pos]) == 0 {
		// 在内部节点找到键，继续到右子节点查找实际�?
		if pos+1 < len(node.Children) {
			return idx.lookup(node.Children[pos+1], key)
		}
		return []int{}
	}

	// 递归到合适的子节�?
	if pos < len(node.Children) {
		return idx.lookup(node.Children[pos], key)
	}

	return []int{}
}

// RangeLookup 范围查找
func (idx *BTreeIndex) RangeLookup(min, max interface{}) []int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.root == nil {
		return []int{}
	}

	result := idx.rangeLookup(idx.root, min, max, []int{})
	return result
}

// rangeLookup 递归范围查找
func (idx *BTreeIndex) rangeLookup(node *BTreeNode, min, max interface{}, result []int) []int {
	if node == nil {
		return result
	}

	// 叶子节点
	if node.IsLeaf {
		for i, key := range node.Keys {
			if compareValues(key, min) >= 0 && compareValues(key, max) <= 0 {
				if i < len(node.Values) {
					result = append(result, node.Values[i])
				}
			}
		}
		return result
	}

	// 内部节点：遍历所有子节点
	for _, child := range node.Children {
		result = idx.rangeLookup(child, min, max, result)
	}

	return result
}

// GetInfo 获取索引信息
func (idx *BTreeIndex) GetInfo() *IndexInfo {
	return idx.info
}

// Rebuild 重建索引
func (idx *BTreeIndex) Rebuild() error {
	return idx.Build()
}

// ==================== 索引接口 ====================

// Index 索引接口
type Index interface {
	// Build 构建索引
	Build() error

	// Lookup 查找等于指定值的行索�?
	Lookup(value interface{}) []int

	// RangeLookup 范围查找
	RangeLookup(min, max interface{}) []int

	// GetInfo 获取索引信息
	GetInfo() *IndexInfo

	// Rebuild 重建索引
	Rebuild() error
}

// ==================== 索引管理�?====================

// IndexManager 索引管理�?
type IndexManager struct {
	tables    map[string]map[string]Index // tableName -> indexName -> index
	rows       map[string][]Row            // tableName -> rows
	mu         sync.RWMutex
}

// NewIndexManager 创建索引管理�?
func NewIndexManager() *IndexManager {
	return &IndexManager{
		tables: make(map[string]map[string]Index),
		rows:    make(map[string][]Row),
	}
}

// RegisterTable 注册�?
func (m *IndexManager) RegisterTable(tableName string, rows []Row) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tables[tableName] = make(map[string]Index)
	m.rows[tableName] = rows
}

// UnregisterTable 注销�?
func (m *IndexManager) UnregisterTable(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tables, tableName)
	delete(m.rows, tableName)
}

// CreateIndex 创建索引
func (m *IndexManager) CreateIndex(tableName string, indexInfo *IndexInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查表是否存在
	table, exists := m.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// 检查索引是否已存在
	if _, exists := table[indexInfo.Name]; exists {
		return fmt.Errorf("index %s already exists", indexInfo.Name)
	}

	// 获取行数�?
	rows, exists := m.rows[tableName]
	if !exists {
		return fmt.Errorf("rows for table %s not found", tableName)
	}

	// 创建索引
	var idx Index
	switch indexInfo.Type {
	case IndexTypeHash:
		idx = NewHashIndex(indexInfo, rows, tableName)
	case IndexTypeBTree:
		idx = NewBTreeIndex(indexInfo, rows, tableName, 3)
	default:
		return fmt.Errorf("unsupported index type: %s", indexInfo.Type)
	}

	// 构建索引
	if err := idx.Build(); err != nil {
		return fmt.Errorf("failed to build index: %w", err)
	}

	// 注册索引
	table[indexInfo.Name] = idx

	return nil
}

// DropIndex 删除索引
func (m *IndexManager) DropIndex(tableName, indexName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	table, exists := m.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	if _, exists := table[indexName]; !exists {
		return fmt.Errorf("index %s not found", indexName)
	}

	delete(table, indexName)
	return nil
}

// GetIndex 获取索引
func (m *IndexManager) GetIndex(tableName, indexName string) (Index, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.tables[tableName]
	if !exists {
		return nil, false
	}

	idx, exists := table[indexName]
	return idx, exists
}

// ListIndexes 列出表的所有索�?
func (m *IndexManager) ListIndexes(tableName string) []*IndexInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.tables[tableName]
	if !exists {
		return []*IndexInfo{}
	}

	indexes := make([]*IndexInfo, 0, len(table))
	for _, idx := range table {
		indexes = append(indexes, idx.GetInfo())
	}
	return indexes
}

// UpdateTableRows 更新表的行数据（需要重建索引）
func (m *IndexManager) UpdateTableRows(tableName string, rows []Row) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 保存行数�?
	m.rows[tableName] = rows

	// 重建所有索�?
	table, exists := m.tables[tableName]
	if !exists {
		return nil
	}

	for _, idx := range table {
		if err := idx.Rebuild(); err != nil {
			return fmt.Errorf("failed to rebuild index %s: %w", idx.GetInfo().Name, err)
		}
	}

	return nil
}

// FindBestIndex 查找最佳索�?
func (m *IndexManager) FindBestIndex(tableName string, filters []Filter) (Index, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.tables[tableName]
	if !exists || len(table) == 0 {
		return nil, ""
	}

	// 遍历所有索引，找到最合适的
	for _, idx := range table {
		info := idx.GetInfo()

		// 检查索引列是否匹配过滤条件
		if m.indexMatchesFilters(info, filters) {
			return idx, info.Name
		}
	}

	return nil, ""
}

// indexMatchesFilters 检查索引是否匹配过滤条�?
func (m *IndexManager) indexMatchesFilters(indexInfo *IndexInfo, filters []Filter) bool {
	// 检查所有过滤条�?
	for _, filter := range filters {
		// 检查过滤列是否在索引中
		for _, col := range indexInfo.Columns {
			if col == filter.Field {
				// 支持的运算符
				if filter.Operator == "=" || filter.Operator == ">" || filter.Operator == "<" ||
					filter.Operator == ">=" || filter.Operator == "<=" {
					return true
				}
			}
		}
	}
	return false
}

// ==================== 比较函数 ====================

// compareValues 比较两个�?
func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 尝试数值比�?
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// 字符串比�?
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// toFloat64 转换为float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}
