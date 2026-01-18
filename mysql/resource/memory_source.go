package resource

import (
	"context"
	"fmt"
	"sync"
)

// MemorySource 内存数据源实现
type MemorySource struct {
	config      *DataSourceConfig
	connected   bool
	writable    bool
	mu          sync.RWMutex
	tables      map[string]*TableInfo
	data        map[string][]Row
	autoID      map[string]int64
}

// MemoryFactory 内存数据源工厂
type MemoryFactory struct{}

// NewMemoryFactory 创建内存数据源工厂
func NewMemoryFactory() *MemoryFactory {
	return &MemoryFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *MemoryFactory) GetType() DataSourceType {
	return DataSourceTypeMemory
}

// Create 实现DataSourceFactory接口
func (f *MemoryFactory) Create(config *DataSourceConfig) (DataSource, error) {
	// 内存数据源默认可写
	writable := true
	if config != nil {
		writable = config.Writable
	}
	return &MemorySource{
		config:   config,
		writable: writable,
		tables:   make(map[string]*TableInfo),
		data:     make(map[string][]Row),
		autoID:   make(map[string]int64),
	}, nil
}

// Connect 连接数据源
func (s *MemorySource) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = true
	return nil
}

// Close 关闭连接
func (s *MemorySource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (s *MemorySource) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// GetConfig 获取数据源配置
func (s *MemorySource) GetConfig() *DataSourceConfig {
	return s.config
}

// IsWritable 检查是否可写
func (s *MemorySource) IsWritable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writable
}

// GetTables 获取所有表
func (s *MemorySource) GetTables(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	tables := make([]string, 0, len(s.tables))
	for name := range s.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// GetTableInfo 获取表信息
func (s *MemorySource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	table, ok := s.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return table, nil
}

// Query 查询数据
func (s *MemorySource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// 检查表是否存在
	table, ok := s.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	
	// 获取所有数据
	rows, ok := s.data[tableName]
	if !ok {
		rows = []Row{}
	}
	
	// 应用过滤器
	filteredRows := s.applyFilters(rows, options)
	
	// 应用排序
	sortedRows := s.applyOrder(filteredRows, options)
	
	// 应用分页
	pagedRows := s.applyPagination(sortedRows, options)

	// Total应该是返回的行数，而不是过滤前的总行数
	total := int64(len(pagedRows))

	return &QueryResult{
		Columns: table.Columns,
		Rows:    pagedRows,
		Total:   total,
	}, nil
}

// Insert 插入数据
func (s *MemorySource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 检查是否可写
	if !s.writable {
		return 0, fmt.Errorf("data source is read-only")
	}
	
	// 检查表是否存在
	table, ok := s.tables[tableName]
	if !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	
	// 获取或初始化数据
	data, ok := s.data[tableName]
	if !ok {
		data = []Row{}
	}
	
	inserted := int64(0)
	for _, row := range rows {
		// 处理自增ID
		newRow := make(Row)
		for k, v := range row {
			newRow[k] = v
		}
		
		// 查找主键列并处理自增
		for _, col := range table.Columns {
			if col.Primary {
				if _, exists := newRow[col.Name]; !exists {
					s.autoID[tableName]++
					newRow[col.Name] = s.autoID[tableName]
				}
			}
		}
		
		// 如果设置了Replace选项，检查是否已存在
		if options != nil && options.Replace {
			// 简化实现：直接追加
			data = append(data, newRow)
			inserted++
		} else {
			// 直接追加
			data = append(data, newRow)
			inserted++
		}
	}
	
	s.data[tableName] = data
	return inserted, nil
}

// Update 更新数据
func (s *MemorySource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 检查是否可写
	if !s.writable {
		return 0, fmt.Errorf("data source is read-only")
	}
	
	// 检查表是否存在
	if _, ok := s.tables[tableName]; !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	
	// 获取数据
	rows, ok := s.data[tableName]
	if !ok {
		rows = []Row{}
	}
	
	updated := int64(0)
	queryOpts := &QueryOptions{Filters: filters}
	matchedIndices := s.findMatchedRows(rows, queryOpts)
	
	for _, idx := range matchedIndices {
		for k, v := range updates {
			rows[idx][k] = v
		}
		updated++
	}
	
	s.data[tableName] = rows
	return updated, nil
}

// Delete 删除数据
func (s *MemorySource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 检查是否可写
	if !s.writable {
		return 0, fmt.Errorf("data source is read-only")
	}
	
	// 检查表是否存在
	if _, ok := s.tables[tableName]; !ok {
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	
	// 获取数据
	rows, ok := s.data[tableName]
	if !ok {
		rows = []Row{}
	}
	
	// 如果没有过滤器且没有Force选项，报错
	if len(filters) == 0 && (options == nil || !options.Force) {
		return 0, fmt.Errorf("delete operation requires filters or force option")
	}
	
	queryOpts := &QueryOptions{Filters: filters}
	matchedIndices := s.findMatchedRows(rows, queryOpts)
	
	// 从后往前删除，避免索引错位
	deleted := int64(0)
	for i := len(matchedIndices) - 1; i >= 0; i-- {
		idx := matchedIndices[i]
		rows = append(rows[:idx], rows[idx+1:]...)
		deleted++
	}
	
	s.data[tableName] = rows
	return deleted, nil
}

// CreateTable 创建表
func (s *MemorySource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, ok := s.tables[tableInfo.Name]; ok {
		return fmt.Errorf("table %s already exists", tableInfo.Name)
	}
	
	// 深拷贝表信息
	cols := make([]ColumnInfo, len(tableInfo.Columns))
	copy(cols, tableInfo.Columns)
	
	s.tables[tableInfo.Name] = &TableInfo{
		Name:    tableInfo.Name,
		Schema:  tableInfo.Schema,
		Columns: cols,
	}
	s.data[tableInfo.Name] = []Row{}
	
	// 初始化自增ID
	s.autoID[tableInfo.Name] = 0
	
	return nil
}

// DropTable 删除表
func (s *MemorySource) DropTable(ctx context.Context, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, ok := s.tables[tableName]; !ok {
		return fmt.Errorf("table %s not found", tableName)
	}
	
	delete(s.tables, tableName)
	delete(s.data, tableName)
	delete(s.autoID, tableName)
	
	return nil
}

// TruncateTable 清空表
func (s *MemorySource) TruncateTable(ctx context.Context, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, ok := s.tables[tableName]; !ok {
		return fmt.Errorf("table %s not found", tableName)
	}
	
	s.data[tableName] = []Row{}
	s.autoID[tableName] = 0
	
	return nil
}

// Execute 执行自定义SQL语句
func (s *MemorySource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	// 内存数据源不支持SQL执行
	return nil, fmt.Errorf("memory data source does not support SQL execution")
}

// applyFilters 应用过滤器
func (s *MemorySource) applyFilters(rows []Row, options *QueryOptions) []Row {
	if options == nil || len(options.Filters) == 0 {
		return rows
	}

	result := []Row{}
	for _, row := range rows {
		if s.matchesFilters(row, options.Filters) {
			result = append(result, row)
		}
	}
	return result
}

// matchesFilters 检查行是否匹配过滤器列表
func (s *MemorySource) matchesFilters(row Row, filters []Filter) bool {
	// 如果没有过滤器，所有行都匹配
	if len(filters) == 0 {
		return true
	}

	// 检查第一个过滤器
	filter := filters[0]

	// 如果有逻辑操作符
	if filter.LogicOp == "OR" || filter.LogicOp == "or" {
		// OR 操作：行的任何子过滤器匹配即可
		return s.matchesAnySubFilter(row, filter.SubFilters)
	}

	// 如果有 AND 逻辑操作符
	if filter.LogicOp == "AND" || filter.LogicOp == "and" {
		// AND 操作：行的所有子过滤器都必须匹配
		return s.matchesAllSubFilters(row, filter.SubFilters)
	}

	// 默认（AND 逻辑）：所有过滤器都必须匹配
	for _, f := range filters {
		if !s.matchFilter(row, f) {
			return false
		}
	}
	return true
}

// matchesAnySubFilter 检查行是否匹配任意子过滤器（OR 逻辑）
func (s *MemorySource) matchesAnySubFilter(row Row, subFilters []Filter) bool {
	// 如果没有子过滤器，返回 true
	if len(subFilters) == 0 {
		return true
	}
	// 检查是否有子过滤器匹配
	for _, subFilter := range subFilters {
		if s.matchFilter(row, subFilter) {
			return true
		}
	}
	return false
}

// matchesAllSubFilters 检查行是否匹配所有子过滤器（AND 逻辑）
func (s *MemorySource) matchesAllSubFilters(row Row, subFilters []Filter) bool {
	// 如果没有子过滤器，返回 true
	if len(subFilters) == 0 {
		return true
	}
	// 检查是否所有子过滤器都匹配
	for _, subFilter := range subFilters {
		if !s.matchFilter(row, subFilter) {
			return false
		}
	}
	return true
}

// matchFilter 匹配过滤器
func (s *MemorySource) matchFilter(row Row, filter Filter) bool {
	value, exists := row[filter.Field]
	if !exists {
		return false
	}

	switch filter.Operator {
	case "=":
		return compareEqual(value, filter.Value)
	case "!=":
		return !compareEqual(value, filter.Value)
	case ">":
		return compareGreater(value, filter.Value)
	case "<":
		return !compareGreater(value, filter.Value) && !compareEqual(value, filter.Value)
	case ">=":
		return compareGreater(value, filter.Value) || compareEqual(value, filter.Value)
	case "<=":
		return !compareGreater(value, filter.Value)
	case "LIKE":
		return compareLike(value, filter.Value)
	case "NOT LIKE":
		return !compareLike(value, filter.Value)
	case "IN":
		return compareIn(value, filter.Value)
	case "NOT IN":
		return !compareIn(value, filter.Value)
	case "BETWEEN":
		return compareBetween(value, filter.Value)
	case "NOT BETWEEN":
		return !compareBetween(value, filter.Value)
	default:
		return false
	}
}

// applyOrder 应用排序
func (s *MemorySource) applyOrder(rows []Row, options *QueryOptions) []Row {
	if options == nil || options.OrderBy == "" {
		return rows
	}
	
	result := make([]Row, len(rows))
	copy(result, rows)
	
	// 简化的排序实现
	order := options.Order
	if order == "" {
		order = "ASC"
	}
	
	// 这里应该实现更复杂的排序逻辑
	// 为简化，仅返回原始顺序
	return result
}

// applyPagination 应用分页
func (s *MemorySource) applyPagination(rows []Row, options *QueryOptions) []Row {
	if options == nil || options.Limit <= 0 {
		return rows
	}
	
	start := options.Offset
	if start < 0 {
		start = 0
	}
	if start >= len(rows) {
		return []Row{}
	}
	
	end := start + options.Limit
	if end > len(rows) {
		end = len(rows)
	}
	
	result := make([]Row, 0, options.Limit)
	for i := start; i < end; i++ {
		result = append(result, rows[i])
	}
	return result
}

// findMatchedRows 查找匹配的行索引
func (s *MemorySource) findMatchedRows(rows []Row, options *QueryOptions) []int {
	matched := []int{}
	for i, row := range rows {
		match := true
		for _, filter := range options.Filters {
			if !s.matchFilter(row, filter) {
				match = false
				break
			}
		}
		if match {
			matched = append(matched, i)
		}
	}
	return matched
}

// 比较辅助函数
// compareEqual 比较两个值是否相等
func compareEqual(a, b interface{}) bool {
	// 直接类型断言比较
	// int vs int64
	if aInt, aOk := a.(int); aOk {
		if bInt, bOk := b.(int); bOk {
			return aInt == bInt
		}
	}
	// int64 vs int64
	if aInt64, aOk := a.(int64); aOk {
		if bInt64, bOk := b.(int64); bOk {
			return aInt64 == bInt64
		}
	}
	// int64 vs int
	if aInt64, aOk := a.(int64); aOk {
		if bInt, bOk := b.(int); bOk {
			return aInt64 == int64(bInt)
		}
	}
	// int vs int64
	if aInt, aOk := a.(int); aOk {
		if bInt64, bOk := b.(int64); bOk {
			return int64(aInt) == bInt64
		}
	}
	
	// 尝试数值比较
	if cmp, ok := compareNumeric(a, b); ok {
		return cmp == 0
	}
	
	// 降级到字符串比较
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func compareGreater(a, b interface{}) bool {
	// 尝试数值比较
	if cmp, ok := compareNumeric(a, b); ok {
		return cmp > 0
	}
	// 降级到字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr > bStr
}

func compareLike(a, b interface{}) bool {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	
	// 简化实现：只支持 * 通配符
	pattern := ""
	for _, ch := range bStr {
		if ch == '*' {
			pattern += ".*"
		} else if ch == '%' {
			pattern += ".*"
		} else if ch == '_' {
			pattern += "."
		} else {
			pattern += string(ch)
		}
	}
	
	// 这里应该使用正则表达式匹配
	// 简化实现：使用简单的包含匹配
	return contains(aStr, bStr)
}

func compareIn(a, b interface{}) bool {
	values, ok := b.([]interface{})
	if !ok {
		return false
	}

	for _, v := range values {
		if compareEqual(a, v) {
			return true
		}
	}
	return false
}

// compareBetween 检查值是否在范围内
func compareBetween(a, b interface{}) bool {
	// b 应该是一个包含两个元素的数组 [min, max]
	slice, ok := b.([]interface{})
	if !ok || len(slice) < 2 {
		return false
	}

	min := slice[0]
	max := slice[1]

	// 对于字符串，使用字符串比较
	aStr := fmt.Sprintf("%v", a)
	minStr := fmt.Sprintf("%v", min)
	maxStr := fmt.Sprintf("%v", max)

	// 对于数值，使用数值比较
	if cmp, ok := compareNumeric(a, min); ok && cmp >= 0 {
		if cmpMax, okMax := compareNumeric(a, max); okMax && cmpMax <= 0 {
			return true
		}
	}

	// 降级到字符串比较：min <= a <= max
	return (aStr >= minStr) && (aStr <= maxStr)
}

// convertToFloat64 将值转换为 float64 进行数值比较
func convertToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// compareNumeric 数值比较
func compareNumeric(a, b interface{}) (int, bool) {
	aFloat, okA := convertToFloat64(a)
	bFloat, okB := convertToFloat64(b)
	if !okA || !okB {
		return 0, false
	}

	if aFloat < bFloat {
		return -1, true
	} else if aFloat > bFloat {
		return 1, true
	}
	return 0, true
}

func contains(s, substr string) bool {
	// 简化实现：将 % 替换为 *
	substr = replaceAll(substr, "%", "*")
	
	if substr == "*" {
		return true
	}
	
	if len(substr) >= 2 && substr[0] == '*' && substr[len(substr)-1] == '*' {
		pattern := substr[1 : len(substr)-1]
		return containsSimple(s, pattern)
	}
	
	if len(substr) >= 1 && substr[0] == '*' {
		pattern := substr[1:]
		return endsWith(s, pattern)
	}
	
	if len(substr) >= 1 && substr[len(substr)-1] == '*' {
		pattern := substr[:len(substr)-1]
		return startsWith(s, pattern)
	}
	
	return s == substr
}

func replaceAll(s, old, new string) string {
	result := ""
	for _, ch := range s {
		if string(ch) == old {
			result += new
		} else {
			result += string(ch)
		}
	}
	return result
}

func startsWith(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

func endsWith(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}

func containsSimple(s, substr string) bool {
	return findSubstring(s, substr) != -1
}

func findSubstring(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func init() {
	RegisterFactory(NewMemoryFactory())
}
