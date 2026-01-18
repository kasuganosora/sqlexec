package resource

import (
	"context"
	"fmt"
	"sync"
)

// MemorySource 内存数据源实现
type MemorySource struct {
	config    *DataSourceConfig
	connected bool
	writable  bool
	mu        sync.RWMutex
	tables    map[string]*TableInfo
	data      map[string][]Row
	autoID    map[string]int64
	// 约束管理
	foreignKeys       map[string]map[string]*ForeignKeyInfo      // tableName -> columnName -> ForeignKeyInfo
	uniqueConstraints map[string]map[string]bool                 // tableName -> columnName -> isUnique
	uniqueValues      map[string]map[string]map[interface{}]bool // tableName -> columnName -> value -> exists
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
		config:    config,
		writable:  writable,
		tables:    make(map[string]*TableInfo),
		data:      make(map[string][]Row),
		autoID:    make(map[string]int64),
		foreignKeys: make(map[string]map[string]*ForeignKeyInfo),
		uniqueConstraints: make(map[string]map[string]bool),
		uniqueValues: make(map[string]map[string]map[interface{}]bool),
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
		// 处理自增ID和默认值
		newRow := make(Row)
		for k, v := range row {
			newRow[k] = v
		}

		// 应用默认值和自增ID
		for _, col := range table.Columns {
			if _, exists := newRow[col.Name]; !exists {
				// 检查是否是自增列
				if col.AutoIncrement {
					s.autoID[tableName]++
					newRow[col.Name] = s.autoID[tableName]
				} else if col.Default != "" {
					// 应用默认值
					newRow[col.Name] = col.Default
				} else if !col.Nullable {
					// 非空列且没有值，报错
					return 0, fmt.Errorf("column %s does not allow NULL and no value provided", col.Name)
				}
			}
		}

		// 验证唯一约束
		if err := s.validateUniqueConstraint(tableName, newRow); err != nil {
			return 0, err
		}

		// 验证外键约束
		if err := s.validateForeignKeyConstraint(tableName, newRow); err != nil {
			return 0, err
		}

		// 添加到数据
		data = append(data, newRow)

		// 更新唯一约束的值跟踪
		s.updateUniqueValues(tableName, newRow, true, nil)
		inserted++
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
		// 验证新值
		for k, v := range updates {
			// 检查唯一约束
			if err := s.validateUniqueConstraintOnUpdate(tableName, k, v, idx, rows); err != nil {
				return 0, err
			}
			// 检查外键约束
			testRow := make(Row)
			for key, val := range rows[idx] {
				testRow[key] = val
			}
			testRow[k] = v
			if err := s.validateForeignKeyConstraint(tableName, testRow); err != nil {
				return 0, err
			}
		}

		// 更新行
		for k, v := range updates {
			rows[idx][k] = v
		}
		updated++
	}

	s.data[tableName] = rows

	// 更新唯一约束的值跟踪
	for _, idx := range matchedIndices {
		s.updateUniqueValues(tableName, rows[idx], false, &rows[idx])
	}

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

	// 检查外键RESTRICT约束
	if err := s.checkRestrictConstraints(tableName, rows, matchedIndices); err != nil {
		return 0, err
	}

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

// checkRestrictConstraints 检查删除操作是否违反RESTRICT外键约束
func (s *MemorySource) checkRestrictConstraints(tableName string, rows []Row, indices []int) error {
	// 获取表的主键列
	table, ok := s.tables[tableName]
	if !ok {
		return nil
	}

	// 查找主键列
	var pkColumn string
	for _, col := range table.Columns {
		if col.Primary {
			pkColumn = col.Name
			break
		}
	}

	// 如果没有主键，无法检查外键约束
	if pkColumn == "" {
		return nil
	}

	// 检查所有引用该表的外键
	for refTableName, fks := range s.foreignKeys {
		for colName, fk := range fks {
			if fk.Table == tableName && fk.Column == pkColumn {
				// 找到引用此表的外键，检查是否有数据引用
				refRows, ok := s.data[refTableName]
				if !ok {
					continue
				}

				for _, row := range refRows {
					if refValue, exists := row[colName]; exists {
						// 检查是否引用了要删除的记录
						for _, idx := range indices {
							delRow := rows[idx]
							if pkValue, pkExists := delRow[pkColumn]; pkExists {
								if compareEqual(pkValue, refValue) {
									// 检查外键的删除策略
									if fk.OnDelete == "RESTRICT" || fk.OnDelete == "" {
										return fmt.Errorf("cannot delete row: foreign key constraint restrict: referenced by %s.%s", refTableName, colName)
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
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

	// 初始化约束管理
	s.initializeConstraints(tableInfo.Name, cols)

	return nil
}

// initializeConstraints 初始化表的约束
func (s *MemorySource) initializeConstraints(tableName string, columns []ColumnInfo) {
	s.foreignKeys[tableName] = make(map[string]*ForeignKeyInfo)
	s.uniqueConstraints[tableName] = make(map[string]bool)
	s.uniqueValues[tableName] = make(map[string]map[interface{}]bool)

	for _, col := range columns {
		// 注册唯一约束
		if col.Unique {
			s.uniqueConstraints[tableName][col.Name] = true
			s.uniqueValues[tableName][col.Name] = make(map[interface{}]bool)
		}

		// 注册外键
		if col.ForeignKey != nil {
			s.foreignKeys[tableName][col.Name] = col.ForeignKey
		}
	}
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
	delete(s.foreignKeys, tableName)
	delete(s.uniqueConstraints, tableName)
	delete(s.uniqueValues, tableName)

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

	// 清空唯一约束的值跟踪
	if colValues, ok := s.uniqueValues[tableName]; ok {
		for _, valMap := range colValues {
			for key := range valMap {
				delete(valMap, key)
			}
		}
	}

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
	if options == nil {
		return rows
	}
	return ApplyPagination(rows, options.Offset, options.Limit)
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

// validateUniqueConstraint 验证唯一约束
func (s *MemorySource) validateUniqueConstraint(tableName string, row Row) error {
	tableCols, ok := s.uniqueConstraints[tableName]
	if !ok {
		return nil // 没有唯一约束
	}

	colValues, ok := s.uniqueValues[tableName]
	if !ok {
		return nil
	}

	for colName, isUnique := range tableCols {
		if isUnique {
			value, exists := row[colName]
			if !exists {
				continue // NULL值不违反唯一约束
			}

			// 检查值是否已存在
			if colValues[colName][value] {
				return fmt.Errorf("unique constraint violation: column %s value %v already exists", colName, value)
			}
		}
	}

	return nil
}

// updateUniqueValues 更新唯一约束的值跟踪
func (s *MemorySource) updateUniqueValues(tableName string, row Row, isInsert bool, oldRow *Row) {
	tableCols, ok := s.uniqueConstraints[tableName]
	if !ok {
		return
	}

	colValues, ok := s.uniqueValues[tableName]
	if !ok {
		return
	}

	for colName, isUnique := range tableCols {
		if isUnique {
			value, exists := row[colName]
			if !exists {
				continue
			}

			// 如果是更新，先删除旧值
			if !isInsert && oldRow != nil {
				oldValue, oldExists := (*oldRow)[colName]
				if oldExists {
					delete(colValues[colName], oldValue)
				}
			}

			// 添加新值
			colValues[colName][value] = true
		}
	}
}

// validateForeignKeyConstraint 验证外键约束
func (s *MemorySource) validateForeignKeyConstraint(tableName string, row Row) error {
	tableFKs, ok := s.foreignKeys[tableName]
	if !ok {
		return nil // 没有外键约束
	}

	for colName, fk := range tableFKs {
		value, exists := row[colName]
		if !exists {
			return nil // NULL值不违反外键约束
		}

		// 检查引用的表是否存在
		referencedTable, ok := s.tables[fk.Table]
		if !ok {
			return fmt.Errorf("foreign key constraint violation: referenced table %s does not exist", fk.Table)
		}

		// 检查引用的列是否存在
		referencedColExists := false
		for _, col := range referencedTable.Columns {
			if col.Name == fk.Column {
				referencedColExists = true
				break
			}
		}
		if !referencedColExists {
			return fmt.Errorf("foreign key constraint violation: referenced column %s.%s does not exist", fk.Table, fk.Column)
		}

		// 检查引用的值是否存在
		referencedRows := s.data[fk.Table]
		valueExists := false
		for _, refRow := range referencedRows {
			if refValue, refExists := refRow[fk.Column]; refExists {
				if compareEqual(refValue, value) {
					valueExists = true
					break
				}
			}
		}

		if !valueExists {
			// 根据策略返回错误
			if fk.OnDelete == "RESTRICT" || fk.OnDelete == "" {
				return fmt.Errorf("foreign key constraint violation: referenced value %v in table %s.%s does not exist", value, fk.Table, fk.Column)
			}
			// SET NULL 或 CASCADE 策略可以接受不存在的值（在实际删除时处理）
		}
	}

	return nil
}

// validateUniqueConstraintOnUpdate 更新时验证唯一约束
func (s *MemorySource) validateUniqueConstraintOnUpdate(tableName string, columnName string, newValue interface{}, currentRowIndex int, allRows []Row) error {
	tableCols, ok := s.uniqueConstraints[tableName]
	if !ok {
		return nil // 没有唯一约束
	}

	// 检查该列是否有唯一约束
	if !tableCols[columnName] {
		return nil // 该列没有唯一约束
	}

	// 获取当前行的旧值
	oldValue, oldExists := allRows[currentRowIndex][columnName]

	// 如果新旧值相同，不需要检查
	if oldExists && compareEqual(oldValue, newValue) {
		return nil
	}

	// 检查新值是否与其他行冲突
	for i, row := range allRows {
		if i == currentRowIndex {
			continue // 跳过当前行
		}

		if rowValue, exists := row[columnName]; exists {
			if compareEqual(rowValue, newValue) {
				return fmt.Errorf("unique constraint violation: column %s value %v already exists", columnName, newValue)
			}
		}
	}

	return nil
}

func init() {
	RegisterFactory(NewMemoryFactory())
}
