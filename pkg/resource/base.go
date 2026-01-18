package resource

import (
	"context"
	"sync"
)

// ==================== BaseDataSource ====================

// BaseDataSource 数据源基类，提供通用实现
type BaseDataSource struct {
	config    *DataSourceConfig
	connected bool
	writable  bool
	mu        sync.RWMutex
}

// NewBaseDataSource 创建基础数据源
func NewBaseDataSource(config *DataSourceConfig, writable bool) *BaseDataSource {
	return &BaseDataSource{
		config:    config,
		connected: false,
		writable:  writable,
	}
}

// Connect 连接数据源（基类实现）
func (b *BaseDataSource) Connect(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connected = true
	return nil
}

// Close 关闭连接（基类实现）
func (b *BaseDataSource) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (b *BaseDataSource) IsConnected() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connected
}

// GetConfig 获取数据源配置
func (b *BaseDataSource) GetConfig() *DataSourceConfig {
	return b.config
}

// IsWritable 检查是否可写
func (b *BaseDataSource) IsWritable() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.writable
}

// SetWritable 设置可写状态
func (b *BaseDataSource) SetWritable(writable bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.writable = writable
}

// CheckConnected 检查连接状态（基类辅助方法）
func (b *BaseDataSource) CheckConnected() error {
	if !b.IsConnected() {
		return ErrNotConnected()
	}
	return nil
}

// CheckWritable 检查可写状态（基类辅助方法）
func (b *BaseDataSource) CheckWritable(dataSourceType string) error {
	if !b.IsWritable() {
		return ErrReadOnly(dataSourceType)
	}
	return nil
}

// ==================== BaseFileDataSource ====================

// BaseFileDataSource 文件数据源基类
type BaseFileDataSource struct {
	*BaseDataSource
	filePath string
	columns  []ColumnInfo
}

// NewBaseFileDataSource 创建文件数据源基类
func NewBaseFileDataSource(config *DataSourceConfig, filePath string, writable bool) *BaseFileDataSource {
	return &BaseFileDataSource{
		BaseDataSource: NewBaseDataSource(config, writable),
		filePath:      filePath,
		columns:       []ColumnInfo{},
	}
}

// GetFilePath 获取文件路径
func (b *BaseFileDataSource) GetFilePath() string {
	return b.filePath
}

// SetColumns 设置列信息
func (b *BaseFileDataSource) SetColumns(columns []ColumnInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.columns = columns
}

// GetColumns 获取列信息
func (b *BaseFileDataSource) GetColumns() []ColumnInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.columns
}

// CheckTableExists 检查表是否存在
func (b *BaseFileDataSource) CheckTableExists(tableName string, expectedTableName string) error {
	if tableName != expectedTableName {
		return ErrTableNotFound(tableName)
	}
	return nil
}

// GetTables 获取所有表（文件数据源默认返回一个表）
func (b *BaseFileDataSource) GetTables(ctx context.Context, tableName string) ([]string, error) {
	if err := b.CheckConnected(); err != nil {
		return nil, err
	}
	return []string{tableName}, nil
}

// GetTableInfo 获取表信息（文件数据源默认实现）
func (b *BaseFileDataSource) GetTableInfo(ctx context.Context, tableName, expectedTableName string) (*TableInfo, error) {
	if err := b.CheckConnected(); err != nil {
		return nil, err
	}

	if err := b.CheckTableExists(tableName, expectedTableName); err != nil {
		return nil, err
	}

	return &TableInfo{
		Name:    expectedTableName,
		Columns: b.GetColumns(),
	}, nil
}

// Execute 执行自定义SQL（文件数据源不支持）
func (b *BaseFileDataSource) Execute(ctx context.Context, dataSourceType, sql string) (*QueryResult, error) {
	return nil, ErrSQLNotSupported(dataSourceType)
}

// Insert 插入数据（文件数据源默认只读实现）
func (b *BaseFileDataSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions, dataSourceType string) (int64, error) {
	if err := b.CheckWritable(dataSourceType); err != nil {
		return 0, err
	}
	return 0, ErrReadOnly(dataSourceType)
}

// Update 更新数据（文件数据源默认只读实现）
func (b *BaseFileDataSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions, dataSourceType string) (int64, error) {
	if err := b.CheckWritable(dataSourceType); err != nil {
		return 0, err
	}
	return 0, ErrReadOnly(dataSourceType)
}

// Delete 删除数据（文件数据源默认只读实现）
func (b *BaseFileDataSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions, dataSourceType string) (int64, error) {
	if err := b.CheckWritable(dataSourceType); err != nil {
		return 0, err
	}
	return 0, ErrReadOnly(dataSourceType)
}

// CreateTable 创建表（文件数据源默认只读实现）
func (b *BaseFileDataSource) CreateTable(ctx context.Context, tableInfo *TableInfo, dataSourceType string) error {
	if err := b.CheckWritable(dataSourceType); err != nil {
		return err
	}
	return ErrReadOnly(dataSourceType)
}

// DropTable 删除表（文件数据源默认只读实现）
func (b *BaseFileDataSource) DropTable(ctx context.Context, tableName string, dataSourceType string) error {
	if err := b.CheckWritable(dataSourceType); err != nil {
		return err
	}
	return ErrReadOnly(dataSourceType)
}

// TruncateTable 清空表（文件数据源默认只读实现）
func (b *BaseFileDataSource) TruncateTable(ctx context.Context, tableName string, dataSourceType string) error {
	if err := b.CheckWritable(dataSourceType); err != nil {
		return err
	}
	return ErrReadOnly(dataSourceType)
}

// FilterColumns 过滤列信息（通用方法）
func (b *BaseFileDataSource) FilterColumns(neededColumns []string) []ColumnInfo {
	columns := b.GetColumns()
	if len(neededColumns) == 0 {
		return columns
	}

	filtered := make([]ColumnInfo, 0, len(neededColumns))
	neededMap := make(map[string]bool)
	for _, col := range neededColumns {
		neededMap[col] = true
	}

	for _, col := range columns {
		if neededMap[col.Name] {
			filtered = append(filtered, col)
		}
	}

	return filtered
}

// GetNeededColumns 获取需要读取的列（通用方法）
func GetNeededColumns(options *QueryOptions) []string {
	if options == nil {
		return nil
	}

	needed := make(map[string]bool)
	for _, filter := range options.Filters {
		needed[filter.Field] = true
	}

	if options.OrderBy != "" {
		needed[options.OrderBy] = true
	}

	if len(options.SelectColumns) > 0 {
		for _, col := range options.SelectColumns {
			needed[col] = true
		}
	}

	if len(needed) == 0 {
		return nil
	}

	result := make([]string, 0, len(needed))
	for col := range needed {
		result = append(result, col)
	}

	return result
}

// ApplyQueryOperations 应用查询操作（过滤器、排序、分页）
func ApplyQueryOperations(rows []Row, options *QueryOptions, columns *[]ColumnInfo) []Row {
	// 应用过滤器
	filteredRows := ApplyFilters(rows, options)

	// 应用排序
	sortedRows := ApplyOrder(filteredRows, options)

	// 应用分页
	pagedRows := ApplyPagination(sortedRows, options.Offset, options.Limit)

	// 如果需要列裁剪
	if options != nil && len(options.SelectColumns) > 0 && columns != nil {
		pagedRows = PruneRows(pagedRows, options.SelectColumns)
	}

	return pagedRows
}

// PruneRows 裁剪行，只保留指定的列
func PruneRows(rows []Row, columns []string) []Row {
	if len(columns) == 0 {
		return rows
	}

	result := make([]Row, len(rows))
	for i, row := range rows {
		pruned := make(Row)
		for _, col := range columns {
			if val, ok := row[col]; ok {
				pruned[col] = val
			}
		}
		result[i] = pruned
	}

	return result
}
