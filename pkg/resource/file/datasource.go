package file

import (
	"context"
	"os"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	errors "github.com/kasuganosora/sqlexec/pkg/resource/infrastructure/errors"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ==================== File DataSource 基类 ====================

// FileDataSource 文件数据源基类
type FileDataSource struct {
	config    *domain.DataSourceConfig
	connected bool
	writable  bool
	filePath  string
	columns   []domain.ColumnInfo
	mu        sync.RWMutex
}

// NewFileDataSource 创建文件数据源基类
func NewFileDataSource(config *domain.DataSourceConfig, filePath string, writable bool) *FileDataSource {
	return &FileDataSource{
		config:   config,
		filePath: filePath,
		writable: writable,
		columns:  []domain.ColumnInfo{},
	}
}

// IsConnected 检查是否已连接
func (f *FileDataSource) IsConnected() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.connected
}

// GetConfig 获取配置
func (f *FileDataSource) GetConfig() *domain.DataSourceConfig {
	return f.config
}

// IsWritable 检查是否可写
func (f *FileDataSource) IsWritable() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.writable
}

// SetWritable 设置可写状态
func (f *FileDataSource) SetWritable(writable bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writable = writable
}

// GetFilePath 获取文件路径
func (f *FileDataSource) GetFilePath() string {
	return f.filePath
}

// SetColumns 设置列信息
func (f *FileDataSource) SetColumns(columns []domain.ColumnInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.columns = columns
}

// GetColumns 获取列信息
func (f *FileDataSource) GetColumns() []domain.ColumnInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.columns
}

// FilterColumns 过滤列信息（通用方法）
func (f *FileDataSource) FilterColumns(neededColumns []string) []domain.ColumnInfo {
	columns := f.GetColumns()
	if len(neededColumns) == 0 {
		return columns
	}

	filtered := make([]domain.ColumnInfo, 0, len(neededColumns))
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

// CheckConnected 检查连接状态
func (f *FileDataSource) CheckConnected() error {
	if !f.IsConnected() {
		return domain.NewErrNotConnected(string(f.config.Type))
	}
	return nil
}

// CheckWritable 检查可写状态
func (f *FileDataSource) CheckWritable() error {
	if !f.IsWritable() {
		return domain.NewErrReadOnly(string(f.config.Type), "write operation")
	}
	return nil
}

// CheckFileExists 检查文件是否存在
func (f *FileDataSource) CheckFileExists() error {
	if _, err := os.Stat(f.filePath); err != nil {
		if os.IsNotExist(err) {
			return errors.NewErrFileNotFound(f.filePath, string(f.config.Type))
		}
		return err
	}
	return nil
}

// CheckTableExists 检查表是否存在（文件数据源默认返回一个表）
func (f *FileDataSource) CheckTableExists(tableName string, expectedTableName string) error {
	if tableName != expectedTableName {
		return domain.NewErrTableNotFound(tableName)
	}
	return nil
}

// GetTables 获取所有表（文件数据源默认返回一个表）
func (f *FileDataSource) GetTables(ctx context.Context, tableName string) ([]string, error) {
	if err := f.CheckConnected(); err != nil {
		return nil, err
	}
	return []string{tableName}, nil
}

// GetTableInfo 获取表信息（文件数据源默认实现）
func (f *FileDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	if err := f.CheckConnected(); err != nil {
		return nil, err
	}

	if err := f.CheckTableExists(tableName, getDefaultTableName()); err != nil {
		return nil, err
	}

	return &domain.TableInfo{
		Name:    getDefaultTableName(),
		Columns: f.GetColumns(),
	}, nil
}

// Execute 执行自定义SQL（文件数据源不支持）
func (f *FileDataSource) Execute(ctx context.Context, sqlStr string) (*domain.QueryResult, error) {
	return nil, errors.NewErrSQLNotSupported(string(f.config.Type))
}

// Insert 插入数据（文件数据源默认只读实现）
func (f *FileDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if err := f.CheckWritable(); err != nil {
		return 0, err
	}
	return 0, errors.NewErrOperationNotSupported(string(f.config.Type), "insert")
}

// Update 更新数据（文件数据源默认只读实现）
func (f *FileDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if err := f.CheckWritable(); err != nil {
		return 0, err
	}
	return 0, errors.NewErrOperationNotSupported(string(f.config.Type), "update")
}

// Delete 删除数据（文件数据源默认只读实现）
func (f *FileDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if err := f.CheckWritable(); err != nil {
		return 0, err
	}
	return 0, errors.NewErrOperationNotSupported(string(f.config.Type), "delete")
}

// CreateTable 创建表（文件数据源默认只读实现）
func (f *FileDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	if err := f.CheckWritable(); err != nil {
		return err
	}
	return errors.NewErrOperationNotSupported(string(f.config.Type), "create table")
}

// DropTable 删除表（文件数据源默认只读实现）
func (f *FileDataSource) DropTable(ctx context.Context, tableName string) error {
	if err := f.CheckWritable(); err != nil {
		return err
	}
	return errors.NewErrOperationNotSupported(string(f.config.Type), "drop table")
}

// TruncateTable 清空表（文件数据源默认只读实现）
func (f *FileDataSource) TruncateTable(ctx context.Context, tableName string) error {
	if err := f.CheckWritable(); err != nil {
		return err
	}
	return errors.NewErrOperationNotSupported(string(f.config.Type), "truncate table")
}

// ApplyQueryOperations 应用查询操作（过滤器、排序、分页）
func (f *FileDataSource) ApplyQueryOperations(rows []domain.Row, options *domain.QueryOptions) []domain.Row {
	return util.ApplyQueryOperations(rows, options, &f.columns)
}

// getDefaultTableName 获取默认表名
func getDefaultTableName() string {
	return "data"
}

// SetConnected 设置连接状态
func (f *FileDataSource) SetConnected(connected bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connected = connected
}
