package excel_source

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource"
)

// ExcelSource Excel文件数据源实现
type ExcelSource struct {
	config      *resource.DataSourceConfig
	connected   bool
	writable    bool
	filePath    string
}

// NewExcelSource 创建Excel数据源
func NewExcelSource(filePath string) *ExcelSource {
	return &ExcelSource{
		filePath:  filePath,
		writable:  false,
	}
}

// Connect 连接数据源
func (e *ExcelSource) Connect(ctx context.Context) error {
	return fmt.Errorf("Excel source not implemented")
}

// Close 关闭数据源
func (e *ExcelSource) Close(ctx context.Context) error {
	e.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (e *ExcelSource) IsConnected() bool {
	return e.connected
}

// IsWritable 检查是否可写
func (e *ExcelSource) IsWritable() bool {
	return e.writable
}

// GetConfig 获取配置
func (e *ExcelSource) GetConfig() *resource.DataSourceConfig {
	return e.config
}

// SetConfig 设置配置
func (e *ExcelSource) SetConfig(config *resource.DataSourceConfig) error {
	e.config = config
	return nil
}

// GetColumns 获取列信息
func (e *ExcelSource) GetColumns() ([]resource.ColumnInfo, error) {
	return []resource.ColumnInfo{}, fmt.Errorf("Excel source not implemented")
}

// Query 执行查询
func (e *ExcelSource) Query(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	return nil, fmt.Errorf("Excel source not implemented")
}

// Insert 插入数据
func (e *ExcelSource) Insert(ctx context.Context, table string, columns []string, values []interface{}) (int64, error) {
	return 0, fmt.Errorf("Excel source is read-only")
}

// Update 更新数据
func (e *ExcelSource) Update(ctx context.Context, table string, set map[string]interface{}, where map[string]interface{}) (int64, error) {
	return 0, fmt.Errorf("Excel source is read-only")
}

// Delete 删除数据
func (e *ExcelSource) Delete(ctx context.Context, table string, where map[string]interface{}) (int64, error) {
	return 0, fmt.Errorf("Excel source is read-only")
}

// Execute 执行语句
func (e *ExcelSource) Execute(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	return nil, fmt.Errorf("Excel source not implemented")
}

// Begin 开始事务
func (e *ExcelSource) Begin(ctx context.Context) (resource.Transaction, error) {
	return nil, fmt.Errorf("Excel source does not support transactions")
}

// Commit 提交事务
func (e *ExcelSource) Commit() error {
	return fmt.Errorf("Excel source does not support transactions")
}

// Rollback 回滚事务
func (e *ExcelSource) Rollback() error {
	return fmt.Errorf("Excel source does not support transactions")
}
