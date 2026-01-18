package csv_source

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/kasuganosora/sqlexec/pkg/resource"
)

// CSVSource CSV文件数据源实现
type CSVSource struct {
	config      *resource.DataSourceConfig
	connected   bool
	writable    bool
	filePath    string
	separator   rune
	hasHeader   bool
}

// NewCSVSource 创建CSV数据源
func NewCSVSource(filePath string) *CSVSource {
	return &CSVSource{
		filePath:  filePath,
		separator: ',',
		hasHeader:  true,
		writable:  false,
	}
}

// Connect 连接数据源
func (c *CSVSource) Connect(ctx context.Context) error {
	file, err := os.Open(c.filePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = c.separator
	reader.FieldsPerRecord = -1

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	c.config = &resource.DataSourceConfig{
		Name:       "csv",
		Type:       resource.DataSourceTypeCSV,
		Options:    make(map[string]interface{}),
		TotalRows:  0,
		Size:       stat.Size(),
	}

	c.connected = true
	return nil
}

// Close 关闭数据源
func (c *CSVSource) Close(ctx context.Context) error {
	c.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (c *CSVSource) IsConnected() bool {
	return c.connected
}

// IsWritable 检查是否可写
func (c *CSVSource) IsWritable() bool {
	return c.writable
}

// GetConfig 获取配置
func (c *CSVSource) GetConfig() *resource.DataSourceConfig {
	return c.config
}

// SetConfig 设置配置
func (c *CSVSource) SetConfig(config *resource.DataSourceConfig) error {
	c.config = config
	return nil
}

// GetColumns 获取列信息
func (c *CSVSource) GetColumns() ([]resource.ColumnInfo, error) {
	file, err := os.Open(c.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = c.separator

	records, err := reader.ReadAll()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return []resource.ColumnInfo{}, nil
	}

	headers := records[0]
	columns := make([]resource.ColumnInfo, len(headers))
	for i, h := range headers {
		columns[i] = resource.ColumnInfo{
			Name:     h,
			Type:      "text",
			Nullable:  true,
			Index:     i,
		}
	}

	c.columns = columns
	return columns, nil
}

// Query 执行查询
func (c *CSVSource) Query(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	return nil, fmt.Errorf("CSV source does not support SQL queries")
}

// Insert 插入数据
func (c *CSVSource) Insert(ctx context.Context, table string, columns []string, values []interface{}) (int64, error) {
	return 0, fmt.Errorf("CSV source is read-only")
}

// Update 更新数据
func (c *CSVSource) Update(ctx context.Context, table string, set map[string]interface{}, where map[string]interface{}) (int64, error) {
	return 0, fmt.Errorf("CSV source is read-only")
}

// Delete 删除数据
func (c *CSVSource) Delete(ctx context.Context, table string, where map[string]interface{}) (int64, error) {
	return 0, fmt.Errorf("CSV source is read-only")
}

// Execute 执行语句
func (c *CSVSource) Execute(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	return nil, fmt.Errorf("CSV source does not support SQL execution")
}

// Begin 开始事务
func (c *CSVSource) Begin(ctx context.Context) (resource.Transaction, error) {
	return nil, fmt.Errorf("CSV source does not support transactions")
}

// Commit 提交事务
func (c *CSVSource) Commit() error {
	return fmt.Errorf("CSV source does not support transactions")
}

// Rollback 回滚事务
func (c *CSVSource) Rollback() error {
	return fmt.Errorf("CSV source does not support transactions")
}
