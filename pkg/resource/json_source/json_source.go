package json_source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource"
)

// JSONSource JSON文件数据源实现
type JSONSource struct {
	config      *resource.DataSourceConfig
	connected   bool
	writable    bool
	mu          sync.RWMutex
	filePath    string
	columns     []resource.ColumnInfo
	chunkSize   int64
	workers     int
	arrayMode   bool
	recordsPath string
}

// NewJSONSource 创建JSON数据源
func NewJSONSource(filePath string) *JSONSource {
	return &JSONSource{
		filePath:  filePath,
		writable:  false,
		chunkSize: 1 << 20, // 1MB
		workers:   4,
	}
}

// Connect 连接数据源
func (j *JSONSource) Connect(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	file, err := os.Open(j.filePath)
	if err != nil {
		return fmt.Errorf("failed to open JSON file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	j.config = &resource.DataSourceConfig{
		Name:       "json",
		Type:       resource.DataSourceTypeJSON,
		Options:    make(map[string]interface{}),
		TotalRows:  0,
		Size:       stat.Size(),
	}

	j.connected = true
	return nil
}

// Close 关闭数据源
func (j *JSONSource) Close(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (j *JSONSource) IsConnected() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.connected
}

// IsWritable 检查是否可写
func (j *JSONSource) IsWritable() bool {
	return j.writable
}

// GetConfig 获取配置
func (j *JSONSource) GetConfig() *resource.DataSourceConfig {
	return j.config
}

// SetConfig 设置配置
func (j *JSONSource) SetConfig(config *resource.DataSourceConfig) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.config = config
	return nil
}

// GetColumns 获取列信息
func (j *JSONSource) GetColumns() ([]resource.ColumnInfo, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	if len(j.columns) > 0 {
		return j.columns, nil
	}

	return []resource.ColumnInfo{}, nil
}

// Query 执行查询
func (j *JSONSource) Query(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	return nil, fmt.Errorf("JSON source does not support SQL queries")
}

// Insert 插入数据
func (j *JSONSource) Insert(ctx context.Context, table string, columns []string, values []interface{}) (int64, error) {
	return 0, fmt.Errorf("JSON source is read-only")
}

// Update 更新数据
func (j *JSONSource) Update(ctx context.Context, table string, set map[string]interface{}, where map[string]interface{}) (int64, error) {
	return 0, fmt.Errorf("JSON source is read-only")
}

// Delete 删除数据
func (j *JSONSource) Delete(ctx context.Context, table string, where map[string]interface{}) (int64, error) {
	return 0, fmt.Errorf("JSON source is read-only")
}

// Execute 执行语句
func (j *JSONSource) Execute(ctx context.Context, sql string, args ...interface{}) (*resource.QueryResult, error) {
	return nil, fmt.Errorf("JSON source does not support SQL execution")
}

// Begin 开始事务
func (j *JSONSource) Begin(ctx context.Context) (resource.Transaction, error) {
	return nil, fmt.Errorf("JSON source does not support transactions")
}

// Commit 提交事务
func (j *JSONSource) Commit() error {
	return fmt.Errorf("JSON source does not support transactions")
}

// Rollback 回滚事务
func (j *JSONSource) Rollback() error {
	return fmt.Errorf("JSON source does not support transactions")
}
