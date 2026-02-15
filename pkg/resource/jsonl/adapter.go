package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// JSONLAdapter JSONL文件数据源适配器
// 继承 MVCCDataSource，只负责JSONL格式的加载和写回
type JSONLAdapter struct {
	*memory.MVCCDataSource
	filePath   string
	writable   bool
	skipErrors bool
}

// NewJSONLAdapter 创建JSONL数据源适配器
func NewJSONLAdapter(config *domain.DataSourceConfig, filePath string) *JSONLAdapter {
	writable := config.Writable
	skipErrors := false

	if config.Options != nil {
		if w, ok := config.Options["writable"]; ok {
			if b, ok := w.(bool); ok {
				writable = b
			}
		}
		if s, ok := config.Options["skip_errors"]; ok {
			if b, ok := s.(bool); ok {
				skipErrors = b
			}
		}
	}

	internalConfig := *config
	internalConfig.Writable = writable

	return &JSONLAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(&internalConfig),
		filePath:       filePath,
		writable:       writable,
		skipErrors:     skipErrors,
	}
}

// Connect 连接数据源 - 加载JSONL文件到内存
func (a *JSONLAdapter) Connect(ctx context.Context) error {
	f, err := os.Open(a.filePath)
	if err != nil {
		return fmt.Errorf("failed to open JSONL file %q: %w", a.filePath, err)
	}
	defer f.Close()

	var rows []domain.Row
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024) // 最大 10MB 每行

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			if a.skipErrors {
				continue
			}
			return fmt.Errorf("failed to parse JSONL line %d: %w", lineNum, err)
		}
		rows = append(rows, domain.Row(obj))
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read JSONL file: %w", err)
	}

	// 推断列信息（允许空文件作为空表）
	var columns []domain.ColumnInfo
	if len(rows) > 0 {
		columns = a.inferColumnTypes(rows)
	}

	tableInfo := &domain.TableInfo{
		Name:    "jsonl_data",
		Schema:  "",
		Columns: columns,
	}

	if err := a.LoadTable("jsonl_data", tableInfo, rows); err != nil {
		return fmt.Errorf("failed to load JSONL data: %w", err)
	}

	return a.MVCCDataSource.Connect(ctx)
}

// Close 关闭连接 - 可选写回JSONL文件
func (a *JSONLAdapter) Close(ctx context.Context) error {
	var writeBackErr error
	if a.writable {
		if err := a.writeBack(); err != nil {
			writeBackErr = fmt.Errorf("failed to write back JSONL file: %w", err)
		}
	}

	closeErr := a.MVCCDataSource.Close(ctx)
	if writeBackErr != nil {
		return writeBackErr
	}
	return closeErr
}

// GetConfig 获取数据源配置
func (a *JSONLAdapter) GetConfig() *domain.DataSourceConfig {
	return a.MVCCDataSource.GetConfig()
}

// GetTables 获取所有表
func (a *JSONLAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo 获取表信息
func (a *JSONLAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query 查询数据
func (a *JSONLAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// Insert 插入数据
func (a *JSONLAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("jsonl", "insert")
	}
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据
func (a *JSONLAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("jsonl", "update")
	}
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据
func (a *JSONLAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("jsonl", "delete")
	}
	return a.MVCCDataSource.Delete(ctx, tableName, filters, options)
}

// CreateTable 创建表（JSONL不支持）
func (a *JSONLAdapter) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return domain.NewErrReadOnly("jsonl", "create table")
}

// DropTable 删除表（JSONL不支持）
func (a *JSONLAdapter) DropTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("jsonl", "drop table")
}

// TruncateTable 清空表（JSONL不支持）
func (a *JSONLAdapter) TruncateTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("jsonl", "truncate table")
}

// Execute 执行SQL（JSONL不支持）
func (a *JSONLAdapter) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation("jsonl", "execute SQL")
}

// IsConnected 检查是否已连接
func (a *JSONLAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// IsWritable 检查是否可写
func (a *JSONLAdapter) IsWritable() bool {
	return a.writable
}

// SupportsWrite 实现IsWritableSource接口
func (a *JSONLAdapter) SupportsWrite() bool {
	return a.writable
}

// ==================== 私有方法 ====================

// inferColumnTypes 推断列类型
func (a *JSONLAdapter) inferColumnTypes(rows []domain.Row) []domain.ColumnInfo {
	if len(rows) == 0 {
		return []domain.ColumnInfo{}
	}

	sampleSize := 100
	if len(rows) < sampleSize {
		sampleSize = len(rows)
	}

	fieldsMap := make(map[string][]interface{})
	for i := 0; i < sampleSize; i++ {
		for key, value := range rows[i] {
			fieldsMap[key] = append(fieldsMap[key], value)
		}
	}

	fieldNames := make([]string, 0, len(fieldsMap))
	for field := range fieldsMap {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)

	columns := make([]domain.ColumnInfo, 0, len(fieldsMap))
	for _, field := range fieldNames {
		colType := inferType(fieldsMap[field])
		columns = append(columns, domain.ColumnInfo{
			Name:     field,
			Type:     colType,
			Nullable: true,
		})
	}

	return columns
}

// inferType 推断值的类型
func inferType(values []interface{}) string {
	if len(values) == 0 {
		return "string"
	}

	typeCounts := map[string]int{
		"int64":   0,
		"float64": 0,
		"bool":    0,
		"string":  0,
	}

	for _, value := range values {
		if value == nil {
			continue
		}
		typeCounts[detectType(value)]++
	}

	typePriority := []string{"int64", "float64", "bool", "string"}
	maxCount := 0
	bestType := "string"
	for _, t := range typePriority {
		count := typeCounts[t]
		if count > maxCount {
			maxCount = count
			bestType = t
		}
	}

	return bestType
}

// detectType 检测值的类型
func detectType(value interface{}) string {
	switch v := value.(type) {
	case bool:
		return "bool"
	case float64:
		if v == math.Trunc(v) && !math.IsInf(v, 0) && !math.IsNaN(v) &&
			v >= math.MinInt64 && v <= math.MaxInt64 {
			return "int64"
		}
		return "float64"
	case string:
		return "string"
	case []interface{}, map[string]interface{}:
		return "string"
	default:
		return "string"
	}
}

// writeBack 写回JSONL文件
func (a *JSONLAdapter) writeBack() error {
	_, rows, err := a.GetLatestTableData("jsonl_data")
	if err != nil {
		return err
	}

	dir := filepath.Dir(a.filePath)
	tmpFile, err := os.CreateTemp(dir, ".jsonl_writeback_*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file for writeBack: %w", err)
	}
	tmpPath := tmpFile.Name()

	writer := bufio.NewWriter(tmpFile)
	for _, row := range rows {
		data, err := json.Marshal(map[string]interface{}(row))
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to marshal row: %w", err)
		}
		if _, err := writer.Write(data); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to write row: %w", err)
		}
		if err := writer.WriteByte('\n'); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, a.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}
