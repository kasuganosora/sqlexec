package resource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// JSONSource JSON文件数据源实�?
type JSONSource struct {
	config      *DataSourceConfig
	connected   bool
	writable    bool // JSON文件默认只读
	mu          sync.RWMutex
	filePath    string
	columns     []ColumnInfo
	// 并行读取配置
	chunkSize   int64
	workers     int
	// JSON格式
	arrayMode   bool // 是否为数组格�?[ {}, {}, ... ]
	recordsPath string // JSONPath 查询路径
}

// JSONFactory JSON数据源工�?
type JSONFactory struct{}

// NewJSONFactory 创建JSON数据源工�?
func NewJSONFactory() *JSONFactory {
	return &JSONFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *JSONFactory) GetType() DataSourceType {
	return DataSourceTypeJSON
}

// Create 实现DataSourceFactory接口
func (f *JSONFactory) Create(config *DataSourceConfig) (DataSource, error) {
	if config.Options == nil {
		config.Options = make(map[string]interface{})
	}
	
	chunkSize := int64(1 << 20) // 1MB
	if cs, ok := config.Options["chunk_size"]; ok {
		if num, ok := cs.(int64); ok && num > 0 {
			chunkSize = num
		}
	}
	
	workers := 4
	if w, ok := config.Options["workers"]; ok {
		if num, ok := w.(int); ok && num > 0 && num <= 32 {
			workers = num
		}
	}
	
	arrayMode := true
	if am, ok := config.Options["array_mode"]; ok {
		if b, ok := am.(bool); ok {
			arrayMode = b
		}
	}
	
	recordsPath := ""
	if rp, ok := config.Options["records_path"]; ok {
		if str, ok := rp.(string); ok {
			recordsPath = str
		}
	}

	return &JSONSource{
		config:      config,
		writable:    false, // JSON文件默认只读
		filePath:    config.Name,
		chunkSize:   chunkSize,
		workers:     workers,
		arrayMode:   arrayMode,
		recordsPath: recordsPath,
	}, nil
}

// Connect 连接数据�?
func (s *JSONSource) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 检查文件是否存�?
	if _, err := os.Stat(s.filePath); err != nil {
		return fmt.Errorf("JSON file not found: %s", s.filePath)
	}
	
	// 推断列信�?
	if err := s.inferSchema(ctx); err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}
	
	s.connected = true
	return nil
}

// Close 关闭连接
func (s *JSONSource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (s *JSONSource) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// GetConfig 获取数据源配�?
func (s *JSONSource) GetConfig() *DataSourceConfig {
	return s.config
}

// IsWritable 检查是否可�?
func (s *JSONSource) IsWritable() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writable
}

// GetTables 获取所有表
func (s *JSONSource) GetTables(ctx context.Context) ([]string, error) {
	if !s.IsConnected() {
		return nil, fmt.Errorf("not connected")
	}
	return []string{"json_data"}, nil
}

// GetTableInfo 获取表信�?
func (s *JSONSource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	if !s.IsConnected() {
		return nil, fmt.Errorf("not connected")
	}
	
	if tableName != "json_data" {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	
	return &TableInfo{
		Name:    "json_data",
		Columns: s.columns,
	}, nil
}

// Query 查询数据
func (s *JSONSource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	if !s.IsConnected() {
		return nil, fmt.Errorf("not connected")
	}
	
	if tableName != "json_data" {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	
	// 读取数据
	rows, err := s.readAll(ctx)
	if err != nil {
		return nil, err
	}
	
	// 应用过滤�?
	filteredRows := s.applyFilters(rows, options)
	
	// 应用排序
	sortedRows := s.applyOrder(filteredRows, options)
	
	// 应用分页
	total := int64(len(sortedRows))
	pagedRows := s.applyPagination(sortedRows, options)
	
	return &QueryResult{
		Columns: s.columns,
		Rows:    pagedRows,
		Total:   total,
	}, nil
}

// Insert 插入数据
func (s *JSONSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	return 0, fmt.Errorf("JSON data source is read-only")
}

// Update 更新数据
func (s *JSONSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	return 0, fmt.Errorf("JSON data source is read-only")
}

// Delete 删除数据
func (s *JSONSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	return 0, fmt.Errorf("JSON data source is read-only")
}

// CreateTable 创建�?
func (s *JSONSource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	return fmt.Errorf("JSON data source is read-only")
}

// DropTable 删除�?
func (s *JSONSource) DropTable(ctx context.Context, tableName string) error {
	return fmt.Errorf("JSON data source is read-only")
}

// TruncateTable 清空�?
func (s *JSONSource) TruncateTable(ctx context.Context, tableName string) error {
	return fmt.Errorf("JSON data source is read-only")
}

// Execute 执行自定义SQL语句
func (s *JSONSource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	return nil, fmt.Errorf("JSON data source does not support SQL execution")
}

// inferSchema 推断JSON文件的列信息
func (s *JSONSource) inferSchema(ctx context.Context) error {
	file, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	// 读取文件内容
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	
	// 解析JSON
	var records []map[string]interface{}
	if s.arrayMode {
		// 数组格式: [ {}, {}, ... ]
		if err := json.Unmarshal(data, &records); err != nil {
			return fmt.Errorf("failed to parse JSON array: %w", err)
		}
	} else {
		// 行分隔格�? 每行一个JSON对象
		lines := splitLines(data)
		for _, line := range lines {
			var record map[string]interface{}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				continue
			}
			records = append(records, record)
		}
	}
	
	// 采样�?000行推断类�?
	sampleSize := 1000
	if len(records) < sampleSize {
		sampleSize = len(records)
	}
	
	// 收集所有字�?
	fieldsMap := make(map[string][]interface{})
	for i := 0; i < sampleSize; i++ {
		for key, value := range records[i] {
			fieldsMap[key] = append(fieldsMap[key], value)
		}
	}
	
	// 推断每列的类�?
	s.columns = make([]ColumnInfo, 0, len(fieldsMap))
	for field, values := range fieldsMap {
		colType := s.inferColumnType(values)
		s.columns = append(s.columns, ColumnInfo{
			Name:     field,
			Type:     colType,
			Nullable: true,
			Primary:  false,
		})
	}
	
	return nil
}

// inferColumnType 推断列类�?
func (s *JSONSource) inferColumnType(values []interface{}) string {
	if len(values) == 0 {
		return "VARCHAR"
	}
	
	typeCounts := map[string]int{
		"INTEGER": 0,
		"FLOAT":   0,
		"BOOLEAN": 0,
		"VARCHAR": 0,
	}
	
	for _, value := range values {
		if value == nil {
			continue
		}
		
		colType := s.detectType(value)
		typeCounts[colType]++
	}
	
	// 选择最常见的类�?
	maxCount := 0
	bestType := "VARCHAR"
	for t, count := range typeCounts {
		if count > maxCount {
			maxCount = count
			bestType = t
		}
	}
	
	return bestType
}

// detectType 检测值的类型
func (s *JSONSource) detectType(value interface{}) string {
	switch v := value.(type) {
	case bool:
		return "BOOLEAN"
	case float64:
		// JSON数字默认为float64,检查是否为整数
		if float64(int(v)) == v {
			return "INTEGER"
		}
		return "FLOAT"
	case int:
		return "INTEGER"
	case int64:
		return "INTEGER"
	case float32:
		return "FLOAT"
	case string:
		return "VARCHAR"
	default:
		return "VARCHAR"
	}
}

// readAll 读取所有数�?
func (s *JSONSource) readAll(ctx context.Context) ([]Row, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	var rows []Row
	
	if s.arrayMode {
		// 数组格式
		data, err := io.ReadAll(file)
		if err != nil {
			return nil, err
		}
		
		var records []map[string]interface{}
		if err := json.Unmarshal(data, &records); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		
		rows = make([]Row, len(records))
		for i, record := range records {
			rows[i] = Row(record)
		}
	} else {
		// 行分隔格�?
		decoder := json.NewDecoder(file)
		for {
			var record map[string]interface{}
			if err := decoder.Decode(&record); err == io.EOF {
				break
			} else if err != nil {
				continue
			}
			rows = append(rows, Row(record))
		}
	}
	
	return rows, nil
}

// applyFilters 应用过滤�?
func (s *JSONSource) applyFilters(rows []Row, options *QueryOptions) []Row {
	if options == nil || len(options.Filters) == 0 {
		return rows
	}
	
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		if s.matchesFilters(row, options.Filters) {
			result = append(result, row)
		}
	}
	return result
}

// matchesFilters 检查行是否匹配过滤�?
func (s *JSONSource) matchesFilters(row Row, filters []Filter) bool {
	for _, filter := range filters {
		if !s.matchFilter(row, filter) {
			return false
		}
	}
	return true
}

// matchFilter 匹配单个过滤�?
func (s *JSONSource) matchFilter(row Row, filter Filter) bool {
	value, exists := row[filter.Field]
	if !exists {
		return false
	}
	
	switch filter.Operator {
	case "=":
		return s.compareEqual(value, filter.Value)
	case "!=":
		return !s.compareEqual(value, filter.Value)
	case ">":
		return s.compareGreater(value, filter.Value)
	case "<":
		return !s.compareGreater(value, filter.Value) && !s.compareEqual(value, filter.Value)
	case ">=":
		return s.compareGreater(value, filter.Value) || s.compareEqual(value, filter.Value)
	case "<=":
		return !s.compareGreater(value, filter.Value)
	default:
		return true
	}
}

// 比较辅助函数
func (s *JSONSource) compareEqual(a, b interface{}) bool {
	if cmp, ok := s.compareNumeric(a, b); ok {
		return cmp == 0
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func (s *JSONSource) compareGreater(a, b interface{}) bool {
	if cmp, ok := s.compareNumeric(a, b); ok {
		return cmp > 0
	}
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr > bStr
}

func (s *JSONSource) compareNumeric(a, b interface{}) (int, bool) {
	aFloat, okA := s.toFloat64(a)
	bFloat, okB := s.toFloat64(b)
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

func (s *JSONSource) toFloat64(v interface{}) (float64, bool) {
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

// applyOrder 应用排序
func (s *JSONSource) applyOrder(rows []Row, options *QueryOptions) []Row {
	if options == nil || options.OrderBy == "" {
		return rows
	}
	
	// 简化实�? 返回原始顺序
	return rows
}

// applyPagination 应用分页
func (s *JSONSource) applyPagination(rows []Row, options *QueryOptions) []Row {
	if options == nil {
		return rows
	}
	return ApplyPagination(rows, options.Offset, options.Limit)
}

// 初始�?
func init() {
	RegisterFactory(NewJSONFactory())
}
