package resource

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// JSONSource JSON文件数据源实现
type JSONSource struct {
	*BaseFileDataSource
	// 并行读取配置
	chunkSize   int64
	workers     int
	// JSON格式
	arrayMode   bool // 是否为数组格式 [ {}, {}, ... ]
	recordsPath string // JSONPath 查询路径
}

// JSONFactory JSON数据源工厂
type JSONFactory struct{}

// NewJSONFactory 创建JSON数据源工厂
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
		BaseFileDataSource: NewBaseFileDataSource(config, config.Name, false),
		chunkSize:         chunkSize,
		workers:           workers,
		arrayMode:         arrayMode,
		recordsPath:       recordsPath,
	}, nil
}

// Connect 连接数据源
func (s *JSONSource) Connect(ctx context.Context) error {
	s.BaseDataSource.mu.Lock()
	defer s.BaseDataSource.mu.Unlock()

	// 检查文件是否存在
	if _, err := os.Stat(s.filePath); err != nil {
		return ErrFileNotFound(s.filePath, "JSON")
	}

	// 推断列信息
	if err := s.inferSchema(ctx); err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	s.connected = true
	return nil
}

// GetTables 获取所有表
func (s *JSONSource) GetTables(ctx context.Context) ([]string, error) {
	return s.BaseFileDataSource.GetTables(ctx, "json_data")
}

// GetTableInfo 获取表信息
func (s *JSONSource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	return s.BaseFileDataSource.GetTableInfo(ctx, tableName, "json_data")
}

// Query 查询数据
func (s *JSONSource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	if err := s.CheckConnected(); err != nil {
		return nil, err
	}

	if err := s.CheckTableExists(tableName, "json_data"); err != nil {
		return nil, err
	}

	// 读取数据
	rows, err := s.readAll(ctx)
	if err != nil {
		return nil, err
	}

	// 获取需要读取的列
	neededColumns := GetNeededColumns(options)

	// 应用查询操作（过滤、排序、分页、列裁剪）
	pagedRows := ApplyQueryOperations(rows, options, &s.columns)

	// 构建列信息
	columns := s.GetColumns()
	if len(neededColumns) > 0 {
		columns = s.FilterColumns(neededColumns)
	}

	return &QueryResult{
		Columns: columns,
		Rows:    pagedRows,
		Total:   int64(len(pagedRows)),
	}, nil
}

// Insert 插入数据
func (s *JSONSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	return s.BaseFileDataSource.Insert(ctx, tableName, rows, options, "JSON")
}

// Update 更新数据
func (s *JSONSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	return s.BaseFileDataSource.Update(ctx, tableName, filters, updates, options, "JSON")
}

// Delete 删除数据
func (s *JSONSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	return s.BaseFileDataSource.Delete(ctx, tableName, filters, options, "JSON")
}

// CreateTable 创建表
func (s *JSONSource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	return s.BaseFileDataSource.CreateTable(ctx, tableInfo, "JSON")
}

// DropTable 删除表
func (s *JSONSource) DropTable(ctx context.Context, tableName string) error {
	return s.BaseFileDataSource.DropTable(ctx, tableName, "JSON")
}

// TruncateTable 清空表
func (s *JSONSource) TruncateTable(ctx context.Context, tableName string) error {
	return s.BaseFileDataSource.TruncateTable(ctx, tableName, "JSON")
}

// Execute 执行自定义SQL语句
func (s *JSONSource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	return s.BaseFileDataSource.Execute(ctx, "JSON", sql)
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
		// 行分隔格式: 每行一个JSON对象
		lines := SplitLines(data)
		for _, line := range lines {
			var record map[string]interface{}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				continue
			}
			records = append(records, record)
		}
	}
	
	// 采样前1000行推断类型
	sampleSize := 1000
	if len(records) < sampleSize {
		sampleSize = len(records)
	}
	
	// 收集所有字段
	fieldsMap := make(map[string][]interface{})
	for i := 0; i < sampleSize; i++ {
		for key, value := range records[i] {
			fieldsMap[key] = append(fieldsMap[key], value)
		}
	}
	
	// 推断每列的类型
	columns := make([]ColumnInfo, 0, len(fieldsMap))
	for field, values := range fieldsMap {
		colType := s.inferColumnType(values)
		columns = append(columns, ColumnInfo{
			Name:     field,
			Type:     colType,
			Nullable: true,
			Primary:  false,
		})
	}
	s.SetColumns(columns)
	
	return nil
}

// inferColumnType 推断列类型
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
	
	// 选择最常见的类型
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

// readAll 读取所有数据
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
		// 行分隔格式
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

// 初始化
func init() {
	RegisterFactory(NewJSONFactory())
}
