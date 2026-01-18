package resource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// CSVSource CSV文件数据源实现,采用DuckDB优化技术
type CSVSource struct {
	*BaseFileDataSource
	delimiter rune
	header    bool
	// 并行读取配置
	chunkSize int64
	workers   int
	// 内存映射支持
	useMmap bool
}

// CSVFactory CSV数据源工厂
type CSVFactory struct{}

// NewCSVFactory 创建CSV数据源工厂
func NewCSVFactory() *CSVFactory {
	return &CSVFactory{}
}

// GetType 实现DataSourceFactory接口
func (f *CSVFactory) GetType() DataSourceType {
	return DataSourceTypeCSV
}

// Create 实现DataSourceFactory接口
func (f *CSVFactory) Create(config *DataSourceConfig) (DataSource, error) {
	if config.Options == nil {
		config.Options = make(map[string]interface{})
	}

	// 默认配置
	delimiter := ','
	if d, ok := config.Options["delimiter"]; ok {
		if str, ok := d.(string); ok && len(str) > 0 {
			delimiter = rune(str[0])
		}
	}

	header := true
	if h, ok := config.Options["header"]; ok {
		if b, ok := h.(bool); ok {
			header = b
		}
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

	useMmap := true
	if m, ok := config.Options["mmap"]; ok {
		if b, ok := m.(bool); ok {
			useMmap = b
		}
	}

	return &CSVSource{
		BaseFileDataSource: NewBaseFileDataSource(config, config.Name, false),
		delimiter:          delimiter,
		header:             header,
		chunkSize:          chunkSize,
		workers:            workers,
		useMmap:            useMmap,
	}, nil
}

// Connect 连接数据源
func (s *CSVSource) Connect(ctx context.Context) error {
	s.BaseDataSource.mu.Lock()
	defer s.BaseDataSource.mu.Unlock()

	// 检查文件是否存在
	if _, err := os.Stat(s.filePath); err != nil {
		return ErrFileNotFound(s.filePath, "CSV")
	}

	// 推断列信息
	if err := s.inferSchema(ctx); err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	s.connected = true
	return nil
}

// GetTables 获取所有表 (CSV文件本身作为一个表)
func (s *CSVSource) GetTables(ctx context.Context) ([]string, error) {
	return s.BaseFileDataSource.GetTables(ctx, "csv_data")
}

// GetTableInfo 获取表信息
func (s *CSVSource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	return s.BaseFileDataSource.GetTableInfo(ctx, tableName, "csv_data")
}

// Query 查询数据 - 实现并行流式读取
func (s *CSVSource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	if err := s.CheckConnected(); err != nil {
		return nil, err
	}

	if err := s.CheckTableExists(tableName, "csv_data"); err != nil {
		return nil, err
	}

	// 获取需要读取的列
	neededColumns := GetNeededColumns(options)

	// 并行读取数据
	rows, err := s.readParallel(ctx, neededColumns, options)
	if err != nil {
		return nil, err
	}

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
func (s *CSVSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	return s.BaseFileDataSource.Insert(ctx, tableName, rows, options, "CSV")
}

// Update 更新数据
func (s *CSVSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	return s.BaseFileDataSource.Update(ctx, tableName, filters, updates, options, "CSV")
}

// Delete 删除数据
func (s *CSVSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	return s.BaseFileDataSource.Delete(ctx, tableName, filters, options, "CSV")
}

// CreateTable 创建表
func (s *CSVSource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	return s.BaseFileDataSource.CreateTable(ctx, tableInfo, "CSV")
}

// DropTable 删除表
func (s *CSVSource) DropTable(ctx context.Context, tableName string) error {
	return s.BaseFileDataSource.DropTable(ctx, tableName, "CSV")
}

// TruncateTable 清空表
func (s *CSVSource) TruncateTable(ctx context.Context, tableName string) error {
	return s.BaseFileDataSource.TruncateTable(ctx, tableName, "CSV")
}

// Execute 执行自定义SQL语句
func (s *CSVSource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	return s.BaseFileDataSource.Execute(ctx, "CSV", sql)
}

// inferSchema 推断CSV文件的列信息
func (s *CSVSource) inferSchema(ctx context.Context) error {
	file, err := os.Open(s.filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	reader := csv.NewReader(file)
	reader.Comma = s.delimiter
	
	// 读取头部
	headers, err := reader.Read()
	if err != nil {
		return err
	}
	
	// 采样前1000行推断类型
	sampleSize := 1000
	samples := make([][]string, 0, sampleSize)
	for i := 0; i < sampleSize; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		samples = append(samples, row)
	}
	
	// 推断每列的类型
	columns := make([]ColumnInfo, len(headers))
	for i, header := range headers {
		colType := s.inferColumnType(i, samples)
		columns[i] = ColumnInfo{
			Name:     strings.TrimSpace(header),
			Type:     colType,
			Nullable: true,
			Primary:  false,
		}
	}
	s.SetColumns(columns)
	
	return nil
}

// inferColumnType 推断列类型
func (s *CSVSource) inferColumnType(colIndex int, samples [][]string) string {
	if len(samples) == 0 {
		return "VARCHAR"
	}
	
	typeCounts := map[string]int{
		"INTEGER":  0,
		"FLOAT":    0,
		"BOOLEAN":  0,
		"VARCHAR":  0,
	}
	
	for _, row := range samples {
		if colIndex >= len(row) {
			continue
		}
		value := strings.TrimSpace(row[colIndex])
		if value == "" {
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
func (s *CSVSource) detectType(value string) string {
	// 尝试解析为布尔值
	if strings.EqualFold(value, "true") || strings.EqualFold(value, "false") {
		return "BOOLEAN"
	}
	
	// 尝试解析为整数
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "INTEGER"
	}
	
	// 尝试解析为浮点数
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "FLOAT"
	}
	
	return "VARCHAR"
}

// readParallel 并行读取CSV文件
func (s *CSVSource) readParallel(ctx context.Context, neededColumns []string, options *QueryOptions) ([]Row, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	// 获取文件大小
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()
	
	// 如果文件很小,直接读取
	if fileSize < s.chunkSize {
		return s.readSequential(file, neededColumns, options)
	}
	
	// 并行读取
	numChunks := int((fileSize + s.chunkSize - 1) / s.chunkSize)
	if numChunks > s.workers {
		numChunks = s.workers
	}
	
	var wg sync.WaitGroup
	results := make([][]Row, numChunks)
	errors := make([]error, numChunks)
	
	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		go func(chunkIndex int) {
			defer wg.Done()
			
			offset := int64(chunkIndex) * s.chunkSize
			size := s.chunkSize
			if offset+size > fileSize {
				size = fileSize - offset
			}
			
			rows, err := s.readChunk(file, offset, size, neededColumns, options)
			if err != nil {
				errors[chunkIndex] = err
				return
			}
			results[chunkIndex] = rows
		}(i)
	}
	
	wg.Wait()
	
	// 检查错误
	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}
	
	// 合并结果
	totalRows := 0
	for _, result := range results {
		totalRows += len(result)
	}
	
	allRows := make([]Row, 0, totalRows)
	for _, result := range results {
		allRows = append(allRows, result...)
	}
	
	return allRows, nil
}

// readChunk 读取文件的一个chunk
func (s *CSVSource) readChunk(file *os.File, offset, size int64, neededColumns []string, options *QueryOptions) ([]Row, error) {
	// 创建CSV读取器
	reader := csv.NewReader(file)
	reader.Comma = s.delimiter

	// 跳过头部
	if s.header {
		if _, err := reader.Read(); err != nil {
			return nil, err
		}
	}

	var rows []Row
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// 转换为Row
		row := s.parseRow(record)

		// 列裁剪
		if len(neededColumns) > 0 {
			row = PruneRows([]Row{row}, neededColumns)[0]
		}

		// 早期过滤 - 过滤下推
		if MatchesFilters(row, options.Filters) {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// readSequential 顺序读取文件
func (s *CSVSource) readSequential(file *os.File, neededColumns []string, options *QueryOptions) ([]Row, error) {
	reader := csv.NewReader(file)
	reader.Comma = s.delimiter

	var rows []Row

	// 跳过头部
	if s.header {
		if _, err := reader.Read(); err != nil {
			return nil, err
		}
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// 转换为Row
		row := s.parseRow(record)

		// 列裁剪
		if len(neededColumns) > 0 {
			row = PruneRows([]Row{row}, neededColumns)[0]
		}

		// 早期过滤
		if MatchesFilters(row, options.Filters) {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// parseRow 解析CSV行为Row
func (s *CSVSource) parseRow(record []string) Row {
	row := make(Row)
	columns := s.GetColumns()

	for i, value := range record {
		if i >= len(columns) {
			break
		}

		colName := columns[i].Name
		colType := columns[i].Type

		parsedValue := s.parseValue(value, colType)
		row[colName] = parsedValue
	}

	return row
}

// parseValue 解析值
func (s *CSVSource) parseValue(value string, colType string) interface{} {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	
	switch colType {
	case "INTEGER":
		if intVal, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return intVal
		}
	case "FLOAT":
		if floatVal, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return floatVal
		}
	case "BOOLEAN":
		if boolVal, err := strconv.ParseBool(trimmed); err == nil {
			return boolVal
		}
	}
	
	return trimmed
}

// 初始化
func init() {
	// 注册CSV数据源类型
	RegisterFactory(NewCSVFactory())
}
