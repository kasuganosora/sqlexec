package resource

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/xuri/excelize/v2"
)

// ExcelConfig Excel数据源配�?
type ExcelConfig struct {
	// FilePath Excel文件路径
	FilePath string
	// ReadOnly 是否只读
	ReadOnly bool
	// SheetName 工作表名称（默认使用第一个）
	SheetName string
}

// DefaultExcelConfig 返回默认配置
func DefaultExcelConfig(filePath string) *ExcelConfig {
	return &ExcelConfig{
		FilePath: filePath,
		ReadOnly: true,
	}
}

// ExcelSource Excel数据源实�?
type ExcelSource struct {
	config     *ExcelConfig
	file       *excelize.File
	connected  bool
	sheetName  string
	mu         sync.RWMutex
	dataConfig *DataSourceConfig
}

// NewExcelSource 创建Excel数据�?
func NewExcelSource(config *ExcelConfig) *ExcelSource {
	return &ExcelSource{
		config: config,
		dataConfig: &DataSourceConfig{
			Type:     DataSourceTypeCSV, // Excel暂不单独定义类型
			Name:     "excel",
			Writable: !config.ReadOnly,
		},
	}
}

// Connect 连接到Excel文件
func (s *ExcelSource) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return nil
	}

	// 检查文件是否存�?
	if _, err := os.Stat(s.config.FilePath); os.IsNotExist(err) {
		// 文件不存在，如果是只读模式则报错
		if s.config.ReadOnly {
			return fmt.Errorf("file not found: %s", s.config.FilePath)
		}
		// 可写模式，创建新文件
		s.file = excelize.NewFile()
		s.connected = true
		return nil
	}

	// 打开现有文件
	file, err := excelize.OpenFile(s.config.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open excel file: %w", err)
	}

	s.file = file

	// 获取工作表名�?
	if s.config.SheetName != "" {
		// 检查指定的工作表是否存�?
		sheets := file.GetSheetList()
		found := false
		for _, sheet := range sheets {
			if sheet == s.config.SheetName {
				found = true
				break
			}
		}
		if !found {
			file.Close()
			return fmt.Errorf("sheet not found: %s", s.config.SheetName)
		}
		s.sheetName = s.config.SheetName
	} else {
		// 使用第一个工作表，如果有默认Sheet1则删除它
		sheets := file.GetSheetList()
		if len(sheets) > 0 && sheets[0] == "Sheet1" {
			file.DeleteSheet("Sheet1")
		}
		if len(file.GetSheetList()) == 0 {
			file.Close()
			return fmt.Errorf("no sheets found in excel file")
		}
		sheets = file.GetSheetList()
		s.sheetName = sheets[0]
	}

	s.connected = true
	return nil
}

// Close 关闭连接
func (s *ExcelSource) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected || s.file == nil {
		return nil
	}

	// 如果是可写模式且有修改，保存文件
	if !s.config.ReadOnly {
		if err := s.file.SaveAs(s.config.FilePath); err != nil {
			return err
		}
	}

	if err := s.file.Close(); err != nil {
		return err
	}

	s.connected = false
	return nil
}

// IsConnected 检查是否已连接
func (s *ExcelSource) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// IsWritable 检查是否可�?
func (s *ExcelSource) IsWritable() bool {
	return !s.config.ReadOnly
}

// GetConfig 获取数据源配�?
func (s *ExcelSource) GetConfig() *DataSourceConfig {
	return s.dataConfig
}

// GetTables 获取所有表（工作表�?
func (s *ExcelSource) GetTables(ctx context.Context) ([]string, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	sheets := s.file.GetSheetList()
	return sheets, nil
}

// GetTableInfo 获取表信息（工作表信息）
func (s *ExcelSource) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	// 检查工作表是否存在
	sheets := s.file.GetSheetList()
	found := false
	for _, sheet := range sheets {
		if sheet == tableName {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("sheet not found: %s", tableName)
	}

	// 获取第一行作为列�?
	rows, err := s.file.GetRows(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("sheet is empty: %s", tableName)
	}

	// 第一行是列头
	headers := rows[0]
	columns := make([]ColumnInfo, 0, len(headers))

	for _, header := range headers {
		columns = append(columns, ColumnInfo{
			Name:     header,
			Type:     "STRING", // Excel默认都是字符串类�?
			Nullable: true,
			Primary:  false,
		})
	}

	return &TableInfo{
		Name:    tableName,
		Schema:  filepath.Base(s.config.FilePath),
		Columns: columns,
	}, nil
}

// Query 查询数据
func (s *ExcelSource) Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error) {
	if !s.connected {
		return nil, fmt.Errorf("not connected")
	}

	// 检查工作表是否存在
	sheets := s.file.GetSheetList()
	found := false
	for _, sheet := range sheets {
		if sheet == tableName {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("sheet not found: %s", tableName)
	}

	// 获取所有行
	allRows, err := s.file.GetRows(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	if len(allRows) == 0 {
		return &QueryResult{
			Columns: []ColumnInfo{},
			Rows:    []Row{},
			Total:   0,
		}, nil
	}

	// 第一行是列头
	headers := allRows[0]
	dataRows := allRows[1:]

	// 应用过滤
	if options != nil && len(options.Filters) > 0 {
		filteredRows := make([][]string, 0)
		for _, row := range dataRows {
			if matchFilters(row, headers, options.Filters) {
				filteredRows = append(filteredRows, row)
			}
		}
		dataRows = filteredRows
	}

	// 应用排序
	if options != nil && options.OrderBy != "" {
		sortRows(dataRows, headers, options.OrderBy, options.Order)
	}

	// 应用分页
	if options != nil && (options.Limit > 0 || options.Offset > 0) {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		if start >= len(dataRows) {
			dataRows = [][]string{}
		} else {
			end := start + options.Limit
			if options.Limit <= 0 || end > len(dataRows) {
				end = len(dataRows)
			}
			dataRows = dataRows[start:end]
		}
	}

	// 转换为Row格式
	result := make([]Row, 0, len(dataRows))
	for _, row := range dataRows {
		rowMap := make(map[string]interface{})
		for i, header := range headers {
			if i < len(row) {
				rowMap[header] = row[i]
			} else {
				rowMap[header] = ""
			}
		}
		result = append(result, rowMap)
	}

	// 构建列信�?
	columns := make([]ColumnInfo, 0, len(headers))
	for _, header := range headers {
		columns = append(columns, ColumnInfo{
			Name:     header,
			Type:     "STRING",
			Nullable: true,
		})
	}

	return &QueryResult{
		Columns: columns,
		Rows:    result,
		Total:   int64(len(result)),
	}, nil
}

// Insert 插入数据
func (s *ExcelSource) Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error) {
	if s.config.ReadOnly {
		return 0, fmt.Errorf("excel source is read-only")
	}

	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	// 检查工作表是否存在
	sheets := s.file.GetSheetList()
	found := false
	for _, sheet := range sheets {
		if sheet == tableName {
			found = true
			break
		}
	}
	if !found {
		return 0, fmt.Errorf("sheet not found: %s", tableName)
	}

	// 获取当前行数
	currentRows, err := s.file.GetRows(tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to get current rows: %w", err)
	}

	// 获取列头
	var headers []string
	if len(currentRows) > 0 {
		headers = currentRows[0]
	} else {
		// 空表，不应该发生（CreateTable应该创建列头�?
		return 0, fmt.Errorf("table has no headers")
	}

	// 插入数据行（从第2行开始，因为�?行是列头�?
	startRow := len(currentRows) + 1
	for i, row := range rows {
		rowNum := startRow + i
		for j, header := range headers {
			cell, _ := excelize.CoordinatesToCellName(j+1, rowNum)
			if val, exists := row[header]; exists {
				s.file.SetCellValue(tableName, cell, val)
			}
		}
	}

	return int64(len(rows)), nil
}

// Update 更新数据（Excel不支持原位更新）
func (s *ExcelSource) Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error) {
	if s.config.ReadOnly {
		return 0, fmt.Errorf("excel source is read-only")
	}

	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	// Excel不支持原位更新，需要读取所有数据、修改、重新写�?
	// 这里简化实现：返回错误
	return 0, fmt.Errorf("update not supported for excel source")
}

// Delete 删除数据（Excel不支持）
func (s *ExcelSource) Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error) {
	if s.config.ReadOnly {
		return 0, fmt.Errorf("excel source is read-only")
	}

	if !s.connected {
		return 0, fmt.Errorf("not connected")
	}

	// Excel不支持删除行
	return 0, fmt.Errorf("delete not supported for excel source")
}

// CreateTable 创建表（工作表）
func (s *ExcelSource) CreateTable(ctx context.Context, tableInfo *TableInfo) error {
	if s.config.ReadOnly {
		return fmt.Errorf("excel source is read-only")
	}

	if !s.connected {
		return fmt.Errorf("not connected")
	}

	if tableInfo == nil {
		return fmt.Errorf("table info is required")
	}

	// 检查工作表是否已存�?
	sheets := s.file.GetSheetList()
	for _, sheet := range sheets {
		if sheet == tableInfo.Name {
			// 工作表已存在，返回错误或清空�?
			return fmt.Errorf("sheet already exists: %s", tableInfo.Name)
		}
	}

	// 创建新工作表
	index, err := s.file.NewSheet(tableInfo.Name)
	if err != nil {
		return fmt.Errorf("failed to create sheet: %w", err)
	}

	// 写入列头
	for i, col := range tableInfo.Columns {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		s.file.SetCellValue(tableInfo.Name, cell, col.Name)
	}

	// 设置为活动工作表
	s.file.SetActiveSheet(index)

	return nil
}

// DropTable 删除表（工作表）
func (s *ExcelSource) DropTable(ctx context.Context, tableName string) error {
	if s.config.ReadOnly {
		return fmt.Errorf("excel source is read-only")
	}

	if !s.connected {
		return fmt.Errorf("not connected")
	}

	if err := s.file.DeleteSheet(tableName); err != nil {
		return fmt.Errorf("failed to delete sheet: %w", err)
	}

	return nil
}

// TruncateTable 清空表（删除所有数据但保留结构�?
func (s *ExcelSource) TruncateTable(ctx context.Context, tableName string) error {
	if s.config.ReadOnly {
		return fmt.Errorf("excel source is read-only")
	}

	if !s.connected {
		return fmt.Errorf("not connected")
	}

	// 获取列头
	rows, err := s.file.GetRows(tableName)
	if err != nil {
		return fmt.Errorf("failed to get rows: %w", err)
	}

	if len(rows) == 0 {
		return nil
	}

	headers := rows[0]

	// 清空工作�?
	s.file.DeleteSheet(tableName)
	index, err := s.file.NewSheet(tableName)
	if err != nil {
		return err
	}

	// 重新写入列头
	for i, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		s.file.SetCellValue(tableName, cell, header)
	}

	// 设置为活动工作表
	s.file.SetActiveSheet(index)

	return nil
}

// Execute 执行自定义SQL（不支持�?
func (s *ExcelSource) Execute(ctx context.Context, sql string) (*QueryResult, error) {
	return nil, fmt.Errorf("execute not supported for excel source")
}

// matchFilters 检查行是否匹配所有过滤条�?
func matchFilters(row []string, headers []string, filters []Filter) bool {
	for _, filter := range filters {
		// 找到列索�?
		colIndex := -1
		for i, header := range headers {
			if header == filter.Field {
				colIndex = i
				break
			}
		}

		if colIndex == -1 {
			continue // 列不存在，跳�?
		}

		if colIndex >= len(row) {
			return false
		}

		cellValue := row[colIndex]

		// 根据操作符比�?
		switch filter.Operator {
		case "=":
			if cellValue != fmt.Sprintf("%v", filter.Value) {
				return false
			}
		case "!=":
			if cellValue == fmt.Sprintf("%v", filter.Value) {
				return false
			}
		case ">":
			if !excelCompareNumeric(cellValue, filter.Value, ">") {
				return false
			}
		case ">=":
			if !excelCompareNumeric(cellValue, filter.Value, ">=") {
				return false
			}
		case "<":
			if !excelCompareNumeric(cellValue, filter.Value, "<") {
				return false
			}
		case "<=":
			if !excelCompareNumeric(cellValue, filter.Value, "<=") {
				return false
			}
		case "LIKE":
			if !matchLike(cellValue, fmt.Sprintf("%v", filter.Value)) {
				return false
			}
		}
	}

	return true
}

// excelCompareNumeric 比较数�?
func excelCompareNumeric(a, b interface{}, op string) bool {
	aNum, err1 := strconv.ParseFloat(fmt.Sprintf("%v", a), 64)
	bNum, err2 := strconv.ParseFloat(fmt.Sprintf("%v", b), 64)

	if err1 != nil || err2 != nil {
		// 非数值，按字符串比较
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		switch op {
		case ">":
			return aStr > bStr
		case ">=":
			return aStr >= bStr
		case "<":
			return aStr < bStr
		case "<=":
			return aStr <= bStr
		default:
			return false
		}
	}

	switch op {
	case ">":
		return aNum > bNum
	case ">=":
		return aNum >= bNum
	case "<":
		return aNum < bNum
	case "<=":
		return aNum <= bNum
	default:
		return false
	}
}

// matchLike 匹配LIKE模式
func matchLike(value, pattern string) bool {
	// 简化的LIKE匹配，只支持通配�?
	valueStr := fmt.Sprintf("%v", value)
	patternStr := fmt.Sprintf("%v", pattern)

	// 简单的字符串包含匹�?
	if patternStr == "%" {
		return true
	}

	if patternStr[0] == '%' && patternStr[len(patternStr)-1] == '%' {
		// %pattern%
		subPattern := patternStr[1 : len(patternStr)-1]
		return len(valueStr) >= len(subPattern) &&
			valueStr[0:len(subPattern)] == subPattern ||
			valueStr[len(valueStr)-len(subPattern):] == subPattern
	}

	if patternStr[0] == '%' {
		// %pattern
		return len(valueStr) >= len(patternStr)-1 &&
			valueStr[len(valueStr)-(len(patternStr)-1):] == patternStr[1:]
	}

	if patternStr[len(patternStr)-1] == '%' {
		// pattern%
		return len(valueStr) >= len(patternStr)-1 &&
			valueStr[0:len(patternStr)-1] == patternStr[:len(patternStr)-1]
	}

	// 无通配符，精确匹配
	return valueStr == patternStr
}

// sortRows 对行进行排序
func sortRows(rows [][]string, headers []string, orderBy string, order string) {
	if orderBy == "" {
		return
	}

	// 找到排序列的索引
	colIndex := -1
	for i, header := range headers {
		if header == orderBy {
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		return
	}

	// 简单的冒泡排序（生产环境应使用更高效的算法�?
	n := len(rows)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			var shouldSwap bool
			val1 := ""
			val2 := ""

			if colIndex < len(rows[j]) {
				val1 = rows[j][colIndex]
			}
			if colIndex < len(rows[j+1]) {
				val2 = rows[j+1][colIndex]
			}

			if order == "DESC" {
				shouldSwap = excelCompareNumeric(val1, val2, "<")
			} else {
				// ASC
				shouldSwap = excelCompareNumeric(val1, val2, ">")
			}

			if shouldSwap {
				rows[j], rows[j+1] = rows[j+1], rows[j]
			}
		}
	}
}
