package excel

import (
	"context"
	"fmt"
	"strconv"

	"github.com/xuri/excelize/v2"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// ExcelAdapter Excel文件数据源适配器
// 继承 MVCCDataSource，只负责Excel格式的加载和写回
type ExcelAdapter struct {
	*memory.MVCCDataSource
	filePath  string
	sheetName string
	writable  bool
	file      *excelize.File
}

// NewExcelAdapter 创建Excel数据源适配器
func NewExcelAdapter(config *domain.DataSourceConfig, filePath string) *ExcelAdapter {
	sheetName := ""
	writable := false // Excel默认只读

	// 从配置中读取选项
	if config.Options != nil {
		if s, ok := config.Options["sheet_name"]; ok {
			if str, ok := s.(string); ok {
				sheetName = str
			}
		}
		if w, ok := config.Options["writable"]; ok {
			if b, ok := w.(bool); ok {
				writable = b
			}
		}
	}

	// 确保config.Writable与writable一致
	config.Writable = writable

	return &ExcelAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(config),
		filePath:       filePath,
		sheetName:      sheetName,
		writable:       writable,
	}
}

// Connect 连接数据源 - 加载Excel文件到内存
func (a *ExcelAdapter) Connect(ctx context.Context) error {
	// 打开Excel文件
	file, err := excelize.OpenFile(a.filePath)
	if err != nil {
		return domain.NewErrNotConnected("excel")
	}

	a.file = file

	// 确定使用的工作表
	sheets := file.GetSheetList()
	if len(sheets) == 0 {
		file.Close()
		return fmt.Errorf("no sheets found in excel file")
	}

	if a.sheetName != "" {
		// 检查指定的工作表是否存在
		found := false
		for _, sheet := range sheets {
			if sheet == a.sheetName {
				found = true
				break
			}
		}
		if !found {
			file.Close()
			return fmt.Errorf("sheet not found: %s", a.sheetName)
		}
	} else {
		// 使用第一个工作表
		a.sheetName = sheets[0]
	}

	// 读取所有行
	rows, err := file.GetRows(a.sheetName)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to read excel rows: %w", err)
	}

	if len(rows) == 0 {
		file.Close()
		return fmt.Errorf("sheet is empty: %s", a.sheetName)
	}

	// 第一行是列头
	headers := rows[0]
	dataRows := rows[1:]

	// 推断列信息（传入headers）
	columns := a.inferColumnTypes(headers, dataRows)
	
	// 转换为Row格式
	convertedRows := a.convertToRows(headers, columns, dataRows)

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name:    a.sheetName,
		Schema:  "",
		Columns: columns,
	}

	// 加载到MVCC内存源
	if err := a.LoadTable(a.sheetName, tableInfo, convertedRows); err != nil {
		file.Close()
		return fmt.Errorf("failed to load Excel data: %w", err)
	}

	// 连接MVCC数据源
	if err := a.MVCCDataSource.Connect(ctx); err != nil {
		file.Close()
		return err
	}

	return nil
}

// Close 关闭连接 - 可选写回Excel文件
func (a *ExcelAdapter) Close(ctx context.Context) error {
	// 如果是可写模式，需要写回Excel文件
	if a.writable && a.file != nil {
		if err := a.writeBack(); err != nil {
			return fmt.Errorf("failed to write back Excel file: %w", err)
		}
	}

	// 关闭Excel文件
	if a.file != nil {
		a.file.Close()
		a.file = nil
	}

	// 关闭MVCC数据源
	return a.MVCCDataSource.Close(ctx)
}

// GetConfig 获取数据源配置
func (a *ExcelAdapter) GetConfig() *domain.DataSourceConfig {
	return a.MVCCDataSource.GetConfig()
}

// GetTables 获取所有表（MVCCDataSource提供）
func (a *ExcelAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo 获取表信息（MVCCDataSource提供）
func (a *ExcelAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query 查询数据（MVCCDataSource提供）
func (a *ExcelAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// Insert 插入数据（MVCCDataSource提供）
func (a *ExcelAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("excel", "insert")
	}
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据（MVCCDataSource提供）
func (a *ExcelAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("excel", "update")
	}
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据（MVCCDataSource提供）
func (a *ExcelAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("excel", "delete")
	}
	return a.MVCCDataSource.Delete(ctx, tableName, filters, options)
}

// CreateTable 创建表（Excel不支持）
func (a *ExcelAdapter) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return domain.NewErrReadOnly("excel", "create table")
}

// DropTable 删除表（Excel不支持）
func (a *ExcelAdapter) DropTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("excel", "drop table")
}

// TruncateTable 清空表（保留header，删除数据行）
func (a *ExcelAdapter) TruncateTable(ctx context.Context, tableName string) error {
	if !a.writable {
		return domain.NewErrReadOnly("excel", "truncate table")
	}

	// 使用MVCCDataSource的truncate（这会创建新版本）
	return a.MVCCDataSource.TruncateTable(ctx, tableName)
}

// Execute 执行SQL（Excel不支持）
func (a *ExcelAdapter) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation("excel", "execute SQL")
}

// ==================== 私有方法 ====================

// inferColumnTypes 推断列类型（使用headers作为列名）
func (a *ExcelAdapter) inferColumnTypes(headers []string, rows [][]string) []domain.ColumnInfo {
	if len(headers) == 0 {
		return []domain.ColumnInfo{}
	}

	if len(rows) == 0 {
		// 如果没有数据行，只返回headers信息
		columns := make([]domain.ColumnInfo, len(headers))
		for i, header := range headers {
			columns[i] = domain.ColumnInfo{
				Name:     header,
				Type:     "string",
				Nullable: true,
			}
		}
		return columns
	}

	// 采样前100行推断类型
	sampleSize := 100
	if len(rows) < sampleSize {
		sampleSize = len(rows)
	}

	typeCounts := make([]map[string]int, len(headers))
	for i := range typeCounts {
		typeCounts[i] = map[string]int{
			"int64":   0,
			"float64": 0,
			"bool":    0,
			"string":  0,
		}
	}

	// 统计每列的类型
	for i := 0; i < sampleSize; i++ {
		row := rows[i]
		for j, value := range row {
			if j >= len(typeCounts) || j >= len(headers) {
				break
			}
			value = value // already trimmed by excelize
			if value == "" {
				continue
			}

			colType := a.detectType(value)
			typeCounts[j][colType]++
		}
	}

	// 选择每列的最常见类型
	columns := make([]domain.ColumnInfo, len(headers))
	for j := 0; j < len(headers); j++ {
		maxCount := 0
		bestType := "string"
		for t, count := range typeCounts[j] {
			if count > maxCount {
				maxCount = count
				bestType = t
			}
		}

		columns[j] = domain.ColumnInfo{
			Name:     headers[j],  // 使用实际的header名称
			Type:     bestType,
			Nullable: true,
		}
	}

	return columns
}

// detectType 检测值的类型
func (a *ExcelAdapter) detectType(value string) string {
	// 尝试解析为布尔值
	if value == "true" || value == "false" {
		return "bool"
	}

	// 尝试解析为整数
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "int64"
	}

	// 尝试解析为浮点数
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "float64"
	}

	return "string"
}

// convertToRows 转换Excel行为Row格式
func (a *ExcelAdapter) convertToRows(headers []string, columns []domain.ColumnInfo, rows [][]string) []domain.Row {
	result := make([]domain.Row, len(rows))

	for i, row := range rows {
		rowMap := make(domain.Row)
		for j, value := range row {
			if j >= len(columns) {
				break
			}

			colName := columns[j].Name
			colType := columns[j].Type
			parsedValue := a.parseValue(value, colType)
			rowMap[colName] = parsedValue
		}
		result[i] = rowMap
	}

	return result
}

// parseValue 解析值
func (a *ExcelAdapter) parseValue(value string, colType string) interface{} {
	if value == "" {
		return nil
	}

	switch colType {
	case "int64":
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	case "float64":
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	case "bool":
		if value == "true" {
			return true
		} else if value == "false" {
			return false
		}
	}

	return value
}

// writeBack 写回Excel文件
func (a *ExcelAdapter) writeBack() error {
	// 获取最新数据
	schema, rows, err := a.GetLatestTableData(a.sheetName)
	if err != nil {
		return err
	}

	// 清空工作表
	if err := a.file.DeleteSheet(a.sheetName); err != nil {
		return err
	}

	// 创建新工作表
	index, err := a.file.NewSheet(a.sheetName)
	if err != nil {
		return err
	}

	a.file.SetActiveSheet(index)

	// 写入header
	for i, col := range schema.Columns {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		a.file.SetCellValue(a.sheetName, cell, col.Name)
	}

	// 写入数据
	for i, row := range rows {
		rowNum := i + 2 // 跳过header行
		for j, col := range schema.Columns {
			cell, _ := excelize.CoordinatesToCellName(j+1, rowNum)
			if val, exists := row[col.Name]; exists {
				a.file.SetCellValue(a.sheetName, cell, val)
			}
		}
	}

	// 保存文件
	return a.file.SaveAs(a.filePath)
}

// IsConnected 检查是否已连接（MVCCDataSource提供）
func (a *ExcelAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// IsWritable 检查是否可写
func (a *ExcelAdapter) IsWritable() bool {
	return a.writable
}

// SupportsWrite 实现IsWritableSource接口
func (a *ExcelAdapter) SupportsWrite() bool {
	return a.writable
}
