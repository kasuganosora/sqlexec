package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/filemeta"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// CSVAdapter CSV文件数据源适配器
// 继承 MVCCDataSource，只负责CSV格式的加载和写回
type CSVAdapter struct {
	*memory.MVCCDataSource
	filePath  string
	delimiter rune
	hasHeader bool
	writable  bool
}

// NewCSVAdapter 创建CSV数据源适配器
func NewCSVAdapter(config *domain.DataSourceConfig, filePath string) *CSVAdapter {
	delimiter := ','
	hasHeader := true
	writable := config.Writable

	// 从配置中读取选项
	if config.Options != nil {
		if d, ok := config.Options["delimiter"]; ok {
			if str, ok := d.(string); ok && len(str) > 0 {
				delimiter = rune(str[0])
			}
		}
		if h, ok := config.Options["header"]; ok {
			if b, ok := h.(bool); ok {
				hasHeader = b
			}
		}
		if w, ok := config.Options["writable"]; ok {
			if b, ok := w.(bool); ok {
				writable = b
			}
		}
	}

	// 创建内部配置副本，确保 Writable 与 Options 一致
	internalConfig := *config
	internalConfig.Writable = writable

	return &CSVAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(&internalConfig),
		filePath:       filePath,
		delimiter:      delimiter,
		hasHeader:      hasHeader,
		writable:       writable,
	}
}

// Connect 连接数据源 - 流式加载CSV文件到内存
func (a *CSVAdapter) Connect(ctx context.Context) error {
	// Check for sidecar metadata
	meta, _ := filemeta.Load(filemeta.MetaPath(a.filePath))

	file, err := os.Open(a.filePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %q: %w", a.filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = a.delimiter

	// Read the header line (if applicable)
	var headers []string
	if a.hasHeader {
		record, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV header: %w", err)
		}
		headers = record
	}

	// Determine schema: use sidecar if available, otherwise infer from data
	var columns []domain.ColumnInfo
	var firstBatch [][]string

	if meta != nil && len(meta.Schema.Columns) > 0 {
		// Use stored schema
		columns = make([]domain.ColumnInfo, len(meta.Schema.Columns))
		for i, col := range meta.Schema.Columns {
			columns[i] = domain.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			}
		}
		// Derive headers from schema if not read from file
		if headers == nil {
			headers = make([]string, len(columns))
			for i, col := range columns {
				headers[i] = col.Name
			}
		}
	} else {
		// Need to read first batch for type inference
		pageSize := a.GetBufferPool().PageSize()
		firstBatch, err = a.readRecords(reader, pageSize)
		if err != nil {
			return fmt.Errorf("failed to read CSV data: %w", err)
		}

		// Generate headers if no header row
		if headers == nil {
			width := 0
			if len(firstBatch) > 0 {
				width = len(firstBatch[0])
			}
			headers = make([]string, width)
			for i := range headers {
				headers[i] = fmt.Sprintf("column_%d", i+1)
			}
		}

		columns = a.inferColumnTypes(headers, firstBatch)
	}

	if len(columns) == 0 && len(headers) == 0 {
		return fmt.Errorf("CSV file is empty")
	}

	// Create table with schema
	tableInfo := &domain.TableInfo{
		Name:    "csv_data",
		Schema:  "",
		Columns: columns,
	}
	if err := a.MVCCDataSource.CreateTable(ctx, tableInfo); err != nil {
		return fmt.Errorf("failed to create CSV table: %w", err)
	}

	// Stream data via BulkLoad
	pageSize := a.GetBufferPool().PageSize()
	if err := a.MVCCDataSource.BulkLoad("csv_data", func(addPage func([]domain.Row)) error {
		// Feed the first batch (read during inference) if any
		if len(firstBatch) > 0 {
			rows := a.convertToRows(headers, columns, firstBatch)
			addPage(rows)
		}

		// Continue reading remaining records in pages
		for {
			batch, err := a.readRecords(reader, pageSize)
			if len(batch) > 0 {
				rows := a.convertToRows(headers, columns, batch)
				addPage(rows)
			}
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				return nil
			}
		}
	}); err != nil {
		return fmt.Errorf("failed to bulk load CSV data: %w", err)
	}

	// Rebuild indexes from sidecar metadata
	if meta != nil {
		for _, idx := range meta.Indexes {
			if err := a.MVCCDataSource.CreateIndexWithColumns(idx.Table, idx.Columns, idx.Type, idx.Unique); err != nil {
				log.Printf("warning: failed to rebuild index %s on %s: %v", idx.Name, idx.Table, err)
			}
		}
	}

	// 连接MVCC数据源
	return a.MVCCDataSource.Connect(ctx)
}

// readRecords reads up to n records from the csv.Reader. Returns nil, nil at EOF.
func (a *CSVAdapter) readRecords(reader *csv.Reader, n int) ([][]string, error) {
	var records [][]string
	for i := 0; i < n; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// PersistIndexMeta saves index metadata to a sidecar file alongside the CSV data file.
func (a *CSVAdapter) PersistIndexMeta(indexes []domain.IndexMetaInfo) error {
	// Read current schema from in-memory table
	tableInfo, err := a.MVCCDataSource.GetTableInfo(context.Background(), "csv_data")
	if err != nil {
		return err
	}

	fm := &filemeta.FileMeta{
		Schema: filemeta.SchemaMeta{
			TableName: tableInfo.Name,
			Columns:   make([]filemeta.ColumnMeta, len(tableInfo.Columns)),
		},
		Indexes: make([]filemeta.IndexMeta, len(indexes)),
	}
	for i, col := range tableInfo.Columns {
		fm.Schema.Columns[i] = filemeta.ColumnMeta{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}
	for i, idx := range indexes {
		fm.Indexes[i] = filemeta.IndexMeta{
			Name:    idx.Name,
			Table:   idx.Table,
			Type:    idx.Type,
			Unique:  idx.Unique,
			Columns: idx.Columns,
		}
	}

	return filemeta.Save(filemeta.MetaPath(a.filePath), fm)
}

// Close 关闭连接 - 可选写回CSV文件
func (a *CSVAdapter) Close(ctx context.Context) error {
	var writeBackErr error
	// 如果是可写模式，需要写回CSV文件
	if a.writable {
		if err := a.writeBack(); err != nil {
			writeBackErr = fmt.Errorf("failed to write back CSV file: %w", err)
		}
	}

	// 始终关闭MVCC数据源，即使写回失败
	closeErr := a.MVCCDataSource.Close(ctx)
	if writeBackErr != nil {
		return writeBackErr
	}
	return closeErr
}

// GetConfig 获取数据源配置
func (a *CSVAdapter) GetConfig() *domain.DataSourceConfig {
	return a.MVCCDataSource.GetConfig()
}

// GetTables 获取所有表（MVCCDataSource提供）
func (a *CSVAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo 获取表信息（MVCCDataSource提供）
func (a *CSVAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query 查询数据（MVCCDataSource提供）
func (a *CSVAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// Insert 插入数据（MVCCDataSource提供）
func (a *CSVAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("csv", "insert")
	}
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据（MVCCDataSource提供）
func (a *CSVAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("csv", "update")
	}
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据（MVCCDataSource提供）
func (a *CSVAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("csv", "delete")
	}
	return a.MVCCDataSource.Delete(ctx, tableName, filters, options)
}

// CreateTable 创建表（CSV不支持）
func (a *CSVAdapter) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return domain.NewErrReadOnly("csv", "create table")
}

// DropTable 删除表（CSV不支持）
func (a *CSVAdapter) DropTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("csv", "drop table")
}

// TruncateTable 清空表（CSV不支持）
func (a *CSVAdapter) TruncateTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("csv", "truncate table")
}

// Execute 执行SQL（CSV不支持）
func (a *CSVAdapter) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation("csv", "execute SQL")
}

// ==================== 私有方法 ====================

// inferColumnTypes 推断列类型
func (a *CSVAdapter) inferColumnTypes(headers []string, rows [][]string) []domain.ColumnInfo {
	if len(rows) == 0 {
		columns := make([]domain.ColumnInfo, len(headers))
		for i, header := range headers {
			columns[i] = domain.ColumnInfo{
				Name:     strings.TrimSpace(header),
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

	typeCounts := make([]map[string]int, len(rows[0]))
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
			if j >= len(typeCounts) {
				break
			}
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}

			colType := a.detectType(value)
			typeCounts[j][colType]++
		}
	}

	// 选择每列的最常见类型
	columns := make([]domain.ColumnInfo, len(headers))
	for j, header := range headers {
		maxCount := 0
		bestType := "string"
		if j < len(typeCounts) {
			for t, count := range typeCounts[j] {
				if count > maxCount {
					maxCount = count
					bestType = t
				}
			}
		}

		columns[j] = domain.ColumnInfo{
			Name:     strings.TrimSpace(header),
			Type:     bestType,
			Nullable: true,
		}
	}

	return columns
}

// detectType 检测值的类型
func (a *CSVAdapter) detectType(value string) string {
	// 尝试解析为布尔值
	if strings.EqualFold(value, "true") || strings.EqualFold(value, "false") {
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

// convertToRows 转换CSV行为Row格式
func (a *CSVAdapter) convertToRows(headers []string, columns []domain.ColumnInfo, rows [][]string) []domain.Row {
	result := make([]domain.Row, len(rows))

	for i, row := range rows {
		rowMap := make(domain.Row, len(columns))
		for j := 0; j < len(columns); j++ {
			colName := strings.TrimSpace(headers[j])
			if j < len(row) {
				rowMap[colName] = a.parseValue(row[j], columns[j].Type)
			} else {
				// CSV rows may have fewer fields than headers; fill with nil
				rowMap[colName] = nil
			}
		}
		result[i] = rowMap
	}

	return result
}

// parseValue 解析值
func (a *CSVAdapter) parseValue(value string, colType string) interface{} {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}

	switch colType {
	case "int64":
		if intVal, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return intVal
		}
	case "float64":
		if floatVal, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return floatVal
		}
	case "bool":
		if boolVal, err := strconv.ParseBool(trimmed); err == nil {
			return boolVal
		}
	}

	return trimmed
}

// writeBack 写回CSV文件
func (a *CSVAdapter) writeBack() error {
	// 获取最新数据
	schema, rows, err := a.GetLatestTableData("csv_data")
	if err != nil {
		return err
	}

	// 打开文件准备写入
	file, err := os.Create(a.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = a.delimiter

	// 写入header
	headers := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		headers[i] = col.Name
	}

	if a.hasHeader {
		if err := writer.Write(headers); err != nil {
			return err
		}
	}

	// 写入数据
	for _, row := range rows {
		record := make([]string, len(headers))
		for i, header := range headers {
			val := row[header]
			if val == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

// IsConnected 检查是否已连接（MVCCDataSource提供）
func (a *CSVAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// IsWritable 检查是否可写
func (a *CSVAdapter) IsWritable() bool {
	return a.writable
}

// SupportsWrite 实现IsWritableSource接口
func (a *CSVAdapter) SupportsWrite() bool {
	return a.writable
}
