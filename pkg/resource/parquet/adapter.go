package parquet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// parquetMagic is the Parquet file format magic bytes ("PAR1").
var parquetMagic = []byte("PAR1")

// parquetSerializedData is the JSON-serialized interchange format used by this
// adapter to persist and reload table data without requiring an external Parquet
// library.  Real Parquet files (detected via the PAR1 magic bytes) are not
// supported and will produce a descriptive error.
type parquetSerializedData struct {
	TableName string              `json:"table_name"`
	Columns   []domain.ColumnInfo `json:"columns"`
	Rows      []domain.Row        `json:"rows"`
}

// ParquetAdapter Parquet文件数据源适配器
// 继承 MVCCDataSource，只负责Parquet格式的加载和写回
// 注意: 这是一个简化实现，实际使用时应该使用 Apache Arrow/Parquet 库
type ParquetAdapter struct {
	*memory.MVCCDataSource
	filePath  string
	tableName string
	writable  bool
}

// NewParquetAdapter 创建Parquet数据源适配器
func NewParquetAdapter(config *domain.DataSourceConfig, filePath string) *ParquetAdapter {
	tableName := "parquet_data"
	writable := config.Writable

	// 从配置中读取选项
	if config.Options != nil {
		if t, ok := config.Options["table_name"]; ok {
			if str, ok := t.(string); ok && str != "" {
				tableName = str
			}
		}
		if w, ok := config.Options["writable"]; ok {
			if b, ok := w.(bool); ok {
				writable = b
			}
		}
	}

	// Create an internal config copy to ensure Writable is synchronised with
	// the resolved writable flag (matching the pattern used by JSONAdapter).
	internalConfig := *config
	internalConfig.Writable = writable

	return &ParquetAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(&internalConfig),
		filePath:       filePath,
		tableName:      tableName,
		writable:       writable,
	}
}

// Connect 连接数据源 - 加载Parquet文件到内存
func (a *ParquetAdapter) Connect(ctx context.Context) error {
	// 检查文件是否存在
	if _, err := os.Stat(a.filePath); err != nil {
		return domain.NewErrNotConnected("parquet")
	}

	// Read file contents
	data, err := os.ReadFile(a.filePath)
	if err != nil {
		return fmt.Errorf("failed to read file %q: %w", a.filePath, err)
	}

	var columns []domain.ColumnInfo
	var rows []domain.Row

	// Detect real Parquet files by checking the PAR1 magic bytes.
	if len(data) >= 4 && string(data[:4]) == string(parquetMagic) {
		return fmt.Errorf("native Parquet format detected (PAR1 magic bytes); " +
			"reading raw Parquet files requires a dedicated library such as parquet-go; " +
			"please convert the file to JSON interchange format or use a different adapter")
	}

	if len(data) == 0 {
		// Empty file -- use default schema so the adapter is still usable in
		// writable mode.
		columns = []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
			{Name: "value", Type: "string", Nullable: true},
		}
		rows = []domain.Row{}
	} else {
		// Attempt to deserialize from JSON interchange format.
		columns, rows, err = a.readFromJSON(data)
		if err != nil {
			// If JSON parsing fails, the file is in an unrecognised format.
			// Fall back to a default empty schema so the adapter can still
			// connect (useful for writable mode where data will be written
			// later).
			columns = []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false, Primary: true},
				{Name: "value", Type: "string", Nullable: true},
			}
			rows = []domain.Row{}
		}
	}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name:    a.tableName,
		Schema:  "default",
		Columns: columns,
	}

	// 加载到MVCC内存源
	if err := a.LoadTable(a.tableName, tableInfo, rows); err != nil {
		return fmt.Errorf("failed to load Parquet data: %w", err)
	}

	// 连接MVCC数据源
	return a.MVCCDataSource.Connect(ctx)
}

// readFromJSON attempts to parse the file content as the JSON interchange
// format used by this adapter.  It supports two layouts:
//   - A JSON object with "columns" and "rows" fields (parquetSerializedData).
//   - A plain JSON array of objects -- columns are inferred from the first row.
func (a *ParquetAdapter) readFromJSON(data []byte) ([]domain.ColumnInfo, []domain.Row, error) {
	// First try the structured format.
	var structured parquetSerializedData
	if err := json.Unmarshal(data, &structured); err == nil && len(structured.Columns) > 0 {
		// Normalise row value types (JSON numbers decode as float64).
		rows := normaliseRows(structured.Rows, structured.Columns)
		return structured.Columns, rows, nil
	}

	// Fall back to a plain JSON array of objects.
	var rawRows []map[string]interface{}
	if err := json.Unmarshal(data, &rawRows); err != nil {
		return nil, nil, fmt.Errorf("file is not valid JSON: %w", err)
	}

	if len(rawRows) == 0 {
		return nil, nil, fmt.Errorf("JSON array is empty")
	}

	// Infer columns from the first row.
	columns := a.inferColumns(rawRows[0])

	// Convert to domain.Row slice.
	rows := make([]domain.Row, 0, len(rawRows))
	for _, raw := range rawRows {
		rows = append(rows, domain.Row(raw))
	}
	rows = normaliseRows(rows, columns)
	return columns, rows, nil
}

// inferColumns builds a column list from a sample row, detecting types with
// detectType.
func (a *ParquetAdapter) inferColumns(sample map[string]interface{}) []domain.ColumnInfo {
	columns := make([]domain.ColumnInfo, 0, len(sample))
	for key, val := range sample {
		col := domain.ColumnInfo{
			Name:     key,
			Type:     a.detectType(val),
			Nullable: true,
		}
		columns = append(columns, col)
	}
	return columns
}

// normaliseRows converts JSON-decoded float64 values back to int64 where the
// column schema says "int64".
func normaliseRows(rows []domain.Row, columns []domain.ColumnInfo) []domain.Row {
	int64Cols := make(map[string]bool)
	for _, col := range columns {
		if col.Type == "int64" {
			int64Cols[col.Name] = true
		}
	}
	for i, row := range rows {
		for k, v := range row {
			if int64Cols[k] {
				switch fv := v.(type) {
				case float64:
					rows[i][k] = int64(fv)
				}
			}
		}
	}
	return rows
}

// Close 关闭连接 - 可选写回Parquet文件
func (a *ParquetAdapter) Close(ctx context.Context) error {
	// 如果是可写模式，需要写回Parquet文件
	if a.writable {
		if err := a.writeBack(); err != nil {
			return fmt.Errorf("failed to write back Parquet file: %w", err)
		}
	}

	// 关闭MVCC数据源
	return a.MVCCDataSource.Close(ctx)
}

// GetConfig 获取数据源配置
func (a *ParquetAdapter) GetConfig() *domain.DataSourceConfig {
	return a.MVCCDataSource.GetConfig()
}

// GetTables 获取所有表（MVCCDataSource提供）
func (a *ParquetAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo 获取表信息（MVCCDataSource提供）
func (a *ParquetAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query 查询数据（MVCCDataSource提供）
func (a *ParquetAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// Insert 插入数据（MVCCDataSource提供）
func (a *ParquetAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("parquet", "insert")
	}
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据（MVCCDataSource提供）
func (a *ParquetAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("parquet", "update")
	}
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据（MVCCDataSource提供）
func (a *ParquetAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("parquet", "delete")
	}
	return a.MVCCDataSource.Delete(ctx, tableName, filters, options)
}

// CreateTable 创建表（Parquet不支持）
func (a *ParquetAdapter) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return domain.NewErrReadOnly("parquet", "create table")
}

// DropTable 删除表（Parquet不支持）
func (a *ParquetAdapter) DropTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("parquet", "drop table")
}

// TruncateTable 清空表（Parquet不支持）
func (a *ParquetAdapter) TruncateTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("parquet", "truncate table")
}

// Execute 执行SQL（Parquet不支持）
func (a *ParquetAdapter) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation("parquet", "execute SQL")
}

// ==================== 私有方法 ====================

// writeBack 写回Parquet文件
// Data is serialised using the JSON interchange format.  To produce real
// Parquet files, a dedicated library such as parquet-go would be needed.
func (a *ParquetAdapter) writeBack() error {
	// 获取最新数据
	schema, rows, err := a.GetLatestTableData(a.tableName)
	if err != nil {
		return err
	}

	serialized := parquetSerializedData{
		TableName: a.tableName,
		Columns:   schema.Columns,
		Rows:      rows,
	}

	data, err := json.MarshalIndent(serialized, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialise data: %w", err)
	}

	if err := os.WriteFile(a.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %q: %w", a.filePath, err)
	}

	return nil
}

// IsConnected 检查是否已连接（MVCCDataSource提供）
func (a *ParquetAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// IsWritable 检查是否可写
func (a *ParquetAdapter) IsWritable() bool {
	return a.writable
}

// SupportsWrite 实现IsWritableSource接口
func (a *ParquetAdapter) SupportsWrite() bool {
	return a.writable
}

// detectType 检测值的类型（私有方法，供测试使用）
func (a *ParquetAdapter) detectType(value interface{}) string {
	switch v := value.(type) {
	case bool:
		return "bool"
	case float64:
		// 检查是否是整数
		if v == float64(int64(v)) {
			return "int64"
		}
		return "float64"
	case string:
		return "string"
	case nil:
		return "string"
	case []interface{}, map[string]interface{}:
		return "string"
	default:
		return "string"
	}
}
