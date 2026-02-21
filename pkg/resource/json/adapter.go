package json

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/filemeta"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// JSONAdapter JSON文件数据源适配器
// 继承 MVCCDataSource，只负责JSON格式的加载和写回
type JSONAdapter struct {
	*memory.MVCCDataSource
	filePath  string
	arrayRoot string
	writable  bool
}

// NewJSONAdapter 创建JSON数据源适配器
func NewJSONAdapter(config *domain.DataSourceConfig, filePath string) *JSONAdapter {
	arrayRoot := ""
	writable := config.Writable

	// 从配置中读取选项
	if config.Options != nil {
		if r, ok := config.Options["array_root"]; ok {
			if str, ok := r.(string); ok {
				arrayRoot = str
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

	return &JSONAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(&internalConfig),
		filePath:       filePath,
		arrayRoot:      arrayRoot,
		writable:       writable,
	}
}

// Connect 连接数据源 - 加载JSON文件到内存
func (a *JSONAdapter) Connect(ctx context.Context) error {
	// Check for sidecar metadata
	meta, _ := filemeta.Load(filemeta.MetaPath(a.filePath))

	// 读取JSON文件
	data, err := os.ReadFile(a.filePath)
	if err != nil {
		return fmt.Errorf("failed to read JSON file %q: %w", a.filePath, err)
	}

	// 解析JSON
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// 获取数据数组
	var rows []interface{}
	found := false

	if a.arrayRoot != "" {
		// 从指定根节点获取数组
		if obj, ok := jsonData.(map[string]interface{}); ok {
			if arr, ok := obj[a.arrayRoot].([]interface{}); ok {
				rows = arr
				found = true
			}
		}
	} else {
		// 尝试直接解析为数组
		if arr, ok := jsonData.([]interface{}); ok {
			rows = arr
			found = true
		}
	}

	if !found {
		return fmt.Errorf("no JSON array found in file (use array_root option for nested arrays)")
	}

	// Determine schema: use sidecar if available, otherwise infer from data
	var columns []domain.ColumnInfo
	var convertedRows []domain.Row

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
		if len(rows) > 0 {
			convertedRows = a.convertToRows(rows)
		}
	} else {
		// Infer schema from data
		if len(rows) > 0 {
			columns = a.inferColumnTypes(rows)
			convertedRows = a.convertToRows(rows)
		}
	}

	// 创建表信息
	tableInfo := &domain.TableInfo{
		Name:    "json_data",
		Schema:  "",
		Columns: columns,
	}

	// 加载到MVCC内存源
	if err := a.LoadTable("json_data", tableInfo, convertedRows); err != nil {
		return fmt.Errorf("failed to load JSON data: %w", err)
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

// PersistIndexMeta saves index metadata to a sidecar file alongside the JSON data file.
func (a *JSONAdapter) PersistIndexMeta(indexes []domain.IndexMetaInfo) error {
	tableInfo, err := a.MVCCDataSource.GetTableInfo(context.Background(), "json_data")
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

// Close 关闭连接 - 可选写回JSON文件
func (a *JSONAdapter) Close(ctx context.Context) error {
	var writeBackErr error
	// 如果是可写模式，需要写回JSON文件
	if a.writable {
		if err := a.writeBack(); err != nil {
			writeBackErr = fmt.Errorf("failed to write back JSON file: %w", err)
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
func (a *JSONAdapter) GetConfig() *domain.DataSourceConfig {
	return a.MVCCDataSource.GetConfig()
}

// GetTables 获取所有表（MVCCDataSource提供）
func (a *JSONAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo 获取表信息（MVCCDataSource提供）
func (a *JSONAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query 查询数据（MVCCDataSource提供）
func (a *JSONAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// Insert 插入数据（MVCCDataSource提供）
func (a *JSONAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("json", "insert")
	}
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据（MVCCDataSource提供）
func (a *JSONAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("json", "update")
	}
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据（MVCCDataSource提供）
func (a *JSONAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("json", "delete")
	}
	return a.MVCCDataSource.Delete(ctx, tableName, filters, options)
}

// CreateTable 创建表（JSON不支持）
func (a *JSONAdapter) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return domain.NewErrReadOnly("json", "create table")
}

// DropTable 删除表（JSON不支持）
func (a *JSONAdapter) DropTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("json", "drop table")
}

// TruncateTable 清空表（JSON不支持）
func (a *JSONAdapter) TruncateTable(ctx context.Context, tableName string) error {
	return domain.NewErrReadOnly("json", "truncate table")
}

// Execute 执行SQL（JSON不支持）
func (a *JSONAdapter) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation("json", "execute SQL")
}

// ==================== 私有方法 ====================

// inferColumnTypes 推断列类型
func (a *JSONAdapter) inferColumnTypes(rows []interface{}) []domain.ColumnInfo {
	if len(rows) == 0 {
		return []domain.ColumnInfo{}
	}

	// 采样前100行推断类型
	sampleSize := 100
	if len(rows) < sampleSize {
		sampleSize = len(rows)
	}

	// 收集所有字段
	fieldsMap := make(map[string][]interface{})
	for i := 0; i < sampleSize; i++ {
		if row, ok := rows[i].(map[string]interface{}); ok {
			for key, value := range row {
				fieldsMap[key] = append(fieldsMap[key], value)
			}
		}
	}

	// 排序字段名以保证确定性的列顺序
	fieldNames := make([]string, 0, len(fieldsMap))
	for field := range fieldsMap {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)

	// 推断每列的类型
	columns := make([]domain.ColumnInfo, 0, len(fieldsMap))
	for _, field := range fieldNames {
		colType := a.inferType(fieldsMap[field])
		columns = append(columns, domain.ColumnInfo{
			Name:     field,
			Type:     colType,
			Nullable: true,
		})
	}

	return columns
}

// inferType 推断值的类型
func (a *JSONAdapter) inferType(values []interface{}) string {
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

		colType := a.detectType(value)
		typeCounts[colType]++
	}

	// 选择最常见的类型（固定优先级打破平局：int64 > float64 > bool > string）
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
func (a *JSONAdapter) detectType(value interface{}) string {
	switch v := value.(type) {
	case bool:
		return "bool"
	case float64:
		// 检查是否是整数（必须无小数部分且在int64范围内）
		if v == math.Trunc(v) && !math.IsInf(v, 0) && !math.IsNaN(v) &&
			v >= math.MinInt64 && v <= math.MaxInt64 {
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

// convertToRows 转换JSON行为Row格式
func (a *JSONAdapter) convertToRows(rows []interface{}) []domain.Row {
	result := make([]domain.Row, 0, len(rows))

	for _, row := range rows {
		if rowMap, ok := row.(map[string]interface{}); ok {
			result = append(result, domain.Row(rowMap))
		}
	}

	return result
}

// writeBack 写回JSON文件
func (a *JSONAdapter) writeBack() error {
	// 获取最新数据
	_, rows, err := a.GetLatestTableData("json_data")
	if err != nil {
		return err
	}

	// domain.Row 就是 map[string]interface{}，直接转换无需深拷贝
	jsonArray := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		jsonArray[i] = map[string]interface{}(row)
	}

	var jsonData interface{}
	if a.arrayRoot != "" {
		// 包装在指定根节点下
		jsonData = map[string]interface{}{
			a.arrayRoot: jsonArray,
		}
	} else {
		// 直接是数组
		jsonData = jsonArray
	}

	// 序列化为JSON
	data, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return err
	}

	// 原子写入：先写临时文件，再重命名
	dir := filepath.Dir(a.filePath)
	tmpFile, err := os.CreateTemp(dir, ".json_writeback_*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file for writeBack: %w", err)
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write temp file: %w", err)
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

// IsConnected 检查是否已连接（MVCCDataSource提供）
func (a *JSONAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// IsWritable 检查是否可写
func (a *JSONAdapter) IsWritable() bool {
	return a.writable
}

// SupportsWrite 实现IsWritableSource接口
func (a *JSONAdapter) SupportsWrite() bool {
	return a.writable
}
