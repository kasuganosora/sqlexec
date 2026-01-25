package json

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
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
	writable := false // JSON默认只读

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

	return &JSONAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(config),
		filePath:       filePath,
		arrayRoot:      arrayRoot,
		writable:       writable,
	}
}

// Connect 连接数据源 - 加载JSON文件到内存
func (a *JSONAdapter) Connect(ctx context.Context) error {
	// 读取JSON文件
	data, err := os.ReadFile(a.filePath)
	if err != nil {
		return domain.NewErrNotConnected("json")
	}

	// 解析JSON
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// 获取数据数组
	var rows []interface{}

	if a.arrayRoot != "" {
		// 从指定根节点获取数组
		if obj, ok := jsonData.(map[string]interface{}); ok {
			if arr, ok := obj[a.arrayRoot].([]interface{}); ok {
				rows = arr
			}
		}
	} else {
		// 尝试直接解析为数组
		if arr, ok := jsonData.([]interface{}); ok {
			rows = arr
		}
	}

	if len(rows) == 0 {
		return fmt.Errorf("no data found in JSON file")
	}

	// 推断列信息
	columns := a.inferColumnTypes(rows)

	// 转换为Row格式
	convertedRows := a.convertToRows(rows)

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

	// 连接MVCC数据源
	return a.MVCCDataSource.Connect(ctx)
}

// Close 关闭连接 - 可选写回JSON文件
func (a *JSONAdapter) Close(ctx context.Context) error {
	// 如果是可写模式，需要写回JSON文件
	if a.writable {
		if err := a.writeBack(); err != nil {
			return fmt.Errorf("failed to write back JSON file: %w", err)
		}
	}

	// 关闭MVCC数据源
	return a.MVCCDataSource.Close(ctx)
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

	// 推断每列的类型
	columns := make([]domain.ColumnInfo, 0, len(fieldsMap))
	for field, values := range fieldsMap {
		colType := a.inferType(values)
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

	// 选择最常见的类型
	maxCount := 0
	bestType := "string"
	for t, count := range typeCounts {
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

	// 转换回JSON数组格式
	jsonArray := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		jsonArray[i] = make(map[string]interface{})
		for k, v := range row {
			jsonArray[i][k] = v
		}
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

	// 写入文件
	return os.WriteFile(a.filePath, data, 0644)
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
