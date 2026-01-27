package slice

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// SliceAdapter 将 []map[string]any 或 []struct 转换为内存数据源
// 适用于将程序内嵌的数据结构提供 SQL 查询和处理能力
type SliceAdapter struct {
	*memory.MVCCDataSource

	mu              sync.RWMutex
	originalData    interface{} // 原始数据 []map[string]any 或 []struct
	tableName       string      // 表名
	databaseName    string      // 数据库名
	writable        bool        // 是否可写
	mvccSupported   bool        // 是否支持 MVCC
	isPointer       bool        // 原始数据是否为指针（用于SyncToOriginal）
	isMapSlice      bool        // 是否为 map slice
	mapSliceType    reflect.Type // map slice 的类型信息
	structFields    []reflect.StructField // 结构体字段信息
}

// NewSliceAdapter 创建一个新的 slice adapter
// data: 原始数据，可以是 []map[string]any 或 []struct
// tableName: 表名
// databaseName: 数据库名
// writable: 是否可写，默认为 true（非指针参数会自动设为 false）
// mvccSupported: 是否支持 MVCC，默认为 true
// 注意：
//   - 如果需要在写入时修改原始数据，data 必须是指针类型
//   - 如果 data 不是指针，会自动设置为不可写模式
func NewSliceAdapter(data interface{}, tableName string, databaseName string, writable, mvccSupported bool) (*SliceAdapter, error) {
	if data == nil {
		return nil, fmt.Errorf("data cannot be nil")
	}

	val := reflect.ValueOf(data)
	originalRef := data
	isPointer := false
	
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("data pointer is nil")
		}
		val = val.Elem()
		isPointer = true
	}

	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("data must be a slice, got %T", data)
	}

	// 如果不是指针，自动设置为不可写（因为无法修改原始数据）
	if !isPointer {
		writable = false
	}

	adapter := &SliceAdapter{
		originalData:  originalRef,
		tableName:     tableName,
		databaseName:  databaseName,
		writable:      writable,
		mvccSupported: mvccSupported,
		isPointer:    isPointer, // 记录是否为指针
	}

	// 检查是 map slice 还是 struct slice
	if val.Len() > 0 {
		elem := val.Index(0).Interface()
		adapter.isMapSlice = isMapStringAny(elem)

			if !adapter.isMapSlice {
				// 获取结构体字段信息
				adapter.structFields = getStructFields(val.Type().Elem())
			} else {
				// 记录 map slice 的类型（使用指针指向的类型）
				adapter.mapSliceType = val.Type()
			}
	} else {
		// 空切片，尝试从类型推断
		elemType := val.Type().Elem()
		adapter.isMapSlice = isMapStringAnyType(elemType)
		if !adapter.isMapSlice && elemType.Kind() == reflect.Struct {
			adapter.structFields = getStructFields(elemType)
		} else {
			adapter.mapSliceType = val.Type()
		}
	}

	// 创建 MVCCDataSource
	config := &domain.DataSourceConfig{
		Writable: writable,
	}
	adapter.MVCCDataSource = memory.NewMVCCDataSource(config)

	// 转换并加载数据
	if err := adapter.loadData(); err != nil {
		return nil, fmt.Errorf("failed to load data: %w", err)
	}

	return adapter, nil
}

// loadData 从原始数据加载数据到 MVCCDataSource
func (a *SliceAdapter) loadData() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	val := reflect.ValueOf(a.originalData)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Slice {
		return fmt.Errorf("data must be a slice, got %v", val.Kind())
	}

	// 获取所有行数据
	var rows []domain.Row
	var schema *domain.TableInfo

	if a.isMapSlice {
		// 处理 []map[string]any
		schema, rows = a.convertMapSlice(val)
	} else {
		// 处理 []struct
		schema, rows = a.convertStructSlice(val)
	}

	// 加载到 MVCCDataSource
	if err := a.LoadTable(a.tableName, schema, rows); err != nil {
		return fmt.Errorf("failed to load table: %w", err)
	}

	return nil
}

// convertMapSlice 将 []map[string]any 转换为 TableInfo 和 []Row
func (a *SliceAdapter) convertMapSlice(sliceValue reflect.Value) (*domain.TableInfo, []domain.Row) {
	if sliceValue.Len() == 0 {
		// 空切片，创建默认 schema
		return &domain.TableInfo{
			Name:    a.tableName,
			Columns: []domain.ColumnInfo{},
		}, []domain.Row{}
	}

	// 收集所有列
	columnSet := make(map[string]bool)
	columns := []domain.ColumnInfo{}
	var rows []domain.Row

	// 第一次遍历：收集所有列
	for i := 0; i < sliceValue.Len(); i++ {
		elem := sliceValue.Index(i)
		if elem.Kind() == reflect.Map {
			for _, key := range elem.MapKeys() {
				keyStr := key.String()
				if !columnSet[keyStr] {
					columnSet[keyStr] = true
				// 推断列类型
				val := elem.MapIndex(key).Interface()
				colType := inferColumnType(val)
					columns = append(columns, domain.ColumnInfo{
						Name:     keyStr,
						Type:     colType,
						Nullable: true,
					})
				}
			}
		}
	}

	// 第二次遍历：构建行数据
	for i := 0; i < sliceValue.Len(); i++ {
		elem := sliceValue.Index(i)
		if elem.Kind() == reflect.Map {
			row := make(domain.Row, len(columns))
			for _, col := range columns {
				val := elem.MapIndex(reflect.ValueOf(col.Name))
				if val.IsValid() {
					row[col.Name] = val.Interface()
				} else {
					row[col.Name] = nil
				}
			}
			rows = append(rows, row)
		}
	}

	return &domain.TableInfo{
		Name:    a.tableName,
		Columns: columns,
	}, rows
}

// convertStructSlice 将 []struct 转换为 TableInfo 和 []Row
func (a *SliceAdapter) convertStructSlice(sliceValue reflect.Value) (*domain.TableInfo, []domain.Row) {
	// 构建列信息
	columns := []domain.ColumnInfo{}
	for _, field := range a.structFields {
		colType := getFieldType(field.Type)
		columns = append(columns, domain.ColumnInfo{
			Name:     field.Name,
			Type:     colType,
			Nullable: isFieldNullable(field.Type),
		})
	}

	// 构建行数据
	var rows []domain.Row
	for i := 0; i < sliceValue.Len(); i++ {
		elem := sliceValue.Index(i)
		row := make(domain.Row, len(a.structFields))
		for _, field := range a.structFields {
			fieldVal := elem.FieldByName(field.Name)
			if fieldVal.IsValid() {
				row[field.Name] = fieldVal.Interface()
			} else {
				row[field.Name] = nil
			}
		}
		rows = append(rows, row)
	}

	return &domain.TableInfo{
		Name:    a.tableName,
		Columns: columns,
	}, rows
}

// isMapStringAny 检查是否为 map[string]any
func isMapStringAny(v interface{}) bool {
	if v == nil {
		return false
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Kind() == reflect.Map && t.Key().Kind() == reflect.String
}

// isMapStringAnyType 检查类型是否为 map[string]any
func isMapStringAnyType(t reflect.Type) bool {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Kind() == reflect.Map && t.Key().Kind() == reflect.String
}

// getStructFields 获取结构体的所有可导出字段
func getStructFields(t reflect.Type) []reflect.StructField {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	var fields []reflect.StructField
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath == "" { // 可导出字段
			fields = append(fields, field)
		}
	}
	return fields
}

// inferColumnTypes 推断列类型
func inferColumnType(value interface{}) string {
	if value == nil {
		return "any"
	}

	switch value.(type) {
	case int, int8, int16, int32, int64:
		return "int64"
	case uint, uint8, uint16, uint32, uint64:
		return "int64"
	case float32, float64:
		return "float64"
	case bool:
		return "bool"
	case string:
		return "string"
	default:
		return "any"
	}
}

// getFieldType 获取字段类型
func getFieldType(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int64"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int64"
	case reflect.Float32, reflect.Float64:
		return "float64"
	case reflect.Bool:
		return "bool"
	case reflect.String:
		return "string"
	case reflect.Interface:
		return "any"
	default:
		return "string"
	}
}

// isFieldNullable 检查字段是否可空
func isFieldNullable(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr
}

// SyncToOriginal 将 MVCCDataSource 中的变更同步回原始数据
// 仅当原始数据是 []map[string]any 时有效
// 注意：原始数据必须是指针类型才能修改
func (a *SliceAdapter) SyncToOriginal(ctx context.Context) error {
	if !a.writable {
		return domain.NewErrReadOnly("slice", "sync to original")
	}

	if !a.isMapSlice {
		return fmt.Errorf("sync to original only supported for []map[string]any, not []struct")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// 获取当前表数据
	_, rows, err := a.GetLatestTableData(a.tableName)
	if err != nil {
		return fmt.Errorf("failed to get table data: %w", err)
	}

	// 获取原始 slice 的 reflect.Value（必须是指针）
	val := reflect.ValueOf(a.originalData)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("original data must be a pointer to enable sync")
	}
	
	sliceVal := val.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("original data is not a slice")
	}

	if !sliceVal.CanSet() {
		return fmt.Errorf("original data is not settable")
	}

	// 创建新的 slice
	newSlice := reflect.MakeSlice(sliceVal.Type(), len(rows), len(rows))
	for i, row := range rows {
		rowMap := make(map[string]interface{})
		for colName, v := range row {
			rowMap[colName] = v
		}
		newSlice.Index(i).Set(reflect.ValueOf(rowMap))
	}

	// 更新原始数据
	sliceVal.Set(newSlice)

	return nil
}

// GetDatabaseName 获取数据库名
func (a *SliceAdapter) GetDatabaseName() string {
	return a.databaseName
}

// GetTableName 获取表名
func (a *SliceAdapter) GetTableName() string {
	return a.tableName
}

// IsWritable 是否可写
func (a *SliceAdapter) IsWritable() bool {
	return a.writable
}

// SupportsMVCC 是否支持 MVCC
func (a *SliceAdapter) SupportsMVCC() bool {
	return a.writable && a.mvccSupported
}

// SupportsWrite 是否支持写入
func (a *SliceAdapter) SupportsWrite() bool {
	return a.writable
}

// Connect 连接数据源
func (a *SliceAdapter) Connect(ctx context.Context) error {
	return a.MVCCDataSource.Connect(ctx)
}

// Close 关闭数据源
func (a *SliceAdapter) Close(ctx context.Context) error {
	return a.MVCCDataSource.Close(ctx)
}

// IsConnected 是否已连接
func (a *SliceAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// GetTables 获取所有表名
func (a *SliceAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo 获取表信息
func (a *SliceAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query 执行查询
func (a *SliceAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// Insert 插入数据
func (a *SliceAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("slice", "insert")
	}
	// 不在insert后同步，只在commit时同步
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据
func (a *SliceAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("slice", "update")
	}
	// 不在update后同步，只在commit时同步
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据
func (a *SliceAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("slice", "delete")
	}
	// 不在delete后同步，只在commit时同步
	return a.MVCCDataSource.Delete(ctx, tableName, filters, options)
}

// TruncateTable 清空表
func (a *SliceAdapter) TruncateTable(ctx context.Context, tableName string) error {
	if !a.writable {
		return domain.NewErrReadOnly("slice", "truncate table")
	}
	return a.MVCCDataSource.TruncateTable(ctx, tableName)
}

// LoadTable 加载表数据
func (a *SliceAdapter) LoadTable(tableName string, schema *domain.TableInfo, rows []domain.Row) error {
	return a.MVCCDataSource.LoadTable(tableName, schema, rows)
}

// GetVersion 获取版本号
func (a *SliceAdapter) GetVersion() int64 {
	return a.MVCCDataSource.GetCurrentVersion()
}

// BeginTx 开始事务
func (a *SliceAdapter) BeginTx(ctx context.Context, readOnly bool) (int64, error) {
	return a.MVCCDataSource.BeginTx(ctx, readOnly)
}

// CommitTx 提交事务
func (a *SliceAdapter) CommitTx(ctx context.Context, txnID int64) error {
	// 先提交MVCC事务
	err := a.MVCCDataSource.CommitTx(ctx, txnID)
	if err != nil {
		return err
	}
	
	// 只有commit成功后，才同步回原始数据
	if a.isMapSlice && a.isPointer {
		_ = a.SyncToOriginal(ctx)
	}
	
	return nil
}

// RollbackTx 回滚事务
func (a *SliceAdapter) RollbackTx(ctx context.Context, txnID int64) error {
	return a.MVCCDataSource.RollbackTx(ctx, txnID)
}
