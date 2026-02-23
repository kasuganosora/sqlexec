package slice

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// SliceAdapter 将 []map[string]any 或 []struct 转换为内存数据源
// 适用于将程序内嵌的数据结构提供 SQL 查询和处理能力
type SliceAdapter struct {
	*memory.MVCCDataSource

	mu            sync.RWMutex
	syncMu        sync.Mutex            // 序列化 CommitTx + SyncToOriginal
	originalData  interface{}           // 原始数据 []map[string]any 或 []struct
	tableName     string                // 表名
	databaseName  string                // 数据库名
	writable      bool                  // 是否可写
	mvccSupported bool                  // 是否支持 MVCC
	isPointer     bool                  // 原始数据是否为指针（用于SyncToOriginal）
	isMapSlice    bool                  // 是否为 map slice
	mapSliceType  reflect.Type          // map slice 的类型信息
	structFields  []reflect.StructField // 结构体字段信息
	fieldMappings []fieldMapping        // struct 字段到列的映射（含 tag 解析结果）
}

// New 创建 SliceAdapter（推荐的构造方式）
// data: 原始数据，可以是 []map[string]any 或 []struct，推荐传指针以支持写入和同步
// tableName: 表名，不能为空
// opts: 可选配置（WithWritable, WithMVCC, WithDatabaseName）
func New(data interface{}, tableName string, opts ...Option) (*SliceAdapter, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewSliceAdapter(data, tableName, cfg.databaseName, cfg.writable, cfg.mvccSupported)
}

// FromMapSlice 为 []map[string]any 数据创建 SliceAdapter
// data 推荐传指针（*[]map[string]any）以支持写入和同步
func FromMapSlice(data *[]map[string]any, tableName string, opts ...Option) (*SliceAdapter, error) {
	return New(data, tableName, opts...)
}

// FromStructSlice 为 struct slice 数据创建 SliceAdapter
// data 必须是指向 struct slice 的指针（如 &[]User{}）
func FromStructSlice(data interface{}, tableName string, opts ...Option) (*SliceAdapter, error) {
	return New(data, tableName, opts...)
}

// NewSliceAdapter 创建一个新的 slice adapter（保留兼容旧 API）
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
	if tableName == "" {
		return nil, fmt.Errorf("tableName cannot be empty")
	}
	if databaseName == "" {
		databaseName = "default"
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
		isPointer:     isPointer,
	}

	// 检查是 map slice 还是 struct slice
	if val.Len() > 0 {
		elem := val.Index(0).Interface()
		adapter.isMapSlice = isMapStringAny(elem)

		if !adapter.isMapSlice {
			// 获取结构体字段信息和 tag 映射
			elemType := val.Type().Elem()
			adapter.structFields = getStructFields(elemType)
			adapter.fieldMappings = resolveFieldMappings(elemType)
		} else {
			// 记录 map slice 的类型
			adapter.mapSliceType = val.Type()
		}
	} else {
		// 空切片，尝试从类型推断
		elemType := val.Type().Elem()
		adapter.isMapSlice = isMapStringAnyType(elemType)
		if !adapter.isMapSlice && elemType.Kind() == reflect.Struct {
			adapter.structFields = getStructFields(elemType)
			adapter.fieldMappings = resolveFieldMappings(elemType)
		} else {
			adapter.mapSliceType = val.Type()
		}
	}

	// 创建 MVCCDataSource
	config := &domain.DataSourceConfig{
		Writable: writable,
	}
	adapter.MVCCDataSource = memory.NewMVCCDataSource(config)

	// Mark as connected before loading data (in-memory, no external resource)
	adapter.MVCCDataSource.Connect(context.Background())

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
// 单次遍历收集列和行数据，列按字母排序保证确定性
func (a *SliceAdapter) convertMapSlice(sliceValue reflect.Value) (*domain.TableInfo, []domain.Row) {
	if sliceValue.Len() == 0 {
		return &domain.TableInfo{
			Name:    a.tableName,
			Columns: []domain.ColumnInfo{},
		}, []domain.Row{}
	}

	// 单次遍历：同时收集列和构建行数据
	columnSet := make(map[string]int) // column name -> index in columns slice
	columns := make([]domain.ColumnInfo, 0)
	rows := make([]domain.Row, 0, sliceValue.Len())

	for i := 0; i < sliceValue.Len(); i++ {
		elem := sliceValue.Index(i)
		if elem.Kind() != reflect.Map {
			continue
		}
		row := make(domain.Row, elem.Len())
		for _, key := range elem.MapKeys() {
			keyStr := key.String()
			val := elem.MapIndex(key).Interface()
			row[keyStr] = val

			if _, exists := columnSet[keyStr]; !exists {
				columnSet[keyStr] = len(columns)
				colType := inferColumnType(val)
				columns = append(columns, domain.ColumnInfo{
					Name:     keyStr,
					Type:     colType,
					Nullable: true,
				})
			}
		}
		rows = append(rows, row)
	}

	// 按字母排序列，保证 schema 确定性
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})

	// 补齐缺失列的 nil 值
	for i, row := range rows {
		for _, col := range columns {
			if _, exists := row[col.Name]; !exists {
				rows[i][col.Name] = nil
			}
		}
	}

	return &domain.TableInfo{
		Name:    a.tableName,
		Columns: columns,
	}, rows
}

// convertStructSlice 将 []struct 转换为 TableInfo 和 []Row
// 使用 fieldMappings 支持 struct tag 解析和字段索引访问
func (a *SliceAdapter) convertStructSlice(sliceValue reflect.Value) (*domain.TableInfo, []domain.Row) {
	// 构建列信息（使用 tag 解析的映射）
	columns := make([]domain.ColumnInfo, 0, len(a.fieldMappings))
	activeMappings := make([]fieldMapping, 0, len(a.fieldMappings))

	for _, fm := range a.fieldMappings {
		if fm.Skip {
			continue
		}
		field := sliceValue.Type().Elem()
		if field.Kind() == reflect.Ptr {
			field = field.Elem()
		}
		fieldType := field.Field(fm.FieldIndex).Type
		colType := getFieldType(fieldType)
		columns = append(columns, domain.ColumnInfo{
			Name:     fm.ColumnName,
			Type:     colType,
			Nullable: isFieldNullable(fieldType),
		})
		activeMappings = append(activeMappings, fm)
	}

	// 构建行数据（使用字段索引访问，比 FieldByName 更快）
	rows := make([]domain.Row, 0, sliceValue.Len())
	for i := 0; i < sliceValue.Len(); i++ {
		elem := sliceValue.Index(i)
		row := make(domain.Row, len(activeMappings))
		for _, fm := range activeMappings {
			fieldVal := elem.Field(fm.FieldIndex)
			if fieldVal.IsValid() {
				row[fm.ColumnName] = fieldVal.Interface()
			} else {
				row[fm.ColumnName] = nil
			}
		}
		rows = append(rows, row)
	}

	return &domain.TableInfo{
		Name:    a.tableName,
		Columns: columns,
	}, rows
}

// Reload 从原始数据重新加载到 MVCCDataSource
// 适用于外部修改了原始 Go 变量后刷新内部状态
func (a *SliceAdapter) Reload(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return a.loadData()
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
		rowMap := make(map[string]interface{}, len(row))
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
	return a.MVCCDataSource.Insert(ctx, tableName, rows, options)
}

// Update 更新数据
func (a *SliceAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("slice", "update")
	}
	return a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
}

// Delete 删除数据
func (a *SliceAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("slice", "delete")
	}
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
// 提交成功后，如果原始数据是 map slice 指针，会自动同步回原始数据
func (a *SliceAdapter) CommitTx(ctx context.Context, txnID int64) error {
	a.syncMu.Lock()
	defer a.syncMu.Unlock()

	// 先提交MVCC事务
	err := a.MVCCDataSource.CommitTx(ctx, txnID)
	if err != nil {
		return err
	}

	// 只有commit成功后，才同步回原始数据
	if a.isMapSlice && a.isPointer {
		if syncErr := a.SyncToOriginal(ctx); syncErr != nil {
			return fmt.Errorf("transaction committed but sync to original failed: %w", syncErr)
		}
	}

	return nil
}

// RollbackTx 回滚事务
func (a *SliceAdapter) RollbackTx(ctx context.Context, txnID int64) error {
	return a.MVCCDataSource.RollbackTx(ctx, txnID)
}
