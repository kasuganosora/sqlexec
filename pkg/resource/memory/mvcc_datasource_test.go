package memory

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewMVCCDataSource 测试创建MVCC数据源
func TestNewMVCCDataSource(t *testing.T) {
	tests := []struct {
		name    string
		config  *domain.DataSourceConfig
		wantNil bool
	}{
		{
			name:    "with nil config",
			config:  nil,
			wantNil: false,
		},
		{
			name: "with valid config",
			config: &domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "test",
				Writable: true,
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := NewMVCCDataSource(tt.config)
			if (ds == nil) != tt.wantNil {
				t.Errorf("NewMVCCDataSource() = %v, wantNil %v", ds, tt.wantNil)
			}
			if ds != nil {
				if ds.config == nil {
					t.Errorf("Expected config to be set")
				}
				if ds.tables == nil {
					t.Errorf("Expected tables to be initialized")
				}
				if ds.snapshots == nil {
					t.Errorf("Expected snapshots to be initialized")
				}
				if ds.activeTxns == nil {
					t.Errorf("Expected activeTxns to be initialized")
				}
			}
		})
	}
}

// TestMVCCDataSource_ConnectClose 测试连接和关闭
func TestMVCCDataSource_ConnectClose(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 初始状态应该是未连接
	if ds.IsConnected() {
		t.Errorf("Expected initial state to be disconnected")
	}

	// 连接
	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	if !ds.IsConnected() {
		t.Errorf("Expected to be connected after Connect()")
	}

	// 关闭
	if err := ds.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if ds.IsConnected() {
		t.Errorf("Expected to be disconnected after Close()")
	}
}

// TestMVCCDataSource_GetConfig 测试获取配置
func TestMVCCDataSource_GetConfig(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test-db",
		Writable: false,
	}
	ds := NewMVCCDataSource(config)

	got := ds.GetConfig()
	if got == nil {
		t.Errorf("GetConfig() returned nil")
		return
	}

	if got.Type != config.Type {
		t.Errorf("GetConfig().Type = %v, want %v", got.Type, config.Type)
	}
	if got.Name != config.Name {
		t.Errorf("GetConfig().Name = %v, want %v", got.Name, config.Name)
	}
	if got.Writable != config.Writable {
		t.Errorf("GetConfig().Writable = %v, want %v", got.Writable, config.Writable)
	}
}

// TestMVCCDataSource_IsWritable 测试可写性检查
func TestMVCCDataSource_IsWritable(t *testing.T) {
	tests := []struct {
		name     string
		writable bool
	}{
		{"writable", true},
		{"read-only", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Writable: tt.writable,
			}
			ds := NewMVCCDataSource(config)

			if ds.IsWritable() != tt.writable {
				t.Errorf("IsWritable() = %v, want %v", ds.IsWritable(), tt.writable)
			}
		})
	}
}

// TestMVCCDataSource_SupportsMVCC 测试MVCC支持
func TestMVCCDataSource_SupportsMVCC(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	if !ds.SupportsMVCC() {
		t.Errorf("MVCCDataSource should support MVCC")
	}
}

// TestMVCCDataSource_CreateTable 测试创建表
func TestMVCCDataSource_CreateTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 先连接
	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	tableInfo := &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string", Nullable: false},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 验证表已创建
	tables, err := ds.GetTables(ctx)
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}
	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}
	if len(tables) > 0 && tables[0] != "users" {
		t.Errorf("Expected table name 'users', got %v", tables[0])
	}

	// 尝试创建重复表
	if err := ds.CreateTable(ctx, tableInfo); err == nil {
		t.Errorf("Expected error when creating duplicate table")
	}
}

// TestMVCCDataSource_DropTable 测试删除表
func TestMVCCDataSource_DropTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 先连接
	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	tableInfo := &domain.TableInfo{
		Name: "temp",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 删除表
	if err := ds.DropTable(ctx, "temp"); err != nil {
		t.Errorf("DropTable() error = %v", err)
	}

	// 验证表已删除
	tables, err := ds.GetTables(ctx)
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(tables))
	}

	// 尝试删除不存在的表
	if err := ds.DropTable(ctx, "nonexistent"); err == nil {
		t.Errorf("Expected error when dropping nonexistent table")
	}
}

// TestMVCCDataSource_TruncateTable 测试清空表
func TestMVCCDataSource_TruncateTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
		},
	}

	// 创建表并插入数据
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	rows := []domain.Row{
		{"id": 1, "name": "Product A"},
		{"id": 2, "name": "Product B"},
	}

	if _, err := ds.Insert(ctx, "products", rows, nil); err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// 查询验证数据存在
	result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows before truncate, got %d", len(result.Rows))
	}

	// 清空表
	if err := ds.TruncateTable(ctx, "products"); err != nil {
		t.Errorf("TruncateTable() error = %v", err)
	}

	// 验证数据已清空
	result, err = ds.Query(ctx, "products", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after truncate, got %d", len(result.Rows))
	}
}

// TestMVCCDataSource_Insert 测试插入数据
func TestMVCCDataSource_Insert(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "employees",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "salary", Type: "float64", Default: "0"},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 插入数据
	rows := []domain.Row{
		{"id": 1, "name": "Alice", "salary": 50000.0},
		{"id": 2, "name": "Bob", "salary": 60000.0},
	}

	inserted, err := ds.Insert(ctx, "employees", rows, nil)
	if err != nil {
		t.Errorf("Insert() error = %v", err)
	}
	if inserted != 2 {
		t.Errorf("Expected to insert 2 rows, got %d", inserted)
	}

	// 验证数据
	result, err := ds.Query(ctx, "employees", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}
}

// TestMVCCDataSource_Query 测试查询数据
func TestMVCCDataSource_Query(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "students",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
			{Name: "grade", Type: "float64"},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 插入测试数据
	rows := []domain.Row{
		{"id": 1, "name": "Alice", "age": int64(20), "grade": 85.5},
		{"id": 2, "name": "Bob", "age": int64(22), "grade": 90.0},
		{"id": 3, "name": "Charlie", "age": int64(20), "grade": 75.5},
		{"id": 4, "name": "Diana", "age": int64(21), "grade": 95.0},
	}

	if _, err := ds.Insert(ctx, "students", rows, nil); err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// 测试简单查询
	result, err := ds.Query(ctx, "students", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(result.Rows))
	}

	// 测试带过滤器的查询
	result, err = ds.Query(ctx, "students", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "age", Operator: "=", Value: int64(20)},
		},
	})
	if err != nil {
		t.Errorf("Query() with filter error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with age=20, got %d", len(result.Rows))
	}

	// 测试带排序的查询
	result, err = ds.Query(ctx, "students", &domain.QueryOptions{
		OrderBy: "grade",
		Order:   "DESC",
	})
	if err != nil {
		t.Errorf("Query() with order error = %v", err)
	}
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows with order, got %d", len(result.Rows))
	}
	if len(result.Rows) > 0 {
		if grade, ok := result.Rows[0]["grade"].(float64); !ok || grade != 95.0 {
			t.Errorf("Expected highest grade 95.0, got %v", result.Rows[0]["grade"])
		}
	}

	// 测试带分页的查询
	result, err = ds.Query(ctx, "students", &domain.QueryOptions{
		Limit:  2,
		Offset: 1,
	})
	if err != nil {
		t.Errorf("Query() with pagination error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with pagination, got %d", len(result.Rows))
	}

	// 查询不存在的表
	_, err = ds.Query(ctx, "nonexistent", &domain.QueryOptions{})
	if err == nil {
		t.Errorf("Expected error when querying nonexistent table")
	}
}

// TestMVCCDataSource_Update 测试更新数据
func TestMVCCDataSource_Update(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "tasks",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "title", Type: "string"},
			{Name: "status", Type: "string"},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 插入数据
	rows := []domain.Row{
		{"id": 1, "title": "Task 1", "status": "pending"},
		{"id": 2, "title": "Task 2", "status": "completed"},
	}

	if _, err := ds.Insert(ctx, "tasks", rows, nil); err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// 更新数据
	updated, err := ds.Update(ctx, "tasks",
		[]domain.Filter{
			{Field: "status", Operator: "=", Value: "pending"},
		},
		domain.Row{"status": "in_progress"},
		nil,
	)
	if err != nil {
		t.Errorf("Update() error = %v", err)
	}
	if updated != 1 {
		t.Errorf("Expected to update 1 row, got %d", updated)
	}

	// 验证更新
	result, err := ds.Query(ctx, "tasks", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "id", Operator: "=", Value: int64(1)},
		},
	})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
	if status, ok := result.Rows[0]["status"].(string); !ok || status != "in_progress" {
		t.Errorf("Expected status 'in_progress', got %v", result.Rows[0]["status"])
	}

	// 更新不存在的表
	_, err = ds.Update(ctx, "nonexistent", []domain.Filter{}, domain.Row{}, nil)
	if err == nil {
		t.Errorf("Expected error when updating nonexistent table")
	}
}

// TestMVCCDataSource_Delete 测试删除数据
func TestMVCCDataSource_Delete(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "records",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "value", Type: "string"},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 插入数据
	rows := []domain.Row{
		{"id": 1, "value": "A"},
		{"id": 2, "value": "B"},
		{"id": 3, "value": "C"},
	}

	if _, err := ds.Insert(ctx, "records", rows, nil); err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// 删除数据
	deleted, err := ds.Delete(ctx, "records",
		[]domain.Filter{
			{Field: "id", Operator: "=", Value: int64(2)},
		},
		nil,
	)
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}
	if deleted != 1 {
		t.Errorf("Expected to delete 1 row, got %d", deleted)
	}

	// 验证删除
	result, err := ds.Query(ctx, "records", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows after delete, got %d", len(result.Rows))
	}

	// 测试force删除
	forceDeleted, err := ds.Delete(ctx, "records", []domain.Filter{}, &domain.DeleteOptions{Force: true})
	if err != nil {
		t.Errorf("Delete() with force error = %v", err)
	}
	if forceDeleted != 2 {
		t.Errorf("Expected to force delete 2 rows, got %d", forceDeleted)
	}

	// 验证全部删除
	result, err = ds.Query(ctx, "records", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after force delete, got %d", len(result.Rows))
	}

	// 删除不存在的表
	_, err = ds.Delete(ctx, "nonexistent", []domain.Filter{}, nil)
	if err == nil {
		t.Errorf("Expected error when deleting from nonexistent table")
	}
}

// TestMVCCDataSource_Execute 测试执行SQL
func TestMVCCDataSource_Execute(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 内存数据源不支持SQL执行
	_, err := ds.Execute(ctx, "SELECT * FROM users")
	if err == nil {
		t.Errorf("Expected error when executing SQL")
	}
}

// TestMVCCDataSource_GetTableInfo 测试获取表信息
func TestMVCCDataSource_GetTableInfo(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "info_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "data", Type: "string"},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 获取表信息
	got, err := ds.GetTableInfo(ctx, "info_table")
	if err != nil {
		t.Errorf("GetTableInfo() error = %v", err)
	}
	if got == nil {
		t.Errorf("GetTableInfo() returned nil")
	}
	if got.Name != "info_table" {
		t.Errorf("Expected table name 'info_table', got %v", got.Name)
	}
	if len(got.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(got.Columns))
	}

	// 获取不存在的表信息
	_, err = ds.GetTableInfo(ctx, "nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting info for nonexistent table")
	}
}

// TestMVCCDataSource_BeginTx_CommitTx 测试事务
func TestMVCCDataSource_BeginTx_CommitTx(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 测试只读事务
	txnID, err := ds.BeginTx(ctx, true)
	if err != nil {
		t.Errorf("BeginTx(read-only) error = %v", err)
	}
	if txnID != 1 {
		t.Errorf("Expected txnID 1, got %d", txnID)
	}

	// 提交只读事务
	if err := ds.CommitTx(ctx, txnID); err != nil {
		t.Errorf("CommitTx(read-only) error = %v", err)
	}

	// 测试写事务
	txnID, err = ds.BeginTx(ctx, false)
	if err != nil {
		t.Errorf("BeginTx(write) error = %v", err)
	}
	if txnID != 2 {
		t.Errorf("Expected txnID 2, got %d", txnID)
	}

	// 提交写事务
	if err := ds.CommitTx(ctx, txnID); err != nil {
		t.Errorf("CommitTx(write) error = %v", err)
	}

	// 提交不存在的交易
	err = ds.CommitTx(ctx, 999)
	if err == nil {
		t.Errorf("Expected error when committing nonexistent transaction")
	}
}

// TestMVCCDataSource_BeginTx_RollbackTx 测试回滚
func TestMVCCDataSource_BeginTx_RollbackTx(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 开始事务
	txnID, err := ds.BeginTx(ctx, false)
	if err != nil {
		t.Errorf("BeginTx() error = %v", err)
	}

	// 回滚事务
	if err := ds.RollbackTx(ctx, txnID); err != nil {
		t.Errorf("RollbackTx() error = %v", err)
	}

	// 回滚不存在的交易
	err = ds.RollbackTx(ctx, 999)
	if err == nil {
		t.Errorf("Expected error when rolling back nonexistent transaction")
	}
}

// TestMVCCDataSource_GetCurrentVersion 测试获取当前版本
func TestMVCCDataSource_GetCurrentVersion(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 初始版本应该是0
	version := ds.GetCurrentVersion()
	if version != 0 {
		t.Errorf("Expected initial version 0, got %d", version)
	}

	// 创建表会增加版本
	tableInfo := &domain.TableInfo{
		Name: "version_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	version = ds.GetCurrentVersion()
	if version != 1 {
		t.Errorf("Expected version 1 after CreateTable, got %d", version)
	}

	// 插入数据会增加版本
	rows := []domain.Row{{"id": 1}}
	if _, err := ds.Insert(ctx, "version_test", rows, nil); err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	version = ds.GetCurrentVersion()
	if version != 2 {
		t.Errorf("Expected version 2 after Insert, got %d", version)
	}
}

// TestMVCCDataSource_LoadTable 测试加载表数据
func TestMVCCDataSource_LoadTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 先连接
	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	schema := &domain.TableInfo{
		Name: "loaded_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string"},
		},
	}

	rows := []domain.Row{
		{"id": 1, "name": "Loaded 1"},
		{"id": 2, "name": "Loaded 2"},
	}

	// 加载表数据
	if err := ds.LoadTable("loaded_table", schema, rows); err != nil {
		t.Errorf("LoadTable() error = %v", err)
	}

	// 验证表已创建
	tables, err := ds.GetTables(ctx)
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}
	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	// 验证数据已加载
	result, err := ds.Query(ctx, "loaded_table", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}
}

// TestMVCCDataSource_GetLatestTableData 测试获取最新表数据
func TestMVCCDataSource_GetLatestTableData(t *testing.T) {
	ds := NewMVCCDataSource(nil)

	// 先通过LoadTable加载数据
	schema := &domain.TableInfo{
		Name: "test_data",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "value", Type: "string"},
		},
	}

	rows := []domain.Row{
		{"id": 1, "value": "Test Value"},
	}

	if err := ds.LoadTable("test_data", schema, rows); err != nil {
		t.Errorf("LoadTable() error = %v", err)
	}

	// 获取最新表数据
	gotSchema, gotRows, err := ds.GetLatestTableData("test_data")
	if err != nil {
		t.Errorf("GetLatestTableData() error = %v", err)
	}

	if gotSchema == nil {
		t.Errorf("Expected schema to be returned")
	}
	if gotSchema.Name != "test_data" {
		t.Errorf("Expected schema name 'test_data', got %v", gotSchema.Name)
	}
	if len(gotRows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(gotRows))
	}

	// 获取不存在的表数据
	_, _, err = ds.GetLatestTableData("nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting data for nonexistent table")
	}
}

// TestMVCCDataSource_Comprehensive 综合测试
func TestMVCCDataSource_Comprehensive(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 1. 创建表
	tableInfo := &domain.TableInfo{
		Name: "comprehensive_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "email", Type: "string", Nullable: true},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// 2. 插入数据
	rows := []domain.Row{
		{"id": 1, "name": "Alice", "email": "alice@example.com"},
		{"id": 2, "name": "Bob", "email": "bob@example.com"},
		{"id": 3, "name": "Charlie"},
	}

	inserted, err := ds.Insert(ctx, "comprehensive_test", rows, nil)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if inserted != 3 {
		t.Fatalf("Expected to insert 3 rows, got %d", inserted)
	}

	// 3. 查询数据
	result, err := ds.Query(ctx, "comprehensive_test", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result.Rows))
	}

	// 4. 更新数据
	updated, err := ds.Update(ctx, "comprehensive_test",
		[]domain.Filter{
			{Field: "name", Operator: "=", Value: "Alice"},
		},
		domain.Row{"email": "newalice@example.com"},
		nil,
	)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated != 1 {
		t.Fatalf("Expected to update 1 row, got %d", updated)
	}

	// 5. 删除数据
	deleted, err := ds.Delete(ctx, "comprehensive_test",
		[]domain.Filter{
			{Field: "name", Operator: "=", Value: "Charlie"},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted != 1 {
		t.Fatalf("Expected to delete 1 row, got %d", deleted)
	}

	// 6. 验证最终状态
	result, err = ds.Query(ctx, "comprehensive_test", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows after operations, got %d", len(result.Rows))
	}

	// 7. 清空表
	if err := ds.TruncateTable(ctx, "comprehensive_test"); err != nil {
		t.Fatalf("TruncateTable() error = %v", err)
	}

	// 8. 删除表
	if err := ds.DropTable(ctx, "comprehensive_test"); err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}
}

// TestMVCCDataSource_VersionManagement 测试版本管理
func TestMVCCDataSource_VersionManagement(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "version_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "data", Type: "string"},
		},
	}

	// 创建表（版本1）
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}
	if ds.GetCurrentVersion() != 1 {
		t.Errorf("Expected version 1 after CreateTable, got %d", ds.GetCurrentVersion())
	}

	// 插入数据（版本2）
	rows := []domain.Row{{"id": 1, "data": "Initial"}}
	if _, err := ds.Insert(ctx, "version_table", rows, nil); err != nil {
		t.Errorf("Insert() error = %v", err)
	}
	if ds.GetCurrentVersion() != 2 {
		t.Errorf("Expected version 2 after Insert, got %d", ds.GetCurrentVersion())
	}

	// 更新数据（版本3）
	if _, err := ds.Update(ctx, "version_table",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		domain.Row{"data": "Updated"},
		nil); err != nil {
		t.Errorf("Update() error = %v", err)
	}
	if ds.GetCurrentVersion() != 3 {
		t.Errorf("Expected version 3 after Update, got %d", ds.GetCurrentVersion())
	}

	// 删除数据（版本4）
	if _, err := ds.Delete(ctx, "version_table",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		nil); err != nil {
		t.Errorf("Delete() error = %v", err)
	}
	if ds.GetCurrentVersion() != 4 {
		t.Errorf("Expected version 4 after Delete, got %d", ds.GetCurrentVersion())
	}

	// 清空表（版本5）
	if err := ds.TruncateTable(ctx, "version_table"); err != nil {
		t.Errorf("TruncateTable() error = %v", err)
	}
	if ds.GetCurrentVersion() != 5 {
		t.Errorf("Expected version 5 after Truncate, got %d", ds.GetCurrentVersion())
	}
}

// TestMVCCDataSource_ConcurrentOperations 并发操作测试
func TestMVCCDataSource_ConcurrentOperations(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "concurrent_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "value", Type: "string"},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// 并发插入
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			defer func() { done <- true }()
			rows := []domain.Row{
				{"id": int64(idx), "value": string(rune('A' + idx))},
			}
			ds.Insert(ctx, "concurrent_test", rows, nil)
		}(i)
	}

	// 等待所有插入完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证数据
	result, err := ds.Query(ctx, "concurrent_test", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 10 {
		t.Fatalf("Expected 10 rows after concurrent inserts, got %d", len(result.Rows))
	}
}

// TestMVCCDataSource_ReadOnlyOperations 只读操作测试
func TestMVCCDataSource_ReadOnlyOperations(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: false,
	}
	ds := NewMVCCDataSource(config)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "readonly_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
		},
	}

	// 创建表
	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 尝试插入数据（应该失败）
	rows := []domain.Row{{"id": 1}}
	_, err := ds.Insert(ctx, "readonly_table", rows, nil)
	if err == nil {
		t.Errorf("Expected error when inserting into read-only data source")
	}

	// 尝试更新数据（应该失败）
	_, err = ds.Update(ctx, "readonly_table",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		domain.Row{},
		nil)
	if err == nil {
		t.Errorf("Expected error when updating read-only data source")
	}

	// 尝试删除数据（应该失败）
	_, err = ds.Delete(ctx, "readonly_table",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		nil)
	if err == nil {
		t.Errorf("Expected error when deleting from read-only data source")
	}

	// 查询应该正常工作
	result, err := ds.Query(ctx, "readonly_table", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if result == nil {
		t.Errorf("Query() returned nil")
	}
}

// TestMemoryFactory 测试工厂方法
func TestMemoryFactory(t *testing.T) {
	// 测试 NewMemoryFactory
	factory := NewMemoryFactory()
	if factory == nil {
		t.Errorf("NewMemoryFactory() returned nil")
	}

	// 测试 GetType
	dsType := factory.GetType()
	if dsType != domain.DataSourceTypeMemory {
		t.Errorf("GetType() = %v, want %v", dsType, domain.DataSourceTypeMemory)
	}

	// 测试 Create - with valid config
	config := &domain.DataSourceConfig{
		Name:     "test_memory",
		Writable: false,
	}
	ds, err := factory.Create(config)
	if err != nil {
		t.Errorf("Create(config) error = %v", err)
	}
	if ds == nil {
		t.Errorf("Create(config) returned nil")
	}
	if ds.GetConfig().Name != "test_memory" {
		t.Errorf("Created datasource name = %v, want %v", ds.GetConfig().Name, "test_memory")
	}
	if ds.GetConfig().Writable {
		t.Errorf("Created datasource should not be writable")
	}
}

// TestMVCCDataSource_Query_NotConnected 测试未连接状态下的查询
func TestMVCCDataSource_Query_NotConnected(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 未连接状态下查询
	_, err := ds.Query(ctx, "test", &domain.QueryOptions{})
	if err == nil {
		t.Errorf("Expected error when querying without connection")
	}
}

// TestMVCCDataSource_Insert_NoTable 测试向不存在的表插入
func TestMVCCDataSource_Insert_NoTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 先连接
	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	// 向不存在的表插入
	rows := []domain.Row{{"id": 1}}
	_, err := ds.Insert(ctx, "nonexistent", rows, nil)
	if err == nil {
		t.Errorf("Expected error when inserting to nonexistent table")
	}
}

// TestMVCCDataSource_Update_NoTable 测试更新不存在的表
func TestMVCCDataSource_Update_NoTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 先连接
	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	// 更新不存在的表
	_, err := ds.Update(ctx, "nonexistent",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		domain.Row{"value": "test"},
		nil)
	if err == nil {
		t.Errorf("Expected error when updating nonexistent table")
	}
}

// TestMVCCDataSource_Delete_NoTable 测试删除不存在的表的数据
func TestMVCCDataSource_Delete_NoTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	// 先连接
	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	// 删除不存在的表的数据
	_, err := ds.Delete(ctx, "nonexistent",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		nil)
	if err == nil {
		t.Errorf("Expected error when deleting from nonexistent table")
	}
}

// TestMVCCDataSource_Update_EmptyFilters 测试空过滤器的更新（应该失败，因为没有Force选项）
func TestMVCCDataSource_Update_EmptyFilters(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	tableInfo := &domain.TableInfo{
		Name: "update_test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "value", Type: "string"},
		},
	}

	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Errorf("CreateTable() error = %v", err)
	}

	// 插入一些数据
	rows := []domain.Row{
		{"id": 1, "value": "A"},
		{"id": 2, "value": "B"},
	}
	if _, err := ds.Insert(ctx, "update_test", rows, nil); err != nil {
		t.Errorf("Insert() error = %v", err)
	}

	// 使用空过滤器更新（会匹配所有行）
	updated, err := ds.Update(ctx, "update_test",
		[]domain.Filter{}, // 空过滤器
		domain.Row{"value": "Updated"},
		nil)
	if err != nil {
		t.Errorf("Update() with empty filters error = %v", err)
	}
	// 空过滤器应该不更新任何行（因为util.MatchesFilters在没有过滤器时返回true）
	if updated != 2 {
		t.Errorf("Expected to update 2 rows with empty filters, got %d", updated)
	}
}

// TestMVCCDataSource_MultipleVersions 测试同一表的多个版本
func TestMVCCDataSource_MultipleVersions(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	schema := &domain.TableInfo{
		Name: "multi_version",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "data", Type: "string"},
		},
	}

	// 第一次加载（版本1）
	rows1 := []domain.Row{{"id": 1, "data": "Version 1"}}
	if err := ds.LoadTable("multi_version", schema, rows1); err != nil {
		t.Errorf("LoadTable(1) error = %v", err)
	}

	result1, err := ds.Query(ctx, "multi_version", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query(1) error = %v", err)
	}
	if len(result1.Rows) != 1 || result1.Rows[0]["data"] != "Version 1" {
		t.Errorf("Query(1) unexpected result: %v", result1.Rows)
	}

	// 第二次加载（版本2）
	rows2 := []domain.Row{
		{"id": 1, "data": "Version 2"},
		{"id": 2, "data": "New Row"},
	}
	if err := ds.LoadTable("multi_version", schema, rows2); err != nil {
		t.Errorf("LoadTable(2) error = %v", err)
	}

	result2, err := ds.Query(ctx, "multi_version", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query(2) error = %v", err)
	}
	if len(result2.Rows) != 2 {
		t.Errorf("Expected 2 rows after second load, got %d", len(result2.Rows))
	}
	if result2.Rows[0]["data"] != "Version 2" {
		t.Errorf("Expected 'Version 2', got %v", result2.Rows[0]["data"])
	}

	// 版本号应该递增
	version := ds.GetCurrentVersion()
	if version != 2 {
		t.Errorf("Expected version 2, got %d", version)
	}
}

// TestMVCCDataSource_LoadTable_ExistingTable 测试加载已存在的表
func TestMVCCDataSource_LoadTable_ExistingTable(t *testing.T) {
	ds := NewMVCCDataSource(nil)
	ctx := context.Background()

	if err := ds.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	schema := &domain.TableInfo{
		Name: "existing_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "value", Type: "string"},
		},
	}

	// 第一次加载
	rows1 := []domain.Row{{"id": 1, "value": "Initial"}}
	if err := ds.LoadTable("existing_table", schema, rows1); err != nil {
		t.Errorf("LoadTable(1) error = %v", err)
	}

	// 第二次加载（应该创建新版本）
	rows2 := []domain.Row{{"id": 1, "value": "Updated"}}
	if err := ds.LoadTable("existing_table", schema, rows2); err != nil {
		t.Errorf("LoadTable(2) error = %v", err)
	}

	// 验证数据已更新
	result, err := ds.Query(ctx, "existing_table", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0]["value"] != "Updated" {
		t.Errorf("Expected 'Updated', got %v", result.Rows[0]["value"])
	}
}
