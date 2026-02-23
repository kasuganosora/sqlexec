package excel

import (
	"context"
	"os"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/xuri/excelize/v2"
)

// createTestExcelFile 创建测试用的Excel文件（单sheet）
func createTestExcelFile(t *testing.T) string {
	f := excelize.NewFile()

	// 使用默认的Sheet1作为测试表
	f.SetCellValue("Sheet1", "A1", "id")
	f.SetCellValue("Sheet1", "B1", "name")
	f.SetCellValue("Sheet1", "C1", "email")
	f.SetCellValue("Sheet1", "A2", 1)
	f.SetCellValue("Sheet1", "B2", "Alice")
	f.SetCellValue("Sheet1", "C2", "alice@example.com")
	f.SetCellValue("Sheet1", "A3", 2)
	f.SetCellValue("Sheet1", "B3", "Bob")
	f.SetCellValue("Sheet1", "C3", "bob@example.com")

	// 保存文件
	tmpFile, err := os.CreateTemp("", "test*.xlsx")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	if err := f.SaveAs(tmpFile.Name()); err != nil {
		t.Fatalf("Failed to save Excel file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}

	return tmpFile.Name()
}

// TestNewExcelAdapter 测试创建Excel数据源
func TestNewExcelAdapter(t *testing.T) {
	tests := []struct {
		name     string
		config   *domain.DataSourceConfig
		filePath string
	}{
		{
			name: "with config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeExcel,
				Name: "test-excel",
				Options: map[string]interface{}{
					"sheet_name": "MySheet",
					"writable":   true,
				},
			},
			filePath: "/tmp/test.xlsx",
		},
		{
			name: "without config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeExcel,
				Name: "test-excel",
			},
			filePath: "/tmp/test.xlsx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es := NewExcelAdapter(tt.config, tt.filePath)

			if es == nil {
				t.Errorf("NewExcelAdapter() returned nil")
			}

			if es.filePath != tt.filePath {
				t.Errorf("Expected filePath %s, got %s", tt.filePath, es.filePath)
			}
		})
	}
}

// TestNewExcelAdapter_Connect 测试连接Excel文件
func TestNewExcelAdapter_Connect(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	// 测试连接
	err := es.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	if !es.IsConnected() {
		t.Errorf("Expected to be connected after Connect()")
	}

	// 测试关闭连接
	err = es.Close(ctx)
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if es.IsConnected() {
		t.Errorf("Expected to be disconnected after Close()")
	}
}

// TestExcelSource_GetConfig 测试获取配置
func TestExcelSource_GetConfig(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}
	es := NewExcelAdapter(config, "/tmp/test.xlsx")

	got := es.GetConfig()
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
}

// TestExcelSource_IsWritable 测试可写性
func TestExcelSource_IsWritable(t *testing.T) {
	// 默认只读
	readonlyConfig := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}
	readonlyAdapter := NewExcelAdapter(readonlyConfig, "/tmp/test.xlsx")
	if readonlyAdapter.IsWritable() {
		t.Errorf("Expected IsWritable() to return false by default")
	}

	// 设置为可写
	writableConfig := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
		Options: map[string]interface{}{
			"writable": true,
		},
	}
	writableAdapter := NewExcelAdapter(writableConfig, "/tmp/test.xlsx")
	if !writableAdapter.IsWritable() {
		t.Errorf("Expected IsWritable() to return true when writable=true")
	}
}

// TestExcelSource_GetTables 测试获取所有表（sheets）
func TestExcelSource_GetTables(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	tables, err := es.GetTables(ctx)
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}

	// 应该有1个sheet
	if len(tables) != 1 {
		t.Errorf("Expected 1 table (sheet), got %d", len(tables))
	}

	// 检查是否包含Sheet1
	if len(tables) > 0 && tables[0] != "Sheet1" {
		t.Errorf("Expected sheet 'Sheet1', got %v", tables[0])
	}
}

// TestExcelSource_GetTableInfo 测试获取表信息
func TestExcelSource_GetTableInfo(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 测试获取Sheet1表信息
	tableInfo, err := es.GetTableInfo(ctx, "Sheet1")
	if err != nil {
		t.Errorf("GetTableInfo() error = %v", err)
	}

	if tableInfo == nil {
		t.Errorf("GetTableInfo() returned nil")
		return
	}

	if tableInfo.Name != "Sheet1" {
		t.Errorf("Expected table name 'Sheet1', got %v", tableInfo.Name)
	}

	// 应该有3列：id, name, email
	if len(tableInfo.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(tableInfo.Columns))
	}

	// 验证列名
	columnNames := make(map[string]bool)
	for _, col := range tableInfo.Columns {
		columnNames[col.Name] = true
	}

	expectedColumns := []string{"id", "name", "email"}
	for _, expectedCol := range expectedColumns {
		if !columnNames[expectedCol] {
			t.Errorf("Expected column %s not found", expectedCol)
		}
	}

	// 测试获取不存在的表
	_, err = es.GetTableInfo(ctx, "nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting info for nonexistent table")
	}
}

// TestExcelSource_Query 测试查询数据
func TestExcelSource_Query(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 测试查询Sheet1表
	result, err := es.Query(ctx, "Sheet1", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if result == nil {
		t.Errorf("Query() returned nil")
		return
	}

	// 应该有2行数据（不包括header）
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// 验证第一行数据
	if len(result.Rows) > 0 {
		firstRow := result.Rows[0]
		if firstRow["name"] != "Alice" {
			t.Errorf("Expected name 'Alice', got %v", firstRow["name"])
		}
	}

	// 测试查询不存在的表
	_, err = es.Query(ctx, "nonexistent", &domain.QueryOptions{})
	if err == nil {
		t.Errorf("Expected error when querying nonexistent table")
	}
}

// TestExcelSource_Insert 测试插入数据
func TestExcelSource_Insert(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试插入数据（Excel默认只读）
	rows := []domain.Row{{"name": "Charlie"}}
	_, err := es.Insert(ctx, "Sheet1", rows, nil)
	if err == nil {
		t.Errorf("Expected error when inserting into read-only Excel datasource")
	}
}

// TestExcelSource_Update 测试更新数据
func TestExcelSource_Update(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试更新数据（Excel默认只读）
	_, err := es.Update(ctx, "Sheet1",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "Alice"}},
		domain.Row{"name": "Updated Alice"},
		nil)
	if err == nil {
		t.Errorf("Expected error when updating read-only Excel datasource")
	}
}

// TestExcelSource_Delete 测试删除数据
func TestExcelSource_Delete(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试删除数据（Excel默认只读）
	_, err := es.Delete(ctx, "Sheet1",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "Alice"}},
		nil)
	if err == nil {
		t.Errorf("Expected error when deleting from read-only Excel datasource")
	}
}

// TestExcelSource_CreateTable 测试创建表
func TestExcelSource_CreateTable(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试创建表（Excel默认只读，应该失败）
	tableInfo := &domain.TableInfo{
		Name: "new_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true},
			{Name: "value", Type: "string"},
		},
	}

	err := es.CreateTable(ctx, tableInfo)
	if err == nil {
		t.Errorf("Expected error when creating table in read-only Excel datasource")
	}
}

// TestExcelSource_DropTable 测试删除表
func TestExcelSource_DropTable(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试删除表（Excel默认只读，应该失败）
	err := es.DropTable(ctx, "Sheet1")
	if err == nil {
		t.Errorf("Expected error when dropping table in read-only Excel datasource")
	}
}

// TestExcelSource_TruncateTable 测试清空表
func TestExcelSource_TruncateTable(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	// 使用writable配置
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
		Options: map[string]interface{}{
			"writable": true,
		},
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 验证初始有2行数据（不包括header）
	result, err := es.Query(ctx, "Sheet1", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows before truncate, got %d", len(result.Rows))
	}

	// 清空表（保留header）
	if err := es.TruncateTable(ctx, "Sheet1"); err != nil {
		t.Errorf("TruncateTable() error = %v", err)
	}

	// 验证表已清空（应该只有header，所以数据行为0）
	result, err = es.Query(ctx, "Sheet1", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after truncate, got %d", len(result.Rows))
	}
}

// TestExcelSource_Execute 测试执行SQL
func TestExcelSource_Execute(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}

	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	// Excel不支持SQL执行
	_, err := es.Execute(ctx, "SELECT * FROM Sheet1")
	if err == nil {
		t.Errorf("Expected error when executing SQL")
	}
}

// TestExcelAdapter_SupportsWrite 测试写支持
func TestExcelAdapter_SupportsWrite(t *testing.T) {
	// 测试可写
	writableConfig := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
		Options: map[string]interface{}{
			"writable": true,
		},
	}
	writableAdapter := NewExcelAdapter(writableConfig, "/tmp/test.xlsx")
	if !writableAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return true for writable adapter")
	}

	// 测试只读
	readonlyConfig := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}
	readonlyAdapter := NewExcelAdapter(readonlyConfig, "/tmp/test.xlsx")
	if readonlyAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return false for readonly adapter")
	}
}

// TestExcelFactory 测试Excel工厂
func TestExcelFactory(t *testing.T) {
	// 创建工厂
	factory := NewExcelFactory()
	if factory == nil {
		t.Errorf("NewExcelFactory() returned nil")
	}

	// 测试 GetType
	dsType := factory.GetType()
	if dsType != domain.DataSourceTypeExcel {
		t.Errorf("GetType() = %v, want %v", dsType, domain.DataSourceTypeExcel)
	}

	// 测试 Create
	config := &domain.DataSourceConfig{
		Name:     "test_excel_factory",
		Database: "test_excel_factory.xlsx",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}

	ds, err := factory.Create(config)
	if err != nil {
		t.Errorf("Create() error = %v", err)
	}
	if ds == nil {
		t.Errorf("Create() returned nil")
	}

	// 验证数据源名称
	if ds.GetConfig().Name != "test_excel_factory" {
		t.Errorf("Created datasource name = %v, want %v", ds.GetConfig().Name, "test_excel_factory")
	}
}

// TestExcelAdapter_Close 测试关闭
func TestExcelAdapter_Close(t *testing.T) {
	filePath := createTestExcelFile(t)
	defer os.Remove(filePath)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeExcel,
		Name: "test-excel",
	}
	es := NewExcelAdapter(config, filePath)
	ctx := context.Background()

	if err := es.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 验证已连接
	if !es.IsConnected() {
		t.Errorf("Expected to be connected")
	}

	// 关闭
	if err := es.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// 验证已断开
	if es.IsConnected() {
		t.Errorf("Expected to be disconnected after Close()")
	}
}
