package parquet

import (
	"context"
	"os"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewParquetAdapter 测试创建Parquet数据源
func TestNewParquetAdapter(t *testing.T) {
	tests := []struct {
		name     string
		config   *domain.DataSourceConfig
		filePath string
	}{
		{
			name: "with config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeParquet,
				Name: "test-parquet",
				Options: map[string]interface{}{
					"table_name": "my_table",
				},
			},
			filePath: "/tmp/test.parquet",
		},
		{
			name: "without config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeParquet,
				Name: "test-parquet",
			},
			filePath: "/tmp/test.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := NewParquetAdapter(tt.config, tt.filePath)

			if ps == nil {
				t.Errorf("NewParquetAdapter() returned nil")
			}

			if ps.writable != false {
				t.Errorf("Expected writable to be false for Parquet (read-only)")
			}

			if ps.filePath != tt.filePath {
				t.Errorf("Expected filePath %s, got %s", tt.filePath, ps.filePath)
			}
		})
	}
}

// TestParquetSource_Connect 测试连接Parquet文件
func TestParquetSource_Connect(t *testing.T) {
	// 创建测试Parquet文件
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	// 测试连接
	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	if !ps.IsConnected() {
		t.Errorf("Expected to be connected after Connect()")
	}

	// 测试关闭连接
	err = ps.Close(ctx)
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if ps.IsConnected() {
		t.Errorf("Expected to be disconnected after Close()")
	}
}

// TestParquetSource_GetConfig 测试获取配置
func TestParquetSource_GetConfig(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, "/tmp/test.parquet")

	if got := ps.GetConfig(); got == nil {
		t.Errorf("GetConfig() returned nil")
	} else if got != config {
		t.Errorf("GetConfig() = %v, want %v", got, config)
	}
}

// TestParquetSource_IsWritable 测试是否可写
func TestParquetSource_IsWritable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, "/tmp/test.parquet")

	if ps.IsWritable() {
		t.Errorf("Expected IsWritable() to return false for Parquet (read-only)")
	}
}

// TestParquetSource_GetTables 测试获取表列表
func TestParquetSource_GetTables(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	tables, err := ps.GetTables(ctx)
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}

	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	if tables[0] != "parquet_data" {
		t.Errorf("Expected table name 'parquet_data', got '%s'", tables[0])
	}
}

// TestParquetSource_GetTableInfo 测试获取表信息
func TestParquetSource_GetTableInfo(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	tableInfo, err := ps.GetTableInfo(ctx, "parquet_data")
	if err != nil {
		t.Errorf("GetTableInfo() error = %v", err)
	}

	if tableInfo == nil {
		t.Fatal("GetTableInfo() returned nil")
	}

	if tableInfo.Name != "parquet_data" {
		t.Errorf("Expected table name 'parquet_data', got '%s'", tableInfo.Name)
	}

	// 验证固定列结构
	if len(tableInfo.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(tableInfo.Columns))
	}

	// 验证列名
	expectedColumns := []string{"id", "value"}
	for i, col := range tableInfo.Columns {
		if i < len(expectedColumns) && col.Name != expectedColumns[i] {
			t.Errorf("Expected column %d to be %s, got %s", i, expectedColumns[i], col.Name)
		}
	}

	// 获取不存在的表信息
	_, err = ps.GetTableInfo(ctx, "nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting info for nonexistent table")
	}
}

// TestParquetSource_Query 测试查询数据
func TestParquetSource_Query(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	result, err := ps.Query(ctx, "parquet_data", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if result == nil {
		t.Fatal("Query() returned nil")
	}

	// 验证返回的是固定测试数据
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows (simplified implementation), got %d", len(result.Rows))
	}
}

// TestParquetSource_Insert 测试插入数据
func TestParquetSource_Insert(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	rows := []domain.Row{
		{"id": int64(1), "value": "test"},
	}

	_, err = ps.Insert(ctx, "parquet_data", rows, &domain.InsertOptions{})
	if err == nil {
		t.Errorf("Expected error when inserting into read-only Parquet datasource")
	}
}

// TestParquetSource_Update 测试更新数据
func TestParquetSource_Update(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: int64(1)},
	}
	updates := domain.Row{"value": "updated"}

	_, err = ps.Update(ctx, "parquet_data", filters, updates, &domain.UpdateOptions{})
	if err == nil {
		t.Errorf("Expected error when updating read-only Parquet datasource")
	}
}

// TestParquetSource_Delete 测试删除数据
func TestParquetSource_Delete(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	filters := []domain.Filter{
		{Field: "id", Operator: "=", Value: int64(1)},
	}

	_, err = ps.Delete(ctx, "parquet_data", filters, &domain.DeleteOptions{})
	if err == nil {
		t.Errorf("Expected error when deleting from read-only Parquet datasource")
	}
}

// TestParquetSource_CreateTable 测试创建表
func TestParquetSource_CreateTable(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	tableInfo := &domain.TableInfo{
		Name: "new_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false, Primary: true},
		},
	}

	err = ps.CreateTable(ctx, tableInfo)
	if err == nil {
		t.Errorf("Expected error when creating table in read-only Parquet datasource")
	}
}

// TestParquetSource_DropTable 测试删除表
func TestParquetSource_DropTable(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	err = ps.DropTable(ctx, "parquet_data")
	if err == nil {
		t.Errorf("Expected error when dropping table from read-only Parquet datasource")
	}
}

// TestParquetSource_TruncateTable 测试清空表
func TestParquetSource_TruncateTable(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, tmpFile.Name())
	ctx := context.Background()

	err = ps.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	err = ps.TruncateTable(ctx, "parquet_data")
	if err == nil {
		t.Errorf("Expected error when truncating table in read-only Parquet datasource")
	}
}

// TestParquetSource_Execute 测试执行SQL
func TestParquetSource_Execute(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, "/tmp/test.parquet")
	ctx := context.Background()

	_, err := ps.Execute(ctx, "SELECT * FROM test")
	if err == nil {
		t.Errorf("Expected error when executing SQL on Parquet datasource")
	}
}

// TestParquetSource_Connect_Disconnected 测试未连接状态下的操作
func TestParquetSource_Connect_Disconnected(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, "/tmp/test.parquet")
	ctx := context.Background()

	// 测试未连接时查询
	_, err := ps.Query(ctx, "parquet_data", &domain.QueryOptions{})
	if err == nil {
		t.Errorf("Expected error when querying while disconnected")
	}

	// 测试未连接时获取表列表
	_, err = ps.GetTables(ctx)
	if err == nil {
		t.Errorf("Expected error when getting tables while disconnected")
	}
}

// TestParquetSource_Connect_Nonexistent 测试连接不存在的文件
func TestParquetSource_Connect_Nonexistent(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test-parquet",
	}

	ps := NewParquetAdapter(config, "/nonexistent/file.parquet")
	ctx := context.Background()

	err := ps.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error when connecting to nonexistent file")
	}

	if ps.IsConnected() {
		t.Errorf("Expected to be disconnected when file doesn't exist")
	}
}
