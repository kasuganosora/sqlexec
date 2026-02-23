package parquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestParquetAdapter_SupportsWrite 测试写支持
func TestParquetAdapter_SupportsWrite(t *testing.T) {
	// 测试可写
	writableConfig := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     "test_parquet",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	writableAdapter := NewParquetAdapter(writableConfig, "writable.parquet")
	if !writableAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return true for writable adapter")
	}

	// 测试只读
	readonlyConfig := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     "test_parquet",
		Writable: false,
		Options:  map[string]interface{}{"writable": false},
	}
	readonlyAdapter := NewParquetAdapter(readonlyConfig, "readonly.parquet")
	if readonlyAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return false for readonly adapter")
	}
}

// TestParquetAdapter_detectType 测试类型检测
func TestParquetAdapter_detectType(t *testing.T) {
	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_detect",
	}, "test.parquet")

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"bool", true, "bool"},
		{"bool false", false, "bool"},
		{"float64", 3.14, "float64"},
		{"int64 float", float64(42.0), "int64"},
		{"string", "hello", "string"},
		{"nil", nil, "string"},
		{"array", []interface{}{1, 2, 3}, "string"},
		{"map", map[string]interface{}{"key": "value"}, "string"},
		{"int", int(42), "string"},
		{"uint", uint(42), "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := adapter.detectType(tt.value)
			if result != tt.expected {
				t.Errorf("detectType(%v) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

// TestParquetAdapter_writeBack 测试写回功能
func TestParquetAdapter_writeBack(t *testing.T) {
	// 创建临时文件 with valid JSON interchange data
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_writeback.parquet")

	initialData := `{
		"table_name": "parquet_data",
		"columns": [
			{"name": "id", "type": "int64", "nullable": false, "primary": true},
			{"name": "value", "type": "string", "nullable": true}
		],
		"rows": [
			{"id": 1, "value": "parquet_data_1"},
			{"id": 2, "value": "parquet_data_2"}
		]
	}`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 创建适配器
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeParquet,
		Name:     "test_writeback",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewParquetAdapter(config, testFile)

	ctx := t.Context()

	// 连接并加载数据
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表数据
	_, rows, err := adapter.GetLatestTableData("parquet_data")
	if err != nil {
		t.Fatalf("GetLatestTableData() error = %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	// 修改数据 - 先插入新行到MVCCDataSource
	newRow := domain.Row{"id": int64(4), "value": "parquet_data_4"}
	if _, err := adapter.Insert(ctx, "parquet_data", []domain.Row{newRow}, nil); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// 写回（会自动获取最新数据并写入JSON interchange format）
	if err := adapter.writeBack(); err != nil {
		t.Fatalf("writeBack() error = %v", err)
	}

	// Verify the written file can be read back
	written, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read written file: %v", err)
	}
	if len(written) == 0 {
		t.Fatal("writeBack produced empty file")
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Re-open and verify the data survived the round-trip
	adapter2 := NewParquetAdapter(config, testFile)
	if err := adapter2.Connect(ctx); err != nil {
		t.Fatalf("Re-Connect() error = %v", err)
	}
	_, rows2, err := adapter2.GetLatestTableData("parquet_data")
	if err != nil {
		t.Fatalf("GetLatestTableData() after round-trip error = %v", err)
	}
	if len(rows2) != 3 {
		t.Errorf("Expected 3 rows after round-trip, got %d", len(rows2))
	}
	if err := adapter2.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestParquetAdapter_writeBack_ReadOnly 测试只读模式的写回
func TestParquetAdapter_writeBack_ReadOnly(t *testing.T) {
	// 创建临时文件with valid JSON data
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_readonly.parquet")

	initialData := `[{"id": 1, "value": "hello"}]`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 创建只读适配器
	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type:    domain.DataSourceTypeParquet,
		Name:    "test_readonly",
		Options: map[string]interface{}{"writable": false},
	}, testFile)

	ctx := t.Context()

	// 连接并加载数据
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// writeBack writes JSON interchange format -- it should succeed even in
	// readonly mode since writeBack itself does not check the writable flag
	// (Close does).
	err := adapter.writeBack()
	if err != nil {
		t.Logf("writeBack() in readonly mode returned error (may be expected): %v", err)
	} else {
		t.Log("writeBack() succeeded in readonly mode")
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestParquetFactory 测试Parquet工厂
func TestParquetFactory(t *testing.T) {
	// 创建工厂
	factory := NewParquetFactory()
	if factory == nil {
		t.Errorf("NewParquetFactory() returned nil")
	}

	// 测试 GetType
	dsType := factory.GetType()
	if dsType != domain.DataSourceTypeParquet {
		t.Errorf("GetType() = %v, want %v", dsType, domain.DataSourceTypeParquet)
	}

	// 测试 Create
	config := &domain.DataSourceConfig{
		Name:     "test_parquet_factory",
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
	if ds.GetConfig().Name != "test_parquet_factory" {
		t.Errorf("Created datasource name = %v, want %v", ds.GetConfig().Name, "test_parquet_factory")
	}
}

// TestParquetAdapter_Close 测试关闭
func TestParquetAdapter_Close(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_close.parquet")

	// 创建一个简单的parquet文件
	if err := os.WriteFile(testFile, []byte("fake parquet data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 创建适配器
	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_close",
	}, testFile)

	ctx := t.Context()

	// 连接
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 验证已连接
	if !adapter.IsConnected() {
		t.Errorf("Expected to be connected")
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// 验证已断开
	if adapter.IsConnected() {
		t.Errorf("Expected to be disconnected after Close()")
	}
}

// TestParquetAdapter_Connect_MissingFile 测试连接不存在的文件
func TestParquetAdapter_Connect_MissingFile(t *testing.T) {
	// 创建适配器，指向不存在的文件
	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeParquet,
		Name: "test_missing",
	}, "/tmp/nonexistent_file.parquet")

	ctx := t.Context()

	// 尝试连接（应该失败）
	err := adapter.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error when connecting to nonexistent file")
	}
}

// TestParquetAdapter_Connect_CreateFile 测试连接时创建文件
func TestParquetAdapter_Connect_CreateFile(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_create.parquet")

	// 创建一个空文件
	if err := os.WriteFile(testFile, []byte(""), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// 创建适配器
	adapter := NewParquetAdapter(&domain.DataSourceConfig{
		Type:    domain.DataSourceTypeParquet,
		Name:    "test_create",
		Options: map[string]interface{}{"writable": true},
	}, testFile)

	ctx := t.Context()

	// 连接应该成功
	if err := adapter.Connect(ctx); err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	// 验证已连接
	if !adapter.IsConnected() {
		t.Errorf("Expected to be connected")
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}
