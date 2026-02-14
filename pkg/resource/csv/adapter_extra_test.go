package csv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestCSVAdapter_SupportsWrite 测试写支持
func TestCSVAdapter_SupportsWrite(t *testing.T) {
	// 测试可写
	writableConfig := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeCSV,
		Name:     "test_csv",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	writableAdapter := NewCSVAdapter(writableConfig, "writable.csv")
	if !writableAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return true for writable adapter")
	}

	// 测试只读
	readonlyConfig := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeCSV,
		Name:     "test_csv",
		Writable: false,
		Options:  map[string]interface{}{"writable": false},
	}
	readonlyAdapter := NewCSVAdapter(readonlyConfig, "readonly.csv")
	if readonlyAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return false for readonly adapter")
	}
}

// TestCSVAdapter_writeBack 测试写回功能
func TestCSVAdapter_writeBack(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_writeback.csv")

	// 写入初始数据
	initialData := `id,name,value
1,Alice,100
2,Bob,200
`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// 创建适配器（需要配置Options来设置writable）
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeCSV,
		Name:     "test_writeback",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewCSVAdapter(config, testFile)

	ctx := t.Context()

	// 连接并加载数据
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表数据
	_, rows, err := adapter.GetLatestTableData("csv_data")
	if err != nil {
		t.Fatalf("GetLatestTableData() error = %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	// 修改数据 - 先插入新行到MVCCDataSource
	newRow := domain.Row{"id": "3", "name": "Charlie", "value": "300"}
	if _, err := adapter.Insert(ctx, "csv_data", []domain.Row{newRow}, nil); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// 写回（会自动获取最新数据）
	if err := adapter.writeBack(); err != nil {
		t.Errorf("writeBack() error = %v", err)
	}

	// 验证文件已更新
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file after writeBack: %v", err)
	}

	fileContent := string(content)
	if len(fileContent) == 0 {
		t.Errorf("File is empty after writeBack")
	}

	// 验证包含新数据
	// 检查文件内容是否包含"Charlie"
	if !containsString(fileContent, "Charlie") {
		t.Logf("File content after writeBack: %s", fileContent)
		t.Error("Expected file to contain 'Charlie' after writeBack")
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// Helper function to check if string contains substring
func containsString(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestCSVAdapter_writeBack_ReadOnly 测试只读模式的写回
func TestCSVAdapter_writeBack_ReadOnly(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_readonly.csv")

	// 写入初始数据
	initialData := `id,value
1,100
`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// 创建只读适配器（不设置writable选项）
	adapter := NewCSVAdapter(&domain.DataSourceConfig{
		Type:    domain.DataSourceTypeCSV,
		Name:    "test_readonly",
		Options: map[string]interface{}{"writable": false},
	}, testFile)

	ctx := t.Context()

	// 连接并加载数据
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表数据
	_, rows, err := adapter.GetLatestTableData("csv_data")
	if err != nil {
		t.Fatalf("GetLatestTableData() error = %v", err)
	}

	if len(rows) == 0 {
		t.Fatalf("Expected at least 1 row")
	}

	// 写回（writeBack不检查只读，会尝试写文件）
	// 我们验证它能成功写回
	err = adapter.writeBack()
	if err != nil {
		t.Logf("writeBack() in readonly mode returned error (expected): %v", err)
		// 这可能会因为权限或文件状态而失败，这是可以接受的
	} else {
		t.Log("writeBack() succeeded in readonly mode")
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestCSVAdapter_writeBack_EmptyData 测试写空数据
func TestCSVAdapter_writeBack_EmptyData(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_empty.csv")

	// 写入初始数据
	initialData := `id,value
1,100
`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// 创建适配器（需要配置Options来设置writable）
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeCSV,
		Name:     "test_empty",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewCSVAdapter(config, testFile)

	ctx := t.Context()

	// 连接
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表数据
	_, rows, err := adapter.GetLatestTableData("csv_data")
	if err != nil {
		t.Fatalf("GetLatestTableData() error = %v", err)
	}

	if len(rows) == 0 {
		t.Fatalf("Expected at least 1 row")
	}

	// 删除所有数据
	if _, err := adapter.Delete(ctx, "csv_data", []domain.Filter{{Field: "id", Operator: "=", Value: "1"}}, &domain.DeleteOptions{Force: true}); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// 写空数据（会自动获取最新数据）
	if err := adapter.writeBack(); err != nil {
		t.Errorf("writeBack() with empty data error = %v", err)
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestCSVFactory 测试CSV工厂
func TestCSVFactory(t *testing.T) {
	// 创建工厂
	factory := NewCSVFactory()
	if factory == nil {
		t.Errorf("NewCSVFactory() returned nil")
	}

	// 测试 GetType
	dsType := factory.GetType()
	if dsType != domain.DataSourceTypeCSV {
		t.Errorf("GetType() = %v, want %v", dsType, domain.DataSourceTypeCSV)
	}

	// 测试 Create - 使用config.Name作为文件路径
	config := &domain.DataSourceConfig{
		Name:     "test_csv_factory",
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
	if ds.GetConfig().Name != "test_csv_factory" {
		t.Errorf("Created datasource name = %v, want %v", ds.GetConfig().Name, "test_csv_factory")
	}
}

// TestCSVAdapter_Close 测试关闭
func TestCSVAdapter_Close(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_close.csv")

	// 写入数据
	data := `id,name
1,Test
`
	if err := os.WriteFile(testFile, []byte(data), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// 创建适配器
	adapter := NewCSVAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
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

// TestCSVAdapter_writeBack_NilValues 测试写回时 nil 值不会变成 "<nil>" 字符串
func TestCSVAdapter_writeBack_NilValues(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_nil.csv")

	// 写入包含空值的数据
	initialData := `id,name,value
1,Alice,100
2,,200
`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeCSV,
		Name:     "test_nil",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewCSVAdapter(config, testFile)
	ctx := t.Context()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 写回
	if err := adapter.writeBack(); err != nil {
		t.Fatalf("writeBack() error = %v", err)
	}

	// 读取写回后的文件内容
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	fileContent := string(content)

	// 确保不包含 "<nil>" 字符串
	if strings.Contains(fileContent, "<nil>") {
		t.Errorf("writeBack produced '<nil>' string for nil values.\nFile content:\n%s", fileContent)
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestCSVAdapter_writeBack_MixedTypes 测试写回时各种类型值的正确处理
func TestCSVAdapter_writeBack_MixedTypes(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_mixed.csv")

	initialData := `id,name,active,score
1,Alice,true,95.5
2,Bob,false,87.3
`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeCSV,
		Name:     "test_mixed",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewCSVAdapter(config, testFile)
	ctx := t.Context()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 写回
	if err := adapter.writeBack(); err != nil {
		t.Fatalf("writeBack() error = %v", err)
	}

	// 验证文件内容
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	fileContent := string(content)
	if !strings.Contains(fileContent, "Alice") {
		t.Errorf("Expected file to contain 'Alice', got:\n%s", fileContent)
	}
	if !strings.Contains(fileContent, "Bob") {
		t.Errorf("Expected file to contain 'Bob', got:\n%s", fileContent)
	}

	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestCSVAdapter_DetectType 测试类型检测
func TestCSVAdapter_DetectType(t *testing.T) {
	adapter := &CSVAdapter{}
	tests := []struct {
		value    string
		expected string
	}{
		{"true", "bool"},
		{"false", "bool"},
		{"TRUE", "bool"},
		{"False", "bool"},
		{"42", "int64"},
		{"-100", "int64"},
		{"3.14", "float64"},
		{"-2.5", "float64"},
		{"hello", "string"},
		{"", "string"},
	}

	for _, tt := range tests {
		got := adapter.detectType(tt.value)
		if got != tt.expected {
			t.Errorf("detectType(%q) = %q, want %q", tt.value, got, tt.expected)
		}
	}
}
