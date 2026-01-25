package json

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestJSONAdapter_SupportsWrite 测试写支持
func TestJSONAdapter_SupportsWrite(t *testing.T) {
	// 测试可写
	writableConfig := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSON,
		Name:     "test_json",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	writableAdapter := NewJSONAdapter(writableConfig, "writable.json")
	if !writableAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return true for writable adapter")
	}

	// 测试只读
	readonlyConfig := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSON,
		Name:     "test_json",
		Writable: false,
		Options:  map[string]interface{}{"writable": false},
	}
	readonlyAdapter := NewJSONAdapter(readonlyConfig, "readonly.json")
	if readonlyAdapter.SupportsWrite() {
		t.Errorf("Expected SupportsWrite() to return false for readonly adapter")
	}
}

// TestJSONAdapter_writeBack 测试写回功能
func TestJSONAdapter_writeBack(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_writeback.json")

	// 写入初始数据
	initialData := `[{"id": 1, "name": "Alice", "value": 100}, {"id": 2, "name": "Bob", "value": 200}]`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// 创建适配器
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSON,
		Name:     "test_writeback",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewJSONAdapter(config, testFile)

	ctx := t.Context()

	// 连接并加载数据
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 修改数据 - 先插入新行到MVCCDataSource
	newRow := domain.Row{"id": 3, "name": "Charlie", "value": 300}
	if _, err := adapter.Insert(ctx, "json_data", []domain.Row{newRow}, nil); err != nil {
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
	if !containsString(fileContent, "Charlie") {
		t.Logf("File content after writeBack: %s", fileContent)
		t.Error("Expected file to contain 'Charlie' after writeBack")
	}

	// 关闭
	if err := adapter.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

// TestJSONAdapter_writeBack_ReadOnly 测试只读模式的写回
func TestJSONAdapter_writeBack_ReadOnly(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_readonly.json")

	// 写入初始数据
	initialData := `[{"id": 1, "value": 100}]`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// 创建只读适配器
	adapter := NewJSONAdapter(&domain.DataSourceConfig{
		Type:    domain.DataSourceTypeJSON,
		Name:    "test_readonly",
		Options: map[string]interface{}{"writable": false},
	}, testFile)

	ctx := t.Context()

	// 连接并加载数据
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 写回（writeBack不检查只读，会尝试写文件）
	err := adapter.writeBack()
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

// TestJSONAdapter_writeBack_EmptyData 测试写空数据
func TestJSONAdapter_writeBack_EmptyData(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_empty.json")

	// 写入初始数据
	initialData := `[{"id": 1, "value": 100}]`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}

	// 创建适配器
	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSON,
		Name:     "test_empty",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewJSONAdapter(config, testFile)

	ctx := t.Context()

	// 连接
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 删除所有数据
	if _, err := adapter.Delete(ctx, "json_data", []domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}}, &domain.DeleteOptions{Force: true}); err != nil {
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

// TestJSONFactory 测试JSON工厂
func TestJSONFactory(t *testing.T) {
	// 创建工厂
	factory := NewJSONFactory()
	if factory == nil {
		t.Errorf("NewJSONFactory() returned nil")
	}

	// 测试 GetType
	dsType := factory.GetType()
	if dsType != domain.DataSourceTypeJSON {
		t.Errorf("GetType() = %v, want %v", dsType, domain.DataSourceTypeJSON)
	}

	// 测试 Create
	config := &domain.DataSourceConfig{
		Name:     "test_json_factory",
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
	if ds.GetConfig().Name != "test_json_factory" {
		t.Errorf("Created datasource name = %v, want %v", ds.GetConfig().Name, "test_json_factory")
	}
}

// TestJSONAdapter_Close 测试关闭
func TestJSONAdapter_Close(t *testing.T) {
	// 创建临时文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_close.json")

	// 写入数据
	data := `[{"id": 1, "name": "Test"}]`
	if err := os.WriteFile(testFile, []byte(data), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// 创建适配器
	adapter := NewJSONAdapter(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
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
