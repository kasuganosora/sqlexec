package json

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
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
		Database: "test_json_factory.json",
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

// TestJSONAdapter_DeterministicColumnOrder 测试列顺序确定性
func TestJSONAdapter_DeterministicColumnOrder(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_order.json")

	// 创建包含多个字段的JSON（字段名故意无序）
	data := `[{"zebra": 1, "apple": 2, "mango": 3, "banana": 4}]`
	if err := os.WriteFile(testFile, []byte(data), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test_order",
	}

	// 多次加载验证列顺序始终一致
	for i := 0; i < 10; i++ {
		adapter := NewJSONAdapter(config, testFile)
		ctx := t.Context()

		if err := adapter.Connect(ctx); err != nil {
			t.Fatalf("Connect() error = %v", err)
		}

		tableInfo, err := adapter.GetTableInfo(ctx, "json_data")
		if err != nil {
			t.Fatalf("GetTableInfo() error = %v", err)
		}

		// 列必须按字母顺序排序
		expected := []string{"apple", "banana", "mango", "zebra"}
		if len(tableInfo.Columns) != len(expected) {
			t.Fatalf("Expected %d columns, got %d", len(expected), len(tableInfo.Columns))
		}
		for j, col := range tableInfo.Columns {
			if col.Name != expected[j] {
				t.Errorf("Run %d: Column %d = %q, want %q", i, j, col.Name, expected[j])
			}
		}

		adapter.Close(ctx)
	}
}

// TestJSONAdapter_DetectType 测试类型检测
func TestJSONAdapter_DetectType(t *testing.T) {
	adapter := &JSONAdapter{}
	tests := []struct {
		value    interface{}
		expected string
	}{
		{true, "bool"},
		{false, "bool"},
		{float64(42), "int64"},                     // 整数float64 → int64
		{float64(3.14), "float64"},                 // 小数 → float64
		{float64(0), "int64"},                      // 零 → int64
		{float64(-1), "int64"},                     // 负整数 → int64
		{math.Inf(1), "float64"},                   // +Inf → float64
		{math.Inf(-1), "float64"},                  // -Inf → float64
		{math.NaN(), "float64"},                    // NaN → float64
		{float64(1e18), "int64"},                   // 大整数仍在int64范围 → int64
		{float64(1e19), "float64"},                 // 超出int64范围 → float64
		{"hello", "string"},                        // 字符串
		{nil, "string"},                            // nil
		{[]interface{}{1}, "string"},               // 数组 → string
		{map[string]interface{}{"a": 1}, "string"}, // 对象 → string
	}

	for _, tt := range tests {
		got := adapter.detectType(tt.value)
		if got != tt.expected {
			t.Errorf("detectType(%v) = %q, want %q", tt.value, got, tt.expected)
		}
	}
}

// TestJSONAdapter_InferType_Deterministic 测试类型推断的确定性（平局时的优先级）
func TestJSONAdapter_InferType_Deterministic(t *testing.T) {
	adapter := &JSONAdapter{}

	// int64 和 float64 各 1 个 → int64 优先
	result := adapter.inferType([]interface{}{float64(42), float64(3.14)})
	if result != "int64" {
		t.Errorf("Expected int64 (higher priority on tie), got %q", result)
	}

	// 全部是 bool → bool
	result = adapter.inferType([]interface{}{true, false, true})
	if result != "bool" {
		t.Errorf("Expected bool, got %q", result)
	}

	// 空值列表 → string
	result = adapter.inferType([]interface{}{})
	if result != "string" {
		t.Errorf("Expected string for empty values, got %q", result)
	}

	// 全 nil → string（所有计数为0，string是默认值）
	result = adapter.inferType([]interface{}{nil, nil})
	if result != "string" {
		t.Errorf("Expected string for all-nil values, got %q", result)
	}
}

// TestJSONAdapter_Connect_EmptyArray 测试连接空JSON数组
func TestJSONAdapter_Connect_EmptyArray(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "empty.json")

	// 写入空数组
	if err := os.WriteFile(testFile, []byte("[]"), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test_empty_array",
	}
	adapter := NewJSONAdapter(config, testFile)
	ctx := t.Context()

	// 连接应该成功（空表是合法的）
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() should succeed for empty array, got error: %v", err)
	}

	// 应该有表但无数据
	tables, err := adapter.GetTables(ctx)
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "json_data" {
		t.Errorf("Expected [json_data], got %v", tables)
	}

	// 查询应返回0行
	result, err := adapter.Query(ctx, "json_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result.Rows))
	}

	adapter.Close(ctx)
}

// TestJSONAdapter_Connect_NotArray 测试连接非数组JSON
func TestJSONAdapter_Connect_NotArray(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "not_array.json")

	// 写入非数组JSON
	if err := os.WriteFile(testFile, []byte(`{"key": "value"}`), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test_not_array",
	}
	adapter := NewJSONAdapter(config, testFile)
	ctx := t.Context()

	// 连接应该失败（不是数组）
	err := adapter.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error when JSON is not an array")
	}
	if err != nil && !strings.Contains(err.Error(), "no JSON array found") {
		t.Errorf("Expected 'no JSON array found' error, got: %v", err)
	}
}

// TestJSONAdapter_Connect_InvalidJSON 测试连接无效JSON文件
func TestJSONAdapter_Connect_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "invalid.json")

	// 写入无效JSON
	if err := os.WriteFile(testFile, []byte(`{invalid json}`), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test_invalid",
	}
	adapter := NewJSONAdapter(config, testFile)
	ctx := t.Context()

	err := adapter.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error for invalid JSON")
	}
}

// TestJSONAdapter_writeBack_WithArrayRoot 测试写回时保留array_root结构
func TestJSONAdapter_writeBack_WithArrayRoot(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_arrayroot.json")

	initialData := `{"records": [{"id": 1, "name": "Alice"}]}`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSON,
		Name:     "test_arrayroot",
		Writable: true,
		Options: map[string]interface{}{
			"writable":   true,
			"array_root": "records",
		},
	}
	adapter := NewJSONAdapter(config, testFile)
	ctx := t.Context()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 插入新行
	newRow := domain.Row{"id": float64(2), "name": "Bob"}
	if _, err := adapter.Insert(ctx, "json_data", []domain.Row{newRow}, nil); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// 写回
	if err := adapter.writeBack(); err != nil {
		t.Fatalf("writeBack() error = %v", err)
	}

	// 验证写回后的JSON结构保留了 array_root
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(content, &result); err != nil {
		t.Fatalf("Failed to parse written JSON: %v", err)
	}

	records, ok := result["records"]
	if !ok {
		t.Fatalf("Expected 'records' key in JSON, got keys: %v", result)
	}

	arr, ok := records.([]interface{})
	if !ok {
		t.Fatalf("Expected 'records' to be an array")
	}
	if len(arr) != 2 {
		t.Errorf("Expected 2 records, got %d", len(arr))
	}

	adapter.Close(ctx)
}

// TestJSONAdapter_writeBack_NilValues 测试写回时nil值处理
func TestJSONAdapter_writeBack_NilValues(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_nil.json")

	initialData := `[{"id": 1, "name": "Alice", "email": "a@b.com"}]`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSON,
		Name:     "test_nil",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewJSONAdapter(config, testFile)
	ctx := t.Context()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 插入部分字段缺失的行
	newRow := domain.Row{"id": float64(2), "name": "Bob"} // email 缺失
	if _, err := adapter.Insert(ctx, "json_data", []domain.Row{newRow}, nil); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// 写回不应panic或出错
	if err := adapter.writeBack(); err != nil {
		t.Fatalf("writeBack() error = %v", err)
	}

	// 验证文件可以正常解析
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var arr []map[string]interface{}
	if err := json.Unmarshal(content, &arr); err != nil {
		t.Fatalf("Failed to parse written JSON: %v (content: %s)", err, string(content))
	}

	if len(arr) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(arr))
	}

	adapter.Close(ctx)
}

// TestJSONAdapter_writeBack_AtomicWrite 测试写回的原子性（不会产生残留临时文件）
func TestJSONAdapter_writeBack_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_atomic.json")

	initialData := `[{"id": 1, "name": "Alice"}]`
	if err := os.WriteFile(testFile, []byte(initialData), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSON,
		Name:     "test_atomic",
		Writable: true,
		Options:  map[string]interface{}{"writable": true},
	}
	adapter := NewJSONAdapter(config, testFile)
	ctx := t.Context()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 写回
	if err := adapter.writeBack(); err != nil {
		t.Fatalf("writeBack() error = %v", err)
	}

	// 检查目录中没有残留的临时文件
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".json_writeback_") || strings.HasSuffix(entry.Name(), ".tmp") {
			t.Errorf("Found leftover temp file: %s", entry.Name())
		}
	}

	// 验证原始文件仍然可读且有效
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read file after writeBack: %v", err)
	}
	var arr []interface{}
	if err := json.Unmarshal(content, &arr); err != nil {
		t.Fatalf("File content is not valid JSON after writeBack: %v", err)
	}

	adapter.Close(ctx)
}

// TestJSONAdapter_Connect_EmptyArrayRoot 测试连接带array_root的空数组
func TestJSONAdapter_Connect_EmptyArrayRoot(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "empty_root.json")

	if err := os.WriteFile(testFile, []byte(`{"data": []}`), 0644); err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test_empty_root",
		Options: map[string]interface{}{
			"array_root": "data",
		},
	}
	adapter := NewJSONAdapter(config, testFile)
	ctx := t.Context()

	// 连接应该成功
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() should succeed for empty array_root, got error: %v", err)
	}

	result, err := adapter.Query(ctx, "json_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result.Rows))
	}

	adapter.Close(ctx)
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
