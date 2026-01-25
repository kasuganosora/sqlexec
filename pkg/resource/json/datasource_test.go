package json

import (
	"context"
	"os"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewJSONAdapter 测试创建JSON数据源
func TestNewJSONAdapter(t *testing.T) {
	tests := []struct {
		name     string
		config   *domain.DataSourceConfig
		filePath string
	}{
		{
			name: "with config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeJSON,
				Name: "test-json",
				Options: map[string]interface{}{
					"array_root": "data",
				},
			},
			filePath: "/tmp/test.json",
		},
		{
			name: "without config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeJSON,
				Name: "test-json",
			},
			filePath: "/tmp/test.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := NewJSONAdapter(tt.config, tt.filePath)

			if js == nil {
				t.Errorf("NewJSONAdapter() returned nil")
			}

			if js.writable != false {
				t.Errorf("Expected writable to be false for JSON")
			}

			if js.filePath != tt.filePath {
				t.Errorf("Expected filePath %s, got %s", tt.filePath, js.filePath)
			}
		})
	}
}

// TestJSONSource_Connect 测试连接JSON文件
func TestJSONSource_Connect(t *testing.T) {
	// 创建测试JSON文件
	testData := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	// 测试连接
	err = js.Connect(ctx)
	if err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	if !js.IsConnected() {
		t.Errorf("Expected to be connected after Connect()")
	}

	// 测试连接不存在的文件
	js2 := NewJSONAdapter(config, "/nonexistent/file.json")
	err = js2.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error when connecting to nonexistent file")
	}
}

// TestJSONSource_Close 测试关闭JSON数据源
func TestJSONSource_Close(t *testing.T) {
	// 创建测试JSON文件
	testData := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 关闭连接
	err = js.Close(ctx)
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if js.IsConnected() {
		t.Errorf("Expected to be disconnected after Close()")
	}
}

// TestJSONSource_GetConfig 测试获取配置
func TestJSONSource_GetConfig(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")

	got := js.GetConfig()
	if got == nil {
		t.Errorf("GetConfig() returned nil")
	}

	if got.Type != config.Type {
		t.Errorf("GetConfig().Type = %v, want %v", got.Type, config.Type)
	}
}

// TestJSONSource_IsWritable 测试可写性检查
func TestJSONSource_IsWritable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")

	if js.IsWritable() != false {
		t.Errorf("IsWritable() should return false for JSON (read-only)")
	}
}

// TestJSONSource_GetTables 测试获取表列表
func TestJSONSource_GetTables(t *testing.T) {
	// 创建测试JSON文件
	testData := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表列表
	tables, err := js.GetTables(ctx)
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}

	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	if tables[0] != "json_data" {
		t.Errorf("Expected table name 'json_data', got %s", tables[0])
	}
}

// TestJSONSource_GetTableInfo 测试获取表信息
func TestJSONSource_GetTableInfo(t *testing.T) {
	// 创建测试JSON文件
	testData := `[{"id": 1, "name": "Alice", "age": 25}, {"id": 2, "name": "Bob", "age": 30}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表信息
	tableInfo, err := js.GetTableInfo(ctx, "json_data")
	if err != nil {
		t.Errorf("GetTableInfo() error = %v", err)
	}

	if tableInfo == nil {
		t.Errorf("GetTableInfo() returned nil")
	}

	if tableInfo.Name != "json_data" {
		t.Errorf("Expected table name 'json_data', got %s", tableInfo.Name)
	}

	if len(tableInfo.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(tableInfo.Columns))
	}

	// 验证列名
	columnNames := make(map[string]bool)
	for _, col := range tableInfo.Columns {
		columnNames[col.Name] = true
	}

	expectedColumns := []string{"id", "name", "age"}
	for _, expectedCol := range expectedColumns {
		if !columnNames[expectedCol] {
			t.Errorf("Expected column '%s' to be present", expectedCol)
		}
	}

	// 获取不存在的表信息
	_, err = js.GetTableInfo(ctx, "nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting info for nonexistent table")
	}
}

// TestJSONSource_Query 测试查询数据
func TestJSONSource_Query(t *testing.T) {
	// 创建测试JSON文件
	testData := `[{"id": 1, "name": "Alice", "age": 25}, {"id": 2, "name": "Bob", "age": 30}, {"id": 3, "name": "Charlie", "age": 35}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询所有数据
	result, err := js.Query(ctx, "json_data", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}

	// 验证数据
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if name, ok := row["name"].(string); !ok || name != "Alice" {
			t.Errorf("Expected name 'Alice', got %v", row["name"])
		}
	}

	// 查询不存在的表
	_, err = js.Query(ctx, "nonexistent", &domain.QueryOptions{})
	if err == nil {
		t.Errorf("Expected error when querying nonexistent table")
	}
}

// TestJSONSource_Query_WithFilters 测试带过滤器的查询
func TestJSONSource_Query_WithFilters(t *testing.T) {
	// 创建测试JSON文件
	testData := `[{"id": 1, "name": "Alice", "age": 25}, {"id": 2, "name": "Bob", "age": 30}, {"id": 3, "name": "Charlie", "age": 35}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询带过滤器
	result, err := js.Query(ctx, "json_data", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "age", Operator: ">=", Value: 30},
		},
	})
	if err != nil {
		t.Errorf("Query() with filters error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with age>=30, got %d", len(result.Rows))
	}
}

// TestJSONSource_Query_WithPagination 测试带分页的查询
func TestJSONSource_Query_WithPagination(t *testing.T) {
	// 创建测试JSON文件
	testData := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}, {"id": 4, "name": "David"}, {"id": 5, "name": "Eve"}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询带分页
	result, err := js.Query(ctx, "json_data", &domain.QueryOptions{
		Limit:  2,
		Offset: 1,
	})
	if err != nil {
		t.Errorf("Query() with pagination error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with pagination, got %d", len(result.Rows))
	}

	// 验证返回的是第2和第3行
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if name, ok := row["name"].(string); !ok || name != "Bob" {
			t.Errorf("Expected first row name 'Bob', got %v", row["name"])
		}
	}
}

// TestJSONSource_Query_WithArrayRoot 测试使用array_root配置
func TestJSONSource_Query_WithArrayRoot(t *testing.T) {
	// 创建带有嵌套数组的JSON文件
	testData := `{
		"metadata": {"total": 3},
		"data": [
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
			{"id": 3, "name": "Charlie"}
		]
	}`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
		Options: map[string]interface{}{
			"array_root": "data",
		},
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询数据
	result, err := js.Query(ctx, "json_data", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows from array_root, got %d", len(result.Rows))
	}
}

// TestJSONSource_Insert 测试插入数据（JSON只读）
func TestJSONSource_Insert(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 尝试插入数据（JSON只读）
	rows := []domain.Row{{"id": 1, "name": "Test"}}
	count, err := js.Insert(ctx, "json_data", rows, nil)
	if err == nil {
		t.Errorf("Expected error when inserting to read-only JSON")
	}

	if count != 0 {
		t.Errorf("Expected 0 rows inserted, got %d", count)
	}
}

// TestJSONSource_Update 测试更新数据（JSON只读）
func TestJSONSource_Update(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 尝试更新数据（JSON只读）
	filters := []domain.Filter{{Field: "id", Operator: "=", Value: 1}}
	updates := domain.Row{"name": "Updated"}
	count, err := js.Update(ctx, "json_data", filters, updates, nil)
	if err == nil {
		t.Errorf("Expected error when updating read-only JSON")
	}

	if count != 0 {
		t.Errorf("Expected 0 rows updated, got %d", count)
	}
}

// TestJSONSource_Delete 测试删除数据（JSON只读）
func TestJSONSource_Delete(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 尝试删除数据（JSON只读）
	filters := []domain.Filter{{Field: "id", Operator: "=", Value: 1}}
	count, err := js.Delete(ctx, "json_data", filters, nil)
	if err == nil {
		t.Errorf("Expected error when deleting from read-only JSON")
	}

	if count != 0 {
		t.Errorf("Expected 0 rows deleted, got %d", count)
	}
}

// TestJSONSource_CreateTable 测试创建表（JSON不支持）
func TestJSONSource_CreateTable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 尝试创建表（JSON不支持）
	tableInfo := &domain.TableInfo{
		Name: "new_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
		},
	}
	err := js.CreateTable(ctx, tableInfo)
	if err == nil {
		t.Errorf("Expected error when creating table in JSON")
	}
}

// TestJSONSource_DropTable 测试删除表（JSON不支持）
func TestJSONSource_DropTable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 尝试删除表（JSON不支持）
	err := js.DropTable(ctx, "json_data")
	if err == nil {
		t.Errorf("Expected error when dropping table in JSON")
	}
}

// TestJSONSource_TruncateTable 测试清空表（JSON不支持）
func TestJSONSource_TruncateTable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 尝试清空表（JSON不支持）
	err := js.TruncateTable(ctx, "json_data")
	if err == nil {
		t.Errorf("Expected error when truncating table in JSON")
	}
}

// TestJSONSource_Execute 测试执行SQL（JSON不支持）
func TestJSONSource_Execute(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 尝试执行SQL（JSON不支持）
	_, err := js.Execute(ctx, "SELECT * FROM json_data")
	if err == nil {
		t.Errorf("Expected error when executing SQL in JSON")
	}
}

// TestJSONSource_Connect_Disconnected 测试未连接时的操作
func TestJSONSource_Connect_Disconnected(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, "/tmp/test.json")
	ctx := context.Background()

	// 测试未连接时查询
	_, err := js.Query(ctx, "json_data", &domain.QueryOptions{})
	if err == nil {
		t.Errorf("Expected error when querying while disconnected")
	}

	// 测试未连接时获取表列表
	_, err = js.GetTables(ctx)
	if err == nil {
		t.Errorf("Expected error when getting tables while disconnected")
	}
}

// TestJSONSource_Query_WithDifferentTypes 测试不同类型的JSON数据
func TestJSONSource_Query_WithDifferentTypes(t *testing.T) {
	// 创建包含不同类型的JSON文件
	testData := `[{"id": 1, "name": "Alice", "active": true, "score": 95.5}, {"id": 2, "name": "Bob", "active": false, "score": 88.0}]`

	tmpFile, err := os.CreateTemp("", "test*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "test-json",
	}

	js := NewJSONAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := js.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询数据
	result, err := js.Query(ctx, "json_data", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// 验证不同类型的值
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if active, ok := row["active"].(bool); !ok || active != true {
			t.Errorf("Expected active to be true, got %v", row["active"])
		}
		if score, ok := row["score"].(float64); !ok || score != 95.5 {
			t.Errorf("Expected score 95.5, got %v", row["score"])
		}
	}
}
