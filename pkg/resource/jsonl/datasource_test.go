package jsonl

import (
	"context"
	"os"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// helper: 创建临时JSONL文件
func createTempJSONL(t *testing.T, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test*.jsonl")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	if _, err := tmpFile.Write([]byte(content)); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()
	return tmpFile.Name()
}

// helper: 创建已连接的adapter
func createConnectedAdapter(t *testing.T, content string, opts map[string]interface{}) (*JSONLAdapter, string) {
	t.Helper()
	path := createTempJSONL(t, content)
	config := &domain.DataSourceConfig{
		Type:    domain.DataSourceTypeJSONL,
		Name:    "test-jsonl",
		Options: opts,
	}
	adapter := NewJSONLAdapter(config, path)
	if err := adapter.Connect(context.Background()); err != nil {
		os.Remove(path)
		t.Fatalf("Connect() error = %v", err)
	}
	return adapter, path
}

func TestNewJSONLAdapter(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
	}
	adapter := NewJSONLAdapter(config, "/tmp/test.jsonl")

	if adapter == nil {
		t.Fatal("NewJSONLAdapter() returned nil")
	}
	if adapter.filePath != "/tmp/test.jsonl" {
		t.Errorf("filePath = %q, want /tmp/test.jsonl", adapter.filePath)
	}
	if adapter.writable {
		t.Error("writable should default to false")
	}
	if adapter.skipErrors {
		t.Error("skipErrors should default to false")
	}
}

func TestNewJSONLAdapter_WithOptions(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
		Options: map[string]interface{}{
			"writable":    true,
			"skip_errors": true,
		},
	}
	adapter := NewJSONLAdapter(config, "/tmp/test.jsonl")

	if !adapter.writable {
		t.Error("writable should be true")
	}
	if !adapter.skipErrors {
		t.Error("skipErrors should be true")
	}
}

func TestConnect_BasicJSONL(t *testing.T) {
	content := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
{"id": 3, "name": "Charlie"}
`
	adapter, path := createConnectedAdapter(t, content, nil)
	defer os.Remove(path)

	if !adapter.IsConnected() {
		t.Error("should be connected after Connect()")
	}
}

func TestConnect_EmptyFile(t *testing.T) {
	adapter, path := createConnectedAdapter(t, "", nil)
	defer os.Remove(path)

	tables, err := adapter.GetTables(context.Background())
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "jsonl_data" {
		t.Errorf("GetTables() = %v, want [jsonl_data]", tables)
	}
}

func TestConnect_EmptyLines(t *testing.T) {
	content := `{"id": 1}

{"id": 2}

`
	adapter, path := createConnectedAdapter(t, content, nil)
	defer os.Remove(path)

	result, err := adapter.Query(context.Background(), "jsonl_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows (empty lines skipped), got %d", len(result.Rows))
	}
}

func TestConnect_NonexistentFile(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
	}
	adapter := NewJSONLAdapter(config, "/nonexistent/file.jsonl")
	err := adapter.Connect(context.Background())
	if err == nil {
		t.Error("Connect() should fail for nonexistent file")
	}
}

func TestConnect_InvalidJSON(t *testing.T) {
	path := createTempJSONL(t, `{"id": 1}
not json
{"id": 3}
`)
	defer os.Remove(path)

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
	}
	adapter := NewJSONLAdapter(config, path)
	err := adapter.Connect(context.Background())
	if err == nil {
		t.Error("Connect() should fail on invalid JSON line without skip_errors")
	}
}

func TestConnect_SkipErrors(t *testing.T) {
	content := `{"id": 1, "name": "Alice"}
not json at all
{"id": 3, "name": "Charlie"}
`
	adapter, path := createConnectedAdapter(t, content, map[string]interface{}{
		"skip_errors": true,
	})
	defer os.Remove(path)

	result, err := adapter.Query(context.Background(), "jsonl_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows (bad line skipped), got %d", len(result.Rows))
	}
}

func TestQuery_AllRows(t *testing.T) {
	content := `{"id": 1, "name": "Alice", "age": 25}
{"id": 2, "name": "Bob", "age": 30}
{"id": 3, "name": "Charlie", "age": 35}
`
	adapter, path := createConnectedAdapter(t, content, nil)
	defer os.Remove(path)
	ctx := context.Background()

	result, err := adapter.Query(ctx, "jsonl_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}

	if name, ok := result.Rows[0]["name"].(string); !ok || name != "Alice" {
		t.Errorf("first row name = %v, want Alice", result.Rows[0]["name"])
	}
}

func TestQuery_WithFilters(t *testing.T) {
	content := `{"id": 1, "name": "Alice", "age": 25}
{"id": 2, "name": "Bob", "age": 30}
{"id": 3, "name": "Charlie", "age": 35}
`
	adapter, path := createConnectedAdapter(t, content, nil)
	defer os.Remove(path)
	ctx := context.Background()

	result, err := adapter.Query(ctx, "jsonl_data", &domain.QueryOptions{
		Filters: []domain.Filter{
			{Field: "age", Operator: ">=", Value: 30},
		},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with age>=30, got %d", len(result.Rows))
	}
}

func TestQuery_WithPagination(t *testing.T) {
	content := `{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
{"id": 3, "name": "Charlie"}
{"id": 4, "name": "David"}
{"id": 5, "name": "Eve"}
`
	adapter, path := createConnectedAdapter(t, content, nil)
	defer os.Remove(path)
	ctx := context.Background()

	result, err := adapter.Query(ctx, "jsonl_data", &domain.QueryOptions{
		Limit:  2,
		Offset: 1,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}
	if name, ok := result.Rows[0]["name"].(string); !ok || name != "Bob" {
		t.Errorf("first row = %v, want Bob", result.Rows[0]["name"])
	}
}

func TestQuery_NonexistentTable(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	_, err := adapter.Query(context.Background(), "no_such_table", &domain.QueryOptions{})
	if err == nil {
		t.Error("Query() should fail for nonexistent table")
	}
}

func TestGetTables(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	tables, err := adapter.GetTables(context.Background())
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "jsonl_data" {
		t.Errorf("GetTables() = %v, want [jsonl_data]", tables)
	}
}

func TestGetTableInfo(t *testing.T) {
	content := `{"id": 1, "name": "Alice", "active": true}
{"id": 2, "name": "Bob", "active": false}
`
	adapter, path := createConnectedAdapter(t, content, nil)
	defer os.Remove(path)
	ctx := context.Background()

	info, err := adapter.GetTableInfo(ctx, "jsonl_data")
	if err != nil {
		t.Fatalf("GetTableInfo() error = %v", err)
	}
	if info.Name != "jsonl_data" {
		t.Errorf("table name = %q, want jsonl_data", info.Name)
	}
	if len(info.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(info.Columns))
	}

	colMap := make(map[string]string)
	for _, col := range info.Columns {
		colMap[col.Name] = col.Type
	}
	if colMap["active"] != "bool" {
		t.Errorf("active column type = %q, want bool", colMap["active"])
	}

	_, err = adapter.GetTableInfo(ctx, "nonexistent")
	if err == nil {
		t.Error("GetTableInfo() should fail for nonexistent table")
	}
}

func TestGetTableInfo_TypeInference(t *testing.T) {
	content := `{"int_col": 42, "float_col": 3.14, "bool_col": true, "str_col": "hello"}
{"int_col": 100, "float_col": 2.71, "bool_col": false, "str_col": "world"}
`
	adapter, path := createConnectedAdapter(t, content, nil)
	defer os.Remove(path)

	info, err := adapter.GetTableInfo(context.Background(), "jsonl_data")
	if err != nil {
		t.Fatalf("GetTableInfo() error = %v", err)
	}

	colMap := make(map[string]string)
	for _, col := range info.Columns {
		colMap[col.Name] = col.Type
	}

	expected := map[string]string{
		"int_col":   "int64",
		"float_col": "float64",
		"bool_col":  "bool",
		"str_col":   "string",
	}
	for name, wantType := range expected {
		if colMap[name] != wantType {
			t.Errorf("column %q type = %q, want %q", name, colMap[name], wantType)
		}
	}
}

func TestInsert_ReadOnly(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	_, err := adapter.Insert(context.Background(), "jsonl_data", []domain.Row{{"id": 2}}, nil)
	if err == nil {
		t.Error("Insert() should fail on read-only adapter")
	}
}

func TestUpdate_ReadOnly(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	_, err := adapter.Update(context.Background(), "jsonl_data",
		[]domain.Filter{{Field: "id", Operator: "=", Value: 1}},
		domain.Row{"id": 2}, nil)
	if err == nil {
		t.Error("Update() should fail on read-only adapter")
	}
}

func TestDelete_ReadOnly(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	_, err := adapter.Delete(context.Background(), "jsonl_data",
		[]domain.Filter{{Field: "id", Operator: "=", Value: 1}}, nil)
	if err == nil {
		t.Error("Delete() should fail on read-only adapter")
	}
}

func TestInsert_Writable(t *testing.T) {
	content := `{"id": 1, "name": "Alice"}
`
	adapter, path := createConnectedAdapter(t, content, map[string]interface{}{
		"writable": true,
	})
	defer os.Remove(path)
	ctx := context.Background()

	count, err := adapter.Insert(ctx, "jsonl_data", []domain.Row{
		{"id": int64(2), "name": "Bob"},
	}, nil)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if count != 1 {
		t.Errorf("Insert() count = %d, want 1", count)
	}

	result, err := adapter.Query(ctx, "jsonl_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows after insert, got %d", len(result.Rows))
	}
}

func TestWriteBack(t *testing.T) {
	content := `{"id": 1, "name": "Alice"}
`
	adapter, path := createConnectedAdapter(t, content, map[string]interface{}{
		"writable": true,
	})
	defer os.Remove(path)
	ctx := context.Background()

	// 插入一行
	adapter.Insert(ctx, "jsonl_data", []domain.Row{
		{"id": int64(2), "name": "Bob"},
	}, nil)

	// 关闭（触发写回）
	if err := adapter.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// 重新读取文件验证
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	fileContent := string(data)
	if fileContent == "" {
		t.Error("written file should not be empty")
	}

	// 用新adapter重新加载验证
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "verify",
	}
	adapter2 := NewJSONLAdapter(config, path)
	if err := adapter2.Connect(ctx); err != nil {
		t.Fatalf("re-Connect() error = %v", err)
	}
	result, err := adapter2.Query(ctx, "jsonl_data", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("re-Query() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows after reload, got %d", len(result.Rows))
	}
}

func TestCreateTable_Unsupported(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	err := adapter.CreateTable(context.Background(), &domain.TableInfo{Name: "new"})
	if err == nil {
		t.Error("CreateTable() should fail")
	}
}

func TestDropTable_Unsupported(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	err := adapter.DropTable(context.Background(), "jsonl_data")
	if err == nil {
		t.Error("DropTable() should fail")
	}
}

func TestTruncateTable_Unsupported(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	err := adapter.TruncateTable(context.Background(), "jsonl_data")
	if err == nil {
		t.Error("TruncateTable() should fail")
	}
}

func TestExecute_Unsupported(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	_, err := adapter.Execute(context.Background(), "SELECT 1")
	if err == nil {
		t.Error("Execute() should fail")
	}
}

func TestClose_ReadOnly(t *testing.T) {
	adapter, path := createConnectedAdapter(t, `{"id": 1}`, nil)
	defer os.Remove(path)

	err := adapter.Close(context.Background())
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
	if adapter.IsConnected() {
		t.Error("should be disconnected after Close()")
	}
}

func TestIsWritable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
	}
	adapter := NewJSONLAdapter(config, "/tmp/test.jsonl")
	if adapter.IsWritable() {
		t.Error("default should be not writable")
	}
	if adapter.SupportsWrite() {
		t.Error("default should not support write")
	}
}

func TestGetConfig(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
	}
	adapter := NewJSONLAdapter(config, "/tmp/test.jsonl")

	got := adapter.GetConfig()
	if got == nil {
		t.Fatal("GetConfig() returned nil")
	}
	if got.Type != domain.DataSourceTypeJSONL {
		t.Errorf("GetConfig().Type = %v, want jsonl", got.Type)
	}
}

func TestFactory_Create(t *testing.T) {
	factory := NewJSONLFactory()
	if factory.GetType() != domain.DataSourceTypeJSONL {
		t.Errorf("GetType() = %v, want jsonl", factory.GetType())
	}

	// nil config
	_, err := factory.Create(nil)
	if err == nil {
		t.Error("Create(nil) should fail")
	}

	// no path
	_, err = factory.Create(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
	})
	if err == nil {
		t.Error("Create() without path should fail")
	}

	// path in Database
	ds, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeJSONL,
		Name:     "test",
		Database: "/tmp/test.jsonl",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if ds == nil {
		t.Error("Create() returned nil")
	}

	// path in Options
	ds, err = factory.Create(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
		Options: map[string]interface{}{
			"path": "/tmp/test2.jsonl",
		},
	})
	if err != nil {
		t.Fatalf("Create() with options path error = %v", err)
	}
	if ds == nil {
		t.Error("Create() with options path returned nil")
	}
}

func TestDisconnected_Operations(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSONL,
		Name: "test",
	}
	adapter := NewJSONLAdapter(config, "/tmp/test.jsonl")
	ctx := context.Background()

	_, err := adapter.Query(ctx, "jsonl_data", &domain.QueryOptions{})
	if err == nil {
		t.Error("Query() should fail when disconnected")
	}

	_, err = adapter.GetTables(ctx)
	if err == nil {
		t.Error("GetTables() should fail when disconnected")
	}
}
