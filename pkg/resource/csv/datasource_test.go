package csv

import (
	"context"
	"os"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewCSVAdapter 测试创建CSV数据源
func TestNewCSVAdapter(t *testing.T) {
	tests := []struct {
		name     string
		config   *domain.DataSourceConfig
		filePath string
	}{
		{
			name: "with config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeCSV,
				Name: "test-csv",
				Options: map[string]interface{}{
					"delimiter": ";",
					"header":    true,
				},
			},
			filePath: "/tmp/test.csv",
		},
		{
			name: "without config",
			config: &domain.DataSourceConfig{
				Type: domain.DataSourceTypeCSV,
				Name: "test-csv",
			},
			filePath: "/tmp/test.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewCSVAdapter(tt.config, tt.filePath)

			if cs == nil {
				t.Errorf("NewCSVAdapter() returned nil")
			}

			if cs.writable != false {
				t.Errorf("Expected writable to be false for CSV")
			}

			if cs.filePath != tt.filePath {
				t.Errorf("Expected filePath %s, got %s", tt.filePath, cs.filePath)
			}
		})
	}
}

// TestCSVSource_Connect 测试连接CSV文件
func TestCSVSource_Connect(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25
2,Bob,30`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	// 测试连接
	err = cs.Connect(ctx)
	if err != nil {
		t.Errorf("Connect() error = %v", err)
	}

	if !cs.IsConnected() {
		t.Errorf("Expected to be connected after Connect()")
	}

	// 测试连接不存在的文件
	cs2 := NewCSVAdapter(config, "/nonexistent/file.csv")
	err = cs2.Connect(ctx)
	if err == nil {
		t.Errorf("Expected error when connecting to nonexistent file")
	}
}

// TestCSVSource_Close 测试关闭CSV数据源
func TestCSVSource_Close(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25
2,Bob,30`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 关闭连接
	err = cs.Close(ctx)
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if cs.IsConnected() {
		t.Errorf("Expected to be disconnected after Close()")
	}
}

// TestCSVSource_GetConfig 测试获取配置
func TestCSVSource_GetConfig(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, "/tmp/test.csv")

	got := cs.GetConfig()
	if got == nil {
		t.Errorf("GetConfig() returned nil")
	}

	if got.Type != config.Type {
		t.Errorf("GetConfig().Type = %v, want %v", got.Type, config.Type)
	}
}

// TestCSVSource_IsWritable 测试可写性检查
func TestCSVSource_IsWritable(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, "/tmp/test.csv")

	if cs.IsWritable() != false {
		t.Errorf("IsWritable() should return false for CSV (read-only)")
	}
}

// TestCSVSource_GetTables 测试获取表列表
func TestCSVSource_GetTables(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25
2,Bob,30`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表列表
	tables, err := cs.GetTables(ctx)
	if err != nil {
		t.Errorf("GetTables() error = %v", err)
	}

	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	if tables[0] != "csv_data" {
		t.Errorf("Expected table name 'csv_data', got %s", tables[0])
	}
}

// TestCSVSource_GetTableInfo 测试获取表信息
func TestCSVSource_GetTableInfo(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25
2,Bob,30`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 获取表信息
	tableInfo, err := cs.GetTableInfo(ctx, "csv_data")
	if err != nil {
		t.Errorf("GetTableInfo() error = %v", err)
	}

	if tableInfo == nil {
		t.Errorf("GetTableInfo() returned nil")
	}

	if tableInfo.Name != "csv_data" {
		t.Errorf("Expected table name 'csv_data', got %s", tableInfo.Name)
	}

	if len(tableInfo.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(tableInfo.Columns))
	}

	expectedColumns := []string{"id", "name", "age"}
	for i, col := range tableInfo.Columns {
		if col.Name != expectedColumns[i] {
			t.Errorf("Column %d: Expected name %s, got %s", i, expectedColumns[i], col.Name)
		}
	}

	// 获取不存在的表信息
	_, err = cs.GetTableInfo(ctx, "nonexistent")
	if err == nil {
		t.Errorf("Expected error when getting info for nonexistent table")
	}
}

// TestCSVSource_Query 测试查询数据
func TestCSVSource_Query(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25
2,Bob,30
3,Charlie,35`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询所有数据
	result, err := cs.Query(ctx, "csv_data", &domain.QueryOptions{})
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

	// 测试查询不存在的表
	_, err = cs.Query(ctx, "nonexistent", &domain.QueryOptions{})
	if err == nil {
		t.Errorf("Expected error when querying nonexistent table")
	}
}

// TestCSVSource_Insert 测试插入数据（CSV只读）
func TestCSVSource_Insert(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试插入数据（CSV只读）
	rows := []domain.Row{{"id": 2, "name": "Bob"}}
	count, err := cs.Insert(ctx, "csv_data", rows, nil)
	if err == nil {
		t.Errorf("Expected error when inserting to read-only CSV")
	}

	if count != 0 {
		t.Errorf("Expected 0 rows inserted, got %d", count)
	}
}

// TestCSVSource_Update 测试更新数据（CSV只读）
func TestCSVSource_Update(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试更新数据（CSV只读）
	filters := []domain.Filter{{Field: "id", Operator: "=", Value: 1}}
	updates := domain.Row{"name": "Updated"}
	count, err := cs.Update(ctx, "csv_data", filters, updates, nil)
	if err == nil {
		t.Errorf("Expected error when updating read-only CSV")
	}

	if count != 0 {
		t.Errorf("Expected 0 rows updated, got %d", count)
	}
}

// TestCSVSource_Delete 测试删除数据（CSV只读）
func TestCSVSource_Delete(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试删除数据（CSV只读）
	filters := []domain.Filter{{Field: "id", Operator: "=", Value: 1}}
	count, err := cs.Delete(ctx, "csv_data", filters, nil)
	if err == nil {
		t.Errorf("Expected error when deleting from read-only CSV")
	}

	if count != 0 {
		t.Errorf("Expected 0 rows deleted, got %d", count)
	}
}

// TestCSVSource_CreateTable 测试创建表（CSV不支持）
func TestCSVSource_CreateTable(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试创建表（CSV不支持）
	tableInfo := &domain.TableInfo{
		Name: "new_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64"},
		},
	}
	err = cs.CreateTable(ctx, tableInfo)
	if err == nil {
		t.Errorf("Expected error when creating table in CSV")
	}
}

// TestCSVSource_DropTable 测试删除表（CSV不支持）
func TestCSVSource_DropTable(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试删除表（CSV不支持）
	err = cs.DropTable(ctx, "csv_data")
	if err == nil {
		t.Errorf("Expected error when dropping table in CSV")
	}
}

// TestCSVSource_TruncateTable 测试清空表（CSV不支持）
func TestCSVSource_TruncateTable(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 尝试清空表（CSV不支持）
	err = cs.TruncateTable(ctx, "csv_data")
	if err == nil {
		t.Errorf("Expected error when truncating table in CSV")
	}
}

// TestCSVSource_Execute 测试执行SQL（CSV不支持）
func TestCSVSource_Execute(t *testing.T) {
	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, "/tmp/test.csv")
	ctx := context.Background()

	// 尝试执行SQL（CSV不支持）
	_, err := cs.Execute(ctx, "SELECT * FROM csv_data")
	if err == nil {
		t.Errorf("Expected error when executing SQL in CSV")
	}
}

// TestCSVSource_Query_WithoutHeader 测试无表头CSV
func TestCSVSource_Query_WithoutHeader(t *testing.T) {
	// 创建无表头的测试CSV文件
	testData := `1,Alice,25
2,Bob,30
3,Charlie,35`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
		Options: map[string]interface{}{
			"header": false,
		},
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询数据
	result, err := cs.Query(ctx, "csv_data", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}

	// 验证列名应该使用默认命名（Column0, Column1, Column2）
	if len(result.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(result.Columns))
	}
}

// TestCSVSource_Query_WithDifferentDelimiter 测试不同分隔符
func TestCSVSource_Query_WithDifferentDelimiter(t *testing.T) {
	// 创建使用分号的CSV文件
	testData := `id;name;age
1;Alice;25
2;Bob;30`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
		Options: map[string]interface{}{
			"delimiter": ";",
		},
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询数据
	result, err := cs.Query(ctx, "csv_data", &domain.QueryOptions{})
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// 验证数据正确解析
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if name, ok := row["name"].(string); !ok || name != "Alice" {
			t.Errorf("Expected name 'Alice', got %v", row["name"])
		}
	}
}

// TestCSVSource_Query_WithFilters 测试带过滤器的查询
func TestCSVSource_Query_WithFilters(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25
2,Bob,30
3,Charlie,35`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询带过滤器
	result, err := cs.Query(ctx, "csv_data", &domain.QueryOptions{
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

// TestCSVSource_Query_WithPagination 测试带分页的查询
func TestCSVSource_Query_WithPagination(t *testing.T) {
	// 创建测试CSV文件
	testData := `id,name,age
1,Alice,25
2,Bob,30
3,Charlie,35
4,David,40
5,Eve,45`

	tmpFile, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(testData)); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	config := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "test-csv",
	}

	cs := NewCSVAdapter(config, tmpFile.Name())
	ctx := context.Background()

	if err := cs.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// 查询带分页
	result, err := cs.Query(ctx, "csv_data", &domain.QueryOptions{
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
