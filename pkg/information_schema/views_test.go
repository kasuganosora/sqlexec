package information_schema

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

func TestViewsTable_GetSchema(t *testing.T) {
	// Create a mock data source manager
	dsManager := application.NewDataSourceManager()

	viewsTable := NewViewsTable(dsManager)
	schema := viewsTable.GetSchema()

	// Check that schema has 10 columns
	if len(schema) != 10 {
		t.Errorf("Expected 10 columns, got %d", len(schema))
	}

	expectedColumns := []string{
		"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION",
		"CHECK_OPTION", "IS_UPDATABLE", "DEFINER", "SECURITY_TYPE",
		"CHARACTER_SET_CLIENT", "COLLATION_CONNECTION",
	}

	for i, col := range schema {
		if col.Name != expectedColumns[i] {
			t.Errorf("Expected column %s at index %d, got %s", expectedColumns[i], i, col.Name)
		}
	}
}

func TestViewsTable_Query_Empty(t *testing.T) {
	dsManager := application.NewDataSourceManager()
	viewsTable := NewViewsTable(dsManager)

	ctx := context.Background()
	result, err := viewsTable.Query(ctx, nil, nil)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should return empty result but with correct schema
	if result.Total != 0 {
		t.Errorf("Expected 0 rows, got %d", result.Total)
	}

	if len(result.Columns) != 10 {
		t.Errorf("Expected 10 columns, got %d", len(result.Columns))
	}
}

func TestViewsTable_Query_SimpleView(t *testing.T) {
	dsManager := application.NewDataSourceManager()

	// Create a test database with a table
	ctx := context.Background()

	// Register a memory data source
	memoryDS := memory.NewMVCCDataSource(nil)
	err := memoryDS.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect data source: %v", err)
	}

	err = dsManager.Register("test_db", memoryDS)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create a test table
	tableInfo := &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true},
			{Name: "name", Type: "text", Nullable: false},
		},
	}

	err = dsManager.CreateTable(ctx, "test_db", tableInfo)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create a view
	viewData := map[string]interface{}{
		domain.ViewMetaKey: domain.ViewInfo{
			Algorithm:   domain.ViewAlgorithmMerge,
			SelectStmt:  "SELECT id, name FROM test_table",
			CheckOption: domain.ViewCheckOptionNone,
			Definer:     "'root'@'localhost'",
			Security:    domain.ViewSecurityDefiner,
			Cols:        []string{"id", "name"},
			Updatable:   true,
			Charset:     "utf8mb4",
			Collate:     "utf8mb4_general_ci",
		},
	}

	viewTableInfo := &domain.TableInfo{
		Name: "test_view",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "text"},
		},
		Atts: viewData,
	}

	err = dsManager.CreateTable(ctx, "test_db", viewTableInfo)
	if err != nil {
		t.Fatalf("Failed to create test view: %v", err)
	}

	// Query views table
	viewsTable := NewViewsTable(dsManager)
	result, err := viewsTable.Query(ctx, nil, nil)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should return one view
	if result.Total != 1 {
		t.Errorf("Expected 1 row, got %d", result.Total)
	}

	// Check the view row
	row := result.Rows[0]
	if row["TABLE_SCHEMA"] != "test_db" {
		t.Errorf("Expected TABLE_SCHEMA 'test_db', got '%s'", row["TABLE_SCHEMA"])
	}
	if row["TABLE_NAME"] != "test_view" {
		t.Errorf("Expected TABLE_NAME 'test_view', got '%s'", row["TABLE_NAME"])
	}
	if row["IS_UPDATABLE"] != "YES" {
		t.Errorf("Expected IS_UPDATABLE 'YES', got '%s'", row["IS_UPDATABLE"])
	}
}

func TestViewsTable_Query_WithCheckOption(t *testing.T) {
	dsManager := application.NewDataSourceManager()
	ctx := context.Background()

	// Set up test database
	memoryDS := memory.NewMVCCDataSource(nil)
	err := memoryDS.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect data source: %v", err)
	}

	err = dsManager.Register("test_db", memoryDS)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create a view with CASCADED check option
	viewData := map[string]interface{}{
		domain.ViewMetaKey: domain.ViewInfo{
			Algorithm:   domain.ViewAlgorithmMerge,
			SelectStmt:  "SELECT id FROM test_table WHERE id > 0",
			CheckOption: domain.ViewCheckOptionCascaded,
			Cols:        []string{"id"},
			Updatable:   true,
		},
	}

	viewTableInfo := &domain.TableInfo{
		Name: "test_view_cascaded",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Atts: viewData,
	}

	err = dsManager.CreateTable(ctx, "test_db", viewTableInfo)
	if err != nil {
		t.Fatalf("Failed to create test view: %v", err)
	}

	// Query views table
	viewsTable := NewViewsTable(dsManager)
	result, err := viewsTable.Query(ctx, nil, nil)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	row := result.Rows[0]
	if row["CHECK_OPTION"] != "CASCADED" {
		t.Errorf("Expected CHECK_OPTION 'CASCADED', got '%s'", row["CHECK_OPTION"])
	}
}

func TestViewsTable_Query_WithLimit(t *testing.T) {
	dsManager := application.NewDataSourceManager()
	ctx := context.Background()

	// Set up test database
	memoryDS := memory.NewMVCCDataSource(nil)
	err := memoryDS.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect data source: %v", err)
	}

	err = dsManager.Register("test_db", memoryDS)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create a view
	viewData := map[string]interface{}{
		domain.ViewMetaKey: domain.ViewInfo{
			Algorithm:  domain.ViewAlgorithmMerge,
			SelectStmt: "SELECT id FROM test_table",
			Cols:       []string{"id"},
			Updatable:  true,
		},
	}

	viewTableInfo := &domain.TableInfo{
		Name: "test_view",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
		},
		Atts: viewData,
	}

	err = dsManager.CreateTable(ctx, "test_db", viewTableInfo)
	if err != nil {
		t.Fatalf("Failed to create test view: %v", err)
	}

	// Query with limit
	viewsTable := NewViewsTable(dsManager)
	result, err := viewsTable.Query(ctx, nil, &domain.QueryOptions{Limit: 1})

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should return only 1 row
	if result.Total != 1 {
		t.Errorf("Expected 1 row, got %d", result.Total)
	}
}

func TestViewsTable_Query_WithFilters(t *testing.T) {
	dsManager := application.NewDataSourceManager()
	ctx := context.Background()

	// Set up test database
	memoryDS := memory.NewMVCCDataSource(nil)
	err := memoryDS.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect data source: %v", err)
	}

	err = dsManager.Register("test_db", memoryDS)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create multiple views
	for i := 0; i < 3; i++ {
		viewData := map[string]interface{}{
			domain.ViewMetaKey: domain.ViewInfo{
				SelectStmt: "SELECT id FROM test_table",
				Cols:       []string{"id"},
				Updatable:  true,
			},
		}

		viewTableInfo := &domain.TableInfo{
			Name: testTableName(i),
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int"},
			},
			Atts: viewData,
		}

		err := dsManager.CreateTable(ctx, "test_db", viewTableInfo)
		if err != nil {
			t.Fatalf("Failed to create test view: %v", err)
		}
	}

	// Query with filter
	viewsTable := NewViewsTable(dsManager)
	filters := []domain.Filter{
		{Field: "TABLE_NAME", Operator: "=", Value: "view_0"},
	}

	result, err := viewsTable.Query(ctx, filters, nil)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should return only 1 row
	if result.Total != 1 {
		t.Errorf("Expected 1 row after filter, got %d", result.Total)
	}

	if result.Rows[0]["TABLE_NAME"] != "view_0" {
		t.Errorf("Expected TABLE_NAME 'view_0', got '%s'", result.Rows[0]["TABLE_NAME"])
	}
}

func testTableName(i int) string {
	return "view_" + string(rune('0'+i))
}

func TestViewsTable_GetName(t *testing.T) {
	dsManager := application.NewDataSourceManager()
	viewsTable := NewViewsTable(dsManager)

	name := viewsTable.GetName()
	if name != "VIEWS" {
		t.Errorf("Expected name 'VIEWS', got '%s'", name)
	}
}
