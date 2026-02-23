package dataaccess

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// =============================================================================
// P1-1: selectColumns optimization compares count instead of column names
// service.go:212 checks len(selectColumns) >= len(tableInfo.Columns), which
// incorrectly skips column filtering when selecting different columns that
// happen to match the table's column count.
// =============================================================================

func TestSelectColumns_DifferentColumnsMatchingCount(t *testing.T) {
	// Table has columns [id, name, email] (3 columns)
	// User selects [id, name, phone] (3 columns, but phone doesn't exist in result)
	// Bug: 3 >= 3 triggers the optimization, returning email which wasn't requested

	ds := &SelectColumnsTestDS{}
	service := NewDataService(ds).(*DataService)

	ctx := context.Background()
	options := &QueryOptions{
		SelectColumns: []string{"id", "name", "phone"}, // phone doesn't exist in table
	}

	result, err := service.Query(ctx, "users", options)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Result rows should NOT contain the "email" column (not requested)
	for i, row := range result.Rows {
		if _, hasEmail := row["email"]; hasEmail {
			t.Errorf("Row[%d] contains 'email' column which was not requested in SelectColumns", i)
		}
	}
}

// SelectColumnsTestDS returns 3-column data for testing selectColumns optimization
type SelectColumnsTestDS struct{}

func (s *SelectColumnsTestDS) Connect(ctx context.Context) error { return nil }
func (s *SelectColumnsTestDS) Close(ctx context.Context) error   { return nil }
func (s *SelectColumnsTestDS) IsConnected() bool                 { return true }
func (s *SelectColumnsTestDS) IsWritable() bool                  { return true }
func (s *SelectColumnsTestDS) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory}
}
func (s *SelectColumnsTestDS) GetTables(ctx context.Context) ([]string, error) {
	return []string{"users"}, nil
}
func (s *SelectColumnsTestDS) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "email", Type: "string"},
		},
		Rows: []domain.Row{
			{"id": 1, "name": "Alice", "email": "alice@example.com"},
			{"id": 2, "name": "Bob", "email": "bob@example.com"},
		},
	}, nil
}
func (s *SelectColumnsTestDS) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}
func (s *SelectColumnsTestDS) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}
func (s *SelectColumnsTestDS) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}
func (s *SelectColumnsTestDS) CreateTable(ctx context.Context, info *domain.TableInfo) error {
	return nil
}
func (s *SelectColumnsTestDS) DropTable(ctx context.Context, tableName string) error     { return nil }
func (s *SelectColumnsTestDS) TruncateTable(ctx context.Context, tableName string) error { return nil }
func (s *SelectColumnsTestDS) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
			{Name: "email", Type: "string"},
		},
	}, nil
}
func (s *SelectColumnsTestDS) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, nil
}

// =============================================================================
// P1-2: Query panics on nil QueryOptions
// service.go:156-158 dereferences options without nil check.
// The method signature accepts *QueryOptions, so nil is valid.
// =============================================================================

func TestQuery_NilOptionsShouldNotPanic(t *testing.T) {
	ds := &MockDataSource{}
	service := NewDataService(ds)
	ctx := context.Background()

	// This should not panic - nil options should be treated as empty/default
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Query with nil options panicked: %v", r)
		}
	}()

	result, err := service.Query(ctx, "test_table", nil)
	if err != nil {
		t.Logf("Query returned error (acceptable): %v", err)
		return
	}

	if result == nil {
		t.Error("Query returned nil result with nil options")
	}
}
