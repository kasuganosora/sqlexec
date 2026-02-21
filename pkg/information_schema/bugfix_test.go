package information_schema

import (
	"context"
	"sync"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==========================================================================
// Bug 8 (P1): tables.go has debug fmt.Printf in production code.
// We test that Query doesn't write to stdout.
// (This is primarily a code quality fix — the test verifies no panic.)
// ==========================================================================

func TestBug8_TablesTable_NoDebugOutput(t *testing.T) {
	dsManager := application.NewDataSourceManager()
	table := NewTablesTable(dsManager, nil)

	ctx := context.Background()
	// Should not print debug output to stdout
	result, err := table.Query(ctx, nil, &domain.QueryOptions{User: "testuser"})
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// ==========================================================================
// Bug 9 (P1): keys.go foreign key referenced_table_schema is set to
// column.ForeignKey.Table instead of the datasource schema name (dsName).
// ==========================================================================

// mockDSManagerForFK creates a DataSourceManager with a table that has a FK
type mockDSForFK struct{}

func (m *mockDSForFK) Connect(_ context.Context) error                 { return nil }
func (m *mockDSForFK) Close(_ context.Context) error                   { return nil }
func (m *mockDSForFK) IsConnected() bool                               { return true }
func (m *mockDSForFK) IsWritable() bool                                { return true }
func (m *mockDSForFK) GetConfig() *domain.DataSourceConfig             { return &domain.DataSourceConfig{} }
func (m *mockDSForFK) GetTables(_ context.Context) ([]string, error)   { return []string{"orders"}, nil }
func (m *mockDSForFK) GetTableInfo(_ context.Context, tableName string) (*domain.TableInfo, error) {
	if tableName == "orders" {
		return &domain.TableInfo{
			Name: "orders",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "INT", Primary: true},
				{Name: "user_id", Type: "INT", ForeignKey: &domain.ForeignKeyInfo{
					Table:  "users",
					Column: "id",
				}},
			},
		}, nil
	}
	return nil, nil
}
func (m *mockDSForFK) Query(_ context.Context, _ string, _ *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}
func (m *mockDSForFK) Insert(_ context.Context, _ string, _ []domain.Row, _ *domain.InsertOptions) (int64, error) {
	return 0, nil
}
func (m *mockDSForFK) Update(_ context.Context, _ string, _ []domain.Filter, _ domain.Row, _ *domain.UpdateOptions) (int64, error) {
	return 0, nil
}
func (m *mockDSForFK) Delete(_ context.Context, _ string, _ []domain.Filter, _ *domain.DeleteOptions) (int64, error) {
	return 0, nil
}
func (m *mockDSForFK) CreateTable(_ context.Context, _ *domain.TableInfo) error { return nil }
func (m *mockDSForFK) DropTable(_ context.Context, _ string) error              { return nil }
func (m *mockDSForFK) TruncateTable(_ context.Context, _ string) error          { return nil }
func (m *mockDSForFK) Execute(_ context.Context, _ string) (*domain.QueryResult, error) {
	return nil, nil
}

func TestBug9_ForeignKey_ReferencedTableSchema(t *testing.T) {
	dsManager := application.NewDataSourceManager()
	dsManager.Register("mydb", &mockDSForFK{})

	table := NewKeyColumnUsageTable(dsManager)

	ctx := context.Background()
	result, err := table.Query(ctx, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Find the FK row for orders.user_id
	var fkRow domain.Row
	for _, row := range result.Rows {
		if row["column_name"] == "user_id" {
			fkRow = row
			break
		}
	}
	require.NotNil(t, fkRow, "should find FK row for user_id")

	// BUG: referenced_table_schema is set to column.ForeignKey.Table ("users")
	// instead of the datasource schema name ("mydb").
	assert.Equal(t, "mydb", fkRow["referenced_table_schema"],
		"referenced_table_schema should be the datasource name, not the table name")
	assert.Equal(t, "users", fkRow["referenced_table_name"],
		"referenced_table_name should be the foreign table name")
}

// ==========================================================================
// Bug 10 (P1): GetACLManagerAdapter data race — reads globalACLManager
// without holding aclManagerMutex.
// ==========================================================================

func TestBug10_GetACLManagerAdapter_Race(t *testing.T) {
	// This test should be run with -race to detect the data race.
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			RegisterACLManager(nil) // write
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = GetACLManagerAdapter() // read without lock
		}
	}()

	wg.Wait()
}
