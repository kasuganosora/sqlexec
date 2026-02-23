package api

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Mock datasource for testing
type mockDataSource struct {
	closed       bool
	tables       map[string]*domain.TableInfo
	transactions int
}

func newMockDataSource() *mockDataSource {
	return &mockDataSource{
		tables: make(map[string]*domain.TableInfo),
	}
}

// NewMockDataSourceWithTableInfo creates a mock datasource with specific table info
func NewMockDataSourceWithTableInfo(name string, columns []domain.ColumnInfo) domain.DataSource {
	mds := &mockDataSource{
		tables: make(map[string]*domain.TableInfo),
	}

	// Create a test table with given columns
	tableInfo := &domain.TableInfo{
		Name:    "test_table",
		Schema:  name,
		Columns: columns,
	}
	mds.tables["test_table"] = tableInfo

	return mds
}

func (m *mockDataSource) Connect(ctx context.Context) error {
	return nil
}

func (m *mockDataSource) Close(ctx context.Context) error {
	m.closed = true
	return nil
}

func (m *mockDataSource) IsConnected() bool {
	return !m.closed
}

func (m *mockDataSource) IsWritable() bool {
	return true
}

func (m *mockDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
		Name: "mock",
	}
}

func (m *mockDataSource) CreateTable(ctx context.Context, info *domain.TableInfo) error {
	m.tables[info.Name] = info
	return nil
}

func (m *mockDataSource) DropTable(ctx context.Context, name string) error {
	delete(m.tables, name)
	return nil
}

func (m *mockDataSource) GetTableInfo(ctx context.Context, name string) (*domain.TableInfo, error) {
	if info, ok := m.tables[name]; ok {
		return info, nil
	}
	return nil, NewError(ErrCodeTableNotFound, "table not found", nil)
}

func (m *mockDataSource) GetTables(ctx context.Context) ([]string, error) {
	tables := make([]string, 0, len(m.tables))
	for name := range m.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

func (m *mockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	if _, ok := m.tables[tableName]; !ok {
		return nil, NewError(ErrCodeTableNotFound, "table not found", nil)
	}
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

func (m *mockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if _, ok := m.tables[tableName]; !ok {
		return 0, NewError(ErrCodeTableNotFound, "table not found", nil)
	}
	return int64(len(rows)), nil
}

func (m *mockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if _, ok := m.tables[tableName]; !ok {
		return 0, NewError(ErrCodeTableNotFound, "table not found", nil)
	}
	return 1, nil
}

func (m *mockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if _, ok := m.tables[tableName]; !ok {
		return 0, NewError(ErrCodeTableNotFound, "table not found", nil)
	}
	return 1, nil
}

func (m *mockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	if _, ok := m.tables[tableName]; !ok {
		return NewError(ErrCodeTableNotFound, "table not found", nil)
	}
	return nil
}

func (m *mockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, NewError(ErrCodeNotSupported, "EXECUTE not supported in mock datasource", nil)
}

func (m *mockDataSource) BeginTransaction(ctx context.Context, options *domain.TransactionOptions) (domain.Transaction, error) {
	m.transactions++
	return &mockTransaction{ds: m, id: m.transactions}, nil
}

// Mock transaction for testing
type mockTransaction struct {
	ds          *mockDataSource
	id          int
	commitErr   error
	rollbackErr error
}

func newMockTransaction() *mockTransaction {
	return &mockTransaction{}
}

func (m *mockTransaction) Commit(ctx context.Context) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	return nil
}

func (m *mockTransaction) Rollback(ctx context.Context) error {
	if m.rollbackErr != nil {
		return m.rollbackErr
	}
	return nil
}

func (m *mockTransaction) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

func (m *mockTransaction) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

func (m *mockTransaction) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return int64(len(rows)), nil
}

func (m *mockTransaction) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 1, nil
}

func (m *mockTransaction) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 1, nil
}
