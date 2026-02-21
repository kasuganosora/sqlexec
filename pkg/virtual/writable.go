package virtual

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// WritableVirtualTable extends VirtualTable with write operations
// Tables implementing this interface support INSERT, UPDATE, and DELETE
type WritableVirtualTable interface {
	VirtualTable

	// Insert inserts rows into the virtual table
	Insert(ctx context.Context, rows []domain.Row) (int64, error)

	// Update updates rows matching the filters
	Update(ctx context.Context, filters []domain.Filter, updates domain.Row) (int64, error)

	// Delete deletes rows matching the filters
	Delete(ctx context.Context, filters []domain.Filter) (int64, error)
}

// WritableVirtualDataSource implements the DataSource interface for writable virtual tables
// Unlike VirtualDataSource, this supports INSERT, UPDATE, and DELETE operations
type WritableVirtualDataSource struct {
	provider VirtualTableProvider
	name     string // data source name (e.g., "config")
}

// NewWritableVirtualDataSource creates a new WritableVirtualDataSource
func NewWritableVirtualDataSource(provider VirtualTableProvider, name string) *WritableVirtualDataSource {
	return &WritableVirtualDataSource{
		provider: provider,
		name:     name,
	}
}

// Connect is a no-op for virtual data source (always connected)
func (w *WritableVirtualDataSource) Connect(ctx context.Context) error {
	return nil
}

// Close is a no-op for virtual data source
func (w *WritableVirtualDataSource) Close(ctx context.Context) error {
	return nil
}

// IsConnected always returns true for virtual data source
func (w *WritableVirtualDataSource) IsConnected() bool {
	return true
}

// IsWritable returns true - this data source supports write operations
func (w *WritableVirtualDataSource) IsWritable() bool {
	return true
}

// GetConfig returns a minimal data source config
func (w *WritableVirtualDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{
		Type: "virtual",
		Name: w.name,
	}
}

// GetTables returns all virtual table names
func (w *WritableVirtualDataSource) GetTables(ctx context.Context) ([]string, error) {
	if w.provider == nil {
		return nil, fmt.Errorf("writable virtual data source has no provider configured")
	}
	return w.provider.ListVirtualTables(), nil
}

// GetTableInfo returns the schema information for a virtual table
func (w *WritableVirtualDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	if w.provider == nil {
		return nil, fmt.Errorf("writable virtual data source has no provider configured")
	}
	vt, err := w.provider.GetVirtualTable(tableName)
	if err != nil {
		return nil, err
	}

	return &domain.TableInfo{
		Name:    tableName,
		Schema:  w.name,
		Columns: vt.GetSchema(),
	}, nil
}

// Query executes a query against a virtual table
func (w *WritableVirtualDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	if w.provider == nil {
		return nil, fmt.Errorf("writable virtual data source has no provider configured")
	}
	vt, err := w.provider.GetVirtualTable(tableName)
	if err != nil {
		return nil, err
	}

	var filters []domain.Filter
	if options != nil {
		filters = options.Filters
	}
	return vt.Query(ctx, filters, options)
}

// Insert inserts rows into a virtual table
func (w *WritableVirtualDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if w.provider == nil {
		return 0, fmt.Errorf("writable virtual data source has no provider configured")
	}
	vt, err := w.provider.GetVirtualTable(tableName)
	if err != nil {
		return 0, err
	}

	wvt, ok := vt.(WritableVirtualTable)
	if !ok {
		return 0, fmt.Errorf("%s.%s is read-only: INSERT operation not supported", w.name, tableName)
	}

	return wvt.Insert(ctx, rows)
}

// Update updates rows in a virtual table
func (w *WritableVirtualDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if w.provider == nil {
		return 0, fmt.Errorf("writable virtual data source has no provider configured")
	}
	vt, err := w.provider.GetVirtualTable(tableName)
	if err != nil {
		return 0, err
	}

	wvt, ok := vt.(WritableVirtualTable)
	if !ok {
		return 0, fmt.Errorf("%s.%s is read-only: UPDATE operation not supported", w.name, tableName)
	}

	return wvt.Update(ctx, filters, updates)
}

// Delete deletes rows from a virtual table
func (w *WritableVirtualDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if w.provider == nil {
		return 0, fmt.Errorf("writable virtual data source has no provider configured")
	}
	vt, err := w.provider.GetVirtualTable(tableName)
	if err != nil {
		return 0, err
	}

	wvt, ok := vt.(WritableVirtualTable)
	if !ok {
		return 0, fmt.Errorf("%s.%s is read-only: DELETE operation not supported", w.name, tableName)
	}

	return wvt.Delete(ctx, filters)
}

// CreateTable is not supported for virtual data source
func (w *WritableVirtualDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return fmt.Errorf("%s is a virtual database: CREATE TABLE operation not supported", w.name)
}

// DropTable is not supported for virtual data source
func (w *WritableVirtualDataSource) DropTable(ctx context.Context, tableName string) error {
	return fmt.Errorf("%s is a virtual database: DROP TABLE operation not supported", w.name)
}

// TruncateTable is not supported for virtual data source
func (w *WritableVirtualDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return fmt.Errorf("%s is a virtual database: TRUNCATE operation not supported", w.name)
}

// Execute is not supported for virtual data source
func (w *WritableVirtualDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, fmt.Errorf("%s is a virtual database: raw SQL EXECUTE operation not supported", w.name)
}
