package virtual

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VirtualDataSource implements the DataSource interface for virtual tables
// It acts as a read-only data source that delegates queries to virtual tables
type VirtualDataSource struct {
	provider VirtualTableProvider
}

// NewVirtualDataSource creates a new VirtualDataSource with the given provider
func NewVirtualDataSource(provider VirtualTableProvider) *VirtualDataSource {
	return &VirtualDataSource{
		provider: provider,
	}
}

// Connect is a no-op for virtual data source (always connected)
func (v *VirtualDataSource) Connect(ctx context.Context) error {
	return nil
}

// Close is a no-op for virtual data source
func (v *VirtualDataSource) Close(ctx context.Context) error {
	return nil
}

// IsConnected always returns true for virtual data source
func (v *VirtualDataSource) IsConnected() bool {
	return true
}

// IsWritable returns false - virtual data source is read-only
func (v *VirtualDataSource) IsWritable() bool {
	return false
}

// GetConfig returns a minimal data source config for virtual data source
func (v *VirtualDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{
		Type:   "virtual",
		Name:   "information_schema",
	}
}

// GetTables returns all virtual table names
func (v *VirtualDataSource) GetTables(ctx context.Context) ([]string, error) {
	if v.provider == nil {
		return nil, fmt.Errorf("virtual data source has no provider configured")
	}
	return v.provider.ListVirtualTables(), nil
}

// GetTableInfo returns the schema information for a virtual table
func (v *VirtualDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	if v.provider == nil {
		return nil, fmt.Errorf("virtual data source has no provider configured")
	}
	vt, err := v.provider.GetVirtualTable(tableName)
	if err != nil {
		return nil, err
	}

	return &domain.TableInfo{
		Name:    tableName,
		Schema:  "information_schema",
		Columns: vt.GetSchema(),
	}, nil
}

// Query executes a query against a virtual table
func (v *VirtualDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	if v.provider == nil {
		return nil, fmt.Errorf("virtual data source has no provider configured")
	}
	vt, err := v.provider.GetVirtualTable(tableName)
	if err != nil {
		return nil, err
	}

	// Delegate query to the virtual table
	var filters []domain.Filter
	if options != nil {
		filters = options.Filters
	}
	return vt.Query(ctx, filters, options)
}

// Insert is not supported - virtual data source is read-only
func (v *VirtualDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, fmt.Errorf("information_schema is read-only: INSERT operation not supported")
}

// Update is not supported - virtual data source is read-only
func (v *VirtualDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, fmt.Errorf("information_schema is read-only: UPDATE operation not supported")
}

// Delete is not supported - virtual data source is read-only
func (v *VirtualDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, fmt.Errorf("information_schema is read-only: DELETE operation not supported")
}

// CreateTable is not supported - virtual data source is read-only
func (v *VirtualDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return fmt.Errorf("information_schema is read-only: CREATE TABLE operation not supported")
}

// DropTable is not supported - virtual data source is read-only
func (v *VirtualDataSource) DropTable(ctx context.Context, tableName string) error {
	return fmt.Errorf("information_schema is read-only: DROP TABLE operation not supported")
}

// TruncateTable is not supported - virtual data source is read-only
func (v *VirtualDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return fmt.Errorf("information_schema is read-only: TRUNCATE operation not supported")
}

// Execute is not supported - virtual data source is read-only
func (v *VirtualDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, fmt.Errorf("information_schema is read-only: EXECUTE operation not supported")
}
