package virtual

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// VirtualTable represents a virtual table that generates data dynamically
// Virtual tables don't store persistent data; they compute results on-the-fly
type VirtualTable interface {
	// GetName returns the table name
	GetName() string

	// GetSchema returns the table schema (column definitions)
	GetSchema() []domain.ColumnInfo

	// Query executes a query against the virtual table
	// It applies filters and options to generate the result set dynamically
	Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error)
}

// VirtualTableProvider provides access to a collection of virtual tables
type VirtualTableProvider interface {
	// GetVirtualTable returns a virtual table by name
	// Returns error if table doesn't exist
	GetVirtualTable(name string) (VirtualTable, error)

	// ListVirtualTables returns all available virtual table names
	ListVirtualTables() []string

	// HasTable returns true if a virtual table with the given name exists
	HasTable(name string) bool
}
