package domain

import "context"

// TableRepository defines the interface for table data access operations.
// This follows the Repository pattern from DDD, separating persistence logic from domain logic.
type TableRepository interface {
	// FindByID retrieves a table by its name
	FindByID(ctx context.Context, tableName string) (*TableInfo, error)

	// FindAll retrieves all tables
	FindAll(ctx context.Context) ([]*TableInfo, error)

	// FindWithFilter retrieves rows from a table with filtering
	FindWithFilter(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)

	// Save persists a table (create or update)
	Save(ctx context.Context, table *TableInfo) error

	// Delete removes a table
	Delete(ctx context.Context, tableName string) error

	// Exists checks if a table exists
	Exists(ctx context.Context, tableName string) (bool, error)
}

// SchemaRepository defines the interface for schema metadata operations.
// This handles DDL-related operations separately from data operations.
type SchemaRepository interface {
	// GetTableInfo retrieves table metadata
	GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)

	// GetAllTables retrieves all table names
	GetAllTables(ctx context.Context) ([]string, error)

	// TableExists checks if a table exists
	TableExists(ctx context.Context, tableName string) (bool, error)

	// CreateTable creates a new table
	CreateTable(ctx context.Context, tableInfo *TableInfo) error

	// DropTable drops a table
	DropTable(ctx context.Context, tableName string) error

	// GetSchema retrieves schema information
	GetSchema(ctx context.Context) (*Schema, error)
}

// DataRowRepository defines the interface for data row operations.
// This separates data access from schema operations.
type DataRowRepository interface {
	// Query retrieves rows from a table
	Query(ctx context.Context, tableName string, options *QueryOptions) (*QueryResult, error)

	// Insert inserts rows into a table
	Insert(ctx context.Context, tableName string, rows []Row, options *InsertOptions) (int64, error)

	// Update updates rows in a table
	Update(ctx context.Context, tableName string, filters []Filter, updates Row, options *UpdateOptions) (int64, error)

	// Delete deletes rows from a table
	Delete(ctx context.Context, tableName string, filters []Filter, options *DeleteOptions) (int64, error)

	// Count counts rows in a table
	Count(ctx context.Context, tableName string, filters []Filter) (int64, error)
}

// IndexRepository defines the interface for index operations.
type IndexRepository interface {
	// GetIndexes retrieves all indexes for a table
	GetIndexes(ctx context.Context, tableName string) ([]*Index, error)

	// CreateIndex creates a new index
	CreateIndex(ctx context.Context, index *Index) error

	// DropIndex drops an index
	DropIndex(ctx context.Context, tableName string, indexName string) error

	// IndexExists checks if an index exists
	IndexExists(ctx context.Context, tableName string, indexName string) (bool, error)
}
