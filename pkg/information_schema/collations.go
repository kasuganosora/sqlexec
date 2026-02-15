package information_schema

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// CollationsTable represents information_schema.collations
// It lists all supported collations in the system
type CollationsTable struct{}

// NewCollationsTable creates a new CollationsTable
func NewCollationsTable() virtual.VirtualTable {
	return &CollationsTable{}
}

// GetName returns the table name
func (t *CollationsTable) GetName() string {
	return "collations"
}

// GetSchema returns the table schema
func (t *CollationsTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "COLLATION_NAME", Type: "varchar(64)", Nullable: false},
		{Name: "CHARACTER_SET_NAME", Type: "varchar(32)", Nullable: false},
		{Name: "ID", Type: "bigint", Nullable: false},
		{Name: "IS_DEFAULT", Type: "varchar(3)", Nullable: false},
		{Name: "IS_COMPILED", Type: "varchar(3)", Nullable: false},
		{Name: "SORTLEN", Type: "bigint", Nullable: false},
	}
}

// Query executes a query against the collations table
func (t *CollationsTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	engine := utils.GetGlobalCollationEngine()
	collations := engine.ListCollations()

	rows := make([]domain.Row, 0, len(collations))
	for _, c := range collations {
		isDefault := "No"
		if c.Name == "utf8mb4_0900_ai_ci" {
			isDefault = "Yes"
		}

		row := domain.Row{
			"COLLATION_NAME":     c.Name,
			"CHARACTER_SET_NAME": c.Charset,
			"ID":                 int64(c.ID),
			"IS_DEFAULT":         isDefault,
			"IS_COMPILED":        "Yes",
			"SORTLEN":            int64(8),
		}
		rows = append(rows, row)
	}

	// Apply filters if provided
	if len(filters) > 0 {
		var err error
		rows, err = utils.ApplyFilters(rows, filters)
		if err != nil {
			return nil, fmt.Errorf("failed to apply filters: %w", err)
		}
	}

	// Apply limit/offset if specified
	if options != nil && options.Limit > 0 {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		end := start + int(options.Limit)
		if end > len(rows) {
			end = len(rows)
		}
		if start >= len(rows) {
			rows = []domain.Row{}
		} else {
			rows = rows[start:end]
		}
	}

	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    rows,
		Total:   int64(len(rows)),
	}, nil
}
