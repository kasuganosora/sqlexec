package information_schema

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
	"github.com/kasuganosora/sqlexec/pkg/virtual"
)

// EnginesTable represents information_schema.ENGINES
// It lists all available storage engines based on registered data sources
type EnginesTable struct {
	dsManager *application.DataSourceManager
	registry  *application.Registry
}

// NewEnginesTable creates a new EnginesTable
func NewEnginesTable(dsManager *application.DataSourceManager) virtual.VirtualTable {
	var registry *application.Registry
	if dsManager != nil {
		registry = dsManager.GetRegistry()
	}
	return &EnginesTable{dsManager: dsManager, registry: registry}
}

// GetName returns table name
func (t *EnginesTable) GetName() string {
	return "ENGINES"
}

// GetSchema returns table schema
func (t *EnginesTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "ENGINE", Type: "varchar(64)", Nullable: false},
		{Name: "SUPPORT", Type: "varchar(8)", Nullable: false},
		{Name: "COMMENT", Type: "varchar(80)", Nullable: false},
		{Name: "TRANSACTIONS", Type: "varchar(3)", Nullable: true},
		{Name: "XA", Type: "varchar(3)", Nullable: true},
		{Name: "SAVEPOINTS", Type: "varchar(3)", Nullable: true},
	}
}

// Query executes a query against ENGINES table
func (t *EnginesTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	engines := t.getEngines()

	var err error
	if len(filters) > 0 {
		engines, err = utils.ApplyFilters(engines, filters)
		if err != nil {
			return nil, err
		}
	}

	if options != nil && options.Limit > 0 {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		end := start + int(options.Limit)
		if end > len(engines) {
			end = len(engines)
		}
		if start >= len(engines) {
			engines = []domain.Row{}
		} else {
			engines = engines[start:end]
		}
	}

	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    engines,
		Total:   int64(len(engines)),
	}, nil
}

// getEngines returns all available storage engines based on registered data sources
func (t *EnginesTable) getEngines() []domain.Row {
	rows := make([]domain.Row, 0)
	seenEngines := make(map[string]bool)

	// If dsManager is available, get engines from registered data sources
	if t.dsManager != nil {
		dataSources := t.dsManager.GetAllDataSources()
		defaultName := t.dsManager.GetDefaultName()

		for name, ds := range dataSources {
			config := ds.GetConfig()
			if config == nil {
				continue
			}

			engineName := string(config.Type)
			// Skip if already seen (use type as engine name)
			if seenEngines[engineName] {
				continue
			}
			seenEngines[engineName] = true

			// Get metadata from factory registry
			var meta domain.DriverMetadata
			if t.registry != nil {
				if factory, err := t.registry.Get(config.Type); err == nil {
					meta = factory.GetMetadata()
				}
			}
			// Fallback to default metadata if factory not found
			if meta.Comment == "" {
				meta = domain.DriverMetadata{
					Comment:      "Unknown storage engine",
					Transactions: "NO",
					XA:           "NO",
					Savepoints:   "NO",
				}
			}

			support := "YES"
			if name == defaultName {
				support = "DEFAULT"
			}

			rows = append(rows, domain.Row{
				"ENGINE":       engineName,
				"SUPPORT":      support,
				"COMMENT":      meta.Comment,
				"TRANSACTIONS": meta.Transactions,
				"XA":           meta.XA,
				"SAVEPOINTS":   meta.Savepoints,
			})
		}
	}

	// If no data sources registered, return default InnoDB-like engine
	if len(rows) == 0 {
		rows = append(rows, domain.Row{
			"ENGINE":       "InnoDB",
			"SUPPORT":      "DEFAULT",
			"COMMENT":      "Supports transactions, row-level locking, and foreign keys",
			"TRANSACTIONS": "YES",
			"XA":           "YES",
			"SAVEPOINTS":   "YES",
		})
	}

	return rows
}
