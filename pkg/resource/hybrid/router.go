package hybrid

import (
	"github.com/dgraph-io/badger/v4"
	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// DataSourceRouter routes operations to appropriate data sources
type DataSourceRouter struct {
	tableConfig *TableConfigManager
	memory      *memory.MVCCDataSource
	badgerDS    domain.DataSource
	badgerDB    *badger.DB
}

// NewDataSourceRouter creates a new DataSourceRouter
func NewDataSourceRouter(
	tableConfig *TableConfigManager,
	memoryDS *memory.MVCCDataSource,
	badgerDS domain.DataSource,
	badgerDB *badger.DB,
) *DataSourceRouter {
	return &DataSourceRouter{
		tableConfig: tableConfig,
		memory:      memoryDS,
		badgerDS:    badgerDS,
		badgerDB:    badgerDB,
	}
}

// Decide determines which data source(s) to use for an operation
func (r *DataSourceRouter) Decide(tableName string, op OperationType) RouteDecision {
	config := r.tableConfig.GetConfig(tableName)

	if config.Persistent {
		// Table is configured for persistence
		if r.badgerDS == nil {
			// Badger not available, fall back to memory
			return RouteMemoryOnly
		}
		return RouteBadgerOnly
	}

	// Default: memory only
	return RouteMemoryOnly
}

// DecideWithBadger determines routing considering Badger availability
// This is used when testing router logic without actual Badger instance
func (r *DataSourceRouter) DecideWithBadger(tableName string, op OperationType, hasBadger bool) RouteDecision {
	config := r.tableConfig.GetConfig(tableName)

	if config.Persistent {
		if !hasBadger {
			return RouteMemoryOnly
		}
		return RouteBadgerOnly
	}

	return RouteMemoryOnly
}

// GetReadSource returns the data source for read operations
func (r *DataSourceRouter) GetReadSource(tableName string) domain.DataSource {
	decision := r.Decide(tableName, OpRead)
	switch decision {
	case RouteBadgerOnly:
		return r.badgerDS
	case RouteBoth:
		// For dual-read, prefer memory cache if available
		if r.memory != nil {
			return r.memory
		}
		return r.badgerDS
	default:
		return r.memory
	}
}

// GetWriteSource returns the data source for write operations
func (r *DataSourceRouter) GetWriteSource(tableName string) domain.DataSource {
	decision := r.Decide(tableName, OpWrite)
	switch decision {
	case RouteBadgerOnly:
		return r.badgerDS
	case RouteBoth:
		// For dual-write, use both (handled separately)
		return r.memory
	default:
		return r.memory
	}
}

// GetDDLSource returns the data source for DDL operations
func (r *DataSourceRouter) GetDDLSource(tableName string) domain.DataSource {
	decision := r.Decide(tableName, OpDDL)
	switch decision {
	case RouteBadgerOnly:
		return r.badgerDS
	default:
		return r.memory
	}
}

// ShouldDualWrite returns true if both data sources should be written to
func (r *DataSourceRouter) ShouldDualWrite(tableName string) bool {
	decision := r.Decide(tableName, OpWrite)
	return decision == RouteBoth
}

// HasBadger returns true if Badger backend is available
func (r *DataSourceRouter) HasBadger() bool {
	return r.badgerDS != nil
}

// GetMemorySource returns the memory data source
func (r *DataSourceRouter) GetMemorySource() *memory.MVCCDataSource {
	return r.memory
}

// GetBadgerSource returns the Badger data source
func (r *DataSourceRouter) GetBadgerSource() domain.DataSource {
	return r.badgerDS
}

// GetBadgerDB returns the underlying Badger DB
func (r *DataSourceRouter) GetBadgerDB() *badger.DB {
	return r.badgerDB
}

// SetBadgerDS sets the Badger data source
func (r *DataSourceRouter) SetBadgerDS(ds domain.DataSource, db *badger.DB) {
	r.badgerDS = ds
	r.badgerDB = db
}
