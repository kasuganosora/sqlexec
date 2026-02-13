package memory

import (
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== MVCC Data Source Implementation ====================

// MVCCDataSource is an in-memory data source with multi-version concurrency control
// This is the foundation for all external data sources, all data sources should map here
type MVCCDataSource struct {
	config    *domain.DataSourceConfig
	connected bool
	mu        sync.RWMutex

	// Index management
	indexManager *IndexManager
	queryPlanner *QueryPlanner

	// MVCC related
	nextTxID   int64
	currentVer int64
	snapshots  map[int64]*Snapshot
	activeTxns map[int64]*Transaction

	// Data storage (version managed)
	tables map[string]*TableVersions

	// Temporary tables (automatically deleted when session ends)
	tempTables map[string]bool
}

// NewMVCCDataSource creates an MVCC in-memory data source
func NewMVCCDataSource(config *domain.DataSourceConfig) *MVCCDataSource {
	if config == nil {
		config = &domain.DataSourceConfig{
			Type:     domain.DataSourceTypeMemory,
			Name:     "memory",
			Writable: true,
		}
	}

	indexMgr := NewIndexManager()
	return &MVCCDataSource{
		config:        config,
		connected:     false,
		indexManager:  indexMgr,
		queryPlanner:  NewQueryPlanner(indexMgr),
		nextTxID:      1,
		currentVer:    0,
		snapshots:     make(map[int64]*Snapshot),
		activeTxns:    make(map[int64]*Transaction),
		tables:        make(map[string]*TableVersions),
		tempTables:    make(map[string]bool),
	}
}
