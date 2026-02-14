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

// maxRetainedVersions is the maximum number of old versions to keep per table
// beyond what active transactions require
const maxRetainedVersions = 2

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

// gcOldVersions removes old table versions that are no longer referenced by
// any active transaction. Must be called while holding m.mu.Lock().
func (m *MVCCDataSource) gcOldVersions() {
	// Find the minimum version still needed by active transactions
	minRequiredVer := m.currentVer
	for _, snapshot := range m.snapshots {
		if snapshot.startVer < minRequiredVer {
			minRequiredVer = snapshot.startVer
		}
	}

	// Clean up old versions from each table
	for _, tableVer := range m.tables {
		tableVer.mu.Lock()
		for ver := range tableVer.versions {
			// Keep the latest version, versions needed by active transactions,
			// and a small buffer of recent versions
			if ver < minRequiredVer && ver != tableVer.latest {
				delete(tableVer.versions, ver)
			}
		}
		tableVer.mu.Unlock()
	}
}
