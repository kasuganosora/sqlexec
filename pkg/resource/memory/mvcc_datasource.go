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

	// Buffer pool for page-based virtual memory
	bufferPool *BufferPool

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

// NewMVCCDataSource creates an MVCC in-memory data source.
// An optional *PagingConfig can be passed to configure the buffer pool.
// If nil or not provided, the buffer pool runs in passthrough mode (no eviction).
func NewMVCCDataSource(config *domain.DataSourceConfig, opts ...*PagingConfig) *MVCCDataSource {
	if config == nil {
		config = &domain.DataSourceConfig{
			Type:     domain.DataSourceTypeMemory,
			Name:     "memory",
			Writable: true,
		}
	}

	var pagingCfg *PagingConfig
	if len(opts) > 0 {
		pagingCfg = opts[0]
	}

	indexMgr := NewIndexManager()
	return &MVCCDataSource{
		config:        config,
		connected:     false,
		indexManager:  indexMgr,
		queryPlanner:  NewQueryPlanner(indexMgr),
		bufferPool:    NewBufferPool(pagingCfg),
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
	for tableName, tableVer := range m.tables {
		tableVer.mu.Lock()
		for ver, data := range tableVer.versions {
			// Keep the latest version, versions needed by active transactions,
			// and a small buffer of recent versions
			if ver < minRequiredVer && ver != tableVer.latest {
				// Release paged rows to free buffer pool memory and spill files
				if data != nil && data.rows != nil {
					data.rows.Release()
				}
				delete(tableVer.versions, ver)
			}
		}
		// Update buffer pool latest version for eviction priority
		if m.bufferPool != nil {
			m.bufferPool.UpdateLatestVersion(tableName, tableVer.latest)
		}
		tableVer.mu.Unlock()
	}
}

