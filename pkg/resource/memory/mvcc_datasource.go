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

	// Auto-increment counters: tableName.columnName -> next value
	autoIncCounters map[string]int64

	snapshotStore VectorSnapshotStore
	vectorDirty   map[string]struct{}
	vectorDirtyMu sync.Mutex
}


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
		config:          config,
		connected:       false,
		indexManager:    indexMgr,
		queryPlanner:    NewQueryPlanner(indexMgr),
		bufferPool:      NewBufferPool(pagingCfg),
		nextTxID:        1,
		currentVer:      0,
		snapshots:       make(map[int64]*Snapshot),
		activeTxns:      make(map[int64]*Transaction),
		tables:          make(map[string]*TableVersions),
		tempTables:      make(map[string]bool),
		autoIncCounters: make(map[string]int64),
		vectorDirty:     make(map[string]struct{}),
	}
}

// GetBufferPool returns the buffer pool used by this data source.
func (m *MVCCDataSource) GetBufferPool() *BufferPool {
	return m.bufferPool
}

// gcOldVersions removes old table versions that are no longer referenced by
// any active transaction. Must be called while holding m.mu.Lock().
//
// Retention is per-table: a transaction pins each table's latest version at
// BeginTx (COWTableSnapshot.snapshotVer). That pin can be far behind
// snapshot.startVer / currentVer when other tables have been written, so GC
// must not use the global startVer as the only watermark.
func (m *MVCCDataSource) gcOldVersions() {
	for tableName, tableVer := range m.tables {
		tableVer.mu.Lock()
		minRequiredVer := tableVer.latest
		for _, snapshot := range m.snapshots {
			if cow, ok := snapshot.tableSnapshots[tableName]; ok && cow.snapshotVer < minRequiredVer {
				minRequiredVer = cow.snapshotVer
			}
		}
		for ver, data := range tableVer.versions {
			if ver < minRequiredVer && ver != tableVer.latest {
				if data != nil && data.rows != nil {
					data.rows.Release()
				}
				delete(tableVer.versions, ver)
			}
		}
		if m.bufferPool != nil {
			m.bufferPool.UpdateLatestVersion(tableName, tableVer.latest)
		}
		tableVer.mu.Unlock()
	}
}
