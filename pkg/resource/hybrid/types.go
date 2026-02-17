// Package hybrid provides a hybrid data source that combines memory and persistent storage.
// It allows per-table configuration of persistence behavior.
package hybrid

import (
	"github.com/dgraph-io/badger/v4"
	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// HybridDataSourceConfig configuration for HybridDataSource
type HybridDataSourceConfig struct {
	// DataDir directory for persistent data storage
	DataDir string `json:"data_dir"`

	// DefaultPersistent default persistence policy (default: false)
	// If true, all tables are persisted by default unless explicitly disabled
	DefaultPersistent bool `json:"default_persistent"`

	// EnableBadger whether to enable Badger backend (default: true)
	EnableBadger bool `json:"enable_badger"`

	// BadgerOptions custom Badger options (optional)
	BadgerOptions *badger.Options `json:"-"`

	// CacheConfig memory cache configuration for persistent tables
	CacheConfig *CacheConfig `json:"cache_config"`
}

// DefaultHybridConfig returns default configuration
func DefaultHybridConfig(dataDir string) *HybridDataSourceConfig {
	return &HybridDataSourceConfig{
		DataDir:           dataDir,
		DefaultPersistent: false,
		EnableBadger:      true,
		CacheConfig:       DefaultCacheConfig(),
	}
}

// CacheConfig memory cache configuration
type CacheConfig struct {
	// Enabled whether to cache persistent tables in memory
	Enabled bool `json:"enabled"`

	// MaxSizeMB maximum cache size in MB
	MaxSizeMB int `json:"max_size_mb"`

	// EvictionPolicy cache eviction policy ("lru", "lfu")
	EvictionPolicy string `json:"eviction_policy"`
}

// DefaultCacheConfig returns default cache configuration
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		Enabled:        true,
		MaxSizeMB:      256,
		EvictionPolicy: "lru",
	}
}

// TableConfig table-level persistence configuration
type TableConfig struct {
	TableName     string `json:"table_name"`
	Persistent    bool   `json:"persistent"`      // Whether to persist this table
	SyncOnWrite   bool   `json:"sync_on_write"`   // Sync to disk on each write
	CacheInMemory bool   `json:"cache_in_memory"` // Keep in memory cache
}

// PersistenceOption functional option for table persistence configuration
type PersistenceOption func(*TableConfig)

// WithSyncOnWrite sets sync on write option
func WithSyncOnWrite(sync bool) PersistenceOption {
	return func(tc *TableConfig) {
		tc.SyncOnWrite = sync
	}
}

// WithCacheInMemory sets cache in memory option
func WithCacheInMemory(cache bool) PersistenceOption {
	return func(tc *TableConfig) {
		tc.CacheInMemory = cache
	}
}

// OperationType type of operation being performed
type OperationType int

const (
	// OpRead read operation
	OpRead OperationType = iota
	// OpWrite write operation (insert/update/delete)
	OpWrite
	// OpDDL DDL operation (create/drop/truncate table)
	OpDDL
)

// RouteDecision routing decision for data source
type RouteDecision int

const (
	// RouteMemoryOnly route to memory data source only
	RouteMemoryOnly RouteDecision = iota
	// RouteBadgerOnly route to Badger data source only
	RouteBadgerOnly
	// RouteBoth route to both data sources (for dual-write scenarios)
	RouteBoth
)

// Stats statistics for HybridDataSource
type Stats struct {
	// Table stats
	TotalTableCount   int `json:"total_table_count"`
	MemoryTableCount  int `json:"memory_table_count"`
	PersistentCount   int `json:"persistent_table_count"`

	// Operation stats
	TotalReads      int64 `json:"total_reads"`
	TotalWrites     int64 `json:"total_writes"`
	MemoryReads     int64 `json:"memory_reads"`
	MemoryWrites    int64 `json:"memory_writes"`
	BadgerReads     int64 `json:"badger_reads"`
	BadgerWrites    int64 `json:"badger_writes"`
	CacheHits       int64 `json:"cache_hits"`
	CacheMisses     int64 `json:"cache_misses"`
}

// Ensure interfaces are implemented
var (
	_ domain.DataSource = (*HybridDataSource)(nil)
	_ domain.TransactionalDataSource = (*HybridDataSource)(nil)
)
