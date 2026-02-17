// Package badger provides a persistent storage backend using Badger KV store.
// It implements the domain.DataSource and domain.TransactionalDataSource interfaces.
package badger

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Key prefixes for Badger key-value store
const (
	// PrefixTable table metadata prefix
	PrefixTable = "table:"
	// PrefixRow row data prefix
	PrefixRow = "row:"
	// PrefixIndex index data prefix
	PrefixIndex = "idx:"
	// PrefixSeq sequence number prefix (for auto-increment)
	PrefixSeq = "seq:"
	// PrefixConfig table configuration prefix
	PrefixConfig = "config:"
)

// DataSourceConfig configuration for BadgerDataSource
type DataSourceConfig struct {
	// DataDir directory for storing data files
	DataDir string `json:"data_dir"`

	// InMemory if true, runs in pure memory mode (no disk persistence)
	InMemory bool `json:"in_memory"`

	// SyncWrites if true, syncs writes to disk immediately
	SyncWrites bool `json:"sync_writes"`

	// ValueThreshold values larger than this are stored in value log
	ValueThreshold int64 `json:"value_threshold"`

	// NumMemtables number of in-memory tables
	NumMemtables int `json:"num_memtables"`

	// BaseTableSize base size for LSM tables
	BaseTableSize int64 `json:"base_table_size"`

	// Compression compression type (0=none, 1=snappy, 2=zstd)
	Compression int `json:"compression"`

	// EncryptionKey optional encryption key
	EncryptionKey []byte `json:"-"`

	// Logger optional custom logger
	Logger badger.Logger `json:"-"`
}

// DefaultDataSourceConfig returns default configuration
func DefaultDataSourceConfig(dataDir string) *DataSourceConfig {
	return &DataSourceConfig{
		DataDir:        dataDir,
		InMemory:       false,
		SyncWrites:     false,
		ValueThreshold: 1 << 10, // 1KB
		NumMemtables:   5,
		BaseTableSize:  2 << 20, // 2MB
		Compression:    1,       // snappy
	}
}

// IndexInfo index metadata
type IndexInfo struct {
	TableName string    `json:"table_name"`
	Columns   []string  `json:"columns"`     // Support composite index (multi-column)
	Unique    bool      `json:"unique"`
	CreatedAt time.Time `json:"created_at"`
}

// TableConfig table-level persistence configuration
type TableConfig struct {
	TableName     string `json:"table_name"`
	Persistent    bool   `json:"persistent"`      // Whether to persist this table
	SyncOnWrite   bool   `json:"sync_on_write"`   // Sync to disk on each write
	CacheInMemory bool   `json:"cache_in_memory"` // Keep in memory cache
}

// Stats statistics for BadgerDataSource
type Stats struct {
	// Table stats
	TableCount int `json:"table_count"`

	// Badger stats
	LSMSize    int64 `json:"lsm_size"`
	VLogSize   int64 `json:"vlog_size"`
	KeyCount   int64 `json:"key_count"`

	// Performance stats
	TotalReads    int64 `json:"total_reads"`
	TotalWrites   int64 `json:"total_writes"`
	CacheHits     int64 `json:"cache_hits"`
	CacheMisses   int64 `json:"cache_misses"`

	// Last updated
	UpdatedAt time.Time `json:"updated_at"`
}
