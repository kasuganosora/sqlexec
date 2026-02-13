package memory

import (
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Core Types ====================

// TableVersions stores multi-version data for a table
type TableVersions struct {
	mu       sync.RWMutex
	versions map[int64]*TableData // version -> data
	latest   int64                // latest version number
}

// TableData represents a single version of table data
type TableData struct {
	version   int64
	createdAt time.Time
	schema    *domain.TableInfo
	rows      []domain.Row
}

// COWTableSnapshot represents a copy-on-write snapshot of a table
type COWTableSnapshot struct {
	tableName    string
	copied       bool                // whether a modified copy has been created
	baseData     *TableData          // base data reference (when not modified)
	modifiedData *TableData          // modified data
	rowLocks     map[int64]bool      // row-level locks: track which rows are modified
	rowCopies    map[int64]domain.Row // row-level copies: store modified rows
	deletedRows  map[int64]bool      // row-level deletion: mark deleted rows
	mu           sync.RWMutex
}

// Snapshot represents a transaction snapshot (copy-on-write)
type Snapshot struct {
	txnID         int64
	startVer      int64
	createdAt     time.Time
	tableSnapshots map[string]*COWTableSnapshot // COW snapshot per table
}

// Transaction represents transaction information
type Transaction struct {
	txnID     int64
	startTime time.Time
	readOnly  bool
}
