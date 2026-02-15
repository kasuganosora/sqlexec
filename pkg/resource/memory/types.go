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
	rows      *PagedRows
}

// Rows materializes all paged rows into a flat slice for compatibility
// with existing code that expects []domain.Row.
func (td *TableData) Rows() []domain.Row {
	if td == nil || td.rows == nil {
		return nil
	}
	return td.rows.Materialize()
}

// RowCount returns the total number of rows without materializing.
func (td *TableData) RowCount() int {
	if td == nil || td.rows == nil {
		return 0
	}
	return td.rows.Len()
}

// COWTableSnapshot represents a copy-on-write snapshot of a table
type COWTableSnapshot struct {
	tableName     string
	copied        bool                // whether a modified copy has been created
	baseData      *TableData          // base data reference (when not modified)
	modifiedData  *TableData          // modified data
	rowLocks      map[int64]bool      // row-level locks: track which rows are modified
	rowCopies     map[int64]domain.Row // row-level copies: store modified rows
	deletedRows   map[int64]bool      // row-level deletion: mark deleted rows
	insertedCount int64               // number of rows inserted in this transaction
	mu            sync.RWMutex
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

// deepCopySchema returns a deep copy of a TableInfo
func deepCopySchema(src *domain.TableInfo) *domain.TableInfo {
	if src == nil {
		return nil
	}
	cols := make([]domain.ColumnInfo, len(src.Columns))
	copy(cols, src.Columns)

	var atts map[string]interface{}
	if src.Atts != nil {
		atts = make(map[string]interface{}, len(src.Atts))
		for k, v := range src.Atts {
			atts[k] = v
		}
	}

	return &domain.TableInfo{
		Name:      src.Name,
		Schema:    src.Schema,
		Columns:   cols,
		Temporary: src.Temporary,
		Atts:      atts,
	}
}

// deepCopyRow returns a deep copy of a Row
func deepCopyRow(src domain.Row) domain.Row {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
