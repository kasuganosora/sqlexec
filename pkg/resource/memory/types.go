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
	snapshotVer   int64                // table version pinned at transaction start (for snapshot isolation)
	copied        bool                 // whether a modified copy has been created
	baseData      *TableData           // base data reference (when not modified)
	modifiedData  *TableData           // modified data
	rowLocks      map[int64]bool       // row-level locks: track which rows are modified
	rowCopies     map[int64]domain.Row // row-level copies: store modified rows
	deletedRows   map[int64]bool       // row-level deletion: mark deleted rows
	insertedCount int64                // number of rows inserted in this transaction
	mu            sync.RWMutex
}

// Snapshot represents a transaction snapshot (copy-on-write)
type Snapshot struct {
	txnID          int64
	startVer       int64
	createdAt      time.Time
	tableSnapshots map[string]*COWTableSnapshot // COW snapshot per table
}

// Transaction represents transaction information
type Transaction struct {
	txnID     int64
	startTime time.Time
	readOnly  bool
}

// deepCopySchema returns a deep copy of a TableInfo, including pointer fields
// and slices inside ColumnInfo to ensure full MVCC isolation between versions.
func deepCopySchema(src *domain.TableInfo) *domain.TableInfo {
	if src == nil {
		return nil
	}
	cols := make([]domain.ColumnInfo, len(src.Columns))
	for i, col := range src.Columns {
		cols[i] = col
		// Deep copy pointer fields
		if col.ForeignKey != nil {
			fkCopy := *col.ForeignKey
			cols[i].ForeignKey = &fkCopy
		}
		// Deep copy slice fields
		if col.GeneratedDepends != nil {
			deps := make([]string, len(col.GeneratedDepends))
			copy(deps, col.GeneratedDepends)
			cols[i].GeneratedDepends = deps
		}
	}

	var atts map[string]interface{}
	if src.Atts != nil {
		atts = make(map[string]interface{}, len(src.Atts))
		for k, v := range src.Atts {
			atts[k] = deepCopyValue(v)
		}
	}

	return &domain.TableInfo{
		Name:      src.Name,
		Schema:    src.Schema,
		Columns:   cols,
		Temporary: src.Temporary,
		Atts:      atts,
		Charset:   src.Charset,
		Collation: src.Collation,
	}
}

// deepCopyRow returns a deep copy of a Row, recursively cloning reference types.
func deepCopyRow(src domain.Row) domain.Row {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = deepCopyValue(v)
	}
	return dst
}

// deepCopyValue recursively deep-copies a value, handling common types stored
// in Row maps. Unknown types are copied by value (acceptable for immutable types).
func deepCopyValue(v interface{}) interface{} {
	switch val := v.(type) {
	case nil:
		return nil
	case []byte:
		cp := make([]byte, len(val))
		copy(cp, val)
		return cp
	case []string:
		cp := make([]string, len(val))
		copy(cp, val)
		return cp
	case []float32:
		cp := make([]float32, len(val))
		copy(cp, val)
		return cp
	case []float64:
		cp := make([]float64, len(val))
		copy(cp, val)
		return cp
	case []int64:
		cp := make([]int64, len(val))
		copy(cp, val)
		return cp
	case []interface{}:
		cp := make([]interface{}, len(val))
		for i, item := range val {
			cp[i] = deepCopyValue(item)
		}
		return cp
	case map[string]interface{}:
		cp := make(map[string]interface{}, len(val))
		for k, item := range val {
			cp[k] = deepCopyValue(item)
		}
		return cp
	default:
		// Scalar types (bool, int, int64, float64, string, time.Time, etc.)
		// are immutable or value types — safe to copy by value.
		return v
	}
}
