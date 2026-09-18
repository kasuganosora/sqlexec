package memory

import (
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func autoIncKey(tableName, columnName string) string {
	return tableName + "." + columnName
}

func asInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		return 0, false
	}
}

func isAutoIncUnset(val interface{}, exists bool) bool {
	if !exists || val == nil {
		return true
	}
	n, ok := asInt64(val)
	return ok && n == 0
}

// assignAutoInc fills missing AUTO_INCREMENT values.
// Caller must hold m.mu. Transaction inserts reserve IDs on the snapshot and
// only publish them on a successful commit, so rollback does not skip values.
func (m *MVCCDataSource) assignAutoInc(tableName string, schema *domain.TableInfo, rows []domain.Row, snapshot *Snapshot) {
	if schema == nil {
		return
	}
	for _, row := range rows {
		for _, col := range schema.Columns {
			if !col.AutoIncrement {
				continue
			}
			key := autoIncKey(tableName, col.Name)
			val, exists := row[col.Name]
			if isAutoIncUnset(val, exists) {
				row[col.Name] = m.nextAutoInc(key, snapshot)
				continue
			}
			if n, ok := asInt64(val); ok {
				m.observeAutoInc(key, n, snapshot)
			}
		}
	}
}

func (m *MVCCDataSource) nextAutoInc(key string, snapshot *Snapshot) int64 {
	base := m.autoIncHighWater(key, snapshot)
	next := base + 1
	if snapshot != nil {
		if snapshot.autoIncReserved == nil {
			snapshot.autoIncReserved = make(map[string]int64)
		}
		snapshot.autoIncReserved[key] = next
		return next
	}
	m.autoIncCounters[key] = next
	return next
}

func (m *MVCCDataSource) observeAutoInc(key string, val int64, snapshot *Snapshot) {
	if snapshot != nil {
		if snapshot.autoIncReserved == nil {
			snapshot.autoIncReserved = make(map[string]int64)
		}
		if val > snapshot.autoIncReserved[key] {
			snapshot.autoIncReserved[key] = val
		}
		return
	}
	if val > m.autoIncCounters[key] {
		m.autoIncCounters[key] = val
	}
}

func (m *MVCCDataSource) autoIncHighWater(key string, snapshot *Snapshot) int64 {
	base := m.autoIncCounters[key]
	if snapshot != nil && snapshot.autoIncReserved != nil {
		if reserved := snapshot.autoIncReserved[key]; reserved > base {
			return reserved
		}
	}
	return base
}

func (m *MVCCDataSource) publishAutoInc(snapshot *Snapshot) {
	if snapshot == nil {
		return
	}
	for key, reserved := range snapshot.autoIncReserved {
		if reserved > m.autoIncCounters[key] {
			m.autoIncCounters[key] = reserved
		}
	}
}

func (m *MVCCDataSource) snapshotAutoInc(keys []string) map[string]int64 {
	if len(keys) == 0 {
		return nil
	}
	prev := make(map[string]int64, len(keys))
	for _, key := range keys {
		prev[key] = m.autoIncCounters[key]
	}
	return prev
}

func (m *MVCCDataSource) restoreAutoInc(prev map[string]int64) {
	for key, val := range prev {
		m.autoIncCounters[key] = val
	}
}

func autoIncKeysForSchema(tableName string, schema *domain.TableInfo) []string {
	if schema == nil {
		return nil
	}
	keys := make([]string, 0, 1)
	for _, col := range schema.Columns {
		if col.AutoIncrement {
			keys = append(keys, autoIncKey(tableName, col.Name))
		}
	}
	return keys
}

func maxAutoIncFromRows(tableName string, schema *domain.TableInfo, rows []domain.Row) map[string]int64 {
	maxes := make(map[string]int64)
	if schema == nil {
		return maxes
	}
	for _, col := range schema.Columns {
		if !col.AutoIncrement {
			continue
		}
		key := autoIncKey(tableName, col.Name)
		for _, row := range rows {
			if n, ok := asInt64(row[col.Name]); ok && n > maxes[key] {
				maxes[key] = n
			}
		}
	}
	return maxes
}

func (m *MVCCDataSource) raiseAutoInc(maxes map[string]int64) {
	for key, val := range maxes {
		if val > m.autoIncCounters[key] {
			m.autoIncCounters[key] = val
		}
	}
}

func (m *MVCCDataSource) clearAutoIncForTable(tableName string) {
	prefix := tableName + "."
	for key := range m.autoIncCounters {
		if strings.HasPrefix(key, prefix) {
			delete(m.autoIncCounters, key)
		}
	}
}
