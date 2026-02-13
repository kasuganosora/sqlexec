package memory

import (
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Adapter Interface ====================

// LoadTable loads table data into memory (for external data source adapters)
func (m *MVCCDataSource) LoadTable(tableName string, schema *domain.TableInfo, rows []domain.Row) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new version
	m.currentVer++

	// Deep copy schema
	cols := make([]domain.ColumnInfo, len(schema.Columns))
	copy(cols, schema.Columns)

	// Deep copy table attributes
	var atts map[string]interface{}
	if schema.Atts != nil {
		atts = make(map[string]interface{}, len(schema.Atts))
		for k, v := range schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    schema.Name,
			Schema:  schema.Schema,
			Columns: cols,
			Atts:    atts,
		},
		rows: rows,
	}

	if existing, ok := m.tables[tableName]; ok {
		existing.mu.Lock()
		existing.versions[m.currentVer] = versionData
		existing.latest = m.currentVer
		existing.mu.Unlock()
	} else {
		m.tables[tableName] = &TableVersions{
			versions: map[int64]*TableData{
				m.currentVer: versionData,
			},
			latest: m.currentVer,
		}
	}

	// Rebuild index
	_ = m.indexManager.RebuildIndex(tableName, versionData.schema, rows)

	return nil
}

// GetLatestTableData gets latest table data (for external data source adapter write-back)
func (m *MVCCDataSource) GetLatestTableData(tableName string) (*domain.TableInfo, []domain.Row, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return nil, nil, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.RLock()
	defer tableVer.mu.RUnlock()

	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return nil, nil, domain.NewErrTableNotFound(tableName)
	}

	return latest.schema, latest.rows, nil
}

// GetCurrentVersion gets current version number
func (m *MVCCDataSource) GetCurrentVersion() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentVer
}
