package memory

import (
	"context"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
)

// ==================== Table Management ====================

// GetTables gets all tables (excluding temporary tables)
func (m *MVCCDataSource) GetTables(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.connected {
		return nil, domain.NewErrNotConnected("memory")
	}

	tables := make([]string, 0, len(m.tables))
	for name := range m.tables {
		// Exclude temporary tables
		if !m.tempTables[name] {
			tables = append(tables, name)
		}
	}
	return tables, nil
}

// GetAllTables gets all tables (including temporary tables)
func (m *MVCCDataSource) GetAllTables(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.connected {
		return nil, domain.NewErrNotConnected("memory")
	}

	tables := make([]string, 0, len(m.tables))
	for name := range m.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// GetTemporaryTables gets all temporary tables
func (m *MVCCDataSource) GetTemporaryTables(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.connected {
		return nil, domain.NewErrNotConnected("memory")
	}

	tables := make([]string, 0, len(m.tempTables))
	for name := range m.tempTables {
		tables = append(tables, name)
	}
	return tables, nil
}

// GetTableInfo gets table information
func (m *MVCCDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.RLock()
	defer tableVer.mu.RUnlock()

	// Get latest version data
	latest := tableVer.versions[tableVer.latest]
	if latest == nil {
		return nil, domain.NewErrTableNotFound(tableName)
	}

	// Deep copy table info
	cols := make([]domain.ColumnInfo, len(latest.schema.Columns))
	copy(cols, latest.schema.Columns)

	// Deep copy table attributes
	var atts map[string]interface{}
	if latest.schema.Atts != nil {
		atts = make(map[string]interface{}, len(latest.schema.Atts))
		for k, v := range latest.schema.Atts {
			atts[k] = v
		}
	}

	return &domain.TableInfo{
		Name:    latest.schema.Name,
		Schema:  latest.schema.Schema,
		Columns: cols,
		Atts:    atts,
	}, nil
}

// CreateTable creates a table
func (m *MVCCDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[tableInfo.Name]; ok {
		return domain.NewErrTableAlreadyExists(tableInfo.Name)
	}

	// Validate generated column definitions (if any)
	validator := &generated.GeneratedColumnValidator{}
	if err := validator.ValidateSchema(tableInfo); err != nil {
		return domain.NewErrGeneratedColumnValidation(err.Error())
	}

	// Deep copy table info
	cols := make([]domain.ColumnInfo, len(tableInfo.Columns))
	copy(cols, tableInfo.Columns)

	// Deep copy table attributes
	var atts map[string]interface{}
	if tableInfo.Atts != nil {
		atts = make(map[string]interface{}, len(tableInfo.Atts))
		for k, v := range tableInfo.Atts {
			atts[k] = v
		}
	}

	// Create new version
	m.currentVer++
	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:      tableInfo.Name,
			Schema:    tableInfo.Schema,
			Columns:   cols,
			Temporary: tableInfo.Temporary,
			Atts:      atts,
		},
		rows: NewEmptyPagedRows(m.bufferPool, 0),
	}

	m.tables[tableInfo.Name] = &TableVersions{
		versions: map[int64]*TableData{
			m.currentVer: versionData,
		},
		latest: m.currentVer,
	}

	// If temporary table, add to temporary table list
	if tableInfo.Temporary {
		m.tempTables[tableInfo.Name] = true
	}

	return nil
}

// DropTable drops a table
func (m *MVCCDataSource) DropTable(ctx context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tables[tableName]; !ok {
		return domain.NewErrTableNotFound(tableName)
	}

	delete(m.tables, tableName)
	// Drop indexes
	_ = m.indexManager.DropTableIndexes(tableName)
	return nil
}

// TruncateTable truncates a table
func (m *MVCCDataSource) TruncateTable(ctx context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		return domain.NewErrTableNotFound(tableName)
	}

	tableVer.mu.Lock()
	defer tableVer.mu.Unlock()

	// Create new version (empty data)
	m.currentVer++

	// Deep copy table attributes
	var atts map[string]interface{}
	if tableVer.versions[tableVer.latest].schema.Atts != nil {
		atts = make(map[string]interface{}, len(tableVer.versions[tableVer.latest].schema.Atts))
		for k, v := range tableVer.versions[tableVer.latest].schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    tableVer.versions[tableVer.latest].schema.Name,
			Schema:  tableVer.versions[tableVer.latest].schema.Schema,
			Columns: tableVer.versions[tableVer.latest].schema.Columns,
			Atts:    atts,
		},
		rows: NewEmptyPagedRows(m.bufferPool, 0),
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return nil
}

// CreateIndex creates an index (backward compatibility wrapper)
func (m *MVCCDataSource) CreateIndex(tableName, columnName, indexType string, unique bool) error {
	return m.CreateIndexWithColumns(tableName, []string{columnName}, indexType, unique)
}

// CreateIndexWithColumns creates an index on one or more columns (composite index support)
func (m *MVCCDataSource) CreateIndexWithColumns(tableName string, columnNames []string, indexType string, unique bool) error {
	// Convert index type
	var idxType IndexType
	switch indexType {
	case "btree":
		idxType = IndexTypeBTree
	case "hash":
		idxType = IndexTypeHash
	case "fulltext":
		idxType = IndexTypeFullText
	default:
		idxType = IndexTypeBTree // Default
	}

	// Create index
	_, err := m.indexManager.CreateIndexWithColumns(tableName, columnNames, idxType, unique)
	if err != nil {
		return domain.NewErrIndexCreationFailed(tableName, strings.Join(columnNames, ","), err.Error())
	}

	return nil
}

// DropIndex drops an index
func (m *MVCCDataSource) DropIndex(tableName, indexName string) error {
	err := m.indexManager.DropIndex(tableName, indexName)
	if err != nil {
		return domain.NewErrIndexDropFailed(tableName, indexName, err.Error())
	}

	return nil
}
