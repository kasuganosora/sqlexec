package memory

import (
	"context"
	"encoding/json"
	"fmt"
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

	tableVer, ok := m.tables[tableName]
	if !ok {
		return domain.NewErrTableNotFound(tableName)
	}

	// Release all PagedRows across all versions to free buffer pool memory
	tableVer.mu.Lock()
	for _, data := range tableVer.versions {
		if data != nil && data.rows != nil {
			data.rows.Release()
		}
	}
	tableVer.mu.Unlock()

	delete(m.tables, tableName)
	delete(m.tempTables, tableName)
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

	// Get latest version data (defensive nil check)
	latestData := tableVer.versions[tableVer.latest]
	if latestData == nil {
		return domain.NewErrTableNotFound(tableName)
	}

	// Create new version (empty data)
	m.currentVer++

	// Deep copy table attributes
	var atts map[string]interface{}
	if latestData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(latestData.schema.Atts))
		for k, v := range latestData.schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    latestData.schema.Name,
			Schema:  latestData.schema.Schema,
			Columns: latestData.schema.Columns,
			Atts:    atts,
		},
		rows: NewEmptyPagedRows(m.bufferPool, 0),
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	m.rebuildTableIndexes(tableName, versionData.schema, nil)
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

// GetTableIndexes returns index metadata for all indexes on a table
func (m *MVCCDataSource) GetTableIndexes(tableName string) ([]*IndexInfo, error) {
	return m.indexManager.GetTableIndexes(tableName)
}

// GetIndexManager returns the in-process index manager used for btree/vector indexes.
func (m *MVCCDataSource) GetIndexManager() *IndexManager {
	return m.indexManager
}

// CreateVectorIndex creates a vector index and builds it from current table rows.
func (m *MVCCDataSource) CreateVectorIndex(tableName, columnName string, metricType string, indexType string, dimension int, params map[string]interface{}) error {
	if tableName == "" || columnName == "" {
		return fmt.Errorf("vector index requires table and column")
	}

	schema, rows, err := m.snapshotTable(tableName)
	if err != nil {
		return err
	}

	colFound := false
	for _, col := range schema.Columns {
		if col.Name == columnName {
			colFound = true
			if dimension <= 0 {
				dimension = col.VectorDim
			}
			break
		}
	}
	if !colFound {
		return fmt.Errorf("column %s not found on table %s", columnName, tableName)
	}
	if dimension <= 0 {
		return fmt.Errorf("vector dimension is required for column %s", columnName)
	}
	if params == nil {
		params = map[string]interface{}{}
	}

	idx, err := m.indexManager.CreateVectorIndex(
		tableName,
		columnName,
		ParseVectorMetricType(metricType),
		ParseVectorIndexType(indexType),
		dimension,
		params,
	)
	if err != nil {
		return err
	}

	records := ExtractVectorRecords(schema, rows, columnName, dimension)
	if m.tryRestoreVectorSnapshot(idx, tableName, columnName, int64(len(records))) {
		return nil
	}
	if len(records) > 0 {
		if err := idx.Build(context.Background(), &sliceVectorLoader{records: records}); err != nil {
			return err
		}
	}
	m.saveVectorSnapshot(tableName, columnName, idx)
	return nil
}

func (m *MVCCDataSource) snapshotTable(tableName string) (*domain.TableInfo, []domain.Row, error) {
	m.mu.RLock()
	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.RUnlock()
		return nil, nil, domain.NewErrTableNotFound(tableName)
	}
	tableVer.mu.RLock()
	latest := tableVer.versions[tableVer.latest]
	tableVer.mu.RUnlock()
	m.mu.RUnlock()
	if latest == nil {
		return nil, nil, domain.NewErrTableNotFound(tableName)
	}
	return deepCopySchema(latest.schema), latest.Rows(), nil
}

// SetVectorSnapshotStore attaches durable storage for built vector indexes.
func (m *MVCCDataSource) SetVectorSnapshotStore(store VectorSnapshotStore) {
	m.snapshotStore = store
}

func (m *MVCCDataSource) markVectorDirty(tableName string) {
	m.vectorDirtyMu.Lock()
	m.vectorDirty[tableName] = struct{}{}
	m.vectorDirtyMu.Unlock()
}

func (m *MVCCDataSource) tryRestoreVectorSnapshot(idx VectorIndex, table, column string, expectCount int64) bool {
	if m.snapshotStore == nil {
		return false
	}
	data, err := m.snapshotStore.LoadVectorSnapshot(table, column)
	if err != nil || len(data) == 0 {
		return false
	}
	if err := ApplyVectorSnapshot(idx, data, expectCount); err != nil {
		return false
	}
	return true
}

func (m *MVCCDataSource) saveVectorSnapshot(table, column string, idx VectorIndex) {
	if m.snapshotStore == nil || idx == nil {
		return
	}
	data, err := EncodeVectorSnapshot(idx)
	if err != nil {
		return
	}
	_ = m.snapshotStore.SaveVectorSnapshot(table, column, data)
}

// FlushVectorSnapshots writes dirty vector indexes to the snapshot store.
func (m *MVCCDataSource) FlushVectorSnapshots() error {
	if m.snapshotStore == nil {
		return nil
	}
	m.vectorDirtyMu.Lock()
	tables := make([]string, 0, len(m.vectorDirty))
	for t := range m.vectorDirty {
		tables = append(tables, t)
	}
	m.vectorDirty = make(map[string]struct{})
	m.vectorDirtyMu.Unlock()

	for _, table := range tables {
		for _, info := range m.indexManager.GetVectorIndexInfos(table) {
			idx, err := m.indexManager.GetVectorIndex(table, info.ColumnName)
			if err != nil {
				continue
			}
			m.saveVectorSnapshot(table, info.ColumnName, idx)
		}
	}
	return nil
}

// CollectIndexMeta returns btree and vector index metadata for persistence.
func (m *MVCCDataSource) CollectIndexMeta(tableName string) ([]domain.IndexMetaInfo, error) {
	var out []domain.IndexMetaInfo
	if infos, err := m.GetTableIndexes(tableName); err == nil {
		for _, info := range infos {
			out = append(out, domain.IndexMetaInfo{
				Name:    info.Name,
				Table:   info.TableName,
				Type:    string(info.Type),
				Unique:  info.Unique,
				Columns: info.Columns,
			})
		}
	}
	for _, v := range m.indexManager.GetVectorIndexInfos(tableName) {
		paramsJSON := ""
		if v.Params != nil {
			if b, err := json.Marshal(v.Params); err == nil {
				paramsJSON = string(b)
			}
		}
		out = append(out, domain.IndexMetaInfo{
			Name:       "vec_" + v.ColumnName,
			Table:      tableName,
			Type:       string(v.Type),
			Columns:    []string{v.ColumnName},
			IsVector:   true,
			Metric:     string(v.Metric),
			Dimension:  v.Dimension,
			ParamsJSON: paramsJSON,
		})
	}
	return out, nil
}

// RestorePersistedIndex recreates a btree or vector index from sidecar metadata.
func (m *MVCCDataSource) RestorePersistedIndex(info domain.IndexMetaInfo) error {
	if info.IsVector || IndexType(info.Type).IsVectorIndex() {
		column := ""
		if len(info.Columns) > 0 {
			column = info.Columns[0]
		}
		var params map[string]interface{}
		if info.ParamsJSON != "" {
			_ = json.Unmarshal([]byte(info.ParamsJSON), &params)
			coerceJSONParams(params)
		}
		return m.CreateVectorIndex(info.Table, column, info.Metric, info.Type, info.Dimension, params)
	}
	return m.CreateIndexWithColumns(info.Table, info.Columns, info.Type, info.Unique)
}

func coerceJSONParams(params map[string]interface{}) {
	for k, v := range params {
		n, ok := v.(float64)
		if !ok || n != float64(int64(n)) {
			continue
		}
		params[k] = int(n)
	}
}

// DropVectorIndex drops a vector index and removes its durable snapshot.
func (m *MVCCDataSource) DropVectorIndex(tableName, columnName string) error {
	if err := m.indexManager.DropVectorIndex(tableName, columnName); err != nil {
		return err
	}
	if m.snapshotStore != nil {
		_ = m.snapshotStore.RemoveVectorSnapshot(tableName, columnName)
	}
	return nil
}
