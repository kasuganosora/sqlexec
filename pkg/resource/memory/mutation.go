package memory

import (
	"context"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// ==================== Data Mutation ====================

// Insert inserts data
func (m *MVCCDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !m.IsWritable() {
		return 0, domain.NewErrReadOnly(string(m.config.Type), "insert")
	}

	txnID, hasTxn := GetTransactionID(ctx)

	// Get global lock first
	m.mu.Lock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.Unlock()
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// Get table schema
	sourceData := tableVer.versions[tableVer.latest]
	schema := sourceData.schema

	// Process generated columns: distinguish between STORED and VIRTUAL types
	processedRows := make([]domain.Row, 0, len(rows))
	evaluator := generated.NewGeneratedColumnEvaluator()

	for _, row := range rows {
		// 1. Filter explicit insert values for generated columns (not allowed)
		filteredRow := generated.FilterGeneratedColumns(row, schema)

		// 2. Distinguish between STORED and VIRTUAL columns
		// STORED columns: calculate and store
		// VIRTUAL columns: don't store in table data
		var storedRow domain.Row

		// Check if table contains VIRTUAL columns
		hasVirtualCols := false
		for _, col := range schema.Columns {
			if col.IsGenerated && col.GeneratedType == "VIRTUAL" {
				hasVirtualCols = true
				break
			}
		}

		if hasVirtualCols {
			// Calculate all STORED generated columns (not including VIRTUAL)
			computedRow, err := evaluator.EvaluateAll(filteredRow, schema)
			if err != nil {
				computedRow = generated.SetGeneratedColumnsToNULL(filteredRow, schema)
			}

			// Remove VIRTUAL columns (don't store)
			storedRow = m.removeVirtualColumns(computedRow, schema)
			// Ensure only base columns and STORED generated columns are kept
			storedRow = make(map[string]interface{})
			for k, v := range computedRow {
				keep := true
				for _, col := range schema.Columns {
					if col.Name == k && col.IsGenerated && col.GeneratedType == "VIRTUAL" {
						keep = false
						break
					}
				}
				if keep {
					storedRow[k] = v
				}
			}
		} else {
			// No VIRTUAL columns, keep original logic
			computedRow, err := evaluator.EvaluateAll(filteredRow, schema)
			if err != nil {
				computedRow = generated.SetGeneratedColumnsToNULL(filteredRow, schema)
			}
			storedRow = computedRow
		}

		processedRows = append(processedRows, storedRow)
	}

	// Replace original rows with processed rows
	rows = processedRows

	if hasTxn {
		// In transaction, use COW snapshot
		snapshot, ok := m.snapshots[txnID]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTransactionNotFound(txnID)
		}

		cowSnapshot, ok := snapshot.tableSnapshots[tableName]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTableNotFound(tableName)
		}

		// Ensure data is copied (copy-on-write, row-level COW)
		if err := cowSnapshot.ensureCopied(tableVer); err != nil {
			m.mu.Unlock()
			return 0, err
		}

		m.mu.Unlock()

		// Row-level COW: don't directly copy entire table, only record newly inserted rows
		cowSnapshot.mu.Lock()

		// Get base data row count
		baseRowsCount := int64(len(cowSnapshot.baseData.rows))
		inserted := int64(0)

		for _, row := range rows {
			// Each new row uses incrementing rowID (starting from base data row count + 1)
			rowID := baseRowsCount + inserted + 1
			cowSnapshot.rowLocks[rowID] = true

			// Deep copy row data
			rowCopy := make(map[string]interface{}, len(row))
			for k, v := range row {
				rowCopy[k] = v
			}
			cowSnapshot.rowCopies[rowID] = rowCopy

			inserted++
		}

		cowSnapshot.mu.Unlock()
		return inserted, nil
	}

	// Non-transaction mode: lock order: global lock first, then table-level lock

	// Increment global version number first (while holding global lock)
	m.currentVer++

	// Get table-level lock
	tableVer.mu.Lock()

	// Now safe to release global lock since we hold table lock
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	latestData := tableVer.versions[tableVer.latest]
	if latestData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// Non-transaction insert, create new version

	// Deep copy schema
	cols := make([]domain.ColumnInfo, len(latestData.schema.Columns))
	for i := range latestData.schema.Columns {
		cols[i] = domain.ColumnInfo{
			Name:            latestData.schema.Columns[i].Name,
			Type:            latestData.schema.Columns[i].Type,
			Nullable:        latestData.schema.Columns[i].Nullable,
			Primary:         latestData.schema.Columns[i].Primary,
			Default:         latestData.schema.Columns[i].Default,
			Unique:          latestData.schema.Columns[i].Unique,
			AutoIncrement:   latestData.schema.Columns[i].AutoIncrement,
			ForeignKey:      latestData.schema.Columns[i].ForeignKey,
			IsGenerated:     latestData.schema.Columns[i].IsGenerated,
			GeneratedType:   latestData.schema.Columns[i].GeneratedType,
			GeneratedExpr:   latestData.schema.Columns[i].GeneratedExpr,
			GeneratedDepends: latestData.schema.Columns[i].GeneratedDepends,
		}
	}

	// Deep copy table attributes
	var atts map[string]interface{}
	if latestData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(latestData.schema.Atts))
		for k, v := range latestData.schema.Atts {
			atts[k] = v
		}
	}

	newRows := make([]domain.Row, len(latestData.rows)+len(rows))
	copy(newRows, latestData.rows)
	copy(newRows[len(latestData.rows):], rows)

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    latestData.schema.Name,
			Schema:  latestData.schema.Schema,
			Columns: cols,
			Atts:    atts,
		},
		rows: newRows,
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return int64(len(rows)), nil
}

// Update updates data
func (m *MVCCDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !m.IsWritable() {
		return 0, domain.NewErrReadOnly(string(m.config.Type), "update")
	}

	txnID, hasTxn := GetTransactionID(ctx)

	// Get global lock first
	m.mu.Lock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.Unlock()
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// Get table schema
	sourceData := tableVer.versions[tableVer.latest]
	schema := sourceData.schema

	// Filter generated column update values (explicit update not allowed)
	filteredUpdates := generated.FilterGeneratedColumns(updates, schema)

	// Get affected generated columns (recursive)
	updatedCols := make([]string, 0, len(filteredUpdates))
	for k := range filteredUpdates {
		updatedCols = append(updatedCols, k)
	}
	affectedGeneratedCols := generated.GetAffectedGeneratedColumns(updatedCols, schema)

	if hasTxn {
		// In transaction, use COW snapshot
		snapshot, ok := m.snapshots[txnID]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTransactionNotFound(txnID)
		}

		cowSnapshot, ok := snapshot.tableSnapshots[tableName]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTableNotFound(tableName)
		}

		// Ensure data is copied (copy-on-write, row-level COW)
		if err := cowSnapshot.ensureCopied(tableVer); err != nil {
			m.mu.Unlock()
			return 0, err
		}

		m.mu.Unlock()

		// Create evaluator
		evaluator := generated.NewGeneratedColumnEvaluator()

		// Row-level COW: traverse base data, copy and modify matching rows
		cowSnapshot.mu.Lock()
		defer cowSnapshot.mu.Unlock()

		updated := int64(0)
		for i, row := range cowSnapshot.baseData.rows {
			rowID := int64(i + 1) // Row ID starts from 1
			if util.MatchesFilters(row, filters) {
				// Row matches filter, needs modification
				if _, alreadyModified := cowSnapshot.rowLocks[rowID]; !alreadyModified {
					// First modification of this row, create deep copy
					rowCopy := make(map[string]interface{}, len(row))
					for k, v := range row {
						rowCopy[k] = v
					}
					// Apply updates
					for k, v := range filteredUpdates {
						rowCopy[k] = v
					}
					// Calculate affected generated columns
					for _, genColName := range affectedGeneratedCols {
						colInfo := getColumnInfo(genColName, schema)
						if colInfo != nil && colInfo.IsGenerated {
							val, err := evaluator.Evaluate(colInfo.GeneratedExpr, rowCopy, schema)
							if err != nil {
								val = nil // Calculation failed, set to NULL
							}
							rowCopy[genColName] = val
						}
					}
					// Store modified row
					cowSnapshot.rowCopies[rowID] = rowCopy
					cowSnapshot.rowLocks[rowID] = true
				} else {
					// Row already modified, directly update existing copy
					if existingRow, ok := cowSnapshot.rowCopies[rowID]; ok {
						for k, v := range filteredUpdates {
							existingRow[k] = v
						}
						// Calculate affected generated columns
						for _, genColName := range affectedGeneratedCols {
							colInfo := getColumnInfo(genColName, schema)
							if colInfo != nil && colInfo.IsGenerated {
								val, err := evaluator.Evaluate(colInfo.GeneratedExpr, existingRow, schema)
								if err != nil {
									val = nil // Calculation failed, set to NULL
								}
								existingRow[genColName] = val
							}
						}
					}
				}
				updated++
			}
		}
		return updated, nil
	}

	// Non-transaction mode: lock order: global lock first, then table-level lock
	tableVer.mu.Lock()
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	sourceData = tableVer.versions[tableVer.latest]
	if sourceData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// Create evaluator
	evaluator := generated.NewGeneratedColumnEvaluator()

	// Non-transaction update, create new version
	m.currentVer++
	newRows := make([]domain.Row, len(sourceData.rows))
	copy(newRows, sourceData.rows)

	updated := int64(0)
	for i, row := range newRows {
		if util.MatchesFilters(row, filters) {
			// Apply updates
			for k, v := range filteredUpdates {
				newRows[i][k] = v
			}
			// Calculate affected generated columns
			for _, genColName := range affectedGeneratedCols {
				colInfo := getColumnInfo(genColName, schema)
				if colInfo != nil && colInfo.IsGenerated {
					val, err := evaluator.Evaluate(colInfo.GeneratedExpr, newRows[i], schema)
					if err != nil {
						val = nil // Calculation failed, set to NULL
					}
					newRows[i][genColName] = val
				}
			}
			updated++
		}
	}

	// Deep copy table attributes
	var atts map[string]interface{}
	if sourceData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(sourceData.schema.Atts))
		for k, v := range sourceData.schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    sourceData.schema.Name,
			Schema:  sourceData.schema.Schema,
			Columns: sourceData.schema.Columns,
			Atts:    atts,
		},
		rows: newRows,
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return updated, nil
}

// Delete deletes data
func (m *MVCCDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !m.IsWritable() {
		return 0, domain.NewErrReadOnly(string(m.config.Type), "delete")
	}

	txnID, hasTxn := GetTransactionID(ctx)

	// Get global lock first
	m.mu.Lock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.Unlock()
		return 0, domain.NewErrTableNotFound(tableName)
	}

	var sourceData *TableData
	if hasTxn {
		// In transaction, use COW snapshot
		snapshot, ok := m.snapshots[txnID]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTransactionNotFound(txnID)
		}

		cowSnapshot, ok := snapshot.tableSnapshots[tableName]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTableNotFound(tableName)
		}

		// Ensure data is copied (copy-on-write, row-level COW)
		if err := cowSnapshot.ensureCopied(tableVer); err != nil {
			m.mu.Unlock()
			return 0, err
		}

		m.mu.Unlock()

		// Row-level COW: mark rows to delete, don't immediately modify data
		cowSnapshot.mu.Lock()
		defer cowSnapshot.mu.Unlock()

		deleted := int64(0)
		for i, row := range cowSnapshot.baseData.rows {
			rowID := int64(i + 1) // Row ID starts from 1

			// Check if row matches delete condition
			if util.MatchesFilters(row, filters) {
				// If this row was already modified, need to remove from rowCopies
				if _, alreadyModified := cowSnapshot.rowLocks[rowID]; alreadyModified {
					delete(cowSnapshot.rowCopies, rowID)
				}
				// Mark as deleted
				cowSnapshot.deletedRows[rowID] = true
				delete(cowSnapshot.rowLocks, rowID)
				deleted++
			}
		}
		return deleted, nil
	}

	// Non-transaction mode: lock order: global lock first, then table-level lock
	tableVer.mu.Lock()
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	sourceData = tableVer.versions[tableVer.latest]
	if sourceData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// Non-transaction delete, create new version
	m.currentVer++
	newRows := make([]domain.Row, 0, len(sourceData.rows))

	deleted := int64(0)
	for _, row := range sourceData.rows {
		if !util.MatchesFilters(row, filters) {
			newRows = append(newRows, row)
		} else {
			deleted++
		}
	}

	// Deep copy table attributes
	var atts map[string]interface{}
	if sourceData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(sourceData.schema.Atts))
		for k, v := range sourceData.schema.Atts {
			atts[k] = v
		}
	}

	versionData := &TableData{
		version:   m.currentVer,
		createdAt: time.Now(),
		schema: &domain.TableInfo{
			Name:    sourceData.schema.Name,
			Schema:  sourceData.schema.Schema,
			Columns: sourceData.schema.Columns,
			Atts:    atts,
		},
		rows: newRows,
	}

	tableVer.versions[m.currentVer] = versionData
	tableVer.latest = m.currentVer

	return deleted, nil
}

// Execute executes custom SQL statement
func (m *MVCCDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	// Memory data source does not support SQL execution
	return nil, domain.NewErrUnsupportedOperation(string(m.config.Type), "execute SQL")
}

// removeVirtualColumns removes VIRTUAL columns from row (not stored)
func (m *MVCCDataSource) removeVirtualColumns(row domain.Row, schema *domain.TableInfo) domain.Row {
	result := make(domain.Row)
	for k, v := range row {
		// Only keep non-VIRTUAL columns
		if !generated.IsVirtualColumn(k, schema) {
			result[k] = v
		}
	}
	return result
}

// getColumnInfo gets column information
func getColumnInfo(name string, schema *domain.TableInfo) *domain.ColumnInfo {
	for i, col := range schema.Columns {
		if col.Name == name {
			return &schema.Columns[i]
		}
	}
	return nil
}
