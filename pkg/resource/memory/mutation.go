package memory

import (
	"context"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
	"github.com/kasuganosora/sqlexec/pkg/utils"
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

	// Get table schema (need table lock to safely read versions map)
	tableVer.mu.RLock()
	sourceData := tableVer.versions[tableVer.latest]
	schema := deepCopySchema(sourceData.schema) // Deep copy so we can release the lock
	tableVer.mu.RUnlock()

	// Process auto-increment columns and fill in generated IDs
	// Note: lastInsertID is tracked but not returned via the interface (interface only returns rowsAffected)
	// The auto-incremented ID is set in the row map, so callers can read it from there
	for _, row := range rows {
		// Convert types based on schema (e.g., int64(0/1) to bool for BOOL columns)
		convertRowTypesBasedOnSchema(row, schema)

		// Handle auto-increment columns
		for _, col := range schema.Columns {
			if col.AutoIncrement {
				key := tableName + "." + col.Name
				// Check if the value is missing or is 0/null
				if val, exists := row[col.Name]; !exists || val == nil || val == int64(0) || val == float64(0) {
					// Generate next auto-increment ID
					m.autoIncCounters[key]++
					nextID := m.autoIncCounters[key]
					row[col.Name] = nextID
				} else {
					// Value was provided, update counter if needed
					if intVal, ok := val.(int64); ok && intVal > m.autoIncCounters[key] {
						m.autoIncCounters[key] = intVal
					} else if floatVal, ok := val.(float64); ok && int64(floatVal) > m.autoIncCounters[key] {
						m.autoIncCounters[key] = int64(floatVal)
					}
				}
			}
		}
	}

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

		// Get base data row count and previously inserted count
		baseRowsCount := int64(cowSnapshot.baseData.RowCount())
		inserted := int64(0)

		for _, row := range rows {
			// Each new row uses incrementing rowID, accounting for previously inserted rows
			cowSnapshot.insertedCount++
			rowID := baseRowsCount + cowSnapshot.insertedCount
			cowSnapshot.rowLocks[rowID] = true
			cowSnapshot.rowCopies[rowID] = deepCopyRow(row)
			inserted++
		}

		cowSnapshot.mu.Unlock()
		return inserted, nil
	}

	// Non-transaction mode: lock order: global lock first, then table-level lock

	// Increment global version number first (while holding global lock)
	m.currentVer++
	newVer := m.currentVer // Capture before releasing global lock

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
	existingRows := latestData.Rows()
	newRows := make([]domain.Row, len(existingRows), len(existingRows)+len(rows))
	copy(newRows, existingRows)
	// Deep copy inserted rows to prevent external mutation
	for _, row := range rows {
		newRows = append(newRows, deepCopyRow(row))
	}

	versionData := &TableData{
		version:   newVer,
		createdAt: time.Now(),
		schema:    deepCopySchema(latestData.schema),
		rows:      NewPagedRows(m.bufferPool, newRows, 0, tableName, newVer),
	}

	tableVer.versions[newVer] = versionData
	tableVer.latest = newVer

	return int64(len(rows)), nil
}

// convertRowTypesBasedOnSchema converts row values based on column types defined in schema
func convertRowTypesBasedOnSchema(row domain.Row, schema *domain.TableInfo) {
	utils.ConvertBoolColumnsBasedOnSchema(row, schema)
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

	// Get table schema (need table lock to safely read versions map)
	tableVer.mu.RLock()
	schema := deepCopySchema(tableVer.versions[tableVer.latest].schema)
	tableVer.mu.RUnlock()

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

		// Row-level COW: traverse base data and inserted rows, copy and modify matching rows
		cowSnapshot.mu.Lock()
		defer cowSnapshot.mu.Unlock()

		updated := int64(0)
		baseRowsCount := int64(cowSnapshot.baseData.RowCount())

		// Helper to apply updates to a row
		applyUpdates := func(rowID int64, row domain.Row, isBase bool) {
			if !util.MatchesFilters(row, filters) {
				return
			}
			// Skip deleted rows
			if cowSnapshot.deletedRows[rowID] {
				return
			}

			if _, alreadyModified := cowSnapshot.rowLocks[rowID]; !alreadyModified {
				// First modification of this row, create deep copy (only for base rows)
				var rowCopy domain.Row
				if isBase {
					rowCopy = make(map[string]interface{}, len(row))
					for k, v := range row {
						rowCopy[k] = v
					}
				} else {
					// Already a copy in rowCopies, use it directly
					rowCopy = row
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
							val = nil
						}
						rowCopy[genColName] = val
					}
				}
				cowSnapshot.rowCopies[rowID] = rowCopy
				cowSnapshot.rowLocks[rowID] = true
			} else {
				// Row already modified, directly update existing copy
				if existingRow, ok := cowSnapshot.rowCopies[rowID]; ok {
					for k, v := range filteredUpdates {
						existingRow[k] = v
					}
					for _, genColName := range affectedGeneratedCols {
						colInfo := getColumnInfo(genColName, schema)
						if colInfo != nil && colInfo.IsGenerated {
							val, err := evaluator.Evaluate(colInfo.GeneratedExpr, existingRow, schema)
							if err != nil {
								val = nil
							}
							existingRow[genColName] = val
						}
					}
				}
			}
			updated++
		}

		// Check base data rows
		for i, row := range cowSnapshot.baseData.Rows() {
			rowID := int64(i + 1)
			applyUpdates(rowID, row, true)
		}

		// Check newly inserted rows in this transaction
		for rowID := baseRowsCount + 1; rowID <= baseRowsCount+cowSnapshot.insertedCount; rowID++ {
			if row, ok := cowSnapshot.rowCopies[rowID]; ok {
				applyUpdates(rowID, row, false)
			}
		}

		return updated, nil
	}

	// Non-transaction mode: increment version while holding global lock to avoid race
	m.currentVer++
	newVer := m.currentVer
	tableVer.mu.Lock()
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	latestData := tableVer.versions[tableVer.latest]
	if latestData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// Create evaluator
	evaluator := generated.NewGeneratedColumnEvaluator()

	// Non-transaction update, create new version
	// Deep copy rows to avoid mutating the previous version's data (MVCC isolation)
	srcRows := latestData.Rows()
	newRows := make([]domain.Row, len(srcRows))
	for i, row := range srcRows {
		newRows[i] = deepCopyRow(row)
	}

	updated := int64(0)
	for i, row := range newRows {
		if util.MatchesFilters(row, filters) {
			// Apply updates
			for k, v := range filteredUpdates {
				row[k] = v
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

	versionData := &TableData{
		version:   newVer,
		createdAt: time.Now(),
		schema:    deepCopySchema(latestData.schema),
		rows:      NewPagedRows(m.bufferPool, newRows, 0, tableName, newVer),
	}

	tableVer.versions[newVer] = versionData
	tableVer.latest = newVer

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
		baseRowsCount := int64(cowSnapshot.baseData.RowCount())

		// Check base data rows
		for i, row := range cowSnapshot.baseData.Rows() {
			rowID := int64(i + 1)

			// Skip already deleted rows
			if cowSnapshot.deletedRows[rowID] {
				continue
			}

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

		// Check newly inserted rows in this transaction
		for rowID := baseRowsCount + 1; rowID <= baseRowsCount+cowSnapshot.insertedCount; rowID++ {
			// Skip already deleted rows
			if cowSnapshot.deletedRows[rowID] {
				continue
			}
			if row, ok := cowSnapshot.rowCopies[rowID]; ok {
				if util.MatchesFilters(row, filters) {
					delete(cowSnapshot.rowCopies, rowID)
					cowSnapshot.deletedRows[rowID] = true
					delete(cowSnapshot.rowLocks, rowID)
					deleted++
				}
			}
		}

		return deleted, nil
	}

	// Non-transaction mode: increment version while holding global lock to avoid race
	m.currentVer++
	newVer := m.currentVer
	tableVer.mu.Lock()
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

	latestData := tableVer.versions[tableVer.latest]
	if latestData == nil {
		return 0, domain.NewErrTableNotFound(tableName)
	}

	// Non-transaction delete, create new version
	delSrcRows := latestData.Rows()
	newRows := make([]domain.Row, 0, len(delSrcRows))

	deleted := int64(0)
	for _, row := range delSrcRows {
		if !util.MatchesFilters(row, filters) {
			newRows = append(newRows, row)
		} else {
			deleted++
		}
	}

	versionData := &TableData{
		version:   newVer,
		createdAt: time.Now(),
		schema:    deepCopySchema(latestData.schema),
		rows:      NewPagedRows(m.bufferPool, newRows, 0, tableName, newVer),
	}

	tableVer.versions[newVer] = versionData
	tableVer.latest = newVer

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
