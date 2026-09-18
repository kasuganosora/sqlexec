package memory

import (
	"context"
	"fmt"
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

	var snapshot *Snapshot
	var cowSnapshot *COWTableSnapshot
	if hasTxn {
		var ok bool
		snapshot, ok = m.snapshots[txnID]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTransactionNotFound(txnID)
		}
		cowSnapshot, ok = snapshot.tableSnapshots[tableName]
		if !ok {
			m.mu.Unlock()
			return 0, domain.NewErrTableNotFound(tableName)
		}
		if err := cowSnapshot.ensureCopied(tableVer); err != nil {
			m.mu.Unlock()
			return 0, err
		}
	}

	for _, row := range rows {
		convertRowTypesBasedOnSchema(row, schema)
	}

	// Transaction inserts reserve AUTO_INCREMENT values on the snapshot.
	// Global counters move only on a successful commit (or immediately for
	// non-transaction inserts).
	var autoIncPrev map[string]int64
	if !hasTxn {
		autoIncPrev = m.snapshotAutoInc(autoIncKeysForSchema(tableName, schema))
	}
	m.assignAutoInc(tableName, schema, rows, snapshot)

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

			// Remove VIRTUAL columns (don't store, only keep base + STORED generated)
			storedRow = m.removeVirtualColumns(computedRow, schema)
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

	// Non-transaction mode: lock order: global lock first, then table-level lock.
	// Validate unique constraints before bumping currentVer / publishing so a
	// rejected insert does not leak AUTO_INCREMENT values or version numbers.
	tableVer.mu.Lock()
	latestData := tableVer.versions[tableVer.latest]
	if latestData == nil {
		tableVer.mu.Unlock()
		m.restoreAutoInc(autoIncPrev)
		m.mu.Unlock()
		return 0, domain.NewErrTableNotFound(tableName)
	}
	existingRows := latestData.Rows()
	if err := m.checkUniqueConstraints(tableName, schema, existingRows, rows); err != nil {
		tableVer.mu.Unlock()
		m.restoreAutoInc(autoIncPrev)
		m.mu.Unlock()
		return 0, err
	}

	m.currentVer++
	newVer := m.currentVer
	m.mu.Unlock()
	defer tableVer.mu.Unlock()

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

	// Maintain indexes: rebuild from the new version's rows
	m.rebuildTableIndexes(tableName, versionData.schema, newRows)

	return int64(len(rows)), nil
}

// BulkLoad creates a new version for the table and populates it incrementally.
// The loadFn receives an addPage callback; each call to addPage creates one RowPage
// and registers it with the buffer pool. This avoids holding all rows in memory at once,
// allowing the buffer pool to evict cold pages during loading.
// The caller transfers ownership of each rows slice passed to addPage.
func (m *MVCCDataSource) BulkLoad(tableName string, loadFn func(addPage func(rows []domain.Row)) error) error {
	m.mu.Lock()

	tableVer, ok := m.tables[tableName]
	if !ok {
		m.mu.Unlock()
		return domain.NewErrTableNotFound(tableName)
	}

	m.currentVer++
	newVer := m.currentVer

	tableVer.mu.Lock()
	m.mu.Unlock()

	latestData := tableVer.versions[tableVer.latest]
	if latestData == nil {
		tableVer.mu.Unlock()
		return domain.NewErrTableNotFound(tableName)
	}

	pr := NewPagedRowsBuilder(m.bufferPool, 0, tableName, newVer)

	if err := loadFn(pr.AppendPage); err != nil {
		pr.Release()
		tableVer.mu.Unlock()
		return err
	}

	// Ensure at least one empty page for consistency with NewPagedRows behavior
	if pr.Len() == 0 {
		pr.AppendPage([]domain.Row{})
	}

	versionData := &TableData{
		version:   newVer,
		createdAt: time.Now(),
		schema:    deepCopySchema(latestData.schema),
		rows:      pr,
	}
	tableVer.versions[newVer] = versionData
	tableVer.latest = newVer

	if m.bufferPool != nil {
		m.bufferPool.UpdateLatestVersion(tableName, newVer)
	}

	loadedRows := versionData.Rows()
	m.rebuildTableIndexes(tableName, versionData.schema, loadedRows)
	autoIncMax := maxAutoIncFromRows(tableName, versionData.schema, loadedRows)
	tableVer.mu.Unlock()

	if len(autoIncMax) > 0 {
		m.mu.Lock()
		m.raiseAutoInc(autoIncMax)
		m.mu.Unlock()
	}
	return nil
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

		recalcGenerated := func(row domain.Row) {
			for _, genColName := range affectedGeneratedCols {
				colInfo := getColumnInfo(genColName, schema)
				if colInfo != nil && colInfo.IsGenerated {
					val, err := evaluator.Evaluate(colInfo.GeneratedExpr, row, schema)
					if err != nil {
						val = nil
					}
					row[genColName] = val
				}
			}
		}

		// Match the transaction-visible row (prior in-txn updates), not the
		// frozen base snapshot. Otherwise UPDATE/DELETE see stale values.
		applyUpdates := func(rowID int64, visible domain.Row) {
			if cowSnapshot.deletedRows[rowID] {
				return
			}
			if !util.MatchesFilters(visible, filters) {
				return
			}

			if existingRow, ok := cowSnapshot.rowCopies[rowID]; ok {
				for k, v := range filteredUpdates {
					existingRow[k] = v
				}
				recalcGenerated(existingRow)
			} else {
				rowCopy := deepCopyRow(visible)
				for k, v := range filteredUpdates {
					rowCopy[k] = v
				}
				recalcGenerated(rowCopy)
				cowSnapshot.rowCopies[rowID] = rowCopy
				cowSnapshot.rowLocks[rowID] = true
			}
			updated++
		}

		for i, row := range cowSnapshot.baseData.Rows() {
			rowID := int64(i + 1)
			visible := row
			if copy, ok := cowSnapshot.rowCopies[rowID]; ok {
				visible = copy
			}
			applyUpdates(rowID, visible)
		}

		for rowID := baseRowsCount + 1; rowID <= baseRowsCount+cowSnapshot.insertedCount; rowID++ {
			if row, ok := cowSnapshot.rowCopies[rowID]; ok {
				applyUpdates(rowID, row)
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

	// Maintain indexes: rebuild from the new version's rows
	m.rebuildTableIndexes(tableName, versionData.schema, newRows)

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

		// Check base data rows against the transaction-visible value
		for i, row := range cowSnapshot.baseData.Rows() {
			rowID := int64(i + 1)

			if cowSnapshot.deletedRows[rowID] {
				continue
			}

			visible := row
			if copy, ok := cowSnapshot.rowCopies[rowID]; ok {
				visible = copy
			}
			if !util.MatchesFilters(visible, filters) {
				continue
			}

			delete(cowSnapshot.rowCopies, rowID)
			cowSnapshot.deletedRows[rowID] = true
			delete(cowSnapshot.rowLocks, rowID)
			deleted++
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
	// Deep copy retained rows to maintain MVCC isolation between versions
	delSrcRows := latestData.Rows()
	newRows := make([]domain.Row, 0, len(delSrcRows))

	deleted := int64(0)
	for _, row := range delSrcRows {
		if !util.MatchesFilters(row, filters) {
			newRows = append(newRows, deepCopyRow(row))
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

	// Maintain indexes: rebuild from the new version's rows
	m.rebuildTableIndexes(tableName, versionData.schema, newRows)

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

// rebuildTableIndexes rebuilds all indexes for a table from the given rows.
// This is called after each non-transaction mutation to keep indexes in sync.
func (m *MVCCDataSource) rebuildTableIndexes(tableName string, schema *domain.TableInfo, rows []domain.Row) {
	_ = m.indexManager.RebuildIndex(tableName, schema, rows)
	m.markVectorDirty(tableName)
}

// checkUniqueConstraints verifies that no row in newRows would violate a unique
// or primary-key constraint when combined with existingRows.
// Checks both column-level Unique/Primary flags and unique indexes in the
// index manager (covers gorm:"uniqueIndex" which doesn't set col.Unique).
// Returns a "Duplicate entry" error compatible with isUniqueViolation detectors.
func (m *MVCCDataSource) checkUniqueConstraints(tableName string, schema *domain.TableInfo, existingRows []domain.Row, newRows []domain.Row) error {
	// 1. Column-level unique/primary constraints
	for _, col := range schema.Columns {
		if !col.Unique && !col.Primary {
			continue
		}
		seen := make(map[string]bool, len(existingRows))
		for _, row := range existingRows {
			val, ok := row[col.Name]
			if !ok || val == nil {
				continue
			}
			seen[fmt.Sprintf("%v", val)] = true
		}
		for _, row := range newRows {
			val, ok := row[col.Name]
			if !ok || val == nil {
				continue
			}
			key := fmt.Sprintf("%v", val)
			if seen[key] {
				return fmt.Errorf("Duplicate entry '%v' for key '%s'", val, col.Name)
			}
			seen[key] = true
		}
	}

	// 2. Unique indexes registered in the index manager (e.g. CREATE UNIQUE INDEX).
	// The index holds the state of the previous version (existingRows), so Find()
	// tells us whether the value already exists in the table.
	tableIndexes, err := m.indexManager.GetTableIndexes(tableName)
	if err != nil || len(tableIndexes) == 0 {
		return nil
	}
	for _, idxInfo := range tableIndexes {
		if !idxInfo.Unique {
			continue
		}
		if len(idxInfo.Columns) == 1 {
			// Single-column unique index: use index lookup for efficiency
			colName := idxInfo.Columns[0]
			idx, err := m.indexManager.GetIndex(tableName, colName)
			if err != nil {
				continue
			}
			newSeen := make(map[string]bool, len(newRows))
			for _, row := range newRows {
				val, ok := row[colName]
				if !ok || val == nil {
					continue
				}
				if _, exists := idx.Find(val); exists {
					return fmt.Errorf("Duplicate entry '%v' for key '%s'", val, colName)
				}
				key := fmt.Sprintf("%v", val)
				if newSeen[key] {
					return fmt.Errorf("Duplicate entry '%v' for key '%s'", val, colName)
				}
				newSeen[key] = true
			}
		} else {
			// Composite unique index: check all columns together
			existingSeen := make(map[string]bool, len(existingRows))
			for _, row := range existingRows {
				keyParts := make([]string, 0, len(idxInfo.Columns))
				allPresent := true
				for _, colName := range idxInfo.Columns {
					val, ok := row[colName]
					if !ok || val == nil {
						allPresent = false
						break
					}
					keyParts = append(keyParts, fmt.Sprintf("%v", val))
				}
				if allPresent {
					existingSeen[fmt.Sprintf("%v", keyParts)] = true
				}
			}
			for _, row := range newRows {
				keyParts := make([]string, 0, len(idxInfo.Columns))
				allPresent := true
				for _, colName := range idxInfo.Columns {
					val, ok := row[colName]
					if !ok || val == nil {
						allPresent = false
						break
					}
					keyParts = append(keyParts, fmt.Sprintf("%v", val))
				}
				if !allPresent {
					continue
				}
				compositeKey := fmt.Sprintf("%v", keyParts)
				if existingSeen[compositeKey] {
					return fmt.Errorf("Duplicate entry for key '%s'", idxInfo.Name)
				}
				existingSeen[compositeKey] = true
			}
		}
	}
	return nil
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
