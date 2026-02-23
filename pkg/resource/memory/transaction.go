package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Transaction Management ====================

// BeginTransaction implements TransactionalDataSource interface
func (m *MVCCDataSource) BeginTransaction(ctx context.Context, options *domain.TransactionOptions) (domain.Transaction, error) {
	readOnly := false
	if options != nil {
		readOnly = options.ReadOnly
	}

	txnID, err := m.BeginTx(ctx, readOnly)
	if err != nil {
		return nil, err
	}

	return &MVCCTransaction{
		ds:    m,
		txnID: txnID,
	}, nil
}

// BeginTx starts a new transaction (copy-on-write)
func (m *MVCCDataSource) BeginTx(ctx context.Context, readOnly bool) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := m.nextTxID
	m.nextTxID++

	// Create COW snapshot structure without copying data.
	// Pin each table's current latest version for snapshot isolation.
	tableSnapshots := make(map[string]*COWTableSnapshot)
	for tableName, tableVer := range m.tables {
		tableVer.mu.RLock()
		pinnedVer := tableVer.latest
		tableVer.mu.RUnlock()

		tableSnapshots[tableName] = &COWTableSnapshot{
			tableName:    tableName,
			snapshotVer:  pinnedVer,
			copied:       false,
			baseData:     nil, // Lazy load on access
			modifiedData: nil,
		}
	}

	snapshot := &Snapshot{
		txnID:          txnID,
		startVer:       m.currentVer,
		createdAt:      time.Now(),
		tableSnapshots: tableSnapshots,
	}

	txn := &Transaction{
		txnID:     txnID,
		startTime: time.Now(),
		readOnly:  readOnly,
	}

	m.snapshots[txnID] = snapshot
	m.activeTxns[txnID] = txn

	return txnID, nil
}

// CommitTx commits a transaction (COW optimization with row-level COW)
func (m *MVCCDataSource) CommitTx(ctx context.Context, txnID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn, ok := m.activeTxns[txnID]
	if !ok {
		return domain.NewErrTransactionNotFound(txnID)
	}

	snapshot, ok := m.snapshots[txnID]
	if !ok {
		return domain.NewErrSnapshotNotFound(txnID)
	}

	if txn.readOnly {
		// Read-only transaction ends directly
		delete(m.activeTxns, txnID)
		delete(m.snapshots, txnID)
		return nil
	}

	// Write transaction: commit only modified tables
	var commitErr error
	for tableName, cowSnapshot := range snapshot.tableSnapshots {
		tableVer := m.tables[tableName]
		if tableVer != nil && cowSnapshot.copied {
			// Use closure to ensure locks are released via defer even on panic
			func() {
				cowSnapshot.mu.Lock()
				defer cowSnapshot.mu.Unlock()

				// Check if there are row-level modifications
				if len(cowSnapshot.rowCopies) == 0 && len(cowSnapshot.deletedRows) == 0 {
					// No rows modified, no need to create new version
					return
				}

				// Row-level COW: merge base data and modified rows
				tableVer.mu.Lock()
				defer tableVer.mu.Unlock()
				m.currentVer++

				// Merge base data and row-level modifications
				baseRows := cowSnapshot.baseData.Rows()
				newRows := make([]domain.Row, 0, len(baseRows))
				for i, row := range baseRows {
					rowID := int64(i + 1)

					// Skip deleted rows
					if cowSnapshot.deletedRows[rowID] {
						continue
					}

					// Use modified row or deep copy original
					if modifiedRow, ok := cowSnapshot.rowCopies[rowID]; ok {
						newRows = append(newRows, modifiedRow)
					} else {
						newRows = append(newRows, deepCopyRow(row))
					}
				}

				// Append newly inserted rows in order (rowID > base data row count)
				baseRowsCount := int64(cowSnapshot.baseData.RowCount())
				for rowID := baseRowsCount + 1; rowID <= baseRowsCount+cowSnapshot.insertedCount; rowID++ {
					if cowSnapshot.deletedRows[rowID] {
						continue
					}
					if row, ok := cowSnapshot.rowCopies[rowID]; ok {
						newRows = append(newRows, row)
					}
				}

				// Check unique constraints on the final merged rows before committing
				schema := cowSnapshot.modifiedData.schema
				if uerr := m.checkUniqueConstraintsFinal(tableName, schema, newRows); uerr != nil {
					m.currentVer--
					commitErr = uerr
					return
				}

				// Create new version
				newVersionData := &TableData{
					version:   m.currentVer,
					createdAt: time.Now(),
					schema:    deepCopySchema(schema),
					rows:      NewPagedRows(m.bufferPool, newRows, 0, tableName, m.currentVer),
				}

				tableVer.versions[m.currentVer] = newVersionData
				tableVer.latest = m.currentVer

				// Maintain indexes: rebuild from the committed version's rows
				_ = m.indexManager.RebuildIndex(tableName, newVersionData.schema, newRows)
			}()
			if commitErr != nil {
				// Unique constraint violation; clean up and return error
				delete(m.activeTxns, txnID)
				delete(m.snapshots, txnID)
				return commitErr
			}
		}
	}

	delete(m.activeTxns, txnID)
	delete(m.snapshots, txnID)

	// Garbage collect old versions no longer needed by any transaction
	m.gcOldVersions()

	return nil
}

// RollbackTx rolls back a transaction
func (m *MVCCDataSource) RollbackTx(ctx context.Context, txnID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.activeTxns[txnID]; !ok {
		return domain.NewErrTransactionNotFound(txnID)
	}

	// With copy-on-write, rollback just deletes the snapshot, no data to release
	delete(m.activeTxns, txnID)
	delete(m.snapshots, txnID)

	// Garbage collect old versions no longer needed by any transaction
	m.gcOldVersions()

	return nil
}

// checkUniqueConstraintsFinal validates that the complete set of rows
// (after merging base + modified + inserted - deleted) has no duplicate
// values for any unique or primary-key column.
func (m *MVCCDataSource) checkUniqueConstraintsFinal(tableName string, schema *domain.TableInfo, rows []domain.Row) error {
	if schema == nil {
		return nil
	}
	// 1. Column-level unique/primary constraints
	for _, col := range schema.Columns {
		if !col.Unique && !col.Primary {
			continue
		}
		seen := make(map[string]bool, len(rows))
		for _, row := range rows {
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

	// 2. Unique indexes (covers gorm:"uniqueIndex")
	tableIndexes, err := m.indexManager.GetTableIndexes(tableName)
	if err != nil || len(tableIndexes) == 0 {
		return nil
	}
	for _, idxInfo := range tableIndexes {
		if !idxInfo.Unique {
			continue
		}
		seen := make(map[string]bool, len(rows))
		for _, row := range rows {
			// Build composite key from all index columns
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
			if seen[compositeKey] {
				return fmt.Errorf("Duplicate entry for key '%s'", idxInfo.Name)
			}
			seen[compositeKey] = true
		}
	}
	return nil
}

// ==================== COW Snapshot Methods ====================

// ensureCopied ensures table data is copied to transaction snapshot (copy-on-write)
// Uses row-level COW: only creates structure, does not immediately copy all rows
func (s *COWTableSnapshot) ensureCopied(tableVer *TableVersions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check under lock to avoid data race
	if s.copied {
		return nil
	}

	// Get the pinned snapshot version data (not the current latest)
	tableVer.mu.RLock()
	baseData := tableVer.versions[s.snapshotVer]
	tableVer.mu.RUnlock()

	if baseData == nil {
		return domain.NewErrTableNotFound(s.tableName)
	}

	// Create modified data structure without immediately copying all rows
	// Uses row-level COW: only creates structure, rows are copied on demand
	s.modifiedData = &TableData{
		version:   baseData.version,
		createdAt: baseData.createdAt,
		schema:    deepCopySchema(baseData.schema),
		rows:      nil, // Row data is lazy loaded and copied
	}

	// Initialize row-level tracking structures
	s.rowLocks = make(map[int64]bool)        // Track modified rows
	s.rowCopies = make(map[int64]domain.Row) // Store modified rows
	s.deletedRows = make(map[int64]bool)     // Mark deleted rows

	s.baseData = baseData
	s.copied = true

	return nil
}

// getTableData retrieves table data from COW snapshot (row-level COW)
func (s *COWTableSnapshot) getTableData(tableVer *TableVersions) *TableData {
	s.mu.RLock()
	copied := s.copied
	s.mu.RUnlock()

	if !copied {
		// No copy created — read the pinned snapshot version (not the current latest)
		// to guarantee snapshot isolation.
		tableVer.mu.RLock()
		data := tableVer.versions[s.snapshotVer]
		tableVer.mu.RUnlock()
		return data
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Copy created, need to merge base data and row-level modifications
	if len(s.rowCopies) == 0 && len(s.deletedRows) == 0 {
		// No rows modified or deleted, return base data
		return s.baseData
	}

	// Merge base data and modified rows, skipping deleted rows
	baseRowsCount := int64(s.baseData.RowCount())
	srcRows := s.baseData.Rows()
	mergedRows := make([]domain.Row, 0, len(srcRows))
	for i, row := range srcRows {
		rowID := int64(i + 1) // Row ID starts from 1

		// Skip deleted rows
		if s.deletedRows[rowID] {
			continue
		}

		if modifiedRow, ok := s.rowCopies[rowID]; ok {
			// Use modified row
			mergedRows = append(mergedRows, modifiedRow)
		} else {
			// Use original row
			mergedRows = append(mergedRows, row)
		}
	}

	// Append newly inserted rows (rowID > baseRowsCount)
	for rowID := baseRowsCount + 1; rowID <= baseRowsCount+s.insertedCount; rowID++ {
		if row, ok := s.rowCopies[rowID]; ok {
			// Skip if it was subsequently deleted
			if !s.deletedRows[rowID] {
				mergedRows = append(mergedRows, row)
			}
		}
	}

	return &TableData{
		version:   s.modifiedData.version,
		createdAt: s.modifiedData.createdAt,
		schema:    s.modifiedData.schema,
		rows:      NewPagedRows(nil, mergedRows, 0, s.tableName, s.modifiedData.version),
	}
}
