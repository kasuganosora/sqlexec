package memory

import (
	"context"
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

	// Create COW snapshot structure without copying data
	tableSnapshots := make(map[string]*COWTableSnapshot)
	for tableName := range m.tables {
		// Only create snapshot structure, reference base data
		tableSnapshots[tableName] = &COWTableSnapshot{
			tableName:    tableName,
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
	for tableName, cowSnapshot := range snapshot.tableSnapshots {
		tableVer := m.tables[tableName]
		if tableVer != nil && cowSnapshot.copied {
			cowSnapshot.mu.Lock()

			// Check if there are row-level modifications
			if len(cowSnapshot.rowCopies) == 0 && len(cowSnapshot.deletedRows) == 0 {
				// No rows modified, no need to create new version
				cowSnapshot.mu.Unlock()
				continue
			}

			// Row-level COW: merge base data and modified rows
			tableVer.mu.Lock()
			m.currentVer++

			// Merge base data and row-level modifications
			newRows := make([]domain.Row, 0, len(cowSnapshot.baseData.rows))
			for i, row := range cowSnapshot.baseData.rows {
				rowID := int64(i + 1)

				// Check if this row is deleted
				if _, deleted := cowSnapshot.deletedRows[rowID]; deleted {
					continue // Skip deleted rows
				}

				// Check if this row is modified
				if modifiedRow, ok := cowSnapshot.rowCopies[rowID]; ok {
					// Use modified row
					newRows = append(newRows, modifiedRow)
				} else {
					// Use original row (deep copy)
					rowCopy := make(map[string]interface{}, len(row))
					for k, v := range row {
						rowCopy[k] = v
					}
					newRows = append(newRows, rowCopy)
				}
			}

			// Handle newly inserted rows (rowID exceeds base data row count)
			baseRowsCount := len(cowSnapshot.baseData.rows)
			for rowID, row := range cowSnapshot.rowCopies {
				if rowID > int64(baseRowsCount) {
					// This is a newly inserted row
					newRows = append(newRows, row)
				}
			}

			// Create new version
			cols := make([]domain.ColumnInfo, len(cowSnapshot.modifiedData.schema.Columns))
			copy(cols, cowSnapshot.modifiedData.schema.Columns)

			// Deep copy table attributes
			var atts map[string]interface{}
			if cowSnapshot.modifiedData.schema.Atts != nil {
				atts = make(map[string]interface{}, len(cowSnapshot.modifiedData.schema.Atts))
				for k, v := range cowSnapshot.modifiedData.schema.Atts {
					atts[k] = v
				}
			}

			newVersionData := &TableData{
				version:   m.currentVer,
				createdAt: time.Now(),
				schema: &domain.TableInfo{
					Name:    cowSnapshot.modifiedData.schema.Name,
					Schema:  cowSnapshot.modifiedData.schema.Schema,
					Columns: cols,
					Atts:    atts,
				},
				rows: newRows,
			}

			tableVer.versions[m.currentVer] = newVersionData
			tableVer.latest = m.currentVer
			tableVer.mu.Unlock()

			cowSnapshot.mu.Unlock()
		}
	}

	delete(m.activeTxns, txnID)
	delete(m.snapshots, txnID)

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

	return nil
}

// ==================== COW Snapshot Methods ====================

// ensureCopied ensures table data is copied to transaction snapshot (copy-on-write)
// Uses row-level COW: only creates structure, does not immediately copy all rows
func (s *COWTableSnapshot) ensureCopied(tableVer *TableVersions) error {
	if s.copied {
		return nil // Already created copy
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double check to avoid duplicate creation
	if s.copied {
		return nil
	}

	// Get main version data
	tableVer.mu.RLock()
	baseData := tableVer.versions[tableVer.latest]
	tableVer.mu.RUnlock()

	if baseData == nil {
		return domain.NewErrTableNotFound(s.tableName)
	}

	// Copy schema
	cols := make([]domain.ColumnInfo, len(baseData.schema.Columns))
	copy(cols, baseData.schema.Columns)

	// Deep copy table attributes
	var atts map[string]interface{}
	if baseData.schema.Atts != nil {
		atts = make(map[string]interface{}, len(baseData.schema.Atts))
		for k, v := range baseData.schema.Atts {
			atts[k] = v
		}
	}

	// Create modified data structure without immediately copying all rows
	// Uses row-level COW: only creates structure, rows are copied on demand
	s.modifiedData = &TableData{
		version:   baseData.version,
		createdAt: baseData.createdAt,
		schema: &domain.TableInfo{
			Name:    baseData.schema.Name,
			Schema:  baseData.schema.Schema,
			Columns: cols,
			Atts:    atts,
		},
		rows: nil, // Row data is lazy loaded and copied
	}

	// Initialize row-level tracking structures
	s.rowLocks = make(map[int64]bool)       // Track modified rows
	s.rowCopies = make(map[int64]domain.Row) // Store modified rows
	s.deletedRows = make(map[int64]bool)    // Mark deleted rows

	s.baseData = baseData
	s.copied = true

	return nil
}

// getTableData retrieves table data from COW snapshot (row-level COW)
func (s *COWTableSnapshot) getTableData(tableVer *TableVersions) *TableData {
	if !s.copied {
		// No copy created, read main version directly
		tableVer.mu.RLock()
		data := tableVer.versions[tableVer.latest]
		tableVer.mu.RUnlock()
		return data
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Copy created, need to merge base data and row-level modifications
	if len(s.rowCopies) == 0 {
		// No rows modified, return base data
		return s.baseData
	}

	// Merge base data and modified rows
	mergedRows := make([]domain.Row, 0, len(s.baseData.rows))
	for i, row := range s.baseData.rows {
		rowID := int64(i + 1) // Row ID starts from 1
		if modifiedRow, ok := s.rowCopies[rowID]; ok {
			// Use modified row
			mergedRows = append(mergedRows, modifiedRow)
		} else {
			// Use original row
			mergedRows = append(mergedRows, row)
		}
	}

	return &TableData{
		version:   s.modifiedData.version,
		createdAt: s.modifiedData.createdAt,
		schema:    s.modifiedData.schema,
		rows:      mergedRows,
	}
}
