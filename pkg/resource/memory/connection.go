package memory

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Connection Management ====================

// Connect establishes connection to the data source
func (m *MVCCDataSource) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = true
	return nil
}

// Close closes the connection
func (m *MVCCDataSource) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Delete all temporary tables
	for tableName := range m.tempTables {
		delete(m.tables, tableName)
	}
	m.tempTables = make(map[string]bool)

	// Clear all snapshots and transactions
	m.snapshots = make(map[int64]*Snapshot)
	m.activeTxns = make(map[int64]*Transaction)
	m.connected = false
	return nil
}

// IsConnected checks if the connection is established
func (m *MVCCDataSource) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connected
}

// IsWritable checks if the data source is writable
func (m *MVCCDataSource) IsWritable() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Writable
}

// GetConfig returns the data source configuration
func (m *MVCCDataSource) GetConfig() *domain.DataSourceConfig {
	return m.config
}

// SupportsMVCC implements IsMVCCable interface
func (m *MVCCDataSource) SupportsMVCC() bool {
	return true
}
