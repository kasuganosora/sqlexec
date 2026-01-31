package mvcc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	assert.NotNil(t, mgr)
	assert.NotNil(t, mgr.config)
	assert.Equal(t, XIDBootstrap, mgr.xid)
	assert.False(t, mgr.closed)
}

func TestNewManager_WithConfig(t *testing.T) {
	cfg := &Config{
		EnableWarning:  false,
		AutoDowngrade:  false,
		GCInterval:    10 * time.Second,
		GCAgeThreshold: 30 * time.Minute,
		MaxActiveTxns:  5000,
	}

	mgr := NewManager(cfg)
	defer mgr.Close()
	assert.Equal(t, cfg.EnableWarning, mgr.config.EnableWarning)
	assert.Equal(t, cfg.AutoDowngrade, mgr.config.AutoDowngrade)
	assert.Equal(t, cfg.GCInterval, mgr.config.GCInterval)
	assert.Equal(t, cfg.GCAgeThreshold, mgr.config.GCAgeThreshold)
	assert.Equal(t, cfg.MaxActiveTxns, mgr.config.MaxActiveTxns)
}

func TestManager_Begin(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()

	features := NewDataSourceFeatures("test", CapabilityFull)
	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)
	assert.NotNil(t, txn)
	assert.Equal(t, TxnStatusInProgress, txn.Status())
	assert.True(t, txn.IsMVCC())
	assert.Equal(t, ReadCommitted, txn.Level())
}

func TestManager_Begin_MaxActiveTxns(t *testing.T) {
	cfg := &Config{MaxActiveTxns: 2}
	mgr := NewManager(cfg)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	// Create 2 transactions
	_, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)
	_, err = mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	// Third transaction should fail
	_, err = mgr.Begin(ReadCommitted, features)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too many active transactions")
}

func TestManager_Begin_NonMVCC(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()

	// Use non-MVCC data source
	features := NewDataSourceFeatures("test", CapabilityNone)
	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)
	assert.False(t, txn.IsMVCC())
	assert.Equal(t, XIDNone, txn.XID())
}

func TestManager_Commit(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	err = mgr.Commit(txn)
	assert.NoError(t, err)
	assert.Equal(t, TxnStatusCommitted, txn.Status())

	// Check transaction is removed from active list
	assert.False(t, mgr.IsTransactionActive(txn.XID()))
}

func TestManager_Commit_NonMVCC(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityNone)

	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	err = mgr.Commit(txn)
	assert.NoError(t, err)
	assert.Equal(t, TxnStatusCommitted, txn.Status())
}

func TestManager_Commit_AlreadyCommitted(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	err = mgr.Commit(txn)
	require.NoError(t, err)

	// Try to commit again
	err = mgr.Commit(txn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in progress")
}

func TestManager_Rollback(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	err = mgr.Rollback(txn)
	assert.NoError(t, err)
	assert.Equal(t, TxnStatusAborted, txn.Status())

	// Check transaction is removed from active list
	assert.False(t, mgr.IsTransactionActive(txn.XID()))
}

func TestManager_Rollback_NonMVCC(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityNone)

	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	err = mgr.Rollback(txn)
	assert.NoError(t, err)
	assert.Equal(t, TxnStatusAborted, txn.Status())
}

func TestManager_Rollback_AlreadyAborted(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	err = mgr.Rollback(txn)
	require.NoError(t, err)

	// Try to rollback again
	err = mgr.Rollback(txn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in progress")
}

func TestManager_Close(t *testing.T) {
	mgr := NewManager(nil)

	// Start a transaction
	features := NewDataSourceFeatures("test", CapabilityFull)
	_, err := mgr.Begin(ReadCommitted, features)
	require.NoError(t, err)

	err = mgr.Close()
	assert.NoError(t, err)
	assert.True(t, mgr.closed)

	// Try to begin transaction after close
	_, err = mgr.Begin(ReadCommitted, features)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manager is closed")
}

func TestManager_Close_Idempotent(t *testing.T) {
	mgr := NewManager(nil)

	err := mgr.Close()
	assert.NoError(t, err)

	// Close again should not error
	err = mgr.Close()
	assert.NoError(t, err)
}

func TestManager_GetStatistics(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	// Start some transactions (which will advance XID)
	txn1, _ := mgr.Begin(ReadCommitted, features)
	_, _ = mgr.Begin(ReadCommitted, features)

	stats := mgr.GetStatistics()
	// XID should have advanced by 2
	assert.Equal(t, XIDBootstrap+2, stats["current_xid"])
	assert.Equal(t, 2, stats["active_txns"])
	assert.Equal(t, 2, stats["cached_snapshots"])
	assert.Equal(t, false, stats["closed"])

	// Commit one transaction
	_ = mgr.Commit(txn1)

	stats = mgr.GetStatistics()
	assert.Equal(t, 1, stats["active_txns"])
	assert.Equal(t, 1, stats["cached_snapshots"])
}

func TestManager_ListActiveTransactions(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn1, _ := mgr.Begin(ReadCommitted, features)
	txn2, _ := mgr.Begin(ReadCommitted, features)
	txn3, _ := mgr.Begin(ReadCommitted, features)

	txns := mgr.ListActiveTransactions()
	assert.Len(t, txns, 3)
	assert.Contains(t, txns, txn1.XID())
	assert.Contains(t, txns, txn2.XID())
	assert.Contains(t, txns, txn3.XID())
}

func TestManager_IsTransactionActive(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, _ := mgr.Begin(ReadCommitted, features)
	assert.True(t, mgr.IsTransactionActive(txn.XID()))

	_ = mgr.Commit(txn)
	assert.False(t, mgr.IsTransactionActive(txn.XID()))
}

func TestManager_GetTransaction(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, _ := mgr.Begin(ReadCommitted, features)

	retrieved, exists := mgr.GetTransaction(txn.XID())
	assert.True(t, exists)
	assert.Equal(t, txn.XID(), retrieved.XID())
}

func TestManager_GetSnapshot(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, _ := mgr.Begin(ReadCommitted, features)

	snapshot, exists := mgr.GetSnapshot(txn.XID())
	assert.True(t, exists)
	assert.NotNil(t, snapshot)
	assert.Equal(t, txn.Snapshot().Xmin(), snapshot.Xmin())
	assert.Equal(t, txn.Snapshot().Xmax(), snapshot.Xmax())
}

func TestManager_CurrentXID(t *testing.T) {
	mgr := NewManager(nil)

	xid1 := mgr.CurrentXID()
	assert.Equal(t, XIDBootstrap, xid1)

	features := NewDataSourceFeatures("test", CapabilityFull)
	_, _ = mgr.Begin(ReadCommitted, features)

	xid2 := mgr.CurrentXID()
	assert.Greater(t, xid2, xid1)
}

func TestManager_RegisterDataSource(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test_ds", CapabilityFull)

	mgr.RegisterDataSource(features)

	retrieved, exists := mgr.GetDataSource("test_ds")
	assert.True(t, exists)
	assert.Equal(t, features.Name, retrieved.Name)
	assert.Equal(t, features.Capability, retrieved.Capability)
}

func TestManager_SetTransactionStatus(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	txn, _ := mgr.Begin(ReadCommitted, features)

	// Set status to committed
	err := mgr.SetTransactionStatus(txn.XID(), TxnStatusCommitted)
	assert.NoError(t, err)
	assert.Equal(t, TxnStatusCommitted, txn.Status())

	// Transaction should be removed from active list
	assert.False(t, mgr.IsTransactionActive(txn.XID()))
}

func TestManager_GC(t *testing.T) {
	cfg := &Config{
		GCAgeThreshold: 10 * time.Millisecond,
	}
	mgr := NewManager(cfg)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	// Create transactions
	_, _ = mgr.Begin(ReadCommitted, features)
	_, _ = mgr.Begin(ReadCommitted, features)

	// Wait for snapshots to age
	time.Sleep(20 * time.Millisecond)

	// Run GC
	mgr.GC()

	// Old snapshots should be cleaned
	stats := mgr.GetStatistics()
	assert.Equal(t, 0, stats["cached_snapshots"])
}

func TestManager_ConcurrentTransactions(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			txn, err := mgr.Begin(ReadCommitted, features)
			assert.NoError(t, err)
			_ = mgr.Commit(txn)
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// All transactions should be committed
	currentStats := mgr.GetStatistics()
	assert.Equal(t, 0, currentStats["active_txns"])
}

func TestManager_GetGlobalManager(t *testing.T) {
	mgr1 := GetGlobalManager()
	mgr2 := GetGlobalManager()

	assert.Same(t, mgr1, mgr2)
	assert.NotNil(t, mgr1)
}

func TestManager_NextXID(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	// First transaction
	txn1, _ := mgr.Begin(ReadCommitted, features)
	assert.Equal(t, XIDBootstrap+1, txn1.XID())

	// Second transaction
	txn2, _ := mgr.Begin(ReadCommitted, features)
	assert.Equal(t, XIDBootstrap+2, txn2.XID())

	// Current XID should advance
	currentXID := mgr.CurrentXID()
	assert.Equal(t, XIDBootstrap+2, currentXID)
}

func TestManager_SnapshotIsolation(t *testing.T) {
	mgr := NewManager(nil)
	defer mgr.Close()
	features := NewDataSourceFeatures("test", CapabilityFull)

	// Begin first transaction
	txn1, _ := mgr.Begin(RepeatableRead, features)
	snap1 := txn1.Snapshot()

	// Begin second transaction
	_, _ = mgr.Begin(RepeatableRead, features)

	// Transaction 1's snapshot should not see transaction 2 as active
	assert.Len(t, snap1.Xip(), 0)
}
