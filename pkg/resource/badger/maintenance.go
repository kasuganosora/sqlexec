package badger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// MaintenanceManager handles database maintenance operations
type MaintenanceManager struct {
	ds      *BadgerDataSource
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
}

// NewMaintenanceManager creates a new MaintenanceManager
func NewMaintenanceManager(ds *BadgerDataSource) *MaintenanceManager {
	return &MaintenanceManager{
		ds:     ds,
		stopCh: make(chan struct{}),
	}
}

// MaintenanceConfig configuration for maintenance operations
type MaintenanceConfig struct {
	// EnableAutoGC whether to enable automatic garbage collection
	EnableAutoGC bool `json:"enable_auto_gc"`

	// GCInterval interval between GC runs (in seconds)
	GCInterval int `json:"gc_interval"`

	// GCDiscardRatio ratio of data to discard in each GC run (0.0-1.0)
	GCDiscardRatio float64 `json:"gc_discard_ratio"`

	// EnableAutoCompaction whether to enable automatic compaction
	EnableAutoCompaction bool `json:"enable_auto_compaction"`

	// CompactionInterval interval between compaction runs (in seconds)
	CompactionInterval int `json:"compaction_interval"`
}

// DefaultMaintenanceConfig returns default maintenance configuration
func DefaultMaintenanceConfig() *MaintenanceConfig {
	return &MaintenanceConfig{
		EnableAutoGC:         true,
		GCInterval:           300, // 5 minutes
		GCDiscardRatio:       0.5,
		EnableAutoCompaction: true,
		CompactionInterval:   3600, // 1 hour
	}
}

// StartAutoMaintenance starts automatic maintenance routines
func (m *MaintenanceManager) StartAutoMaintenance(config *MaintenanceConfig) error {
	if config == nil {
		config = DefaultMaintenanceConfig()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("maintenance already running")
	}

	m.running = true
	m.stopCh = make(chan struct{})

	// Start GC routine
	if config.EnableAutoGC {
		go m.runGC(config)
	}

	// Start compaction routine
	if config.EnableAutoCompaction {
		go m.runCompaction(config)
	}

	return nil
}

// StopAutoMaintenance stops automatic maintenance routines
func (m *MaintenanceManager) StopAutoMaintenance() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return
	}

	close(m.stopCh)
	m.running = false
}

// runGC runs garbage collection periodically
func (m *MaintenanceManager) runGC(config *MaintenanceConfig) {
	ticker := time.NewTicker(time.Duration(config.GCInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.RunGC(config.GCDiscardRatio)
		}
	}
}

// runCompaction runs compaction periodically
func (m *MaintenanceManager) runCompaction(config *MaintenanceConfig) {
	ticker := time.NewTicker(time.Duration(config.CompactionInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.RunCompaction()
		}
	}
}

// RunGC runs garbage collection once
func (m *MaintenanceManager) RunGC(discardRatio float64) error {
	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected || m.ds.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Run value log GC
	for {
		err := m.ds.db.RunValueLogGC(discardRatio)
		if err != nil {
			// ErrNoRewrite means no files need GC
			if err == badger.ErrNoRewrite {
				return nil
			}
			return err
		}
		// Continue until no more files need GC
	}
}

// RunCompaction runs manual compaction
func (m *MaintenanceManager) RunCompaction() error {
	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected || m.ds.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Flatten the LSM tree
	return m.ds.db.Flatten(2)
}

// DatabaseStats represents database statistics
type DatabaseStats struct {
	// LSM size
	LSMSize int64 `json:"lsm_size"`

	// Value log size
	VLogSize int64 `json:"vlog_size"`

	// Number of keys
	KeyCount int64 `json:"key_count"`

	// Number of tables
	TableCount int `json:"table_count"`

	// Disk usage
	DiskUsage int64 `json:"disk_usage"`

	// Last GC time
	LastGCTime time.Time `json:"last_gc_time"`

	// Last compaction time
	LastCompactionTime time.Time `json:"last_compaction_time"`
}

// GetStats returns database statistics
func (m *MaintenanceManager) GetStats() (*DatabaseStats, error) {
	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected || m.ds.db == nil {
		return nil, fmt.Errorf("database not connected")
	}

	stats := &DatabaseStats{
		TableCount: len(m.ds.tables),
	}

	// Get Badger size using directory walk for persistent mode
	if !m.ds.badgerCfg.InMemory && m.ds.badgerCfg.DataDir != "" {
		stats.DiskUsage = m.calculateDiskUsage()

		// Estimate LSM and VLog sizes from directory
		lsmDir := filepath.Join(m.ds.badgerCfg.DataDir, "")
		filepath.Walk(lsmDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			ext := filepath.Ext(path)
			switch ext {
			case ".sst":
				stats.LSMSize += info.Size()
			case ".vlog":
				stats.VLogSize += info.Size()
			}
			return nil
		})
	}

	// Count keys by iterating
	keyCount := int64(0)
	m.ds.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			keyCount++
		}
		return nil
	})
	stats.KeyCount = keyCount

	return stats, nil
}

// calculateDiskUsage calculates total disk usage
func (m *MaintenanceManager) calculateDiskUsage() int64 {
	dir := m.ds.badgerCfg.DataDir
	if dir == "" {
		return 0
	}

	var size int64
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size
}

// Backup creates a backup of the database
func (m *MaintenanceManager) Backup(ctx context.Context, backupPath string) error {
	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected || m.ds.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Ensure backup directory exists
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create backup file
	backupFile := filepath.Join(backupPath, fmt.Sprintf("backup-%d.bak", time.Now().Unix()))
	f, err := os.Create(backupFile)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer f.Close()

	// Run backup
	since := uint64(0)
	if _, err := m.ds.db.Backup(f, since); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	return nil
}

// Restore restores database from a backup
func (m *MaintenanceManager) Restore(ctx context.Context, backupFile string) error {
	m.ds.mu.Lock()
	defer m.ds.mu.Unlock()

	if !m.ds.connected || m.ds.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Open backup file
	f, err := os.Open(backupFile)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer f.Close()

	// Run restore
	if err := m.ds.db.Load(f, 100); err != nil {
		return fmt.Errorf("failed to restore backup: %w", err)
	}

	// Reload tables into cache
	if err := m.ds.loadTablesFromDB(ctx); err != nil {
		return fmt.Errorf("failed to reload tables: %w", err)
	}

	return nil
}

// CompactAndGC runs both compaction and GC
func (m *MaintenanceManager) CompactAndGC(discardRatio float64) error {
	if err := m.RunCompaction(); err != nil {
		return err
	}
	return m.RunGC(discardRatio)
}

// StreamBackup creates a streaming backup (simplified version)
func (m *MaintenanceManager) StreamBackup(ctx context.Context, outputPath string) error {
	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected || m.ds.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Open output file
	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	// Use backup API
	_, err = m.ds.db.Backup(f, 0)
	return err
}

// VerifyIntegrity verifies database integrity
func (m *MaintenanceManager) VerifyIntegrity(ctx context.Context) error {
	m.ds.mu.RLock()
	defer m.ds.mu.RUnlock()

	if !m.ds.connected || m.ds.db == nil {
		return fmt.Errorf("database not connected")
	}

	// Check each table
	for tableName := range m.ds.tables {
		prefix := m.ds.keyEncoder.EncodeRowPrefix(tableName)

		err := m.ds.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix

			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				// Verify value can be read
				if err := item.Value(func(val []byte) error {
					if len(val) == 0 {
						return fmt.Errorf("empty value for key %s", string(item.Key()))
					}
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			return fmt.Errorf("integrity check failed for table %s: %w", tableName, err)
		}
	}

	return nil
}
