package hybrid

import (
	"context"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	badgerds "github.com/kasuganosora/sqlexec/pkg/resource/badger"
)

// HybridDataSource combines memory and persistent storage
// It routes operations to appropriate data source based on table configuration
type HybridDataSource struct {
	config    *HybridDataSourceConfig
	domainCfg *domain.DataSourceConfig
	connected bool
	mu        sync.RWMutex

	// Sub data sources
	memory   *memory.MVCCDataSource
	badgerDS *badgerds.BadgerDataSource
	badgerDB *badger.DB

	// Table configuration manager
	tableConfig *TableConfigManager

	// Router
	router *DataSourceRouter

	// Stats
	stats Stats

	// Close channel
	closeCh chan struct{}
}

// NewHybridDataSource creates a new HybridDataSource
func NewHybridDataSource(domainCfg *domain.DataSourceConfig, config *HybridDataSourceConfig) *HybridDataSource {
	if config == nil {
		config = DefaultHybridConfig("")
	}

	return &HybridDataSource{
		config:      config,
		domainCfg:   domainCfg,
		tableConfig: NewTableConfigManager(config.DefaultPersistent),
		closeCh:     make(chan struct{}),
	}
}

// Connect establishes connection to all data sources
func (ds *HybridDataSource) Connect(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.connected {
		return nil
	}

	// Initialize memory data source
	ds.memory = memory.NewMVCCDataSource(ds.domainCfg)
	if err := ds.memory.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect memory data source: %w", err)
	}

	// Initialize Badger if enabled
	if ds.config.EnableBadger {
		badgerCfg := badgerds.DefaultDataSourceConfig(ds.config.DataDir)
		if ds.config.DataDir == "" {
			badgerCfg.InMemory = true
		}

		ds.badgerDS = badgerds.NewBadgerDataSourceWithConfig(ds.domainCfg, badgerCfg)
		if err := ds.badgerDS.Connect(ctx); err != nil {
			ds.memory.Close(ctx)
			return fmt.Errorf("failed to connect Badger data source: %w", err)
		}

		// Get Badger DB from config for table config persistence
		// Note: BadgerDataSource manages its own DB, we need to get it
		// For now, we'll manage table configs in memory only
		// TODO: Implement DB sharing or config persistence
	}

	// Initialize router
	ds.router = NewDataSourceRouter(
		ds.tableConfig,
		ds.memory,
		ds.badgerDS,
		ds.badgerDB,
	)

	// Load table configs from Badger if available
	if ds.badgerDB != nil {
		ds.tableConfig.SetBadgerDB(ds.badgerDB)
		if err := ds.tableConfig.LoadFromDB(ctx); err != nil {
			// Non-fatal: continue with empty config
		}
	}

	ds.connected = true
	return nil
}

// Close closes all data sources
func (ds *HybridDataSource) Close(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return nil
	}

	close(ds.closeCh)

	var errs []error

	// Close memory data source
	if ds.memory != nil {
		if err := ds.memory.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("memory close error: %w", err))
		}
	}

	// Close Badger data source
	if ds.badgerDS != nil {
		if err := ds.badgerDS.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("badger close error: %w", err))
		}
	}

	ds.connected = false

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	return nil
}

// IsConnected returns connection status
func (ds *HybridDataSource) IsConnected() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.connected
}

// IsWritable returns true (always writable when connected)
func (ds *HybridDataSource) IsWritable() bool {
	return ds.IsConnected()
}

// GetConfig returns the data source configuration
func (ds *HybridDataSource) GetConfig() *domain.DataSourceConfig {
	return ds.domainCfg
}

// ==================== Table Management ====================

// GetTables returns list of all table names from all data sources
func (ds *HybridDataSource) GetTables(ctx context.Context) ([]string, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	tableSet := make(map[string]bool)

	// Get tables from memory
	memTables, err := ds.memory.GetTables(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range memTables {
		tableSet[t] = true
	}

	// Get tables from Badger
	if ds.badgerDS != nil {
		badgerTables, err := ds.badgerDS.GetTables(ctx)
		if err != nil {
			return nil, err
		}
		for _, t := range badgerTables {
			tableSet[t] = true
		}
	}

	result := make([]string, 0, len(tableSet))
	for t := range tableSet {
		result = append(result, t)
	}
	return result, nil
}

// GetTableInfo returns table information
func (ds *HybridDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	// Route to appropriate data source
	source := ds.router.GetReadSource(tableName)
	if source == nil {
		return nil, fmt.Errorf("no data source available for table %s", tableName)
	}

	info, err := source.GetTableInfo(ctx, tableName)
	if err != nil {
		// Try memory if Badger failed
		if ds.router.HasBadger() && source == ds.badgerDS {
			return ds.memory.GetTableInfo(ctx, tableName)
		}
		return nil, err
	}
	return info, nil
}

// CreateTable creates a table in the appropriate data source
func (ds *HybridDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	if tableInfo == nil {
		return fmt.Errorf("table info is nil")
	}

	// Route to appropriate data source
	source := ds.router.GetDDLSource(tableInfo.Name)
	if source == nil {
		return fmt.Errorf("no data source available for table %s", tableInfo.Name)
	}

	return source.CreateTable(ctx, tableInfo)
}

// DropTable drops a table
func (ds *HybridDataSource) DropTable(ctx context.Context, tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	// Route to appropriate data source
	source := ds.router.GetDDLSource(tableName)
	if source == nil {
		return fmt.Errorf("no data source available for table %s", tableName)
	}

	// Also remove table config
	defer ds.tableConfig.RemoveConfig(ctx, tableName)

	return source.DropTable(ctx, tableName)
}

// TruncateTable truncates a table
func (ds *HybridDataSource) TruncateTable(ctx context.Context, tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	// Route to appropriate data source
	source := ds.router.GetDDLSource(tableName)
	if source == nil {
		return fmt.Errorf("no data source available for table %s", tableName)
	}

	return source.TruncateTable(ctx, tableName)
}

// ==================== CRUD Operations ====================

// Query queries rows from a table
func (ds *HybridDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	ds.stats.TotalReads++

	// Route to appropriate data source
	source := ds.router.GetReadSource(tableName)
	if source == nil {
		return nil, fmt.Errorf("no data source available for table %s", tableName)
	}

	// Track stats
	if source == ds.memory {
		ds.stats.MemoryReads++
	} else if source == ds.badgerDS {
		ds.stats.BadgerReads++
	}

	return source.Query(ctx, tableName, options)
}

// Insert inserts rows into a table
func (ds *HybridDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return 0, fmt.Errorf("data source not connected")
	}

	ds.stats.TotalWrites++

	// Route to appropriate data source
	source := ds.router.GetWriteSource(tableName)
	if source == nil {
		return 0, fmt.Errorf("no data source available for table %s", tableName)
	}

	// Track stats
	if source == ds.memory {
		ds.stats.MemoryWrites++
	} else if source == ds.badgerDS {
		ds.stats.BadgerWrites++
	}

	// Handle dual-write if needed
	if ds.router.ShouldDualWrite(tableName) {
		return ds.dualWrite(ctx, tableName, rows, options)
	}

	return source.Insert(ctx, tableName, rows, options)
}

// Update updates rows in a table
func (ds *HybridDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return 0, fmt.Errorf("data source not connected")
	}

	ds.stats.TotalWrites++

	// Route to appropriate data source
	source := ds.router.GetWriteSource(tableName)
	if source == nil {
		return 0, fmt.Errorf("no data source available for table %s", tableName)
	}

	// Track stats
	if source == ds.memory {
		ds.stats.MemoryWrites++
	} else if source == ds.badgerDS {
		ds.stats.BadgerWrites++
	}

	// Handle dual-write if needed
	if ds.router.ShouldDualWrite(tableName) {
		return ds.dualUpdate(ctx, tableName, filters, updates, options)
	}

	return source.Update(ctx, tableName, filters, updates, options)
}

// Delete deletes rows from a table
func (ds *HybridDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return 0, fmt.Errorf("data source not connected")
	}

	ds.stats.TotalWrites++

	// Route to appropriate data source
	source := ds.router.GetWriteSource(tableName)
	if source == nil {
		return 0, fmt.Errorf("no data source available for table %s", tableName)
	}

	// Track stats
	if source == ds.memory {
		ds.stats.MemoryWrites++
	} else if source == ds.badgerDS {
		ds.stats.BadgerWrites++
	}

	// Handle dual-write if needed
	if ds.router.ShouldDualWrite(tableName) {
		return ds.dualDelete(ctx, tableName, filters, options)
	}

	return source.Delete(ctx, tableName, filters, options)
}

// Execute executes raw SQL (delegated to memory data source)
func (ds *HybridDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	// Execute is typically handled by the memory data source
	return ds.memory.Execute(ctx, sql)
}

// ==================== Transaction Support ====================

// BeginTransaction begins a new transaction
func (ds *HybridDataSource) BeginTransaction(ctx context.Context, options *domain.TransactionOptions) (domain.Transaction, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	// For now, use memory transaction
	// TODO: Implement hybrid transaction that coordinates both data sources
	return ds.memory.BeginTransaction(ctx, options)
}

// ==================== Dual-Write Operations ====================

// dualWrite writes to both data sources
func (ds *HybridDataSource) dualWrite(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	var inserted int64
	var firstErr error

	// Write to memory first
	memInserted, err := ds.memory.Insert(ctx, tableName, rows, options)
	if err != nil {
		firstErr = err
	} else {
		inserted = memInserted
	}

	// Write to Badger
	if ds.badgerDS != nil {
		_, err := ds.badgerDS.Insert(ctx, tableName, rows, options)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return inserted, firstErr
}

// dualUpdate updates in both data sources
func (ds *HybridDataSource) dualUpdate(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	var updated int64
	var firstErr error

	// Update memory first
	memUpdated, err := ds.memory.Update(ctx, tableName, filters, updates, options)
	if err != nil {
		firstErr = err
	} else {
		updated = memUpdated
	}

	// Update Badger
	if ds.badgerDS != nil {
		_, err := ds.badgerDS.Update(ctx, tableName, filters, updates, options)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return updated, firstErr
}

// dualDelete deletes from both data sources
func (ds *HybridDataSource) dualDelete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	var deleted int64
	var firstErr error

	// Delete from memory first
	memDeleted, err := ds.memory.Delete(ctx, tableName, filters, options)
	if err != nil {
		firstErr = err
	} else {
		deleted = memDeleted
	}

	// Delete from Badger
	if ds.badgerDS != nil {
		_, err := ds.badgerDS.Delete(ctx, tableName, filters, options)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return deleted, firstErr
}

// ==================== Persistence Control ====================

// EnablePersistence enables persistence for a table
func (ds *HybridDataSource) EnablePersistence(ctx context.Context, tableName string, opts ...PersistenceOption) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	if ds.badgerDS == nil {
		return fmt.Errorf("Badger data source not available")
	}

	// Create config with defaults
	config := &TableConfig{
		TableName:     tableName,
		Persistent:    true,
		SyncOnWrite:   false,
		CacheInMemory: true,
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	// Save config
	if err := ds.tableConfig.SetConfig(ctx, config); err != nil {
		return err
	}

	return nil
}

// DisablePersistence disables persistence for a table
func (ds *HybridDataSource) DisablePersistence(ctx context.Context, tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	config := &TableConfig{
		TableName:     tableName,
		Persistent:    false,
		SyncOnWrite:   false,
		CacheInMemory: true,
	}

	return ds.tableConfig.SetConfig(ctx, config)
}

// GetPersistenceConfig returns the persistence configuration for a table
func (ds *HybridDataSource) GetPersistenceConfig(tableName string) (*TableConfig, bool) {
	return ds.tableConfig.GetConfig(tableName), true
}

// ListPersistentTables returns list of all persistent table names
func (ds *HybridDataSource) ListPersistentTables() []string {
	return ds.tableConfig.ListPersistentTables()
}

// ==================== Data Migration ====================

// MigrateToPersistent migrates a memory table to persistent storage
func (ds *HybridDataSource) MigrateToPersistent(ctx context.Context, tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	if ds.badgerDS == nil {
		return fmt.Errorf("Badger data source not available")
	}

	// Get table info from memory
	tableInfo, err := ds.memory.GetTableInfo(ctx, tableName)
	if err != nil {
		return fmt.Errorf("table not found in memory: %w", err)
	}

	// Create table in Badger
	if err := ds.badgerDS.CreateTable(ctx, tableInfo); err != nil {
		return fmt.Errorf("failed to create table in Badger: %w", err)
	}

	// Query all rows from memory
	result, err := ds.memory.Query(ctx, tableName, &domain.QueryOptions{})
	if err != nil {
		ds.badgerDS.DropTable(ctx, tableName)
		return fmt.Errorf("failed to query rows from memory: %w", err)
	}

	// Insert all rows to Badger
	if len(result.Rows) > 0 {
		if _, err := ds.badgerDS.Insert(ctx, tableName, result.Rows, nil); err != nil {
			ds.badgerDS.DropTable(ctx, tableName)
			return fmt.Errorf("failed to insert rows to Badger: %w", err)
		}
	}

	// Update config to enable persistence
	config := &TableConfig{
		TableName:     tableName,
		Persistent:    true,
		SyncOnWrite:   false,
		CacheInMemory: true,
	}
	ds.tableConfig.SetConfig(ctx, config)

	// Drop table from memory
	ds.memory.DropTable(ctx, tableName)

	return nil
}

// LoadToMemory loads a persistent table to memory
func (ds *HybridDataSource) LoadToMemory(ctx context.Context, tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	if ds.badgerDS == nil {
		return fmt.Errorf("Badger data source not available")
	}

	// Get table info from Badger
	tableInfo, err := ds.badgerDS.GetTableInfo(ctx, tableName)
	if err != nil {
		return fmt.Errorf("table not found in Badger: %w", err)
	}

	// Create table in memory
	if err := ds.memory.CreateTable(ctx, tableInfo); err != nil {
		return fmt.Errorf("failed to create table in memory: %w", err)
	}

	// Query all rows from Badger
	result, err := ds.badgerDS.Query(ctx, tableName, &domain.QueryOptions{})
	if err != nil {
		ds.memory.DropTable(ctx, tableName)
		return fmt.Errorf("failed to query rows from Badger: %w", err)
	}

	// Insert all rows to memory
	if len(result.Rows) > 0 {
		if _, err := ds.memory.Insert(ctx, tableName, result.Rows, nil); err != nil {
			ds.memory.DropTable(ctx, tableName)
			return fmt.Errorf("failed to insert rows to memory: %w", err)
		}
	}

	// Update config to disable persistence
	config := &TableConfig{
		TableName:     tableName,
		Persistent:    false,
		SyncOnWrite:   false,
		CacheInMemory: true,
	}
	ds.tableConfig.SetConfig(ctx, config)

	return nil
}

// Stats returns data source statistics
func (ds *HybridDataSource) Stats() Stats {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.stats
}
