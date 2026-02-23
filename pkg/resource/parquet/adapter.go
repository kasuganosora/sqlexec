package parquet

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/filemeta"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// Default flush interval for periodic persistence.
const defaultFlushInterval = 30 * time.Second

// ParquetAdapter is a Parquet datasource adapter with full write, persistence,
// multi-table, WAL, and index support. It embeds MVCCDataSource for all
// in-memory features (MVCC, transactions, indexing, query planning, buffer pool).
type ParquetAdapter struct {
	*memory.MVCCDataSource

	dataDir     string
	compression string

	// WAL
	wal *WAL

	// Dirty table tracking for flush
	dirtyTables map[string]bool
	dirtyMu     sync.Mutex

	// Periodic flush
	flushInterval time.Duration
	flushTicker   *time.Ticker
	stopCh        chan struct{}

	// General config
	writable bool
}

// NewParquetAdapter creates a Parquet datasource adapter.
// config.Name is used as the data directory path (one directory = one database,
// one .parquet file per table).
func NewParquetAdapter(config *domain.DataSourceConfig) *ParquetAdapter {
	writable := config.Writable
	compression := "snappy"
	flushInterval := defaultFlushInterval

	if config.Options != nil {
		if w, ok := config.Options["writable"]; ok {
			if b, ok := w.(bool); ok {
				writable = b
			}
		}
		if c, ok := config.Options["compression"]; ok {
			if s, ok := c.(string); ok && s != "" {
				compression = s
			}
		}
		if fi, ok := config.Options["flush_interval"]; ok {
			if s, ok := fi.(string); ok {
				if d, err := time.ParseDuration(s); err == nil {
					flushInterval = d
				}
			}
		}
	}

	internalConfig := *config
	internalConfig.Writable = writable

	return &ParquetAdapter{
		MVCCDataSource: memory.NewMVCCDataSource(&internalConfig),
		dataDir:        config.Name,
		compression:    compression,
		flushInterval:  flushInterval,
		dirtyTables:    make(map[string]bool),
		writable:       writable,
	}
}

// Connect loads data from Parquet files, replays WAL, rebuilds indexes, and starts flusher.
func (a *ParquetAdapter) Connect(ctx context.Context) error {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(a.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory %q: %w", a.dataDir, err)
	}

	// Scan for .parquet files
	entries, err := os.ReadDir(a.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory %q: %w", a.dataDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".parquet") {
			continue
		}

		filePath := filepath.Join(a.dataDir, entry.Name())
		tableInfo, rows, err := readParquetFile(filePath)
		if err != nil {
			log.Printf("warning: failed to read parquet file %q: %v", filePath, err)
			continue
		}

		if err := a.LoadTable(tableInfo.Name, tableInfo, rows); err != nil {
			log.Printf("warning: failed to load table %q: %v", tableInfo.Name, err)
			continue
		}
	}

	// Load sidecar metadata for indexes
	a.loadIndexMeta()

	// Initialize WAL
	if a.writable {
		wal, err := newWAL(a.dataDir)
		if err != nil {
			return fmt.Errorf("failed to initialize WAL: %w", err)
		}
		a.wal = wal

		// Replay WAL entries
		if err := a.replayWAL(ctx); err != nil {
			return fmt.Errorf("failed to replay WAL: %w", err)
		}

		// Start periodic flusher
		a.startFlusher()
	}

	return a.MVCCDataSource.Connect(ctx)
}

// replayWAL replays WAL entries to reconstruct unflushed state.
func (a *ParquetAdapter) replayWAL(ctx context.Context) error {
	entries, err := ReadAll(a.dataDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Type {
		case WALInsert:
			if _, err := a.MVCCDataSource.Insert(ctx, entry.TableName, entry.Rows, nil); err != nil {
				log.Printf("WAL replay: insert into %s failed: %v", entry.TableName, err)
			}
		case WALUpdate:
			if _, err := a.MVCCDataSource.Update(ctx, entry.TableName, entry.Filters, entry.Updates, nil); err != nil {
				log.Printf("WAL replay: update %s failed: %v", entry.TableName, err)
			}
		case WALDelete:
			if _, err := a.MVCCDataSource.Delete(ctx, entry.TableName, entry.Filters, nil); err != nil {
				log.Printf("WAL replay: delete from %s failed: %v", entry.TableName, err)
			}
		case WALCreateTable:
			if err := a.MVCCDataSource.CreateTable(ctx, entry.Schema); err != nil {
				log.Printf("WAL replay: create table %s failed: %v", entry.TableName, err)
			}
		case WALDropTable:
			if err := a.MVCCDataSource.DropTable(ctx, entry.TableName); err != nil {
				log.Printf("WAL replay: drop table %s failed: %v", entry.TableName, err)
			}
		case WALTruncateTable:
			if err := a.MVCCDataSource.TruncateTable(ctx, entry.TableName); err != nil {
				log.Printf("WAL replay: truncate table %s failed: %v", entry.TableName, err)
			}
		}
	}

	return nil
}

// loadIndexMeta loads index metadata from sidecar file.
func (a *ParquetAdapter) loadIndexMeta() {
	metaPath := filepath.Join(a.dataDir, ".sqlexec_meta")
	meta, err := filemeta.Load(metaPath)
	if err != nil || meta == nil {
		return
	}

	for _, idx := range meta.Indexes {
		if err := a.MVCCDataSource.CreateIndexWithColumns(idx.Table, idx.Columns, idx.Type, idx.Unique); err != nil {
			log.Printf("warning: failed to rebuild index %s on %s: %v", idx.Name, idx.Table, err)
		}
	}
}

// ==================== CRUD Operations (WAL-integrated) ====================

// Insert inserts rows with WAL logging.
func (a *ParquetAdapter) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("parquet", "insert")
	}

	// Write WAL before memory operation
	if a.wal != nil {
		if err := a.wal.Append(&WALEntry{
			Type:      WALInsert,
			TableName: tableName,
			Rows:      rows,
		}); err != nil {
			return 0, fmt.Errorf("WAL write failed: %w", err)
		}
	}

	result, err := a.MVCCDataSource.Insert(ctx, tableName, rows, options)
	if err != nil {
		return 0, err
	}

	a.markDirty(tableName)
	return result, nil
}

// Update updates rows with WAL logging.
func (a *ParquetAdapter) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("parquet", "update")
	}

	if a.wal != nil {
		if err := a.wal.Append(&WALEntry{
			Type:      WALUpdate,
			TableName: tableName,
			Filters:   filters,
			Updates:   updates,
		}); err != nil {
			return 0, fmt.Errorf("WAL write failed: %w", err)
		}
	}

	result, err := a.MVCCDataSource.Update(ctx, tableName, filters, updates, options)
	if err != nil {
		return 0, err
	}

	a.markDirty(tableName)
	return result, nil
}

// Delete deletes rows with WAL logging.
func (a *ParquetAdapter) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	if !a.writable {
		return 0, domain.NewErrReadOnly("parquet", "delete")
	}

	if a.wal != nil {
		if err := a.wal.Append(&WALEntry{
			Type:      WALDelete,
			TableName: tableName,
			Filters:   filters,
		}); err != nil {
			return 0, fmt.Errorf("WAL write failed: %w", err)
		}
	}

	result, err := a.MVCCDataSource.Delete(ctx, tableName, filters, options)
	if err != nil {
		return 0, err
	}

	a.markDirty(tableName)
	return result, nil
}

// ==================== DDL Operations ====================

// CreateTable creates a table and writes an empty Parquet file.
func (a *ParquetAdapter) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	if !a.writable {
		return domain.NewErrReadOnly("parquet", "create table")
	}

	if a.wal != nil {
		if err := a.wal.Append(&WALEntry{
			Type:      WALCreateTable,
			TableName: tableInfo.Name,
			Schema:    tableInfo,
		}); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if err := a.MVCCDataSource.CreateTable(ctx, tableInfo); err != nil {
		return err
	}

	// Write empty Parquet file
	filePath := filepath.Join(a.dataDir, tableInfo.Name+".parquet")
	if err := writeParquetFile(filePath, tableInfo, nil, a.compression); err != nil {
		log.Printf("warning: failed to write initial parquet file for %s: %v", tableInfo.Name, err)
	}

	return nil
}

// DropTable drops a table and removes its Parquet file.
func (a *ParquetAdapter) DropTable(ctx context.Context, tableName string) error {
	if !a.writable {
		return domain.NewErrReadOnly("parquet", "drop table")
	}

	if a.wal != nil {
		if err := a.wal.Append(&WALEntry{
			Type:      WALDropTable,
			TableName: tableName,
		}); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if err := a.MVCCDataSource.DropTable(ctx, tableName); err != nil {
		return err
	}

	// Remove Parquet file
	filePath := filepath.Join(a.dataDir, tableName+".parquet")
	os.Remove(filePath)

	a.removeDirty(tableName)
	return nil
}

// TruncateTable truncates a table.
func (a *ParquetAdapter) TruncateTable(ctx context.Context, tableName string) error {
	if !a.writable {
		return domain.NewErrReadOnly("parquet", "truncate table")
	}

	if a.wal != nil {
		if err := a.wal.Append(&WALEntry{
			Type:      WALTruncateTable,
			TableName: tableName,
		}); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if err := a.MVCCDataSource.TruncateTable(ctx, tableName); err != nil {
		return err
	}

	a.markDirty(tableName)
	return nil
}

// Execute is not supported for Parquet datasource.
func (a *ParquetAdapter) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, domain.NewErrUnsupportedOperation("parquet", "execute SQL")
}

// ==================== Flush Mechanism ====================

// startFlusher starts the periodic flush goroutine.
func (a *ParquetAdapter) startFlusher() {
	a.flushTicker = time.NewTicker(a.flushInterval)
	a.stopCh = make(chan struct{})
	go func() {
		for {
			select {
			case <-a.flushTicker.C:
				a.flushDirtyTables()
			case <-a.stopCh:
				return
			}
		}
	}()
}

// flushDirtyTables writes all dirty tables to Parquet files and checkpoints WAL.
func (a *ParquetAdapter) flushDirtyTables() {
	a.dirtyMu.Lock()
	tables := make(map[string]bool, len(a.dirtyTables))
	for k, v := range a.dirtyTables {
		tables[k] = v
	}
	a.dirtyMu.Unlock()

	if len(tables) == 0 {
		return
	}

	allFlushed := true
	for tableName := range tables {
		if err := a.flushTable(tableName); err != nil {
			log.Printf("flush %s failed: %v", tableName, err)
			allFlushed = false
			continue
		}
		a.removeDirty(tableName)
	}

	// If all tables flushed successfully, checkpoint and truncate WAL
	if allFlushed && a.wal != nil {
		if err := a.wal.Append(&WALEntry{Type: WALCheckpoint}); err != nil {
			log.Printf("WAL checkpoint failed: %v", err)
			return
		}
		if err := a.wal.Truncate(); err != nil {
			log.Printf("WAL truncate failed: %v", err)
		}
	}
}

// flushTable writes a single table to a Parquet file.
func (a *ParquetAdapter) flushTable(tableName string) error {
	schema, rows, err := a.GetLatestTableData(tableName)
	if err != nil {
		return err
	}

	filePath := filepath.Join(a.dataDir, tableName+".parquet")
	return writeParquetFile(filePath, schema, rows, a.compression)
}

// markDirty marks a table as needing flush.
func (a *ParquetAdapter) markDirty(tableName string) {
	a.dirtyMu.Lock()
	defer a.dirtyMu.Unlock()
	a.dirtyTables[tableName] = true
}

// removeDirty removes a table from the dirty list.
func (a *ParquetAdapter) removeDirty(tableName string) {
	a.dirtyMu.Lock()
	defer a.dirtyMu.Unlock()
	delete(a.dirtyTables, tableName)
}

// ==================== Close ====================

// Close stops the flusher, flushes all dirty tables, persists metadata, and closes WAL.
func (a *ParquetAdapter) Close(ctx context.Context) error {
	// Stop flusher
	if a.stopCh != nil {
		close(a.stopCh)
	}
	if a.flushTicker != nil {
		a.flushTicker.Stop()
	}

	// Final flush
	if a.writable {
		a.flushDirtyTables()
		a.persistAllIndexMeta()
	}

	// Close WAL
	if a.wal != nil {
		a.wal.Close()
	}

	return a.MVCCDataSource.Close(ctx)
}

// ==================== Index Persistence ====================

// PersistIndexMeta saves index metadata to a sidecar file.
// Implements domain.IndexPersister interface.
func (a *ParquetAdapter) PersistIndexMeta(indexes []domain.IndexMetaInfo) error {
	metaPath := filepath.Join(a.dataDir, ".sqlexec_meta")

	fm := &filemeta.FileMeta{
		Indexes: make([]filemeta.IndexMeta, len(indexes)),
	}

	for i, idx := range indexes {
		fm.Indexes[i] = filemeta.IndexMeta{
			Name:    idx.Name,
			Table:   idx.Table,
			Type:    idx.Type,
			Unique:  idx.Unique,
			Columns: idx.Columns,
		}
	}

	return filemeta.Save(metaPath, fm)
}

// persistAllIndexMeta collects and persists all index metadata.
func (a *ParquetAdapter) persistAllIndexMeta() {
	ctx := context.Background()
	tables, err := a.GetTables(ctx)
	if err != nil {
		return
	}

	var allIndexes []domain.IndexMetaInfo
	for _, table := range tables {
		indexes, err := a.GetTableIndexes(table)
		if err != nil {
			continue
		}
		for _, idx := range indexes {
			allIndexes = append(allIndexes, domain.IndexMetaInfo{
				Name:    idx.Name,
				Table:   table,
				Type:    string(idx.Type),
				Unique:  idx.Unique,
				Columns: idx.Columns,
			})
		}
	}

	if len(allIndexes) > 0 {
		if err := a.PersistIndexMeta(allIndexes); err != nil {
			log.Printf("warning: failed to persist index metadata: %v", err)
		}
	}
}

// ==================== Delegated Methods ====================

// GetConfig returns the datasource configuration.
func (a *ParquetAdapter) GetConfig() *domain.DataSourceConfig {
	return a.MVCCDataSource.GetConfig()
}

// GetTables returns all tables.
func (a *ParquetAdapter) GetTables(ctx context.Context) ([]string, error) {
	return a.MVCCDataSource.GetTables(ctx)
}

// GetTableInfo returns table information.
func (a *ParquetAdapter) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return a.MVCCDataSource.GetTableInfo(ctx, tableName)
}

// Query queries table data.
func (a *ParquetAdapter) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return a.MVCCDataSource.Query(ctx, tableName, options)
}

// IsConnected checks if the datasource is connected.
func (a *ParquetAdapter) IsConnected() bool {
	return a.MVCCDataSource.IsConnected()
}

// IsWritable checks if the datasource is writable.
func (a *ParquetAdapter) IsWritable() bool {
	return a.writable
}

// SupportsWrite implements IsWritableSource interface.
func (a *ParquetAdapter) SupportsWrite() bool {
	return a.writable
}
