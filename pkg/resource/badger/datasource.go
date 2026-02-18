package badger

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// BadgerDataSource implements domain.DataSource using Badger KV store
type BadgerDataSource struct {
	config    *domain.DataSourceConfig
	badgerCfg *DataSourceConfig
	db        *badger.DB
	connected bool
	mu        sync.RWMutex

	// codecs
	rowCodec     *RowCodec
	tableCodec   *TableInfoCodec
	indexCodec   *IndexValueCodec
	configCodec  *TableConfigCodec
	keyEncoder   *KeyEncoder
	pkGenerator  *PrimaryKeyGenerator
	valueConv    *ValueConverter

	// managers
	indexManager *IndexManager
	txnManager   *TransactionManager
	seqManager   *SequenceManager

	// table metadata cache
	tables map[string]*domain.TableInfo

	// close channel
	closeCh chan struct{}

	// stats
	stats Stats
}

// NewBadgerDataSource creates a new BadgerDataSource
func NewBadgerDataSource(config *domain.DataSourceConfig) *BadgerDataSource {
	badgerCfg := DefaultDataSourceConfig("")
	if config != nil && config.Options != nil {
		if dir, ok := config.Options["data_dir"].(string); ok {
			badgerCfg.DataDir = dir
		}
		if inMem, ok := config.Options["in_memory"].(bool); ok {
			badgerCfg.InMemory = inMem
		}
	}

	return &BadgerDataSource{
		config:      config,
		badgerCfg:   badgerCfg,
		rowCodec:    NewRowCodec(),
		tableCodec:  NewTableInfoCodec(),
		indexCodec:  NewIndexValueCodec(),
		configCodec: NewTableConfigCodec(),
		keyEncoder:  NewKeyEncoder(),
		pkGenerator: NewPrimaryKeyGenerator(),
		valueConv:   NewValueConverter(),
		tables:      make(map[string]*domain.TableInfo),
		closeCh:     make(chan struct{}),
		stats:       Stats{},
	}
}

// NewBadgerDataSourceWithConfig creates a new BadgerDataSource with custom config
func NewBadgerDataSourceWithConfig(domainCfg *domain.DataSourceConfig, badgerCfg *DataSourceConfig) *BadgerDataSource {
	if badgerCfg == nil {
		badgerCfg = DefaultDataSourceConfig("")
	}

	return &BadgerDataSource{
		config:      domainCfg,
		badgerCfg:   badgerCfg,
		rowCodec:    NewRowCodec(),
		tableCodec:  NewTableInfoCodec(),
		indexCodec:  NewIndexValueCodec(),
		configCodec: NewTableConfigCodec(),
		keyEncoder:  NewKeyEncoder(),
		pkGenerator: NewPrimaryKeyGenerator(),
		valueConv:   NewValueConverter(),
		tables:      make(map[string]*domain.TableInfo),
		closeCh:     make(chan struct{}),
		stats:       Stats{},
	}
}

// Connect establishes connection to Badger database
func (ds *BadgerDataSource) Connect(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.connected {
		return nil
	}

	// Build Badger options
	var opts badger.Options
	if ds.badgerCfg.InMemory {
		opts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		opts = badger.DefaultOptions(ds.badgerCfg.DataDir)
	}
	opts = opts.WithSyncWrites(ds.badgerCfg.SyncWrites)
	opts = opts.WithValueThreshold(ds.badgerCfg.ValueThreshold)
	opts = opts.WithNumMemtables(ds.badgerCfg.NumMemtables)
	opts = opts.WithBaseTableSize(ds.badgerCfg.BaseTableSize)

	// Set compression
	switch ds.badgerCfg.Compression {
	case 1:
		opts = opts.WithCompression(1) // Snappy
	case 2:
		opts = opts.WithCompression(2) // ZSTD
	default:
		opts = opts.WithCompression(0) // None
	}

	// Set custom logger if provided
	if ds.badgerCfg.Logger != nil {
		opts = opts.WithLogger(ds.badgerCfg.Logger)
	}

	// Set encryption key if provided
	if len(ds.badgerCfg.EncryptionKey) > 0 {
		opts = opts.WithEncryptionKey(ds.badgerCfg.EncryptionKey)
	}

	// Open database
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open badger database: %w", err)
	}

	ds.db = db
	ds.connected = true

	// Initialize managers
	ds.indexManager = NewIndexManager(db)
	ds.txnManager = NewTransactionManager(db)
	ds.seqManager = NewSequenceManager(db)

	// Load existing tables into cache
	if err := ds.loadTablesFromDB(ctx); err != nil {
		ds.db.Close()
		ds.connected = false
		return fmt.Errorf("failed to load tables: %w", err)
	}

	return nil
}

// Close closes the database connection
func (ds *BadgerDataSource) Close(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return nil
	}

	close(ds.closeCh)

	// Close sequence manager
	if ds.seqManager != nil {
		ds.seqManager.Close()
	}

	// Close database
	if ds.db != nil {
		if err := ds.db.Close(); err != nil {
			return fmt.Errorf("failed to close badger database: %w", err)
		}
	}

	ds.connected = false
	return nil
}

// IsConnected returns connection status
func (ds *BadgerDataSource) IsConnected() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.connected
}

// IsWritable returns true (Badger is always writable when connected)
func (ds *BadgerDataSource) IsWritable() bool {
	return ds.IsConnected()
}

// GetConfig returns the data source configuration
func (ds *BadgerDataSource) GetConfig() *domain.DataSourceConfig {
	return ds.config
}

// loadTablesFromDB loads all existing tables from database into cache
func (ds *BadgerDataSource) loadTablesFromDB(ctx context.Context) error {
	return ds.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(PrefixTable)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			tableName, ok := ds.keyEncoder.DecodeTableKey(item.Key())
			if !ok {
				continue
			}

			var info *domain.TableInfo
			if err := item.Value(func(val []byte) error {
				var err error
				info, err = ds.tableCodec.Decode(val)
				return err
			}); err != nil {
				return fmt.Errorf("failed to decode table %s: %w", tableName, err)
			}

			if info != nil {
				ds.tables[tableName] = info
			}
		}
		return nil
	})
}

// GetTables returns list of all table names
func (ds *BadgerDataSource) GetTables(ctx context.Context) ([]string, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	tables := make([]string, 0, len(ds.tables))
	for name := range ds.tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// GetTableInfo returns information about a specific table
func (ds *BadgerDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	info, ok := ds.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return info, nil
}

// CreateTable creates a new table
func (ds *BadgerDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	if tableInfo == nil {
		return fmt.Errorf("table info is nil")
	}

	if _, exists := ds.tables[tableInfo.Name]; exists {
		return fmt.Errorf("table %s already exists", tableInfo.Name)
	}

	// Encode table info
	data, err := ds.tableCodec.Encode(tableInfo)
	if err != nil {
		return fmt.Errorf("failed to encode table info: %w", err)
	}

	// Store table metadata
	key := ds.keyEncoder.EncodeTableKey(tableInfo.Name)
	if err := ds.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	}); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Add to cache
	ds.tables[tableInfo.Name] = tableInfo

	// Initialize sequence for auto-increment columns
	for _, col := range tableInfo.Columns {
		if col.AutoIncrement {
			seqKey := string(ds.keyEncoder.EncodeSeqKey(tableInfo.Name, col.Name))
			if err := ds.seqManager.InitSequence(seqKey, 1); err != nil {
				return fmt.Errorf("failed to initialize sequence for %s.%s: %w", tableInfo.Name, col.Name, err)
			}
		}
	}

	return nil
}

// DropTable drops a table
func (ds *BadgerDataSource) DropTable(ctx context.Context, tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	if _, exists := ds.tables[tableName]; !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Delete all rows
	rowPrefix := ds.keyEncoder.EncodeRowPrefix(tableName)
	if err := ds.deleteByPrefix(rowPrefix); err != nil {
		return fmt.Errorf("failed to delete rows: %w", err)
	}

	// Delete all indexes
	idxPrefix := ds.keyEncoder.EncodeIndexPrefix(tableName, "")
	idxPrefix = []byte(PrefixIndex + tableName + ":")
	if err := ds.deleteByPrefix(idxPrefix); err != nil {
		return fmt.Errorf("failed to delete indexes: %w", err)
	}

	// Delete table metadata
	tableKey := ds.keyEncoder.EncodeTableKey(tableName)
	if err := ds.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(tableKey)
	}); err != nil {
		return fmt.Errorf("failed to delete table metadata: %w", err)
	}

	// Remove from cache
	delete(ds.tables, tableName)

	return nil
}

// TruncateTable truncates a table (removes all rows but keeps structure)
func (ds *BadgerDataSource) TruncateTable(ctx context.Context, tableName string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return fmt.Errorf("data source not connected")
	}

	if _, exists := ds.tables[tableName]; !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Delete all rows
	rowPrefix := ds.keyEncoder.EncodeRowPrefix(tableName)
	if err := ds.deleteByPrefix(rowPrefix); err != nil {
		return fmt.Errorf("failed to delete rows: %w", err)
	}

	// Delete all indexes
	idxPrefix := []byte(PrefixIndex + tableName + ":")
	if err := ds.deleteByPrefix(idxPrefix); err != nil {
		return fmt.Errorf("failed to delete indexes: %w", err)
	}

	// Reset sequences
	tableInfo := ds.tables[tableName]
	for _, col := range tableInfo.Columns {
		if col.AutoIncrement {
			seqKey := string(ds.keyEncoder.EncodeSeqKey(tableName, col.Name))
			if err := ds.seqManager.ResetSequence(seqKey, 1); err != nil {
				return fmt.Errorf("failed to reset sequence: %w", err)
			}
		}
	}

	return nil
}

// deleteByPrefix deletes all keys with given prefix
func (ds *BadgerDataSource) deleteByPrefix(prefix []byte) error {
	return ds.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		keysToDelete := make([][]byte, 0)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())
			keysToDelete = append(keysToDelete, key)
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// Execute executes raw SQL (not implemented for Badger - use Query/Insert/Update/Delete)
func (ds *BadgerDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, fmt.Errorf("raw SQL execution not supported in Badger data source")
}

// Query queries rows from a table
func (ds *BadgerDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if !ds.connected {
		return nil, fmt.Errorf("data source not connected")
	}

	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	if options == nil {
		options = &domain.QueryOptions{}
	}

	return ds.queryTable(ctx, tableInfo, options)
}

// queryTable performs the actual query
func (ds *BadgerDataSource) queryTable(ctx context.Context, tableInfo *domain.TableInfo, options *domain.QueryOptions) (*domain.QueryResult, error) {
	result := &domain.QueryResult{
		Columns: tableInfo.Columns,
		Rows:    make([]domain.Row, 0),
	}

	err := ds.db.View(func(txn *badger.Txn) error {
		prefix := ds.keyEncoder.EncodeRowPrefix(tableInfo.Name)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			var row domain.Row
			if err := item.Value(func(val []byte) error {
				var err error
				row, err = ds.rowCodec.Decode(val)
				return err
			}); err != nil {
				return err
			}

			if row == nil {
				continue
			}

			// Convert row types based on schema before returning
			ds.convertRowTypesBasedOnSchema(row, tableInfo)

			// Apply filters
			if !ds.matchesFilters(row, options.Filters) {
				continue
			}

			result.Rows = append(result.Rows, row)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Apply ORDER BY
	if options.OrderBy != "" {
		ds.sortRows(result.Rows, options.OrderBy, options.Order)
	}

	// Apply limit and offset
	if options.Offset > 0 && options.Offset < len(result.Rows) {
		result.Rows = result.Rows[options.Offset:]
	}
	if options.Limit > 0 && options.Limit < len(result.Rows) {
		result.Rows = result.Rows[:options.Limit]
	}

	result.Total = int64(len(result.Rows))
	return result, nil
}

// sortRows sorts rows by the specified column
func (ds *BadgerDataSource) sortRows(rows []domain.Row, orderBy, order string) {
	if len(rows) == 0 || orderBy == "" {
		return
	}

	// Determine sort direction
	ascending := true
	if order == "DESC" || order == "desc" {
		ascending = false
	}

	// Simple bubble sort (for small datasets)
	// For production, consider using sort.Slice with a more efficient comparison
	for i := 0; i < len(rows)-1; i++ {
		for j := i + 1; j < len(rows); j++ {
			cmp := ds.compareValues(rows[i][orderBy], rows[j][orderBy])
			shouldSwap := (ascending && cmp > 0) || (!ascending && cmp < 0)
			if shouldSwap {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
}

// convertRowTypesBasedOnSchema converts row values based on column types defined in schema
func (ds *BadgerDataSource) convertRowTypesBasedOnSchema(row domain.Row, tableInfo *domain.TableInfo) {
	if tableInfo == nil || row == nil {
		return
	}
	for _, col := range tableInfo.Columns {
		val, exists := row[col.Name]
		if !exists || val == nil {
			continue
		}

		colType := strings.ToUpper(col.Type)
		// Handle BOOL/BOOLEAN conversion only (TINYINT is numeric, not boolean)
		if colType == "BOOL" || colType == "BOOLEAN" {
			switch v := val.(type) {
			case int64:
				row[col.Name] = v != 0
			case int:
				row[col.Name] = v != 0
			case float64:
				row[col.Name] = v != 0.0
			case float32:
				row[col.Name] = v != 0.0
			}
		}
	}
}

// matchesFilters checks if a row matches the filter conditions
func (ds *BadgerDataSource) matchesFilters(row domain.Row, filters []domain.Filter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if !ds.matchesFilter(row, filter) {
			return false
		}
	}
	return true
}

// matchesFilter checks a single filter
func (ds *BadgerDataSource) matchesFilter(row domain.Row, filter domain.Filter) bool {
	if filter.Logic != "" && len(filter.SubFilters) > 0 {
		switch filter.Logic {
		case "AND":
			for _, sf := range filter.SubFilters {
				if !ds.matchesFilter(row, sf) {
					return false
				}
			}
			return true
		case "OR":
			for _, sf := range filter.SubFilters {
				if ds.matchesFilter(row, sf) {
					return true
				}
			}
			return false
		}
	}

	// Simple filter
	val, exists := row[filter.Field]
	if !exists {
		return false
	}

	return ds.matchesOperator(val, filter.Operator, filter.Value)
}

// matchesOperator checks if value matches operator
func (ds *BadgerDataSource) matchesOperator(val interface{}, op string, target interface{}) bool {
	if val == nil && target == nil {
		return op == "=" || op == "==" || op == "IS"
	}
	if val == nil {
		return op == "!=" || op == "<>" || op == "IS NOT"
	}

	switch op {
	case "=", "==":
		return fmt.Sprintf("%v", val) == fmt.Sprintf("%v", target)
	case "!=", "<>":
		return fmt.Sprintf("%v", val) != fmt.Sprintf("%v", target)
	case ">":
		return ds.compareValues(val, target) > 0
	case ">=":
		return ds.compareValues(val, target) >= 0
	case "<":
		return ds.compareValues(val, target) < 0
	case "<=":
		return ds.compareValues(val, target) <= 0
	case "LIKE", "like":
		return ds.matchLike(fmt.Sprintf("%v", val), fmt.Sprintf("%v", target))
	case "IN", "in":
		return ds.matchIn(val, target)
	default:
		return false
	}
}

// compareValues compares two values
func (ds *BadgerDataSource) compareValues(a, b interface{}) int {
	// Simple string comparison for now
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

// matchLike performs LIKE pattern matching
// Supports SQL wildcards: % (matches any sequence) and _ (matches any single character)
func (ds *BadgerDataSource) matchLike(s, pattern string) bool {
	// Convert SQL LIKE pattern to regex-like matching
	// % matches any sequence of characters
	// _ matches any single character

	sIdx := 0
	pIdx := 0
	starIdx := -1
	sTmpIdx := 0

	for sIdx < len(s) {
		if pIdx < len(pattern) && (pattern[pIdx] == s[sIdx] || pattern[pIdx] == '_') {
			sIdx++
			pIdx++
		} else if pIdx < len(pattern) && pattern[pIdx] == '%' {
			starIdx = pIdx
			sTmpIdx = sIdx
			pIdx++
		} else if starIdx != -1 {
			pIdx = starIdx + 1
			sTmpIdx++
			sIdx = sTmpIdx
		} else {
			return false
		}
	}

	for pIdx < len(pattern) && pattern[pIdx] == '%' {
		pIdx++
	}

	return pIdx == len(pattern)
}

// matchIn checks if value is in list
func (ds *BadgerDataSource) matchIn(val interface{}, list interface{}) bool {
	// Check if list is a slice
	switch v := list.(type) {
	case []interface{}:
		for _, item := range v {
			if fmt.Sprintf("%v", val) == fmt.Sprintf("%v", item) {
				return true
			}
		}
	case []string:
		for _, item := range v {
			if fmt.Sprintf("%v", val) == item {
				return true
			}
		}
	}
	return false
}

// Insert inserts rows into a table
func (ds *BadgerDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return 0, fmt.Errorf("data source not connected")
	}

	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	if len(rows) == 0 {
		return 0, nil
	}

	var inserted int64
	err := ds.db.Update(func(txn *badger.Txn) error {
		for _, row := range rows {
			// Convert row types based on schema before processing
			ds.convertRowTypesBasedOnSchema(row, tableInfo)

			// Generate primary key
			pk, err := ds.pkGenerator.GenerateFromRow(tableInfo, row)
			if err != nil {
				// Try to generate auto-increment key (also writes ID to row)
				pk, err = ds.generateAutoIncrementPK(txn, tableInfo, row)
				if err != nil {
					return fmt.Errorf("failed to generate primary key: %w", err)
				}
			}

			// Encode row
			data, err := ds.rowCodec.Encode(row)
			if err != nil {
				return fmt.Errorf("failed to encode row: %w", err)
			}

			// Store row
			key := ds.keyEncoder.EncodeRowKey(tableName, pk)
			if err := txn.Set(key, data); err != nil {
				return fmt.Errorf("failed to insert row: %w", err)
			}

			// Update indexes
			if err := ds.updateIndexes(txn, tableInfo, pk, row, nil); err != nil {
				return fmt.Errorf("failed to update indexes: %w", err)
			}

			inserted++
		}
		return nil
	})

	return inserted, err
}

// generateAutoIncrementPK generates auto-increment primary key and writes it to row
func (ds *BadgerDataSource) generateAutoIncrementPK(txn *badger.Txn, tableInfo *domain.TableInfo, row domain.Row) (string, error) {
	for _, col := range tableInfo.Columns {
		if col.AutoIncrement {
			seqKey := string(ds.keyEncoder.EncodeSeqKey(tableInfo.Name, col.Name))
			seq, err := ds.seqManager.GetSequence(seqKey)
			if err != nil {
				return "", err
			}
			num, err := seq.Next()
			if err != nil {
				return "", err
			}
			autoID := int64(num)
			// Write auto-increment ID back to row for LastInsertID support
			row[col.Name] = autoID
			return ds.pkGenerator.FormatIntKey(autoID), nil
		}
	}
	return "", fmt.Errorf("no auto-increment column found")
}

// updateIndexes updates index entries
func (ds *BadgerDataSource) updateIndexes(txn *badger.Txn, tableInfo *domain.TableInfo, pk string, newRow, oldRow domain.Row) error {
	for _, col := range tableInfo.Columns {
		// Create index for primary key and unique columns
		if !col.Primary && !col.Unique {
			continue
		}

		// Handle old row - remove from index
		if oldRow != nil {
			if oldVal, ok := oldRow[col.Name]; ok && oldVal != nil {
				oldValStr := ds.valueConv.ToString(oldVal)
				if err := ds.indexManager.RemoveFromIndex(txn, tableInfo.Name, col.Name, oldValStr, pk); err != nil {
					return fmt.Errorf("failed to remove from index %s.%s: %w", tableInfo.Name, col.Name, err)
				}
			}
		}

		// Handle new row - add to index
		if newRow != nil {
			if newVal, ok := newRow[col.Name]; ok && newVal != nil {
				newValStr := ds.valueConv.ToString(newVal)
				if err := ds.indexManager.AddToIndex(txn, tableInfo.Name, col.Name, newValStr, pk); err != nil {
					return fmt.Errorf("failed to add to index %s.%s: %w", tableInfo.Name, col.Name, err)
				}
			}
		}
	}
	return nil
}

// Update updates rows in a table
func (ds *BadgerDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return 0, fmt.Errorf("data source not connected")
	}

	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	var updated int64
	err := ds.db.Update(func(txn *badger.Txn) error {
		prefix := ds.keyEncoder.EncodeRowPrefix(tableInfo.Name)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			var row domain.Row
			if err := item.Value(func(val []byte) error {
				var err error
				row, err = ds.rowCodec.Decode(val)
				return err
			}); err != nil {
				return err
			}

			if row == nil {
				continue
			}

			// Check filters
			if !ds.matchesFilters(row, filters) {
				continue
			}

			// Apply updates
			for k, v := range updates {
				row[k] = v
			}

			// Encode and save
			data, err := ds.rowCodec.Encode(row)
			if err != nil {
				return fmt.Errorf("failed to encode row: %w", err)
			}

			if err := txn.Set(key, data); err != nil {
				return fmt.Errorf("failed to update row: %w", err)
			}

			updated++
		}
		return nil
	})

	return updated, err
}

// Delete deletes rows from a table
func (ds *BadgerDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.connected {
		return 0, fmt.Errorf("data source not connected")
	}

	tableInfo, exists := ds.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table %s not found", tableName)
	}

	var deleted int64
	err := ds.db.Update(func(txn *badger.Txn) error {
		prefix := ds.keyEncoder.EncodeRowPrefix(tableInfo.Name)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		keysToDelete := make([][]byte, 0)

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := make([]byte, len(item.Key()))
			copy(key, item.Key())

			var row domain.Row
			if err := item.Value(func(val []byte) error {
				var err error
				row, err = ds.rowCodec.Decode(val)
				return err
			}); err != nil {
				return err
			}

			if row == nil {
				continue
			}

			// Check filters
			if !ds.matchesFilters(row, filters) {
				continue
			}

			keysToDelete = append(keysToDelete, key)
		}

		// Delete matched rows
		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return fmt.Errorf("failed to delete row: %w", err)
			}
			deleted++
		}
		return nil
	})

	return deleted, err
}

// Stats returns data source statistics
func (ds *BadgerDataSource) Stats() Stats {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	stats := ds.stats
	stats.TableCount = len(ds.tables)
	stats.UpdatedAt = ds.stats.UpdatedAt
	return stats
}
