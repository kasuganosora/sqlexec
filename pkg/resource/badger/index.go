package badger

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// IndexManager manages indexes for tables
type IndexManager struct {
	mu      sync.RWMutex
	db      *badger.DB
	indexes map[string]map[string]*IndexInfo // table -> indexName -> indexInfo
	codec   *IndexValueCodec
	encoder *KeyEncoder
}

// NewIndexManager creates a new IndexManager
func NewIndexManager(db *badger.DB) *IndexManager {
	return &IndexManager{
		db:      db,
		indexes: make(map[string]map[string]*IndexInfo),
		codec:   NewIndexValueCodec(),
		encoder: NewKeyEncoder(),
	}
}

// CreateIndex creates an index on a column (convenience wrapper for CreateIndexWithColumns)
// This method is kept for internal use
func (m *IndexManager) CreateIndex(tableName, columnName string, unique bool) error {
	return m.CreateIndexWithColumns(tableName, []string{columnName}, unique)
}

// CreateIndexWithColumns creates an index on one or more columns (composite index support)
func (m *IndexManager) CreateIndexWithColumns(tableName string, columnNames []string, unique bool) error {
	if len(columnNames) == 0 {
		return fmt.Errorf("at least one column is required for index")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.indexes[tableName]; !ok {
		m.indexes[tableName] = make(map[string]*IndexInfo)
	}

	// Generate index name from columns
	indexName := fmt.Sprintf("idx_%s_%s", tableName, strings.Join(columnNames, "_"))
	if _, ok := m.indexes[tableName][indexName]; ok {
		return fmt.Errorf("index already exists: %s", indexName)
	}

	m.indexes[tableName][indexName] = &IndexInfo{
		TableName: tableName,
		Columns:   columnNames,
		Unique:    unique,
		CreatedAt: time.Now(),
	}

	return nil
}

// DropIndex drops an index by column name (drops all indexes containing this column)
func (m *IndexManager) DropIndex(tableName, columnName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tableIndexes, ok := m.indexes[tableName]; ok {
		// Find and delete all indexes that contain this column
		for indexName, idxInfo := range tableIndexes {
			for _, col := range idxInfo.Columns {
				if col == columnName {
					delete(tableIndexes, indexName)
					break
				}
			}
		}
		if len(tableIndexes) == 0 {
			delete(m.indexes, tableName)
		}
	}

	return nil
}

// HasIndex checks if an index exists on a column
func (m *IndexManager) HasIndex(tableName, columnName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if tableIndexes, ok := m.indexes[tableName]; ok {
		// Check if any index contains this column
		for _, idxInfo := range tableIndexes {
			for _, col := range idxInfo.Columns {
				if col == columnName {
					return true
				}
			}
		}
	}
	return false
}

// IsUnique checks if an index on a column is unique
func (m *IndexManager) IsUnique(tableName, columnName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if tableIndexes, ok := m.indexes[tableName]; ok {
		// Find the first index containing this column and return its uniqueness
		for _, idxInfo := range tableIndexes {
			for _, col := range idxInfo.Columns {
				if col == columnName {
					return idxInfo.Unique
				}
			}
		}
	}
	return false
}

// AddToIndex adds a row key to an index
func (m *IndexManager) AddToIndex(txn *badger.Txn, tableName, columnName, value, rowKey string) error {
	idxKey := m.encoder.EncodeIndexKey(tableName, columnName, value)

	var keys []string
	item, err := txn.Get(idxKey)
	if err == badger.ErrKeyNotFound {
		keys = []string{rowKey}
	} else if err != nil {
		return err
	} else {
		if err := item.Value(func(val []byte) error {
			var err error
			keys, err = m.codec.Decode(val)
			return err
		}); err != nil {
			return err
		}
		keys = append(keys, rowKey)
	}

	data, err := m.codec.Encode(keys)
	if err != nil {
		return err
	}

	return txn.Set(idxKey, data)
}

// RemoveFromIndex removes a row key from an index
func (m *IndexManager) RemoveFromIndex(txn *badger.Txn, tableName, columnName, value, rowKey string) error {
	idxKey := m.encoder.EncodeIndexKey(tableName, columnName, value)

	item, err := txn.Get(idxKey)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	var keys []string
	if err := item.Value(func(val []byte) error {
		var err error
		keys, err = m.codec.Decode(val)
		return err
	}); err != nil {
		return err
	}

	// Remove the row key
	newKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != rowKey {
			newKeys = append(newKeys, k)
		}
	}

	if len(newKeys) == 0 {
		return txn.Delete(idxKey)
	}

	data, err := m.codec.Encode(newKeys)
	if err != nil {
		return err
	}

	return txn.Set(idxKey, data)
}

// LookupIndex looks up row keys by index value
func (m *IndexManager) LookupIndex(txn *badger.Txn, tableName, columnName, value string) ([]string, error) {
	idxKey := m.encoder.EncodeIndexKey(tableName, columnName, value)

	item, err := txn.Get(idxKey)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var keys []string
	if err := item.Value(func(val []byte) error {
		var err error
		keys, err = m.codec.Decode(val)
		return err
	}); err != nil {
		return nil, err
	}

	return keys, nil
}

// GetIndexes returns all indexes for a table
func (m *IndexManager) GetIndexes(tableName string) map[string]*IndexInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*IndexInfo)
	if tableIndexes, ok := m.indexes[tableName]; ok {
		for k, v := range tableIndexes {
			result[k] = v
		}
	}
	return result
}
