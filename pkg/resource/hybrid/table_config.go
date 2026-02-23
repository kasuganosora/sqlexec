package hybrid

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// TableConfigManager manages table-level persistence configuration
type TableConfigManager struct {
	mu                sync.RWMutex
	configs           map[string]*TableConfig // table_name -> config
	defaultPersistent bool

	// persistence for configs themselves
	db        *badger.DB
	connected bool
}

// NewTableConfigManager creates a new TableConfigManager
func NewTableConfigManager(defaultPersistent bool) *TableConfigManager {
	return &TableConfigManager{
		configs:           make(map[string]*TableConfig),
		defaultPersistent: defaultPersistent,
	}
}

// SetBadgerDB sets the Badger DB for persisting configurations
func (m *TableConfigManager) SetBadgerDB(db *badger.DB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.db = db
	m.connected = (db != nil)
}

// GetConfig returns the configuration for a table
// Returns the table-specific config if set, otherwise returns default config
func (m *TableConfigManager) GetConfig(tableName string) *TableConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if config, ok := m.configs[tableName]; ok {
		return config
	}

	// Return default config based on global default
	return &TableConfig{
		TableName:     tableName,
		Persistent:    m.defaultPersistent,
		SyncOnWrite:   false,
		CacheInMemory: true,
	}
}

// SetConfig sets the configuration for a table
func (m *TableConfigManager) SetConfig(ctx context.Context, config *TableConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if config == nil {
		return nil
	}

	m.configs[config.TableName] = config

	// Persist to Badger if available
	if m.connected && m.db != nil {
		if err := m.persistConfig(config); err != nil {
			return err
		}
	}

	return nil
}

// RemoveConfig removes the configuration for a table
func (m *TableConfigManager) RemoveConfig(ctx context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.configs, tableName)

	// Remove from Badger if available
	if m.connected && m.db != nil {
		if err := m.deleteConfig(tableName); err != nil {
			return err
		}
	}

	return nil
}

// IsPersistent returns whether a table is configured to be persistent
func (m *TableConfigManager) IsPersistent(tableName string) bool {
	return m.GetConfig(tableName).Persistent
}

// ListPersistentTables returns list of all persistent table names
func (m *TableConfigManager) ListPersistentTables() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, 0)
	for name, config := range m.configs {
		if config.Persistent {
			result = append(result, name)
		}
	}
	return result
}

// ListMemoryTables returns list of all memory-only table names
func (m *TableConfigManager) ListMemoryTables() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, 0)
	for name, config := range m.configs {
		if !config.Persistent {
			result = append(result, name)
		}
	}
	return result
}

// GetAllConfigs returns all table configurations
func (m *TableConfigManager) GetAllConfigs() map[string]*TableConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*TableConfig)
	for k, v := range m.configs {
		result[k] = v
	}
	return result
}

// SetDefaultPersistent sets the default persistence policy
func (m *TableConfigManager) SetDefaultPersistent(defaultPersistent bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultPersistent = defaultPersistent
}

// GetDefaultPersistent returns the default persistence policy
func (m *TableConfigManager) GetDefaultPersistent() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultPersistent
}

// LoadFromDB loads all table configurations from Badger
func (m *TableConfigManager) LoadFromDB(ctx context.Context) error {
	if m.db == nil {
		return nil
	}

	return m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("hybrid:config:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var config TableConfig
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &config)
			}); err != nil {
				continue
			}

			m.configs[config.TableName] = &config
		}
		return nil
	})
}

// persistConfig persists a single table config to Badger
func (m *TableConfigManager) persistConfig(config *TableConfig) error {
	if m.db == nil {
		return nil
	}

	data, err := json.Marshal(config)
	if err != nil {
		return err
	}

	key := []byte("hybrid:config:" + config.TableName)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// deleteConfig deletes a table config from Badger
func (m *TableConfigManager) deleteConfig(tableName string) error {
	if m.db == nil {
		return nil
	}

	key := []byte("hybrid:config:" + tableName)
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
