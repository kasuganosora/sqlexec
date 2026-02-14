package api

import (
	"context"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/session"
)

// DB is the main database object for managing datasources and creating sessions
type DB struct {
	mu           sync.RWMutex
	dataSources  map[string]domain.DataSource
	defaultDS    string
	dsManager    *application.DataSourceManager
	cache        *QueryCache
	logger       Logger
	config       *DBConfig
}

// DBConfig contains configuration options for the DB object
type DBConfig struct {
	CacheEnabled          bool
	CacheSize           int
	CacheTTL            int // seconds
	DefaultLogger       Logger
	DebugMode           bool
	QueryTimeout        time.Duration // 全局查询超时, 0表示不限制
	UseEnhancedOptimizer bool         // 是否使用增强优化器（默认true）
}

// NewDB creates a new DB object with the given configuration
func NewDB(config *DBConfig) (*DB, error) {
	if config == nil {
		config = &DBConfig{
			CacheEnabled:          true,
			CacheSize:           1000,
			CacheTTL:            300, // 5 minutes
			DefaultLogger:       NewDefaultLogger(LogInfo),
			DebugMode:           false,
			UseEnhancedOptimizer: true, // 默认启用增强优化器
		}
	}

	// Ensure logger is set
	if config.DefaultLogger == nil {
		config.DefaultLogger = NewDefaultLogger(LogInfo)
	}

	// Ensure UseEnhancedOptimizer is set to true if not specified
	if config.UseEnhancedOptimizer == false && config == nil {
		config.UseEnhancedOptimizer = true
	}

	cache := NewQueryCache(CacheConfig{
		Enabled:  config.CacheEnabled,
		TTL:      time.Duration(config.CacheTTL) * time.Second,
		MaxSize:  config.CacheSize,
	})

	dsManager := application.NewDataSourceManager()

	return &DB{
		dataSources: make(map[string]domain.DataSource),
		dsManager:   dsManager,
		cache:       cache,
		logger:      config.DefaultLogger,
		config:      config,
	}, nil
}

// RegisterDataSource registers a datasource with the given name
func (db *DB) RegisterDataSource(name string, ds domain.DataSource) error {
	if name == "" {
		return NewError(ErrCodeInvalidParam, "datasource name cannot be empty", nil)
	}
	if ds == nil {
		return NewError(ErrCodeInvalidParam, "datasource cannot be nil", nil)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.dataSources[name]; exists {
		return NewError(ErrCodeDSAlreadyExists, "datasource '"+name+"' already exists", nil)
	}

	db.dataSources[name] = ds

	// Also register in DataSourceManager for information_schema access
	err := db.dsManager.Register(name, ds)
	if err != nil {
		// Rollback if registration fails
		delete(db.dataSources, name)
		return err
	}

	// If this is the first datasource, set it as default
	if db.defaultDS == "" {
		db.defaultDS = name
	}

	db.logger.Debug("Registered datasource: %s", name)
	return nil
}

// GetDataSource returns the datasource with the given name
func (db *DB) GetDataSource(name string) (domain.DataSource, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	ds, exists := db.dataSources[name]
	if !exists {
		return nil, NewError(ErrCodeDSNotFound, "datasource '"+name+"' not found", nil)
	}

	return ds, nil
}

// GetDefaultDataSource returns the default datasource
func (db *DB) GetDefaultDataSource() (domain.DataSource, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.defaultDS == "" {
		return nil, NewError(ErrCodeDSNotFound, "no default datasource set", nil)
	}

	ds, exists := db.dataSources[db.defaultDS]
	if !exists {
		return nil, NewError(ErrCodeDSNotFound, "default datasource '"+db.defaultDS+"' not found", nil)
	}

	return ds, nil
}

// SetDefaultDataSource sets the default datasource
func (db *DB) SetDefaultDataSource(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.dataSources[name]; !exists {
		return NewError(ErrCodeDSNotFound, "datasource '"+name+"' not found", nil)
	}

	db.defaultDS = name
	db.logger.Debug("Default datasource set to: %s", name)
	return nil
}

// Session creates a new session with the default datasource and default options
func (db *DB) Session() *Session {
	return db.SessionWithOptions(&SessionOptions{
		DataSourceName: db.defaultDS,
		Isolation:      IsolationRepeatableRead,
		ReadOnly:       false,
		CacheEnabled:   db.config.CacheEnabled,
	})
}

// SessionWithOptions creates a new session with custom options
func (db *DB) SessionWithOptions(opts *SessionOptions) *Session {
	dsName := opts.DataSourceName
	if dsName == "" {
		dsName = db.defaultDS
	}

	ds, err := db.GetDataSource(dsName)
	if err != nil {
		// If we can't get the datasource, return a session that will error on first use
		return &Session{
			db:          db,
			coreSession: nil,
			options:     opts,
			logger:      db.logger,
			err:         err,
		}
	}

	// Create CoreSession with DataSourceManager for information_schema support
	// 根据 UseEnhancedOptimizer 配置选择优化器类型
	useEnhanced := true
	if opts.UseEnhancedOptimizer != nil {
		useEnhanced = *opts.UseEnhancedOptimizer
	} else {
		if db.config != nil {
			useEnhanced = db.config.UseEnhancedOptimizer
		}
	}

	coreSession := session.NewCoreSessionWithDSManagerAndEnhanced(ds, db.dsManager, true, useEnhanced)

	// 设置查询超时 (Session级别覆盖DB级别)
	queryTimeout := opts.QueryTimeout
	if queryTimeout == 0 && db.config != nil {
		queryTimeout = db.config.QueryTimeout
	}
	coreSession.SetQueryTimeout(queryTimeout)

	apiSession := &Session{
		db:           db,
		coreSession:   coreSession,
		options:      opts,
		cacheEnabled:  opts.CacheEnabled,
		logger:       db.logger,
	}

	db.logger.Debug("Created new session for datasource: %s", dsName)
	return apiSession
}

// GetDSManager returns the DataSourceManager
func (db *DB) GetDSManager() *application.DataSourceManager {
	return db.dsManager
}

// SetLogger sets the logger for the DB object
func (db *DB) SetLogger(logger Logger) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.logger = logger
}

// GetLogger returns the current logger
func (db *DB) GetLogger() Logger {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.logger
}

// ClearCache clears the entire query cache
func (db *DB) ClearCache() {
	if db.cache != nil {
		db.cache.Clear()
		db.logger.Debug("Query cache cleared")
	}
}

// ClearTableCache clears cache entries for a specific table
func (db *DB) ClearTableCache(tableName string) {
	if db.cache != nil {
		db.cache.ClearTable(tableName)
		db.logger.Debug("Query cache cleared for table: %s", tableName)
	}
}

// GetCacheStats returns statistics about the query cache
func (db *DB) GetCacheStats() CacheStats {
	if db.cache != nil {
		return db.cache.Stats()
	}
	return CacheStats{}
}

// Close closes all datasources and releases resources
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var lastErr error
	for name, ds := range db.dataSources {
		if err := ds.Close(context.Background()); err != nil {
			lastErr = err
			db.logger.Error("Error closing datasource '%s': %v", name, err)
		}
	}

	db.dataSources = make(map[string]domain.DataSource)
	if db.cache != nil {
		db.cache.Clear()
	}
	db.logger.Info("DB closed")

	return lastErr
}

// GetDataSourceNames returns a list of all registered datasource names
func (db *DB) GetDataSourceNames() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.dataSources))
	for name := range db.dataSources {
		names = append(names, name)
	}
	return names
}
