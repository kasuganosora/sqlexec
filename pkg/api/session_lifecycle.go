package api

import (
	"context"
)

// GetDB returns DB object that created this session
func (s *Session) GetDB() *DB {
	return s.db
}

// SetCurrentDB sets the current database for this session
// This is used by the MySQL protocol handler when COM_INIT_DB is received
func (s *Session) SetCurrentDB(dbName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.coreSession != nil {
		s.coreSession.SetCurrentDB(dbName)
	}

	// 同时更新缓存的数据库上下文，确保缓存键包含正确的数据库
	if s.db != nil && s.db.cache != nil {
		s.db.cache.SetCurrentDB(dbName)
	}
}

// SetConfigDir sets the config directory for the config virtual database
func (s *Session) SetConfigDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.coreSession != nil {
		s.coreSession.SetConfigDir(dir)
	}
}

// GetCurrentDB returns the current database for this session
func (s *Session) GetCurrentDB() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.coreSession != nil {
		return s.coreSession.GetCurrentDB()
	}
	return ""
}

// Close closes the session and releases resources
// Temporary tables created in this session are automatically dropped
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.coreSession == nil {
		return nil
	}

	ctx := context.Background()
	core := s.coreSession

	// Rollback any active transaction
	if core.InTx() {
		s.logger.Warn("Rolling back uncommitted transaction")
		if err := core.RollbackTx(ctx); err != nil {
			s.logger.Error("Failed to rollback transaction: %v", err)
		}
	}

	// Drop temporary tables
	tempTables := core.GetTempTables()
	ds := core.GetDataSource()
	for _, tableName := range tempTables {
		if err := ds.DropTable(ctx, tableName); err != nil {
			s.logger.Error("Failed to drop temporary table '%s': %v", tableName, err)
		}
	}

	// Close core session
	if err := core.Close(ctx); err != nil {
		return WrapError(err, ErrCodeInternal, "failed to close session")
	}

	s.logger.Debug("Session closed")
	return nil
}
