package api

import (
	"context"
)

// GetDB returns DB object that created this session
func (s *Session) GetDB() *DB {
	return s.db
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
