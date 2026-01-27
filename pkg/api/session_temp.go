package api

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// CreateTempTable creates a temporary table in this session
// Temporary tables are automatically dropped when session is closed
func (s *Session) CreateTempTable(name string, schema *domain.TableInfo) error {
	if name == "" {
		return NewError(ErrCodeInvalidParam, "table name cannot be empty", nil)
	}
	if schema == nil {
		return NewError(ErrCodeInvalidParam, "table schema cannot be nil", nil)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}

	schema.Temporary = true
	schema.Name = name

	ds := s.coreSession.GetDataSource()
	if err := ds.CreateTable(context.Background(), schema); err != nil {
		return WrapError(err, ErrCodeInternal, "failed to create temporary table")
	}

	s.coreSession.AddTempTable(name)
	s.logger.Debug("Created temporary table: %s", name)

	return nil
}
