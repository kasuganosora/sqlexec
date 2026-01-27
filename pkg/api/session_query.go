package api

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Query executes a SELECT, SHOW, or DESCRIBE query and returns a Query object for iterating through results
// Supports parameter binding with ? placeholders
// Example: session.Query("SELECT * FROM users WHERE id = ?", 1)
func (s *Session) Query(sql string, args ...interface{}) (*Query, error) {
	s.mu.RLock()
	if s.err != nil {
		s.mu.RUnlock()
		return nil, s.err
	}
	s.mu.RUnlock()

	// Bind parameters if provided
	boundSQL := sql
	if len(args) > 0 {
		var err error
		boundSQL, err = bindParams(sql, args)
		if err != nil {
			return nil, WrapError(err, ErrCodeInvalidParam, "failed to bind parameters")
		}
	}

	s.logger.Debug("Query: %s", boundSQL)

	// Check cache if enabled
	if s.cacheEnabled {
		if result, found := s.db.cache.Get(boundSQL, nil); found {
			s.logger.Debug("Cache hit for query")
			return NewQuery(s, result, boundSQL, nil), nil
		}
	}

	// Parse and execute query
	ctx := context.Background()

	result, err := s.coreSession.ExecuteQuery(ctx, boundSQL)
	if err != nil {
		return nil, WrapError(err, ErrCodeSyntax, "failed to execute query")
	}

	// Cache result (with bound SQL, not original)
	if s.cacheEnabled {
		s.db.cache.Set(boundSQL, nil, result)
	}

	return NewQuery(s, result, boundSQL, nil), nil
}

// QueryAll executes a query and returns all rows at once
// Supports parameter binding with ? placeholders
func (s *Session) QueryAll(sql string, args ...interface{}) ([]domain.Row, error) {
	query, err := s.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	var rows []domain.Row
	for query.Next() {
		rows = append(rows, query.Row())
	}

	if query.Err() != nil {
		return nil, query.Err()
	}

	return rows, nil
}

// QueryOne executes a query and returns first row only
// Supports parameter binding with ? placeholders
func (s *Session) QueryOne(sql string, args ...interface{}) (domain.Row, error) {
	query, err := s.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer query.Close()

	if !query.Next() {
		return nil, NewError(ErrCodeInternal, "no rows found", nil)
	}

	return query.Row(), nil
}
