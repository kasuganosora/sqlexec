package api

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Query executes a SELECT, SHOW, or DESCRIBE query and returns a Query object for iterating through results
func (s *Session) Query(sql string, args ...interface{}) (*Query, error) {
	s.mu.RLock()
	if s.err != nil {
		s.mu.RUnlock()
		return nil, s.err
	}
	s.mu.RUnlock()

	s.logger.Debug("Query: %s", sql)

	// Check cache if enabled
	if s.cacheEnabled {
		if result, found := s.db.cache.Get(sql, args); found {
			s.logger.Debug("Cache hit for query")
			return NewQuery(s, result, sql, args), nil
		}
	}

	// Parse and execute query
	ctx := context.Background()

	result, err := s.coreSession.ExecuteQuery(ctx, sql)
	if err != nil {
		return nil, WrapError(err, ErrCodeSyntax, "failed to execute query")
	}

	// Cache result
	if s.cacheEnabled {
		s.db.cache.Set(sql, args, result)
	}

	return NewQuery(s, result, sql, args), nil
}

// QueryAll executes a query and returns all rows at once
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
