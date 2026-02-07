package api

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// SelectStmt interface for accessing Select statement
type SelectStmt interface {
	GetFrom() string
}

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

	// Parse and execute query (使用 context.Background,超时由CoreSession内部处理)
	ctx := context.Background()

	result, err := s.coreSession.ExecuteQuery(ctx, boundSQL)
	if err != nil {
		// 检查错误类型并返回适当的错误码
		if err.Error() == "query execution timed out" || err.Error() == "query was killed" {
			return nil, WrapError(err, ErrCodeTimeout, "failed to execute query")
		}
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

// Explain executes an EXPLAIN statement and returns execution plan
// Supports parameter binding with ? placeholders
// Example: session.Explain("SELECT * FROM users WHERE id = ?", 1)
// This uses actual optimizer to generate execution plans
func (s *Session) Explain(sql string, args ...interface{}) (string, error) {
	s.mu.RLock()
	if s.err != nil {
		s.mu.RUnlock()
		return "", s.err
	}
	s.mu.RUnlock()

	// Bind parameters if provided
	boundSQL := sql
	if len(args) > 0 {
		var err error
		boundSQL, err = bindParams(sql, args)
		if err != nil {
			return "", WrapError(err, ErrCodeInvalidParam, "failed to bind parameters")
		}
	}

	s.logger.Debug("Explain: %s", boundSQL)

	// Parse SQL
	parseResult, err := s.coreSession.GetAdapter().Parse(boundSQL)
	if err != nil {
		return "", WrapError(err, ErrCodeSyntax, "failed to parse SQL")
	}

	if !parseResult.Success {
		return "", NewError(ErrCodeSyntax, "SQL parse error: "+parseResult.Error, nil)
	}

	// Explain only works with SELECT statements
	if parseResult.Statement.Type != parser.SQLTypeSelect || parseResult.Statement.Select == nil {
		return "", NewError(ErrCodeSyntax, "EXPLAIN only supports SELECT statements", nil)
	}

	// Check cache if enabled
	cacheKey := "EXPLAIN " + boundSQL
	if s.cacheEnabled {
		if explain, found := s.db.cache.GetExplain(cacheKey); found {
			s.logger.Debug("Cache hit for explain")
			return explain, nil
		}
	}

	// Get optimizer from executor
	executor := s.coreSession.GetExecutor()
	if executor == nil {
		return "", NewError(ErrCodeInternal, "executor not available", nil)
	}

	enhancedOptimizer := executor.GetOptimizer()
	if enhancedOptimizer == nil {
		return "", NewError(ErrCodeInternal, "optimizer not available", nil)
	}

	// Build SQLStatement for optimizer
	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: parseResult.Statement.Select,
	}

	// Optimize to get physical plan using EnhancedOptimizer
	ctx := context.Background()
	physicalPlan, err := enhancedOptimizer.Optimize(ctx, sqlStmt)
	if err != nil {
		return "", WrapError(err, ErrCodeInternal, "failed to generate execution plan")
	}

	if physicalPlan == nil {
		return "", NewError(ErrCodeInternal, "generated physical plan is nil", nil)
	}

	// Generate execution plan using ExplainPlan
	output := "Query Execution Plan\n====================\n\n"
	output += fmt.Sprintf("SQL: %s\n\n", boundSQL)
	output += optimizer.ExplainPlanV2(physicalPlan)

	// Cache explain result
	if s.cacheEnabled {
		s.db.cache.SetExplain(cacheKey, output)
	}

	return output, nil
}
