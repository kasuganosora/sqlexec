package api

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Transaction 事务对象（不支持嵌套）
type Transaction struct {
	session *Session
	tx      domain.Transaction
	active  bool
	mu      sync.Mutex
}

// NewTransaction 创建 Transaction
func NewTransaction(session *Session, tx domain.Transaction) *Transaction {
	return &Transaction{
		session: session,
		tx:      tx,
		active:  true,
	}
}

// Query 事务内查询
// Supports parameter binding with ? placeholders
func (t *Transaction) Query(sql string, args ...interface{}) (*Query, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil, NewError(ErrCodeTransaction, "transaction is not active", nil)
	}

	// Bind parameters if provided
	boundSQL := sql
	if len(args) > 0 {
		var err error
		boundSQL, err = bindParams(sql, args)
		if err != nil {
			return nil, WrapError(err, ErrCodeInvalidParam, "failed to bind parameters")
		}
	}

	// Parse SQL to extract table name and query options
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(boundSQL)
	if err != nil {
		return nil, WrapError(err, ErrCodeSyntax, "failed to parse SQL")
	}
	if !parseResult.Success {
		return nil, NewError(ErrCodeSyntax, "SQL parse error: "+parseResult.Error, nil)
	}
	if parseResult.Statement.Type != parser.SQLTypeSelect || parseResult.Statement.Select == nil {
		return nil, NewError(ErrCodeInvalidParam, "transaction.Query only supports SELECT statements", nil)
	}

	selectStmt := parseResult.Statement.Select
	tableName := selectStmt.From
	options := &domain.QueryOptions{}

	// Extract WHERE filters
	if selectStmt.Where != nil {
		options.Filters = expressionToFilters(selectStmt.Where)
	}

	// Extract ORDER BY
	if len(selectStmt.OrderBy) > 0 {
		options.OrderBy = selectStmt.OrderBy[0].Column
		options.Order = selectStmt.OrderBy[0].Direction
	}

	// Extract LIMIT and OFFSET
	if selectStmt.Limit != nil {
		options.Limit = int(*selectStmt.Limit)
	}
	if selectStmt.Offset != nil {
		options.Offset = int(*selectStmt.Offset)
	}

	// Extract select columns
	hasWildcard := false
	for _, col := range selectStmt.Columns {
		if col.IsWildcard {
			hasWildcard = true
			break
		}
	}
	if hasWildcard {
		options.SelectAll = true
	} else {
		cols := make([]string, 0, len(selectStmt.Columns))
		for _, col := range selectStmt.Columns {
			if col.Alias != "" {
				cols = append(cols, col.Alias)
			} else {
				cols = append(cols, col.Name)
			}
		}
		options.SelectColumns = cols
	}

	result, err := t.tx.Query(context.Background(), tableName, options)
	if err != nil {
		return nil, WrapError(err, ErrCodeTransaction, "transaction query failed")
	}

	return NewQuery(t.session, result, boundSQL, nil), nil
}

// Execute 事务内执行命令
// Supports parameter binding with ? placeholders
func (t *Transaction) Execute(sql string, args ...interface{}) (*Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil, NewError(ErrCodeTransaction, "transaction is not active", nil)
	}

	// Bind parameters if provided
	boundSQL := sql
	if len(args) > 0 {
		var err error
		boundSQL, err = bindParams(sql, args)
		if err != nil {
			return nil, WrapError(err, ErrCodeInvalidParam, "failed to bind parameters")
		}
	}

	// Parse SQL to determine statement type and extract parameters
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(boundSQL)
	if err != nil {
		return nil, WrapError(err, ErrCodeSyntax, "failed to parse SQL")
	}
	if !parseResult.Success {
		return nil, NewError(ErrCodeSyntax, "SQL parse error: "+parseResult.Error, nil)
	}

	ctx := context.Background()

	switch parseResult.Statement.Type {
	case parser.SQLTypeInsert:
		insertStmt := parseResult.Statement.Insert
		if insertStmt == nil {
			return nil, NewError(ErrCodeSyntax, "invalid INSERT statement", nil)
		}
		// Convert parsed values to domain.Row slice
		rows := make([]domain.Row, 0, len(insertStmt.Values))
		for _, vals := range insertStmt.Values {
			row := domain.Row{}
			for i, val := range vals {
				if i < len(insertStmt.Columns) {
					row[insertStmt.Columns[i]] = val
				}
			}
			rows = append(rows, row)
		}
		affected, err := t.tx.Insert(ctx, insertStmt.Table, rows, nil)
		if err != nil {
			return nil, WrapError(err, ErrCodeTransaction, "transaction insert failed")
		}
		return NewResult(affected, 0, nil), nil

	case parser.SQLTypeUpdate:
		updateStmt := parseResult.Statement.Update
		if updateStmt == nil {
			return nil, NewError(ErrCodeSyntax, "invalid UPDATE statement", nil)
		}
		// Convert SET clause to domain.Row
		updates := domain.Row{}
		for col, val := range updateStmt.Set {
			updates[col] = val
		}
		// Convert WHERE clause to filters
		var filters []domain.Filter
		if updateStmt.Where != nil {
			filters = expressionToFilters(updateStmt.Where)
		}
		affected, err := t.tx.Update(ctx, updateStmt.Table, filters, updates, nil)
		if err != nil {
			return nil, WrapError(err, ErrCodeTransaction, "transaction update failed")
		}
		return NewResult(affected, 0, nil), nil

	case parser.SQLTypeDelete:
		deleteStmt := parseResult.Statement.Delete
		if deleteStmt == nil {
			return nil, NewError(ErrCodeSyntax, "invalid DELETE statement", nil)
		}
		// Convert WHERE clause to filters
		var filters []domain.Filter
		if deleteStmt.Where != nil {
			filters = expressionToFilters(deleteStmt.Where)
		}
		affected, err := t.tx.Delete(ctx, deleteStmt.Table, filters, nil)
		if err != nil {
			return nil, WrapError(err, ErrCodeTransaction, "transaction delete failed")
		}
		return NewResult(affected, 0, nil), nil

	default:
		return nil, NewError(ErrCodeNotSupported,
			fmt.Sprintf("transaction.Execute does not support %s statements", parseResult.Statement.Type), nil)
	}
}

// Commit 提交事务
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return NewError(ErrCodeTransaction, "transaction is not active", nil)
	}

	err := t.tx.Commit(context.Background())
	if err != nil {
		return WrapError(err, ErrCodeTransaction, "commit failed")
	}

	t.active = false

	if t.session.logger != nil {
		t.session.logger.Debug("[TX] Transaction committed")
	}

	return nil
}

// Rollback 回滚事务
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return NewError(ErrCodeTransaction, "transaction is not active", nil)
	}

	err := t.tx.Rollback(context.Background())
	if err != nil {
		return WrapError(err, ErrCodeTransaction, "rollback failed")
	}

	t.active = false

	if t.session.logger != nil {
		t.session.logger.Warn("[TX] Transaction rolled back")
	}

	return nil
}

// Close 关闭事务（等同于 Rollback）
func (t *Transaction) Close() error {
	t.mu.Lock()
	active := t.active
	t.mu.Unlock()
	if active {
		return t.Rollback()
	}
	return nil
}

// IsActive 检查事务是否活跃
func (t *Transaction) IsActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}

// expressionToFilters converts a parser.Expression tree into a slice of domain.Filter.
// It handles AND expressions by flattening them, and converts binary comparison
// expressions (column op value) into individual filters.
func expressionToFilters(expr *parser.Expression) []domain.Filter {
	if expr == nil {
		return nil
	}

	// If it's an AND operator, recursively flatten both sides
	if expr.Type == parser.ExprTypeOperator && strings.EqualFold(expr.Operator, "and") {
		var filters []domain.Filter
		if expr.Left != nil {
			filters = append(filters, expressionToFilters(expr.Left)...)
		}
		if expr.Right != nil {
			filters = append(filters, expressionToFilters(expr.Right)...)
		}
		return filters
	}

	// If it's an OR operator, create a nested filter with Logic="OR"
	if expr.Type == parser.ExprTypeOperator && strings.EqualFold(expr.Operator, "or") {
		var subFilters []domain.Filter
		if expr.Left != nil {
			subFilters = append(subFilters, expressionToFilters(expr.Left)...)
		}
		if expr.Right != nil {
			subFilters = append(subFilters, expressionToFilters(expr.Right)...)
		}
		return []domain.Filter{
			{
				Logic:      "OR",
				SubFilters: subFilters,
			},
		}
	}

	// Handle unary operators: IS NULL, IS NOT NULL
	if expr.Type == parser.ExprTypeOperator && expr.Left != nil && expr.Right == nil {
		op := strings.ToUpper(expr.Operator)
		if op == "IS NULL" || op == "ISNULL" || op == "IS NOT NULL" || op == "ISNOTNULL" {
			if expr.Left.Type == parser.ExprTypeColumn && expr.Left.Column != "" {
				return []domain.Filter{
					{
						Field:    expr.Left.Column,
						Operator: op,
						Value:    nil,
					},
				}
			}
		}
	}

	// Handle binary comparison: column op value
	if expr.Type == parser.ExprTypeOperator && expr.Left != nil && expr.Right != nil {
		if expr.Left.Type == parser.ExprTypeColumn && expr.Left.Column != "" {
			if expr.Right.Type == parser.ExprTypeValue {
				operator := mapParserOperator(expr.Operator)
				return []domain.Filter{
					{
						Field:    expr.Left.Column,
						Operator: operator,
						Value:    expr.Right.Value,
					},
				}
			}
		}
	}

	return nil
}

// mapParserOperator converts parser operator strings to domain filter operator strings.
func mapParserOperator(op string) string {
	switch strings.ToLower(op) {
	case "eq", "===":
		return "="
	case "ne":
		return "!="
	case "gt":
		return ">"
	case "gte":
		return ">="
	case "lt":
		return "<"
	case "lte":
		return "<="
	case "like":
		return "LIKE"
	case "not like", "notlike":
		return "NOT LIKE"
	case "in":
		return "IN"
	case "not in", "notin":
		return "NOT IN"
	case "between":
		return "BETWEEN"
	default:
		return op
	}
}
