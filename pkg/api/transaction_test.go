package api

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewTransaction(t *testing.T) {
	session := &Session{}
	tx := newMockTransaction()

	transaction := NewTransaction(session, tx)

	assert.NotNil(t, transaction)
	assert.Equal(t, session, transaction.session)
	assert.Equal(t, tx, transaction.tx)
	assert.True(t, transaction.active)
}

func TestTransaction_Query(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)

	// Query on active transaction
	query, err := transaction.Query("SELECT * FROM users")
	assert.NoError(t, err)
	assert.NotNil(t, query)
}

func TestTransaction_Query_ParsesTableName(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	query, err := transaction.Query("SELECT * FROM users")
	assert.NoError(t, err)
	assert.NotNil(t, query)

	// Verify the parser extracted table name "users"
	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.Equal(t, "users", rtx.queryCalls[0].tableName)
}

func TestTransaction_Query_WithWhereClause(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	query, err := transaction.Query("SELECT * FROM orders WHERE status = 'active'")
	assert.NoError(t, err)
	assert.NotNil(t, query)

	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.Equal(t, "orders", rtx.queryCalls[0].tableName)
	// Verify filters were extracted from WHERE clause
	assert.NotNil(t, rtx.queryCalls[0].options)
	assert.True(t, len(rtx.queryCalls[0].options.Filters) > 0)
	assert.Equal(t, "status", rtx.queryCalls[0].options.Filters[0].Field)
	assert.Equal(t, "=", rtx.queryCalls[0].options.Filters[0].Operator)
	assert.Equal(t, "active", rtx.queryCalls[0].options.Filters[0].Value)
}

func TestTransaction_Query_WithParameterBinding(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	query, err := transaction.Query("SELECT * FROM users WHERE id = ?", 42)
	assert.NoError(t, err)
	assert.NotNil(t, query)

	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.Equal(t, "users", rtx.queryCalls[0].tableName)
}

func TestTransaction_Query_WithLimitOffset(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	query, err := transaction.Query("SELECT * FROM products LIMIT 10 OFFSET 5")
	assert.NoError(t, err)
	assert.NotNil(t, query)

	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.Equal(t, "products", rtx.queryCalls[0].tableName)
	assert.Equal(t, 10, rtx.queryCalls[0].options.Limit)
	assert.Equal(t, 5, rtx.queryCalls[0].options.Offset)
}

func TestTransaction_Query_WithOrderBy(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	query, err := transaction.Query("SELECT * FROM products ORDER BY name ASC")
	assert.NoError(t, err)
	assert.NotNil(t, query)

	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.Equal(t, "products", rtx.queryCalls[0].tableName)
	assert.Equal(t, "name", rtx.queryCalls[0].options.OrderBy)
	assert.Equal(t, "ASC", rtx.queryCalls[0].options.Order)
}

func TestTransaction_Query_WithSelectColumns(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	query, err := transaction.Query("SELECT name, email FROM users")
	assert.NoError(t, err)
	assert.NotNil(t, query)

	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.Equal(t, "users", rtx.queryCalls[0].tableName)
	assert.False(t, rtx.queryCalls[0].options.SelectAll)
	assert.Contains(t, rtx.queryCalls[0].options.SelectColumns, "name")
	assert.Contains(t, rtx.queryCalls[0].options.SelectColumns, "email")
}

func TestTransaction_Query_SelectAll(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	query, err := transaction.Query("SELECT * FROM users")
	assert.NoError(t, err)
	assert.NotNil(t, query)

	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.True(t, rtx.queryCalls[0].options.SelectAll)
}

func TestTransaction_Query_InvalidSQL(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	_, err := transaction.Query("NOT VALID SQL AT ALL !!!")
	assert.Error(t, err)
}

func TestTransaction_Query_NonSelectSQL(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	_, err := transaction.Query("INSERT INTO users VALUES (1)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SELECT")
}

func TestTransaction_Execute_Insert(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	result, err := transaction.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowsAffected)

	// Verify Insert was called with correct table name and row data
	assert.Equal(t, 1, len(rtx.insertCalls))
	assert.Equal(t, "users", rtx.insertCalls[0].tableName)
	assert.Equal(t, 1, len(rtx.insertCalls[0].rows))
	assert.Equal(t, float64(1), rtx.insertCalls[0].rows[0]["id"])
	assert.Equal(t, "Alice", rtx.insertCalls[0].rows[0]["name"])
}

func TestTransaction_Execute_InsertMultipleRows(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	result, err := transaction.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(2), result.RowsAffected)

	assert.Equal(t, 1, len(rtx.insertCalls))
	assert.Equal(t, "users", rtx.insertCalls[0].tableName)
	assert.Equal(t, 2, len(rtx.insertCalls[0].rows))
}

func TestTransaction_Execute_Update(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	result, err := transaction.Execute("UPDATE users SET name = 'Bob' WHERE id = 1")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowsAffected)

	// Verify Update was called with correct table name, filters and updates
	assert.Equal(t, 1, len(rtx.updateCalls))
	assert.Equal(t, "users", rtx.updateCalls[0].tableName)
	assert.Equal(t, "Bob", rtx.updateCalls[0].updates["name"])
	assert.True(t, len(rtx.updateCalls[0].filters) > 0)
	assert.Equal(t, "id", rtx.updateCalls[0].filters[0].Field)
}

func TestTransaction_Execute_Delete(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	result, err := transaction.Execute("DELETE FROM users WHERE id = 1")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowsAffected)

	// Verify Delete was called with correct table name and filters
	assert.Equal(t, 1, len(rtx.deleteCalls))
	assert.Equal(t, "users", rtx.deleteCalls[0].tableName)
	assert.True(t, len(rtx.deleteCalls[0].filters) > 0)
	assert.Equal(t, "id", rtx.deleteCalls[0].filters[0].Field)
}

func TestTransaction_Execute_DeleteNoWhere(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	result, err := transaction.Execute("DELETE FROM users")
	assert.NoError(t, err)
	assert.NotNil(t, result)

	assert.Equal(t, 1, len(rtx.deleteCalls))
	assert.Equal(t, "users", rtx.deleteCalls[0].tableName)
	assert.Equal(t, 0, len(rtx.deleteCalls[0].filters))
}

func TestTransaction_Execute_WithParameterBinding(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	result, err := transaction.Execute("DELETE FROM users WHERE id = ?", 42)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	assert.Equal(t, 1, len(rtx.deleteCalls))
	assert.Equal(t, "users", rtx.deleteCalls[0].tableName)
}

func TestTransaction_Execute_InvalidSQL(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	_, err := transaction.Execute("NOT VALID SQL AT ALL !!!")
	assert.Error(t, err)
}

func TestTransaction_Execute_UnsupportedType(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	// SELECT is not supported in Execute
	_, err := transaction.Execute("SELECT * FROM users")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not support SELECT")
}

func TestTransaction_QueryNotActive(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)
	transaction.active = false

	// Query on inactive transaction should error
	_, err := transaction.Query("SELECT * FROM users")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not active")
}

func TestTransaction_Execute(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	// Execute INSERT on active transaction
	result, err := transaction.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowsAffected)
}

func TestTransaction_ExecuteNotActive(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)
	transaction.active = false

	_, err := transaction.Execute("INSERT INTO users VALUES (1)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not active")
}

func TestTransaction_Commit(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)

	// Commit should succeed
	err := transaction.Commit()
	assert.NoError(t, err)
	assert.False(t, transaction.active)
}

func TestTransaction_CommitNotActive(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)
	transaction.active = false

	// Commit on inactive transaction should error
	err := transaction.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not active")
}

func TestTransaction_CommitWithError(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransactionWithErrors(NewError(ErrCodeTransaction, "commit failed", nil), nil)
	transaction := NewTransaction(session, tx)

	// Commit with error should return error
	err := transaction.Commit()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	// When commit fails, transaction remains active
	assert.True(t, transaction.active)
}

func TestTransaction_Rollback(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)

	// Rollback should succeed
	err := transaction.Rollback()
	assert.NoError(t, err)
	assert.False(t, transaction.active)
}

func TestTransaction_RollbackNotActive(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)
	transaction.active = false

	// Rollback on inactive transaction should error
	err := transaction.Rollback()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not active")
}

func TestTransaction_RollbackWithError(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransactionWithErrors(nil, NewError(ErrCodeTransaction, "rollback failed", nil))
	transaction := NewTransaction(session, tx)

	// Rollback with error should return error
	err := transaction.Rollback()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rollback failed")
	// When rollback fails, transaction remains active
	assert.True(t, transaction.active)
}

func TestTransaction_Close(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)

	// Close should rollback active transaction
	err := transaction.Close()
	assert.NoError(t, err)
	assert.False(t, transaction.active)
}

func TestTransaction_CloseAlreadyClosed(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)
	transaction.active = false

	// Close on already inactive transaction should return nil
	err := transaction.Close()
	assert.NoError(t, err)
}

func TestTransaction_IsActive(t *testing.T) {
	session := &Session{}
	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)

	// Initially active
	assert.True(t, transaction.IsActive())

	// After commit, should be inactive
	_ = transaction.Commit()
	assert.False(t, transaction.IsActive())
}

// Mock transaction for testing with custom errors
type mockTransactionWithErrors struct {
	commitErr   error
	rollbackErr error
}

func newMockTransactionWithErrors(commitErr, rollbackErr error) *mockTransactionWithErrors {
	return &mockTransactionWithErrors{
		commitErr:   commitErr,
		rollbackErr: rollbackErr,
	}
}

func (m *mockTransactionWithErrors) Commit(ctx context.Context) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	return nil
}

func (m *mockTransactionWithErrors) Rollback(ctx context.Context) error {
	if m.rollbackErr != nil {
		return m.rollbackErr
	}
	return nil
}

func (m *mockTransactionWithErrors) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}

func (m *mockTransactionWithErrors) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}

func (m *mockTransactionWithErrors) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}

func (m *mockTransactionWithErrors) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}

func (m *mockTransactionWithErrors) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}

// Recording mock transaction that captures call arguments for verification
type queryCall struct {
	tableName string
	options   *domain.QueryOptions
}

type insertCall struct {
	tableName string
	rows      []domain.Row
}

type updateCall struct {
	tableName string
	filters   []domain.Filter
	updates   domain.Row
}

type deleteCall struct {
	tableName string
	filters   []domain.Filter
}

type recordingMockTransaction struct {
	queryCalls  []queryCall
	insertCalls []insertCall
	updateCalls []updateCall
	deleteCalls []deleteCall
}

func newRecordingMockTransaction() *recordingMockTransaction {
	return &recordingMockTransaction{}
}

func (m *recordingMockTransaction) Commit(ctx context.Context) error {
	return nil
}

func (m *recordingMockTransaction) Rollback(ctx context.Context) error {
	return nil
}

func (m *recordingMockTransaction) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return &domain.QueryResult{}, nil
}

func (m *recordingMockTransaction) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	m.queryCalls = append(m.queryCalls, queryCall{tableName: tableName, options: options})
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

func (m *recordingMockTransaction) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	m.insertCalls = append(m.insertCalls, insertCall{tableName: tableName, rows: rows})
	return int64(len(rows)), nil
}

func (m *recordingMockTransaction) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	m.updateCalls = append(m.updateCalls, updateCall{tableName: tableName, filters: filters, updates: updates})
	return 1, nil
}

func (m *recordingMockTransaction) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	m.deleteCalls = append(m.deleteCalls, deleteCall{tableName: tableName, filters: filters})
	return 1, nil
}

// ---------------------------------------------------------------------------
// Tests for expressionToFilters
// ---------------------------------------------------------------------------

func TestExpressionToFilters_SimpleComparison(t *testing.T) {
	// column = value  =>  single filter {Field:"column", Operator:"=", Value:"hello"}
	expr := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "=",
		Left: &parser.Expression{
			Type:   parser.ExprTypeColumn,
			Column: "column",
		},
		Right: &parser.Expression{
			Type:  parser.ExprTypeValue,
			Value: "hello",
		},
	}

	filters := expressionToFilters(expr)
	assert.Equal(t, 1, len(filters))
	assert.Equal(t, "column", filters[0].Field)
	assert.Equal(t, "=", filters[0].Operator)
	assert.Equal(t, "hello", filters[0].Value)
}

func TestExpressionToFilters_AndExpression(t *testing.T) {
	// a = 1 AND b = 2  =>  flattened into 2 filters
	expr := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "AND",
		Left: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "=",
			Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"},
			Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: float64(1)},
		},
		Right: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "=",
			Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "b"},
			Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: float64(2)},
		},
	}

	filters := expressionToFilters(expr)
	assert.Equal(t, 2, len(filters))
	assert.Equal(t, "a", filters[0].Field)
	assert.Equal(t, "=", filters[0].Operator)
	assert.Equal(t, float64(1), filters[0].Value)
	assert.Equal(t, "b", filters[1].Field)
	assert.Equal(t, "=", filters[1].Operator)
	assert.Equal(t, float64(2), filters[1].Value)
}

func TestExpressionToFilters_OrExpression(t *testing.T) {
	// a = 1 OR b = 2  =>  single filter with Logic="OR" and 2 SubFilters
	expr := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "OR",
		Left: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "=",
			Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"},
			Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: float64(1)},
		},
		Right: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "=",
			Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "b"},
			Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: float64(2)},
		},
	}

	filters := expressionToFilters(expr)
	assert.Equal(t, 1, len(filters))
	assert.Equal(t, "OR", filters[0].Logic)
	assert.Equal(t, 2, len(filters[0].SubFilters))
	assert.Equal(t, "a", filters[0].SubFilters[0].Field)
	assert.Equal(t, "=", filters[0].SubFilters[0].Operator)
	assert.Equal(t, float64(1), filters[0].SubFilters[0].Value)
	assert.Equal(t, "b", filters[0].SubFilters[1].Field)
	assert.Equal(t, "=", filters[0].SubFilters[1].Operator)
	assert.Equal(t, float64(2), filters[0].SubFilters[1].Value)
}

func TestExpressionToFilters_IsNull(t *testing.T) {
	// column IS NULL  =>  filter with Operator="IS NULL"
	expr := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "IS NULL",
		Left: &parser.Expression{
			Type:   parser.ExprTypeColumn,
			Column: "column",
		},
		Right: nil,
	}

	filters := expressionToFilters(expr)
	assert.Equal(t, 1, len(filters))
	assert.Equal(t, "column", filters[0].Field)
	assert.Equal(t, "IS NULL", filters[0].Operator)
	assert.Nil(t, filters[0].Value)
}

func TestExpressionToFilters_IsNotNull(t *testing.T) {
	// column IS NOT NULL  =>  filter with Operator="IS NOT NULL"
	expr := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "IS NOT NULL",
		Left: &parser.Expression{
			Type:   parser.ExprTypeColumn,
			Column: "column",
		},
		Right: nil,
	}

	filters := expressionToFilters(expr)
	assert.Equal(t, 1, len(filters))
	assert.Equal(t, "column", filters[0].Field)
	assert.Equal(t, "IS NOT NULL", filters[0].Operator)
	assert.Nil(t, filters[0].Value)
}

func TestExpressionToFilters_NestedAndOr(t *testing.T) {
	// (a = 1 AND b = 2) OR c = 3
	// The top-level is OR, so we get a single filter with Logic="OR".
	// The left side (AND) flattens into 2 sub-filters, plus c = 3 makes 3 total.
	expr := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "OR",
		Left: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "AND",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"},
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: float64(1)},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "=",
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "b"},
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: float64(2)},
			},
		},
		Right: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "=",
			Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "c"},
			Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: float64(3)},
		},
	}

	filters := expressionToFilters(expr)
	assert.Equal(t, 1, len(filters))
	assert.Equal(t, "OR", filters[0].Logic)
	// Left AND flattens to 2 + right 1 = 3 sub-filters
	assert.Equal(t, 3, len(filters[0].SubFilters))
	assert.Equal(t, "a", filters[0].SubFilters[0].Field)
	assert.Equal(t, "b", filters[0].SubFilters[1].Field)
	assert.Equal(t, "c", filters[0].SubFilters[2].Field)
}

func TestExpressionToFilters_NilExpression(t *testing.T) {
	filters := expressionToFilters(nil)
	assert.Nil(t, filters)
}

// ---------------------------------------------------------------------------
// Tests for mapParserOperator
// ---------------------------------------------------------------------------

func TestMapParserOperator_AllCases(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"eq", "="},
		{"===", "="},
		{"ne", "!="},
		{"gt", ">"},
		{"gte", ">="},
		{"lt", "<"},
		{"lte", "<="},
		{"like", "LIKE"},
		{"not like", "NOT LIKE"},
		{"notlike", "NOT LIKE"},
		{"in", "IN"},
		{"not in", "NOT IN"},
		{"notin", "NOT IN"},
		{"between", "BETWEEN"},
		// Unknown operators should be returned unchanged
		{"=", "="},
		{"!=", "!="},
		{"CUSTOM_OP", "CUSTOM_OP"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := mapParserOperator(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ---------------------------------------------------------------------------
// Tests for Transaction.Query with multiple ORDER BY
// ---------------------------------------------------------------------------

func TestTransaction_Query_WithMultipleOrderBy(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	// SQL with two ORDER BY columns; only the first should be applied
	query, err := transaction.Query("SELECT * FROM products ORDER BY name ASC, price DESC")
	assert.NoError(t, err)
	assert.NotNil(t, query)

	assert.Equal(t, 1, len(rtx.queryCalls))
	assert.Equal(t, "products", rtx.queryCalls[0].tableName)
	// Only the first ORDER BY item is used
	assert.Equal(t, "name", rtx.queryCalls[0].options.OrderBy)
	assert.Equal(t, "ASC", rtx.queryCalls[0].options.Order)
}

// ---------------------------------------------------------------------------
// Tests for Transaction.Execute INSERT with explicit columns
// ---------------------------------------------------------------------------

func TestTransaction_Execute_InsertWithColumns(t *testing.T) {
	session := &Session{
		logger: NewNoOpLogger(),
	}

	rtx := newRecordingMockTransaction()
	transaction := NewTransaction(session, rtx)

	result, err := transaction.Execute("INSERT INTO employees (emp_id, first_name, last_name) VALUES (100, 'John', 'Doe')")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowsAffected)

	// Verify Insert was called with the correct table and column mapping
	assert.Equal(t, 1, len(rtx.insertCalls))
	assert.Equal(t, "employees", rtx.insertCalls[0].tableName)
	assert.Equal(t, 1, len(rtx.insertCalls[0].rows))

	row := rtx.insertCalls[0].rows[0]
	assert.Equal(t, float64(100), row["emp_id"])
	assert.Equal(t, "John", row["first_name"])
	assert.Equal(t, "Doe", row["last_name"])
}
