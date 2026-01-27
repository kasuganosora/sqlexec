package api

import (
	"context"
	"testing"

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

	tx := newMockTransaction()
	transaction := NewTransaction(session, tx)

	// Execute on active transaction
	// Note: Currently returns "not fully implemented" error
	_, err := transaction.Execute("INSERT INTO users VALUES (1, 'Alice')")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not fully implemented")
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
