package api

import (
	"context"
)

// Begin starts a new transaction
// If already in a transaction, returns an error (no nesting allowed)
func (s *Session) Begin() (*Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return nil, s.err
	}

	// Check if already in transaction
	if s.coreSession.InTx() {
		return nil, NewError(ErrCodeTransaction, "nested transactions are not supported", nil)
	}

	ctx := context.Background()
	tx, err := s.coreSession.BeginTx(ctx)
	if err != nil {
		return nil, WrapError(err, ErrCodeTransaction, "failed to begin transaction")
	}

	s.logger.Debug("Transaction started")

	transaction := &Transaction{
		session: s,
		tx:      tx,
		active:  true,
	}

	return transaction, nil
}

// InTransaction returns true if session is currently in a transaction
func (s *Session) InTransaction() bool {
	return s.coreSession.InTx()
}

// IsolationLevel returns current transaction isolation level
func (s *Session) IsolationLevel() IsolationLevel {
	if s.options == nil {
		return IsolationRepeatableRead
	}
	return s.options.Isolation
}

// SetIsolationLevel sets transaction isolation level for new transactions
func (s *Session) SetIsolationLevel(level IsolationLevel) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.options == nil {
		s.options = &SessionOptions{}
	}
	s.options.Isolation = level

	s.logger.Debug("Isolation level set to: %s", level.String())
}
