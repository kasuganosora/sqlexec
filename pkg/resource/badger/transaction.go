package badger

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TransactionManager manages Badger transactions
type TransactionManager struct {
	mu     sync.RWMutex
	db     *badger.DB
	txns   map[int64]*Transaction
	nextID int64
}

// NewTransactionManager creates a new TransactionManager
func NewTransactionManager(db *badger.DB) *TransactionManager {
	return &TransactionManager{
		db:   db,
		txns: make(map[int64]*Transaction),
	}
}

// Begin starts a new transaction
func (m *TransactionManager) Begin(readOnly bool) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := atomic.AddInt64(&m.nextID, 1)
	txn := m.db.NewTransaction(!readOnly)

	t := &Transaction{
		ID:        id,
		Txn:       txn,
		StartTime: time.Now(),
		ReadOnly:  readOnly,
		Changes:   make([]ChangeRecord, 0),
	}

	m.txns[id] = t
	return t, nil
}

// Get retrieves a transaction by ID
func (m *TransactionManager) Get(id int64) (*Transaction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	txn, ok := m.txns[id]
	return txn, ok
}

// Commit commits a transaction
func (m *TransactionManager) Commit(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn, ok := m.txns[id]
	if !ok {
		return fmt.Errorf("transaction %d not found", id)
	}

	if err := txn.Txn.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction %d: %w", id, err)
	}

	delete(m.txns, id)
	return nil
}

// Rollback rolls back a transaction
func (m *TransactionManager) Rollback(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn, ok := m.txns[id]
	if !ok {
		return fmt.Errorf("transaction %d not found", id)
	}

	txn.Txn.Discard()
	delete(m.txns, id)
	return nil
}

// Transaction represents a Badger transaction wrapper
type Transaction struct {
	ID        int64
	Txn       *badger.Txn
	StartTime time.Time
	ReadOnly  bool
	Changes   []ChangeRecord
	mu        sync.RWMutex
}

// ChangeRecord records a change made in transaction
type ChangeRecord struct {
	Operation string    // "insert", "update", "delete"
	TableName string
	RowKey    string
	OldData   domain.Row
	NewData   domain.Row
	Timestamp time.Time
}

// AddChange records a change
func (t *Transaction) AddChange(op, tableName, rowKey string, oldData, newData domain.Row) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Changes = append(t.Changes, ChangeRecord{
		Operation: op,
		TableName: tableName,
		RowKey:    rowKey,
		OldData:   oldData,
		NewData:   newData,
		Timestamp: time.Now(),
	})
}

// GetChanges returns all recorded changes
func (t *Transaction) GetChanges() []ChangeRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]ChangeRecord, len(t.Changes))
	copy(result, t.Changes)
	return result
}

// SequenceManager manages auto-increment sequences
type SequenceManager struct {
	mu   sync.RWMutex
	db   *badger.DB
	seqs map[string]*badger.Sequence
}

// NewSequenceManager creates a new SequenceManager
func NewSequenceManager(db *badger.DB) *SequenceManager {
	return &SequenceManager{
		db:   db,
		seqs: make(map[string]*badger.Sequence),
	}
}

// InitSequence initializes a sequence with given key and start value
func (m *SequenceManager) InitSequence(key string, start uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.seqs[key]; ok {
		return nil // Already initialized
	}

	seq, err := m.db.GetSequence([]byte(key), 1000)
	if err != nil {
		return fmt.Errorf("failed to create sequence %s: %w", key, err)
	}

	m.seqs[key] = seq
	return nil
}

// GetSequence returns a sequence by key
func (m *SequenceManager) GetSequence(key string) (*badger.Sequence, error) {
	m.mu.RLock()
	seq, ok := m.seqs[key]
	m.mu.RUnlock()

	if ok {
		return seq, nil
	}

	// Initialize if not exists
	if err := m.InitSequence(key, 1); err != nil {
		return nil, err
	}

	m.mu.RLock()
	seq = m.seqs[key]
	m.mu.RUnlock()

	return seq, nil
}

// ResetSequence resets a sequence to start value
func (m *SequenceManager) ResetSequence(key string, start uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Release existing sequence
	if seq, ok := m.seqs[key]; ok {
		seq.Release()
		delete(m.seqs, key)
	}

	// Create new sequence
	seq, err := m.db.GetSequence([]byte(key), 1000)
	if err != nil {
		return fmt.Errorf("failed to reset sequence %s: %w", key, err)
	}

	m.seqs[key] = seq
	return nil
}

// Close releases all sequences
func (m *SequenceManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, seq := range m.seqs {
		seq.Release()
	}
	m.seqs = make(map[string]*badger.Sequence)
}
