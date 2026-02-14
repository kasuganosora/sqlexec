package mvcc

import (
	"sync"
	"time"
)

type Transaction struct {
	xid       XID
	snapshot  *Snapshot
	status    TransactionStatus
	createdAt time.Time
	startTime time.Time
	endTime   time.Time
	manager   *Manager
	level     IsolationLevel
	mvcc      bool
	commands  []Command
	reads     map[string]bool
	writes    map[string]*TupleVersion
	locks     map[string]bool
	mu        sync.RWMutex
}

func (t *Transaction) XID() XID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.xid
}

func (t *Transaction) Snapshot() *Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.snapshot
}

func (t *Transaction) Status() TransactionStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

func (t *Transaction) Level() IsolationLevel {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.level
}

func (t *Transaction) IsMVCC() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mvcc
}

func (t *Transaction) Age() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return time.Since(t.startTime)
}

func (t *Transaction) SetStatus(status TransactionStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = status
}

func (t *Transaction) GetStatus() TransactionStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

func (t *Transaction) SetEndTime(endTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.endTime = endTime
}

type Command interface {
	Apply() error
	Rollback() error
}

type WriteCommand struct {
	transaction *Transaction
	key         string
	version     *TupleVersion
	applied     bool
}

func (cmd *WriteCommand) Apply() error {
	if cmd.applied { return nil }
	cmd.applied = true
	return nil
}

func (cmd *WriteCommand) Rollback() error {
	if !cmd.applied { return nil }
	return nil
}

type DeleteCommand struct {
	transaction *Transaction
	key         string
	applied     bool
}

func (cmd *DeleteCommand) Apply() error {
	if cmd.applied { return nil }
	cmd.applied = true
	return nil
}

func (cmd *DeleteCommand) Rollback() error {
	if !cmd.applied { return nil }
	return nil
}

type UpdateCommand struct {
	transaction *Transaction
	key         string
	oldVersion  *TupleVersion
	newVersion  *TupleVersion
	applied     bool
}

func (cmd *UpdateCommand) Apply() error {
	if cmd.applied { return nil }
	if cmd.oldVersion != nil {
		cmd.oldVersion.MarkDeleted(cmd.transaction.xid, 0)
	}
	cmd.applied = true
	return nil
}

func (cmd *UpdateCommand) Rollback() error {
	if !cmd.applied { return nil }
	if cmd.oldVersion != nil {
		cmd.oldVersion.mu.Lock()
		cmd.oldVersion.Expired = false
		cmd.oldVersion.Xmax = 0
		cmd.oldVersion.mu.Unlock()
	}
	return nil
}
