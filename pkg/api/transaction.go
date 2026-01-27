package api

import (
	"context"
	"sync"

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
func (t *Transaction) Query(sql string, args ...interface{}) (*Query, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil, NewError(ErrCodeTransaction, "transaction is not active", nil)
	}

	// 使用事务执行查询
	// TODO: Parse SQL to get table name
	result, err := t.tx.Query(context.Background(), "SELECT", &domain.QueryOptions{})
	if err != nil {
		return nil, WrapError(err, ErrCodeTransaction, "transaction query failed")
	}

	return NewQuery(t.session, result, sql, args), nil
}

// Execute 事务内执行命令
func (t *Transaction) Execute(sql string, args ...interface{}) (*Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil, NewError(ErrCodeTransaction, "transaction is not active", nil)
	}

	// 解析 SQL 确定操作类型
	// 简化实现：假设用户直接调用 DataSource 的方法
	// 实际实现需要解析 SQL 并调用相应的方法

	// TODO: 解析 SQL 并执行
	// 这里需要完善：解析 SQL -> 调用 Insert/Update/Delete 方法
	// 临时返回错误
	return nil, NewError(ErrCodeNotSupported, "transaction.Execute not fully implemented yet", nil)
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
	if t.active {
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
