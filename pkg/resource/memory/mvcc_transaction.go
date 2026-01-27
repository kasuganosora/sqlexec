package memory

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MVCCTransaction wraps existing transaction methods to implement Transaction interface
type MVCCTransaction struct {
	ds    *MVCCDataSource
	txnID int64
}

func (t *MVCCTransaction) Commit(ctx context.Context) error {
	return t.ds.CommitTx(ctx, t.txnID)
}

func (t *MVCCTransaction) Rollback(ctx context.Context) error {
	return t.ds.RollbackTx(ctx, t.txnID)
}

func (t *MVCCTransaction) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	// Temporary implementation: return empty result
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{},
		Rows:    []domain.Row{},
		Total:   0,
	}, nil
}

func (t *MVCCTransaction) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return t.ds.Query(ctx, tableName, options)
}

func (t *MVCCTransaction) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return t.ds.Insert(ctx, tableName, rows, options)
}

func (t *MVCCTransaction) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return t.ds.Update(ctx, tableName, filters, updates, options)
}

func (t *MVCCTransaction) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return t.ds.Delete(ctx, tableName, filters, options)
}
