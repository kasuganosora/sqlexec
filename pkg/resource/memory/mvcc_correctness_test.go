package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/require"
)

func newWritableMVCC(t *testing.T) *MVCCDataSource {
	t.Helper()
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	require.NoError(t, ds.Connect(context.Background()))
	return ds
}

// TestMVCC_GCPreservesPinnedTableVersion reproduces a GC watermark bug:
// snapshot.startVer can be far ahead of a table's pinned snapshotVer when
// other tables have been written. GC must keep the pinned table version.
func TestMVCC_GCPreservesPinnedTableVersion(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}))
	_, err := ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(1), "name": "alice"},
	}, nil)
	require.NoError(t, err)

	ds.mu.RLock()
	pinned := ds.tables["users"].latest
	ds.mu.RUnlock()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "other",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER"},
		},
	}))
	for i := 0; i < 8; i++ {
		_, err = ds.Insert(ctx, "other", []domain.Row{{"id": int64(i)}}, nil)
		require.NoError(t, err)
	}

	tx1ID, err := ds.BeginTx(ctx, true)
	require.NoError(t, err)

	ds.mu.RLock()
	startVer := ds.snapshots[tx1ID].startVer
	snapVer := ds.snapshots[tx1ID].tableSnapshots["users"].snapshotVer
	ds.mu.RUnlock()
	require.Equal(t, pinned, snapVer)
	require.Greater(t, startVer, snapVer, "need startVer >> pinned table version to hit the old GC bug")

	_, err = ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(2), "name": "bob"},
	}, nil)
	require.NoError(t, err)

	dummy, err := ds.BeginTx(ctx, false)
	require.NoError(t, err)
	require.NoError(t, ds.RollbackTx(ctx, dummy))

	ds.mu.RLock()
	_, stillPinned := ds.tables["users"].versions[snapVer]
	ds.mu.RUnlock()
	require.True(t, stillPinned, "GC dropped the version pinned by an active transaction")

	tx1Ctx := SetTransactionID(ctx, tx1ID)
	result, err := ds.Query(tx1Ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "alice", result.Rows[0]["name"])

	require.NoError(t, ds.RollbackTx(ctx, tx1ID))
}

// TestMVCC_FirstCommitterWins ensures a later commit cannot silently overwrite
// a concurrent commit that advanced the same table (lost-update).
func TestMVCC_FirstCommitterWins(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR"},
			{Name: "age", Type: "INTEGER"},
		},
	}))
	_, err := ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(1), "name": "alice", "age": int64(10)},
	}, nil)
	require.NoError(t, err)

	tx1, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	tx2, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)

	_, err = tx1.Update(ctx, "users",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		domain.Row{"name": "bob"}, nil)
	require.NoError(t, err)

	_, err = tx2.Update(ctx, "users",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		domain.Row{"age": int64(20)}, nil)
	require.NoError(t, err)

	require.NoError(t, tx1.Commit(ctx))

	err = tx2.Commit(ctx)
	require.Error(t, err)
	var conflict *domain.ErrWriteConflict
	require.True(t, errors.As(err, &conflict), "expected write conflict, got %v", err)
	require.Equal(t, "users", conflict.TableName)

	result, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "bob", result.Rows[0]["name"])
	require.Equal(t, int64(10), result.Rows[0]["age"])
}

func TestMVCC_IndependentTablesDoNotConflict(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "users",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
	}))
	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "items",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
	}))
	_, err := ds.Insert(ctx, "users", []domain.Row{{"id": int64(1), "name": "alice"}}, nil)
	require.NoError(t, err)
	_, err = ds.Insert(ctx, "items", []domain.Row{{"id": int64(1), "name": "pen"}}, nil)
	require.NoError(t, err)

	tx1, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	tx2, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)

	_, err = tx1.Update(ctx, "users",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		domain.Row{"name": "bob"}, nil)
	require.NoError(t, err)
	_, err = tx2.Update(ctx, "items",
		[]domain.Filter{{Field: "id", Operator: "=", Value: int64(1)}},
		domain.Row{"name": "pencil"}, nil)
	require.NoError(t, err)

	require.NoError(t, tx1.Commit(ctx))
	require.NoError(t, tx2.Commit(ctx))

	users, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, "bob", users.Rows[0]["name"])
	items, err := ds.Query(ctx, "items", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, "pencil", items.Rows[0]["name"])
}

func TestMVCC_UpdateMatchesTxnVisibleRow(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "users",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
	}))
	_, err := ds.Insert(ctx, "users", []domain.Row{{"id": int64(1), "name": "alice"}}, nil)
	require.NoError(t, err)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)

	n, err := txn.Update(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "alice"}},
		domain.Row{"name": "bob"}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = txn.Update(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "bob"}},
		domain.Row{"name": "carol"}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	result, err := txn.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, "carol", result.Rows[0]["name"])
	require.NoError(t, txn.Commit(ctx))
}

func TestMVCC_DeleteMatchesTxnVisibleRow(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "users",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
	}))
	_, err := ds.Insert(ctx, "users", []domain.Row{{"id": int64(1), "name": "alice"}}, nil)
	require.NoError(t, err)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)

	_, err = txn.Update(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "alice"}},
		domain.Row{"name": "bob"}, nil)
	require.NoError(t, err)

	n, err := txn.Delete(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "alice"}}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), n, "delete must not match the stale base value")

	n, err = txn.Delete(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "bob"}}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	result, err := txn.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Total)
	require.NoError(t, txn.Commit(ctx))
}

func TestMVCC_MultiTableCommitIsAtomicOnUniqueFailure(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "accounts",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true, Unique: true},
		},
	}))
	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "names",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR", Unique: true},
		},
	}))
	_, err := ds.Insert(ctx, "accounts", []domain.Row{{"id": int64(1)}}, nil)
	require.NoError(t, err)
	_, err = ds.Insert(ctx, "names", []domain.Row{{"id": int64(1), "name": "dup"}}, nil)
	require.NoError(t, err)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)

	_, err = txn.Insert(ctx, "accounts", []domain.Row{{"id": int64(2)}}, nil)
	require.NoError(t, err)
	_, err = txn.Insert(ctx, "names", []domain.Row{{"id": int64(2), "name": "dup"}}, nil)
	require.NoError(t, err)

	err = txn.Commit(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Duplicate entry")

	accounts, err := ds.Query(ctx, "accounts", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(1), accounts.Total, "unique failure must not leave a partial multi-table commit")

	names, err := ds.Query(ctx, "names", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(1), names.Total)
}

func TestMVCC_BeginTransactionIsolatesUncommittedWrites(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name:    "users",
		Columns: []domain.ColumnInfo{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}},
	}))
	_, err := ds.Insert(ctx, "users", []domain.Row{{"id": int64(1), "name": "alice"}}, nil)
	require.NoError(t, err)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	_, err = txn.Insert(ctx, "users", []domain.Row{{"id": int64(2), "name": "bob"}}, nil)
	require.NoError(t, err)

	outside, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(1), outside.Total, "uncommitted insert must not be visible outside the transaction")

	inside, err := txn.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(2), inside.Total)

	require.NoError(t, txn.Rollback(ctx))
	after, err := ds.Query(ctx, "users", &domain.QueryOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(1), after.Total)
}

func TestMVCC_TxnQueryIgnoresStaleGlobalIndex(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()

	require.NoError(t, ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	}))
	require.NoError(t, ds.CreateIndex("users", "name", "btree", false))
	_, err := ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}, nil)
	require.NoError(t, err)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{ReadOnly: true})
	require.NoError(t, err)

	_, err = ds.Delete(ctx, "users",
		[]domain.Filter{{Field: "name", Operator: "=", Value: "alice"}}, nil)
	require.NoError(t, err)

	result, err := txn.Query(ctx, "users", &domain.QueryOptions{
		Filters: []domain.Filter{{Field: "name", Operator: "=", Value: "bob"}},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Total)
	require.Equal(t, "bob", result.Rows[0]["name"])
	require.Equal(t, int64(2), result.Rows[0]["id"])

	require.NoError(t, txn.Rollback(ctx))
}

func createAutoIncUsers(t *testing.T, ds *MVCCDataSource) {
	t.Helper()
	require.NoError(t, ds.CreateTable(context.Background(), &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "VARCHAR", Unique: true},
		},
	}))
}

func TestMVCC_AutoIncRollbackDoesNotSkip(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	rows := []domain.Row{{"name": "alice"}, {"name": "bob"}}
	_, err = txn.Insert(ctx, "users", rows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows[0]["id"])
	require.Equal(t, int64(2), rows[1]["id"])
	require.NoError(t, txn.Rollback(ctx))

	next := []domain.Row{{"name": "carol"}}
	_, err = ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), next[0]["id"], "rolled-back AUTO_INCREMENT values must be reused")
}

func TestMVCC_AutoIncCommitAdvancesCounter(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	rows := []domain.Row{{"name": "alice"}}
	_, err = txn.Insert(ctx, "users", rows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows[0]["id"])
	require.NoError(t, txn.Commit(ctx))

	next := []domain.Row{{"name": "bob"}}
	_, err = ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), next[0]["id"])
}

func TestMVCC_AutoIncExplicitValueRollback(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	_, err = txn.Insert(ctx, "users", []domain.Row{{"id": int64(10), "name": "alice"}}, nil)
	require.NoError(t, err)
	require.NoError(t, txn.Rollback(ctx))

	next := []domain.Row{{"name": "bob"}}
	_, err = ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), next[0]["id"], "explicit id in a rolled-back txn must not raise the global counter")
}

func TestMVCC_AutoIncExplicitValueCommit(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	_, err = txn.Insert(ctx, "users", []domain.Row{{"id": int64(10), "name": "alice"}}, nil)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	next := []domain.Row{{"name": "bob"}}
	_, err = ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(11), next[0]["id"])
}

func TestMVCC_AutoIncUniqueFailureDoesNotSkip(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	_, err := ds.Insert(ctx, "users", []domain.Row{{"name": "alice"}}, nil)
	require.NoError(t, err)

	_, err = ds.Insert(ctx, "users", []domain.Row{{"name": "alice"}}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Duplicate entry")

	next := []domain.Row{{"name": "bob"}}
	_, err = ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), next[0]["id"], "failed unique insert must not consume AUTO_INCREMENT")
}

func TestMVCC_AutoIncTxnUniqueFailureDoesNotPublish(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	_, err := ds.Insert(ctx, "users", []domain.Row{{"name": "alice"}}, nil)
	require.NoError(t, err)

	txn, err := ds.BeginTransaction(ctx, &domain.TransactionOptions{})
	require.NoError(t, err)
	rows := []domain.Row{{"name": "alice"}}
	_, err = txn.Insert(ctx, "users", rows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), rows[0]["id"])
	require.Error(t, txn.Commit(ctx))

	next := []domain.Row{{"name": "bob"}}
	_, err = ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), next[0]["id"])
}

func TestMVCC_DropTableResetsAutoInc(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	_, err := ds.Insert(ctx, "users", []domain.Row{{"name": "alice"}}, nil)
	require.NoError(t, err)
	require.NoError(t, ds.DropTable(ctx, "users"))
	createAutoIncUsers(t, ds)

	next := []domain.Row{{"name": "bob"}}
	_, err = ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), next[0]["id"])
}

func TestMVCC_BulkLoadRaisesAutoInc(t *testing.T) {
	ds := newWritableMVCC(t)
	ctx := context.Background()
	createAutoIncUsers(t, ds)

	require.NoError(t, ds.BulkLoad("users", func(addPage func(rows []domain.Row)) error {
		addPage([]domain.Row{
			{"id": int64(1), "name": "alice"},
			{"id": int64(5), "name": "bob"},
		})
		return nil
	}))

	next := []domain.Row{{"name": "carol"}}
	_, err := ds.Insert(ctx, "users", next, nil)
	require.NoError(t, err)
	require.Equal(t, int64(6), next[0]["id"])
}
