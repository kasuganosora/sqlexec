package memory

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestSnapshotIsolation_ReadDoesNotSeeNewerCommits verifies that a transaction
// started before another transaction's commit does NOT see the committed data.
// This is the core guarantee of snapshot isolation.
func TestSnapshotIsolation_ReadDoesNotSeeNewerCommits(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	ds.Connect(ctx)

	// Create table and insert initial row
	ds.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER", Primary: true},
			{Name: "name", Type: "VARCHAR"},
		},
	})
	ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(1), "name": "alice"},
	}, nil)

	// TX1: start a read-only transaction — should see snapshot at this point
	tx1ID, err := ds.BeginTx(ctx, true)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	// Now, outside the transaction, insert a new row
	ds.Insert(ctx, "users", []domain.Row{
		{"id": int64(2), "name": "bob"},
	}, nil)

	// TX1: query inside the transaction — should only see alice, not bob
	tx1Ctx := SetTransactionID(ctx, tx1ID)
	result, err := ds.Query(tx1Ctx, "users", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query in tx1 failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("snapshot isolation violated: tx1 should see 1 row (alice), got %d rows", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	} else if result.Rows[0]["name"] != "alice" {
		t.Errorf("snapshot isolation violated: expected alice, got %v", result.Rows[0]["name"])
	}

	// Cleanup
	ds.RollbackTx(ctx, tx1ID)
}

// TestSnapshotIsolation_WriteTxDoesNotSeeOtherWrites verifies that a write
// transaction does not see rows inserted by concurrent commits after it started.
func TestSnapshotIsolation_WriteTxDoesNotSeeOtherWrites(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	ds.Connect(ctx)

	ds.CreateTable(ctx, &domain.TableInfo{
		Name: "items",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "VARCHAR"},
		},
	})
	ds.Insert(ctx, "items", []domain.Row{
		{"id": int64(1), "value": "original"},
	}, nil)

	// TX1: start a write transaction
	tx1ID, err := ds.BeginTx(ctx, false)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	// Outside tx1: insert a new row and update the existing one
	ds.Insert(ctx, "items", []domain.Row{
		{"id": int64(2), "value": "new_outside"},
	}, nil)

	// TX1: read — should not see the new row
	tx1Ctx := SetTransactionID(ctx, tx1ID)
	result, err := ds.Query(tx1Ctx, "items", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query in tx1 failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("snapshot isolation violated: tx1 should see 1 row, got %d rows", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	ds.RollbackTx(ctx, tx1ID)
}

// TestSnapshotIsolation_TxSeesOwnInserts verifies that a transaction can see
// its own inserts even though they are not yet committed.
func TestSnapshotIsolation_TxSeesOwnInserts(t *testing.T) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test",
		Writable: true,
	})
	ctx := context.Background()
	ds.Connect(ctx)

	ds.CreateTable(ctx, &domain.TableInfo{
		Name: "items",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "VARCHAR"},
		},
	})

	// TX1: begin, insert, then read own insert
	tx1ID, err := ds.BeginTx(ctx, false)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	tx1Ctx := SetTransactionID(ctx, tx1ID)

	ds.Insert(tx1Ctx, "items", []domain.Row{
		{"id": int64(1), "value": "from_tx1"},
	}, nil)

	result, err := ds.Query(tx1Ctx, "items", &domain.QueryOptions{})
	if err != nil {
		t.Fatalf("Query in tx1 failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("tx should see its own inserts: expected 1 row, got %d", len(result.Rows))
	}

	ds.RollbackTx(ctx, tx1ID)
}
