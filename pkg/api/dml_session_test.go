package api

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDMLSession(t *testing.T) *Session {
	t.Helper()

	ds := memory.NewMVCCDataSource(nil)
	require.NoError(t, ds.Connect(context.Background()))
	t.Cleanup(func() { _ = ds.Close(context.Background()) })

	db, err := NewDB(nil)
	require.NoError(t, err)
	require.NoError(t, db.RegisterDataSource("test", ds))
	require.NoError(t, db.SetDefaultDataSource("test"))

	session := db.Session()
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestSession_TruncateTablePreservesSchema(t *testing.T) {
	session := setupDMLSession(t)

	_, err := session.Execute(`
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	require.NoError(t, err)

	_, err = session.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)

	rows, err := session.QueryAll("SELECT * FROM users")
	require.NoError(t, err)
	require.Len(t, rows, 2)

	result, err := session.Execute("TRUNCATE TABLE users")
	require.NoError(t, err)
	require.NotNil(t, result)

	rows, err = session.QueryAll("SELECT * FROM users")
	require.NoError(t, err)
	assert.Empty(t, rows, "TRUNCATE should clear rows")

	_, err = session.Execute("INSERT INTO users (id, name) VALUES (3, 'Carol')")
	require.NoError(t, err, "table schema must survive TRUNCATE")

	rows, err = session.QueryAll("SELECT id, name FROM users")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "Carol", rows[0]["name"])
}

func TestSession_UpdateArithmeticExpression(t *testing.T) {
	session := setupDMLSession(t)

	_, err := session.Execute(`
		CREATE TABLE inventory (
			id INT PRIMARY KEY,
			gold INT,
			qty INT
		)
	`)
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO inventory (id, gold, qty) VALUES (1, 1000, 10)")
	require.NoError(t, err)

	result, err := session.Execute("UPDATE inventory SET gold = gold - 300, qty = qty + 2 WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	row, err := session.QueryOne("SELECT gold, qty FROM inventory WHERE id = 1")
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, int64(700), toInt64(t, row["gold"]))
	assert.Equal(t, int64(12), toInt64(t, row["qty"]))
}

func TestSession_QueryAndExecute_WithCache(t *testing.T) {
	ds := memory.NewMVCCDataSource(nil)
	require.NoError(t, ds.Connect(context.Background()))
	t.Cleanup(func() { _ = ds.Close(context.Background()) })

	db, err := NewDB(&DBConfig{CacheEnabled: true})
	require.NoError(t, err)
	require.NoError(t, db.RegisterDataSource("test", ds))
	require.NoError(t, db.SetDefaultDataSource("test"))

	session := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "test",
		CacheEnabled:   true,
	})
	t.Cleanup(func() { _ = session.Close() })

	_, err = session.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)

	row, err := session.QueryOne("SELECT name FROM users WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, "Alice", row["name"])

	result, err := session.Execute("UPDATE users SET name = 'Alicia' WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	row, err = session.QueryOne("SELECT name FROM users WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, "Alicia", row["name"], "DML should invalidate cached SELECT for the table")
}

func toInt64(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		t.Fatalf("unexpected numeric type %T (%v)", v, v)
		return 0
	}
}
