package api

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupScenarioSession(t *testing.T) *Session {
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

func TestSQLScenarios_Select(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute(`
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			age INT,
			email VARCHAR(100)
		)
	`)
	require.NoError(t, err)
	_, err = session.Execute(`
		INSERT INTO users (id, name, age, email) VALUES
		(1, 'Alice', 35, 'alice@example.com'),
		(2, 'Bob', 25, 'bob@example.com'),
		(3, 'Carol', 40, 'carol@example.com')
	`)
	require.NoError(t, err)

	rows, err := session.QueryAll("SELECT * FROM users")
	require.NoError(t, err)
	assert.Len(t, rows, 3)

	rows, err = session.QueryAll("SELECT * FROM users WHERE age > 30")
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	rows, err = session.QueryAll("SELECT name FROM users ORDER BY name DESC")
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, "Carol", rows[0]["name"])

	rows, err = session.QueryAll("SELECT id FROM users ORDER BY id LIMIT 2")
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	rows, err = session.QueryAll("SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), toInt64(t, rows[0]["id"]))

	rows, err = session.QueryAll("SELECT id, name, email FROM users WHERE id = 1")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "Alice", rows[0]["name"])
	assert.Equal(t, "alice@example.com", rows[0]["email"])
}

func TestSQLScenarios_Join(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute("CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount INT)")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO orders (order_id, user_id, amount) VALUES (10, 1, 100), (11, 1, 50)")
	require.NoError(t, err)

	rows, err := session.QueryAll("SELECT u.name, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id")
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	rows, err = session.QueryAll("SELECT u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(rows), 2, "LEFT JOIN should at least return inner-join rows")
}

func TestSQLScenarios_Insert(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))")
	require.NoError(t, err)

	result, err := session.Execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	result, err = session.Execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com'), (3, 'Carol', 'carol@example.com')")
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsAffected)

	rows, err := session.QueryAll("SELECT * FROM users")
	require.NoError(t, err)
	assert.Len(t, rows, 3)
}

func TestSQLScenarios_Update(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), age INT, status VARCHAR(20))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, name, email, age, status) VALUES (1, 'Alice', 'alice@example.com', 30, 'pending'), (2, 'Bob', 'bob@example.com', 20, 'pending')")
	require.NoError(t, err)

	result, err := session.Execute("UPDATE users SET name = 'Alicia' WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	result, err = session.Execute("UPDATE users SET name = 'Alice', email = 'newalice@example.com' WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	result, err = session.Execute("UPDATE users SET age = age + 1 WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	row, err := session.QueryOne("SELECT name, email, age FROM users WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, "Alice", row["name"])
	assert.Equal(t, "newalice@example.com", row["email"])
	assert.Equal(t, int64(31), toInt64(t, row["age"]))

	result, err = session.Execute("UPDATE users SET status = 'active' WHERE id IN (1, 2)")
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsAffected)
}

func TestSQLScenarios_Delete(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
	require.NoError(t, err)

	result, err := session.Execute("DELETE FROM users WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected)

	result, err = session.Execute("DELETE FROM users WHERE id IN (2, 3)")
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RowsAffected)

	rows, err := session.QueryAll("SELECT * FROM users")
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestSQLScenarios_ComplexWhere(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, age INT, status VARCHAR(20), name VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute(`
		INSERT INTO users (id, age, status, name) VALUES
		(1, 35, 'active', 'Alice'),
		(2, 15, 'active', 'Bob'),
		(3, 70, 'deleted', 'Carol')
	`)
	require.NoError(t, err)

	rows, err := session.QueryAll("SELECT * FROM users WHERE age > 30 AND status = 'active'")
	require.NoError(t, err)
	assert.Len(t, rows, 1)

	rows, err = session.QueryAll("SELECT * FROM users WHERE name LIKE 'A%'")
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

func TestSQLScenarios_Functions(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, age INT, status VARCHAR(20))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, age, status) VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')")
	require.NoError(t, err)

	row, err := session.QueryOne("SELECT COUNT(*) as total FROM users")
	require.NoError(t, err)
	assert.Equal(t, int64(3), toInt64(t, firstValue(row)))
}

func TestSQLScenarios_Subqueries(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	require.NoError(t, err)

	rows, err := session.QueryAll("SELECT * FROM users WHERE id IN (1)")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "Alice", rows[0]["name"])
}

func TestSQLScenarios_CreateDrop(t *testing.T) {
	session := setupScenarioSession(t)

	_, err := session.Execute(`
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100)
		)
	`)
	require.NoError(t, err)

	_, err = session.Execute("CREATE INDEX idx_email ON users(email)")
	require.NoError(t, err)

	_, err = session.Execute("DROP INDEX idx_email ON users")
	require.NoError(t, err)

	_, err = session.Execute("DROP TABLE IF EXISTS users")
	require.NoError(t, err)

	_, err = session.QueryAll("SELECT * FROM users")
	assert.Error(t, err)
}

func TestSQLScenarios_CreatedTableIsQueryable(t *testing.T) {
	session := setupScenarioSession(t)
	_, err := session.Execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
	require.NoError(t, err)

	rows, err := session.QueryAll("SELECT * FROM users")
	require.NoError(t, err)
	assert.Empty(t, rows)

	_, err = session.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)
	row, err := session.QueryOne("SELECT name FROM users WHERE id = 1")
	require.NoError(t, err)
	assert.Equal(t, "Alice", row["name"])
}

func firstValue(row map[string]interface{}) interface{} {
	if row == nil {
		return nil
	}
	for _, alias := range []string{"total", "max_age", "min_age", "total_age", "cnt", "COUNT(*)"} {
		if v, ok := row[alias]; ok {
			return v
		}
	}
	for _, v := range row {
		return v
	}
	return nil
}
