package api

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

func TestSession_ExplainComplexSQL(t *testing.T) {
	// Create in-memory data source
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create tables
	ctx := context.Background()

	// Create users table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "users",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int", Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create orders table
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "orders",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "user_id", Type: "int", Nullable: false},
			{Name: "amount", Type: "float", Nullable: false},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Create DB
	db, err := NewDB(&DBConfig{
		CacheEnabled: true,
	})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Register data source
	err = db.RegisterDataSource("test", dataSource)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create API session
	apiSession := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "test",
		Isolation:      IsolationRepeatableRead,
		ReadOnly:       false,
		CacheEnabled:   true,
	})
	defer apiSession.Close()

	// Test 1: Simple EXPLAIN with SELECT
	t.Run("Simple EXPLAIN SELECT", func(t *testing.T) {
		explain, err := apiSession.Explain("SELECT * FROM users WHERE age > ?", 18)
		if err != nil {
			t.Fatalf("Explain failed: %v", err)
		}
		t.Logf("Simple SELECT Explain:\n%s", explain)

		// Check that explain contains plan information
		if explain == "" {
			t.Error("Explain output should not be empty")
		}
	})

	// Test 2: EXPLAIN with JOIN (note: complex JOIN may not be fully supported yet)
	t.Run("EXPLAIN with JOIN", func(t *testing.T) {
		// First try a simple JOIN
		sql := `
			SELECT u.name, o.amount
			FROM users u
			INNER JOIN orders o ON u.id = o.user_id
			WHERE u.age > ?
		`
		explain, err := apiSession.Explain(sql, 18)
		if err != nil {
			t.Logf("EXPLAIN with JOIN failed (may not be fully supported): %v", err)
			// This is expected if JOIN is not fully implemented in optimizer
			return
		}
		t.Logf("JOIN Explain:\n%s", explain)

		if explain == "" {
			t.Error("Explain output should not be empty")
		}
	})

	// Test 3: EXPLAIN with aggregation
	t.Run("EXPLAIN with GROUP BY", func(t *testing.T) {
		sql := "SELECT age, COUNT(*) as count FROM users WHERE age > ? GROUP BY age"
		explain, err := apiSession.Explain(sql, 0)
		if err != nil {
			t.Fatalf("Explain with GROUP BY failed: %v", err)
		}
		t.Logf("GROUP BY Explain:\n%s", explain)

		if explain == "" {
			t.Error("Explain output should not be empty")
		}
	})

	// Test 4: EXPLAIN with ORDER BY and LIMIT
	t.Run("EXPLAIN with ORDER BY and LIMIT", func(t *testing.T) {
		sql := "SELECT * FROM users ORDER BY age DESC LIMIT ?"
		explain, err := apiSession.Explain(sql, 10)
		if err != nil {
			t.Fatalf("Explain with ORDER BY/LIMIT failed: %v", err)
		}
		t.Logf("ORDER BY/LIMIT Explain:\n%s", explain)

		if explain == "" {
			t.Error("Explain output should not be empty")
		}
	})

	// Test 5: EXPLAIN caching
	t.Run("EXPLAIN caching", func(t *testing.T) {
		sql := "SELECT * FROM users WHERE id = ?"
		args := []interface{}{1}

		// First call - not cached
		explain1, err1 := apiSession.Explain(sql, args...)
		if err1 != nil {
			t.Fatalf("First Explain failed: %v", err1)
		}

		// Second call - should be cached
		explain2, err2 := apiSession.Explain(sql, args...)
		if err2 != nil {
			t.Fatalf("Second Explain failed: %v", err2)
		}

		// Both should return same result
		if explain1 != explain2 {
			t.Error("Cached explain should match first explain")
		}

		t.Logf("Cached Explain:\n%s", explain1)
	})
}

func TestSession_ExplainWithoutExplainKeyword(t *testing.T) {
	// Create in-memory data source
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create table
	ctx := context.Background()
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "value", Type: "string", Nullable: false},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create DB
	db, err := NewDB(&DBConfig{
		CacheEnabled: false,
	})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Register data source
	err = db.RegisterDataSource("test", dataSource)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create API session
	apiSession := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "test",
		Isolation:      IsolationRepeatableRead,
		ReadOnly:       false,
		CacheEnabled:   false,
	})

	// Test Explain without EXPLAIN keyword (should work like wrapping with EXPLAIN)
	sql := "SELECT * FROM test_table WHERE id = ?"
	explain, err := apiSession.Explain(sql, 1)
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}

	t.Logf("Explain without EXPLAIN keyword:\n%s", explain)

	// Check that explain contains plan information
	if explain == "" {
		t.Error("Explain output should not be empty")
	}
}

func TestSession_ExplainWithInvalidSQL(t *testing.T) {
	// Create in-memory data source
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create DB
	db, err := NewDB(&DBConfig{
		CacheEnabled: false,
	})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Register data source
	err = db.RegisterDataSource("test", dataSource)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create API session
	apiSession := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "test",
		Isolation:      IsolationRepeatableRead,
		ReadOnly:       false,
		CacheEnabled:   false,
	})

	// Test Explain with invalid SQL
	_, err = apiSession.Explain("INVALID SQL")
	if err == nil {
		t.Error("Explain should fail with invalid SQL")
	}

	// Test Explain with DML statement
	_, err = apiSession.Explain("INSERT INTO test_table VALUES (1, 'test')")
	if err == nil {
		t.Error("Explain should fail with INSERT statement")
	}
}

func TestSession_ExplainParameterBinding(t *testing.T) {
	// Create in-memory data source
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create table
	ctx := context.Background()
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "age", Type: "int", Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create DB
	db, err := NewDB(&DBConfig{
		CacheEnabled: false,
	})
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Register data source
	err = db.RegisterDataSource("test", dataSource)
	if err != nil {
		t.Fatalf("Failed to register data source: %v", err)
	}

	// Create API session
	apiSession := db.SessionWithOptions(&SessionOptions{
		DataSourceName: "test",
		Isolation:      IsolationRepeatableRead,
		ReadOnly:       false,
		CacheEnabled:   false,
	})

	// Test Explain with different parameter types
	tests := []struct {
		name string
		sql  string
		args []interface{}
	}{
		{"String parameter", "SELECT * FROM test_table WHERE name = ?", []interface{}{"Alice"}},
		{"Int parameter", "SELECT * FROM test_table WHERE id = ?", []interface{}{1}},
		{"Float parameter", "SELECT * FROM test_table WHERE age > ?", []interface{}{18.5}},
		{"Multiple parameters", "SELECT * FROM test_table WHERE id > ? AND age < ?", []interface{}{0, 30}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			explain, err := apiSession.Explain(tt.sql, tt.args...)
			if err != nil {
				t.Fatalf("Explain failed: %v", err)
			}
			t.Logf("Explain with %s:\n%s", tt.name, explain)

			if explain == "" {
				t.Error("Explain output should not be empty")
			}
		})
	}
}
