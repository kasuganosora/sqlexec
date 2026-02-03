package testing

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestTableOperations tests table operations (CREATE/DROP/TRUNCATE)
func TestTableOperations(t *testing.T) {
	ctx := context.Background()

	t.Log("=== Starting table operations test ===")

	// Create in-memory data source
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: true,
	})
	err := ds.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to data source: %v", err)
	}
	t.Log("Data source connected successfully")

	// Test 1: Create table
	t.Run("Create table", func(t *testing.T) {
		t.Log("\n=== Test 1: Create table ===")

		productsSchema := &domain.TableInfo{
			Name:   "products",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false, Primary: true},
				{Name: "name", Type: "string", Nullable: false},
				{Name: "price", Type: "float64", Nullable: true},
				{Name: "stock", Type: "int64", Nullable: true},
			},
		}

		err = ds.CreateTable(ctx, productsSchema)
		if err != nil {
			t.Fatalf("Failed to create products table: %v", err)
		}
		t.Log("Products table created successfully")

		// Verify table exists
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get table list: %v", err)
		}

		if len(tables) != 1 || tables[0] != "products" {
			t.Errorf("Expected 1 table [products], got %d tables: %v", len(tables), tables)
		}
		t.Logf("Table list verified: %v", tables)

		// Verify table structure
		tableInfo, err := ds.GetTableInfo(ctx, "products")
		if err != nil {
			t.Fatalf("Failed to get table info: %v", err)
		}

		if tableInfo.Name != "products" {
			t.Errorf("Expected table name products, got %s", tableInfo.Name)
		}
		if len(tableInfo.Columns) != 4 {
			t.Errorf("Expected 4 columns, got %d", len(tableInfo.Columns))
		}

		// Check primary key
		hasPrimaryKey := false
		for _, col := range tableInfo.Columns {
			if col.Primary {
				hasPrimaryKey = true
				if col.Name != "id" {
					t.Errorf("Expected primary key column id, got %s", col.Name)
				}
			}
		}
		if !hasPrimaryKey {
			t.Error("Primary key column not found")
		}
		t.Log("Table structure verified successfully")
	})

	// Test 2: Insert data and query
	t.Run("Insert data", func(t *testing.T) {
		t.Log("\n=== Test 2: Insert data ===")

		productsData := []domain.Row{
			{"id": int64(1), "name": "Product A", "price": float64(99.99), "stock": int64(100)},
			{"id": int64(2), "name": "Product B", "price": float64(199.99), "stock": int64(50)},
			{"id": int64(3), "name": "Product C", "price": float64(299.99), "stock": int64(30)},
		}

		affected, err := ds.Insert(ctx, "products", productsData, &domain.InsertOptions{})
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
		if affected != int64(3) {
			t.Errorf("Expected to insert 3 rows, got %d rows", affected)
		}
		t.Log("Data inserted successfully: 3 rows")

		// Query verification
		result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
		if err != nil {
			t.Fatalf("Failed to query data: %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows of data, got %d rows", len(result.Rows))
		}
		t.Logf("Query verified successfully: %d rows", len(result.Rows))
	})

	// Test 3: TRUNCATE TABLE
	t.Run("Truncate table", func(t *testing.T) {
		t.Log("\n=== Test 3: TRUNCATE TABLE ===")

		err := ds.TruncateTable(ctx, "products")
		if err != nil {
			t.Fatalf("Failed to truncate table: %v", err)
		}
		t.Log("Table truncated successfully")

		// Verify data is cleared
		result, err := ds.Query(ctx, "products", &domain.QueryOptions{})
		if err != nil {
			t.Fatalf("Failed to query data: %v", err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows of data, got %d rows", len(result.Rows))
		}
		t.Log("Table data verified as empty")

		// Verify table structure still exists
		tableInfo, err := ds.GetTableInfo(ctx, "products")
		if err != nil {
			t.Fatalf("Failed to get table info: %v", err)
		}

		if tableInfo.Name != "products" || len(tableInfo.Columns) != 4 {
			t.Error("Table structure was destroyed")
		}
		t.Log("Table structure remains intact")
	})

	// Test 4: DROP TABLE
	t.Run("Drop table", func(t *testing.T) {
		t.Log("\n=== Test 4: DROP TABLE ===")

		err := ds.DropTable(ctx, "products")
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}
		t.Log("Table dropped successfully")

		// Verify table doesn't exist
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get table list: %v", err)
		}

		if len(tables) != 0 {
			t.Errorf("Expected 0 tables, got %d tables: %v", len(tables), tables)
		}
		t.Log("Table list verified as empty")

		// Verify cannot query dropped table
		_, err = ds.Query(ctx, "products", &domain.QueryOptions{})
		if err == nil {
			t.Error("Querying dropped table should return error")
		}
		t.Logf("Querying dropped table correctly returned error: %v", err)
	})

	// Test 5: Create duplicate table
	t.Run("Create duplicate table", func(t *testing.T) {
		t.Log("\n=== Test 5: Create duplicate table ===")

		duplicateSchema := &domain.TableInfo{
			Name:   "users",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false, Primary: true},
				{Name: "name", Type: "string", Nullable: false},
			},
		}

		// First creation
		err := ds.CreateTable(ctx, duplicateSchema)
		if err != nil {
			t.Fatalf("First creation of users table failed: %v", err)
		}
		t.Log("First creation of users table successful")

		// Second creation (should fail)
		err = ds.CreateTable(ctx, duplicateSchema)
		if err == nil {
			t.Error("Creating duplicate table should return error")
		}
		t.Logf("Creating duplicate table correctly returned error: %v", err)
	})

	// Test 6: Drop non-existent table
	t.Run("Drop non-existent table", func(t *testing.T) {
		t.Log("\n=== Test 6: Drop non-existent table ===")

		err := ds.DropTable(ctx, "nonexistent_table")
		if err == nil {
			t.Error("Dropping non-existent table should return error")
		}
		t.Logf("Dropping non-existent table correctly returned error: %v", err)
	})

	// Test 7: Truncate non-existent table
	t.Run("Truncate non-existent table", func(t *testing.T) {
		t.Log("\n=== Test 7: Truncate non-existent table ===")

		err := ds.TruncateTable(ctx, "nonexistent_table")
		if err == nil {
			t.Error("Truncating non-existent table should return error")
		}
		t.Logf("Truncating non-existent table correctly returned error: %v", err)
	})

	t.Log("\n=== Table operations test completed ===")
}

// TestMultipleTables tests multi-table operations
func TestMultipleTables(t *testing.T) {
	ctx := context.Background()

	t.Log("=== Starting multi-table operations test ===")

	// Create in-memory data source
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: true,
	})
	err := ds.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to data source: %v", err)
	}

	// Create multiple tables
	tables := []struct {
		name   string
		schema *domain.TableInfo
		data   []domain.Row
	}{
		{
			name: "customers",
			schema: &domain.TableInfo{
				Name:   "customers",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false, Primary: true},
					{Name: "name", Type: "string", Nullable: false},
					{Name: "email", Type: "string", Nullable: true},
				},
			},
			data: []domain.Row{
				{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
				{"id": int64(2), "name": "Bob", "email": "bob@example.com"},
			},
		},
		{
			name: "orders",
			schema: &domain.TableInfo{
				Name:   "orders",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false, Primary: true},
					{Name: "customer_id", Type: "int64", Nullable: false},
					{Name: "amount", Type: "float64", Nullable: false},
				},
			},
			data: []domain.Row{
				{"id": int64(1), "customer_id": int64(1), "amount": float64(99.99)},
				{"id": int64(2), "customer_id": int64(2), "amount": float64(199.99)},
			},
		},
	}

	// Create all tables
	for _, table := range tables {
		err := ds.CreateTable(ctx, table.schema)
		if err != nil {
			t.Fatalf("Failed to create %s table: %v", table.name, err)
		}
		t.Logf("%s table created successfully", table.name)

		// Insert data
		_, err = ds.Insert(ctx, table.name, table.data, &domain.InsertOptions{})
		if err != nil {
			t.Fatalf("Failed to insert %s table data: %v", table.name, err)
		}
		t.Logf("%s table data inserted successfully: %d rows", table.name, len(table.data))
	}

	// Verify all tables
	tableList, err := ds.GetTables(ctx)
	if err != nil {
		t.Fatalf("Failed to get table list: %v", err)
	}

	if len(tableList) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tableList))
	}
	t.Logf("Table list verified successfully: %v", tableList)

	// Verify data for each table
	for _, table := range tables {
		result, err := ds.Query(ctx, table.name, &domain.QueryOptions{})
		if err != nil {
			t.Fatalf("Failed to query %s table: %v", table.name, err)
		}

		if len(result.Rows) != len(table.data) {
			t.Errorf("%s table expected %d rows, got %d rows", table.name, len(table.data), len(result.Rows))
		}
		t.Logf("%s table data verified successfully: %d rows", table.name, len(result.Rows))
	}

	t.Log("\n=== Multi-table operations test completed ===")
}
