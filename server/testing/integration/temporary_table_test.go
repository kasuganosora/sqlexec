package testing

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// TestTemporaryTables tests temporary table functionality
func TestTemporaryTables(t *testing.T) {
	ctx := context.Background()

	t.Log("=== Starting temporary table test ===")

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

	// Test 1: Create regular table
	t.Run("Create regular table", func(t *testing.T) {
		t.Log("\n=== Test 1: Create regular table ===")

		regularTable := &domain.TableInfo{
			Name:   "regular_users",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false, Primary: true},
				{Name: "name", Type: "string", Nullable: false},
			},
		}

		err := ds.CreateTable(ctx, regularTable)
		if err != nil {
			t.Fatalf("Failed to create regular table: %v", err)
		}
		t.Log("Regular table created successfully")

		// Verify table in regular tables
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get table list: %v", err)
		}

		if len(tables) != 1 || tables[0] != "regular_users" {
			t.Errorf("Expected 1 regular table [regular_users], got %d tables: %v", len(tables), tables)
		}
		t.Log("Regular table verified in list")
	})

	// Test 2: Create temporary table
	t.Run("Create temporary table", func(t *testing.T) {
		t.Log("\n=== Test 2: Create temporary table ===")

		tempTable := &domain.TableInfo{
			Name:      "temp_results",
			Schema:    "test",
			Temporary: true,
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false, Primary: true},
				{Name: "value", Type: "float64", Nullable: false},
			},
		}

		err := ds.CreateTable(ctx, tempTable)
		if err != nil {
			t.Fatalf("Failed to create temporary table: %v", err)
		}
		t.Log("Temporary table created successfully")

		// Verify temporary table NOT in regular tables
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get table list: %v", err)
		}

		// Temporary table should not appear in regular tables
		hasTempTable := false
		for _, name := range tables {
			if name == "temp_results" {
				hasTempTable = true
				break
			}
		}
		if hasTempTable {
			t.Error("Temporary table should not appear in regular tables list")
		}
		if len(tables) != 1 {
			t.Errorf("Expected 1 regular table, got %d tables: %v", len(tables), tables)
		}
		t.Log("Temporary table verified NOT in regular tables list")

		// Verify temporary table in temporary tables
		tempTables, err := ds.GetTemporaryTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get temporary tables list: %v", err)
		}

		if len(tempTables) != 1 || tempTables[0] != "temp_results" {
			t.Errorf("Expected 1 temporary table [temp_results], got %d tables: %v", len(tempTables), tempTables)
		}
		t.Log("Temporary table found in temporary tables list")
	})

	// Test 3: Insert data into temporary table
	t.Run("Insert data into temporary table", func(t *testing.T) {
		t.Log("\n=== Test 3: Insert data into temporary table ===")

		// Insert data into temporary table
		tempData := []domain.Row{
			{"id": int64(1), "value": float64(100.5)},
			{"id": int64(2), "value": float64(200.75)},
			{"id": int64(3), "value": float64(300.25)},
		}

		affected, err := ds.Insert(ctx, "temp_results", tempData, &domain.InsertOptions{})
		if err != nil {
			t.Fatalf("Failed to insert data into temporary table: %v", err)
		}
		if affected != int64(3) {
			t.Errorf("Expected to insert 3 rows, got %d rows", affected)
		}
		t.Log("Data inserted into temporary table successfully: 3 rows")

		// Query temporary table
		result, err := ds.Query(ctx, "temp_results", &domain.QueryOptions{})
		if err != nil {
			t.Fatalf("Failed to query temporary table: %v", err)
		}

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows of data, got %d rows", len(result.Rows))
		}
		t.Log("Temporary table queried successfully: 3 rows")

		// Verify data
		for i, row := range result.Rows {
			if row["id"] != int64(i+1) {
				t.Errorf("Row %d id mismatch", i)
			}
		}
		t.Log("Temporary table data verified successfully")
	})

	// Test 4: Get all tables (both regular and temporary)
	t.Run("Get all tables", func(t *testing.T) {
		t.Log("\n=== Test 4: Get all tables ===")

		// Get all tables
		allTables, err := ds.GetAllTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get all tables: %v", err)
		}

		// Verify both regular and temporary tables
		if len(allTables) != 2 {
			t.Errorf("Expected 2 tables, got %d tables: %v", len(allTables), allTables)
		}

		hasRegular := false
		hasTemp := false
		for _, name := range allTables {
			if name == "regular_users" {
				hasRegular = true
			}
			if name == "temp_results" {
				hasTemp = true
			}
		}

		if !hasRegular {
			t.Error("All tables list missing regular table")
		}
		if !hasTemp {
			t.Error("All tables list missing temporary table")
		}
		t.Logf("All tables list verified successfully: %v", allTables)
	})

	// Test 5: Verify temporary tables are cleared on connection close
	t.Run("Temporary tables cleared on close", func(t *testing.T) {
		t.Log("\n=== Test 5: Temporary tables cleared on close ===")

		// Close connection
		err := ds.Close(ctx)
		if err != nil {
			t.Fatalf("Failed to close connection: %v", err)
		}
		t.Log("Connection closed successfully")

		// Reopen connection
		err = ds.Connect(ctx)
		if err != nil {
			t.Fatalf("Failed to reopen connection: %v", err)
		}
		t.Log("Connection reopened successfully")

		// Verify temporary tables are cleared
		tempTables, err := ds.GetTemporaryTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get temporary tables list: %v", err)
		}

		if len(tempTables) != 0 {
			t.Errorf("Expected 0 temporary tables, got %d tables: %v", len(tempTables), tempTables)
		}
		t.Log("Temporary tables verified as cleared")

		// Verify regular tables are still stored
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get table list: %v", err)
		}

		if len(tables) != 1 || tables[0] != "regular_users" {
			t.Errorf("Expected 1 regular table [regular_users], got %d tables: %v", len(tables), tables)
		}
		t.Log("Regular tables verified as still stored")

		// Verify temporary table is gone
		_, err = ds.Query(ctx, "temp_results", &domain.QueryOptions{})
		if err == nil {
			t.Error("Querying temporary table should fail after connection close")
		}
		t.Logf("Querying temporary table correctly failed after connection close: %v", err)
	})

	// Test 6: Multiple temporary tables
	t.Run("Multiple temporary tables", func(t *testing.T) {
		t.Log("\n=== Test 6: Multiple temporary tables ===")

		// Create multiple temporary tables
		tempTableNames := []string{"temp1", "temp2", "temp3"}
		for _, tableName := range tempTableNames {
			tempSchema := &domain.TableInfo{
				Name:      tableName,
				Temporary: true,
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false, Primary: true},
					{Name: "data", Type: "string", Nullable: true},
				},
			}

			err := ds.CreateTable(ctx, tempSchema)
			if err != nil {
				t.Fatalf("Failed to create temporary table %s: %v", tableName, err)
			}
			t.Logf("Temporary table %s created successfully", tableName)
		}

		// Verify temporary tables in list
		tempList, err := ds.GetTemporaryTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get temporary tables list: %v", err)
		}

		if len(tempList) != 3 {
			t.Errorf("Expected 3 temporary tables, got %d", len(tempList))
		}
		t.Logf("Temporary tables in list: %v", tempList)

		// Verify regular tables count still 1
		tables, err := ds.GetTables(ctx)
		if err != nil {
			t.Fatalf("Failed to get table list: %v", err)
		}

		if len(tables) != 1 {
			t.Errorf("Expected 1 regular table, got %d tables: %v", len(tables), tables)
		}
		t.Log("Regular tables count verified as still 1")
	})

	t.Log("\n=== Temporary table test completed ===")
}
