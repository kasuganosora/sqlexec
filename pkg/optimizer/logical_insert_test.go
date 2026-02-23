package optimizer

import (
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestLogicalInsert_NewLogicalInsert(t *testing.T) {
	columns := []string{"id", "name", "email"}
	values := [][]parser.Expression{
		{
			{Type: parser.ExprTypeValue, Value: 1},
			{Type: parser.ExprTypeValue, Value: "Alice"},
			{Type: parser.ExprTypeValue, Value: "alice@example.com"},
		},
	}

	insert := NewLogicalInsert("users", columns, values)

	if insert.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", insert.TableName)
	}

	if len(insert.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(insert.Columns))
	}

	if len(insert.Values) != 1 {
		t.Errorf("Expected 1 value row, got %d", len(insert.Values))
	}
}

func TestLogicalInsert_Schema(t *testing.T) {
	insert := NewLogicalInsert("test_table", nil, nil)
	schema := insert.Schema()

	if len(schema) != 2 {
		t.Errorf("Expected 2 columns in schema, got %d", len(schema))
	}

	if schema[0].Name != "rows_affected" {
		t.Errorf("Expected first column name 'rows_affected', got '%s'", schema[0].Name)
	}

	if schema[1].Name != "last_insert_id" {
		t.Errorf("Expected second column name 'last_insert_id', got '%s'", schema[1].Name)
	}

	if !schema[1].Nullable {
		t.Error("Expected last_insert_id to be nullable")
	}
}

func TestLogicalInsert_Explain(t *testing.T) {
	tests := []struct {
		name     string
		insert   *LogicalInsert
		contains []string
	}{
		{
			name:     "Simple insert",
			insert:   NewLogicalInsert("users", nil, nil),
			contains: []string{"Insert(users)"},
		},
		{
			name: "Insert with columns and values",
			insert: func() *LogicalInsert {
				columns := []string{"id", "name"}
				values := [][]parser.Expression{
					{{Type: parser.ExprTypeValue, Value: 1}, {Type: parser.ExprTypeValue, Value: "Alice"}},
				}
				return NewLogicalInsert("users", columns, values)
			}(),
			contains: []string{"Insert(users", "id", "name", "1 rows"},
		},
		{
			name: "Insert with columns only",
			insert: func() *LogicalInsert {
				return NewLogicalInsert("users", []string{"id", "name"}, nil)
			}(),
			contains: []string{"Insert(users", "id", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.insert.Explain()
			for _, substr := range tt.contains {
				if !strings.Contains(result, substr) {
					t.Errorf("Explain() result '%s' should contain '%s'", result, substr)
				}
			}
		})
	}
}

func TestLogicalInsert_NewLogicalInsertWithSelect(t *testing.T) {
	selectPlan := NewLogicalDataSource("other_table", createMockTableInfoForInsert("other_table", []string{"id", "name"}))
	insert := NewLogicalInsertWithSelect("users", []string{"id", "name"}, selectPlan)

	if insert.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", insert.TableName)
	}

	if len(insert.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insert.Columns))
	}

	if !insert.HasSelect() {
		t.Error("Expected HasSelect to return true")
	}

	if insert.GetSelectPlan() == nil {
		t.Error("Expected GetSelectPlan to return non-nil")
	}
}

func TestLogicalInsert_Getters(t *testing.T) {
	columns := []string{"id", "name"}
	values := [][]parser.Expression{
		{{Type: parser.ExprTypeValue, Value: 1}, {Type: parser.ExprTypeValue, Value: "Alice"}},
	}
	insert := NewLogicalInsert("users", columns, values)

	// Test GetTableName
	if insert.GetTableName() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", insert.GetTableName())
	}

	// Test GetColumns
	if len(insert.GetColumns()) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insert.GetColumns()))
	}

	// Test GetValues
	if len(insert.GetValues()) != 1 {
		t.Errorf("Expected 1 value row, got %d", len(insert.GetValues()))
	}

	// Test HasSelect with no select
	insertNoSelect := NewLogicalInsert("users", columns, values)
	if insertNoSelect.HasSelect() {
		t.Error("Expected HasSelect to return false when no select plan")
	}

	// Test GetSelectPlan with no select
	if insertNoSelect.GetSelectPlan() != nil {
		t.Error("Expected GetSelectPlan to return nil when no select plan")
	}
}

func TestLogicalInsert_SetOnDuplicate(t *testing.T) {
	insert := NewLogicalInsert("users", nil, nil)
	update := NewLogicalUpdate("users", nil)

	insert.SetOnDuplicate(update)

	if insert.OnDuplicate == nil {
		t.Error("Expected OnDuplicate to be set after SetOnDuplicate")
	}
}

func TestLogicalInsert_Children(t *testing.T) {
	tests := []struct {
		name          string
		insert        *LogicalInsert
		expectedCount int
	}{
		{
			name:          "Insert without select",
			insert:        NewLogicalInsert("users", nil, nil),
			expectedCount: 0,
		},
		{
			name: "Insert with select",
			insert: func() *LogicalInsert {
				selectPlan := NewLogicalDataSource("other_table", createMockTableInfoForInsert("other_table", []string{"id"}))
				return NewLogicalInsertWithSelect("users", nil, selectPlan)
			}(),
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			children := tt.insert.Children()
			if len(children) != tt.expectedCount {
				t.Errorf("Expected %d children, got %d", tt.expectedCount, len(children))
			}
		})
	}
}

// Helper functions
func createMockTableInfoForInsert(tableName string, columnNames []string) *domain.TableInfo {
	return nil // Simplified for testing
}
