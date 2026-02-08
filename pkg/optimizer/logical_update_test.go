package optimizer

import (
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestLogicalUpdate_NewLogicalUpdate(t *testing.T) {
	set := map[string]parser.Expression{
		"name": {Type: parser.ExprTypeValue, Value: "Alice"},
		"age":  {Type: parser.ExprTypeValue, Value: 30},
	}

	update := NewLogicalUpdate("users", set)

	if update.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", update.TableName)
	}

	if len(update.Set) != 2 {
		t.Errorf("Expected 2 SET columns, got %d", len(update.Set))
	}

	if update.Where != nil {
		t.Error("Expected Where to be nil initially")
	}

	if len(update.OrderBy) != 0 {
		t.Error("Expected OrderBy to be empty initially")
	}

	if update.Limit != nil {
		t.Error("Expected Limit to be nil initially")
	}
}

func TestLogicalUpdate_Schema(t *testing.T) {
	update := NewLogicalUpdate("users", nil)
	schema := update.Schema()

	if len(schema) != 1 {
		t.Errorf("Expected 1 column in schema, got %d", len(schema))
	}

	if schema[0].Name != "rows_affected" {
		t.Errorf("Expected column name 'rows_affected', got '%s'", schema[0].Name)
	}

	if schema[0].Type != "int" {
		t.Errorf("Expected column type 'int', got '%s'", schema[0].Type)
	}

	if schema[0].Nullable {
		t.Error("Expected column to be non-nullable")
	}
}

func TestLogicalUpdate_Explain(t *testing.T) {
	tests := []struct {
		name     string
		update   *LogicalUpdate
		contains []string
	}{
		{
			name: "Simple update",
			update: func() *LogicalUpdate {
				set := map[string]parser.Expression{
					"name": {Type: parser.ExprTypeValue, Value: "Alice"},
				}
				return NewLogicalUpdate("users", set)
			}(),
			contains: []string{"Update(users SET", "name"},
		},
		{
			name: "Update with WHERE",
			update: func() *LogicalUpdate {
				set := map[string]parser.Expression{
					"name": {Type: parser.ExprTypeValue, Value: "Alice"},
				}
				where := parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
				}
				update := NewLogicalUpdate("users", set)
				update.SetWhere(&where)
				return update
			}(),
			contains: []string{"Update(users SET", "WHERE"},
		},
		{
			name: "Update with LIMIT",
			update: func() *LogicalUpdate {
				set := map[string]parser.Expression{
					"name": {Type: parser.ExprTypeValue, Value: "Alice"},
				}
				limit := int64(10)
				update := NewLogicalUpdate("users", set)
				update.SetLimit(&limit)
				return update
			}(),
			contains: []string{"Update(users SET", "LIMIT 10"},
		},
		{
			name: "Update with multiple columns",
			update: func() *LogicalUpdate {
				set := map[string]parser.Expression{
					"name": {Type: parser.ExprTypeValue, Value: "Alice"},
					"age":  {Type: parser.ExprTypeValue, Value: 30},
				}
				return NewLogicalUpdate("users", set)
			}(),
			contains: []string{"name", "age"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.update.Explain()
			for _, substr := range tt.contains {
				if !strings.Contains(result, substr) {
					t.Errorf("Explain() result '%s' should contain '%s'", result, substr)
				}
			}
		})
	}
}

func TestLogicalUpdate_GettersSetters(t *testing.T) {
	set := map[string]parser.Expression{
		"name": {Type: parser.ExprTypeValue, Value: "Alice"},
	}
	update := NewLogicalUpdate("users", set)

	// Test GetTableName
	if update.GetTableName() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", update.GetTableName())
	}

	// Test GetSet
	if len(update.GetSet()) != 1 {
		t.Errorf("Expected 1 SET column, got %d", len(update.GetSet()))
	}

	// Test SetWhere/GetWhere
	where := parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}
	update.SetWhere(&where)
	if update.GetWhere() == nil {
		t.Error("Expected GetWhere to return non-nil after SetWhere")
	}

	// Test SetOrderBy/GetOrderBy
	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}
	update.SetOrderBy(orderBy)
	if len(update.GetOrderBy()) != 1 {
		t.Error("Expected GetOrderBy to return 1 item")
	}

	// Test SetLimit/GetLimit
	limit := int64(100)
	update.SetLimit(&limit)
	if update.GetLimit() == nil {
		t.Error("Expected GetLimit to return non-nil after SetLimit")
	}
	if *update.GetLimit() != 100 {
		t.Errorf("Expected limit 100, got %d", *update.GetLimit())
	}
}

func TestLogicalUpdate_Children(t *testing.T) {
	update := NewLogicalUpdate("test_table", nil)

	children := update.Children()
	if children == nil {
		t.Error("Expected Children() to return non-nil slice")
	}

	if len(children) != 0 {
		t.Errorf("Expected 0 children, got %d", len(children))
	}

	// Test SetChildren
	child := NewLogicalDataSource("other_table", createMockTableInfoForUpdate("other_table", []string{"id"}))
	update.SetChildren(child)

	children = update.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

// Helper functions
func createMockTableInfoForUpdate(tableName string, columnNames []string) *domain.TableInfo {
	return nil // Simplified for testing
}
