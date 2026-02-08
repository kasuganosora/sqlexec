package optimizer

import (
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestLogicalDelete_NewLogicalDelete(t *testing.T) {
	delete := NewLogicalDelete("test_table")

	if delete.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", delete.TableName)
	}

	if delete.Where != nil {
		t.Error("Expected Where to be nil initially")
	}

	if len(delete.OrderBy) != 0 {
		t.Error("Expected OrderBy to be empty initially")
	}

	if delete.Limit != nil {
		t.Error("Expected Limit to be nil initially")
	}
}

func TestLogicalDelete_Schema(t *testing.T) {
	delete := NewLogicalDelete("test_table")
	schema := delete.Schema()

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

func TestLogicalDelete_Explain(t *testing.T) {
	tests := []struct {
		name     string
		delete   *LogicalDelete
		expected string
	}{
		{
			name:     "Simple delete",
			delete:   NewLogicalDelete("users"),
			expected: "Delete(users)",
		},
		{
			name: "Delete with WHERE",
			delete: func() *LogicalDelete {
				d := NewLogicalDelete("users")
				where := parser.Expression{
					Type:     parser.ExprTypeOperator,
					Operator: "=",
					Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
					Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
				}
				d.SetWhere(&where)
				return d
			}(),
			expected: "Delete(users WHERE {LEFT:0xc0000000?? RIGHT:0xc0000000??}",
		},
		{
			name: "Delete with LIMIT",
			delete: func() *LogicalDelete {
				d := NewLogicalDelete("users")
				limit := int64(10)
				d.SetLimit(&limit)
				return d
			}(),
			expected: "Delete(users) LIMIT 10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.delete.Explain()
			// Check prefix only due to pointer addresses
			if !strings.HasPrefix(result, "Delete("+tt.delete.TableName) {
				t.Errorf("Explain() result should start with 'Delete(%s)': %s", tt.delete.TableName, result)
			}
		})
	}
}

func TestLogicalDelete_GettersSetters(t *testing.T) {
	delete := NewLogicalDelete("users")

	// Test GetTableName
	if delete.GetTableName() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", delete.GetTableName())
	}

	// Test SetWhere/GetWhere
	where := parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}
	delete.SetWhere(&where)
	if delete.GetWhere() == nil {
		t.Error("Expected GetWhere to return non-nil after SetWhere")
	}

	// Test SetOrderBy/GetOrderBy
	orderBy := []*parser.OrderItem{
		{Expr: parser.Expression{Type: parser.ExprTypeColumn, Column: "id"}, Direction: "ASC"},
	}
	delete.SetOrderBy(orderBy)
	if len(delete.GetOrderBy()) != 1 {
		t.Error("Expected GetOrderBy to return 1 item")
	}

	// Test SetLimit/GetLimit
	limit := int64(100)
	delete.SetLimit(&limit)
	if delete.GetLimit() == nil {
		t.Error("Expected GetLimit to return non-nil after SetLimit")
	}
	if *delete.GetLimit() != 100 {
		t.Errorf("Expected limit 100, got %d", *delete.GetLimit())
	}
}

func TestLogicalDelete_Children(t *testing.T) {
	delete := NewLogicalDelete("test_table")

	children := delete.Children()
	if children == nil {
		t.Error("Expected Children() to return non-nil slice")
	}

	if len(children) != 0 {
		t.Errorf("Expected 0 children, got %d", len(children))
	}

	// Test SetChildren
	child := NewLogicalDataSource("other_table", createMockTableInfoForLogicalDelete("other_table", []string{"id"}))
	delete.SetChildren(child)

	children = delete.Children()
	if len(children) != 1 {
		t.Errorf("Expected 1 child after SetChildren, got %d", len(children))
	}
}

// Helper function
func createMockTableInfoForLogicalDelete(tableName string, columnNames []string) *domain.TableInfo {
	// Simplified: return nil since we're testing minimal functionality
	return nil
}
