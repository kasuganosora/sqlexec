package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestInsertConfig(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		columns   []string
		values    [][]parser.Expression
		onDuplicate *map[string]parser.Expression
	}{
		{
			name:      "Simple insert",
			tableName: "users",
			columns:   []string{},
			values:    [][]parser.Expression{},
			onDuplicate: nil,
		},
		{
			name:      "Insert with columns",
			tableName: "products",
			columns:   []string{"name", "price", "category"},
			values: [][]parser.Expression{
				{
					{Type: parser.ExprTypeValue, Value: "Laptop"},
					{Type: parser.ExprTypeValue, Value: "999.99"},
					{Type: parser.ExprTypeValue, Value: "Electronics"},
				},
			},
			onDuplicate: nil,
		},
		{
			name:      "Multiple rows",
			tableName: "categories",
			columns:   []string{"name", "description"},
			values: [][]parser.Expression{
				{
					{Type: parser.ExprTypeValue, Value: "Electronics"},
					{Type: parser.ExprTypeValue, Value: "Electronic devices"},
				},
				{
					{Type: parser.ExprTypeValue, Value: "Books"},
					{Type: parser.ExprTypeValue, Value: "Reading materials"},
				},
				{
					{Type: parser.ExprTypeValue, Value: "Clothing"},
					{Type: parser.ExprTypeValue, Value: "Apparel items"},
				},
			},
			onDuplicate: nil,
		},
		{
			name:      "Insert with on duplicate",
			tableName: "inventory",
			columns:   []string{"product_id", "quantity"},
			values: [][]parser.Expression{
				{
					{Type: parser.ExprTypeValue, Value: "P001"},
					{Type: parser.ExprTypeValue, Value: "100"},
				},
			},
			onDuplicate: func() *map[string]parser.Expression {
				m := make(map[string]parser.Expression)
				m["quantity"] = parser.Expression{Type: parser.ExprTypeValue, Value: "quantity + VALUES(quantity)"}
				return &m
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &InsertConfig{
				TableName:   tt.tableName,
				Columns:     tt.columns,
				Values:      tt.values,
				OnDuplicate: tt.onDuplicate,
			}

			if config.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", config.TableName, tt.tableName)
			}

			if len(config.Columns) != len(tt.columns) {
				t.Errorf("Columns length = %v, want %v", len(config.Columns), len(tt.columns))
			}

			if len(config.Values) != len(tt.values) {
				t.Errorf("Values length = %v, want %v", len(config.Values), len(tt.values))
			}

			// Verify Columns content
			for i, col := range tt.columns {
				if config.Columns[i] != col {
					t.Errorf("Columns[%d] = %v, want %v", i, config.Columns[i], col)
				}
			}

			// Verify Values content
			for i, row := range tt.values {
				if len(config.Values[i]) != len(row) {
					t.Errorf("Values[%d] length = %v, want %v", i, len(config.Values[i]), len(row))
				}
			}

			if tt.onDuplicate != nil {
				if config.OnDuplicate == nil {
					t.Error("OnDuplicate is nil, want non-nil")
				} else if len(*config.OnDuplicate) != len(*tt.onDuplicate) {
					t.Errorf("OnDuplicate length = %v, want %v", len(*config.OnDuplicate), len(*tt.onDuplicate))
				}
			}
		})
	}
}

func TestInsertConfigWithPlan(t *testing.T) {
	columns := []string{"username", "email", "age"}
	values := [][]parser.Expression{
		{
			{Type: parser.ExprTypeValue, Value: "john_doe"},
			{Type: parser.ExprTypeValue, Value: "john@example.com"},
			{Type: parser.ExprTypeValue, Value: "30"},
		},
	}

	insertConfig := &InsertConfig{
		TableName:   "users",
		Columns:     columns,
		Values:      values,
		OnDuplicate: nil,
	}

	plan := &Plan{
		ID:     "insert_001",
		Type:   TypeInsert,
		Config: insertConfig,
	}

	if plan.Type != TypeInsert {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeInsert)
	}

	retrievedConfig, ok := plan.Config.(*InsertConfig)
	if !ok {
		t.Fatal("Failed to retrieve InsertConfig from Plan")
	}

	if retrievedConfig.TableName != "users" {
		t.Errorf("TableName = %v, want users", retrievedConfig.TableName)
	}

	if len(retrievedConfig.Columns) != 3 {
		t.Errorf("Columns length = %v, want 3", len(retrievedConfig.Columns))
	}

	if len(retrievedConfig.Values) != 1 {
		t.Errorf("Values length = %v, want 1", len(retrievedConfig.Values))
	}

	if plan.Explain() != "Insert[insert_001]" {
		t.Errorf("Plan.Explain() = %v, want Insert[insert_001]", plan.Explain())
	}
}

func TestInsertConfigNilFields(t *testing.T) {
	config := &InsertConfig{
		TableName:   "test",
		Columns:     nil,
		Values:      nil,
		OnDuplicate: nil,
	}

	if config.Columns != nil {
		t.Errorf("Expected Columns to be nil, got %v", config.Columns)
	}

	if config.Values != nil {
		t.Errorf("Expected Values to be nil, got %v", config.Values)
	}

	if config.OnDuplicate != nil {
		t.Errorf("Expected OnDuplicate to be nil, got %v", config.OnDuplicate)
	}
}

func TestInsertConfigEmptyArrays(t *testing.T) {
	config := &InsertConfig{
		TableName:   "test",
		Columns:     []string{},
		Values:      [][]parser.Expression{},
		OnDuplicate: nil,
	}

	if len(config.Columns) != 0 {
		t.Errorf("Columns length = %v, want 0", len(config.Columns))
	}

	if len(config.Values) != 0 {
		t.Errorf("Values length = %v, want 0", len(config.Values))
	}
}
