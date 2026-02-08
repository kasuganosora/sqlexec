package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestDeleteConfig(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		where     *parser.Expression
		orderBy   []*parser.OrderItem
		limit     *int64
	}{
		{
			name:      "Simple delete",
			tableName: "users",
			where:     nil,
			orderBy:   nil,
			limit:     nil,
		},
		{
			name:      "Delete with where clause",
			tableName: "orders",
			where:     &parser.Expression{Type: parser.ExprTypeColumn, Value: "status = 'cancelled'"},
			orderBy:   nil,
			limit:     nil,
		},
		{
		name:      "Delete with order by",
		tableName: "logs",
		where:     nil,
		orderBy: []*parser.OrderItem{
			{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "created_at"}, Direction: "DESC"},
		},
		limit: nil,
	},
		{
			name:      "Delete with limit",
			tableName: "temp_data",
			where:     nil,
			orderBy:   nil,
			limit:     func() *int64 { l := int64(100); return &l }(),
		},
		{
			name:      "Delete with all options",
			tableName: "archive",
			where:     &parser.Expression{Type: parser.ExprTypeColumn, Value: "created_at < '2023-01-01'"},
			orderBy: []*parser.OrderItem{
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "id"}, Direction: "ASC"},
				{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "created_at"}, Direction: "DESC"},
			},
			limit: func() *int64 { l := int64(1000); return &l }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &DeleteConfig{
				TableName: tt.tableName,
				Where:     tt.where,
				OrderBy:   tt.orderBy,
				Limit:     tt.limit,
			}

			if config.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", config.TableName, tt.tableName)
			}

			if tt.where != nil && config.Where != tt.where {
				t.Errorf("Where = %v, want %v", config.Where, tt.where)
			}

			if len(config.OrderBy) != len(tt.orderBy) {
				t.Errorf("OrderBy length = %v, want %v", len(config.OrderBy), len(tt.orderBy))
			}

			if tt.limit != nil {
				if config.Limit == nil {
					t.Error("Limit is nil, want non-nil")
				} else if *config.Limit != *tt.limit {
					t.Errorf("Limit = %v, want %v", *config.Limit, *tt.limit)
				}
			}
		})
	}
}

func TestDeleteConfigWithPlan(t *testing.T) {
	whereExpr := &parser.Expression{Type: parser.ExprTypeColumn, Value: "age > 18"}
	limitVal := int64(50)

deleteConfig := &DeleteConfig{
		TableName: "students",
		Where:     whereExpr,
		OrderBy: []*parser.OrderItem{
			{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "grade"}, Direction: "ASC"},
		},
		Limit: &limitVal,
	}

	plan := &Plan{
		ID:     "delete_001",
		Type:   TypeDelete,
		Config: deleteConfig,
	}

	if plan.Type != TypeDelete {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeDelete)
	}

	retrievedConfig, ok := plan.Config.(*DeleteConfig)
	if !ok {
		t.Fatal("Failed to retrieve DeleteConfig from Plan")
	}

	if retrievedConfig.TableName != "students" {
		t.Errorf("TableName = %v, want students", retrievedConfig.TableName)
	}

	if retrievedConfig.Where != whereExpr {
		t.Error("Where expression mismatch")
	}

	if len(retrievedConfig.OrderBy) != 1 {
		t.Errorf("OrderBy length = %v, want 1", len(retrievedConfig.OrderBy))
	}

	if *retrievedConfig.Limit != 50 {
		t.Errorf("Limit = %v, want 50", *retrievedConfig.Limit)
	}
}

func TestDeleteConfigNilValues(t *testing.T) {
	config := &DeleteConfig{
		TableName: "test_table",
		Where:     nil,
		OrderBy:   nil,
		Limit:     nil,
	}

	if config.TableName != "test_table" {
		t.Errorf("TableName = %v, want test_table", config.TableName)
	}

	if config.Where != nil {
		t.Errorf("Expected Where to be nil, got %v", config.Where)
	}

	if config.OrderBy != nil {
		t.Errorf("Expected OrderBy to be nil, got %v", config.OrderBy)
	}

	if config.Limit != nil {
		t.Errorf("Expected Limit to be nil, got %v", config.Limit)
	}
}
