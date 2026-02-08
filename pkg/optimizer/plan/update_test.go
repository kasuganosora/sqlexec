package plan

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestUpdateConfig(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		set       map[string]parser.Expression
		where     *parser.Expression
		orderBy   []*parser.OrderItem
		limit     *int64
	}{
		{
			name:      "Simple update",
			tableName: "users",
			set:       map[string]parser.Expression{},
			where:     nil,
			orderBy:   nil,
			limit:     nil,
		},
		{
			name:      "Update with SET clause",
			tableName: "products",
			set: map[string]parser.Expression{
				"price":    {Type: parser.ExprTypeValue, Value: "price * 1.1"},
				"updated_at": {Type: parser.ExprTypeValue, Value: "NOW()"},
			},
			where:   nil,
			orderBy: nil,
			limit:   nil,
		},
		{
			name:      "Update with WHERE clause",
			tableName: "orders",
			set: map[string]parser.Expression{
				"status": {Type: parser.ExprTypeValue, Value: "'shipped'"},
			},
			where:   &parser.Expression{Type: parser.ExprTypeValue, Value: "status = 'pending'"},
			orderBy: nil,
			limit:   nil,
		},
		{
			name:      "Update with ORDER BY",
			tableName: "logs",
			set: map[string]parser.Expression{
				"archived": {Type: parser.ExprTypeValue, Value: "true"},
			},
			where: nil,
orderBy: []*parser.OrderItem{
			{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "created_at"}, Direction: "ASC"},
		},
			limit: nil,
		},
		{
			name:      "Update with LIMIT",
			tableName: "temp_data",
			set: map[string]parser.Expression{
				"processed": {Type: parser.ExprTypeValue, Value: "true"},
			},
			where:   nil,
			orderBy: nil,
			limit:   func() *int64 { l := int64(1000); return &l }(),
		},
		{
			name:      "Update with all options",
			tableName: "priority_queue",
			set: map[string]parser.Expression{
				"status":   {Type: parser.ExprTypeValue, Value: "'processing'"},
				"worker_id": {Type: parser.ExprTypeValue, Value: "123"},
			},
			where: &parser.Expression{Type: parser.ExprTypeValue, Value: "status = 'pending'"},
orderBy: []*parser.OrderItem{
			{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "priority"}, Direction: "DESC"},
			{Expr: parser.Expression{Type: parser.ExprTypeValue, Value: "created_at"}, Direction: "ASC"},
		},
			limit: func() *int64 { l := int64(10); return &l }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &UpdateConfig{
				TableName: tt.tableName,
				Set:       tt.set,
				Where:     tt.where,
				OrderBy:   tt.orderBy,
				Limit:     tt.limit,
			}

			if config.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", config.TableName, tt.tableName)
			}

			if len(config.Set) != len(tt.set) {
				t.Errorf("Set length = %v, want %v", len(config.Set), len(tt.set))
			}

			if tt.where != nil {
				if config.Where != tt.where {
					t.Errorf("Where = %v, want %v", config.Where, tt.where)
				}
			} else {
				if config.Where != nil {
					t.Errorf("Expected Where to be nil, got %v", config.Where)
				}
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

func TestUpdateConfigWithPlan(t *testing.T) {
	set := map[string]parser.Expression{
		"status":    {Type: parser.ExprTypeValue, Value: "'active'"},
		"login_count": {Type: parser.ExprTypeValue, Value: "login_count + 1"},
	}
	where := &parser.Expression{Type: parser.ExprTypeValue, Value: "last_login < DATE_SUB(NOW(), INTERVAL 30 DAY)"}

	updateConfig := &UpdateConfig{
		TableName: "users",
		Set:       set,
		Where:     where,
		OrderBy:   nil,
		Limit:     nil,
	}

	plan := &Plan{
		ID:     "update_001",
		Type:   TypeUpdate,
		Config: updateConfig,
	}

	if plan.Type != TypeUpdate {
		t.Errorf("Plan.Type = %v, want %v", plan.Type, TypeUpdate)
	}

	retrievedConfig, ok := plan.Config.(*UpdateConfig)
	if !ok {
		t.Fatal("Failed to retrieve UpdateConfig from Plan")
	}

	if retrievedConfig.TableName != "users" {
		t.Errorf("TableName = %v, want users", retrievedConfig.TableName)
	}

	if len(retrievedConfig.Set) != 2 {
		t.Errorf("Set length = %v, want 2", len(retrievedConfig.Set))
	}

	if retrievedConfig.Where != where {
		t.Error("Where expression mismatch")
	}

	if retrievedConfig.Where.Value != where.Value {
		t.Errorf("Where.Value = %v, want %v", retrievedConfig.Where.Value, where.Value)
	}

	if plan.Explain() != "Update[update_001]" {
		t.Errorf("Plan.Explain() = %v, want Update[update_001]", plan.Explain())
	}
}

func TestUpdateConfigNilFields(t *testing.T) {
	config := &UpdateConfig{
		TableName: "test",
		Set:       map[string]parser.Expression{},
		Where:     nil,
		OrderBy:   nil,
		Limit:     nil,
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

func TestUpdateConfigEmptySet(t *testing.T) {
	config := &UpdateConfig{
		TableName: "test",
		Set:       map[string]parser.Expression{},
		Where:     nil,
		OrderBy:   nil,
		Limit:     nil,
	}

	if len(config.Set) != 0 {
		t.Errorf("Set length = %v, want 0", len(config.Set))
	}
}

func TestUpdateConfigComplexSet(t *testing.T) {
	config := &UpdateConfig{
		TableName: "analytics",
		Set: map[string]parser.Expression{
			"views":           {Type: parser.ExprTypeValue, Value: "views + 1"},
			"last_viewed":     {Type: parser.ExprTypeValue, Value: "NOW()"},
			"unique_viewers":  {Type: parser.ExprTypeValue, Value: "CASE WHEN viewer_id NOT IN (SELECT viewer_id FROM views) THEN unique_viewers + 1 ELSE unique_viewers END"},
			"avg_view_time":   {Type: parser.ExprTypeValue, Value: "(total_view_time + :view_time) / (views + 1)"},
		},
		Where: &parser.Expression{Type: parser.ExprTypeValue, Value: "content_id = :content_id"},
		OrderBy: nil,
		Limit:   nil,
	}

	if len(config.Set) != 4 {
		t.Errorf("Set length = %v, want 4", len(config.Set))
	}

	if config.Set["views"].Value != "views + 1" {
		t.Errorf("views expression = %v, want views + 1", config.Set["views"].Value)
	}

	if config.Set["last_viewed"].Value != "NOW()" {
		t.Errorf("last_viewed expression = %v, want NOW()", config.Set["last_viewed"].Value)
	}

	if config.Where.Value != "content_id = :content_id" {
		t.Errorf("Where.Value = %v, want content_id = :content_id", config.Where.Value)
	}
}
