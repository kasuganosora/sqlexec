package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// MockCardinalityEstimator for testing
type MockCardinalityEstimator struct {
	cardinality int64
}

func (m *MockCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	return m.cardinality
}

func (m *MockCardinalityEstimator) EstimateFilter(tableName string, filters []domain.Filter) int64 {
	return m.cardinality
}

func (m *MockCardinalityEstimator) EstimateJoin(left, right LogicalPlan, joinType JoinType) int64 {
	return m.cardinality
}

func (m *MockCardinalityEstimator) EstimateDistinct(table string, columns []string) int64 {
	return m.cardinality
}

func (m *MockCardinalityEstimator) UpdateStatistics(tableName string, stats *TableStatistics) {
	// No-op for mock
}

func TestJoinEliminationRule_Name(t *testing.T) {
	estimator := &MockCardinalityEstimator{cardinality: 100}
	rule := NewJoinEliminationRule(estimator)
	if rule.Name() != "JoinElimination" {
		t.Errorf("Expected rule name 'JoinElimination', got '%s'", rule.Name())
	}
}

func TestJoinEliminationRule_Match(t *testing.T) {
	estimator := &MockCardinalityEstimator{cardinality: 100}
	rule := NewJoinEliminationRule(estimator)

	tests := []struct {
		name     string
		plan     LogicalPlan
		expected bool
	}{
		{
			name:     "Simple datasource - no join",
			plan:     NewLogicalDataSource("test_table", createMockTableInfoForJoin("test_table", []string{"id", "name"})),
			expected: false,
		},
		{
			name:     "Plan with join",
			plan:     createSimpleJoinPlan(),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.Match(tt.plan)
			if result != tt.expected {
				t.Errorf("Match() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestJoinEliminationRule_Apply(t *testing.T) {
	tests := []struct {
		name      string
		plan      LogicalPlan
		estimator *MockCardinalityEstimator
	}{
		{
			name:      "Apply on simple datasource",
			plan:      NewLogicalDataSource("test_table", createMockTableInfoForJoin("test_table", []string{"id", "name"})),
			estimator: &MockCardinalityEstimator{cardinality: 100},
		},
		{
			name:      "Apply on join plan",
			plan:      createSimpleJoinPlan(),
			estimator: &MockCardinalityEstimator{cardinality: 100},
		},
		{
			name:      "Apply on join with single row table",
			plan:      createSimpleJoinPlan(),
			estimator: &MockCardinalityEstimator{cardinality: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := NewJoinEliminationRule(tt.estimator)
			ctx := context.Background()
			optCtx := &OptimizationContext{}

			result, err := rule.Apply(ctx, tt.plan, optCtx)
			if err != nil {
				t.Errorf("Apply() returned error: %v", err)
			}
			// Rule should return a plan
			if result == nil && tt.plan != nil {
				t.Error("Apply() returned nil for non-nil input")
			}
		})
	}
}

func TestIsEqualityCondition(t *testing.T) {
	tests := []struct {
		name     string
		cond     *JoinCondition
		expected bool
	}{
		{
			name: "Equality condition",
			cond: &JoinCondition{
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
				Right:    &parser.Expression{Type: parser.ExprTypeColumn, Column: "user_id"},
				Operator: "=",
			},
			expected: true,
		},
		{
			name: "Non-equality condition",
			cond: &JoinCondition{
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
				Right:    &parser.Expression{Type: parser.ExprTypeColumn, Column: "user_id"},
				Operator: ">",
			},
			expected: false,
		},
		{
			name: "Missing left operand",
			cond: &JoinCondition{
				Right:    &parser.Expression{Type: parser.ExprTypeColumn, Column: "user_id"},
				Operator: "=",
			},
			expected: false,
		},
		{
			name: "Missing right operand",
			cond: &JoinCondition{
				Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
				Operator: "=",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isEqualityCondition(tt.cond)
			if result != tt.expected {
				t.Errorf("isEqualityCondition() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestExtractTableNames(t *testing.T) {
	tests := []struct {
		name          string
		plan          LogicalPlan
		expectedCount int
	}{
		{
			name:          "Simple datasource",
			plan:          NewLogicalDataSource("test_table", createMockTableInfoForJoin("test_table", []string{"id", "name"})),
			expectedCount: 1,
		},
		{
			name:          "Join plan",
			plan:          createSimpleJoinPlan(),
			expectedCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractTableNames(tt.plan)
			if len(result) != tt.expectedCount {
				t.Errorf("extractTableNames() returned %d tables, expected %d", len(result), tt.expectedCount)
			}
		})
	}
}

// Helper function to create a simple join plan
func createSimpleJoinPlan() LogicalPlan {
	left := NewLogicalDataSource("users", createMockTableInfoForJoin("users", []string{"id", "name"}))
	right := NewLogicalDataSource("orders", createMockTableInfoForJoin("orders", []string{"id", "user_id"}))

	joinCondition := JoinCondition{
		Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "users.id"},
		Right:    &parser.Expression{Type: parser.ExprTypeColumn, Column: "orders.user_id"},
		Operator: "=",
	}

	join := NewLogicalJoin(
		InnerJoin,
		left,
		right,
		[]*JoinCondition{&joinCondition},
	)

	return join
}

// Helper function to create a mock table info for join tests
func createMockTableInfoForJoin(tableName string, columnNames []string) *domain.TableInfo {
	tableInfo := &domain.TableInfo{
		Name:     tableName,
		Columns:  make([]domain.ColumnInfo, 0, len(columnNames)),
	}
	for _, colName := range columnNames {
		tableInfo.Columns = append(tableInfo.Columns, domain.ColumnInfo{
			Name:     colName,
			Type:     "INT",
			Nullable: true,
		})
	}
	return tableInfo
}
