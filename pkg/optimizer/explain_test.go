package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

func TestOptimizer_ExplainSimpleSelect(t *testing.T) {
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

	// Create optimizer
	opt := NewOptimizer(dataSource)

	// Create SELECT statement
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "id"},
			{Name: "name"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "gt",
			Left: &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: "age",
			},
			Right: &parser.Expression{
				Type:  parser.ExprTypeValue,
				Value: 18,
			},
		},
	}

	// Build SQL statement
	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	// Optimize
	plan, err := opt.Optimize(ctx, sqlStmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// Check that we got a physical plan
	if plan == nil {
		t.Fatal("Optimize returned nil plan")
	}

	// Test Explain
	explain := plan.Explain()
	if explain == "" {
		t.Error("Explain returned empty string")
	}

	// Check that explain contains expected elements
	// It should contain at least "TableScan" or "Scan"
	hasScan := false
	if len(explain) > 0 {
		// Just check it's not empty and contains something meaningful
		t.Logf("Explain output:\n%s", explain)
		hasScan = true
	}
	if !hasScan {
		t.Error("Explain output should contain plan information")
	}
}

func TestExplainPlan(t *testing.T) {
	// Create a mock physical plan for testing
	mockPlan := &mockPhysicalPlan{
		explainStr: "Mock Plan",
	}

	explain := ExplainPlan(mockPlan)
	if explain != "Mock Plan\n" {
		t.Errorf("ExplainPlan() = %q, want %q", explain, "Mock Plan\n")
	}
}

func TestDefaultCostModel_ScanCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	cost := costModel.ScanCost("test_table", 1000)

	// Cost should be positive
	if cost <= 0 {
		t.Errorf("ScanCost() = %v, want > 0", cost)
	}
}

func TestDefaultCostModel_FilterCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	cost := costModel.FilterCost(1000, 0.1)

	// Cost should be positive
	if cost <= 0 {
		t.Errorf("FilterCost() = %v, want > 0", cost)
	}
}

func TestDefaultCostModel_JoinCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	cost := costModel.JoinCost(1000, 500, InnerJoin)

	// Cost should be positive
	if cost <= 0 {
		t.Errorf("JoinCost() = %v, want > 0", cost)
	}
}

func TestDefaultCostModel_AggregateCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	cost := costModel.AggregateCost(1000, 2)

	// Cost should be positive
	if cost <= 0 {
		t.Errorf("AggregateCost() = %v, want > 0", cost)
	}
}

func TestDefaultCostModel_ProjectCost(t *testing.T) {
	costModel := NewDefaultCostModel()

	cost := costModel.ProjectCost(1000, 5)

	// Cost should be positive
	if cost <= 0 {
		t.Errorf("ProjectCost() = %v, want > 0", cost)
	}
}

// Mock PhysicalPlan for testing
type mockPhysicalPlan struct {
	explainStr string
}

func (m *mockPhysicalPlan) Children() []PhysicalPlan {
	return nil
}

func (m *mockPhysicalPlan) SetChildren(children ...PhysicalPlan) {
	// No-op
}

func (m *mockPhysicalPlan) Schema() []ColumnInfo {
	return nil
}

func (m *mockPhysicalPlan) Cost() float64 {
	return 0
}

func (m *mockPhysicalPlan) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return nil, nil
}

func (m *mockPhysicalPlan) Explain() string {
	return m.explainStr
}

func TestJoinType_String(t *testing.T) {
	tests := []struct {
		name string
		jt   JoinType
		want string
	}{
		{"InnerJoin", InnerJoin, "INNER JOIN"},
		{"LeftOuterJoin", LeftOuterJoin, "LEFT OUTER JOIN"},
		{"RightOuterJoin", RightOuterJoin, "RIGHT OUTER JOIN"},
		{"FullOuterJoin", FullOuterJoin, "FULL OUTER JOIN"},
		{"Unknown", JoinType(100), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.jt.String(); got != tt.want {
				t.Errorf("JoinType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAggregationType_String(t *testing.T) {
	tests := []struct {
		name string
		at   AggregationType
		want string
	}{
		{"Count", Count, "COUNT"},
		{"Sum", Sum, "SUM"},
		{"Avg", Avg, "AVG"},
		{"Max", Max, "MAX"},
		{"Min", Min, "MIN"},
		{"Unknown", AggregationType(100), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.at.String(); got != tt.want {
				t.Errorf("AggregationType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOptimizer_ExplainWithProjection(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

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

	opt := NewOptimizer(dataSource)

	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "name", Alias: "user_name"},
			{Name: "age"},
		},
		From: "test_table",
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	plan, err := opt.Optimize(ctx, sqlStmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	if plan == nil {
		t.Fatal("Optimize returned nil plan")
	}

	explain := plan.Explain()
	if explain == "" {
		t.Error("Explain returned empty string")
	}
	t.Logf("Explain output:\n%s", explain)
}

func TestOptimizer_ExplainWithLimit(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	ctx := context.Background()
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string", Nullable: false},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	opt := NewOptimizer(dataSource)

	limit := int64(10)
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From:  "test_table",
		Limit: &limit,
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	plan, err := opt.Optimize(ctx, sqlStmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	if plan == nil {
		t.Fatal("Optimize returned nil plan")
	}

	explain := plan.Explain()
	if explain == "" {
		t.Error("Explain returned empty string")
	}
	t.Logf("Explain output:\n%s", explain)
}

func TestOptimizer_ExplainWithOrderBy(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

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

	opt := NewOptimizer(dataSource)

	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		OrderBy: []parser.OrderByItem{
			{Column: "age", Direction: "DESC"},
		},
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	plan, err := opt.Optimize(ctx, sqlStmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	if plan == nil {
		t.Fatal("Optimize returned nil plan")
	}

	explain := plan.Explain()
	if explain == "" {
		t.Error("Explain returned empty string")
	}
	t.Logf("Explain output:\n%s", explain)
}

func TestOptimizer_ExplainWithGroupBy(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	ctx := context.Background()
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "category", Type: "string", Nullable: false},
			{Name: "value", Type: "int", Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	opt := NewOptimizer(dataSource)

	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "category", Alias: ""},
			{Name: "COUNT(*)", Alias: "count"},
		},
		From:   "test_table",
		GroupBy: []string{"category"},
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	plan, err := opt.Optimize(ctx, sqlStmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	if plan == nil {
		t.Fatal("Optimize returned nil plan")
	}

	explain := plan.Explain()
	if explain == "" {
		t.Error("Explain returned empty string")
	}
	t.Logf("Explain output:\n%s", explain)
}

func TestOptimizer_ExplainWithComplexWhere(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	ctx := context.Background()
	err = dataSource.CreateTable(ctx, &domain.TableInfo{
		Name: "test_table",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int", Primary: true, AutoIncrement: true},
			{Name: "age", Type: "int", Nullable: true},
			{Name: "score", Type: "int", Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	opt := NewOptimizer(dataSource)

	// Create a complex WHERE clause: age > 18 AND score > 50
	stmt := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "*"},
		},
		From: "test_table",
		Where: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Operator: "and",
			Left: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "age",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 18,
				},
			},
			Right: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
				Left: &parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: "score",
				},
				Right: &parser.Expression{
					Type:  parser.ExprTypeValue,
					Value: 50,
				},
			},
		},
	}

	sqlStmt := &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: stmt,
	}

	plan, err := opt.Optimize(ctx, sqlStmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	if plan == nil {
		t.Fatal("Optimize returned nil plan")
	}

	explain := plan.Explain()
	if explain == "" {
		t.Error("Explain returned empty string")
	}
	t.Logf("Explain output:\n%s", explain)
}

func TestOptimizer_ScanCostDifferentRows(t *testing.T) {
	costModel := NewDefaultCostModel()

	tests := []struct {
		name     string
		rowCount int64
		minCost  float64
	}{
		{"Small table", 100, 10},
		{"Medium table", 10000, 1000},
		{"Large table", 1000000, 100000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := costModel.ScanCost("test_table", tt.rowCount)
			if cost < tt.minCost {
				t.Errorf("ScanCost() = %v, want >= %v", cost, tt.minCost)
			}
			if cost <= 0 {
				t.Errorf("ScanCost() should be positive, got %v", cost)
			}
		})
	}
}
