package optimizer

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

func TestHintAwareJoinReorderRule_Name(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	name := rule.Name()
	if name != "HintAwareJoinReorder" {
		t.Errorf("Expected rule name 'HintAwareJoinReorder', got '%s'", name)
	}
}

func TestHintAwareJoinReorderRule_Match(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	// Should match LogicalJoin
	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	if !rule.Match(join) {
		t.Error("Rule should match LogicalJoin")
	}

	// Should not match other plan types
	selection := NewLogicalSelection([]*parser.Expression{}, leftDS)
	if rule.Match(selection) {
		t.Error("Rule should not match LogicalSelection")
	}
}

func TestHintAwareJoinReorderRule_Apply_NoHints(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	optCtx := &OptimizationContext{}

	_, err := rule.Apply(context.Background(), join, optCtx)
	if err != nil {
		t.Errorf("Apply should not fail with no hints: %v", err)
	}
}

func TestHintAwareJoinTypeHintRule_Name(t *testing.T) {
	rule := NewHintAwareJoinTypeHintRule()

	name := rule.Name()
	if name != "HintAwareJoinTypeHint" {
		t.Errorf("Expected rule name 'HintAwareJoinTypeHint', got '%s'", name)
	}
}

func TestHintAwareJoinTypeHintRule_Match(t *testing.T) {
	rule := NewHintAwareJoinTypeHintRule()

	// Should match LogicalJoin
	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	if !rule.Match(join) {
		t.Error("Rule should match LogicalJoin")
	}
}

func TestHintAwareJoinTypeHintRule_Apply_NoHints(t *testing.T) {
	rule := NewHintAwareJoinTypeHintRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	optCtx := &OptimizationContext{}

	result, err := rule.Apply(context.Background(), join, optCtx)
	if err != nil {
		t.Errorf("Apply should not fail with no hints: %v", err)
	}

	if result == nil {
		t.Error("Apply should return a plan")
	}
}

func TestHintAwareJoinReorderRule_CollectTables(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	// Create a simple join
	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	tables := rule.collectTables(join)

	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}

	// Check table names
	tableMap := make(map[string]bool)
	for _, table := range tables {
		tableMap[table] = true
	}

	if !tableMap["t1"] {
		t.Error("Should have table 't1'")
	}
	if !tableMap["t2"] {
		t.Error("Should have table 't2'")
	}
}

func TestHintAwareJoinTypeHintRule_CollectTables(t *testing.T) {
	rule := NewHintAwareJoinTypeHintRule()

	// Create a simple join
	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	tables := rule.collectTables(join)

	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
}

func TestHintAwareJoinTypeHintRule_ShouldApplyHint(t *testing.T) {
	rule := NewHintAwareJoinTypeHintRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	tests := []struct {
		name        string
		hintTables  []string
		shouldApply bool
	}{
		{
			name:        "Table in hint list",
			hintTables:  []string{"t1", "t2"},
			shouldApply: true,
		},
		{
			name:        "Partial match",
			hintTables:  []string{"t1"},
			shouldApply: true,
		},
		{
			name:        "No match",
			hintTables:  []string{"t3", "t4"},
			shouldApply: false,
		},
		{
			name:        "Empty hint",
			hintTables:  []string{},
			shouldApply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.shouldApplyHint(join, tt.hintTables)
			if result != tt.shouldApply {
				t.Errorf("Expected shouldApply=%v, got %v", tt.shouldApply, result)
			}
		})
	}
}

func TestHintAwareJoinReorderRule_IsTableAlias(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	// Simplified test - in full implementation, would test actual alias logic
	table1 := "t1"
	table2 := "t2"

	// Same name
	if !rule.isTableAlias(table1, table1) {
		t.Error("Table should be considered alias of itself")
	}

	// Different names
	if rule.isTableAlias(table1, table2) {
		t.Error("Different tables should not be considered aliases")
	}
}

func TestHintAwareJoinReorderRule_CollectTablesRecursive(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	// Create a nested structure: Selection -> Join -> DataSource
	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})
	selection := NewLogicalSelection([]*parser.Expression{}, join)

	tables := make([]string, 0)
	rule.collectTablesRecursive(selection, &tables)

	// Should collect both tables from the join
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables from recursive collection, got %d", len(tables))
	}
}

func TestHintAwareJoinTypeHintRule_CollectTablesRecursive(t *testing.T) {
	rule := NewHintAwareJoinTypeHintRule()

	// Create a nested structure
	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	tables := make([]string, 0)
	rule.collectTablesRecursive(join, &tables)

	// Should collect both tables
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables from recursive collection, got %d", len(tables))
	}
}

func TestHintAwareJoinReorderRule_StraightJoin(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	plan, err := rule.applyStraightJoin(join)
	if err != nil {
		t.Errorf("applyStraightJoin should not fail: %v", err)
	}

	if plan == nil {
		t.Error("applyStraightJoin should return a plan")
	}
}

func TestHintAwareJoinReorderRule_ApplyLeadingOrder(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	tests := []struct {
		name         string
		leadingOrder []string
		shouldError  bool
	}{
		{
			name:         "Complete leading order",
			leadingOrder: []string{"t1", "t2"},
			shouldError:  false,
		},
		{
			name:         "Single table",
			leadingOrder: []string{"t1"},
			shouldError:  false,
		},
		{
			name:         "Incomplete leading order",
			leadingOrder: []string{"t1", "t3"},
			shouldError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rule.applyLeadingOrder(join, tt.leadingOrder)
			if tt.shouldError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("applyLeadingOrder should not fail: %v", err)
				}
				if result == nil {
					t.Error("applyLeadingOrder should return a plan")
				}
			}
		})
	}
}

func TestHintAwareJoinReorderRule_ApplyWithHints_Leading(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	optCtx := &OptimizationContext{}

	// Test with LEADING hint
	hints := &OptimizerHints{
		LeadingOrder: []string{"t1", "t2"},
	}

	result, err := rule.ApplyWithHints(context.Background(), join, optCtx, hints)
	if err != nil {
		t.Errorf("ApplyWithHints should not fail: %v", err)
	}

	if result == nil {
		t.Error("ApplyWithHints should return a plan")
	}
}

func TestHintAwareJoinTypeHintRule_ApplyWithHints(t *testing.T) {
	rule := NewHintAwareJoinTypeHintRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	optCtx := &OptimizationContext{}

	tests := []struct {
		name  string
		hints *OptimizerHints
	}{
		{
			name: "HASH_JOIN hint",
			hints: &OptimizerHints{
				HashJoinTables: []string{"t1", "t2"},
			},
		},
		{
			name: "MERGE_JOIN hint",
			hints: &OptimizerHints{
				MergeJoinTables: []string{"t1", "t2"},
			},
		},
		{
			name: "INL_JOIN hint",
			hints: &OptimizerHints{
				INLJoinTables: []string{"t1", "t2"},
			},
		},
		{
			name: "INL_HASH_JOIN hint",
			hints: &OptimizerHints{
				INLHashJoinTables: []string{"t1", "t2"},
			},
		},
		{
			name: "INL_MERGE_JOIN hint",
			hints: &OptimizerHints{
				INLMergeJoinTables: []string{"t1", "t2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rule.ApplyWithHints(context.Background(), join, optCtx, tt.hints)
			if err != nil {
				t.Errorf("ApplyWithHints should not fail: %v", err)
			}

			if result == nil {
				t.Error("ApplyWithHints should return a plan")
			}
		})
	}
}

func TestHintAwareJoinReorderRule_ApplyNegativeHints(t *testing.T) {
	rule := NewHintAwareJoinReorderRule()

	leftDS := NewLogicalDataSource("t1", nil)
	rightDS := NewLogicalDataSource("t2", nil)
	join := NewLogicalJoin(InnerJoin, leftDS, rightDS, []*JoinCondition{})

	tests := []struct {
		name  string
		hints *OptimizerHints
	}{
		{
			name: "NO_HASH_JOIN hint",
			hints: &OptimizerHints{
				NoHashJoinTables: []string{"t1", "t2"},
			},
		},
		{
			name: "NO_MERGE_JOIN hint",
			hints: &OptimizerHints{
				NoMergeJoinTables: []string{"t1", "t2"},
			},
		},
		{
			name: "NO_INDEX_JOIN hint",
			hints: &OptimizerHints{
				NoIndexJoinTables: []string{"t1", "t2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rule.applyNegativeHints(join, tt.hints)
			if err != nil {
				t.Errorf("applyNegativeHints should not fail: %v", err)
			}
		})
	}
}
