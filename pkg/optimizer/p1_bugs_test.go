package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// =====================================================================
// P1-1: != selectivity should differ from = selectivity
// In cardinality.go, "!=" was handled in the same case as "=",
// returning 1/NDV. For "!=", correct selectivity is (NDV-1)/NDV.
// =====================================================================
func TestNotEqualSelectivityDiffersFromEqual(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()
	estimator.UpdateStatistics("users", &TableStatistics{
		Name:     "users",
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"status": {
				Name:          "status",
				DistinctCount: 10,
				MinValue:      1,
				MaxValue:      10,
			},
		},
	})

	eqFilter := domain.Filter{Field: "status", Operator: "=", Value: 5}
	neqFilter := domain.Filter{Field: "status", Operator: "!=", Value: 5}

	eqSel := estimator.estimateFilterSelectivity("users", eqFilter)
	neqSel := estimator.estimateFilterSelectivity("users", neqFilter)

	// = selectivity should be 1/NDV = 0.1
	if eqSel < 0.09 || eqSel > 0.11 {
		t.Errorf("Expected = selectivity ~0.1, got %f", eqSel)
	}

	// != selectivity should be (NDV-1)/NDV = 0.9
	if neqSel < 0.85 || neqSel > 0.95 {
		t.Errorf("Expected != selectivity ~0.9, got %f", neqSel)
	}

	// They must NOT be equal
	if eqSel == neqSel {
		t.Errorf("= and != selectivity should differ, both are %f", eqSel)
	}
}

// =====================================================================
// P1-2: OR selectivity should use inclusion-exclusion principle
// The code was using naive sum which can exceed 1.0 (clamped to 0.95).
// Correct formula: 1 - (1-s1)*(1-s2)*...*(1-sn)
// =====================================================================
func TestOrSelectivityInclusionExclusion(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()
	estimator.UpdateStatistics("orders", &TableStatistics{
		Name:     "orders",
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"status": {
				Name:          "status",
				DistinctCount: 4,
				MinValue:      1,
				MaxValue:      4,
			},
		},
	})

	// OR of two = conditions with NDV=4: each has sel=0.25
	// Naive sum: 0.25+0.25 = 0.5
	// Inclusion-exclusion: 1 - (1-0.25)*(1-0.25) = 1 - 0.5625 = 0.4375
	orFilter := domain.Filter{
		LogicOp: "OR",
		SubFilters: []domain.Filter{
			{Field: "status", Operator: "=", Value: 1},
			{Field: "status", Operator: "=", Value: 2},
		},
	}

	sel := estimator.estimateLogicSelectivity("orders", orFilter)
	// Should be 0.4375 (inclusion-exclusion), not 0.5 (naive sum)
	if sel > 0.46 {
		t.Errorf("Expected OR selectivity ~0.4375 (inclusion-exclusion), got %f (likely naive sum)", sel)
	}

	// Test with many sub-filters: naive sum would exceed 1.0
	manyOrFilter := domain.Filter{
		LogicOp: "OR",
		SubFilters: []domain.Filter{
			{Field: "status", Operator: "=", Value: 1},
			{Field: "status", Operator: "=", Value: 2},
			{Field: "status", Operator: "=", Value: 3},
			{Field: "status", Operator: "=", Value: 4},
		},
	}
	selMany := estimator.estimateLogicSelectivity("orders", manyOrFilter)
	// 1 - (0.75)^4 = 1 - 0.3164 = 0.6836
	if selMany > 0.75 {
		t.Errorf("Expected OR selectivity ~0.68 (inclusion-exclusion), got %f", selMany)
	}
	// Must never exceed 1.0
	if selMany > 1.0 {
		t.Errorf("OR selectivity must never exceed 1.0, got %f", selMany)
	}
}

// =====================================================================
// P1-3: AVG of empty group should return nil (SQL NULL), not 0.0
// =====================================================================
func TestAggregateAvgEmptyGroupReturnsNil(t *testing.T) {
	agg := &OptimizedAggregate{
		AggFuncs: []*AggregationItem{
			{Type: Avg, Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "value"}, Alias: "avg_val"},
		},
		GroupByCols: []string{"group_id"},
	}

	// Empty aggregate state: no rows contributed to this group's AVG
	state := &aggregateState{
		key:    int64(1),
		count:  0,
		perAgg: make([]perAggState, 1), // avgCount=0, avgSum=0
	}

	result := agg.calculateFinalAggregation(0, agg.AggFuncs[0], state)
	if result != nil {
		t.Errorf("Expected nil (SQL NULL) for AVG of empty group, got %v", result)
	}
}

// =====================================================================
// P1-4: Slice corruption in enhanced_predicate_pushdown.go
// append(existingSelection.Conditions(), ...) may mutate the internal slice
// =====================================================================
func TestCreateOrMergeSelectionNoSliceCorruption(t *testing.T) {
	rule := &EnhancedPredicatePushdownRule{}

	// Create a selection with some conditions
	innerDS := NewLogicalDataSource("t1", nil)
	cond1 := &parser.Expression{Type: parser.ExprTypeColumn, Column: "a"}
	cond2 := &parser.Expression{Type: parser.ExprTypeColumn, Column: "b"}
	existingSel := NewLogicalSelection([]*parser.Expression{cond1, cond2}, innerDS)

	// Save original conditions count
	origLen := len(existingSel.Conditions())

	// Merge with new conditions
	cond3 := &parser.Expression{Type: parser.ExprTypeColumn, Column: "c"}
	_ = rule.createOrMergeSelection(existingSel, []*parser.Expression{cond3})

	// Original selection's conditions should NOT be modified
	afterConds := existingSel.Conditions()
	if len(afterConds) != origLen {
		t.Errorf("Original selection conditions were corrupted: had %d, now has %d", origLen, len(afterConds))
	}
}

// =====================================================================
// P1-7: getDefaultSelectivity should differentiate != from =
// =====================================================================
func TestGetDefaultSelectivityNeqDiffersFromEq(t *testing.T) {
	estimator := NewSimpleCardinalityEstimator()

	eqDefault := estimator.getDefaultSelectivity("=")
	neqDefault := estimator.getDefaultSelectivity("!=")

	if eqDefault == neqDefault {
		t.Errorf("Default selectivity for = and != should differ, both are %f", eqDefault)
	}

	// != should have higher selectivity (more rows match)
	if neqDefault <= eqDefault {
		t.Errorf("Default != selectivity (%f) should be higher than = (%f)", neqDefault, eqDefault)
	}
}
