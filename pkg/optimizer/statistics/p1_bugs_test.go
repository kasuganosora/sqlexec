package statistics

import (
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// =====================================================================
// P1-6a: != selectivity should differ from = in estimator.go
// =====================================================================
func TestEstimator_NotEqualSelectivityDiffersFromEqual(t *testing.T) {
	cache := NewStatisticsCache(5 * time.Minute)
	estimator := NewEnhancedCardinalityEstimator(cache)
	estimator.UpdateStatistics("users", &TableStatistics{
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"status": {
				DistinctCount: 10,
				MinValue:      float64(1),
				MaxValue:      float64(10),
			},
		},
		Histograms: make(map[string]*Histogram),
	})

	eqFilter := domain.Filter{Field: "status", Operator: "=", Value: float64(5)}
	neqFilter := domain.Filter{Field: "status", Operator: "!=", Value: float64(5)}

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

	if eqSel == neqSel {
		t.Errorf("= and != selectivity should differ, both are %f", eqSel)
	}
}

// =====================================================================
// P1-6b: OR selectivity should use inclusion-exclusion in estimator.go
// =====================================================================
func TestEstimator_OrSelectivityInclusionExclusion(t *testing.T) {
	cache := NewStatisticsCache(5 * time.Minute)
	estimator := NewEnhancedCardinalityEstimator(cache)
	estimator.UpdateStatistics("orders", &TableStatistics{
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"status": {
				DistinctCount: 4,
				MinValue:      float64(1),
				MaxValue:      float64(4),
			},
		},
		Histograms: make(map[string]*Histogram),
	})

	// OR of two = conditions with NDV=4: each has sel=0.25
	orFilter := domain.Filter{
		LogicOp: "OR",
		SubFilters: []domain.Filter{
			{Field: "status", Operator: "=", Value: float64(1)},
			{Field: "status", Operator: "=", Value: float64(2)},
		},
	}

	sel := estimator.estimateLogicSelectivity("orders", orFilter)
	// Should be 0.4375 (inclusion-exclusion), not 0.5 (naive sum)
	if sel > 0.46 {
		t.Errorf("Expected OR selectivity ~0.4375, got %f (likely naive sum)", sel)
	}
}

// =====================================================================
// P1-6c: estimateOrSelectivity should also use inclusion-exclusion
// =====================================================================
func TestEstimator_EstimateOrSelectivityInclusionExclusion(t *testing.T) {
	cache := NewStatisticsCache(5 * time.Minute)
	estimator := NewEnhancedCardinalityEstimator(cache)
	estimator.UpdateStatistics("orders", &TableStatistics{
		RowCount: 1000,
		ColumnStats: map[string]*ColumnStatistics{
			"status": {
				DistinctCount: 4,
				MinValue:      float64(1),
				MaxValue:      float64(4),
			},
		},
		Histograms: make(map[string]*Histogram),
	})

	orFilter := domain.Filter{
		LogicOp: "OR",
		SubFilters: []domain.Filter{
			{Field: "status", Operator: "=", Value: float64(1)},
			{Field: "status", Operator: "=", Value: float64(2)},
		},
	}

	sel := estimator.estimateOrSelectivity("orders", orFilter)
	// Should be 0.4375 (inclusion-exclusion), not 0.5 (naive sum)
	if sel > 0.46 {
		t.Errorf("Expected OR selectivity ~0.4375, got %f (likely naive sum)", sel)
	}
}

// =====================================================================
// P1-7: getDefaultSelectivity should differentiate != from = in estimator
// =====================================================================
func TestEstimator_GetDefaultSelectivityNeqDiffersFromEq(t *testing.T) {
	cache := NewStatisticsCache(5 * time.Minute)
	estimator := NewEnhancedCardinalityEstimator(cache)

	eqDefault := estimator.getDefaultSelectivity("=")
	neqDefault := estimator.getDefaultSelectivity("!=")

	if eqDefault == neqDefault {
		t.Errorf("Default selectivity for = and != should differ, both are %f", eqDefault)
	}

	if neqDefault <= eqDefault {
		t.Errorf("Default != selectivity (%f) should be higher than = (%f)", neqDefault, eqDefault)
	}
}
