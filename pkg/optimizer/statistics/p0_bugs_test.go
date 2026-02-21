package statistics

import (
	"testing"
)

// =============================================================================
// P0-9: totalCount includes NullCount despite comment saying "excluding NULL"
// =============================================================================

func TestHistogram_TotalCountExcludesNull(t *testing.T) {
	h := &Histogram{
		NullCount: 10,
		Buckets: []*HistogramBucket{
			{Count: 100},
			{Count: 200},
		},
	}

	total := h.totalCount()

	// Should be 300 (100+200), NOT 310 (100+200+10)
	if total != 300 {
		t.Errorf("totalCount() = %d, expected 300 (excluding NullCount of 10)", total)
	}
}

// =============================================================================
// P0-8: Range selectivity inversion in statistics/estimator.go
// estimateRangeSelectivityUsingStats(operator, value, colStats)
// =============================================================================

func TestEstimateRangeSelectivity_BoundaryInversion(t *testing.T) {
	estimator := &EnhancedCardinalityEstimator{}

	// Column stats with range [10, 100]
	colStats := &ColumnStatistics{
		MinValue: float64(10),
		MaxValue: float64(100),
	}

	// Test: val > 5 when range is [10, 100]
	// ALL data satisfies > 5, so selectivity should be ~1.0
	sel := estimator.estimateRangeSelectivityUsingStats(">", float64(5), colStats)
	if sel < 0.9 {
		t.Errorf("> below min: expected ~1.0, got %f", sel)
	}

	// Test: val > 200 when range is [10, 100]
	// NO data satisfies > 200, selectivity should be ~0.0
	sel = estimator.estimateRangeSelectivityUsingStats(">", float64(200), colStats)
	if sel > 0.1 {
		t.Errorf("> above max: expected ~0.0, got %f", sel)
	}

	// Test: val < 200 when range is [10, 100]
	// ALL data satisfies < 200, selectivity should be ~1.0
	sel = estimator.estimateRangeSelectivityUsingStats("<", float64(200), colStats)
	if sel < 0.9 {
		t.Errorf("< above max: expected ~1.0, got %f", sel)
	}

	// Test: val < 5 when range is [10, 100]
	// NO data satisfies < 5, selectivity should be ~0.0
	sel = estimator.estimateRangeSelectivityUsingStats("<", float64(5), colStats)
	if sel > 0.1 {
		t.Errorf("< below min: expected ~0.0, got %f", sel)
	}
}
