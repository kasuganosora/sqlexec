package builtin

import (
	"testing"
	"time"
)

// almostEqual compares two float64 values within an epsilon tolerance.
func almostEqual(a, b, epsilon float64) bool {
	if a == b {
		return true
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < epsilon
}

// --- Tests for Advanced Aggregate Functions ---

func TestAdvancedAggCorr(t *testing.T) {
	// Perfect positive correlation: y = 2x
	ctx := NewAggregateContext()
	xVals := []float64{1, 2, 3, 4, 5}
	yVals := []float64{2, 4, 6, 8, 10}

	for i := range xVals {
		err := aggCorr(ctx, []interface{}{xVals[i], yVals[i]})
		if err != nil {
			t.Fatalf("aggCorr() error = %v", err)
		}
	}

	result, err := aggCorrResult(ctx)
	if err != nil {
		t.Fatalf("aggCorrResult() error = %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !almostEqual(result.(float64), 1.0, 1e-9) {
		t.Errorf("corr = %v, want 1.0", result)
	}
}

func TestAdvancedAggCorrNegative(t *testing.T) {
	// Perfect negative correlation: y = -x
	ctx := NewAggregateContext()
	xVals := []float64{1, 2, 3, 4, 5}
	yVals := []float64{-1, -2, -3, -4, -5}

	for i := range xVals {
		err := aggCorr(ctx, []interface{}{xVals[i], yVals[i]})
		if err != nil {
			t.Fatalf("aggCorr() error = %v", err)
		}
	}

	result, err := aggCorrResult(ctx)
	if err != nil {
		t.Fatalf("aggCorrResult() error = %v", err)
	}
	if !almostEqual(result.(float64), -1.0, 1e-9) {
		t.Errorf("corr = %v, want -1.0", result)
	}
}

func TestAdvancedAggCorrInsufficient(t *testing.T) {
	// Less than 2 points returns nil
	ctx := NewAggregateContext()
	_ = aggCorr(ctx, []interface{}{1.0, 2.0})

	result, err := aggCorrResult(ctx)
	if err != nil {
		t.Fatalf("aggCorrResult() error = %v", err)
	}
	// With only one point, cannot compute correlation
	// Our implementation requires n >= 2
	// With 1 data point, denom is 0, so returns nil
	if result != nil {
		t.Errorf("expected nil for single data point, got %v", result)
	}
}

func TestAdvancedAggCovarPop(t *testing.T) {
	ctx := NewAggregateContext()
	// x = {1, 2, 3, 4, 5}, y = {2, 4, 6, 8, 10}
	// meanX = 3, meanY = 6
	// covar_pop = sum((x-3)(y-6))/5 = ((1-3)(2-6)+(2-3)(4-6)+(3-3)(6-6)+(4-3)(8-6)+(5-3)(10-6))/5
	// = (8+2+0+2+8)/5 = 20/5 = 4.0
	xVals := []float64{1, 2, 3, 4, 5}
	yVals := []float64{2, 4, 6, 8, 10}

	for i := range xVals {
		err := aggCovarPop(ctx, []interface{}{xVals[i], yVals[i]})
		if err != nil {
			t.Fatalf("aggCovarPop() error = %v", err)
		}
	}

	result, err := aggCovarPopResult(ctx)
	if err != nil {
		t.Fatalf("aggCovarPopResult() error = %v", err)
	}
	if !almostEqual(result.(float64), 4.0, 1e-9) {
		t.Errorf("covar_pop = %v, want 4.0", result)
	}
}

func TestAdvancedAggCovarSamp(t *testing.T) {
	ctx := NewAggregateContext()
	// Same data as above, covar_samp = 20/4 = 5.0
	xVals := []float64{1, 2, 3, 4, 5}
	yVals := []float64{2, 4, 6, 8, 10}

	for i := range xVals {
		err := aggCovarSamp(ctx, []interface{}{xVals[i], yVals[i]})
		if err != nil {
			t.Fatalf("aggCovarSamp() error = %v", err)
		}
	}

	result, err := aggCovarSampResult(ctx)
	if err != nil {
		t.Fatalf("aggCovarSampResult() error = %v", err)
	}
	if !almostEqual(result.(float64), 5.0, 1e-9) {
		t.Errorf("covar_samp = %v, want 5.0", result)
	}
}

func TestAdvancedAggCovarSampInsufficient(t *testing.T) {
	ctx := NewAggregateContext()
	_ = aggCovarSamp(ctx, []interface{}{1.0, 2.0})
	result, err := aggCovarSampResult(ctx)
	if err != nil {
		t.Fatalf("aggCovarSampResult() error = %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for single data point, got %v", result)
	}
}

func TestAdvancedAggSkewness(t *testing.T) {
	// Symmetric data => skewness = 0
	ctx := NewAggregateContext()
	vals := []float64{1, 2, 3, 4, 5}
	for _, v := range vals {
		err := aggSkewness(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggSkewness() error = %v", err)
		}
	}

	result, err := aggSkewnessResult(ctx)
	if err != nil {
		t.Fatalf("aggSkewnessResult() error = %v", err)
	}
	if !almostEqual(result.(float64), 0.0, 1e-9) {
		t.Errorf("skewness = %v, want 0.0", result)
	}
}

func TestAdvancedAggSkewnessPositive(t *testing.T) {
	// Right-skewed data
	ctx := NewAggregateContext()
	vals := []float64{1, 1, 1, 2, 5}
	for _, v := range vals {
		err := aggSkewness(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggSkewness() error = %v", err)
		}
	}

	result, err := aggSkewnessResult(ctx)
	if err != nil {
		t.Fatalf("aggSkewnessResult() error = %v", err)
	}
	if result.(float64) <= 0 {
		t.Errorf("expected positive skewness for right-skewed data, got %v", result)
	}
}

func TestAdvancedAggSkewnessInsufficient(t *testing.T) {
	ctx := NewAggregateContext()
	_ = aggSkewness(ctx, []interface{}{1.0})
	_ = aggSkewness(ctx, []interface{}{2.0})
	result, err := aggSkewnessResult(ctx)
	if err != nil {
		t.Fatalf("aggSkewnessResult() error = %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for fewer than 3 values, got %v", result)
	}
}

func TestAdvancedAggKurtosis(t *testing.T) {
	// Uniform distribution {1,2,3,4,5}
	// Population excess kurtosis for uniform = -1.3
	ctx := NewAggregateContext()
	vals := []float64{1, 2, 3, 4, 5}
	for _, v := range vals {
		err := aggKurtosis(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggKurtosis() error = %v", err)
		}
	}

	result, err := aggKurtosisResult(ctx)
	if err != nil {
		t.Fatalf("aggKurtosisResult() error = %v", err)
	}
	// For {1,2,3,4,5}: m2=2, m4=6.8, kurtosis = 6.8/4 - 3 = -1.3
	if !almostEqual(result.(float64), -1.3, 0.01) {
		t.Errorf("kurtosis = %v, want -1.3", result)
	}
}

func TestAdvancedAggKurtosisInsufficient(t *testing.T) {
	ctx := NewAggregateContext()
	for _, v := range []float64{1, 2, 3} {
		_ = aggKurtosis(ctx, []interface{}{v})
	}
	result, err := aggKurtosisResult(ctx)
	if err != nil {
		t.Fatalf("aggKurtosisResult() error = %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for fewer than 4 values, got %v", result)
	}
}

func TestAdvancedAggMode(t *testing.T) {
	ctx := NewAggregateContext()
	vals := []interface{}{"apple", "banana", "apple", "cherry", "apple", "banana"}
	for _, v := range vals {
		err := aggMode(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggMode() error = %v", err)
		}
	}

	result, err := aggModeResult(ctx)
	if err != nil {
		t.Fatalf("aggModeResult() error = %v", err)
	}
	if result != "apple" {
		t.Errorf("mode = %v, want 'apple'", result)
	}
}

func TestAdvancedAggModeNumeric(t *testing.T) {
	ctx := NewAggregateContext()
	vals := []interface{}{1, 2, 2, 3, 3, 3}
	for _, v := range vals {
		err := aggMode(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggMode() error = %v", err)
		}
	}

	result, err := aggModeResult(ctx)
	if err != nil {
		t.Fatalf("aggModeResult() error = %v", err)
	}
	if result != "3" {
		t.Errorf("mode = %v, want '3'", result)
	}
}

func TestAdvancedAggModeEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggModeResult(ctx)
	if err != nil {
		t.Fatalf("aggModeResult() error = %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

func TestAdvancedAggEntropy(t *testing.T) {
	// Two equally likely values => entropy = log2(2) = 1.0
	ctx := NewAggregateContext()
	vals := []interface{}{"a", "b", "a", "b"}
	for _, v := range vals {
		err := aggEntropy(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggEntropy() error = %v", err)
		}
	}

	result, err := aggEntropyResult(ctx)
	if err != nil {
		t.Fatalf("aggEntropyResult() error = %v", err)
	}
	if !almostEqual(result.(float64), 1.0, 1e-9) {
		t.Errorf("entropy = %v, want 1.0", result)
	}
}

func TestAdvancedAggEntropyUniform(t *testing.T) {
	// Four equally likely values => entropy = log2(4) = 2.0
	ctx := NewAggregateContext()
	vals := []interface{}{"a", "b", "c", "d"}
	for _, v := range vals {
		err := aggEntropy(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggEntropy() error = %v", err)
		}
	}

	result, err := aggEntropyResult(ctx)
	if err != nil {
		t.Fatalf("aggEntropyResult() error = %v", err)
	}
	if !almostEqual(result.(float64), 2.0, 1e-9) {
		t.Errorf("entropy = %v, want 2.0", result)
	}
}

func TestAdvancedAggEntropySingleValue(t *testing.T) {
	// All same value => entropy = 0
	ctx := NewAggregateContext()
	vals := []interface{}{"x", "x", "x"}
	for _, v := range vals {
		err := aggEntropy(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggEntropy() error = %v", err)
		}
	}

	result, err := aggEntropyResult(ctx)
	if err != nil {
		t.Fatalf("aggEntropyResult() error = %v", err)
	}
	if !almostEqual(result.(float64), 0.0, 1e-9) {
		t.Errorf("entropy = %v, want 0.0", result)
	}
}

func TestAdvancedAggEntropyEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggEntropyResult(ctx)
	if err != nil {
		t.Fatalf("aggEntropyResult() error = %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

func TestAdvancedAggApproxCountDistinct(t *testing.T) {
	ctx := NewAggregateContext()
	vals := []interface{}{"a", "b", "c", "a", "b", "a"}
	for _, v := range vals {
		err := aggApproxCountDistinct(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggApproxCountDistinct() error = %v", err)
		}
	}

	result, err := aggApproxCountDistinctResult(ctx)
	if err != nil {
		t.Fatalf("aggApproxCountDistinctResult() error = %v", err)
	}
	if result != int64(3) {
		t.Errorf("approx_count_distinct = %v, want 3", result)
	}
}

func TestAdvancedAggApproxCountDistinctNumeric(t *testing.T) {
	ctx := NewAggregateContext()
	vals := []interface{}{1, 2, 3, 4, 5, 1, 2, 3}
	for _, v := range vals {
		err := aggApproxCountDistinct(ctx, []interface{}{v})
		if err != nil {
			t.Fatalf("aggApproxCountDistinct() error = %v", err)
		}
	}

	result, err := aggApproxCountDistinctResult(ctx)
	if err != nil {
		t.Fatalf("aggApproxCountDistinctResult() error = %v", err)
	}
	if result != int64(5) {
		t.Errorf("approx_count_distinct = %v, want 5", result)
	}
}

func TestAdvancedAggApproxCountDistinctEmpty(t *testing.T) {
	ctx := NewAggregateContext()
	result, err := aggApproxCountDistinctResult(ctx)
	if err != nil {
		t.Fatalf("aggApproxCountDistinctResult() error = %v", err)
	}
	if result != int64(0) {
		t.Errorf("approx_count_distinct = %v, want 0", result)
	}
}

func TestAdvancedAggNullHandling(t *testing.T) {
	// Verify that nil args are skipped
	t.Run("corr_nil", func(t *testing.T) {
		ctx := NewAggregateContext()
		err := aggCorr(ctx, []interface{}{nil, 1.0})
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		st := getAdvState(ctx)
		if len(st.pairsX) != 0 {
			t.Errorf("expected no pairs for nil x, got %d", len(st.pairsX))
		}
	})

	t.Run("skewness_nil", func(t *testing.T) {
		ctx := NewAggregateContext()
		err := aggSkewness(ctx, []interface{}{nil})
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		st := getAdvState(ctx)
		if len(st.values) != 0 {
			t.Errorf("expected no values for nil, got %d", len(st.values))
		}
	})

	t.Run("mode_nil", func(t *testing.T) {
		ctx := NewAggregateContext()
		err := aggMode(ctx, []interface{}{nil})
		if err != nil {
			t.Fatalf("error = %v", err)
		}
		st := getAdvState(ctx)
		if len(st.freqMap) != 0 {
			t.Errorf("expected empty freq map for nil, got %d", len(st.freqMap))
		}
	})
}

func TestAdvancedAggRegistration(t *testing.T) {
	// Verify all advanced aggregates are registered
	names := []string{"corr", "covar_pop", "covar_samp", "skewness", "kurtosis",
		"mode", "entropy", "approx_count_distinct"}
	for _, name := range names {
		_, exists := GetAggregate(name)
		if !exists {
			t.Errorf("aggregate function %q should be registered", name)
		}
	}
}

// --- Tests for Batch 9 Advanced Date Functions ---

func TestDateAge(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want string
	}{
		{"same date", "2024-01-01", "2024-01-01", "0 years 0 months 0 days"},
		{"one year", "2025-01-01", "2024-01-01", "1 years 0 months 0 days"},
		{"mixed", "2026-03-15", "2024-01-01", "2 years 2 months 14 days"},
		{"reversed", "2024-01-01", "2026-03-15", "-2 years 2 months 14 days"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := dateAge([]interface{}{tt.a, tt.b})
			if err != nil {
				t.Fatalf("dateAge() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("dateAge(%s, %s) = %v, want %v", tt.a, tt.b, result, tt.want)
			}
		})
	}
}

func TestDateTimeBucket(t *testing.T) {
	tests := []struct {
		name     string
		interval int64
		ts       string
		want     string
	}{
		{"hour bucket", 3600, "2024-01-01 12:34:56", "2024-01-01 12:00:00"},
		{"15min bucket", 900, "2024-01-01 12:34:56", "2024-01-01 12:30:00"},
		{"day bucket", 86400, "2024-01-01 12:34:56", "2024-01-01 00:00:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := dateTimeBucket([]interface{}{tt.interval, tt.ts})
			if err != nil {
				t.Fatalf("dateTimeBucket() error = %v", err)
			}
			rt := result.(time.Time)
			got := rt.Format("2006-01-02 15:04:05")
			if got != tt.want {
				t.Errorf("time_bucket(%d, %s) = %v, want %v", tt.interval, tt.ts, got, tt.want)
			}
		})
	}
}

func TestDateCentury(t *testing.T) {
	tests := []struct {
		date string
		want int64
	}{
		{"2024-03-15", 21},
		{"2000-01-01", 20},
		{"2001-01-01", 21},
		{"1999-12-31", 20},
		{"0100-01-01", 1},
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			result, err := dateCentury([]interface{}{tt.date})
			if err != nil {
				t.Fatalf("dateCentury() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("century(%s) = %v, want %v", tt.date, result, tt.want)
			}
		})
	}
}

func TestDateDecade(t *testing.T) {
	tests := []struct {
		date string
		want int64
	}{
		{"2024-03-15", 202},
		{"1990-01-01", 199},
		{"2000-06-15", 200},
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			result, err := dateDecade([]interface{}{tt.date})
			if err != nil {
				t.Fatalf("dateDecade() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("decade(%s) = %v, want %v", tt.date, result, tt.want)
			}
		})
	}
}

func TestDateMillennium(t *testing.T) {
	tests := []struct {
		date string
		want int64
	}{
		{"2024-03-15", 3},
		{"2000-01-01", 2},
		{"2001-01-01", 3},
		{"1000-06-15", 1},
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			result, err := dateMillennium([]interface{}{tt.date})
			if err != nil {
				t.Fatalf("dateMillennium() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("millennium(%s) = %v, want %v", tt.date, result, tt.want)
			}
		})
	}
}

func TestDateEra(t *testing.T) {
	tests := []struct {
		date string
		want string
	}{
		{"2024-03-15", "AD"},
		{"0001-01-01", "AD"},
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			result, err := dateEra([]interface{}{tt.date})
			if err != nil {
				t.Fatalf("dateEra() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("era(%s) = %v, want %v", tt.date, result, tt.want)
			}
		})
	}
}

func TestDateIsoDow(t *testing.T) {
	tests := []struct {
		date string
		want int64
	}{
		{"2024-01-01", 1}, // Monday
		{"2024-01-07", 7}, // Sunday
		{"2024-01-06", 6}, // Saturday
		{"2024-01-03", 3}, // Wednesday
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			result, err := dateIsoDow([]interface{}{tt.date})
			if err != nil {
				t.Fatalf("dateIsoDow() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("isodow(%s) = %v, want %v", tt.date, result, tt.want)
			}
		})
	}
}

func TestDateIsoYear(t *testing.T) {
	tests := []struct {
		date string
		want int64
	}{
		{"2024-01-01", 2024},
		{"2024-12-30", 2025}, // This date belongs to ISO year 2025
		{"2025-01-01", 2025},
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			result, err := dateIsoYear([]interface{}{tt.date})
			if err != nil {
				t.Fatalf("dateIsoYear() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("isoyear(%s) = %v, want %v", tt.date, result, tt.want)
			}
		})
	}
}

func TestDateJulianDay(t *testing.T) {
	// Known Julian Day: 2000-01-01 12:00:00 UTC = JD 2451545.0
	result, err := dateJulianDay([]interface{}{"2000-01-01"})
	if err != nil {
		t.Fatalf("dateJulianDay() error = %v", err)
	}
	jd := result.(float64)
	// At midnight (00:00), JD = 2451544.5
	if !almostEqual(jd, 2451544.5, 0.01) {
		t.Errorf("julian_day('2000-01-01') = %v, want ~2451544.5", jd)
	}
}

func TestDateJulianDayNoon(t *testing.T) {
	// 2000-01-01 12:00:00 = JD 2451545.0
	result, err := dateJulianDay([]interface{}{"2000-01-01 12:00:00"})
	if err != nil {
		t.Fatalf("dateJulianDay() error = %v", err)
	}
	jd := result.(float64)
	if !almostEqual(jd, 2451545.0, 0.01) {
		t.Errorf("julian_day('2000-01-01 12:00:00') = %v, want ~2451545.0", jd)
	}
}

func TestDateYearWeek(t *testing.T) {
	tests := []struct {
		date string
		want int64
	}{
		{"2024-01-01", 202401},
		{"2024-03-15", 202411},
		{"2024-12-31", 202501}, // Dec 31, 2024 is in ISO week 1 of 2025
	}

	for _, tt := range tests {
		t.Run(tt.date, func(t *testing.T) {
			result, err := dateYearWeek([]interface{}{tt.date})
			if err != nil {
				t.Fatalf("dateYearWeek() error = %v", err)
			}
			if result != tt.want {
				t.Errorf("year_week(%s) = %v, want %v", tt.date, result, tt.want)
			}
		})
	}
}

func TestDateFunctionRegistration(t *testing.T) {
	// Verify all Batch 9 date functions are registered
	names := []string{"age", "time_bucket", "century", "decade", "millennium",
		"era", "isodow", "iso_day_of_week", "isoyear", "iso_year",
		"julian_day", "year_week"}
	for _, name := range names {
		_, exists := GetGlobal(name)
		if !exists {
			t.Errorf("date function %q should be registered", name)
		}
	}
}
