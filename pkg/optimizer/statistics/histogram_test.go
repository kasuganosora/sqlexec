package statistics

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestBuildEquiWidthHistogram(t *testing.T) {
	tests := []struct {
		name        string
		values      []interface{}
		bucketCount int
		check       func(t *testing.T, hist *Histogram)
	}{
		{
			name:        "empty values",
			values:      []interface{}{},
			bucketCount: 10,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, EquiWidthHistogram, hist.Type)
				assert.Empty(t, hist.Buckets)
				assert.Equal(t, 10, hist.BucketCount)
			},
		},
		{
			name:        "all nil values",
			values:      []interface{}{nil, nil, nil},
			bucketCount: 10,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, EquiWidthHistogram, hist.Type)
				assert.Equal(t, int64(3), hist.NullCount)
				assert.Empty(t, hist.Buckets)
			},
		},
		{
			name:        "small numeric values",
			values:      []interface{}{1, 2, 3, 4, 5},
			bucketCount: 5,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, EquiWidthHistogram, hist.Type)
				assert.Equal(t, int64(1), int64(hist.MinValue.(int)))
				assert.Equal(t, int64(5), int64(hist.MaxValue.(int)))
				assert.Equal(t, int64(5), hist.NDV)
				assert.Len(t, hist.Buckets, 5)
			},
		},
		{
			name:        "large numeric values",
			values:      makeNumericValues(100),
			bucketCount: 10,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, EquiWidthHistogram, hist.Type)
				assert.GreaterOrEqual(t, hist.MinValue.(int64), int64(0))
				assert.LessOrEqual(t, hist.MaxValue.(int64), int64(99))
				assert.GreaterOrEqual(t, len(hist.Buckets), 1)
			},
		},
		{
			name:        "string values",
			values:      []interface{}{"apple", "banana", "cherry", "date", "fig"},
			bucketCount: 5,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, EquiWidthHistogram, hist.Type)
				assert.Equal(t, "apple", hist.MinValue.(string))
				assert.Equal(t, "fig", hist.MaxValue.(string))
				assert.Len(t, hist.Buckets, 5)
			},
		},
		{
			name:        "zero bucket count",
			values:      []interface{}{1, 2, 3, 4, 5},
			bucketCount: 0,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Len(t, hist.Buckets, 5) // should default to 10, but limited by values
			},
		},
		{
			name:        "bucket count larger than values",
			values:      []interface{}{1, 2, 3},
			bucketCount: 10,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Len(t, hist.Buckets, 3) // should be limited to value count
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hist := BuildEquiWidthHistogram(tt.values, tt.bucketCount)
			tt.check(t, hist)
		})
	}
}

func TestBuildFrequencyHistogram(t *testing.T) {
	tests := []struct {
		name        string
		values      []interface{}
		bucketCount int
		check       func(t *testing.T, hist *Histogram)
	}{
		{
			name:        "empty values",
			values:      []interface{}{},
			bucketCount: 10,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, FrequencyHistogram, hist.Type)
				assert.Empty(t, hist.Buckets)
			},
		},
		{
			name:        "uniform distribution",
			values:      []interface{}{1, 2, 3, 4, 5},
			bucketCount: 5,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, FrequencyHistogram, hist.Type)
				assert.Len(t, hist.Buckets, 5)
			},
		},
		{
			name: "skewed distribution",
			values: []interface{}{
				1, 1, 1, 1, 1, // many 1s
				2, 2, 2, // some 2s
				3, // few 3s
			},
			bucketCount: 3,
			check: func(t *testing.T, hist *Histogram) {
				assert.NotNil(t, hist)
				assert.Equal(t, FrequencyHistogram, hist.Type)
				assert.Len(t, hist.Buckets, 3)
				// First bucket should have highest count
				assert.Greater(t, hist.Buckets[0].Count, hist.Buckets[1].Count)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hist := BuildFrequencyHistogram(tt.values, tt.bucketCount)
			tt.check(t, hist)
		})
	}
}

func TestHistogram_EstimateSelectivity(t *testing.T) {
	tests := []struct {
		name      string
		histogram *Histogram
		filter    domain.Filter
		check     func(t *testing.T, selectivity float64)
	}{
		{
			name:      "nil histogram",
			histogram: nil,
			filter:    domain.Filter{Operator: "=", Value: 5},
			check: func(t *testing.T, sel float64) {
				assert.Equal(t, 0.1, sel, "default selectivity should be 0.1")
			},
		},
		{
			name:      "empty histogram",
			histogram: &Histogram{Buckets: []*HistogramBucket{}},
			filter:    domain.Filter{Operator: "=", Value: 5},
			check: func(t *testing.T, sel float64) {
				assert.Equal(t, 0.1, sel, "default selectivity should be 0.1")
			},
		},
		{
			name: "equality filter",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10, NDV: 10},
				},
				NDV: 10,
			},
			filter: domain.Filter{Operator: "=", Value: int64(5)},
			check: func(t *testing.T, sel float64) {
				assert.Greater(t, sel, 0.0, "selectivity should be positive")
				assert.LessOrEqual(t, sel, 1.0, "selectivity should be <= 1.0")
			},
		},
		{
			name: "greater than filter",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10},
					{LowerBound: int64(11), UpperBound: int64(20), Count: 10},
				},
				MinValue: int64(1),
				MaxValue: int64(20),
			},
			filter: domain.Filter{Operator: ">", Value: int64(10)},
			check: func(t *testing.T, sel float64) {
				assert.Greater(t, sel, 0.0, "selectivity should be positive")
				assert.LessOrEqual(t, sel, 1.0, "selectivity should be <= 1.0")
			},
		},
		{
			name: "IN filter",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10},
				},
				NDV: 10,
			},
			filter: domain.Filter{
				Operator: "IN",
				Value:    []interface{}{int64(1), int64(2), int64(3)},
			},
			check: func(t *testing.T, sel float64) {
				assert.GreaterOrEqual(t, sel, 0.0, "selectivity should be >= 0")
				assert.LessOrEqual(t, sel, 1.0, "selectivity should be <= 1.0")
			},
		},
		{
			name: "LIKE filter",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: "a", UpperBound: "z", Count: 10},
				},
			},
			filter: domain.Filter{Operator: "LIKE", Value: "abc%"},
			check: func(t *testing.T, sel float64) {
				assert.Equal(t, 0.25, sel, "LIKE default selectivity should be 0.25")
			},
		},
		{
			name: "BETWEEN filter",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10},
				},
				MinValue: int64(1),
				MaxValue: int64(10),
			},
			filter: domain.Filter{
				Operator: "BETWEEN",
				Value:    []interface{}{int64(3), int64(7)},
			},
			check: func(t *testing.T, sel float64) {
				assert.GreaterOrEqual(t, sel, 0.0, "selectivity should be >= 0")
				assert.LessOrEqual(t, sel, 1.0, "selectivity should be <= 1.0")
			},
		},
		{
			name: "unknown operator",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10},
				},
			},
			filter: domain.Filter{Operator: "UNKNOWN", Value: int64(5)},
			check: func(t *testing.T, sel float64) {
				assert.Equal(t, 0.5, sel, "unknown operator default selectivity should be 0.5")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectivity := tt.histogram.EstimateSelectivity(tt.filter)
			tt.check(t, selectivity)
		})
	}
}

func TestHistogram_EstimateEqualitySelectivity(t *testing.T) {
	tests := []struct {
		name         string
		histogram    *Histogram
		value        interface{}
		wantZero     bool
		wantPositive bool
	}{
		{
			name: "value in bucket",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10, NDV: 10},
				},
				NDV: 10,
			},
			value:        int64(5),
			wantZero:     false,
			wantPositive: true,
		},
		{
			name: "value not in bucket",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10, NDV: 10},
				},
				NDV: 10,
			},
			value:        int64(20),
			wantZero:     true,
			wantPositive: false,
		},
		{
			name: "nil value",
			histogram: &Histogram{
				Buckets: []*HistogramBucket{
					{LowerBound: int64(1), UpperBound: int64(10), Count: 10},
				},
			},
			value:        nil,
			wantZero:     true,
			wantPositive: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := tt.histogram.EstimateEqualitySelectivity(tt.value)
			if tt.wantZero {
				assert.Equal(t, 0.0, sel)
			}
			if tt.wantPositive {
				assert.Greater(t, sel, 0.0)
			}
		})
	}
}

func TestHistogram_EstimateRangeSelectivity(t *testing.T) {
	histogram := &Histogram{
		Buckets: []*HistogramBucket{
			{LowerBound: int64(1), UpperBound: int64(10), Count: 10},
			{LowerBound: int64(11), UpperBound: int64(20), Count: 10},
			{LowerBound: int64(21), UpperBound: int64(30), Count: 10},
		},
		MinValue: int64(1),
		MaxValue: int64(30),
		Type:     EquiWidthHistogram,
	}

	tests := []struct {
		name      string
		operator  string
		value     interface{}
		wantValid bool
	}{
		{
			name:      "greater than mid",
			operator:  ">",
			value:     int64(15),
			wantValid: true,
		},
		{
			name:      "greater than max",
			operator:  ">",
			value:     int64(30),
			wantValid: true,
		},
		{
			name:      "less than mid",
			operator:  "<",
			value:     int64(15),
			wantValid: true,
		},
		{
			name:      "less than min",
			operator:  "<",
			value:     int64(1),
			wantValid: true,
		},
		{
			name:      "non-numeric value",
			operator:  ">",
			value:     "string",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := histogram.estimateRangeSelectivity(tt.operator, tt.value)
			if tt.wantValid {
				assert.GreaterOrEqual(t, sel, 0.0, "selectivity should be >= 0")
				assert.LessOrEqual(t, sel, 1.0, "selectivity should be <= 1.0")
			} else {
				assert.Equal(t, 0.3, sel, "invalid value should return default selectivity")
			}
		})
	}
}

func TestHistogram_EstimateInSelectivity(t *testing.T) {
	histogram := &Histogram{
		Buckets: []*HistogramBucket{
			{LowerBound: int64(1), UpperBound: int64(10), Count: 10},
			{LowerBound: int64(11), UpperBound: int64(20), Count: 10},
		},
		NDV: 20,
	}

	tests := []struct {
		name      string
		values    []interface{}
		wantValid bool
	}{
		{
			name:      "single value",
			values:    []interface{}{int64(5)},
			wantValid: true,
		},
		{
			name:      "multiple values",
			values:    []interface{}{int64(5), int64(15), int64(25)},
			wantValid: true,
		},
		{
			name:      "with nil",
			values:    []interface{}{int64(5), nil, int64(15)},
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := histogram.estimateInSelectivity(tt.values)
			if tt.wantValid {
				assert.GreaterOrEqual(t, sel, 0.0, "selectivity should be >= 0")
				assert.LessOrEqual(t, sel, 1.0, "selectivity should be <= 1.0")
			}
		})
	}
}

func TestHistogram_IsValueInRange(t *testing.T) {
	tests := []struct {
		name     string
		histType HistogramType
		value    interface{}
		lower    interface{}
		upper    interface{}
		expected bool
	}{
		{
			name:     "equi width value inside",
			histType: EquiWidthHistogram,
			value:    int64(5),
			lower:    int64(1),
			upper:    int64(10),
			expected: true,
		},
		{
			name:     "equi width value at lower bound",
			histType: EquiWidthHistogram,
			value:    int64(1),
			lower:    int64(1),
			upper:    int64(10),
			expected: true,
		},
		{
			name:     "equi width value at upper bound",
			histType: EquiWidthHistogram,
			value:    int64(10),
			lower:    int64(1),
			upper:    int64(10),
			expected: true,
		},
		{
			name:     "equi width value outside",
			histType: EquiWidthHistogram,
			value:    int64(15),
			lower:    int64(1),
			upper:    int64(10),
			expected: false,
		},
		{
			name:     "nil value",
			histType: EquiWidthHistogram,
			value:    nil,
			lower:    int64(1),
			upper:    int64(10),
			expected: false,
		},
		{
			name:     "string values",
			histType: EquiWidthHistogram,
			value:    "banana",
			lower:    "apple",
			upper:    "cherry",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hist := &Histogram{Type: tt.histType}
			result := hist.isValueInRange(tt.value, tt.lower, tt.upper)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHistogram_TotalCount(t *testing.T) {
	tests := []struct {
		name     string
		hist     *Histogram
		expected int64
	}{
		{
			name: "with buckets and nulls",
			hist: &Histogram{
				Buckets: []*HistogramBucket{
					{Count: 10},
					{Count: 20},
				},
				NullCount: 5,
			},
			expected: 30, // totalCount excludes NullCount
		},
		{
			name: "only buckets",
			hist: &Histogram{
				Buckets: []*HistogramBucket{
					{Count: 10},
				},
				NullCount: 0,
			},
			expected: 10,
		},
		{
			name: "only nulls",
			hist: &Histogram{
				Buckets:   []*HistogramBucket{},
				NullCount: 100,
			},
			expected: 0, // totalCount excludes NullCount
		},
		{
			name: "empty",
			hist: &Histogram{
				Buckets:   []*HistogramBucket{},
				NullCount: 0,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := tt.hist.totalCount()
			assert.Equal(t, tt.expected, count)
		})
	}
}

func TestHistogram_Explain(t *testing.T) {
	tests := []struct {
		name     string
		hist     *Histogram
		contains []string
	}{
		{
			name:     "nil histogram",
			hist:     nil,
			contains: []string{"Empty Histogram"},
		},
		{
			name: "equi width histogram",
			hist: &Histogram{
				Type:        EquiWidthHistogram,
				Buckets:     []*HistogramBucket{{Count: 10}},
				BucketCount: 10,
				NDV:         100,
				MinValue:    int64(1),
				MaxValue:    int64(100),
			},
			contains: []string{"Equi-Width", "buckets=10", "ndv=100"},
		},
		{
			name: "frequency histogram",
			hist: &Histogram{
				Type:        FrequencyHistogram,
				Buckets:     []*HistogramBucket{{Count: 10}},
				BucketCount: 10,
				NDV:         100,
			},
			contains: []string{"Frequency", "buckets=10", "ndv=100"},
		},
		{
			name: "unknown type",
			hist: &Histogram{
				Type: HistogramType(100),
			},
			contains: []string{"Unknown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			explanation := tt.hist.Explain()
			for _, str := range tt.contains {
				assert.Contains(t, explanation, str, "explanation should contain "+str)
			}
		})
	}
}

func TestHistogramBucket(t *testing.T) {
	bucket := &HistogramBucket{
		LowerBound: int64(1),
		UpperBound: int64(10),
		Count:      100,
		Distinct:   10,
		NDV:        10,
	}

	assert.Equal(t, int64(1), bucket.LowerBound)
	assert.Equal(t, int64(10), bucket.UpperBound)
	assert.Equal(t, int64(100), bucket.Count)
	assert.Equal(t, int64(10), bucket.Distinct)
	assert.Equal(t, int64(10), bucket.NDV)
}

// Helper function to generate numeric values
func makeNumericValues(count int) []interface{} {
	values := make([]interface{}, count)
	for i := 0; i < count; i++ {
		values[i] = int64(i)
	}
	return values
}

func TestCompareHistogramValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: 0,
		},
		{
			name:     "a nil",
			a:        nil,
			b:        int64(5),
			expected: -1,
		},
		{
			name:     "b nil",
			a:        int64(5),
			b:        nil,
			expected: 1,
		},
		{
			name:     "numeric less",
			a:        int64(3),
			b:        int64(5),
			expected: -1,
		},
		{
			name:     "numeric equal",
			a:        int64(5),
			b:        int64(5),
			expected: 0,
		},
		{
			name:     "numeric greater",
			a:        int64(7),
			b:        int64(5),
			expected: 1,
		},
		{
			name:     "float less",
			a:        float64(3.5),
			b:        float64(5.0),
			expected: -1,
		},
		{
			name:     "string less",
			a:        "apple",
			b:        "banana",
			expected: -1,
		},
		{
			name:     "string greater",
			a:        "zebra",
			b:        "apple",
			expected: 1,
		},
		{
			name:     "mixed numeric int and float",
			a:        int64(5),
			b:        float64(5.0),
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareHistogramValues(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Benchmark tests
func BenchmarkBuildEquiWidthHistogram(b *testing.B) {
	values := makeNumericValues(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BuildEquiWidthHistogram(values, 10)
	}
}

func BenchmarkBuildFrequencyHistogram(b *testing.B) {
	values := makeNumericValues(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BuildFrequencyHistogram(values, 10)
	}
}

func BenchmarkHistogram_EstimateSelectivity(b *testing.B) {
	hist := BuildEquiWidthHistogram(makeNumericValues(1000), 10)
	filter := domain.Filter{Operator: ">", Value: int64(500)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hist.EstimateSelectivity(filter)
	}
}

func BenchmarkCompareHistogramValues(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compareHistogramValues(int64(i), int64(i+1))
	}
}
