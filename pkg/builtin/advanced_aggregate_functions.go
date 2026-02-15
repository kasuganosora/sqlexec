package builtin

import (
	"fmt"
	"math"

	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// advancedAggState holds additional state for advanced aggregate functions.
// It is stored in the AggregateContext.AllValues field as the first element.
type advancedAggState struct {
	pairsX   []float64
	pairsY   []float64
	values   []float64
	freqMap  map[string]int
	distinct map[string]struct{}
}

// getAdvState retrieves or creates the advancedAggState from the context.
func getAdvState(ctx *AggregateContext) *advancedAggState {
	if len(ctx.AllValues) > 0 {
		if st, ok := ctx.AllValues[0].(*advancedAggState); ok {
			return st
		}
	}
	st := &advancedAggState{
		pairsX:   make([]float64, 0),
		pairsY:   make([]float64, 0),
		values:   make([]float64, 0),
		freqMap:  make(map[string]int),
		distinct: make(map[string]struct{}),
	}
	ctx.AllValues = append([]interface{}{st}, ctx.AllValues...)
	return st
}

func init() {
	advancedAggregates := []*AggregateFunctionInfo{
		{
			Name:        "corr",
			Handler:     aggCorr,
			Result:      aggCorrResult,
			Description: "Pearson correlation coefficient",
			Example:     "CORR(x, y) -> 0.98",
			Category:    "aggregate",
		},
		{
			Name:        "covar_pop",
			Handler:     aggCovarPop,
			Result:      aggCovarPopResult,
			Description: "Population covariance",
			Example:     "COVAR_POP(x, y) -> 2.5",
			Category:    "aggregate",
		},
		{
			Name:        "covar_samp",
			Handler:     aggCovarSamp,
			Result:      aggCovarSampResult,
			Description: "Sample covariance",
			Example:     "COVAR_SAMP(x, y) -> 3.125",
			Category:    "aggregate",
		},
		{
			Name:        "skewness",
			Handler:     aggSkewness,
			Result:      aggSkewnessResult,
			Description: "Skewness (third central moment / sigma^3)",
			Example:     "SKEWNESS(x) -> 0.0",
			Category:    "aggregate",
		},
		{
			Name:        "kurtosis",
			Handler:     aggKurtosis,
			Result:      aggKurtosisResult,
			Description: "Excess kurtosis (fourth central moment / sigma^4 - 3)",
			Example:     "KURTOSIS(x) -> -1.2",
			Category:    "aggregate",
		},
		{
			Name:        "mode",
			Handler:     aggMode,
			Result:      aggModeResult,
			Description: "Most frequent value",
			Example:     "MODE(x) -> 'apple'",
			Category:    "aggregate",
		},
		{
			Name:        "entropy",
			Handler:     aggEntropy,
			Result:      aggEntropyResult,
			Description: "Information entropy -sum(p*log2(p))",
			Example:     "ENTROPY(x) -> 1.585",
			Category:    "aggregate",
		},
		{
			Name:        "approx_count_distinct",
			Handler:     aggApproxCountDistinct,
			Result:      aggApproxCountDistinctResult,
			Description: "Approximate count of distinct values",
			Example:     "APPROX_COUNT_DISTINCT(x) -> 42",
			Category:    "aggregate",
		},
	}

	for _, fn := range advancedAggregates {
		RegisterAggregate(fn)
	}
}

// --- corr ---

func aggCorr(ctx *AggregateContext, args []interface{}) error {
	if len(args) < 2 {
		return fmt.Errorf("corr() requires 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil
	}
	x, err := utils.ToFloat64(args[0])
	if err != nil {
		return err
	}
	y, err := utils.ToFloat64(args[1])
	if err != nil {
		return err
	}
	st := getAdvState(ctx)
	st.pairsX = append(st.pairsX, x)
	st.pairsY = append(st.pairsY, y)
	return nil
}

func aggCorrResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	n := len(st.pairsX)
	if n < 2 {
		return nil, nil
	}
	nf := float64(n)

	// Compute means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += st.pairsX[i]
		sumY += st.pairsY[i]
	}
	meanX := sumX / nf
	meanY := sumY / nf

	// Compute sums of deviations
	var sumXY, sumX2, sumY2 float64
	for i := 0; i < n; i++ {
		dx := st.pairsX[i] - meanX
		dy := st.pairsY[i] - meanY
		sumXY += dx * dy
		sumX2 += dx * dx
		sumY2 += dy * dy
	}

	denom := math.Sqrt(sumX2 * sumY2)
	if denom == 0 {
		return nil, nil
	}
	return sumXY / denom, nil
}

// --- covar_pop ---

func aggCovarPop(ctx *AggregateContext, args []interface{}) error {
	return aggCorr(ctx, args)
}

func aggCovarPopResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	n := len(st.pairsX)
	if n == 0 {
		return nil, nil
	}
	nf := float64(n)

	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += st.pairsX[i]
		sumY += st.pairsY[i]
	}
	meanX := sumX / nf
	meanY := sumY / nf

	var sumXY float64
	for i := 0; i < n; i++ {
		sumXY += (st.pairsX[i] - meanX) * (st.pairsY[i] - meanY)
	}
	return sumXY / nf, nil
}

// --- covar_samp ---

func aggCovarSamp(ctx *AggregateContext, args []interface{}) error {
	return aggCorr(ctx, args)
}

func aggCovarSampResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	n := len(st.pairsX)
	if n < 2 {
		return nil, nil
	}
	nf := float64(n)

	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += st.pairsX[i]
		sumY += st.pairsY[i]
	}
	meanX := sumX / nf
	meanY := sumY / nf

	var sumXY float64
	for i := 0; i < n; i++ {
		sumXY += (st.pairsX[i] - meanX) * (st.pairsY[i] - meanY)
	}
	return sumXY / (nf - 1), nil
}

// --- skewness ---

func aggSkewness(ctx *AggregateContext, args []interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("skewness() requires 1 argument")
	}
	if args[0] == nil {
		return nil
	}
	v, err := utils.ToFloat64(args[0])
	if err != nil {
		return err
	}
	st := getAdvState(ctx)
	st.values = append(st.values, v)
	return nil
}

func aggSkewnessResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	n := len(st.values)
	if n < 3 {
		return nil, nil
	}
	nf := float64(n)

	var sum float64
	for _, v := range st.values {
		sum += v
	}
	mean := sum / nf

	var m2, m3 float64
	for _, v := range st.values {
		d := v - mean
		d2 := d * d
		m2 += d2
		m3 += d2 * d
	}
	m2 /= nf
	m3 /= nf

	sigma := math.Sqrt(m2)
	if sigma == 0 {
		return 0.0, nil
	}
	return m3 / (sigma * sigma * sigma), nil
}

// --- kurtosis ---

func aggKurtosis(ctx *AggregateContext, args []interface{}) error {
	return aggSkewness(ctx, args)
}

func aggKurtosisResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	n := len(st.values)
	if n < 4 {
		return nil, nil
	}
	nf := float64(n)

	var sum float64
	for _, v := range st.values {
		sum += v
	}
	mean := sum / nf

	var m2, m4 float64
	for _, v := range st.values {
		d := v - mean
		d2 := d * d
		m2 += d2
		m4 += d2 * d2
	}
	m2 /= nf
	m4 /= nf

	if m2 == 0 {
		return 0.0, nil
	}
	return m4/(m2*m2) - 3.0, nil
}

// --- mode ---

func aggMode(ctx *AggregateContext, args []interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("mode() requires 1 argument")
	}
	if args[0] == nil {
		return nil
	}
	st := getAdvState(ctx)
	key := toString(args[0])
	st.freqMap[key]++
	return nil
}

func aggModeResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	if len(st.freqMap) == 0 {
		return nil, nil
	}
	var modeVal string
	maxCount := 0
	for k, c := range st.freqMap {
		if c > maxCount {
			maxCount = c
			modeVal = k
		}
	}
	return modeVal, nil
}

// --- entropy ---

func aggEntropy(ctx *AggregateContext, args []interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("entropy() requires 1 argument")
	}
	if args[0] == nil {
		return nil
	}
	st := getAdvState(ctx)
	key := toString(args[0])
	st.freqMap[key]++
	ctx.Count++
	return nil
}

func aggEntropyResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	if ctx.Count == 0 {
		return nil, nil
	}
	total := float64(ctx.Count)
	var ent float64
	for _, c := range st.freqMap {
		p := float64(c) / total
		if p > 0 {
			ent -= p * math.Log2(p)
		}
	}
	return ent, nil
}

// --- approx_count_distinct ---

func aggApproxCountDistinct(ctx *AggregateContext, args []interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("approx_count_distinct() requires 1 argument")
	}
	if args[0] == nil {
		return nil
	}
	st := getAdvState(ctx)
	key := toString(args[0])
	st.distinct[key] = struct{}{}
	return nil
}

func aggApproxCountDistinctResult(ctx *AggregateContext) (interface{}, error) {
	st := getAdvState(ctx)
	return int64(len(st.distinct)), nil
}
