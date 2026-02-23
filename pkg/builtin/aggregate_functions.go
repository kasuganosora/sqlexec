package builtin

import (
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// AggregateContext 聚合函数上下文
type AggregateContext struct {
	Count       int64
	Sum         float64
	Min         interface{}
	Max         interface{}
	AvgSum      float64
	Values      []float64     // 用于标准差等
	Strings     []string      // for GROUP_CONCAT
	AllValues   []interface{} // for ARRAY_AGG, MEDIAN, MODE
	BoolAnd     *bool         // for BOOL_AND
	BoolOr      *bool         // for BOOL_OR
	Separator   string        // for GROUP_CONCAT separator
	ProductVal  float64       // for PRODUCT, init to 1.0
	ProductInit bool          // for PRODUCT
}

// NewAggregateContext 创建聚合上下文
func NewAggregateContext() *AggregateContext {
	return &AggregateContext{
		Values:     make([]float64, 0),
		Strings:    make([]string, 0),
		AllValues:  make([]interface{}, 0),
		Separator:  ",",
		ProductVal: 1.0,
	}
}

// AggregateHandle 聚合函数处理函数
type AggregateHandle func(ctx *AggregateContext, args []interface{}) error

// AggregateResult 聚合结果函数
type AggregateResult func(ctx *AggregateContext) (interface{}, error)

// AggregateFunctionInfo 聚合函数信息
type AggregateFunctionInfo struct {
	Name        string
	Handler     AggregateHandle
	Result      AggregateResult
	Description string
	Example     string
	Category    string
}

func init() {
	// 聚合函数列表（将在initAggregateFunctions中注册）
}

// InitAggregateFunctions 初始化聚合函数
func InitAggregateFunctions() {
	aggregateFunctions := []*AggregateFunctionInfo{
		{
			Name:        "count",
			Handler:     aggCount,
			Result:      aggCountResult,
			Description: "计算行数",
			Example:     "COUNT(*) -> 100",
			Category:    "aggregate",
		},
		{
			Name:        "sum",
			Handler:     aggSum,
			Result:      aggSumResult,
			Description: "计算和",
			Example:     "SUM(price) -> 1000.50",
			Category:    "aggregate",
		},
		{
			Name:        "avg",
			Handler:     aggAvg,
			Result:      aggAvgResult,
			Description: "计算平均值",
			Example:     "AVG(price) -> 100.05",
			Category:    "aggregate",
		},
		{
			Name:        "min",
			Handler:     aggMin,
			Result:      aggMinResult,
			Description: "计算最小值",
			Example:     "MIN(price) -> 10.00",
			Category:    "aggregate",
		},
		{
			Name:        "max",
			Handler:     aggMax,
			Result:      aggMaxResult,
			Description: "计算最大值",
			Example:     "MAX(price) -> 1000.00",
			Category:    "aggregate",
		},
		{
			Name:        "stddev",
			Handler:     aggStdDev,
			Result:      aggStdDevResult,
			Description: "计算标准差",
			Example:     "STDDEV(price) -> 50.25",
			Category:    "aggregate",
		},
		{
			Name:        "variance",
			Handler:     aggVariance,
			Result:      aggVarianceResult,
			Description: "计算方差",
			Example:     "VARIANCE(price) -> 2525.06",
			Category:    "aggregate",
		},
		{
			Name:        "group_concat",
			Handler:     aggGroupConcat,
			Result:      aggGroupConcatResult,
			Description: "Concatenate strings with separator",
			Example:     "GROUP_CONCAT(name) -> 'a,b,c'",
			Category:    "aggregate",
		},
		{
			Name:        "string_agg",
			Handler:     aggGroupConcat,
			Result:      aggGroupConcatResult,
			Description: "Concatenate strings with separator (alias for group_concat)",
			Example:     "STRING_AGG(name, ',') -> 'a,b,c'",
			Category:    "aggregate",
		},
		{
			Name:        "listagg",
			Handler:     aggGroupConcat,
			Result:      aggGroupConcatResult,
			Description: "Concatenate strings with separator (alias for group_concat)",
			Example:     "LISTAGG(name, ',') -> 'a,b,c'",
			Category:    "aggregate",
		},
		{
			Name:        "count_if",
			Handler:     aggCountIf,
			Result:      aggCountIfResult,
			Description: "Count rows where condition is truthy",
			Example:     "COUNT_IF(is_active) -> 5",
			Category:    "aggregate",
		},
		{
			Name:        "bool_and",
			Handler:     aggBoolAnd,
			Result:      aggBoolAndResult,
			Description: "Logical AND of all values",
			Example:     "BOOL_AND(is_active) -> true",
			Category:    "aggregate",
		},
		{
			Name:        "every",
			Handler:     aggBoolAnd,
			Result:      aggBoolAndResult,
			Description: "Logical AND of all values (alias for bool_and)",
			Example:     "EVERY(is_active) -> true",
			Category:    "aggregate",
		},
		{
			Name:        "bool_or",
			Handler:     aggBoolOr,
			Result:      aggBoolOrResult,
			Description: "Logical OR of all values",
			Example:     "BOOL_OR(is_active) -> true",
			Category:    "aggregate",
		},
		{
			Name:        "stddev_pop",
			Handler:     aggStdDevPop,
			Result:      aggStdDevPopResult,
			Description: "Population standard deviation",
			Example:     "STDDEV_POP(price) -> 50.25",
			Category:    "aggregate",
		},
		{
			Name:        "stddev_samp",
			Handler:     aggStdDevSamp,
			Result:      aggStdDevSampResult,
			Description: "Sample standard deviation",
			Example:     "STDDEV_SAMP(price) -> 51.50",
			Category:    "aggregate",
		},
		{
			Name:        "var_pop",
			Handler:     aggVarPop,
			Result:      aggVarPopResult,
			Description: "Population variance",
			Example:     "VAR_POP(price) -> 2525.06",
			Category:    "aggregate",
		},
		{
			Name:        "var_samp",
			Handler:     aggVarSamp,
			Result:      aggVarSampResult,
			Description: "Sample variance",
			Example:     "VAR_SAMP(price) -> 2652.31",
			Category:    "aggregate",
		},
		{
			Name:        "median",
			Handler:     aggMedian,
			Result:      aggMedianResult,
			Description: "Compute median value",
			Example:     "MEDIAN(price) -> 50.00",
			Category:    "aggregate",
		},
		{
			Name:        "percentile_cont",
			Handler:     aggPercentileCont,
			Result:      aggPercentileContResult,
			Description: "Continuous percentile interpolation",
			Example:     "PERCENTILE_CONT(0.5, price) -> 50.00",
			Category:    "aggregate",
		},
		{
			Name:        "percentile_disc",
			Handler:     aggPercentileDisc,
			Result:      aggPercentileDiscResult,
			Description: "Discrete percentile (nearest value)",
			Example:     "PERCENTILE_DISC(0.5, price) -> 50",
			Category:    "aggregate",
		},
		{
			Name:        "array_agg",
			Handler:     aggArrayAgg,
			Result:      aggArrayAggResult,
			Description: "Accumulate values into an array",
			Example:     "ARRAY_AGG(name) -> ['a','b','c']",
			Category:    "aggregate",
		},
		{
			Name:        "list",
			Handler:     aggArrayAgg,
			Result:      aggArrayAggResult,
			Description: "Accumulate values into an array (alias for array_agg)",
			Example:     "LIST(name) -> ['a','b','c']",
			Category:    "aggregate",
		},
		{
			Name:        "product",
			Handler:     aggProduct,
			Result:      aggProductResult,
			Description: "Multiply all values together",
			Example:     "PRODUCT(quantity) -> 120",
			Category:    "aggregate",
		},
	}

	for _, fn := range aggregateFunctions {
		RegisterAggregate(fn)
	}
}

// AggregateRegistry 聚合函数注册表
var (
	aggregateRegistry   = make(map[string]*AggregateFunctionInfo)
	aggregateRegistryMu sync.RWMutex
)

// RegisterAggregate 注册聚合函数
func RegisterAggregate(info *AggregateFunctionInfo) {
	aggregateRegistryMu.Lock()
	aggregateRegistry[info.Name] = info
	aggregateRegistryMu.Unlock()
}

// GetAggregate 获取聚合函数
func GetAggregate(name string) (*AggregateFunctionInfo, bool) {
	aggregateRegistryMu.RLock()
	info, exists := aggregateRegistry[name]
	aggregateRegistryMu.RUnlock()
	return info, exists
}

// 辅助函数：比较两个值
func compareValues(a, b interface{}) int {
	return utils.CompareValuesForSort(a, b)
}

// 聚合函数实现
func aggCount(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		// COUNT(*)
		ctx.Count++
		return nil
	}

	// COUNT(column) - 忽略NULL
	for _, arg := range args {
		if arg != nil {
			ctx.Count++
			break
		}
	}
	return nil
}

func aggCountResult(ctx *AggregateContext) (interface{}, error) {
	return ctx.Count, nil
}

func aggSum(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}

	for _, arg := range args {
		if arg != nil {
			val, err := utils.ToFloat64(arg)
			if err != nil {
				return err
			}
			ctx.Sum += val
		}
	}
	return nil
}

func aggSumResult(ctx *AggregateContext) (interface{}, error) {
	return ctx.Sum, nil
}

func aggAvg(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}

	for _, arg := range args {
		if arg != nil {
			val, err := utils.ToFloat64(arg)
			if err != nil {
				return err
			}
			ctx.AvgSum += val
			ctx.Count++
		}
	}
	return nil
}

func aggAvgResult(ctx *AggregateContext) (interface{}, error) {
	if ctx.Count == 0 {
		return nil, nil
	}
	return ctx.AvgSum / float64(ctx.Count), nil
}

func aggMin(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}

	for _, arg := range args {
		if arg == nil {
			continue
		}
		if ctx.Min == nil {
			ctx.Min = arg
		} else if compareValues(arg, ctx.Min) < 0 {
			ctx.Min = arg
		}
	}
	return nil
}

func aggMinResult(ctx *AggregateContext) (interface{}, error) {
	return ctx.Min, nil
}

func aggMax(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}

	for _, arg := range args {
		if arg == nil {
			continue
		}
		if ctx.Max == nil {
			ctx.Max = arg
		} else if compareValues(arg, ctx.Max) > 0 {
			ctx.Max = arg
		}
	}
	return nil
}

func aggMaxResult(ctx *AggregateContext) (interface{}, error) {
	return ctx.Max, nil
}

func aggStdDev(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}

	for _, arg := range args {
		if arg != nil {
			val, err := utils.ToFloat64(arg)
			if err != nil {
				return err
			}
			ctx.Values = append(ctx.Values, val)
			ctx.AvgSum += val
		}
	}
	return nil
}

func aggStdDevResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.Values) == 0 {
		return nil, nil
	}

	mean := ctx.AvgSum / float64(len(ctx.Values))
	sumSquares := 0.0

	for _, val := range ctx.Values {
		diff := val - mean
		sumSquares += diff * diff
	}

	variance := sumSquares / float64(len(ctx.Values))
	return math.Sqrt(variance), nil
}

func aggVariance(ctx *AggregateContext, args []interface{}) error {
	return aggStdDev(ctx, args)
}

func aggVarianceResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.Values) == 0 {
		return nil, nil
	}

	mean := ctx.AvgSum / float64(len(ctx.Values))
	sumSquares := 0.0

	for _, val := range ctx.Values {
		diff := val - mean
		sumSquares += diff * diff
	}

	variance := sumSquares / float64(len(ctx.Values))
	return variance, nil
}

// isTruthy checks if a value is truthy for count_if / bool_and / bool_or
func isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case string:
		return v != "" && v != "0" && v != "false"
	case int:
		return v != 0
	case int8:
		return v != 0
	case int16:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case uint:
		return v != 0
	case uint8:
		return v != 0
	case uint16:
		return v != 0
	case uint32:
		return v != 0
	case uint64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	default:
		return true
	}
}

// GROUP_CONCAT / STRING_AGG / LISTAGG
func aggGroupConcat(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}
	// First arg is the value, optional second arg is the separator
	if len(args) >= 2 && args[1] != nil {
		ctx.Separator = utils.ToString(args[1])
	}
	if args[0] != nil {
		ctx.Strings = append(ctx.Strings, utils.ToString(args[0]))
	}
	return nil
}

func aggGroupConcatResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.Strings) == 0 {
		return nil, nil
	}
	return strings.Join(ctx.Strings, ctx.Separator), nil
}

// COUNT_IF
func aggCountIf(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}
	if isTruthy(args[0]) {
		ctx.Count++
	}
	return nil
}

func aggCountIfResult(ctx *AggregateContext) (interface{}, error) {
	return ctx.Count, nil
}

// BOOL_AND / EVERY
func aggBoolAnd(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}
	if args[0] == nil {
		return nil
	}
	val := isTruthy(args[0])
	if ctx.BoolAnd == nil {
		ctx.BoolAnd = &val
	} else {
		result := *ctx.BoolAnd && val
		ctx.BoolAnd = &result
	}
	return nil
}

func aggBoolAndResult(ctx *AggregateContext) (interface{}, error) {
	if ctx.BoolAnd == nil {
		return nil, nil
	}
	return *ctx.BoolAnd, nil
}

// BOOL_OR
func aggBoolOr(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}
	if args[0] == nil {
		return nil
	}
	val := isTruthy(args[0])
	if ctx.BoolOr == nil {
		ctx.BoolOr = &val
	} else {
		result := *ctx.BoolOr || val
		ctx.BoolOr = &result
	}
	return nil
}

func aggBoolOrResult(ctx *AggregateContext) (interface{}, error) {
	if ctx.BoolOr == nil {
		return nil, nil
	}
	return *ctx.BoolOr, nil
}

// STDDEV_POP — population standard deviation (divide by N)
func aggStdDevPop(ctx *AggregateContext, args []interface{}) error {
	return aggStdDev(ctx, args) // reuse Values accumulation
}

func aggStdDevPopResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.Values) == 0 {
		return nil, nil
	}
	mean := ctx.AvgSum / float64(len(ctx.Values))
	sumSq := 0.0
	for _, v := range ctx.Values {
		d := v - mean
		sumSq += d * d
	}
	return math.Sqrt(sumSq / float64(len(ctx.Values))), nil
}

// STDDEV_SAMP — sample standard deviation (divide by N-1)
func aggStdDevSamp(ctx *AggregateContext, args []interface{}) error {
	return aggStdDev(ctx, args)
}

func aggStdDevSampResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.Values) < 2 {
		return nil, nil
	}
	mean := ctx.AvgSum / float64(len(ctx.Values))
	sumSq := 0.0
	for _, v := range ctx.Values {
		d := v - mean
		sumSq += d * d
	}
	return math.Sqrt(sumSq / float64(len(ctx.Values)-1)), nil
}

// VAR_POP — population variance (divide by N)
func aggVarPop(ctx *AggregateContext, args []interface{}) error {
	return aggStdDev(ctx, args)
}

func aggVarPopResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.Values) == 0 {
		return nil, nil
	}
	mean := ctx.AvgSum / float64(len(ctx.Values))
	sumSq := 0.0
	for _, v := range ctx.Values {
		d := v - mean
		sumSq += d * d
	}
	return sumSq / float64(len(ctx.Values)), nil
}

// VAR_SAMP — sample variance (divide by N-1)
func aggVarSamp(ctx *AggregateContext, args []interface{}) error {
	return aggStdDev(ctx, args)
}

func aggVarSampResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.Values) < 2 {
		return nil, nil
	}
	mean := ctx.AvgSum / float64(len(ctx.Values))
	sumSq := 0.0
	for _, v := range ctx.Values {
		d := v - mean
		sumSq += d * d
	}
	return sumSq / float64(len(ctx.Values)-1), nil
}

// MEDIAN
func aggMedian(ctx *AggregateContext, args []interface{}) error {
	return aggStdDev(ctx, args) // reuse Values accumulation
}

func aggMedianResult(ctx *AggregateContext) (interface{}, error) {
	n := len(ctx.Values)
	if n == 0 {
		return nil, nil
	}
	sorted := make([]float64, n)
	copy(sorted, ctx.Values)
	sort.Float64s(sorted)

	if n%2 == 1 {
		return sorted[n/2], nil
	}
	// even: average of middle two
	return (sorted[n/2-1] + sorted[n/2]) / 2.0, nil
}

// PERCENTILE_CONT — continuous percentile with interpolation
// args[0] = percentile (0..1), args[1] = value
func aggPercentileCont(ctx *AggregateContext, args []interface{}) error {
	if len(args) < 2 {
		return nil
	}
	// Store percentile in AvgSum on first call (we reuse it; Count tracks calls)
	if args[0] != nil && ctx.Count == 0 {
		p, err := utils.ToFloat64(args[0])
		if err != nil {
			return err
		}
		ctx.AvgSum = p
	}
	if args[1] != nil {
		val, err := utils.ToFloat64(args[1])
		if err != nil {
			return err
		}
		ctx.Values = append(ctx.Values, val)
	}
	ctx.Count++
	return nil
}

func aggPercentileContResult(ctx *AggregateContext) (interface{}, error) {
	n := len(ctx.Values)
	if n == 0 {
		return nil, nil
	}
	sorted := make([]float64, n)
	copy(sorted, ctx.Values)
	sort.Float64s(sorted)

	p := ctx.AvgSum // percentile stored here
	if p <= 0 {
		return sorted[0], nil
	}
	if p >= 1 {
		return sorted[n-1], nil
	}

	// Linear interpolation
	pos := p * float64(n-1)
	lower := int(math.Floor(pos))
	upper := lower + 1
	if upper >= n {
		return sorted[n-1], nil
	}
	frac := pos - float64(lower)
	return sorted[lower] + frac*(sorted[upper]-sorted[lower]), nil
}

// PERCENTILE_DISC — discrete percentile (nearest value)
// args[0] = percentile (0..1), args[1] = value
func aggPercentileDisc(ctx *AggregateContext, args []interface{}) error {
	return aggPercentileCont(ctx, args) // same accumulation
}

func aggPercentileDiscResult(ctx *AggregateContext) (interface{}, error) {
	n := len(ctx.Values)
	if n == 0 {
		return nil, nil
	}
	sorted := make([]float64, n)
	copy(sorted, ctx.Values)
	sort.Float64s(sorted)

	p := ctx.AvgSum
	if p <= 0 {
		return sorted[0], nil
	}
	if p >= 1 {
		return sorted[n-1], nil
	}

	// Nearest rank method: ceil(p * N) - 1
	idx := int(math.Ceil(p*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx], nil
}

// ARRAY_AGG / LIST
func aggArrayAgg(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}
	// Include nil values in array_agg (standard SQL behavior)
	ctx.AllValues = append(ctx.AllValues, args[0])
	return nil
}

func aggArrayAggResult(ctx *AggregateContext) (interface{}, error) {
	if len(ctx.AllValues) == 0 {
		return nil, nil
	}
	return ctx.AllValues, nil
}

// PRODUCT
func aggProduct(ctx *AggregateContext, args []interface{}) error {
	if len(args) == 0 {
		return nil
	}
	for _, arg := range args {
		if arg != nil {
			val, err := utils.ToFloat64(arg)
			if err != nil {
				return err
			}
			ctx.ProductVal *= val
			ctx.ProductInit = true
		}
	}
	return nil
}

func aggProductResult(ctx *AggregateContext) (interface{}, error) {
	if !ctx.ProductInit {
		return nil, nil
	}
	return ctx.ProductVal, nil
}
