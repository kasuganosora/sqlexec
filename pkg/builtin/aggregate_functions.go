package builtin

import (
	"math"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// AggregateContext 聚合函数上下文
type AggregateContext struct {
	Count   int64
	Sum     float64
	Min     interface{}
	Max     interface{}
	AvgSum  float64
	Values  []float64 // 用于标准差等
}

// NewAggregateContext 创建聚合上下文
func NewAggregateContext() *AggregateContext {
	return &AggregateContext{
		Values: make([]float64, 0),
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
