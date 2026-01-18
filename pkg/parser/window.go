package parser

import (
	"fmt"
)

// WindowSpec 窗口规范
type WindowSpec struct {
	Name       string          // 窗口名称(如果有)
	PartitionBy []Expression   // PARTITION BY表达式
	OrderBy     []OrderItem     // ORDER BY表达式
	Frame       *WindowFrame     // 窗口帧定义
}

// WindowFrame 窗口帧
type WindowFrame struct {
	Mode      FrameMode       // 帧模式(ROWS/RANGE)
	Start      FrameBound      // 起始边界
	End        *FrameBound     // 结束边界(可为空)
}

// FrameMode 帧模式
type FrameMode int

const (
	FrameModeRows  FrameMode = iota // ROWS
	FrameModeRange                  // RANGE
	FrameModeGroups                 // GROUPS(暂不支持)
)

// FrameBound 帧边界
type FrameBound struct {
	Type  BoundType
	Value Expression // 帧偏移值(可为空)
}

// BoundType 边界类型
type BoundType int

const (
	BoundUnboundedPreceding BoundType = iota // UNBOUNDED PRECEDING
	BoundPreceding                        // n PRECEDING
	BoundCurrentRow                      // CURRENT ROW
	BoundFollowing                        // n FOLLOWING
	BoundUnboundedFollowing               // UNBOUNDED FOLLOWING
)

// WindowExpression 窗口函数表达式
type WindowExpression struct {
	FuncName  string       // 函数名
	Args      []Expression // 函数参数
	Spec      *WindowSpec  // 窗口规范
	Distinct  bool         // DISTINCT标记
}

// OrderItem 排序项
type OrderItem struct {
	Expr      Expression
	Direction string
}

// 支持的窗口函数
var SupportedWindowFunctions = map[string]bool{
	// 排名函数
	"ROW_NUMBER":  true,
	"RANK":       true,
	"DENSE_RANK":  true,
	"PERCENT_RANK": true,
	"CUME_DIST":   true,
	"NTILE":       true,
	
	// 偏移函数
	"LAG":    true,
	"LEAD":   true,
	"FIRST_VALUE": true,
	"LAST_VALUE":  true,
	"NTH_VALUE":   true,
	
	// 聚合窗口函数
	"COUNT":   true,
	"SUM":     true,
	"AVG":     true,
	"MIN":     true,
	"MAX":     true,
	"STDDEV":  true,
	"VAR":     true,
}

// ParseWindowSpec 解析窗口规范
func ParseWindowSpec(windowName string, partitionBy []Expression, orderBy []OrderItem, frame *WindowFrame) *WindowSpec {
	return &WindowSpec{
		Name:       windowName,
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
		Frame:       frame,
	}
}

// ParseWindowFrame 解析窗口帧
func ParseWindowFrame(mode FrameMode, start BoundType, startValue Expression, end BoundType, endValue Expression) *WindowFrame {
	frame := &WindowFrame{
		Mode: mode,
		Start: FrameBound{
			Type:  start,
			Value: startValue,
		},
	}
	
	if end != BoundUnboundedFollowing {
		frame.End = &FrameBound{
			Type:  end,
			Value: endValue,
		}
	}
	
	return frame
}

// NewWindowExpression 创建窗口函数表达式
func NewWindowExpression(funcName string, args []Expression, spec *WindowSpec) (*WindowExpression, error) {
	if !SupportedWindowFunctions[funcName] {
		return nil, fmt.Errorf("unsupported window function: %s", funcName)
	}
	
	return &WindowExpression{
		FuncName: funcName,
		Args:     args,
		Spec:     spec,
		Distinct: false,
	}, nil
}

// IsWindowFunction 检查是否为窗口函数
func IsWindowFunction(funcName string) bool {
	return SupportedWindowFunctions[funcName]
}

// WindowType 窗口函数类型
type WindowType int

const (
	WindowTypeRanking     WindowType = iota // 排名函数
	WindowTypeOffset                         // 偏移函数
	WindowTypeAggregate                      // 聚合函数
	WindowTypeValue                          // 值函数
)

// GetWindowType 获取窗口函数类型
func (we *WindowExpression) GetWindowType() WindowType {
	switch we.FuncName {
	case "ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST", "NTILE":
		return WindowTypeRanking
	case "LAG", "LEAD":
		return WindowTypeOffset
	case "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
		return WindowTypeValue
	default:
		return WindowTypeAggregate
	}
}

// IsRankingFunction 检查是否为排名函数
func (we *WindowExpression) IsRankingFunction() bool {
	return we.GetWindowType() == WindowTypeRanking
}

// IsOffsetFunction 检查是否为偏移函数
func (we *WindowExpression) IsOffsetFunction() bool {
	return we.GetWindowType() == WindowTypeOffset
}

// IsAggregateFunction 检查是否为聚合函数
func (we *WindowExpression) IsAggregateFunction() bool {
	return we.GetWindowType() == WindowTypeAggregate
}

// IsValueFunction 检查是否为值函数
func (we *WindowExpression) IsValueFunction() bool {
	return we.GetWindowType() == WindowTypeValue
}

// 窗口函数帮助函数

// CreateRankingWindow 创建排名窗口
func CreateRankingWindow(funcName string, partitionBy []Expression, orderBy []OrderItem) *WindowExpression {
	return &WindowExpression{
		FuncName: funcName,
		Args:     []Expression{},
		Spec: &WindowSpec{
			PartitionBy: partitionBy,
			OrderBy:     orderBy,
		},
	}
}

// CreateOffsetWindow 创建偏移窗口
func CreateOffsetWindow(funcName string, args []Expression, partitionBy []Expression, orderBy []OrderItem) *WindowExpression {
	spec := &WindowSpec{
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
	}
	
	// LAG/LEAD默认为UNBOUNDED PRECEDING
	defaultFrame := &WindowFrame{
		Mode:  FrameModeRows,
		Start:  FrameBound{Type: BoundUnboundedPreceding},
		End:    &FrameBound{Type: BoundCurrentRow},
	}
	spec.Frame = defaultFrame
	
	return &WindowExpression{
		FuncName: funcName,
		Args:     args,
		Spec:     spec,
	}
}

// CreateAggregateWindow 创建聚合窗口
func CreateAggregateWindow(funcName string, args []Expression, partitionBy []Expression, orderBy []OrderItem, frame *WindowFrame) *WindowExpression {
	spec := &WindowSpec{
		PartitionBy: partitionBy,
		OrderBy:     orderBy,
		Frame:       frame,
	}
	
	// 聚合函数默认为UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING
	if frame == nil {
		spec.Frame = &WindowFrame{
			Mode:  FrameModeRows,
			Start:  FrameBound{Type: BoundUnboundedPreceding},
			End:    &FrameBound{Type: BoundUnboundedFollowing},
		}
	}
	
	return &WindowExpression{
		FuncName: funcName,
		Args:     args,
		Spec:     spec,
	}
}

// 窗口函数验证

// ValidateWindowExpression 验证窗口函数表达式
func ValidateWindowExpression(we *WindowExpression) error {
	// 检查函数名是否支持
	if !IsWindowFunction(we.FuncName) {
		return fmt.Errorf("unsupported window function: %s", we.FuncName)
	}
	
	// 检查参数数量
	switch we.FuncName {
	case "ROW_NUMBER", "RANK", "DENSE_RANK":
		if len(we.Args) != 0 {
			return fmt.Errorf("%s() requires no arguments", we.FuncName)
		}
	case "NTILE":
		if len(we.Args) != 1 {
			return fmt.Errorf("NTILE() requires 1 argument")
		}
	case "LAG", "LEAD":
		if len(we.Args) == 0 || len(we.Args) > 2 {
			return fmt.Errorf("%s() requires 1 or 2 arguments", we.FuncName)
		}
	case "FIRST_VALUE", "LAST_VALUE":
		if len(we.Args) != 1 {
			return fmt.Errorf("%s() requires 1 argument", we.FuncName)
		}
	}
	
	// 检查窗口规范
	if we.Spec == nil {
		return fmt.Errorf("window function requires OVER clause")
	}
	
	// 检查ORDER BY
	if we.Spec.OrderBy == nil || len(we.Spec.OrderBy) == 0 {
		// 排名函数和偏移函数需要ORDER BY
		if we.IsRankingFunction() || we.IsOffsetFunction() {
			return fmt.Errorf("%s() requires ORDER BY in OVER clause", we.FuncName)
		}
	}
	
	// 检查帧定义
	if we.Spec.Frame != nil {
		// ROWS模式需要ORDER BY
		if we.Spec.Frame.Mode == FrameModeRows && len(we.Spec.OrderBy) == 0 {
			return fmt.Errorf("ROWS frame requires ORDER BY")
		}
	}
	
	return nil
}

// Clone 克隆窗口表达式
func (we *WindowExpression) Clone() *WindowExpression {
	clonedArgs := make([]Expression, len(we.Args))
	copy(clonedArgs, we.Args)
	
	var clonedSpec *WindowSpec
	if we.Spec != nil {
		clonedPartitionBy := make([]Expression, len(we.Spec.PartitionBy))
		copy(clonedPartitionBy, we.Spec.PartitionBy)
		
		clonedOrderBy := make([]OrderItem, len(we.Spec.OrderBy))
		copy(clonedOrderBy, we.Spec.OrderBy)
		
		clonedSpec = &WindowSpec{
			Name:       we.Spec.Name,
			PartitionBy: clonedPartitionBy,
			OrderBy:     clonedOrderBy,
		}
		
		if we.Spec.Frame != nil {
			clonedSpec.Frame = &WindowFrame{
				Mode:  we.Spec.Frame.Mode,
				Start:  we.Spec.Frame.Start,
			}
			if we.Spec.Frame.End != nil {
				clonedSpec.Frame.End = &FrameBound{
					Type:  we.Spec.Frame.End.Type,
					Value: we.Spec.Frame.End.Value,
				}
			}
		}
	}
	
	return &WindowExpression{
		FuncName: we.FuncName,
		Args:     clonedArgs,
		Spec:     clonedSpec,
		Distinct: we.Distinct,
	}
}
