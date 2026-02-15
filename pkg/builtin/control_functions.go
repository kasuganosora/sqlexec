package builtin

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/utils"
)

func init() {
	controlFunctions := []*FunctionInfo{
		{
			Name: "coalesce",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "coalesce", ReturnType: "any", ParamTypes: []string{"any"}, Variadic: true},
			},
			Handler:     controlCoalesce,
			Description: "返回第一个非NULL参数",
			Example:     "COALESCE(NULL, NULL, 'hello') -> 'hello'",
			Category:    "control",
		},
		{
			Name: "nullif",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "nullif", ReturnType: "any", ParamTypes: []string{"any", "any"}, Variadic: false},
			},
			Handler:     controlNullIf,
			Description: "如果两个参数相等则返回NULL，否则返回第一个参数",
			Example:     "NULLIF(1, 1) -> NULL",
			Category:    "control",
		},
		{
			Name: "ifnull",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ifnull", ReturnType: "any", ParamTypes: []string{"any", "any"}, Variadic: false},
			},
			Handler:     controlIfNull,
			Description: "如果第一个参数为NULL则返回第二个参数",
			Example:     "IFNULL(NULL, 'default') -> 'default'",
			Category:    "control",
		},
		{
			Name: "nvl",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "nvl", ReturnType: "any", ParamTypes: []string{"any", "any"}, Variadic: false},
			},
			Handler:     controlIfNull,
			Description: "如果第一个参数为NULL则返回第二个参数（IFNULL的别名）",
			Example:     "NVL(NULL, 'default') -> 'default'",
			Category:    "control",
		},
		{
			Name: "if",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "if", ReturnType: "any", ParamTypes: []string{"boolean", "any", "any"}, Variadic: false},
			},
			Handler:     controlIf,
			Description: "条件表达式：如果条件为true返回第二个参数，否则返回第三个参数",
			Example:     "IF(1 > 0, 'yes', 'no') -> 'yes'",
			Category:    "control",
		},
		{
			Name: "iif",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "iif", ReturnType: "any", ParamTypes: []string{"boolean", "any", "any"}, Variadic: false},
			},
			Handler:     controlIf,
			Description: "条件表达式（IF的别名）",
			Example:     "IIF(1 > 0, 'yes', 'no') -> 'yes'",
			Category:    "control",
		},
		{
			Name: "greatest",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "greatest", ReturnType: "any", ParamTypes: []string{"any"}, Variadic: true},
			},
			Handler:     controlGreatest,
			Description: "返回参数列表中的最大值",
			Example:     "GREATEST(1, 5, 3) -> 5",
			Category:    "control",
		},
		{
			Name: "least",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "least", ReturnType: "any", ParamTypes: []string{"any"}, Variadic: true},
			},
			Handler:     controlLeast,
			Description: "返回参数列表中的最小值",
			Example:     "LEAST(1, 5, 3) -> 1",
			Category:    "control",
		},
	}

	for _, fn := range controlFunctions {
		RegisterGlobal(fn)
	}
}

// controlCoalesce returns the first non-NULL argument.
func controlCoalesce(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}
	for _, arg := range args {
		if arg != nil {
			return arg, nil
		}
	}
	return nil, nil
}

// controlNullIf returns NULL if a == b, otherwise returns a.
func controlNullIf(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("nullif() requires exactly 2 arguments")
	}
	a, b := args[0], args[1]
	if a == nil && b == nil {
		return nil, nil
	}
	if a == nil || b == nil {
		return a, nil
	}
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
		return nil, nil
	}
	return a, nil
}

// controlIfNull returns default if a is NULL.
func controlIfNull(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ifnull() requires exactly 2 arguments")
	}
	if args[0] == nil {
		return args[1], nil
	}
	return args[0], nil
}

// controlIf evaluates condition and returns the appropriate branch.
func controlIf(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("if() requires exactly 3 arguments")
	}
	cond := toBool(args[0])
	if cond {
		return args[1], nil
	}
	return args[2], nil
}

// controlGreatest returns the largest value among its arguments.
func controlGreatest(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("greatest() requires at least 1 argument")
	}
	var best interface{}
	for _, arg := range args {
		if arg == nil {
			continue
		}
		if best == nil || utils.CompareValuesForSort(arg, best) > 0 {
			best = arg
		}
	}
	return best, nil
}

// controlLeast returns the smallest value among its arguments.
func controlLeast(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("least() requires at least 1 argument")
	}
	var best interface{}
	for _, arg := range args {
		if arg == nil {
			continue
		}
		if best == nil || utils.CompareValuesForSort(arg, best) < 0 {
			best = arg
		}
	}
	return best, nil
}

// toBool converts a value to boolean for conditional evaluation.
func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int:
		return val != 0
	case int8:
		return val != 0
	case int16:
		return val != 0
	case int32:
		return val != 0
	case int64:
		return val != 0
	case float32:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != "" && val != "0" && val != "false"
	default:
		return true
	}
}
