package builtin

import (
	"fmt"
	"math"
)

func init() {
	// 注册数学函数
	mathFunctions := []*FunctionInfo{
		{
			Name: "abs",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "abs", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathAbs,
			Description: "返回绝对�?,
			Example:     "ABS(-5) -> 5",
			Category:    "math",
		},
		{
			Name: "ceil",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ceil", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathCeil,
			Description: "向上取整",
			Example:     "CEIL(3.14) -> 4",
			Category:    "math",
		},
		{
			Name: "ceiling",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ceiling", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathCeil,
			Description: "向上取整（ceil的别名）",
			Example:     "CEILING(3.14) -> 4",
			Category:    "math",
		},
		{
			Name: "floor",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "floor", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathFloor,
			Description: "向下取整",
			Example:     "FLOOR(3.14) -> 3",
			Category:    "math",
		},
		{
			Name: "round",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "round", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
				{Name: "round", ReturnType: "number", ParamTypes: []string{"number", "integer"}, Variadic: false},
			},
			Handler:     mathRound,
			Description: "四舍五入",
			Example:     "ROUND(3.14159, 2) -> 3.14",
			Category:    "math",
		},
		{
			Name: "sqrt",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sqrt", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathSqrt,
			Description: "计算平方�?,
			Example:     "SQRT(16) -> 4",
			Category:    "math",
		},
		{
			Name: "pow",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "pow", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     mathPow,
			Description: "计算�?,
			Example:     "POW(2, 3) -> 8",
			Category:    "math",
		},
		{
			Name: "power",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "power", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     mathPow,
			Description: "计算幂（pow的别名）",
			Example:     "POWER(2, 3) -> 8",
			Category:    "math",
		},
		{
			Name: "exp",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "exp", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathExp,
			Description: "计算e的x次方",
			Example:     "EXP(1) -> 2.718281828459045",
			Category:    "math",
		},
		{
			Name: "log",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "log", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
				{Name: "log", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     mathLog,
			Description: "计算对数",
			Example:     "LOG(10) -> 2.302585092994046",
			Category:    "math",
		},
		{
			Name: "log10",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "log10", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathLog10,
			Description: "计算�?0为底的对�?,
			Example:     "LOG10(100) -> 2",
			Category:    "math",
		},
		{
			Name: "log2",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "log2", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathLog2,
			Description: "计算�?为底的对�?,
			Example:     "LOG2(8) -> 3",
			Category:    "math",
		},
		{
			Name: "ln",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ln", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathLn,
			Description: "计算自然对数",
			Example:     "LN(10) -> 2.302585092994046",
			Category:    "math",
		},
		{
			Name: "sin",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sin", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathSin,
			Description: "计算正弦�?,
			Example:     "SIN(PI()/2) -> 1",
			Category:    "math",
		},
		{
			Name: "cos",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "cos", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathCos,
			Description: "计算余弦�?,
			Example:     "COS(0) -> 1",
			Category:    "math",
		},
		{
			Name: "tan",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "tan", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathTan,
			Description: "计算正切�?,
			Example:     "TAN(PI()/4) -> 1",
			Category:    "math",
		},
		{
			Name: "asin",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "asin", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathAsin,
			Description: "计算反正弦�?,
			Example:     "ASIN(0.5) -> 0.5235987755982989",
			Category:    "math",
		},
		{
			Name: "acos",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "acos", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathAcos,
			Description: "计算反余弦�?,
			Example:     "ACOS(0.5) -> 1.0471975511965979",
			Category:    "math",
		},
		{
			Name: "atan",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "atan", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathAtan,
			Description: "计算反正切�?,
			Example:     "ATAN(1) -> 0.7853981633974483",
			Category:    "math",
		},
		{
			Name: "atan2",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "atan2", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     mathAtan2,
			Description: "计算反正切值（2个参数）",
			Example:     "ATAN2(1, 1) -> 0.7853981633974483",
			Category:    "math",
		},
		{
			Name: "degrees",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "degrees", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathDegrees,
			Description: "弧度转角�?,
			Example:     "DEGREES(PI()) -> 180",
			Category:    "math",
		},
		{
			Name: "radians",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "radians", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathRadians,
			Description: "角度转弧�?,
			Example:     "RADIANS(180) -> 3.141592653589793",
			Category:    "math",
		},
		{
			Name: "pi",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "pi", ReturnType: "number", ParamTypes: []string{}, Variadic: false},
			},
			Handler:     mathPi,
			Description: "返回圆周率π",
			Example:     "PI() -> 3.141592653589793",
			Category:    "math",
		},
		{
			Name: "sign",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sign", ReturnType: "integer", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathSign,
			Description: "返回符号�?1, 0, 1�?,
			Example:     "SIGN(-10) -> -1",
			Category:    "math",
		},
		{
			Name: "truncate",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "truncate", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
				{Name: "truncate", ReturnType: "number", ParamTypes: []string{"number", "integer"}, Variadic: false},
			},
			Handler:     mathTruncate,
			Description: "截断数字",
			Example:     "TRUNCATE(3.14159, 2) -> 3.14",
			Category:    "math",
		},
		{
			Name: "mod",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "mod", ReturnType: "number", ParamTypes: []string{"number", "number"}, Variadic: false},
			},
			Handler:     mathMod,
			Description: "取模运算",
			Example:     "MOD(10, 3) -> 1",
			Category:    "math",
		},
		{
			Name: "rand",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "rand", ReturnType: "number", ParamTypes: []string{}, Variadic: false},
				{Name: "rand", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathRand,
			Description: "生成随机�?,
			Example:     "RAND() -> 0.123456789",
			Category:    "math",
		},
	}

	for _, fn := range mathFunctions {
		RegisterGlobal(fn)
	}
}

// 辅助函数：将参数转换为float64
func toFloat64(arg interface{}) (float64, error) {
	switch v := arg.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", arg)
	}
}

// 数学函数实现
func mathAbs(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("abs() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Abs(val), nil
}

func mathCeil(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ceil() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Ceil(val), nil
}

func mathFloor(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("floor() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Floor(val), nil
}

func mathRound(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("round() requires 1 or 2 arguments")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	
	decimals := int64(0)
	if len(args) == 2 {
		decimalsVal, err := toFloat64(args[1])
		if err != nil {
			return nil, err
		}
		decimals = int64(decimalsVal)
	}
	
	multiplier := math.Pow(10, float64(decimals))
	return math.Round(val*multiplier) / multiplier, nil
}

func mathSqrt(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sqrt() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	if val < 0 {
		return nil, fmt.Errorf("sqrt() requires non-negative argument")
	}
	return math.Sqrt(val), nil
}

func mathPow(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("pow() requires exactly 2 arguments")
	}
	base, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	exp, err := toFloat64(args[1])
	if err != nil {
		return nil, err
	}
	return math.Pow(base, exp), nil
}

func mathExp(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("exp() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Exp(val), nil
}

func mathLog(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("log() requires 1 or 2 arguments")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	if len(args) == 1 {
		return math.Log(val), nil
	}
	
	base, err := toFloat64(args[1])
	if err != nil {
		return nil, err
	}
	return math.Log(val) / math.Log(base), nil
}

func mathLog10(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("log10() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Log10(val), nil
}

func mathLog2(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("log2() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Log2(val), nil
}

func mathLn(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ln() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Log(val), nil
}

func mathSin(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sin() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Sin(val), nil
}

func mathCos(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("cos() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Cos(val), nil
}

func mathTan(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("tan() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Tan(val), nil
}

func mathAsin(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("asin() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	if val < -1 || val > 1 {
		return nil, fmt.Errorf("asin() argument must be between -1 and 1")
	}
	return math.Asin(val), nil
}

func mathAcos(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("acos() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	if val < -1 || val > 1 {
		return nil, fmt.Errorf("acos() argument must be between -1 and 1")
	}
	return math.Acos(val), nil
}

func mathAtan(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("atan() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Atan(val), nil
}

func mathAtan2(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("atan2() requires exactly 2 arguments")
	}
	y, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	x, err := toFloat64(args[1])
	if err != nil {
		return nil, err
	}
	return math.Atan2(y, x), nil
}

func mathDegrees(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("degrees() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return val * (180.0 / math.Pi), nil
}

func mathRadians(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("radians() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return val * (math.Pi / 180.0), nil
}

func mathPi(args []interface{}) (interface{}, error) {
	return math.Pi, nil
}

func mathSign(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sign() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	if val > 0 {
		return 1.0, nil
	} else if val < 0 {
		return -1.0, nil
	}
	return 0.0, nil
}

func mathTruncate(args []interface{}) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("truncate() requires 1 or 2 arguments")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	
	decimals := int64(0)
	if len(args) == 2 {
		decimalsVal, err := toFloat64(args[1])
		if err != nil {
			return nil, err
		}
		decimals = int64(decimalsVal)
	}
	
	multiplier := math.Pow(10, float64(decimals))
	return float64(int64(val*multiplier)) / multiplier, nil
}

func mathMod(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("mod() requires exactly 2 arguments")
	}
	a, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	b, err := toFloat64(args[1])
	if err != nil {
		return nil, err
	}
	return math.Mod(a, b), nil
}

func mathRand(args []interface{}) (interface{}, error) {
	seed := int64(0)
	if len(args) == 1 {
		seedVal, err := toFloat64(args[0])
		if err != nil {
			return nil, err
		}
		seed = int64(seedVal)
	}
	
	if seed != 0 {
		// 使用种子（简化实现）
		return float64(seed) / float64(^uint32(0)), nil
	}
	
	// 简化实现：返回一个伪随机�?
	return 0.5, nil
}
