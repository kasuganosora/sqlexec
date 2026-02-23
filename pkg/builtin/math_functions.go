package builtin

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/kasuganosora/sqlexec/pkg/utils"
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
			Description: "返回绝对值",
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
			Description: "计算平方根",
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
			Description: "计算幂",
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
			Description: "计算以10为底的对数",
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
			Description: "计算以2为底的对数",
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
			Description: "计算正弦值",
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
			Description: "计算余弦值",
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
			Description: "计算正切值",
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
			Description: "计算反正弦值",
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
			Description: "计算反余弦值",
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
			Description: "计算反正切值",
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
			Description: "弧度转角度",
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
			Description: "角度转弧度",
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
			Description: "返回符号（-1, 0, 1）",
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
			Description: "生成随机数",
			Example:     "RAND() -> 0.123456789",
			Category:    "math",
		},
		{
			Name: "cbrt",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "cbrt", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathCbrt,
			Description: "Compute cube root",
			Example:     "CBRT(27) -> 3",
			Category:    "math",
		},
		{
			Name: "cot",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "cot", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathCot,
			Description: "Compute cotangent",
			Example:     "COT(1) -> 0.6420926159343306",
			Category:    "math",
		},
		{
			Name: "sinh",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "sinh", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathSinh,
			Description: "Compute hyperbolic sine",
			Example:     "SINH(1) -> 1.1752011936438014",
			Category:    "math",
		},
		{
			Name: "cosh",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "cosh", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathCosh,
			Description: "Compute hyperbolic cosine",
			Example:     "COSH(1) -> 1.5430806348152437",
			Category:    "math",
		},
		{
			Name: "tanh",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "tanh", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathTanh,
			Description: "Compute hyperbolic tangent",
			Example:     "TANH(1) -> 0.7615941559557649",
			Category:    "math",
		},
		{
			Name: "asinh",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "asinh", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathAsinh,
			Description: "Compute inverse hyperbolic sine",
			Example:     "ASINH(1) -> 0.8813736198100534",
			Category:    "math",
		},
		{
			Name: "acosh",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "acosh", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathAcosh,
			Description: "Compute inverse hyperbolic cosine",
			Example:     "ACOSH(1) -> 0",
			Category:    "math",
		},
		{
			Name: "atanh",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "atanh", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathAtanh,
			Description: "Compute inverse hyperbolic tangent",
			Example:     "ATANH(0.5) -> 0.5493061443340549",
			Category:    "math",
		},
		{
			Name: "factorial",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "factorial", ReturnType: "integer", ParamTypes: []string{"integer"}, Variadic: false},
			},
			Handler:     mathFactorial,
			Description: "Compute factorial of n (n <= 20)",
			Example:     "FACTORIAL(5) -> 120",
			Category:    "math",
		},
		{
			Name: "gcd",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "gcd", ReturnType: "integer", ParamTypes: []string{"integer", "integer"}, Variadic: false},
			},
			Handler:     mathGcd,
			Description: "Compute greatest common divisor",
			Example:     "GCD(12, 8) -> 4",
			Category:    "math",
		},
		{
			Name: "lcm",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "lcm", ReturnType: "integer", ParamTypes: []string{"integer", "integer"}, Variadic: false},
			},
			Handler:     mathLcm,
			Description: "Compute least common multiple",
			Example:     "LCM(4, 6) -> 12",
			Category:    "math",
		},
		{
			Name: "even",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "even", ReturnType: "number", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathEven,
			Description: "Round away from zero to nearest even number",
			Example:     "EVEN(3) -> 4",
			Category:    "math",
		},
		{
			Name: "is_finite",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "is_finite", ReturnType: "boolean", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathIsFinite,
			Description: "Check if value is finite (not Inf and not NaN)",
			Example:     "IS_FINITE(1.0) -> true",
			Category:    "math",
		},
		{
			Name: "is_infinite",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "is_infinite", ReturnType: "boolean", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathIsInfinite,
			Description: "Check if value is infinite",
			Example:     "IS_INFINITE(1.0/0.0) -> true",
			Category:    "math",
		},
		{
			Name: "is_nan",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "is_nan", ReturnType: "boolean", ParamTypes: []string{"number"}, Variadic: false},
			},
			Handler:     mathIsNan,
			Description: "Check if value is NaN",
			Example:     "IS_NAN(0.0/0.0) -> true",
			Category:    "math",
		},
	}

	for _, fn := range mathFunctions {
		RegisterGlobal(fn)
	}
}

// 辅助函数：将参数转换为float64 (using utils package)
func toFloat64(arg interface{}) (float64, error) {
	return utils.ToFloat64(arg)
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
	if b == 0 {
		return nil, nil // MySQL: MOD(a, 0) returns NULL
	}
	return math.Mod(a, b), nil
}

func mathRand(args []interface{}) (interface{}, error) {
	if len(args) == 1 {
		seedVal, err := toFloat64(args[0])
		if err != nil {
			return nil, err
		}
		// Use the seed to create a deterministic random number
		r := rand.New(rand.NewSource(int64(seedVal)))
		return r.Float64(), nil
	}

	// No seed: use global rand source
	return rand.Float64(), nil
}

// --- Extended math functions (Batch 5) ---

func mathCbrt(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("cbrt() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Cbrt(val), nil
}

func mathCot(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("cot() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	tanVal := math.Tan(val)
	if tanVal == 0 {
		return nil, fmt.Errorf("cot() undefined: tan(x) is zero")
	}
	return 1.0 / tanVal, nil
}

func mathSinh(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("sinh() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Sinh(val), nil
}

func mathCosh(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("cosh() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Cosh(val), nil
}

func mathTanh(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("tanh() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Tanh(val), nil
}

func mathAsinh(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("asinh() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.Asinh(val), nil
}

func mathAcosh(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("acosh() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	if val < 1 {
		return nil, fmt.Errorf("acosh() argument must be >= 1")
	}
	return math.Acosh(val), nil
}

func mathAtanh(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("atanh() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	if val <= -1 || val >= 1 {
		return nil, fmt.Errorf("atanh() argument must be between -1 and 1 (exclusive)")
	}
	return math.Atanh(val), nil
}

func mathFactorial(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("factorial() requires exactly 1 argument")
	}
	n, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("factorial(): %v", err)
	}
	if n < 0 {
		return nil, fmt.Errorf("factorial() requires non-negative argument")
	}
	if n > 20 {
		return nil, fmt.Errorf("factorial() argument must be <= 20")
	}
	var result int64 = 1
	for i := int64(2); i <= n; i++ {
		result *= i
	}
	return result, nil
}

func mathGcd(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("gcd() requires exactly 2 arguments")
	}
	a, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("gcd(): %v", err)
	}
	b, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("gcd(): %v", err)
	}
	return gcdInt64(a, b), nil
}

// gcdInt64 computes the greatest common divisor using the Euclidean algorithm.
func gcdInt64(a, b int64) int64 {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func mathLcm(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("lcm() requires exactly 2 arguments")
	}
	a, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("lcm(): %v", err)
	}
	b, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("lcm(): %v", err)
	}
	if a == 0 || b == 0 {
		return int64(0), nil
	}
	g := gcdInt64(a, b)
	// Use a/g*b to avoid overflow as much as possible
	result := (a / g) * b
	if result < 0 {
		result = -result
	}
	return result, nil
}

func mathEven(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("even() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}

	// Round away from zero to the nearest even integer
	var rounded float64
	if val >= 0 {
		rounded = math.Ceil(val)
		if int64(rounded)%2 != 0 {
			rounded++
		}
	} else {
		rounded = math.Floor(val)
		if int64(rounded)%2 != 0 {
			rounded--
		}
	}
	return rounded, nil
}

func mathIsFinite(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("is_finite() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return !math.IsInf(val, 0) && !math.IsNaN(val), nil
}

func mathIsInfinite(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("is_infinite() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.IsInf(val, 0), nil
}

func mathIsNan(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("is_nan() requires exactly 1 argument")
	}
	val, err := toFloat64(args[0])
	if err != nil {
		return nil, err
	}
	return math.IsNaN(val), nil
}
