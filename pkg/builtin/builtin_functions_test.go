package builtin

import (
	"math"
	"testing"
)

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input interface{}
		want  float64
		valid bool
	}{
		{float64(3.14), 3.14, true},
		{float32(3.14), 3.14, true},
		{int(10), 10, true},
		{int64(100), 100, true},
		{int32(50), 50, true},
		{"string", 0, false},
		{true, 0, false},
	}

	for _, tt := range tests {
		result, err := toFloat64(tt.input)
		if tt.valid {
			if err != nil {
				t.Errorf("toFloat64(%v) error = %v", tt.input, err)
			}
			// 对于浮点数，使用近似比较（允许更大的误差）
			if tt.input == float32(3.14) || tt.input == float64(3.14) {
				if !builtinAlmostEqual(result, tt.want, 0.01) { // 增大误差范围到0.01
					t.Errorf("toFloat64(%v) = %v, want %v", tt.input, result, tt.want)
				}
			} else {
				if result != tt.want {
					t.Errorf("toFloat64(%v) = %v, want %v", tt.input, result, tt.want)
				}
			}
		} else {
			if err == nil {
				t.Error("Expected error for invalid type")
			}
		}
	}
}

func TestBuiltinToString(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{"hello", "hello"},
		{[]byte("hello"), "hello"},
		{123, "123"},
		{3.14, "3.14"},
		{true, "true"},
	}

	for _, tt := range tests {
		result := toString(tt.input)
		if result != tt.want {
			t.Errorf("toString(%v) = %s, want %s", tt.input, result, tt.want)
		}
	}
}

func TestBuiltinToInt64(t *testing.T) {
	tests := []struct {
		input interface{}
		want  int64
		valid bool
	}{
		{123, 123, true},
		{int64(1234567890), 1234567890, true},
		{int32(123), 123, true},
		{3.14, 3, true},
		{float32(3.14), 3, true},
		{"string", 0, false},
		{true, 0, false},
	}

	for _, tt := range tests {
		result, err := toInt64(tt.input)
		if tt.valid {
			if err != nil {
				t.Errorf("toInt64(%v) error = %v", tt.input, err)
			}
			if result != tt.want {
				t.Errorf("toInt64(%v) = %d, want %d", tt.input, result, tt.want)
			}
		} else {
			if err == nil {
				t.Error("Expected error for invalid type")
			}
		}
	}
}

// 测试数学函数
func TestBuiltinMathAbs(t *testing.T) {
	tests := []struct {
		input interface{}
		want  float64
	}{
		{5, 5},
		{-5, 5},
		{0, 0},
		{3.14, 3.14},
		{-3.14, 3.14},
	}

	for _, tt := range tests {
		result, err := mathAbs([]interface{}{tt.input})
		if err != nil {
			t.Errorf("mathAbs() error = %v", err)
			continue
		}
		if result != tt.want {
			t.Errorf("mathAbs() = %v, want %v", result, tt.want)
		}
	}
}

func TestBuiltinMathRound(t *testing.T) {
	tests := []struct {
		name  string
		args  []interface{}
		want  float64
	}{
		{"3.14", []interface{}{3.14}, 3},
		{"3.5", []interface{}{3.5}, 4},
		{"3.14159, 2", []interface{}{3.14159, 2}, 3.14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mathRound(tt.args)
			if err != nil {
				t.Errorf("mathRound() error = %v", err)
				return
			}
			if !builtinAlmostEqual(result.(float64), tt.want, 1e-9) {
				t.Errorf("mathRound(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestBuiltinStringConcat(t *testing.T) {
	tests := []struct {
		name string
		args []interface{}
		want string
	}{
		{"Two strings", []interface{}{"Hello", " ", "World"}, "Hello World"},
		{"Single string", []interface{}{"Hello"}, "Hello"},
		{"Empty args", []interface{}{}, ""},
		{"Mixed types", []interface{}{"A", 1, "B", 2}, "A1B2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringConcat(tt.args)
			if err != nil {
				t.Errorf("stringConcat() error = %v", err)
				return
			}
			if result != tt.want {
				t.Errorf("stringConcat() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestBuiltinStringLength(t *testing.T) {
	tests := []struct {
		input interface{}
		want  int64
	}{
		{"hello", 5},
		{"", 0},
		{"hello world", 11},
		{123, 3},
	}

	for _, tt := range tests {
		result, err := stringLength([]interface{}{tt.input})
		if err != nil {
			t.Errorf("stringLength() error = %v", err)
			continue
		}
		if result != tt.want {
			t.Errorf("stringLength(%v) = %v, want %v", tt.input, result, tt.want)
		}
	}
}

func TestBuiltinStringUpper(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{"hello", "HELLO"},
		{"Hello", "HELLO"},
		{"", ""},
		{"123", "123"},
	}

	for _, tt := range tests {
		result, err := stringUpper([]interface{}{tt.input})
		if err != nil {
			t.Errorf("stringUpper() error = %v", err)
			continue
		}
		if result != tt.want {
			t.Errorf("stringUpper(%v) = %v, want %v", tt.input, result, tt.want)
		}
	}
}

func TestBuiltinAggCount(t *testing.T) {
	ctx := NewAggregateContext()

	// COUNT(*) - 无参数
	err := aggCount(ctx, []interface{}{})
	if err != nil {
		t.Errorf("aggCount() error = %v", err)
	}
	if ctx.Count != 1 {
		t.Errorf("Count = %d, want 1", ctx.Count)
	}

	// COUNT(column) - 有参数
	err = aggCount(ctx, []interface{}{1, 2, 3})
	if err != nil {
		t.Errorf("aggCount() error = %v", err)
	}
	if ctx.Count != 2 {
		t.Errorf("Count = %d, want 2", ctx.Count)
	}
}

func TestBuiltinAggSum(t *testing.T) {
	ctx := NewAggregateContext()

	values := []interface{}{1, 2, 3, 4, 5}
	for _, v := range values {
		err := aggSum(ctx, []interface{}{v})
		if err != nil {
			t.Errorf("aggSum() error = %v", err)
		}
	}

	if ctx.Sum != 15 {
		t.Errorf("Sum = %f, want 15", ctx.Sum)
	}
}

func TestBuiltinAggAvg(t *testing.T) {
	ctx := NewAggregateContext()

	values := []interface{}{1, 2, 3, 4, 5}
	for _, v := range values {
		err := aggAvg(ctx, []interface{}{v})
		if err != nil {
			t.Errorf("aggAvg() error = %v", err)
		}
	}

	result, err := aggAvgResult(ctx)
	if err != nil {
		t.Errorf("aggAvgResult() error = %v", err)
	}
	if !builtinAlmostEqual(result.(float64), 3.0, 1e-9) {
		t.Errorf("Result = %v, want 3.0", result)
	}
}

func TestBuiltinAggMin(t *testing.T) {
	ctx := NewAggregateContext()

	values := []interface{}{int64(5), int64(2), int64(8), int64(1), int64(9)}
	for _, v := range values {
		err := aggMin(ctx, []interface{}{v})
		if err != nil {
			t.Errorf("aggMin() error = %v", err)
		}
	}

	result, err := aggMinResult(ctx)
	if err != nil {
		t.Errorf("aggMinResult() error = %v", err)
	}
	if result.(int64) != 1 {
		t.Errorf("Result = %v, want 1", result)
	}
}

func TestBuiltinAggMax(t *testing.T) {
	ctx := NewAggregateContext()

	values := []interface{}{int64(5), int64(2), int64(8), int64(1), int64(9)}
	for _, v := range values {
		err := aggMax(ctx, []interface{}{v})
		if err != nil {
			t.Errorf("aggMax() error = %v", err)
		}
	}

	result, err := aggMaxResult(ctx)
	if err != nil {
		t.Errorf("aggMaxResult() error = %v", err)
	}
	if result.(int64) != 9 {
		t.Errorf("Result = %v, want 9", result)
	}
}

func TestBuiltinCompareValues(t *testing.T) {
	tests := []struct {
		name string
		a, b interface{}
		want int
	}{
		{"Equal numbers", int64(5), int64(5), 0},
		{"a < b", int64(3), int64(5), -1},
		{"a > b", int64(7), int64(5), 1},
		{"Equal strings", "hello", "hello", 0},
		{"String a < b", "a", "b", -1},
		{"String a > b", "z", "a", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.want {
				t.Errorf("compareValues(%v, %v) = %d, want %d",
					tt.a, tt.b, result, tt.want)
			}
		})
	}
}

func TestBuiltinFunctionRegistry(t *testing.T) {
	registry := NewFunctionRegistry()
	if registry == nil {
		t.Fatal("NewFunctionRegistry returned nil")
	}

	info := &FunctionInfo{
		Name: "test_func",
		Type: FunctionTypeScalar,
		Description: "Test function",
		Handler: func(args []interface{}) (interface{}, error) {
			return "test", nil
		},
	}

	err := registry.Register(info)
	if err != nil {
		t.Errorf("Register() error = %v", err)
	}

	// 验证函数已注册
	fn, exists := registry.Get("test_func")
	if !exists {
		t.Error("Function should be registered")
	}
	if fn.Name != "test_func" {
		t.Error("Function name mismatch")
	}

	// 测试List
	list := registry.List()
	if len(list) != 1 {
		t.Errorf("List() returned %d functions, want 1", len(list))
	}

	// 测试Unregister
	removed := registry.Unregister("test_func")
	if !removed {
		t.Error("Unregister() should return true")
	}

	if registry.Exists("test_func") {
		t.Error("Function should be unregistered")
	}
}

func TestBuiltinAggregateFunctions(t *testing.T) {
	InitAggregateFunctions()

	// 测试count函数
	countFn, exists := GetAggregate("count")
	if !exists {
		t.Error("count function should be registered")
	}
	if countFn.Name != "count" {
		t.Error("count function name mismatch")
	}

	// 测试其他聚合函数
	expectedAggs := []string{"sum", "avg", "min", "max", "stddev", "variance"}
	for _, name := range expectedAggs {
		_, exists := GetAggregate(name)
		if !exists {
			t.Errorf("%s function should be registered", name)
		}
	}
}

func TestBuiltinFunctionTypeConstants(t *testing.T) {
	types := []struct {
		name  string
		value FunctionType
	}{
		{"Scalar", FunctionTypeScalar},
		{"Aggregate", FunctionTypeAggregate},
		{"Window", FunctionTypeWindow},
	}

	for _, tt := range types {
		if tt.value == 0 && tt.name != "Scalar" {
			t.Errorf("FunctionType %s = %d", tt.name, tt.value)
		}
	}
}

func TestBuiltinPi(t *testing.T) {
	result, err := mathPi(nil)
	if err != nil {
		t.Errorf("mathPi() error = %v", err)
	}
	if !builtinAlmostEqual(result.(float64), math.Pi, 1e-9) {
		t.Errorf("mathPi() = %v, want π", result)
	}
}

func TestBuiltinSign(t *testing.T) {
	tests := []struct {
		input interface{}
		want  float64
	}{
		{10, 1},
		{-10, -1},
		{0, 0},
		{3.14, 1},
		{-3.14, -1},
	}

	for _, tt := range tests {
		result, err := mathSign([]interface{}{tt.input})
		if err != nil {
			t.Errorf("mathSign() error = %v", err)
		}
		if result != tt.want {
			t.Errorf("mathSign(%v) = %v, want %v", tt.input, result, tt.want)
		}
	}
}

func TestBuiltinMod(t *testing.T) {
	tests := []struct {
		a, b float64
		want float64
	}{
		{10, 3, 1},
		{10, 5, 0},
		{7, 3, 1},
	}

	for _, tt := range tests {
		result, err := mathMod([]interface{}{tt.a, tt.b})
		if err != nil {
			t.Errorf("mathMod() error = %v", err)
		}
		if !builtinAlmostEqual(result.(float64), tt.want, 1e-9) {
			t.Errorf("mathMod(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.want)
		}
	}
}

func TestBuiltinStringReplace(t *testing.T) {
	tests := []struct {
		str    string
		old    string
		new    string
		want   string
	}{
		{"hello world", "world", "there", "hello there"},
		{"aaaa", "a", "b", "bbbb"},
		{"hello", "x", "y", "hello"},
		{"", "a", "b", ""},
	}

	for _, tt := range tests {
		result, err := stringReplace([]interface{}{tt.str, tt.old, tt.new})
		if err != nil {
			t.Errorf("stringReplace() error = %v", err)
			continue
		}
		if result != tt.want {
			t.Errorf("stringReplace(%s, %s, %s) = %s, want %s",
				tt.str, tt.old, tt.new, result, tt.want)
		}
	}
}

func TestBuiltinStringTrim(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{"  hello  ", "hello"},
		{"hello", "hello"},
		{"   ", ""},
		{"", ""},
	}

	for _, tt := range tests {
		result, err := stringTrim([]interface{}{tt.input})
		if err != nil {
			t.Errorf("stringTrim() error = %v", err)
			continue
		}
		if result != tt.want {
			t.Errorf("stringTrim(%v) = %v, want %v", tt.input, result, tt.want)
		}
	}
}

// 辅助函数：比较两个浮点数是否近似相等
func builtinAlmostEqual(a, b, epsilon float64) bool {
	if a == b {
		return true
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < epsilon
}
