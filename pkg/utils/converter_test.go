package utils

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestToString(t *testing.T) {
	tests := []struct {
		name     string
		v        interface{}
		expected string
	}{
		// 字符串类型
		{"正常字符串", "hello", "hello"},
		{"空字符串", "", ""},
		{"带空格字符串", "hello world", "hello world"},
		{"特殊字符", "!@#$%^&*()", "!@#$%^&*()"},
		{"Unicode", "你好世界", "你好世界"},
		{"数字字符串", "12345", "12345"},

		// 字节数组
		{"字节数组", []byte("hello"), "hello"},
		{"空字节数组", []byte{}, ""},
		{"字节数组Unicode", []byte("你好"), "你好"},

		// 布尔值
		{"布尔值true", true, "true"},
		{"布尔值false", false, "false"},

		// 整数类型
		{"int", int(10), "10"},
		{"int8", int8(10), "10"},
		{"int16", int16(10), "10"},
		{"int32", int32(10), "10"},
		{"int64", int64(10), "10"},

		// 无符号整数
		{"uint", uint(10), "10"},
		{"uint8", uint8(10), "10"},
		{"uint16", uint16(10), "10"},
		{"uint32", uint32(10), "10"},
		{"uint64", uint64(10), "10"},

		// 浮点数
		{"float32", float32(10.5), "10.5"},
		{"float64", float64(10.5), "10.5"},
		{"浮点数整数", float32(10.0), "10"},
		{"浮点数小数", float64(10.123), "10.123"},
		{"科学计数法", float32(1e10), "1e+10"},
		{"大浮点数", float64(1.2345678901234567), "1.2345678901234567"},

		// 特殊数值
		{"零值", 0, "0"},
		{"负数", -10, "-10"},
		{"负浮点", -10.5, "-10.5"},
		{"大整数", 9223372036854775807, "9223372036854775807"},
		{"大无符号数", uint64(18446744073709551615), "18446744073709551615"},

		// nil 值
		{"nil", nil, ""},

		// 默认格式化
		{"结构体", struct{ Name string }{"test"}, "{test}"},
		{"slice", []int{1, 2, 3}, "[1 2 3]"},
		{"map", map[string]int{"a": 1}, "map[a:1]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToString(tt.v); got != tt.expected {
				t.Errorf("ToString(%v) = %q, want %q", tt.v, got, tt.expected)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name     string
		arg      interface{}
		expected int64
		wantErr  bool
	}{
		// 成功转换
		{"int", int(10), 10, false},
		{"int8", int8(10), 10, false},
		{"int16", int16(10), 10, false},
		{"int32", int32(10), 10, false},
		{"int64", int64(10), 10, false},

		// 无符号整数转换
		{"uint", uint(10), 10, false},
		{"uint8", uint8(10), 10, false},
		{"uint16", uint16(10), 10, false},
		{"uint32", uint32(10), 10, false},
		{"uint64", uint64(10), 10, false},

		// 浮点数转换（截断）
		{"float64", float64(10.5), 10, false},
		{"float32", float32(10.7), 10, false},
		{"浮点整数", float64(10.0), 10, false},

		// 特殊数值
		{"零值", 0, 0, false},
		{"负数", -10, -10, false},
		{"大整数", 9223372036854775807, 9223372036854775807, false},
		{"最小整数", -9223372036854775808, -9223372036854775808, false},

		// nil 值
		{"nil", nil, 0, true},

		// 错误情况
		{"字符串", "10", 0, true},
		{"布尔值", true, 0, true},
		{"字节数组", []byte("10"), 0, true},
		{"结构体", struct{}{}, 0, true},
		{"slice", []int{}, 0, true},
		{"map", map[string]int{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToInt64(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToInt64(%v) error = %v, wantErr %v", tt.arg, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ToInt64(%v) = %v, want %v", tt.arg, got, tt.expected)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		arg      interface{}
		expected float64
		wantErr  bool
	}{
		// 浮点数转换
		{"float64", float64(10.5), 10.5, false},
		{"float32", float32(10.5), 10.5, false},
		{"浮点整数", float64(10.0), 10, false},
		{"大浮点数", float64(1.2345678901234567), 1.2345678901234567, false},
		{"小浮点数", float64(0.0001), 0.0001, false},

		// 整数转换
		{"int", int(10), 10, false},
		{"int8", int8(10), 10, false},
		{"int16", int16(10), 10, false},
		{"int32", int32(10), 10, false},
		{"int64", int64(10), 10, false},

		// 无符号整数
		{"uint", uint(10), 10, false},
		{"uint8", uint8(10), 10, false},
		{"uint16", uint16(10), 10, false},
		{"uint32", uint32(10), 10, false},
		{"uint64", uint64(10), 10, false},

		// 字符串转浮点
		{"字符串整数", "10", 10, false},
		{"字符串浮点", "10.5", 10.5, false},
		{"字符串科学计数", "1e10", 1e10, false},
		{"字符串负数", "-10.5", -10.5, false},

		// 特殊数值
		{"零值", 0, 0, false},
		{"负数", -10, -10, false},
		{"负浮点", -10.5, -10.5, false},

		// nil 值
		{"nil", nil, 0, true},

		// 错误情况
		{"无效字符串", "abc", 0, true},
		{"空字符串", "", 0, true},
		{"布尔值", true, 0, true},
		{"字节数组", []byte("10"), 0, true},
		{"结构体", struct{}{}, 0, true},
		{"slice", []int{}, 0, true},
		{"map", map[string]int{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToFloat64(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToFloat64(%v) error = %v, wantErr %v", tt.arg, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ToFloat64(%v) = %v, want %v", tt.arg, got, tt.expected)
			}
		})
	}
}

func TestToStringEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		v        interface{}
		expected string
	}{
		{"浮点精度", 0.1 + 0.2, "0.3"},
		{"浮点精度2", 1.0 / 3.0, "0.3333333333333333"},
		{"非常大的数", 1e100, "1e+100"},
		{"非常小的数", 1e-100, "1e-100"},
		{"Infinity", math.Inf(1), "+Inf"},
		{"负Infinity", math.Inf(-1), "-Inf"},
		{"NaN", math.NaN(), "NaN"},
		{"零浮点", 0.0, "0"},
		{"负零浮点", math.Copysign(0, -1), "-0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToString(tt.v)
			// NaN 的特殊情况
			if tt.expected == "NaN" {
				if got != "NaN" {
					t.Errorf("ToString(%v) = %q, want NaN", tt.v, got)
				}
				return
			}
			if got != tt.expected {
				t.Errorf("ToString(%v) = %q, want %q", tt.v, got, tt.expected)
			}
		})
	}
}

func TestToInt64EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		arg      interface{}
		expected int64
		wantErr  bool
	}{
		{"最大int64", int64(math.MaxInt64), math.MaxInt64, false},
		{"最小int64", int64(math.MinInt64), math.MinInt64, false},
		{"uint转int64最大", uint64(math.MaxInt64), math.MaxInt64, false},
		{"float64大数", float64(1.7976931348623157e+308), math.MinInt64, false}, // overflow returns MinInt64
		{"float64 NaN", math.NaN(), math.MinInt64, false},                     // NaN returns MinInt64
		{"float64 Inf", math.Inf(1), math.MinInt64, false},                    // Inf returns MinInt64
		{"负浮点转整数", float64(-10.9), -10, false},
		{"正浮点转整数", float64(10.9), 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToInt64(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToInt64(%v) error = %v, wantErr %v", tt.arg, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ToInt64(%v) = %v, want %v", tt.arg, got, tt.expected)
			}
		})
	}
}

func TestToFloat64EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		arg      interface{}
		expected float64
		wantErr  bool
	}{
		{"最大float64", float64(math.MaxFloat64), math.MaxFloat64, false},
		{"最小正浮点", float64(math.SmallestNonzeroFloat64), math.SmallestNonzeroFloat64, false},
		{"int64转float64", int64(9223372036854775807), 9223372036854775807, false},
		{"uint64转float64", uint64(18446744073709551615), 18446744073709551615, false},
		{"字符串转科学计数", "1.23e10", 1.23e10, false},
		{"字符串转负科学计数", "-1.23e10", -1.23e10, false},
		{"字符串转Inf", "Inf", math.Inf(1), false}, // Inf is valid float
		{"字符串转NaN", "NaN", math.NaN(), false},  // NaN is valid float
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToFloat64(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToFloat64(%v) error = %v, wantErr %v", tt.arg, err, tt.wantErr)
				return
			}
			// 对于浮点数，使用近似比较
			if !math.IsNaN(tt.expected) && got != tt.expected {
				t.Errorf("ToFloat64(%v) = %v, want %v", tt.arg, got, tt.expected)
			}
		})
	}
}

func TestToFloat64StringEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		arg     interface{}
		wantErr bool
	}{
		{"字符串十六进制", "0x10", true},
		{"字符串二进制", "0b10", true},
		{"字符串八进制", "010", false},
		{"字符串多个小数点", "10.5.5", true},
		{"字符串字母", "10a", true},
		{"字符串特殊字符", "10@#", true},
		{"字符串空格", " 10", true},
		{"字符串制表符", "\t10", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ToFloat64(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToFloat64(%v) error = %v, wantErr %v", tt.arg, err, tt.wantErr)
			}
		})
	}
}

func TestConversionConsistency(t *testing.T) {
	// 测试转换一致性
	value := int64(42)

	// int64 -> string -> float64
	str := ToString(value)
	f, err := ToFloat64(str)
	if err != nil {
		t.Errorf("ToFloat64(%q) error: %v", str, err)
	}
	if f != 42 {
		t.Errorf("Expected 42, got %f", f)
	}

	// int64 -> float64 -> string
	f2, _ := ToFloat64(value)
	str2 := ToString(f2)
	if str2 != "42" {
		t.Errorf("Expected \"42\", got %q", str2)
	}
}

func BenchmarkToStringInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToString(42)
	}
}

func BenchmarkToStringFloat64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToString(42.5)
	}
}

func BenchmarkToStringString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToString("hello")
	}
}

func BenchmarkToInt64Int(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToInt64(42)
	}
}

func BenchmarkToInt64Int64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToInt64(int64(42))
	}
}

func BenchmarkToInt64Float64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToInt64(42.5)
	}
}

func BenchmarkToFloat64Float64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToFloat64(42.5)
	}
}

func BenchmarkToFloat64Int(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToFloat64(42)
	}
}

func BenchmarkToFloat64String(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ToFloat64("42.5")
	}
}

func ExampleToString() {
	// 字符串转换
	result := ToString("hello")
	fmt.Println(result)

	result2 := ToString(42)
	fmt.Println(result2)

	result3 := ToString(3.14)
	fmt.Println(result3)

	result4 := ToString(true)
	fmt.Println(result4)
	// Output:
	// hello
	// 42
	// 3.14
	// true
}

func ExampleToInt64() {
	result, _ := ToInt64(42.7)
	fmt.Println(result)

	result2, _ := ToInt64(int32(100))
	fmt.Println(result2)

	result3, err := ToInt64("hello")
	fmt.Println(result3, err)
	// Output:
	// 42
	// 100
	// 0 cannot convert string to int64
}

func ExampleToFloat64() {
	result, _ := ToFloat64(42)
	fmt.Println(result)

	result2, _ := ToFloat64("3.14")
	fmt.Println(result2)

	result3, _ := ToFloat64(int32(10))
	fmt.Println(result3)

	result4, err := ToFloat64("hello")
	fmt.Println(result4, err)
	// Output:
	// 42
	// 3.14
	// 10
	// 0 cannot convert string to float64
}

func TestToFloat64StringParsing(t *testing.T) {
	// 测试 strconv.ParseFloat 的边界情况
	tests := []struct {
		name     string
		arg      string
		expected float64
		wantErr  bool
	}{
		{"整数", "42", 42, false},
		{"小数", "3.14", 3.14, false},
		{"科学计数法大", "1e10", 1e10, false},
		{"科学计数法小", "1e-10", 1e-10, false},
		{"负数", "-42", -42, false},
		{"负小数", "-3.14", -3.14, false},
		{"大数", "9223372036854775807", 9223372036854775807, false},
		{"十六进制字符串", "0x10", 0, true},
		{"空字符串", "", 0, true},
		{"空格", " ", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := strconv.ParseFloat(tt.arg, 64)
			if (err != nil) != tt.wantErr {
				t.Errorf("strconv.ParseFloat(%q) error = %v, wantErr %v", tt.arg, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("strconv.ParseFloat(%q) = %v, want %v", tt.arg, got, tt.expected)
			}
		})
	}
}

func TestTypeConversionErrors(t *testing.T) {
	// 测试所有转换函数的错误处理
	tests := []struct {
		name     string
		arg      interface{}
		testFunc func(interface{}) (interface{}, error)
	}{
		{"ToInt64 nil", nil, func(v interface{}) (interface{}, error) { return ToInt64(v) }},
		{"ToInt64 字符串", "hello", func(v interface{}) (interface{}, error) { return ToInt64(v) }},
		{"ToInt64 布尔", true, func(v interface{}) (interface{}, error) { return ToInt64(v) }},
		{"ToFloat64 nil", nil, func(v interface{}) (interface{}, error) { return ToFloat64(v) }},
		{"ToFloat64 布尔", true, func(v interface{}) (interface{}, error) { return ToFloat64(v) }},
		{"ToFloat64 无效字符串", "hello", func(v interface{}) (interface{}, error) { return ToFloat64(v) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.testFunc(tt.arg)
			if err == nil {
				t.Error("expected error but got nil")
			}
		})
	}
}

func TestConvertBoolColumnsBasedOnSchema(t *testing.T) {
	tests := []struct {
		name      string
		row       domain.Row
		tableInfo *domain.TableInfo
		expected  domain.Row
	}{
		{
			name: "Convert TINYINT to bool",
			row: domain.Row{
				"id":     int64(1),
				"active": int64(1),
			},
			tableInfo: &domain.TableInfo{
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "INT"},
					{Name: "active", Type: "TINYINT"},
				},
			},
			expected: domain.Row{
				"id":     int64(1),
				"active": true,
			},
		},
		{
			name: "Convert BOOLEAN to bool",
			row: domain.Row{
				"id":     int64(1),
				"active": int64(0),
			},
			tableInfo: &domain.TableInfo{
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "INT"},
					{Name: "active", Type: "BOOLEAN"},
				},
			},
			expected: domain.Row{
				"id":     int64(1),
				"active": false,
			},
		},
		{
			name: "Convert float to bool",
			row: domain.Row{
				"active": float64(1.0),
			},
			tableInfo: &domain.TableInfo{
				Columns: []domain.ColumnInfo{
					{Name: "active", Type: "BOOL"},
				},
			},
			expected: domain.Row{
				"active": true,
			},
		},
		{
			name: "Leave non-bool types unchanged",
			row: domain.Row{
				"id":   int64(1),
				"name": "test",
			},
			tableInfo: &domain.TableInfo{
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "INT"},
					{Name: "name", Type: "VARCHAR"},
				},
			},
			expected: domain.Row{
				"id":   int64(1),
				"name": "test",
			},
		},
		{
			name:      "Nil tableInfo",
			row:       domain.Row{"id": int64(1)},
			tableInfo: nil,
			expected:  domain.Row{"id": int64(1)},
		},
		{
			name:      "Nil row",
			row:       nil,
			tableInfo: &domain.TableInfo{Columns: []domain.ColumnInfo{{Name: "id", Type: "INT"}}},
			expected:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ConvertBoolColumnsBasedOnSchema(tt.row, tt.tableInfo)

			if tt.row == nil && tt.expected == nil {
				return
			}

			for key, expectedVal := range tt.expected {
				actualVal, exists := tt.row[key]
				if !exists {
					t.Errorf("key %s not found in row", key)
					continue
				}

				if actualVal != expectedVal {
					t.Errorf("key %s: expected %v (%T), got %v (%T)", key, expectedVal, expectedVal, actualVal, actualVal)
				}
			}
		})
	}
}
