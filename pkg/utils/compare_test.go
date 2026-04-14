package utils

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		operator string
		expected bool
		wantErr  bool
	}{
		// 等值比较
		{"数值相等", 10, 10, "=", true, false},
		{"数值不等", 10, 20, "=", false, false},
		{"数值不等操作符", 10, 20, "!=", true, false},
		{"字符串相等", "hello", "hello", "=", true, false},
		{"字符串不等", "hello", "world", "=", false, false},

		// 数值比较
		{"大于", 20, 10, ">", true, false},
		{"小于", 10, 20, "<", true, false},
		{"大于等于", 10, 10, ">=", true, false},
		{"小于等于", 10, 10, "<=", true, false},
		{"大于等于真", 20, 10, ">=", true, false},
		{"小于等于真", 10, 20, "<=", true, false},

		// 字符串比较
		{"字符串大于", "world", "hello", ">", true, false},
		{"字符串小于", "hello", "world", "<", true, false},

		// nil 值处理
		{"nil相等", nil, nil, "=", true, false},
		{"nil不等", nil, 10, "!=", true, false},
		{"nil和值比较", nil, 10, ">", false, false},
		{"nil和nil不等", nil, nil, "!=", false, false},

		// IN 操作符
		{"IN中存在", 5, []interface{}{1, 2, 3, 4, 5}, "IN", true, false},
		{"IN中不存在", 6, []interface{}{1, 2, 3, 4, 5}, "IN", false, false},
		{"IN字符串", "hello", []interface{}{"hello", "world"}, "IN", true, false},
		{"IN空数组", 5, []interface{}{}, "IN", false, false},
		{"IN非数组", 5, "not an array", "IN", false, true},
		{"NOT IN", 6, []interface{}{1, 2, 3, 4, 5}, "NOT IN", true, false},
		{"NOT IN不存在", 5, []interface{}{1, 2, 3, 4, 5}, "NOT IN", false, false},

		// BETWEEN 操作符
		{"BETWEEN内", 5, []interface{}{1, 10}, "BETWEEN", true, false},
		{"BETWEEN边界小", 1, []interface{}{1, 10}, "BETWEEN", true, false},
		{"BETWEEN边界大", 10, []interface{}{1, 10}, "BETWEEN", true, false},
		{"BETWEEN外小", 0, []interface{}{1, 10}, "BETWEEN", false, false},
		{"BETWEEN外大", 11, []interface{}{1, 10}, "BETWEEN", false, false},
		{"BETWEEN字符串", "c", []interface{}{"a", "z"}, "BETWEEN", true, false},
		{"BETWEEN非数组", 5, "not an array", "BETWEEN", false, true},
		{"BETWEEN数组太少", 5, []interface{}{1}, "BETWEEN", false, true},
		{"NOT BETWEEN", 0, []interface{}{1, 10}, "NOT BETWEEN", true, false},
		{"NOT BETWEEN内", 5, []interface{}{1, 10}, "NOT BETWEEN", false, false},

		// LIKE 操作符
		{"LIKE精确匹配", "hello", "hello", "LIKE", true, false},
		{"LIKE不匹配", "hello", "world", "LIKE", false, false},
		{"LIKE通配符%", "hello world", "%world", "LIKE", true, false},
		{"LIKE前缀%", "hello", "he%", "LIKE", true, false},
		{"LIKE后缀%", "hello", "%lo", "LIKE", true, false},
		// 注意：MatchesLike 不支持中间通配符
		{"LIKE中间%", "hello world", "%ll%", "LIKE", true, false}, // middle wildcard now supported
		{"LIKE单通配符%", "anything", "%", "LIKE", true, false},
		{"NOT LIKE", "hello", "world", "NOT LIKE", true, false},
		{"NOT LIKE匹配", "hello", "hello", "NOT LIKE", false, false},

		// 操作符大小写
		{"操作符小写", 10, 10, "=", true, false},
		{"操作符大写", 10, 10, "EQ", true, false},
		{"GT操作符", 20, 10, "GT", true, false},
		{"LT操作符", 10, 20, "LT", true, false},
		{"GE操作符", 10, 10, "GE", true, false},
		{"LE操作符", 10, 10, "LE", true, false},
		{"NEQ操作符", 10, 20, "NEQ", true, false},

		// 不支持的类型比较
		{"不支持的类型比较", map[int]int{}, "test", "=", false, true},
		{"不支持的类型比较2", struct{}{}, 10, ">", false, true},

		// 浮点数比较
		{"浮点数相等", 10.5, 10.5, "=", true, false},
		{"浮点数不等", 10.5, 10.6, "!=", true, false},
		{"浮点数大于", 10.6, 10.5, ">", true, false},

		// 大小写操作符
		{"IN大写", 5, []interface{}{1, 2, 3, 4, 5}, "IN", true, false},
		{"BETWEEN大写", 5, []interface{}{1, 10}, "BETWEEN", true, false},
		{"LIKE大写", "hello", "he%", "LIKE", true, false},

		// 特殊情况
		{"不支持的运算符", 10, 20, "UNKNOWN", false, true},

		// IS NULL / IS NOT NULL 操作符
		{"IS NULL true", nil, nil, "IS NULL", true, false},
		{"IS NULL false", "value", nil, "IS NULL", false, false},
		{"IS NOT NULL true", "value", nil, "IS NOT NULL", true, false},
		{"IS NOT NULL false", nil, nil, "IS NOT NULL", false, false},
		{"ISNULL true", nil, nil, "ISNULL", true, false},
		{"ISNOTNULL true", "value", nil, "ISNOTNULL", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareValues(tt.a, tt.b, tt.operator)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValues(%v, %v, %q) error = %v, wantErr %v", tt.a, tt.b, tt.operator, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("CompareValues(%v, %v, %q) = %v, want %v", tt.a, tt.b, tt.operator, got, tt.expected)
			}
		})
	}
}

func TestCompareValuesForSort(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"数值a<b", 5, 10, -1},
		{"数值a>b", 10, 5, 1},
		{"数值a=b", 10, 10, 0},
		{"字符串a<b", "apple", "banana", -1},
		{"字符串a>b", "banana", "apple", 1},
		{"字符串a=b", "apple", "apple", 0},
		{"nil和nil", nil, nil, 0},
		{"nil和非nil", nil, 10, -1},
		{"非nil和nil", 10, nil, 1},
		{"浮点数", 5.5, 10.2, -1},
		{"整数和浮点数", 10, 10.5, -1},
		{"负数", -5, 5, -1},
		{"大数值", 1000000, 1, 1},
		{"空字符串", "", "a", -1},
		{"空字符串相等", "", "", 0},
		{"Unicode字符", "ä", "b", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareValuesForSort(tt.a, tt.b); got != tt.expected {
				t.Errorf("CompareValuesForSort(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestCompareIn(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
		wantErr  bool
	}{
		{"IN中存在", 5, []interface{}{1, 2, 3, 4, 5}, true, false},
		{"IN中不存在", 6, []interface{}{1, 2, 3, 4, 5}, false, false},
		{"IN字符串", "hello", []interface{}{"hello", "world"}, true, false},
		{"IN空数组", 5, []interface{}{}, false, false},
		{"IN非数组", 5, "not an array", false, true},
		{"IN单元素", 1, []interface{}{1}, true, false},
		{"IN重复元素", 5, []interface{}{5, 5, 5}, true, false},
		{"IN字符串匹配", "hello", []interface{}{"HELLO", "hello"}, true, false},
		{"IN浮点数", 5.5, []interface{}{5.5, 10.0}, true, false},
		{"IN混合类型", 5, []interface{}{5, "5", 5.0}, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareValues(tt.a, tt.b, "IN")
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValues(%v, %v, IN) error = %v, wantErr %v", tt.a, tt.b, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("CompareValues(%v, %v, IN) = %v, want %v", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestCompareBetween(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
		wantErr  bool
	}{
		{"BETWEEN内", 5, []interface{}{1, 10}, true, false},
		{"BETWEEN边界小", 1, []interface{}{1, 10}, true, false},
		{"BETWEEN边界大", 10, []interface{}{1, 10}, true, false},
		{"BETWEEN外小", 0, []interface{}{1, 10}, false, false},
		{"BETWEEN外大", 11, []interface{}{1, 10}, false, false},
		{"BETWEEN字符串", "c", []interface{}{"a", "z"}, true, false},
		{"BETWEEN非数组", 5, "not an array", false, true},
		{"BETWEEN数组太少", 5, []interface{}{1}, false, true},
		{"BETWEEN浮点数", 5.5, []interface{}{1.0, 10.0}, true, false},
		{"BETWEEN负数", 0, []interface{}{-5, 5}, true, false},
		{"BETWEEN大范围", 1000000, []interface{}{1, 1000000}, true, false},
		{"BETWEEN倒序", 5, []interface{}{10, 1}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareValues(tt.a, tt.b, "BETWEEN")
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValues(%v, %v, BETWEEN) error = %v, wantErr %v", tt.a, tt.b, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("CompareValues(%v, %v, BETWEEN) = %v, want %v", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestCompareLike(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected bool
		wantErr  bool
	}{
		{"LIKE精确匹配", "hello", "hello", true, false},
		{"LIKE不匹配", "hello", "world", false, false},
		{"LIKE通配符%", "hello world", "%world", true, false},
		{"LIKE前缀%", "hello", "he%", true, false},
		{"LIKE后缀%", "hello", "%lo", true, false},
		{"LIKE中间%", "hello world", "%ll%", true, false},
		{"LIKE单通配符%", "anything", "%", true, false},
		{"LIKE*通配符", "hello", "*lo", true, false},
		{"LIKE*前缀", "hello", "he*", true, false},
		{"LIKE*中间", "hello", "*ll*", true, false}, // * wildcard converts to %, supports middle match
		{"LIKE*全部", "anything", "*", true, false},
		{"LIKE下划线", "hello", "h_llo", true, false},          // _ matches single char
		{"LIKE混合通配符", "hello world", "%ll%o%", true, false}, // complex multi-% pattern
		{"LIKE空模式", "hello", "", false, false},
		{"LIKE区分大小写", "HELLO", "hello", false, false},
		{"LIKE数字后缀", "12345", "%345", true, false},
		{"LIKE特殊字符", "!@#$%", "%@#$%", true, false}, // need % to match middle
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareValues(tt.a, tt.b, "LIKE")
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValues(%v, %v, LIKE) error = %v, wantErr %v", tt.a, tt.b, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("CompareValues(%v, %v, LIKE) = %v, want %v", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestCompareValuesWithErrors(t *testing.T) {
	// 测试 nil 错误映射
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		operator string
		wantErr  bool
	}{
		{"nil错误等于", nil, nil, "=", false},
		{"nil错误不等于", nil, nil, "!=", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompareValues(tt.a, tt.b, tt.operator)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValues(%v, %v, %q) error = %v, wantErr %v", tt.a, tt.b, tt.operator, err, tt.wantErr)
			}
		})
	}
}

func TestCompareValuesComplexCases(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		operator string
		expected bool
		wantErr  bool
	}{
		// 类型转换边界测试
		{"int和int32比较", int(10), int32(10), "=", true, false},
		{"int和int64比较", int(10), int64(10), "=", true, false},
		{"float32和float64比较", float32(10.5), float64(10.5), "=", true, false},
		{"uint和int比较", uint(10), int(10), "=", true, false},
		// Type conversion edge cases - these should return errors as types are incompatible
		{"string和byte", "hello", []byte("hello"), "=", false, true},
		{"string和byte不等", "hello", []byte("world"), "!=", false, true},

		// 特殊数值
		{"零值比较", 0, 0, "=", true, false},
		{"负值比较", -5, -5, "=", true, false},
		{"最大int64", int64(1<<63 - 1), int64(1<<63 - 1), "=", true, false},
		{"最小int64", int64(-1 << 63), int64(-1 << 63), "=", true, false},

		// 字符串边界
		{"长字符串", string(make([]byte, 1000)), string(make([]byte, 1000)), "=", true, false},
		{"Unicode", "你好", "你好", "=", true, false},
		{"Emoji", "😀", "😀", "=", true, false},

		// Large IN array - 5000 IS in array of 0-9999
		{"大IN数组", 5000, genArray(10000), "IN", true, false},

		// 错误类型
		{"channel类型", make(chan int), make(chan int), "=", false, true},
		{"function类型", func() {}, func() {}, "=", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareValues(tt.a, tt.b, tt.operator)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValues(%v, %v, %q) error = %v, wantErr %v", tt.a, tt.b, tt.operator, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("CompareValues(%v, %v, %q) = %v, want %v", tt.a, tt.b, tt.operator, got, tt.expected)
			}
		})
	}
}

// 辅助函数：生成测试数组
func genArray(n int) []interface{} {
	arr := make([]interface{}, n)
	for i := 0; i < n; i++ {
		arr[i] = i
	}
	return arr
}

func BenchmarkCompareValuesInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = CompareValues(10, 20, ">")
	}
}

func BenchmarkCompareValuesString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = CompareValues("hello", "world", "<")
	}
}

func BenchmarkCompareValuesIn(b *testing.B) {
	arr := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := 0; i < b.N; i++ {
		_, _ = CompareValues(5, arr, "IN")
	}
}

func BenchmarkCompareValuesBetween(b *testing.B) {
	arr := []interface{}{1, 10}
	for i := 0; i < b.N; i++ {
		_, _ = CompareValues(5, arr, "BETWEEN")
	}
}

func BenchmarkCompareValuesLike(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = CompareValues("hello world", "hel%", "LIKE")
	}
}

func ExampleCompareValues() {
	// 数值比较
	result, _ := CompareValues(10, 5, ">")
	fmt.Println(result)
	// Output: true
}

func TestCompareValuesErrors(t *testing.T) {
	tests := []struct {
		name      string
		a         interface{}
		b         interface{}
		operator  string
		expectErr bool
	}{
		{
			name:      "不支持的类型比较",
			a:         make(chan int),
			b:         "test",
			operator:  "=",
			expectErr: true,
		},
		{
			name:      "不支持的运算符",
			a:         10,
			b:         20,
			operator:  "INVALID",
			expectErr: true,
		},
		{
			name:      "IN操作符需要数组",
			a:         10,
			b:         "not an array",
			operator:  "IN",
			expectErr: true,
		},
		{
			name:      "BETWEEN操作符需要2个元素",
			a:         10,
			b:         []interface{}{1},
			operator:  "BETWEEN",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompareValues(tt.a, tt.b, tt.operator)
			if (err != nil) != tt.expectErr {
				t.Errorf("CompareValues() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestCompareValuesWithContextErrors(t *testing.T) {
	// 测试上下文错误处理
	tests := []struct {
		name      string
		a         interface{}
		b         interface{}
		operator  string
		expectErr bool
	}{
		{
			name:      "nil等于nil",
			a:         nil,
			b:         nil,
			operator:  "=",
			expectErr: false,
		},
		{
			name:      "nil不等于值",
			a:         nil,
			b:         10,
			operator:  "!=",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompareValues(tt.a, tt.b, tt.operator)
			if (err != nil) != tt.expectErr {
				t.Errorf("CompareValues() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestCompareValuesErrorWrapping(t *testing.T) {
	// 测试错误包装
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		operator string
	}{
		{"字符串和数字比较", "hello", 10, "="},
		{"map和字符串比较", map[string]int{}, "test", "="},
		{"slice和数字比较", []int{}, 10, "="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompareValues(tt.a, tt.b, tt.operator)
			if err == nil {
				t.Error("expected error but got nil")
			}
			// 检查错误消息是否包含有用信息
			errMsg := err.Error()
			if errMsg == "" {
				t.Error("error message should not be empty")
			}
		})
	}
}

func TestCompareValuesEdgeCases(t *testing.T) {
	// Edge cases test
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		operator string
		expected bool
		wantErr  bool
	}{
		{"Max float64", 1.7976931348623157e+308, 1.7976931348623157e+308, "=", true, false},
		{"Min float64", -1.7976931348623157e+308, -1.7976931348623157e+308, "=", true, false},
		{"NaN comparison", math.NaN(), math.NaN(), "=", false, false}, // NaN != NaN per IEEE 754
		{"Empty slice", []int{}, []int{}, "=", false, true},           // slices cannot be compared
		{"Nil slice", ([]int)(nil), ([]int)(nil), "=", false, true},   // slices cannot be compared
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CompareValues(tt.a, tt.b, tt.operator)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompareValues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("CompareValues(%v, %v, %q) = %v, want %v", tt.a, tt.b, tt.operator, result, tt.expected)
			}
		})
	}
}
