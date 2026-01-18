package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/kasuganosora/pkg/builtin"
	"github.com/kasuganosora/pkg/optimizer"
	"github.com/kasuganosora/pkg/parser"
)

// TestBuiltinFunctions 测试内置函数功能
func TestBuiltinFunctions(t *testing.T) {
	builtin.InitBuiltinFunctions()
}

// TestFunctionStats 测试函数统计
func TestFunctionStats(t *testing.T) {
	builtin.InitBuiltinFunctions()

	totalCount := builtin.GetFunctionCount()
	if totalCount == 0 {
		t.Errorf("总函数数应该大于0, 实际为 %d", totalCount)
	}

	categories := builtin.GetAllCategories()
	if len(categories) == 0 {
		t.Error("函数分类列表不应该为空")
	}

	for _, category := range categories {
		count := builtin.GetFunctionCountByCategory(category)
		if count < 0 {
			t.Errorf("分类 %s 的函数数不应该为负数: %d", category, count)
		}
	}
}

// TestMathFunctions 测试数学函数
func TestMathFunctions(t *testing.T) {
	builtin.InitBuiltinFunctions()

	tests := []struct {
		name     string
		function string
		args     []interface{}
		expected interface{}
	}{
		{"ABS(-5)", "abs", []interface{}{-5.0}, 5.0},
		{"CEIL(3.14)", "ceil", []interface{}{3.14}, 4.0},
		{"FLOOR(3.14)", "floor", []interface{}{3.14}, 3.0},
		{"ROUND(3.14159, 2)", "round", []interface{}{3.14159, 2}, 3.14},
		{"SQRT(16)", "sqrt", []interface{}{16.0}, 4.0},
		{"POW(2, 3)", "pow", []interface{}{2.0, 3.0}, 8.0},
		{"EXP(1)", "exp", []interface{}{1.0}, 2.718281828459045},
		{"LOG10(100)", "log10", []interface{}{100.0}, 2.0},
		{"LOG2(8)", "log2", []interface{}{8.0}, 3.0},
		{"PI()", "pi", []interface{}{}, 3.141592653589793},
		{"SIGN(-10)", "sign", []interface{}{-10.0}, -1.0},
		{"MOD(10, 3)", "mod", []interface{}{10.0, 3.0}, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, ok := builtin.GetGlobal(tt.function)
			if !ok {
				t.Fatalf("函数 %s 未找到", tt.function)
			}

			result, err := info.Handler(tt.args)
			if err != nil {
				t.Fatalf("执行函数失败: %v", err)
			}

			if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.expected) {
				t.Errorf("期望 %v, 实际 %v", tt.expected, result)
			}
		})
	}
}

// TestStringFunctions 测试字符串函数
func TestStringFunctions(t *testing.T) {
	builtin.InitBuiltinFunctions()

	tests := []struct {
		name     string
		function string
		args     []interface{}
		expected interface{}
	}{
		{"CONCAT", "concat", []interface{}{"Hello", " ", "World"}, "Hello World"},
		{"CONCAT_WS", "concat_ws", []interface{}{",", "a", "b", "c"}, "a,b,c"},
		{"LENGTH", "length", []interface{}{"hello"}, int64(5)},
		{"UPPER", "upper", []interface{}{"hello"}, "HELLO"},
		{"LOWER", "lower", []interface{}{"HELLO"}, "hello"},
		{"TRIM", "trim", []interface{}{"  hello  "}, "hello"},
		{"LEFT", "left", []interface{}{"hello", int64(3)}, "hel"},
		{"RIGHT", "right", []interface{}{"hello", int64(3)}, "llo"},
		{"SUBSTRING", "substring", []interface{}{"hello", int64(2), int64(3)}, "ell"},
		{"REPLACE", "replace", []interface{}{"hello world", "world", "there"}, "hello there"},
		{"REPEAT", "repeat", []interface{}{"ab", int64(3)}, "ababab"},
		{"REVERSE", "reverse", []interface{}{"hello"}, "olleh"},
		{"LPAD", "lpad", []interface{}{"hello", int64(10), "*"}, "*****hello"},
		{"RPAD", "rpad", []interface{}{"hello", int64(10), "*"}, "hello*****"},
		{"POSITION", "position", []interface{}{"ll", "hello"}, int64(3)},
		{"INSTR", "instr", []interface{}{"hello", "ll"}, int64(3)},
		{"ASCII", "ascii", []interface{}{"A"}, int64(65)},
		{"SPACE", "space", []interface{}{int64(5)}, "     "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, ok := builtin.GetGlobal(tt.function)
			if !ok {
				t.Fatalf("函数 %s 未找到", tt.function)
			}

			result, err := info.Handler(tt.args)
			if err != nil {
				t.Fatalf("执行函数失败: %v", err)
			}

			if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.expected) {
				t.Errorf("期望 %v, 实际 %v", tt.expected, result)
			}
		})
	}
}

// TestDateFunctions 测试日期函数
func TestDateFunctions(t *testing.T) {
	builtin.InitBuiltinFunctions()

	t.Run("NOW", func(t *testing.T) {
		info, ok := builtin.GetGlobal("now")
		if !ok {
			t.Fatal("NOW函数未找到")
		}

		result, err := info.Handler([]interface{}{})
		if err != nil {
			t.Fatalf("执行NOW函数失败: %v", err)
		}

		if _, ok := result.(time.Time); !ok {
			t.Errorf("NOW应该返回时间类型, 实际类型: %T", result)
		}
	})

	t.Run("YEAR", func(t *testing.T) {
		info, ok := builtin.GetGlobal("year")
		if !ok {
			t.Fatal("YEAR函数未找到")
		}

		result, err := info.Handler([]interface{}{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatalf("执行YEAR函数失败: %v", err)
		}

		if fmt.Sprintf("%v", result) != "2024" {
			t.Errorf("期望 2024, 实际 %v", result)
		}
	})

	t.Run("MONTH", func(t *testing.T) {
		info, ok := builtin.GetGlobal("month")
		if !ok {
			t.Fatal("MONTH函数未找到")
		}

		result, err := info.Handler([]interface{}{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatalf("执行MONTH函数失败: %v", err)
		}

		if fmt.Sprintf("%v", result) != "1" {
			t.Errorf("期望 1, 实际 %v", result)
		}
	})

	t.Run("DAY", func(t *testing.T) {
		info, ok := builtin.GetGlobal("day")
		if !ok {
			t.Fatal("DAY函数未找到")
		}

		result, err := info.Handler([]interface{}{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatalf("执行DAY函数失败: %v", err)
		}

		if fmt.Sprintf("%v", result) != "15" {
			t.Errorf("期望 15, 实际 %v", result)
		}
	})
}

// TestAggregateFunctions 测试聚合函数
func TestAggregateFunctions(t *testing.T) {
	builtin.InitBuiltinFunctions()
	builtin.InitAggregateFunctions()

	tests := []struct {
		name     string
		function string
		values   []interface{}
		expected interface{}
	}{
		{"COUNT", "count", []interface{}{1, 2, 3, 4, 5}, int64(5)},
		{"SUM", "sum", []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}, 15.0},
		{"AVG", "avg", []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}, 3.0},
		{"MIN", "min", []interface{}{5, 3, 8, 1, 9}, 1},
		{"MAX", "max", []interface{}{5, 3, 8, 1, 9}, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, ok := builtin.GetAggregate(tt.function)
			if !ok {
				t.Fatalf("函数 %s 未找到", tt.function)
			}

			ctx := builtin.NewAggregateContext()

			// 逐个添加值
			for _, val := range tt.values {
				err := info.Handler(ctx, []interface{}{val})
				if err != nil {
					t.Fatalf("添加值错误: %v", err)
				}
			}

			// 获取结果
			result, err := info.Result(ctx)
			if err != nil {
				t.Fatalf("获取结果错误: %v", err)
			}

			if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.expected) {
				t.Errorf("期望 %v, 实际 %v", tt.expected, result)
			}
		})
	}
}

// TestExpressionEvaluator 测试表达式求值器
func TestExpressionEvaluator(t *testing.T) {
	evaluator := optimizer.NewExpressionEvaluatorWithoutAPI()

	tests := []struct {
		name     string
		expr     *parser.Expression
		row      parser.Row
		expected interface{}
	}{
		{
			"ABS(-5)",
			&parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "abs",
				Args: []parser.Expression{
					{
						Type:  parser.ExprTypeValue,
						Value: -5.0,
					},
				},
			},
			nil,
			5.0,
		},
		{
			"UPPER('hello')",
			&parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "upper",
				Args: []parser.Expression{
					{
						Type:  parser.ExprTypeValue,
						Value: "hello",
					},
				},
			},
			nil,
			"HELLO",
		},
		{
			"CONCAT('Hello', ' ', 'World')",
			&parser.Expression{
				Type:     parser.ExprTypeFunction,
				Function: "concat",
				Args: []parser.Expression{
					{
						Type:  parser.ExprTypeValue,
						Value: "Hello",
					},
					{
						Type:  parser.ExprTypeValue,
						Value: " ",
					},
					{
						Type:  parser.ExprTypeValue,
						Value: "World",
					},
				},
			},
			nil,
			"Hello World",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, tt.row)
			if err != nil {
				t.Fatalf("表达式求值失败: %v", err)
			}

			if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.expected) {
				t.Errorf("期望 %v, 实际 %v", tt.expected, result)
			}
		})
	}
}
