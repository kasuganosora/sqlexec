package main

import (
	"fmt"
	"time"

	"mysql-proxy/mysql/builtin"
	"mysql-proxy/mysql/optimizer"
	"mysql-proxy/mysql/parser"
)

func main() {
	fmt.Println("=== 内置函数测试 ===")
	
	// 初始化所有内置函数
	builtin.InitBuiltinFunctions()
	
	// 打印函数统计信息
	printFunctionStats()
	
	// 测试数学函数
	testMathFunctions()
	
	// 测试字符串函数
	testStringFunctions()
	
	// 测试日期函数
	testDateFunctions()
	
	// 测试聚合函数
	testAggregateFunctions()
	
	// 测试表达式求值器
	testExpressionEvaluator()
	
	fmt.Println("\n=== 所有测试完成 ===")
}

func printFunctionStats() {
	fmt.Println("\n【函数统计】")
	fmt.Printf("总函数数: %d\n", builtin.GetFunctionCount())
	
	for _, category := range builtin.GetAllCategories() {
		count := builtin.GetFunctionCountByCategory(category)
		fmt.Printf("  %s: %d 个函数\n", category, count)
	}
}

func testMathFunctions() {
	fmt.Println("\n【数学函数测试】")
	
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
	
	for _, test := range tests {
		info, ok := builtin.GetGlobal(test.function)
		if !ok {
			fmt.Printf("  ❌ %s: 函数未找到\n", test.name)
			continue
		}
		
		result, err := info.Handler(test.args)
		if err != nil {
			fmt.Printf("  ❌ %s: 错误 - %v\n", test.name, err)
			continue
		}
		
		if fmt.Sprintf("%v", result) == fmt.Sprintf("%v", test.expected) {
			fmt.Printf("  ✅ %s: %v\n", test.name, result)
		} else {
			fmt.Printf("  ❌ %s: 期望 %v, 实际 %v\n", test.name, test.expected, result)
		}
	}
}

func testStringFunctions() {
	fmt.Println("\n【字符串函数测试】")
	
	tests := []struct {
		name     string
		function string
		args     []interface{}
		expected interface{}
	}{
		{"CONCAT('Hello', ' ', 'World')", "concat", []interface{}{"Hello", " ", "World"}, "Hello World"},
		{"CONCAT_WS(',', 'a', 'b', 'c')", "concat_ws", []interface{}{",", "a", "b", "c"}, "a,b,c"},
		{"LENGTH('hello')", "length", []interface{}{"hello"}, int64(5)},
		{"UPPER('hello')", "upper", []interface{}{"hello"}, "HELLO"},
		{"LOWER('HELLO')", "lower", []interface{}{"HELLO"}, "hello"},
		{"TRIM('  hello  ')", "trim", []interface{}{"  hello  "}, "hello"},
		{"LEFT('hello', 3)", "left", []interface{}{"hello", int64(3)}, "hel"},
		{"RIGHT('hello', 3)", "right", []interface{}{"hello", int64(3)}, "llo"},
		{"SUBSTRING('hello', 2, 3)", "substring", []interface{}{"hello", int64(2), int64(3)}, "ell"},
		{"REPLACE('hello world', 'world', 'there')", "replace", []interface{}{"hello world", "world", "there"}, "hello there"},
		{"REPEAT('ab', 3)", "repeat", []interface{}{"ab", int64(3)}, "ababab"},
		{"REVERSE('hello')", "reverse", []interface{}{"hello"}, "olleh"},
		{"LPAD('hello', 10, '*')", "lpad", []interface{}{"hello", int64(10), "*"}, "*****hello"},
		{"RPAD('hello', 10, '*')", "rpad", []interface{}{"hello", int64(10), "*"}, "hello*****"},
		{"POSITION('ll' IN 'hello')", "position", []interface{}{"ll", "hello"}, int64(3)},
		{"INSTR('hello', 'll')", "instr", []interface{}{"hello", "ll"}, int64(3)},
		{"ASCII('A')", "ascii", []interface{}{"A"}, int64(65)},
		{"SPACE(5)", "space", []interface{}{int64(5)}, "     "},
	}
	
	for _, test := range tests {
		info, ok := builtin.GetGlobal(test.function)
		if !ok {
			fmt.Printf("  ❌ %s: 函数未找到\n", test.name)
			continue
		}
		
		result, err := info.Handler(test.args)
		if err != nil {
			fmt.Printf("  ❌ %s: 错误 - %v\n", test.name, err)
			continue
		}
		
		if fmt.Sprintf("%v", result) == fmt.Sprintf("%v", test.expected) {
			fmt.Printf("  ✅ %s: %v\n", test.name, result)
		} else {
			fmt.Printf("  ❌ %s: 期望 %v, 实际 %v\n", test.name, test.expected, result)
		}
	}
}

func testDateFunctions() {
	fmt.Println("\n【日期函数测试】")
	
	// NOW函数
	info, ok := builtin.GetGlobal("now")
	if ok {
		result, err := info.Handler([]interface{}{})
		if err == nil {
			if _, ok := result.(time.Time); ok {
				fmt.Printf("  ✅ NOW(): 返回时间类型\n")
			} else {
				fmt.Printf("  ❌ NOW(): 期望时间类型，实际 %T\n", result)
			}
		} else {
			fmt.Printf("  ❌ NOW(): 错误 - %v\n", err)
		}
	}
	
	// YEAR函数
	info, ok = builtin.GetGlobal("year")
	if ok {
		result, err := info.Handler([]interface{}{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)})
		if err == nil {
			if fmt.Sprintf("%v", result) == "2024" {
				fmt.Printf("  ✅ YEAR('2024-01-01'): %v\n", result)
			} else {
				fmt.Printf("  ❌ YEAR('2024-01-01'): 期望 2024, 实际 %v\n", result)
			}
		} else {
			fmt.Printf("  ❌ YEAR('2024-01-01'): 错误 - %v\n", err)
		}
	}
	
	// MONTH函数
	info, ok = builtin.GetGlobal("month")
	if ok {
		result, err := info.Handler([]interface{}{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)})
		if err == nil {
			if fmt.Sprintf("%v", result) == "1" {
				fmt.Printf("  ✅ MONTH('2024-01-01'): %v\n", result)
			} else {
				fmt.Printf("  ❌ MONTH('2024-01-01'): 期望 1, 实际 %v\n", result)
			}
		} else {
			fmt.Printf("  ❌ MONTH('2024-01-01'): 错误 - %v\n", err)
		}
	}
	
	// DAY函数
	info, ok = builtin.GetGlobal("day")
	if ok {
		result, err := info.Handler([]interface{}{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)})
		if err == nil {
			if fmt.Sprintf("%v", result) == "15" {
				fmt.Printf("  ✅ DAY('2024-01-15'): %v\n", result)
			} else {
				fmt.Printf("  ❌ DAY('2024-01-15'): 期望 15, 实际 %v\n", result)
			}
		} else {
			fmt.Printf("  ❌ DAY('2024-01-15'): 错误 - %v\n", err)
		}
	}
}

func testAggregateFunctions() {
	fmt.Println("\n【聚合函数测试】")
	
	// 初始化聚合函数
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
	
	for _, test := range tests {
		info, ok := builtin.GetAggregate(test.function)
		if !ok {
			fmt.Printf("  ❌ %s: 函数未找到\n", test.name)
			continue
		}
		
		ctx := builtin.NewAggregateContext()
		
		// 逐个添加值
		for _, val := range test.values {
			err := info.Handler(ctx, []interface{}{val})
			if err != nil {
				fmt.Printf("  ❌ %s: 添加值错误 - %v\n", test.name, err)
				continue
			}
		}
		
		// 获取结果
		result, err := info.Result(ctx)
		if err != nil {
			fmt.Printf("  ❌ %s: 获取结果错误 - %v\n", test.name, err)
			continue
		}
		
		if fmt.Sprintf("%v", result) == fmt.Sprintf("%v", test.expected) {
			fmt.Printf("  ✅ %s: %v\n", test.name, result)
		} else {
			fmt.Printf("  ❌ %s: 期望 %v, 实际 %v\n", test.name, test.expected, result)
		}
	}
}

func testExpressionEvaluator() {
	fmt.Println("\n【表达式求值器测试】")
	
	evaluator := optimizer.NewExpressionEvaluator()
	
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
				Arguments: []*parser.Expression{
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
				Arguments: []*parser.Expression{
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
				Arguments: []*parser.Expression{
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
	
	for _, test := range tests {
		result, err := evaluator.Evaluate(test.expr, test.row)
		if err != nil {
			fmt.Printf("  ❌ %s: 错误 - %v\n", test.name, err)
			continue
		}
		
		if fmt.Sprintf("%v", result) == fmt.Sprintf("%v", test.expected) {
			fmt.Printf("  ✅ %s: %v\n", test.name, result)
		} else {
			fmt.Printf("  ❌ %s: 期望 %v, 实际 %v\n", test.name, test.expected, result)
		}
	}
}
