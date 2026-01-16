package datasource

import (
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/dop251/goja"
)

// FunctionType 函数类型
type FunctionType int

const (
	FunctionTypeBuiltin FunctionType = iota
	FunctionTypeUser
)

// Function 函数接口
type Function interface {
	// Name 获取函数名
	Name() string
	// Type 获取函数类型
	Type() FunctionType
	// Call 调用函数
	Call(args ...interface{}) (interface{}, error)
}

// BuiltinFunction 内置函数
type BuiltinFunction struct {
	name     string
	function func(args ...interface{}) (interface{}, error)
}

func (f *BuiltinFunction) Name() string {
	return f.name
}

func (f *BuiltinFunction) Type() FunctionType {
	return FunctionTypeBuiltin
}

func (f *BuiltinFunction) Call(args ...interface{}) (interface{}, error) {
	return f.function(args...)
}

// UserFunction 用户自定义函数
type UserFunction struct {
	name     string
	vm       *goja.Runtime
	function goja.Callable
}

func (f *UserFunction) Name() string {
	return f.name
}

func (f *UserFunction) Type() FunctionType {
	return FunctionTypeUser
}

func (f *UserFunction) Call(args ...interface{}) (interface{}, error) {
	// 转换参数为JavaScript值
	jsArgs := make([]goja.Value, len(args))
	for i, arg := range args {
		jsArgs[i] = f.vm.ToValue(arg)
	}

	// 调用函数
	result, err := f.function(goja.Undefined(), jsArgs...)
	if err != nil {
		return nil, fmt.Errorf("调用函数 %s 失败: %v", f.name, err)
	}

	// 转换结果为Go值
	return result.Export(), nil
}

// FunctionManager 函数管理器
type FunctionManager struct {
	functions map[string]Function
	vm        *goja.Runtime
}

// NewFunctionManager 创建函数管理器
func NewFunctionManager() *FunctionManager {
	vm := goja.New()
	return &FunctionManager{
		functions: make(map[string]Function),
		vm:        vm,
	}
}

// RegisterBuiltinFunction 注册内置函数
func (m *FunctionManager) RegisterBuiltinFunction(name string, function func(args ...interface{}) (interface{}, error)) {
	m.functions[name] = &BuiltinFunction{
		name:     name,
		function: function,
	}
}

// LoadUserFunctions 加载用户自定义函数
func (m *FunctionManager) LoadUserFunctions(dir string) error {
	// 读取functions目录下的所有.js文件
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("读取函数目录失败: %v", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".js") {
			continue
		}

		// 读取文件内容
		content, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return fmt.Errorf("读取函数文件失败: %v", err)
		}

		// 解析函数名
		name := strings.TrimSuffix(file.Name(), ".js")

		// 编译JavaScript代码
		program, err := goja.Compile(name, string(content), false)
		if err != nil {
			return fmt.Errorf("编译函数失败: %v", err)
		}

		// 执行代码
		_, err = m.vm.RunProgram(program)
		if err != nil {
			return fmt.Errorf("执行函数失败: %v", err)
		}

		// 获取函数对象
		fn, ok := goja.AssertFunction(m.vm.Get(name))
		if !ok {
			return fmt.Errorf("函数 %s 不是有效的函数", name)
		}

		// 注册函数
		m.functions[name] = &UserFunction{
			name:     name,
			vm:       m.vm,
			function: fn,
		}
	}

	return nil
}

// GetFunction 获取函数
func (m *FunctionManager) GetFunction(name string) (Function, bool) {
	fn, ok := m.functions[name]
	return fn, ok
}

// 注册内置函数
func (m *FunctionManager) registerBuiltinFunctions() {
	// 字符串函数
	m.RegisterBuiltinFunction("CONCAT", func(args ...interface{}) (interface{}, error) {
		var result strings.Builder
		for _, arg := range args {
			result.WriteString(fmt.Sprintf("%v", arg))
		}
		return result.String(), nil
	})

	m.RegisterBuiltinFunction("UPPER", func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("UPPER函数需要一个参数")
		}
		return strings.ToUpper(fmt.Sprintf("%v", args[0])), nil
	})

	m.RegisterBuiltinFunction("LOWER", func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("LOWER函数需要一个参数")
		}
		return strings.ToLower(fmt.Sprintf("%v", args[0])), nil
	})

	// 数学函数
	m.RegisterBuiltinFunction("ABS", func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("ABS函数需要一个参数")
		}
		switch v := args[0].(type) {
		case int64:
			if v < 0 {
				return -v, nil
			}
			return v, nil
		case float64:
			if v < 0 {
				return -v, nil
			}
			return v, nil
		default:
			return nil, fmt.Errorf("ABS函数参数类型错误")
		}
	})

	m.RegisterBuiltinFunction("ROUND", func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 && len(args) != 2 {
			return nil, fmt.Errorf("ROUND函数需要一个或两个参数")
		}
		var num float64
		var places int
		switch v := args[0].(type) {
		case int64:
			num = float64(v)
		case float64:
			num = v
		default:
			return nil, fmt.Errorf("ROUND函数参数类型错误")
		}
		if len(args) == 2 {
			switch v := args[1].(type) {
			case int64:
				places = int(v)
			default:
				return nil, fmt.Errorf("ROUND函数精度参数类型错误")
			}
		}
		// 实现四舍五入
		scale := math.Pow10(places)
		return math.Round(num*scale) / scale, nil
	})

	// 日期函数
	m.RegisterBuiltinFunction("NOW", func(args ...interface{}) (interface{}, error) {
		return time.Now(), nil
	})

	m.RegisterBuiltinFunction("DATE_FORMAT", func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("DATE_FORMAT函数需要两个参数")
		}
		date, ok := args[0].(time.Time)
		if !ok {
			return nil, fmt.Errorf("DATE_FORMAT函数第一个参数必须是日期类型")
		}
		format, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("DATE_FORMAT函数第二个参数必须是字符串类型")
		}
		return date.Format(format), nil
	})

	// 聚合函数
	m.RegisterBuiltinFunction("COUNT", func(args ...interface{}) (interface{}, error) {
		return len(args), nil
	})

	m.RegisterBuiltinFunction("SUM", func(args ...interface{}) (interface{}, error) {
		var sum float64
		for _, arg := range args {
			switch v := arg.(type) {
			case int64:
				sum += float64(v)
			case float64:
				sum += v
			default:
				return nil, fmt.Errorf("SUM函数参数类型错误")
			}
		}
		return sum, nil
	})

	m.RegisterBuiltinFunction("AVG", func(args ...interface{}) (interface{}, error) {
		if len(args) == 0 {
			return 0, nil
		}
		sumFn, ok := m.GetFunction("SUM")
		if !ok {
			return nil, fmt.Errorf("SUM函数未找到")
		}
		sum, err := sumFn.Call(args...)
		if err != nil {
			return nil, err
		}
		return sum.(float64) / float64(len(args)), nil
	})

	m.RegisterBuiltinFunction("MAX", func(args ...interface{}) (interface{}, error) {
		if len(args) == 0 {
			return nil, fmt.Errorf("MAX函数需要至少一个参数")
		}
		var max interface{}
		for i, arg := range args {
			if i == 0 {
				max = arg
				continue
			}
			switch v1 := max.(type) {
			case int64:
				if v2, ok := arg.(int64); ok && v2 > v1 {
					max = v2
				}
			case float64:
				if v2, ok := arg.(float64); ok && v2 > v1 {
					max = v2
				}
			case string:
				if v2, ok := arg.(string); ok && v2 > v1 {
					max = v2
				}
			case time.Time:
				if v2, ok := arg.(time.Time); ok && v2.After(v1) {
					max = v2
				}
			}
		}
		return max, nil
	})

	m.RegisterBuiltinFunction("MIN", func(args ...interface{}) (interface{}, error) {
		if len(args) == 0 {
			return nil, fmt.Errorf("MIN函数需要至少一个参数")
		}
		var min interface{}
		for i, arg := range args {
			if i == 0 {
				min = arg
				continue
			}
			switch v1 := min.(type) {
			case int64:
				if v2, ok := arg.(int64); ok && v2 < v1 {
					min = v2
				}
			case float64:
				if v2, ok := arg.(float64); ok && v2 < v1 {
					min = v2
				}
			case string:
				if v2, ok := arg.(string); ok && v2 < v1 {
					min = v2
				}
			case time.Time:
				if v2, ok := arg.(time.Time); ok && v2.Before(v1) {
					min = v2
				}
			}
		}
		return min, nil
	})
}
