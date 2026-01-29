package builtin

import (
	"fmt"

	jsonpkg "github.com/kasuganosora/sqlexec/pkg/json"
)

func init() {
	// 注册JSON函数
	jsonFunctions := []*FunctionInfo{
		{
			Name: "json_type",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_type", ReturnType: "string", ParamTypes: []string{"any"}, Variadic: false},
			},
			Handler:     jsonType,
			Description: "返回JSON值的类型",
			Example:     "JSON_TYPE('{\"a\": 1}') -> 'OBJECT'",
			Category:    "json",
		},
		{
			Name: "json_valid",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_valid", ReturnType: "integer", ParamTypes: []string{"any"}, Variadic: false},
			},
			Handler:     jsonValid,
			Description: "验证字符串是否为有效JSON",
			Example:     "JSON_VALID('{\"a\": 1}') -> 1",
			Category:    "json",
		},
		{
			Name: "json_array",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_array", ReturnType: "json", ParamTypes: []string{"any"}, Variadic: true},
			},
			Handler:     jsonArray,
			Description: "创建JSON数组",
			Example:     "JSON_ARRAY(1, 2, 3) -> [1, 2, 3]",
			Category:    "json",
		},
		{
			Name: "json_object",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_object", ReturnType: "json", ParamTypes: []string{"any"}, Variadic: true},
			},
			Handler:     jsonObject,
			Description: "创建JSON对象",
			Example:     "JSON_OBJECT('key', 'value') -> {\"key\": \"value\"}",
			Category:    "json",
		},
		{
			Name: "json_extract",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_extract", ReturnType: "json", ParamTypes: []string{"json", "string"}, Variadic: true},
			},
			Handler:     jsonExtract,
			Description: "使用路径表达式提取JSON值",
			Example:     "JSON_EXTRACT('{\"a\": 1}', '$.a') -> 1",
			Category:    "json",
		},
		{
			Name: "json_contains",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_contains", ReturnType: "integer", ParamTypes: []string{"json", "json"}, Variadic: false},
			},
			Handler:     jsonContains,
			Description: "检查JSON是否包含目标值",
			Example:     "JSON_CONTAINS('{\"a\": 1}', 1) -> 1",
			Category:    "json",
		},
	}

	// 注册所有函数
	for _, fn := range jsonFunctions {
		if err := RegisterGlobal(fn); err != nil {
			panic(fmt.Sprintf("Failed to register JSON function %s: %v", fn.Name, err))
		}
	}
}

// 函数处理器实现
func jsonType(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("JSON_TYPE requires at least one argument")
	}

	var bj jsonpkg.BinaryJSON
	var err error

	if s, ok := args[0].(string); ok {
		bj, err = jsonpkg.ParseJSON(s)
		if err != nil {
			return "NULL", nil
		}
		return bj.Type(), nil
	}

	bj, err = jsonpkg.NewBinaryJSON(args[0])
	if err != nil {
		return "NULL", nil
	}
	return bj.Type(), nil
}

func jsonValid(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return int64(0), nil
	}

	switch v := args[0].(type) {
	case string:
		_, err := jsonpkg.ParseJSON(v)
		if err != nil {
			return int64(0), nil
		}
		return int64(1), nil
	case []byte:
		_, err := jsonpkg.ParseJSON(string(v))
		if err != nil {
			return int64(0), nil
		}
		return int64(1), nil
	default:
		_, err := jsonpkg.NewBinaryJSON(v)
		if err != nil {
			return int64(0), nil
		}
		return int64(1), nil
	}
}

func jsonArray(args []interface{}) (interface{}, error) {
	bj, err := jsonpkg.NewBinaryJSON(args)
	if err != nil {
		return nil, err
	}
	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonObject(args []interface{}) (interface{}, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("JSON_OBJECT requires even number of arguments")
	}

	obj := make(map[string]interface{})
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			if s, ok := args[i].([]byte); ok {
				key = string(s)
			} else {
				return nil, fmt.Errorf("JSON_OBJECT keys must be strings")
			}
		}
		obj[key] = args[i+1]
	}

	bj, err := jsonpkg.NewBinaryJSON(obj)
	if err != nil {
		return nil, err
	}
	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonExtract(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("JSON_EXTRACT requires at least 2 arguments")
	}

	// Parse JSON document
	var bj jsonpkg.BinaryJSON
	var err error
	switch v := args[0].(type) {
	case string:
		bj, err = jsonpkg.ParseJSON(v)
		if err != nil {
			return nil, err
		}
	default:
		bj, err = jsonpkg.NewBinaryJSON(v)
		if err != nil {
			return nil, err
		}
	}

	// Extract with paths
	if len(args) == 2 {
		pathStr := ToString(args[1])
		result, err := bj.Extract(pathStr)
		if err != nil {
			return nil, err
		}
		data, err := result.MarshalJSON()
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}

	// Multiple paths
	results := make([]interface{}, 0)
	for i := 1; i < len(args); i++ {
		pathStr := ToString(args[i])
		result, err := bj.Extract(pathStr)
		if err != nil {
			return nil, err
		}
		results = append(results, result.GetInterface())
	}

	// If only one path, return value directly
	if len(results) == 1 {
		bj, err := jsonpkg.NewBinaryJSON(results[0])
		if err != nil {
			return nil, err
		}
		data, err := bj.MarshalJSON()
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}

	// Otherwise return as array
	bj, err = jsonpkg.NewBinaryJSON(results)
	if err != nil {
		return nil, err
	}
	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonContains(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_CONTAINS requires 2 arguments")
	}

	// Parse target
	var target jsonpkg.BinaryJSON
	var err error
	switch v := args[0].(type) {
	case string:
		target, err = jsonpkg.ParseJSON(v)
		if err != nil {
			return int64(0), nil
		}
	default:
		target, err = jsonpkg.NewBinaryJSON(v)
		if err != nil {
			return int64(0), nil
		}
	}

	// Parse candidate
	var candidate jsonpkg.BinaryJSON
	switch v := args[1].(type) {
	case string:
		candidate, err = jsonpkg.ParseJSON(v)
		if err != nil {
			return int64(0), nil
		}
	default:
		candidate, err = jsonpkg.NewBinaryJSON(v)
		if err != nil {
			return int64(0), nil
		}
	}

	// Simple contains check
	if target.Equals(candidate) {
		return int64(1), nil
	}

	// For arrays, check if candidate is in array
	if target.IsArray() {
		arr, _ := target.GetArray()
		for _, item := range arr {
			bjItem, _ := jsonpkg.NewBinaryJSON(item)
			if bjItem.Equals(candidate) {
				return int64(1), nil
			}
		}
	}

	return int64(0), nil
}
