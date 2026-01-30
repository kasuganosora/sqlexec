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
		{
			Name: "json_quote",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_quote", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     jsonQuote,
			Description: "将字符串转为JSON字符串（添加转义）",
			Example:     "JSON_QUOTE('hello') -> \"\\\"hello\\\"\"",
			Category:    "json",
		},
		{
			Name: "json_unquote",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_unquote", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     jsonUnquote,
			Description: "取消JSON字符串的引号",
			Example:     "JSON_UNQUOTE('\\\"hello\\\"') -> 'hello'",
			Category:    "json",
		},
		{
			Name: "json_contains_path",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_contains_path", ReturnType: "integer", ParamTypes: []string{"json", "string", "string"}, Variadic: true},
			},
			Handler:     jsonContainsPath,
			Description: "检查路径是否存在",
			Example:     "JSON_CONTAINS_PATH('{\"a\": 1}', 'one', '$.a') -> 1",
			Category:    "json",
		},
		{
			Name: "json_keys",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_keys", ReturnType: "json", ParamTypes: []string{"json"}, Variadic: false},
			},
			Handler:     jsonKeys,
			Description: "返回JSON对象的所有键",
			Example:     "JSON_KEYS('{\"a\": 1, \"b\": 2}') -> [\"a\", \"b\"]",
			Category:    "json",
		},
		{
			Name: "json_search",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_search", ReturnType: "json", ParamTypes: []string{"json", "string", "string", "string"}, Variadic: false},
			},
			Handler:     jsonSearch,
			Description: "在JSON中搜索字符串",
			Example:     "JSON_SEARCH('{\"a\": \"hello\"}', 'one', 'hello') -> '$.a'",
			Category:    "json",
		},
		{
			Name: "json_set",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_set", ReturnType: "json", ParamTypes: []string{"json", "string", "any"}, Variadic: true},
			},
			Handler:     jsonSet,
			Description: "设置值（替换或插入）",
			Example:     "JSON_SET('{\"a\": 1}', '$.a', 2) -> {\"a\": 2}",
			Category:    "json",
		},
		{
			Name: "json_insert",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_insert", ReturnType: "json", ParamTypes: []string{"json", "string", "any"}, Variadic: true},
			},
			Handler:     jsonInsert,
			Description: "仅插入新值（不覆盖已存在）",
			Example:     "JSON_INSERT('{\"a\": 1}', '$.b', 2) -> {\"a\": 1, \"b\": 2}",
			Category:    "json",
		},
		{
			Name: "json_replace",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_replace", ReturnType: "json", ParamTypes: []string{"json", "string", "any"}, Variadic: true},
			},
			Handler:     jsonReplace,
			Description: "仅替换已存在值",
			Example:     "JSON_REPLACE('{\"a\": 1}', '$.a', 2) -> {\"a\": 2}",
			Category:    "json",
		},
		{
			Name: "json_remove",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_remove", ReturnType: "json", ParamTypes: []string{"json", "string"}, Variadic: true},
			},
			Handler:     jsonRemove,
			Description: "删除指定路径的值",
			Example:     "JSON_REMOVE('{\"a\": 1, \"b\": 2}', '$.b') -> {\"a\": 1}",
			Category:    "json",
		},
		{
			Name: "json_merge_preserve",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_merge_preserve", ReturnType: "json", ParamTypes: []string{"json", "json"}, Variadic: true},
			},
			Handler:     jsonMergePreserve,
			Description: "保留所有值的合并",
			Example:     "JSON_MERGE_PRESERVE('{\"a\": 1}', '{\"b\": 2}') -> {\"a\": 1, \"b\": 2}",
			Category:    "json",
		},
		{
			Name: "json_merge_patch",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_merge_patch", ReturnType: "json", ParamTypes: []string{"json", "json"}, Variadic: true},
			},
			Handler:     jsonMergePatch,
			Description: "RFC 7396合并（后者覆盖前者）",
			Example:     "JSON_MERGE_PATCH('{\"a\": 1}', '{\"a\": 2, \"b\": 3}') -> {\"a\": 2, \"b\": 3}",
			Category:    "json",
		},
		{
			Name: "json_length",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_length", ReturnType: "integer", ParamTypes: []string{"json"}, Variadic: false},
			},
			Handler:     jsonLength,
			Description: "返回数组长度或对象键数量",
			Example:     "JSON_LENGTH('[1, 2, 3]') -> 3",
			Category:    "json",
		},
		{
			Name: "json_depth",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_depth", ReturnType: "integer", ParamTypes: []string{"json"}, Variadic: false},
			},
			Handler:     jsonDepth,
			Description: "返回JSON最大深度",
			Example:     "JSON_DEPTH('{\"a\": {\"b\": 1}}') -> 2",
			Category:    "json",
		},
		{
			Name: "json_pretty",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_pretty", ReturnType: "string", ParamTypes: []string{"json"}, Variadic: false},
			},
			Handler:     jsonPretty,
			Description: "格式化JSON输出",
			Example:     "JSON_PRETTY('{\"a\":1}') -> '{\n  \"a\": 1\n}'",
			Category:    "json",
		},
		{
			Name: "json_storage_size",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_storage_size", ReturnType: "integer", ParamTypes: []string{"json"}, Variadic: false},
			},
			Handler:     jsonStorageSize,
			Description: "返回JSON值的存储大小",
			Example:     "JSON_STORAGE_SIZE('{\"a\": 1}') -> 9",
			Category:    "json",
		},
		{
			Name: "json_array_append",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_array_append", ReturnType: "json", ParamTypes: []string{"json"}, Variadic: true},
			},
			Handler:     jsonArrayAppend,
			Description: "追加值到数组末尾",
			Example:     "JSON_ARRAY_APPEND('[1, 2]', '$', 3) -> [1, 2, 3]",
			Category:    "json",
		},
		{
			Name: "json_array_insert",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_array_insert", ReturnType: "json", ParamTypes: []string{"json", "string", "string", "any"}, Variadic: false},
			},
			Handler:     jsonArrayInsert,
			Description: "在指定位置插入到数组",
			Example:     "JSON_ARRAY_INSERT('[1, 3]', '$', 1, 2) -> [1, 2, 3]",
			Category:    "json",
		},
		{
			Name: "json_member_of",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_member_of", ReturnType: "integer", ParamTypes: []string{"json", "json"}, Variadic: false},
			},
			Handler:     jsonMemberOf,
			Description: "检查值是否为数组成员",
			Example:     "JSON_MEMBER_OF(1, '[1, 2, 3]') -> 1",
			Category:    "json",
		},
		{
			Name: "json_overlaps",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "json_overlaps", ReturnType: "integer", ParamTypes: []string{"json", "json"}, Variadic: false},
			},
			Handler:     jsonOverlaps,
			Description: "检查两个JSON数组是否有重叠",
			Example:     "JSON_OVERLAPS('[1, 2]', '[2, 3]') -> 1",
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

func jsonQuote(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return "", nil
	}

	str := ToString(args[0])
	return jsonpkg.Quote(str), nil
}

func jsonUnquote(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return "", nil
	}

	str := ToString(args[0])
	return jsonpkg.Unquote(str)
}

func jsonContainsPath(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("JSON_CONTAINS_PATH requires at least 3 arguments")
	}

	// Parse JSON document
	var bj jsonpkg.BinaryJSON
	var err error
	switch v := args[0].(type) {
	case string:
		bj, err = jsonpkg.ParseJSON(v)
		if err != nil {
			return int64(0), nil
		}
	default:
		bj, err = jsonpkg.NewBinaryJSON(v)
		if err != nil {
			return int64(0), nil
		}
	}

	// Get 'one' or 'all'
	oneOrAll := ToString(args[1])

	// Get paths
	paths := make([]string, 0, len(args)-2)
	for i := 2; i < len(args); i++ {
		paths = append(paths, ToString(args[i]))
	}

	result, err := jsonpkg.ContainsPath(bj, oneOrAll, paths...)
	if err != nil {
		return int64(0), nil
	}

	if result {
		return int64(1), nil
	}
	return int64(0), nil
}

func jsonKeys(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("JSON_KEYS requires at least 1 argument")
	}

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

	result, err := jsonpkg.Keys(bj)
	if err != nil {
		return nil, err
	}

	data, err := result.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonSearch(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("JSON_SEARCH requires at least 3 arguments")
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

	_ = bj // TODO: Implement full search functionality

	// Note: Simplified implementation - returns null for now
	// Full implementation would need to search through all paths
	return nil, nil
}

func jsonSet(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("JSON_SET requires at least 3 arguments")
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

	// Process (path, value) pairs
	for i := 1; i+1 < len(args); i += 2 {
		pathStr := ToString(args[i])
		value := args[i+1]
		bj, err = bj.Set(pathStr, value)
		if err != nil {
			return nil, err
		}
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonInsert(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("JSON_INSERT requires at least 3 arguments")
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

	// Process (path, value) pairs
	for i := 1; i+1 < len(args); i += 2 {
		pathStr := ToString(args[i])
		value := args[i+1]
		bj, err = bj.Insert(pathStr, value)
		if err != nil {
			return nil, err
		}
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonReplace(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("JSON_REPLACE requires at least 3 arguments")
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

	// Process (path, value) pairs
	for i := 1; i+1 < len(args); i += 2 {
		pathStr := ToString(args[i])
		value := args[i+1]
		bj, err = bj.Replace(pathStr, value)
		if err != nil {
			return nil, err
		}
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonRemove(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("JSON_REMOVE requires at least 2 arguments")
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

	// Collect paths
	paths := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		paths = append(paths, ToString(args[i]))
	}

	bj, err = bj.Remove(paths...)
	if err != nil {
		return nil, err
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonMergePreserve(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("JSON_MERGE_PRESERVE requires at least 2 arguments")
	}

	// Parse first JSON document
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

	// Merge with remaining arguments
	for i := 1; i < len(args); i++ {
		bj, err = bj.Merge(args[i])
		if err != nil {
			return nil, err
		}
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonMergePatch(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("JSON_MERGE_PATCH requires at least 2 arguments")
	}

	// Parse first JSON document
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

	// Patch with remaining arguments
	for i := 1; i < len(args); i++ {
		bj, err = bj.Patch(args[i])
		if err != nil {
			return nil, err
		}
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonLength(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("JSON_LENGTH requires at least 1 argument")
	}

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

	length, err := jsonpkg.Length(bj)
	if err != nil {
		return nil, err
	}
	return int64(length), nil
}

func jsonDepth(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("JSON_DEPTH requires at least 1 argument")
	}

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

	depth, err := jsonpkg.Depth(bj)
	if err != nil {
		return nil, err
	}
	return int64(depth), nil
}

func jsonPretty(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return "", nil
	}

	var bj jsonpkg.BinaryJSON
	var err error
	switch v := args[0].(type) {
	case string:
		bj, err = jsonpkg.ParseJSON(v)
		if err != nil {
			return "", nil
		}
	default:
		bj, err = jsonpkg.NewBinaryJSON(v)
		if err != nil {
			return "", nil
		}
	}

	return jsonpkg.Pretty(bj)
}

func jsonStorageSize(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("JSON_STORAGE_SIZE requires at least 1 argument")
	}

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

	size, err := jsonpkg.StorageSize(bj)
	if err != nil {
		return nil, err
	}
	return int64(size), nil
}

func jsonArrayAppend(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("JSON_ARRAY_APPEND requires at least 2 arguments")
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

	result, err := jsonpkg.ArrayAppend(bj, args[1:]...)
	if err != nil {
		return nil, err
	}

	data, err := result.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonArrayInsert(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("JSON_ARRAY_INSERT requires at least 3 arguments")
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

	result, err := jsonpkg.ArrayInsert(bj, args[1:]...)
	if err != nil {
		return nil, err
	}

	data, err := result.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func jsonMemberOf(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_MEMBER_OF requires 2 arguments")
	}

	target := args[0]
	container := args[1]

	result, err := jsonpkg.MemberOf(target, container)
	if err != nil {
		return int64(0), nil
	}

	if result {
		return int64(1), nil
	}
	return int64(0), nil
}

func jsonOverlaps(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_OVERLAPS requires 2 arguments")
	}

	result, err := jsonpkg.Overlaps(args[0], args[1])
	if err != nil {
		return int64(0), nil
	}

	if result {
		return int64(1), nil
	}
	return int64(0), nil
}

