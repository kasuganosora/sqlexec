package builtin

import (
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/json"
)

func init() {
	// JSON类型和验证函数
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_type",
		DisplayName:  "JSON_TYPE",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		MinArgs:     1,
		MaxArgs:     1,
		Handler:      jsonType,
		Description: "返回JSON值的类型",
		Examples:     []string{"JSON_TYPE('{\"a\": 1}') -> 'OBJECT'"},
		Parameters:   []FunctionParam{{Name: "json_doc", Type: "any", Required: true}},
		ReturnType:  "string",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_valid",
		DisplayName:  "JSON_VALID",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		MinArgs:     1,
		MaxArgs:     1,
		Handler:      jsonValid,
		Description: "验证字符串是否为有效JSON",
		Examples:     []string{"JSON_VALID('{\"a\": 1}') -> 1"},
		Parameters:   []FunctionParam{{Name: "val", Type: "any", Required: true}},
		ReturnType:  "integer",
	})
	
	// JSON创建和转换函数
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_array",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		Variadic:    true,
		MinArgs:     0,
		Handler:      jsonArray,
		Description:  "创建JSON数组",
		Examples:     []string{"JSON_ARRAY(1, 2, 3) -> [1, 2, 3]"},
		Parameters:   []FunctionParam{{Name: "value", Type: "any", Variadic: true}},
		ReturnType:  "json",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_object",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		Variadic:    true,
		MinArgs:     0,
		Handler:      jsonObject,
		Description:  "创建JSON对象",
		Examples:     []string{"JSON_OBJECT('key', 'value') -> {\"key\": \"value\"}"},
		Parameters:   []FunctionParam{{Name: "key/value", Type: "any", Variadic: true}},
		ReturnType:  "json",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_quote",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		MinArgs:     1,
		Handler:      jsonQuote,
		Description:  "将字符串转为JSON字符串",
		Examples:     []string{"JSON_QUOTE('null') -> \"null\""},
		Parameters:   []FunctionParam{{Name: "str", Type: "string", Required: true}},
		ReturnType:  "json",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_unquote",
		DisplayName:  "JSON_UNQUOTE",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		MinArgs:     1,
		Handler:      jsonUnquote,
		Description:  "取消JSON字符串的引号",
		Examples:     []string{"JSON_UNQUOTE('\\\"hello\\\"') -> 'hello'"},
		Parameters:   []FunctionParam{{Name: "json_str", Type: "json", Required: true}},
		ReturnType:  "string",
	})
	
	// JSON路径和提取函数
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_extract",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		Variadic:    true,
		MinArgs:     2,
		Handler:      jsonExtract,
		Description:  "使用路径表达式提取JSON值",
		Examples:     []string{"JSON_EXTRACT('{\"a\": 1}', '$.a') -> 1"},
		Parameters:   []FunctionParam{
			{Name: "json_doc", Type: "json", Required: true},
			{Name: "path", Type: "string", Variadic: true},
		},
		ReturnType:  "json",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_contains",
		DisplayName:  "JSON_CONTAINS",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		MinArgs:     2,
		Handler:      jsonContains,
		Description:  "检查JSON是否包含目标值",
		Examples:     []string{"JSON_CONTAINS('{\"a\": 1}', 1) -> 1"},
		Parameters:   []FunctionParam{
			{Name: "target", Type: "json", Required: true},
			{Name: "candidate", Type: "json", Required: true},
		},
		ReturnType:  "integer",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_contains_path",
		DisplayName:  "JSON_CONTAINS_PATH",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		Variadic:    true,
		MinArgs:     2,
		Handler:      jsonContainsPath,
		Description:  "检查路径是否存在",
		Examples:     []string{"JSON_CONTAINS_PATH('{\"a\": 1}', 'one', '$.a') -> 1"},
		Parameters:   []FunctionParam{
			{Name: "json_doc", Type: "json", Required: true},
			{Name: "one_or_all", Type: "string", Required: true},
			{Name: "path", Type: "string", Variadic: true},
		},
		ReturnType:  "integer",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_keys",
		DisplayName:  "JSON_KEYS",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		MinArgs:     1,
		Handler:      jsonKeys,
		Description:  "返回JSON对象的所有键",
		Examples:     []string{"JSON_KEYS('{\"a\": 1, \"b\": 2}') -> [\"a\", \"b\"]"},
		Parameters:   []FunctionParam{{Name: "json_doc", Type: "json", Required: true}},
		ReturnType:  "json",
	})
	
	RegisterGlobal(&FunctionMetadata{
		Name:        "json_search",
		DisplayName:  "JSON_SEARCH",
		Type:        FunctionTypeScalar,
		Scope:       ScopeGlobal,
		Category:    CategoryJSON,
		Variadic:    true,
		MinArgs:     2,
		Handler:      jsonSearch,
		Description:  "在JSON中搜索字符串",
		Examples:     []string{"JSON_SEARCH('{\"a\": {\"b\": \"hello\"}}', 'one', 'hello') -> '$.b'"},
		Parameters:   []FunctionParam{
			{Name: "json_doc", Type: "json", Required: true},
			{Name: "one_or_all", Type: "string", Required: true},
			{Name: "search_str", Type: "string", Required: true},
		},
		ReturnType:  "json",
	})
}

// 函数处理器实现
func jsonType(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_TYPE requires at least one argument"}
	}
	bj, err := FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	return bj.Type(), nil
}

func jsonValid(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_VALID requires at least one argument"}
	}
	bj, err := FromBuiltinArg(args[0])
	if err != nil {
		return int64(0), nil
	}
	return int64(1), nil
}

func jsonArray(args []interface{}) (interface{}, error) {
	return NewBinaryJSON(args)
}

func jsonObject(args []interface{}) (interface{}, error) {
	if len(args)%2 != 0 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_OBJECT requires even number of arguments"}
	}
	obj := make(map[string]interface{})
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_OBJECT keys must be strings"}
		}
		obj[key] = args[i+1]
	}
	return NewBinaryJSON(obj), nil
}

func jsonQuote(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_QUOTE requires one argument"}
	}
	return Quote(ConvertToString(args[0])), nil
}

func jsonUnquote(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_UNQUOTE requires one argument"}
	}
	str := ConvertToString(args[0])
	result, err := Unquote(str)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func jsonExtract(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_EXTRACT requires at least 2 arguments"}
	}
	bj, err := FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	if len(args) == 2 {
		pathStr := ConvertToString(args[1])
		result, err := bj.Extract(pathStr)
		if err != nil {
			return nil, err
		}
		return result.ToSQLValue(), nil
	}
	results := make([]interface{}, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		pathStr := ConvertToString(args[i])
		result, err := bj.Extract(pathStr)
		if err != nil {
			return nil, err
		}
		results[i-1] = result.ToSQLValue()
	}
	return NewBinaryJSON(results), nil
}

func jsonContains(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_CONTAINS requires 2 arguments"}
	}
	target, err := FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	candidate, err := FromBuiltinArg(args[1])
	if err != nil {
		return nil, err
	}
	contains, err := json.Contains(target, candidate)
	if err != nil {
		return nil, err
	}
	return int64(0), nil
}

func jsonContainsPath(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_CONTAINS_PATH requires at least 2 arguments"}
	}
	bj, err := FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	oneOrAll := ConvertToString(args[1])
	paths := make([]string, 0)
	for i := 2; i < len(args); i++ {
		paths = append(paths, ConvertToString(args[i]))
	}
	exists, err := json.ContainsPath(bj, oneOrAll, paths...)
	if err != nil {
		return nil, err
	}
	return int64(0), nil
}

func jsonKeys(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_KEYS requires at least one argument"}
	}
	bj, err := FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	if len(args) > 1 {
		pathStr := ConvertToString(args[1])
		target, err := bj.Extract(pathStr)
		if err != nil {
			return nil, err
		}
		bj = target
	}
	keys, err := json.Keys(bj)
	if err != nil {
		return nil, err
	}
	return keys.ToSQLValue(), nil
}

func jsonSearch(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, &JSONError{Code: ErrInvalidParam, Message: "JSON_SEARCH requires at least 2 arguments"}
	}
	bj, err := FromBuiltinArg(args[0])
	if err != nil {
		return nil, err
	}
	oneOrAll := ConvertToString(args[1])
	searchStr := ConvertToString(args[2])
	var paths []string
	if len(args) > 3 {
		paths = make([]string, 0)
		for i := 3; i < len(args); i++ {
			paths = append(paths, ConvertToString(args[i]))
		}
	}
	result, err := json.Search(bj, searchStr, oneOrAll, paths...)
	if err != nil {
		return nil, err
	}
	return result.ToSQLValue(), nil
}
