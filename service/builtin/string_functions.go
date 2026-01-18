package builtin

import (
	"fmt"
	"strings"
	"unicode"
)

func init() {
	// 注册字符串函�?
	stringFunctions := []*FunctionInfo{
		{
			Name: "concat",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "concat", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: true},
			},
			Handler:     stringConcat,
			Description: "连接字符�?,
			Example:     "CONCAT('Hello', ' ', 'World') -> 'Hello World'",
			Category:    "string",
		},
		{
			Name: "concat_ws",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "concat_ws", ReturnType: "string", ParamTypes: []string{"string", "string"}, Variadic: true},
			},
			Handler:     stringConcatWS,
			Description: "使用分隔符连接字符串",
			Example:     "CONCAT_WS(',', 'a', 'b', 'c') -> 'a,b,c'",
			Category:    "string",
		},
		{
			Name: "length",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "length", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringLength,
			Description: "返回字符串长度（字节�?,
			Example:     "LENGTH('hello') -> 5",
			Category:    "string",
		},
		{
			Name: "char_length",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "char_length", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringLength,
			Description: "返回字符串长度（字符�?,
			Example:     "CHAR_LENGTH('hello') -> 5",
			Category:    "string",
		},
		{
			Name: "character_length",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "character_length", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringLength,
			Description: "返回字符串长度（字符�?,
			Example:     "CHARACTER_LENGTH('hello') -> 5",
			Category:    "string",
		},
		{
			Name: "upper",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "upper", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringUpper,
			Description: "转换为大�?,
			Example:     "UPPER('hello') -> 'HELLO'",
			Category:    "string",
		},
		{
			Name: "ucase",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ucase", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringUpper,
			Description: "转换为大写（upper的别名）",
			Example:     "UCASE('hello') -> 'HELLO'",
			Category:    "string",
		},
		{
			Name: "lower",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "lower", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringLower,
			Description: "转换为小�?,
			Example:     "LOWER('HELLO') -> 'hello'",
			Category:    "string",
		},
		{
			Name: "lcase",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "lcase", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringLower,
			Description: "转换为小写（lower的别名）",
			Example:     "LCASE('HELLO') -> 'hello'",
			Category:    "string",
		},
		{
			Name: "trim",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "trim", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringTrim,
			Description: "删除前后空格",
			Example:     "TRIM('  hello  ') -> 'hello'",
			Category:    "string",
		},
		{
			Name: "ltrim",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ltrim", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringLTrim,
			Description: "删除前导空格",
			Example:     "LTRIM('  hello') -> 'hello'",
			Category:    "string",
		},
		{
			Name: "rtrim",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "rtrim", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringRTrim,
			Description: "删除尾部空格",
			Example:     "RTRIM('hello  ') -> 'hello'",
			Category:    "string",
		},
		{
			Name: "left",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "left", ReturnType: "string", ParamTypes: []string{"string", "integer"}, Variadic: false},
			},
			Handler:     stringLeft,
			Description: "返回左边的n个字�?,
			Example:     "LEFT('hello', 3) -> 'hel'",
			Category:    "string",
		},
		{
			Name: "right",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "right", ReturnType: "string", ParamTypes: []string{"string", "integer"}, Variadic: false},
			},
			Handler:     stringRight,
			Description: "返回右边的n个字�?,
			Example:     "RIGHT('hello', 3) -> 'llo'",
			Category:    "string",
		},
		{
			Name: "substring",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "substring", ReturnType: "string", ParamTypes: []string{"string", "integer"}, Variadic: false},
				{Name: "substring", ReturnType: "string", ParamTypes: []string{"string", "integer", "integer"}, Variadic: false},
			},
			Handler:     stringSubstring,
			Description: "返回子字符串",
			Example:     "SUBSTRING('hello', 2, 3) -> 'ell'",
			Category:    "string",
		},
		{
			Name: "substr",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "substr", ReturnType: "string", ParamTypes: []string{"string", "integer"}, Variadic: false},
				{Name: "substr", ReturnType: "string", ParamTypes: []string{"string", "integer", "integer"}, Variadic: false},
			},
			Handler:     stringSubstring,
			Description: "返回子字符串（substring的别名）",
			Example:     "SUBSTR('hello', 2, 3) -> 'ell'",
			Category:    "string",
		},
		{
			Name: "replace",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "replace", ReturnType: "string", ParamTypes: []string{"string", "string", "string"}, Variadic: false},
			},
			Handler:     stringReplace,
			Description: "替换字符�?,
			Example:     "REPLACE('hello world', 'world', 'there') -> 'hello there'",
			Category:    "string",
		},
		{
			Name: "repeat",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "repeat", ReturnType: "string", ParamTypes: []string{"string", "integer"}, Variadic: false},
			},
			Handler:     stringRepeat,
			Description: "重复字符�?,
			Example:     "REPEAT('ab', 3) -> 'ababab'",
			Category:    "string",
		},
		{
			Name: "reverse",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "reverse", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringReverse,
			Description: "反转字符�?,
			Example:     "REVERSE('hello') -> 'olleh'",
			Category:    "string",
		},
		{
			Name: "lpad",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "lpad", ReturnType: "string", ParamTypes: []string{"string", "integer", "string"}, Variadic: false},
			},
			Handler:     stringLPad,
			Description: "左填充字符串",
			Example:     "LPAD('hello', 10, '*') -> '*****hello'",
			Category:    "string",
		},
		{
			Name: "rpad",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "rpad", ReturnType: "string", ParamTypes: []string{"string", "integer", "string"}, Variadic: false},
			},
			Handler:     stringRPad,
			Description: "右填充字符串",
			Example:     "RPAD('hello', 10, '*') -> 'hello*****'",
			Category:    "string",
		},
		{
			Name: "position",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "position", ReturnType: "integer", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     stringPosition,
			Description: "返回子串位置",
			Example:     "POSITION('ll' IN 'hello') -> 3",
			Category:    "string",
		},
		{
			Name: "locate",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "locate", ReturnType: "integer", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     stringPosition,
			Description: "返回子串位置（position的别名）",
			Example:     "LOCATE('ll', 'hello') -> 3",
			Category:    "string",
		},
		{
			Name: "instr",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "instr", ReturnType: "integer", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     stringInstr,
			Description: "返回子串位置",
			Example:     "INSTR('hello', 'll') -> 3",
			Category:    "string",
		},
		{
			Name: "ascii",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ascii", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringASCII,
			Description: "返回字符的ASCII�?,
			Example:     "ASCII('A') -> 65",
			Category:    "string",
		},
		{
			Name: "ord",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ord", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringASCII,
			Description: "返回字符的ASCII�?,
			Example:     "ORD('A') -> 65",
			Category:    "string",
		},
		{
			Name: "space",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "space", ReturnType: "string", ParamTypes: []string{"integer"}, Variadic: false},
			},
			Handler:     stringSpace,
			Description: "返回指定数量的空�?,
			Example:     "SPACE(5) -> '     '",
			Category:    "string",
		},
	}

	for _, fn := range stringFunctions {
		RegisterGlobal(fn)
	}
}

// 辅助函数：将参数转换为字符串
func toString(arg interface{}) string {
	switch v := arg.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// 辅助函数：将参数转换为整�?
func toInt64(arg interface{}) (int64, error) {
	switch v := arg.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", arg)
	}
}

// 字符串函数实�?
func stringConcat(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", nil
	}
	
	result := ""
	for _, arg := range args {
		result += toString(arg)
	}
	return result, nil
}

func stringConcatWS(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", nil
	}
	
	separator := toString(args[0])
	if len(args) == 1 {
		return "", nil
	}
	
	result := toString(args[1])
	for i := 2; i < len(args); i++ {
		result += separator + toString(args[i])
	}
	return result, nil
}

func stringLength(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("length() requires exactly 1 argument")
	}
	return int64(len(toString(args[0]))), nil
}

func stringUpper(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("upper() requires exactly 1 argument")
	}
	return strings.ToUpper(toString(args[0])), nil
}

func stringLower(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("lower() requires exactly 1 argument")
	}
	return strings.ToLower(toString(args[0])), nil
}

func stringTrim(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("trim() requires exactly 1 argument")
	}
	return strings.TrimSpace(toString(args[0])), nil
}

func stringLTrim(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ltrim() requires exactly 1 argument")
	}
	return strings.TrimLeftFunc(toString(args[0]), unicode.IsSpace), nil
}

func stringRTrim(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("rtrim() requires exactly 1 argument")
	}
	return strings.TrimRightFunc(toString(args[0]), unicode.IsSpace), nil
}

func stringLeft(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("left() requires exactly 2 arguments")
	}
	
	str := toString(args[0])
	length, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	
	if length < 0 {
		length = 0
	}
	if int64(len(str)) < length {
		length = int64(len(str))
	}
	
	return str[:length], nil
}

func stringRight(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("right() requires exactly 2 arguments")
	}
	
	str := toString(args[0])
	length, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	
	if length < 0 {
		length = 0
	}
	if int64(len(str)) < length {
		length = int64(len(str))
	}
	
	return str[len(str)-int(length):], nil
}

func stringSubstring(args []interface{}) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("substring() requires 2 or 3 arguments")
	}
	
	str := toString(args[0])
	start, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	
	length := int64(len(str))
	if length == 0 {
		return "", nil
	}
	
	// SQL中索引从1开�?
	if start < 1 {
		start = 1
	}
	
	startPos := start - 1
	if startPos >= length {
		return "", nil
	}
	
	if len(args) == 3 {
		subLen, err := toInt64(args[2])
		if err != nil {
			return nil, err
		}
		
		if subLen < 0 {
			subLen = 0
		}
		
		endPos := startPos + subLen
		if endPos > length {
			endPos = length
		}
		
		return str[startPos:endPos], nil
	}
	
	return str[startPos:], nil
}

func stringReplace(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("replace() requires exactly 3 arguments")
	}
	
	str := toString(args[0])
	oldStr := toString(args[1])
	newStr := toString(args[2])
	
	return strings.ReplaceAll(str, oldStr, newStr), nil
}

func stringRepeat(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("repeat() requires exactly 2 arguments")
	}
	
	str := toString(args[0])
	count, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	
	if count <= 0 {
		return "", nil
	}
	
	return strings.Repeat(str, int(count)), nil
}

func stringReverse(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("reverse() requires exactly 1 argument")
	}
	
	str := toString(args[0])
	runes := []rune(str)
	
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	
	return string(runes), nil
}

func stringLPad(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("lpad() requires exactly 3 arguments")
	}
	
	str := toString(args[0])
	length, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	
	padStr := toString(args[2])
	if len(padStr) == 0 {
		return str, nil
	}
	
	if int64(len(str)) >= length {
		return str[:length], nil
	}
	
	paddingNeeded := int(length) - len(str)
	padding := ""
	for len(padding) < paddingNeeded {
		padding += padStr
	}
	
	return padding[:paddingNeeded] + str, nil
}

func stringRPad(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("rpad() requires exactly 3 arguments")
	}
	
	str := toString(args[0])
	length, err := toInt64(args[1])
	if err != nil {
		return nil, err
	}
	
	padStr := toString(args[2])
	if len(padStr) == 0 {
		return str, nil
	}
	
	if int64(len(str)) >= length {
		return str[:length], nil
	}
	
	paddingNeeded := int(length) - len(str)
	padding := ""
	for len(padding) < paddingNeeded {
		padding += padStr
	}
	
	return str + padding[:paddingNeeded], nil
}

func stringPosition(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("position() requires exactly 2 arguments")
	}
	
	substr := toString(args[0])
	str := toString(args[1])
	
	index := strings.Index(str, substr)
	if index == -1 {
		return int64(0), nil
	}
	
	// SQL中位置从1开�?
	return int64(index + 1), nil
}

func stringInstr(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("instr() requires exactly 2 arguments")
	}
	
	str := toString(args[0])
	substr := toString(args[1])
	
	index := strings.Index(str, substr)
	if index == -1 {
		return int64(0), nil
	}
	
	// SQL中位置从1开�?
	return int64(index + 1), nil
}

func stringASCII(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ascii() requires exactly 1 argument")
	}
	
	str := toString(args[0])
	if len(str) == 0 {
		return int64(0), nil
	}
	
	return int64(str[0]), nil
}

func stringSpace(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("space() requires exactly 1 argument")
	}
	
	count, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	
	if count <= 0 {
		return "", nil
	}
	
	return strings.Repeat(" ", int(count)), nil
}
