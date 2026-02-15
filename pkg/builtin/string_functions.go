package builtin

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/kasuganosora/sqlexec/pkg/utils"
)

func init() {
	// 注册字符串函数
	stringFunctions := []*FunctionInfo{
		{
			Name: "concat",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "concat", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: true},
			},
			Handler:     stringConcat,
			Description: "连接字符串",
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
			Description: "返回字符串长度（字节）",
			Example:     "LENGTH('hello') -> 5",
			Category:    "string",
		},
		{
			Name: "char_length",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "char_length", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringCharLength,
			Description: "返回字符串长度（字符）",
			Example:     "CHAR_LENGTH('hello') -> 5",
			Category:    "string",
		},
		{
			Name: "character_length",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "character_length", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringCharLength,
			Description: "返回字符串长度（字符）",
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
			Description: "转换为大写",
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
			Description: "转换为小写",
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
			Description: "返回左边的n个字符",
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
			Description: "返回右边的n个字符",
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
			Description: "替换字符串",
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
			Description: "重复字符串",
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
			Description: "反转字符串",
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
			Description: "返回字符的ASCII值",
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
			Description: "返回字符的ASCII值",
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
			Description: "返回指定数量的空格",
			Example:     "SPACE(5) -> '     '",
			Category:    "string",
		},
		{
			Name: "chr",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "chr", ReturnType: "string", ParamTypes: []string{"integer"}, Variadic: false},
			},
			Handler:     stringChr,
			Description: "Convert Unicode code point to character",
			Example:     "CHR(65) -> 'A'",
			Category:    "string",
		},
		{
			Name: "char",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "char", ReturnType: "string", ParamTypes: []string{"integer"}, Variadic: false},
			},
			Handler:     stringChr,
			Description: "Convert Unicode code point to character (alias for chr)",
			Example:     "CHAR(65) -> 'A'",
			Category:    "string",
		},
		{
			Name: "unicode",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "unicode", ReturnType: "integer", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringUnicode,
			Description: "Get Unicode code point of first character",
			Example:     "UNICODE('A') -> 65",
			Category:    "string",
		},
		{
			Name: "translate",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "translate", ReturnType: "string", ParamTypes: []string{"string", "string", "string"}, Variadic: false},
			},
			Handler:     stringTranslate,
			Description: "Character-by-character replacement mapping",
			Example:     "TRANSLATE('hello', 'el', 'ip') -> 'hippo'",
			Category:    "string",
		},
		{
			Name: "starts_with",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "starts_with", ReturnType: "boolean", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     stringStartsWith,
			Description: "Check if string starts with prefix",
			Example:     "STARTS_WITH('hello', 'he') -> true",
			Category:    "string",
		},
		{
			Name: "ends_with",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "ends_with", ReturnType: "boolean", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     stringEndsWith,
			Description: "Check if string ends with suffix",
			Example:     "ENDS_WITH('hello', 'lo') -> true",
			Category:    "string",
		},
		{
			Name: "contains",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "contains", ReturnType: "boolean", ParamTypes: []string{"string", "string"}, Variadic: false},
			},
			Handler:     stringContains,
			Description: "Check if string contains substring",
			Example:     "CONTAINS('hello world', 'world') -> true",
			Category:    "string",
		},
		{
			Name: "format",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "format", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: true},
			},
			Handler:     stringFormat,
			Description: "Format string with arguments (simplified sprintf)",
			Example:     "FORMAT('Hello %s, you are %d', 'world', 42) -> 'Hello world, you are 42'",
			Category:    "string",
		},
		{
			Name: "printf",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "printf", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: true},
			},
			Handler:     stringFormat,
			Description: "Format string with arguments (alias for format)",
			Example:     "PRINTF('Hello %s', 'world') -> 'Hello world'",
			Category:    "string",
		},
		{
			Name: "url_encode",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "url_encode", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringURLEncode,
			Description: "URL-encode a string",
			Example:     "URL_ENCODE('hello world') -> 'hello+world'",
			Category:    "string",
		},
		{
			Name: "url_decode",
			Type: FunctionTypeScalar,
			Signatures: []FunctionSignature{
				{Name: "url_decode", ReturnType: "string", ParamTypes: []string{"string"}, Variadic: false},
			},
			Handler:     stringURLDecode,
			Description: "URL-decode a string",
			Example:     "URL_DECODE('hello+world') -> 'hello world'",
			Category:    "string",
		},
	}

	for _, fn := range stringFunctions {
		RegisterGlobal(fn)
	}
}

// 辅助函数：将参数转换为字符串 (using utils package)
func toString(arg interface{}) string {
	return utils.ToString(arg)
}

// 辅助函数：将参数转换为整数 (using utils package)
func toInt64(arg interface{}) (int64, error) {
	return utils.ToInt64(arg)
}

// 字符串函数实现
func stringConcat(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", nil
	}
	
	var result strings.Builder
	for _, arg := range args {
		result.WriteString(toString(arg))
	}
	return result.String(), nil
}

func stringConcatWS(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", nil
	}
	
	separator := toString(args[0])
	if len(args) == 1 {
		return "", nil
	}
	
	var result strings.Builder
	result.WriteString(toString(args[1]))
	for i := 2; i < len(args); i++ {
		result.WriteString(separator)
		result.WriteString(toString(args[i]))
	}
	return result.String(), nil
}

func stringLength(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("length() requires exactly 1 argument")
	}
	return int64(len(toString(args[0]))), nil
}

func stringCharLength(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("char_length() requires exactly 1 argument")
	}
	return int64(utf8.RuneCountInString(toString(args[0]))), nil
}

func stringUpper(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("upper() requires exactly 1 argument")
	}
	return cases.Upper(language.Und).String(toString(args[0])), nil
}

func stringLower(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("lower() requires exactly 1 argument")
	}
	return cases.Lower(language.Und).String(toString(args[0])), nil
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
	
	// SQL中索引从1开始
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
	var padding strings.Builder
	for padding.Len() < paddingNeeded {
		padding.WriteString(padStr)
	}
	
	return padding.String()[:paddingNeeded] + str, nil
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
	var padding strings.Builder
	for padding.Len() < paddingNeeded {
		padding.WriteString(padStr)
	}
	
	return str + padding.String()[:paddingNeeded], nil
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
	
	// SQL中位置从1开始
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
	
	// SQL中位置从1开始
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

// chr / char: convert Unicode code point to character
func stringChr(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("chr() requires exactly 1 argument")
	}
	n, err := toInt64(args[0])
	if err != nil {
		return nil, err
	}
	return string(rune(n)), nil
}

// unicode: get Unicode code point of first character
func stringUnicode(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("unicode() requires exactly 1 argument")
	}
	s := toString(args[0])
	if len(s) == 0 {
		return int64(0), nil
	}
	runes := []rune(s)
	return int64(runes[0]), nil
}

// translate: character-by-character replacement mapping
func stringTranslate(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("translate() requires exactly 3 arguments")
	}
	s := toString(args[0])
	from := []rune(toString(args[1]))
	to := []rune(toString(args[2]))

	// Build replacement map
	mapping := make(map[rune]rune)
	for i, r := range from {
		if _, exists := mapping[r]; !exists {
			if i < len(to) {
				mapping[r] = to[i]
			} else {
				// If 'to' is shorter, map extra 'from' chars to -1 (delete)
				mapping[r] = -1
			}
		}
	}

	var result strings.Builder
	for _, r := range s {
		if repl, ok := mapping[r]; ok {
			if repl != -1 {
				result.WriteRune(repl)
			}
			// repl == -1 means delete the character
		} else {
			result.WriteRune(r)
		}
	}
	return result.String(), nil
}

// starts_with: check if string starts with prefix
func stringStartsWith(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("starts_with() requires exactly 2 arguments")
	}
	s := toString(args[0])
	prefix := toString(args[1])
	return strings.HasPrefix(s, prefix), nil
}

// ends_with: check if string ends with suffix
func stringEndsWith(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ends_with() requires exactly 2 arguments")
	}
	s := toString(args[0])
	suffix := toString(args[1])
	return strings.HasSuffix(s, suffix), nil
}

// contains: check if string contains substring
func stringContains(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("contains() requires exactly 2 arguments")
	}
	s := toString(args[0])
	sub := toString(args[1])
	return strings.Contains(s, sub), nil
}

// format / printf: simplified fmt.Sprintf support
func stringFormat(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("format() requires at least 1 argument")
	}
	format := toString(args[0])
	if len(args) == 1 {
		return format, nil
	}
	fmtArgs := make([]interface{}, len(args)-1)
	copy(fmtArgs, args[1:])
	return fmt.Sprintf(format, fmtArgs...), nil
}

// url_encode: URL-encode a string
func stringURLEncode(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("url_encode() requires exactly 1 argument")
	}
	return url.QueryEscape(toString(args[0])), nil
}

// url_decode: URL-decode a string
func stringURLDecode(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("url_decode() requires exactly 1 argument")
	}
	result, err := url.QueryUnescape(toString(args[0]))
	if err != nil {
		return nil, fmt.Errorf("url_decode: %w", err)
	}
	return result, nil
}
