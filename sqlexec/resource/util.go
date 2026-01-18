
package resource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// startsWith 检查字符串是否以指定前缀开头
func startsWith(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// endsWith 检查字符串是否以指定后缀结尾
func endsWith(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}

// containsSimple 简单字符串包含检查
func containsSimple(s, substr string) bool {
	return findSubstring(s, substr) != -1
}

// findSubstring 查找子串位置
func findSubstring(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// compareEqual 比较两个值是否相等
func compareEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}

	// 尝试数值比较
	if cmp, ok := compareNumeric(a, b); ok {
		return cmp == 0
	}

	// 尝试字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr == bStr
}

// compareNumeric 数值比较，返回 -1 (a<b), 0 (a==b), 1 (a>b) 和成功标志
func compareNumeric(a, b interface{}) (int, bool) {
	aFloat, okA := convertToFloat64(a)
	bFloat, okB := convertToFloat64(b)
	if !okA || !okB {
		return 0, false
	}

	if aFloat < bFloat {
		return -1, true
	} else if aFloat > bFloat {
		return 1, true
	}
	return 0, true
}

// compareGreater 比较a是否大于b
func compareGreater(a, b interface{}) bool {
	// 尝试数值比较
	if cmp, ok := compareNumeric(a, b); ok {
		return cmp > 0
	}
	// 降级到字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr > bStr
}

// compareLike 模糊匹配（支持 * 和 % 通配符）
func compareLike(a, b interface{}) bool {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	// 简化实现：只支持 * 通配符
	pattern := ""
	for _, ch := range bStr {
		if ch == '*' || ch == '%' {
			pattern += ".*"
		} else if ch == '_' {
			pattern += "."
		} else {
			pattern += string(ch)
		}
	}

	// 使用 contains 进行简化匹配
	return contains(aStr, bStr)
}

// compareIn 检查a是否在b的值列表中
func compareIn(a, b interface{}) bool {
	values, ok := b.([]interface{})
	if !ok {
		return false
	}

	for _, v := range values {
		if compareEqual(a, v) {
			return true
		}
	}
	return false
}

// compareBetween 检查值是否在范围内
func compareBetween(a, b interface{}) bool {
	// b 应该是一个包含两个元素的数组 [min, max]
	slice, ok := b.([]interface{})
	if !ok || len(slice) < 2 {
		return false
	}

	min := slice[0]
	max := slice[1]

	// 对于字符串，使用字符串比较
	aStr := fmt.Sprintf("%v", a)
	minStr := fmt.Sprintf("%v", min)
	maxStr := fmt.Sprintf("%v", max)

	// 对于数值，使用数值比较
	if cmp, ok := compareNumeric(a, min); ok && cmp >= 0 {
		if cmpMax, okMax := compareNumeric(a, max); okMax && cmpMax <= 0 {
			return true
		}
	}

	// 降级到字符串比较：min <= a <= max
	return (aStr >= minStr) && (aStr <= maxStr)
}

// convertToFloat64 将值转换为 float64 进行数值比较
func convertToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		// 尝试通过反射获取数值
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return float64(rv.Int()), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return float64(rv.Uint()), true
		case reflect.Float32, reflect.Float64:
			return rv.Float(), true
		}
		return 0, false
	}
}

// contains 字符串包含检查（支持通配符）
func contains(s, substr string) bool {
	// 简化实现：将 % 替换为 *
	substr = replaceAll(substr, "%", "*")

	if substr == "*" {
		return true
	}

	if len(substr) >= 2 && substr[0] == '*' && substr[len(substr)-1] == '*' {
		pattern := substr[1 : len(substr)-1]
		return containsSimple(s, pattern)
	}

	if len(substr) >= 1 && substr[0] == '*' {
		pattern := substr[1:]
		return endsWith(s, pattern)
	}

	if len(substr) >= 1 && substr[len(substr)-1] == '*' {
		pattern := substr[:len(substr)-1]
		return startsWith(s, pattern)
	}

	return s == substr
}

// replaceAll 替换字符串中所有出现的子串
func replaceAll(s, old, new string) string {
	return strings.ReplaceAll(s, old, new)
}

// containsTable 检查SQL是否包含表名
func containsTable(query, tableName string) bool {
	// 简化实现：检查表名是否在查询中
	return len(query) > 0 && len(tableName) > 0 &&
		(query == tableName || containsWord(query, tableName))
}

// containsWord 检查单词是否在字符串中（作为独立单词）
func containsWord(str, word string) bool {
	if len(str) == 0 || len(word) == 0 {
		return false
	}

	// 简化实现：查找是否包含空格+word或word+空格，或在开头/结尾
	wordLower := strings.ToLower(word)
	strLower := strings.ToLower(str)

	// 检查各种可能的位置
	patterns := []string{
		" " + wordLower + " ",
		" " + wordLower + ",",
		" " + wordLower + ";",
		" " + wordLower + ")",
		"(" + wordLower + " ",
		"," + wordLower + " ",
		" " + wordLower + "\n",
		"\n" + wordLower + " ",
	}

	// 检查开头
	if strings.HasPrefix(strLower, wordLower+" ") ||
	   strings.HasPrefix(strLower, wordLower+",") ||
	   strings.HasPrefix(strLower, wordLower+"(") {
		return true
	}

	// 检查结尾
	if strings.HasSuffix(strLower, " "+wordLower) ||
	   strings.HasSuffix(strLower, ","+wordLower) ||
	   strings.HasSuffix(strLower, ")"+wordLower) {
		return true
	}

	// 检查中间
	for _, pattern := range patterns {
		if strings.Contains(strLower, pattern) {
			return true
		}
	}

	return false
}

// splitLines 分割字节数据为行（去除空行）
func splitLines(data []byte) []string {
	str := string(data)
	lines := make([]string, 0)

	start := 0
	for i := 0; i < len(str); i++ {
		if str[i] == '\n' {
			line := strings.TrimSpace(str[start:i])
			if line != "" {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}

	// 添加最后一行
	if start < len(str) {
		line := strings.TrimSpace(str[start:])
		if line != "" {
			lines = append(lines, line)
		}
	}

	return lines
}

// joinWith 用指定分隔符连接字符串数组
func joinWith(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	return strings.Join(strs, sep)
}

// ApplyPagination 应用分页（通用实现）
func ApplyPagination(rows []Row, offset, limit int) []Row {
	if limit <= 0 {
		return rows
	}

	start := offset
	if start < 0 {
		start = 0
	}
	if start >= len(rows) {
		return []Row{}
	}

	end := start + limit
	if end > len(rows) {
		end = len(rows)
	}

	result := make([]Row, 0, limit)
	for i := start; i < end; i++ {
		result = append(result, rows[i])
	}
	return result
}