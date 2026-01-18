
package resource

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// ==================== 字符串工具函数 ====================

// StartsWith 检查字符串是否以指定前缀开头
func StartsWith(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// EndsWith 检查字符串是否以指定后缀结尾
func EndsWith(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}

// ContainsSimple 简单字符串包含检查
func ContainsSimple(s, substr string) bool {
	return FindSubstring(s, substr) != -1
}

// FindSubstring 查找子串位置
func FindSubstring(s, substr string) int {
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

// Contains 字符串包含检查（支持通配符）
func Contains(s, substr string) bool {
	// 简化实现：将 % 替换为 *
	substr = ReplaceAll(substr, "%", "*")

	if substr == "*" {
		return true
	}

	if len(substr) >= 2 && substr[0] == '*' && substr[len(substr)-1] == '*' {
		pattern := substr[1 : len(substr)-1]
		return ContainsSimple(s, pattern)
	}

	if len(substr) >= 1 && substr[0] == '*' {
		pattern := substr[1:]
		return EndsWith(s, pattern)
	}

	if len(substr) >= 1 && substr[len(substr)-1] == '*' {
		pattern := substr[:len(substr)-1]
		return StartsWith(s, pattern)
	}

	return s == substr
}

// ReplaceAll 替换字符串中所有出现的子串
func ReplaceAll(s, old, new string) string {
	return strings.ReplaceAll(s, old, new)
}

// ContainsTable 检查SQL是否包含表名
func ContainsTable(query, tableName string) bool {
	// 简化实现：检查表名是否在查询中
	return len(query) > 0 && len(tableName) > 0 &&
		(query == tableName || ContainsWord(query, tableName))
}

// ContainsWord 检查单词是否在字符串中（作为独立单词）
func ContainsWord(str, word string) bool {
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

// SplitLines 分割字节数据为行（去除空行）
func SplitLines(data []byte) []string {
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

// JoinWith 用指定分隔符连接字符串数组
func JoinWith(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	return strings.Join(strs, sep)
}

// ==================== 比较工具函数 ====================

// CompareEqual 比较两个值是否相等
func CompareEqual(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == b
	}

	// 尝试数值比较
	if cmp, ok := CompareNumeric(a, b); ok {
		return cmp == 0
	}

	// 尝试字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr == bStr
}

// CompareNumeric 数值比较，返回 -1 (a<b), 0 (a==b), 1 (a>b) 和成功标志
func CompareNumeric(a, b interface{}) (int, bool) {
	aFloat, okA := ConvertToFloat64(a)
	bFloat, okB := ConvertToFloat64(b)
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

// CompareGreater 比较a是否大于b
func CompareGreater(a, b interface{}) bool {
	// 尝试数值比较
	if cmp, ok := CompareNumeric(a, b); ok {
		return cmp > 0
	}
	// 降级到字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr > bStr
}

// CompareLike 模糊匹配（支持 * 和 % 通配符）
func CompareLike(a, b interface{}) bool {
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
	return Contains(aStr, bStr)
}

// CompareIn 检查a是否在b的值列表中
func CompareIn(a, b interface{}) bool {
	values, ok := b.([]interface{})
	if !ok {
		return false
	}

	for _, v := range values {
		if CompareEqual(a, v) {
			return true
		}
	}
	return false
}

// CompareBetween 检查值是否在范围内
func CompareBetween(a, b interface{}) bool {
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
	if cmp, ok := CompareNumeric(a, min); ok && cmp >= 0 {
		if cmpMax, okMax := CompareNumeric(a, max); okMax && cmpMax <= 0 {
			return true
		}
	}

	// 降级到字符串比较：min <= a <= max
	return (aStr >= minStr) && (aStr <= maxStr)
}

// CompareValues 比较两个值（用于索引排序）
func CompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 尝试数值比较
	aFloat, aOk := ConvertToFloat64(a)
	bFloat, bOk := ConvertToFloat64(b)
	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// 字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// ConvertToFloat64 将值转换为 float64 进行数值比较
func ConvertToFloat64(v interface{}) (float64, bool) {
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

// ==================== 查询处理工具函数 ====================

// ApplyFilters 应用过滤器（通用实现）
func ApplyFilters(rows []Row, options *QueryOptions) []Row {
	if options == nil || len(options.Filters) == 0 {
		return rows
	}

	result := []Row{}
	for _, row := range rows {
		if MatchesFilters(row, options.Filters) {
			result = append(result, row)
		}
	}
	return result
}

// MatchesFilters 检查行是否匹配过滤器列表
func MatchesFilters(row Row, filters []Filter) bool {
	// 如果没有过滤器，所有行都匹配
	if len(filters) == 0 {
		return true
	}

	// 检查第一个过滤器
	filter := filters[0]

	// 如果有逻辑操作符
	if filter.LogicOp == "OR" || filter.LogicOp == "or" {
		// OR 操作：行的任何子过滤器匹配即可
		return MatchesAnySubFilter(row, filter.SubFilters)
	}

	// 如果有 AND 逻辑操作符
	if filter.LogicOp == "AND" || filter.LogicOp == "and" {
		// AND 操作：行的所有子过滤器都必须匹配
		return MatchesAllSubFilters(row, filter.SubFilters)
	}

	// 默认（AND 逻辑）：所有过滤器都必须匹配
	for _, f := range filters {
		if !MatchFilter(row, f) {
			return false
		}
	}
	return true
}

// MatchesAnySubFilter 检查行是否匹配任意子过滤器（OR 逻辑）
func MatchesAnySubFilter(row Row, subFilters []Filter) bool {
	// 如果没有子过滤器，返回 true
	if len(subFilters) == 0 {
		return true
	}
	// 检查是否有子过滤器匹配
	for _, subFilter := range subFilters {
		if MatchFilter(row, subFilter) {
			return true
		}
	}
	return false
}

// MatchesAllSubFilters 检查行是否匹配所有子过滤器（AND 逻辑）
func MatchesAllSubFilters(row Row, subFilters []Filter) bool {
	// 如果没有子过滤器，返回 true
	if len(subFilters) == 0 {
		return true
	}
	// 检查是否所有子过滤器都匹配
	for _, subFilter := range subFilters {
		if !MatchFilter(row, subFilter) {
			return false
		}
	}
	return true
}

// MatchFilter 匹配单个过滤器
func MatchFilter(row Row, filter Filter) bool {
	value, exists := row[filter.Field]
	if !exists {
		return false
	}

	switch filter.Operator {
	case "=":
		return CompareEqual(value, filter.Value)
	case "!=":
		return !CompareEqual(value, filter.Value)
	case ">":
		return CompareGreater(value, filter.Value)
	case "<":
		return !CompareGreater(value, filter.Value) && !CompareEqual(value, filter.Value)
	case ">=":
		return CompareGreater(value, filter.Value) || CompareEqual(value, filter.Value)
	case "<=":
		return !CompareGreater(value, filter.Value)
	case "LIKE":
		return CompareLike(value, filter.Value)
	case "NOT LIKE":
		return !CompareLike(value, filter.Value)
	case "IN":
		return CompareIn(value, filter.Value)
	case "NOT IN":
		return !CompareIn(value, filter.Value)
	case "BETWEEN":
		return CompareBetween(value, filter.Value)
	case "NOT BETWEEN":
		return !CompareBetween(value, filter.Value)
	default:
		return false
	}
}

// ApplyOrder 应用排序
func ApplyOrder(rows []Row, options *QueryOptions) []Row {
	if options == nil || options.OrderBy == "" {
		return rows
	}

	result := make([]Row, len(rows))
	copy(result, rows)

	// 获取排序列
	column := options.OrderBy

	// 获取排序方向
	order := options.Order
	if order == "" {
		order = "ASC"
	}

	// 排序
	sort.Slice(result, func(i, j int) bool {
		valI, existsI := result[i][column]
		valJ, existsJ := result[j][column]

		if !existsI && !existsJ {
			return false
		}
		if !existsI {
			return order != "ASC"
		}
		if !existsJ {
			return order == "ASC"
		}

		cmp := CompareValues(valI, valJ)
		if order == "DESC" {
			return cmp > 0
		}
		return cmp < 0
	})

	return result
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

// ==================== SQL构建工具函数 ====================

// JoinConditions 连接多个条件
func JoinConditions(conditions []string, logicalOp string) string {
	if logicalOp == "" || logicalOp == "AND" {
		return JoinWith(conditions, " AND ")
	} else if logicalOp == "OR" {
		return JoinWith(conditions, " OR ")
	}
	return JoinWith(conditions, " AND ")
}
