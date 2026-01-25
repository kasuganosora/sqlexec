package util

import "strings"

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

// JoinWith 用指定分隔符连接字符串数组
func JoinWith(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	return strings.Join(strs, sep)
}
