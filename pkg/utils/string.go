package utils

import "strings"

// StartsWith 检查字符串是否以指定前缀开头
func StartsWith(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

// EndsWith 检查字符串是否以指定后缀结尾
func EndsWith(s, suffix string) bool {
	return strings.HasSuffix(s, suffix)
}

// ContainsSimple 简单字符串包含检查
func ContainsSimple(s, substr string) bool {
	return strings.Contains(s, substr)
}

// FindSubstring 查找子串位置
func FindSubstring(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}
	return strings.Index(s, substr)
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

// ToLowerCase 转小写（兼容包装）
func ToLowerCase(s string) string {
	return strings.ToLower(s)
}

// ContainsSubstring 检查字符串是否包含子串
func ContainsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && IndexOfSubstring(s, substr) >= 0)
}

// IndexOfSubstring 查找子串位置
func IndexOfSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
