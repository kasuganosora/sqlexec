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

// FindSubstring finds substring position (returns character index for Unicode support)
func FindSubstring(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	// Use rune-based index for proper Unicode character position
	return IndexOfSubstring(s, substr)
}

// Contains checks if string contains substring (supports wildcards)
func Contains(s, substr string) bool {
	// Handle wildcard patterns
	if strings.Contains(substr, "%") {
		substr = ReplaceAll(substr, "%", "*")
	}

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

	// No wildcard - use simple contains
	return strings.Contains(s, substr)
}

// ReplaceAll replaces all occurrences of old with new in string s
func ReplaceAll(s, old, new string) string {
	// Handle empty old string - return original (Go's ReplaceAll inserts between chars)
	if old == "" {
		return s
	}
	return strings.ReplaceAll(s, old, new)
}

// ContainsWord checks if word exists in string as independent word
func ContainsWord(str, word string) bool {
	if len(str) == 0 || len(word) == 0 {
		return false
	}

	wordLower := strings.ToLower(word)
	strLower := strings.ToLower(str)

	// Word boundary characters
	separators := []string{" ", ",", ";", "(", ")", "\n", "\t", ".", "!", "?"}

	// Check at start
	for _, sep := range separators {
		if strings.HasPrefix(strLower, wordLower+sep) {
			return true
		}
	}
	// Check if entire string is the word
	if strLower == wordLower {
		return true
	}

	// Check at end
	for _, sep := range separators {
		if strings.HasSuffix(strLower, sep+wordLower) {
			return true
		}
	}

	// Check in middle (word surrounded by separators)
	for _, sep1 := range separators {
		for _, sep2 := range separators {
			if strings.Contains(strLower, sep1+wordLower+sep2) {
				return true
			}
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

// IndexOfSubstring 查找子串位置（按字符索引，支持Unicode）
func IndexOfSubstring(s, substr string) int {
	// 转换为rune切片以正确处理Unicode字符
	runes := []rune(s)
	subRunes := []rune(substr)
	
	for i := 0; i <= len(runes)-len(subRunes); i++ {
		match := true
		for j := 0; j < len(subRunes); j++ {
			if runes[i+j] != subRunes[j] {
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
