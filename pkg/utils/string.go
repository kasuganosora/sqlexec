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

// Truncate truncates a string to the specified maximum length
// If the string is longer than maxLen, it returns the first maxLen characters with "..." appended
// If maxLen <= 3, returns the first maxLen characters without ellipsis
func Truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	// Handle Unicode properly by counting runes
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}

	if maxLen <= 3 {
		return string(runes[:maxLen])
	}

	return string(runes[:maxLen-3]) + "..."
}

// TruncateWithEllipsis truncates a string with custom ellipsis
func TruncateWithEllipsis(s string, maxLen int, ellipsis string) string {
	if len(s) <= maxLen {
		return s
	}

	runes := []rune(s)
	ellipsisRunes := []rune(ellipsis)

	if len(runes) <= maxLen {
		return s
	}

	truncateLen := maxLen - len(ellipsisRunes)
	if truncateLen <= 0 {
		return ellipsis
	}

	return string(runes[:truncateLen]) + ellipsis
}

// Reverse reverses a string
func Reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// PadLeft pads a string on the left with a specified character to reach the target length
func PadLeft(s string, padChar rune, targetLen int) string {
	runes := []rune(s)
	if len(runes) >= targetLen {
		return s
	}
	padding := make([]rune, targetLen-len(runes))
	for i := range padding {
		padding[i] = padChar
	}
	return string(padding) + s
}

// PadRight pads a string on the right with a specified character to reach the target length
func PadRight(s string, padChar rune, targetLen int) string {
	runes := []rune(s)
	if len(runes) >= targetLen {
		return s
	}
	padding := make([]rune, targetLen-len(runes))
	for i := range padding {
		padding[i] = padChar
	}
	return s + string(padding)
}

// IsEmpty checks if a string is empty
func IsEmpty(s string) bool {
	return s == ""
}

// IsBlank checks if a string is empty or contains only whitespace
func IsBlank(s string) bool {
	return strings.TrimSpace(s) == ""
}

// IsNotEmpty checks if a string is not empty
func IsNotEmpty(s string) bool {
	return s != ""
}

// IsNotBlank checks if a string is not empty and not just whitespace
func IsNotBlank(s string) bool {
	return strings.TrimSpace(s) != ""
}

// DefaultIfEmpty returns the default value if the string is empty
func DefaultIfEmpty(s, defaultValue string) string {
	if s == "" {
		return defaultValue
	}
	return s
}

// DefaultIfBlank returns the default value if the string is blank (empty or whitespace)
func DefaultIfBlank(s, defaultValue string) string {
	if strings.TrimSpace(s) == "" {
		return defaultValue
	}
	return s
}

// TrimSpace trims whitespace from both ends of a string
func TrimSpace(s string) string {
	return strings.TrimSpace(s)
}

// TrimPrefix removes a prefix from a string if present
func TrimPrefix(s, prefix string) string {
	return strings.TrimPrefix(s, prefix)
}

// TrimSuffix removes a suffix from a string if present
func TrimSuffix(s, suffix string) string {
	return strings.TrimSuffix(s, suffix)
}
