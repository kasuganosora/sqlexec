package util

import (
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// StartsWith 检查字符串是否以指定前缀开头
func StartsWith(s, prefix string) bool {
	return utils.StartsWith(s, prefix)
}

// EndsWith 检查字符串是否以指定后缀结尾
func EndsWith(s, suffix string) bool {
	return utils.EndsWith(s, suffix)
}

// ContainsSimple 简单字符串包含检查
func ContainsSimple(s, substr string) bool {
	return utils.ContainsSimple(s, substr)
}

// FindSubstring 查找子串位置
func FindSubstring(s, substr string) int {
	return utils.FindSubstring(s, substr)
}

// Contains 字符串包含检查（支持通配符）
func Contains(s, substr string) bool {
	return utils.Contains(s, substr)
}

// ReplaceAll 替换字符串中所有出现的子串
func ReplaceAll(s, old, new string) string {
	return utils.ReplaceAll(s, old, new)
}

// ContainsTable 检查SQL是否包含表名
func ContainsTable(query, tableName string) bool {
	// 简化实现：检查表名是否在查询中
	return len(query) > 0 && len(tableName) > 0 &&
		(query == tableName || ContainsWord(query, tableName))
}

// ContainsWord 检查单词是否在字符串中（作为独立单词）
func ContainsWord(str, word string) bool {
	return utils.ContainsWord(str, word)
}

// JoinWith 用指定分隔符连接字符串数组
func JoinWith(strs []string, sep string) string {
	return utils.JoinWith(strs, sep)
}

// ToLowerCase 转小写
func ToLowerCase(s string) string {
	return utils.ToLowerCase(s)
}
