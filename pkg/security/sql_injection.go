package security

import (
	"regexp"
	"strings"
	"unicode"
)

// SQLInjectionDetector SQL注入检测器
type SQLInjectionDetector struct {
	patterns []*regexp.Regexp
}

// NewSQLInjectionDetector 创建SQL注入检测器
func NewSQLInjectionDetector() *SQLInjectionDetector {
	patterns := []*regexp.Regexp{
		// 检测UNION注入（检测 UNION SELECT 模式）
		regexp.MustCompile(`(?i)\bunion\s+(all\s+)?select\b`),
		// 检测OR注入（检测数字比较，如 1 OR 1=1）
		regexp.MustCompile(`(?i)\s+or\s+["']?\d+["']?\s*(=|<|>)\s*["']?\d+["']?`),
		// 检测AND注入（检测数字比较，如 1 AND 1=1）
		regexp.MustCompile(`(?i)\s+and\s+["']?\d+["']?\s*(=|<|>)\s*["']?\d+["']?`),
		// 检测注释注入（末尾的注释或注释后的其他语句）
		regexp.MustCompile(`(?i)(--[^a-zA-Z0-9]|/\*[^*]*\*/)`),
		// 检测堆叠查询（分号后有SQL关键字）
		regexp.MustCompile(`;\s*(select|insert|update|delete|drop|alter|create|exec|execute|waitfor)\b`),
		// 检测EXEC注入
		regexp.MustCompile(`(?i)\bexec(ute)?\s*\(?(\s*(xp_\w+|sp_\w+))`),
		// 检测XP_开头的存储过程（常见于SQL Server注入）
		regexp.MustCompile(`(?i)\bxp_\w+\b`),
		// 检测十六进制编码（十六进制字符串，至少3个字符）
		regexp.MustCompile(`(?i)0x[0-9a-f]{3,}`),
	}

	return &SQLInjectionDetector{
		patterns: patterns,
	}
}

// Detect 检测SQL注入
func (d *SQLInjectionDetector) Detect(sql string) *InjectionResult {
	result := &InjectionResult{
		IsDetected: false,
		Details:    []InjectionDetail{},
	}

	// 检查是否是参数化查询（占位符）
	if d.isParameterizedQuery(sql) {
		return result
	}

	// 检查引号是否成对出现
	if d.hasUnmatchedQuotes(sql) {
		result.IsDetected = true
		result.Details = append(result.Details, InjectionDetail{
			Pattern:  "unmatched_quotes",
			Position: 0,
			Length:   len(sql),
			Fragment: sql,
		})
		return result
	}

	for _, pattern := range d.patterns {
		matches := pattern.FindAllStringIndex(sql, -1)
		if len(matches) > 0 {
			result.IsDetected = true
			for _, match := range matches {
				// 验证匹配是否在合法的上下文中
				if d.isSuspiciousPattern(sql, match[0], match[1]) {
					result.Details = append(result.Details, InjectionDetail{
						Pattern:  pattern.String(),
						Position: match[0],
						Length:   match[1] - match[0],
						Fragment: sql[match[0]:match[1]],
					})
				}
			}
		}
	}

	return result
}

// isParameterizedQuery 检查是否是参数化查询
func (d *SQLInjectionDetector) isParameterizedQuery(sql string) bool {
	// 检查是否包含参数占位符
	hasPlaceholder := regexp.MustCompile(`\?|@p\d+|:\d+`).MatchString(sql)
	if hasPlaceholder {
		return true
	}
	return false
}

// hasUnmatchedQuotes 检查是否有未匹配的引号
func (d *SQLInjectionDetector) hasUnmatchedQuotes(sql string) bool {
	singleQuoteCount := strings.Count(sql, "'")
	doubleQuoteCount := strings.Count(sql, `"`)
	
	// 引号数量为奇数表示未匹配
	if singleQuoteCount%2 != 0 || doubleQuoteCount%2 != 0 {
		return true
	}
	return false
}

// isSuspiciousPattern 检查模式是否出现在可疑的上下文中
func (d *SQLInjectionDetector) isSuspiciousPattern(sql string, start, end int) bool {
	// 如果模式出现在VALUES或SET子句中，可能是合法的
	lowerSQL := strings.ToLower(sql)
	before := strings.LastIndex(lowerSQL[:start], "values(")
	beforeSet := strings.LastIndex(lowerSQL[:start], "set ")
	
	if before > start-50 && before != -1 {
		return false
	}
	if beforeSet > start-50 && beforeSet != -1 {
		return false
	}
	
	// 检查模式后面是否有可疑的内容
	if end < len(sql)-5 {
		nextPart := strings.ToLower(sql[end:end+5])
		// 如果后面跟着数字比较（=1或!=1），可能是注入
		if strings.Contains(nextPart, "=1") || strings.Contains(nextPart, "= '1'") {
			return true
		}
	}
	
	return true
}

// DetectAndSanitize 检测并清理SQL注入
func (d *SQLInjectionDetector) DetectAndSanitize(sql string) (*InjectionResult, string) {
	result := d.Detect(sql)
	if !result.IsDetected {
		return result, sql
	}

	// 清理SQL字符串
	sanitized := d.sanitizeSQL(sql)
	return result, sanitized
}

// sanitizeSQL 清理SQL注入
func (d *SQLInjectionDetector) sanitizeSQL(sql string) string {
	// 移除危险字符
	dangerousChars := []string{"'", "\"", ";", "--", "/*", "*/", "xp_"}
	result := sql
	for _, char := range dangerousChars {
		result = strings.ReplaceAll(result, char, "")
	}
	return result
}

// SanitizeInput 清理用户输入
func (d *SQLInjectionDetector) SanitizeInput(input string) string {
	// 移除所有非字母数字和基本标点的字符
	var result strings.Builder
	for _, r := range input {
		if unicode.IsLetter(r) || unicode.IsDigit(r) ||
			r == '_' || r == '-' || r == '.' || r == '@' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// ValidateString 验证字符串是否安全
func (d *SQLInjectionDetector) ValidateString(str string) bool {
	result := d.Detect(str)
	return !result.IsDetected
}

// ValidateParameter 验证参数是否安全
func (d *SQLInjectionDetector) ValidateParameter(name, value string) bool {
	if name == "" {
		return false
	}

	// 检查参数名是否只包含字母数字和下划线
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}

	// 检查参数值是否包含注入
	return d.ValidateString(value)
}

// InjectionResult 注入检测结果
type InjectionResult struct {
	IsDetected bool
	Details    []InjectionDetail
}

// InjectionDetail 注入详细信息
type InjectionDetail struct {
	Pattern  string
	Position int
	Length   int
	Fragment string
}

// GetSeverity 获取注入严重程度
func (r *InjectionResult) GetSeverity() string {
	if !r.IsDetected {
		return "none"
	}

	if len(r.Details) > 5 {
		return "critical"
	}
	if len(r.Details) > 2 {
		return "high"
	}
	if len(r.Details) > 1 {
		return "medium"
	}
	return "low"
}
