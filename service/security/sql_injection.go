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
		// 检测常见的SQL注入模式
		regexp.MustCompile(`(?i)(['";]+|(--+)|(/\*+)|(\*+/))`),
		// 检测UNION注入
		regexp.MustCompile(`(?i)\bunion\s+(all\s+)?select\b`),
		// 检测OR注入
		regexp.MustCompile(`(?i)\bor\b\s+["']?\d+["']?\s*(=|<|>)\s*["']?\d+["']?`),
		// 检测AND注入
		regexp.MustCompile(`(?i)\band\b\s+["']?\d+["']?\s*(=|<|>)\s*["']?\d+["']?`),
		// 检测注释注�?
		regexp.MustCompile(`(?i)(;|\-\-|\/\*|\*\/|#)`),
		// 检测EXEC注入
		regexp.MustCompile(`(?i)\bexec\s*\(|\bexecute\s*\(|\bsp_executesql\b`),
		// 检测XP_开头的存储过程（常见于SQL Server注入�?
		regexp.MustCompile(`(?i)\bxp_\w+\b`),
		// 检测延迟注�?
		regexp.MustCompile(`(?i)\bwaitfor\s+delay\b`),
		// 检测堆叠查�?
		regexp.MustCompile(`;\s*\w+`),
		// 检测十六进制编�?
		regexp.MustCompile(`(?i)0x[0-9a-f]+`),
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

	for _, pattern := range d.patterns {
		matches := pattern.FindAllStringIndex(sql, -1)
		if len(matches) > 0 {
			result.IsDetected = true
			for _, match := range matches {
				result.Details = append(result.Details, InjectionDetail{
					Pattern:  pattern.String(),
					Position: match[0],
					Length:   match[1] - match[0],
					Fragment: sql[match[0]:match[1]],
				})
			}
		}
	}

	return result
}

// DetectAndSanitize 检测并清理SQL注入
func (d *SQLInjectionDetector) DetectAndSanitize(sql string) (*InjectionResult, string) {
	result := d.Detect(sql)
	if !result.IsDetected {
		return result, sql
	}

	// 清理SQL字符�?
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

// ValidateString 验证字符串是否安�?
func (d *SQLInjectionDetector) ValidateString(str string) bool {
	result := d.Detect(str)
	return !result.IsDetected
}

// ValidateParameter 验证参数是否安全
func (d *SQLInjectionDetector) ValidateParameter(name, value string) bool {
	if name == "" {
		return false
	}

	// 检查参数名是否只包含字母数字和下划�?
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}

	// 检查参数值是否包含注�?
	return d.ValidateString(value)
}

// InjectionResult 注入检测结�?
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
