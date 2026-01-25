package file

import (
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ==================== Schema 推断器实现 ====================

// DefaultSchemaInferor 默认 Schema 推断器
type DefaultSchemaInferor struct {
	sampleSize int
}

// NewDefaultSchemaInferor 创建默认 Schema 推断器
func NewDefaultSchemaInferor() *DefaultSchemaInferor {
	return &DefaultSchemaInferor{
		sampleSize: 1000,
	}
}

// InferSchema 推断表结构
func (s *DefaultSchemaInferor) InferSchema(headers []string, samples [][]string) ([]domain.ColumnInfo, error) {
	columns := make([]domain.ColumnInfo, len(headers))

	for i, header := range headers {
		colType := s.inferColumnType(i, samples)
		columns[i] = domain.ColumnInfo{
			Name:     strings.TrimSpace(header),
			Type:     colType,
			Nullable: true,
			Primary:  false,
		}
	}

	return columns, nil
}

// inferColumnType 推断列类型
func (s *DefaultSchemaInferor) inferColumnType(colIndex int, samples [][]string) string {
	if len(samples) == 0 {
		return "VARCHAR"
	}

	typeCounts := map[string]int{
		"INTEGER": 0,
		"FLOAT":   0,
		"BOOLEAN": 0,
		"VARCHAR": 0,
	}

	for _, row := range samples {
		if colIndex >= len(row) {
			continue
		}
		value := strings.TrimSpace(row[colIndex])
		if value == "" {
			continue
		}

		colType := s.detectType(value)
		typeCounts[colType]++
	}

	// 选择最常见的类型
	maxCount := 0
	bestType := "VARCHAR"
	for t, count := range typeCounts {
		if count > maxCount {
			maxCount = count
			bestType = t
		}
	}

	return bestType
}

// detectType 检测值的类型
func (s *DefaultSchemaInferor) detectType(value string) string {
	// 尝试解析为布尔值
	if strings.EqualFold(value, "true") || strings.EqualFold(value, "false") {
		return "BOOLEAN"
	}

	// 尝试解析为整数
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "INTEGER"
	}

	// 尝试解析为浮点数
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "FLOAT"
	}

	return "VARCHAR"
}
