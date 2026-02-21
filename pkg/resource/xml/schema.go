package xml

import (
	"sort"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// inferColumnTypes 从行数据推断列类型
// 所有 XML 值最初都是字符串，需要尝试解析为更具体的类型
func inferColumnTypes(rows []domain.Row) []domain.ColumnInfo {
	if len(rows) == 0 {
		return []domain.ColumnInfo{
			{Name: "_file", Type: "string", Nullable: false, Primary: true},
		}
	}

	// 收集所有字段名和对应的值
	fieldsMap := make(map[string][]interface{})
	for _, row := range rows {
		for key, value := range row {
			if key == "__atts__" {
				continue
			}
			fieldsMap[key] = append(fieldsMap[key], value)
		}
	}

	// 排序字段名以保证确定性的列顺序
	fieldNames := make([]string, 0, len(fieldsMap))
	for field := range fieldsMap {
		if field == "_file" || field == "_index" {
			continue // 这些列单独处理
		}
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)

	// 构建列信息，_file 列始终在第一位
	columns := []domain.ColumnInfo{
		{Name: "_file", Type: "string", Nullable: false, Primary: true},
	}

	// 如果有 _index 列（列表展开模式），放在第二位
	if _, hasIndex := fieldsMap["_index"]; hasIndex {
		columns = append(columns, domain.ColumnInfo{
			Name:     "_index",
			Type:     "int64",
			Nullable: false,
		})
	}

	// 推断每列的类型
	for _, field := range fieldNames {
		values := fieldsMap[field]
		colType := inferFieldType(values)
		columns = append(columns, domain.ColumnInfo{
			Name:     field,
			Type:     colType,
			Nullable: true,
		})
	}

	return columns
}

// inferFieldType 推断字段类型
// 优先级：int64 > float64 > bool > string
func inferFieldType(values []interface{}) string {
	if len(values) == 0 {
		return "string"
	}

	typeCounts := map[string]int{
		"int64":   0,
		"float64": 0,
		"bool":    0,
		"string":  0,
	}

	for _, value := range values {
		if value == nil {
			continue
		}

		switch v := value.(type) {
		case string:
			t := detectStringType(v)
			typeCounts[t]++
		case int64:
			typeCounts["int64"]++
		case float64:
			typeCounts["float64"]++
		case bool:
			typeCounts["bool"]++
		default:
			typeCounts["string"]++
		}
	}

	// 选择最常见的类型，固定优先级打破平局
	typePriority := []string{"int64", "float64", "bool", "string"}
	maxCount := 0
	bestType := "string"
	for _, t := range typePriority {
		count := typeCounts[t]
		if count > maxCount {
			maxCount = count
			bestType = t
		}
	}

	return bestType
}

// detectStringType 检测字符串值的实际类型
func detectStringType(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "string"
	}

	// 检查布尔值
	lower := strings.ToLower(value)
	if lower == "true" || lower == "false" {
		return "bool"
	}

	// 检查整数
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "int64"
	}

	// 检查浮点数
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "float64"
	}

	// 默认字符串（包括日期、JSON 等复杂字符串）
	return "string"
}
