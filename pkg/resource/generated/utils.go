package generated

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IsGeneratedColumn 检查列是否为生成列
func IsGeneratedColumn(colName string, schema *domain.TableInfo) bool {
	for _, col := range schema.Columns {
		if col.Name == colName && col.IsGenerated {
			return true
		}
	}
	return false
}

// GetAffectedGeneratedColumns 获取受基础列更新影响的生成列（递归）
func GetAffectedGeneratedColumns(
	updatedCols []string,
	schema *domain.TableInfo,
) []string {
	if len(updatedCols) == 0 {
		return []string{}
	}

	// 构建依赖图的反向映射：列 -> 依赖此列的生成列
	reverseDeps := make(map[string][]string)
	for _, col := range schema.Columns {
		if col.IsGenerated && len(col.GeneratedDepends) > 0 {
			for _, dep := range col.GeneratedDepends {
				reverseDeps[dep] = append(reverseDeps[dep], col.Name)
			}
		}
	}

	// 使用 BFS 查找所有受影响的生成列
	affected := make(map[string]bool)
	queue := make([]string, 0, len(updatedCols))

	// 初始化队列
	for _, colName := range updatedCols {
		if !IsGeneratedColumn(colName, schema) {
			queue = append(queue, colName)
		}
	}

	// BFS 搜索
	for len(queue) > 0 {
		col := queue[0]
		queue = queue[1:]

		// 查找依赖此列的生成列
		if deps, ok := reverseDeps[col]; ok {
			for _, genCol := range deps {
				if !affected[genCol] {
					affected[genCol] = true
					queue = append(queue, genCol)
				}
			}
		}
	}

	// 转换为切片并按计算顺序排序
	result := make([]string, 0, len(affected))
	for colName := range affected {
		result = append(result, colName)
	}

	return result
}

// SetGeneratedColumnsToNULL 将生成列值设为 NULL
func SetGeneratedColumnsToNULL(
	row domain.Row,
	schema *domain.TableInfo,
) domain.Row {
	result := make(domain.Row)
	for k, v := range row {
		result[k] = v
	}

	for _, col := range schema.Columns {
		if col.IsGenerated {
			result[col.Name] = nil
		}
	}

	return result
}

// CastToType 将值转换为目标类型
func CastToType(value interface{}, targetType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// 规范化目标类型
	targetType = strings.ToUpper(targetType)

	switch targetType {
	case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return castToInt(value)
	case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
		return castToFloat(value)
	case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return castToString(value)
	case "BOOLEAN", "BOOL":
		return castToBool(value)
	case "DATE", "DATETIME", "TIMESTAMP", "TIME":
		// 简化处理，时间类型暂作为字符串返回
		return castToString(value)
	default:
		// 默认返回原值
		return value, nil
	}
}

// castToInt 转换为整数
func castToInt(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
		return nil, fmt.Errorf("cannot convert string '%s' to int", v)
	case bool:
		if v {
			return int64(1), nil
		} else {
			return int64(0), nil
		}
	default:
		return nil, fmt.Errorf("unsupported type for int conversion: %T", v)
	}
}

// castToFloat 转换为浮点数
func castToFloat(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	case string:
		if v == "" {
			return 0.0, nil
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to float", v)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("unsupported type for float conversion: %T", value)
	}
}

// castToString 转换为字符串
func castToString(value interface{}) (interface{}, error) {
	if value == nil {
		return "", nil
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%f", v), nil
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// castToBool 转换为布尔值
func castToBool(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		return v != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		return v != 0, nil
	case float32:
		return v != 0.0, nil
	case float64:
		return v != 0.0, nil
	case string:
		if v == "" || v == "0" || strings.ToLower(v) == "false" {
			return false, nil
		}
		return true, nil
	default:
		return false, nil
	}
}

// FilterGeneratedColumns 过滤出生成列（不允许显式插入/更新）
func FilterGeneratedColumns(
	row domain.Row,
	schema *domain.TableInfo,
) domain.Row {
	result := make(domain.Row)
	for k, v := range row {
		if !IsGeneratedColumn(k, schema) {
			result[k] = v
		}
	}
	return result
}

// GetGeneratedColumnValues 获取所有生成列的值
func GetGeneratedColumnValues(
	row domain.Row,
	schema *domain.TableInfo,
) domain.Row {
	result := make(domain.Row)
	for k, v := range row {
		if IsGeneratedColumn(k, schema) {
			result[k] = v
		}
	}
	return result
}

// RemoveGeneratedColumns 从行数据中移除生成列
func RemoveGeneratedColumns(
	row domain.Row,
	schema *domain.TableInfo,
) domain.Row {
	result := make(domain.Row)
	for k, v := range row {
		if !IsGeneratedColumn(k, schema) {
			result[k] = v
		}
	}
	return result
}
