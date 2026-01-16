package datasource

import (
	"fmt"
	"strconv"
	"time"
)

// Converter 数据转换器
type Converter struct {
	fields []Field
}

// NewConverter 创建数据转换器
func NewConverter(fields []Field) *Converter {
	return &Converter{
		fields: fields,
	}
}

// Convert 将原始数据转换为MySQL协议格式
func (c *Converter) Convert(row Row) ([]interface{}, error) {
	result := make([]interface{}, len(c.fields))
	for i, field := range c.fields {
		value, err := c.convertValue(row[field.Name], field.Type)
		if err != nil {
			return nil, fmt.Errorf("转换字段 %s 失败: %v", field.Name, err)
		}
		result[i] = value
	}
	return result, nil
}

// convertValue 转换单个值
func (c *Converter) convertValue(value interface{}, fieldType FieldType) (interface{}, error) {
	// 如果值为nil，直接返回nil
	if value == nil {
		return nil, nil
	}

	// 如果值是字符串，尝试转换为目标类型
	if strValue, ok := value.(string); ok {
		switch fieldType {
		case TypeInt:
			return strconv.ParseInt(strValue, 10, 64)
		case TypeFloat:
			return strconv.ParseFloat(strValue, 64)
		case TypeBoolean:
			return strconv.ParseBool(strValue)
		case TypeDate:
			// 尝试多种日期格式
			formats := []string{
				"2006-01-02",
				"2006-01-02 15:04:05",
				time.RFC3339,
			}
			for _, format := range formats {
				if t, err := time.Parse(format, strValue); err == nil {
					return t, nil
				}
			}
			return nil, fmt.Errorf("无效的日期格式: %s", strValue)
		case TypeString:
			return strValue, nil
		default:
			return strValue, nil
		}
	}

	// 如果值已经是目标类型，直接返回
	switch fieldType {
	case TypeInt:
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		}
	case TypeFloat:
		switch v := value.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		}
	case TypeBoolean:
		if b, ok := value.(bool); ok {
			return b, nil
		}
	case TypeDate:
		if t, ok := value.(time.Time); ok {
			return t, nil
		}
	case TypeString:
		return fmt.Sprintf("%v", value), nil
	}

	return nil, fmt.Errorf("无法将值 %v 转换为类型 %s", value, fieldType)
}

// GetColumnTypes 获取列类型
func (c *Converter) GetColumnTypes() []string {
	types := make([]string, len(c.fields))
	for i, field := range c.fields {
		switch field.Type {
		case TypeInt:
			types[i] = "INT"
		case TypeFloat:
			types[i] = "DOUBLE"
		case TypeBoolean:
			types[i] = "BOOLEAN"
		case TypeDate:
			types[i] = "DATETIME"
		default:
			types[i] = "VARCHAR(255)"
		}
	}
	return types
}

// GetColumnNames 获取列名
func (c *Converter) GetColumnNames() []string {
	names := make([]string, len(c.fields))
	for i, field := range c.fields {
		names[i] = field.Name
	}
	return names
}
