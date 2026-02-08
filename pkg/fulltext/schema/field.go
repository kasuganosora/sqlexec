package schema

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// FieldType 字段类型
type FieldType string

const (
	FieldTypeText     FieldType = "text"
	FieldTypeNumeric  FieldType = "numeric"
	FieldTypeBoolean  FieldType = "boolean"
	FieldTypeDatetime FieldType = "datetime"
	FieldTypeJSON     FieldType = "json"
	FieldTypeArray    FieldType = "array"
	FieldTypeVector   FieldType = "vector"
)

// Field 字段定义
type Field struct {
	Name       string
	Type       FieldType
	Indexed    bool
	Stored     bool
	Fast       bool
	Boost      float64
	Tokenizer  string
	Analyzer   string
	RecordType string
	FieldNorms bool

	// 类型特定配置
	TextConfig    *TextFieldConfig
	NumericConfig *NumericFieldConfig
	JSONConfig    *JSONFieldConfig
	VectorConfig  *VectorFieldConfig
}

// TextFieldConfig 文本字段配置
type TextFieldConfig struct {
	MinTokenLen int
	MaxTokenLen int
	StopWords   []string
	Lowercase   bool
	RemoveHTML  bool
}

// NumericFieldConfig 数值字段配置
type NumericFieldConfig struct {
	Precision int
	MinValue  float64
	MaxValue  float64
}

// JSONFieldConfig JSON字段配置
type JSONFieldConfig struct {
	ExpandDots bool     // 展开点表示法，如 "metadata.color"
	Paths      []string // 要索引的路径
}

// VectorFieldConfig 向量字段配置
type VectorFieldConfig struct {
	Dimension int
	Metric    string // cosine, l2, inner_product
}

// NewTextField 创建文本字段
func NewTextField(name string) *Field {
	return &Field{
		Name:       name,
		Type:       FieldTypeText,
		Indexed:    true,
		Stored:     true,
		Fast:       false,
		Boost:      1.0,
		Tokenizer:  "standard",
		RecordType: "position",
		FieldNorms: true,
		TextConfig: &TextFieldConfig{
			MinTokenLen: 2,
			MaxTokenLen: 100,
			Lowercase:   true,
			RemoveHTML:  false,
		},
	}
}

// NewNumericField 创建数值字段
func NewNumericField(name string) *Field {
	return &Field{
		Name:       name,
		Type:       FieldTypeNumeric,
		Indexed:    true,
		Stored:     true,
		Fast:       true,
		Boost:      1.0,
		RecordType: "basic",
		NumericConfig: &NumericFieldConfig{
			Precision: -1, // 使用原始精度
		},
	}
}

// NewBooleanField 创建布尔字段
func NewBooleanField(name string) *Field {
	return &Field{
		Name:       name,
		Type:       FieldTypeBoolean,
		Indexed:    true,
		Stored:     true,
		Fast:       true,
		Boost:      1.0,
		RecordType: "basic",
	}
}

// NewDatetimeField 创建日期时间字段
func NewDatetimeField(name string) *Field {
	return &Field{
		Name:       name,
		Type:       FieldTypeDatetime,
		Indexed:    true,
		Stored:     true,
		Fast:       true,
		Boost:      1.0,
		RecordType: "basic",
	}
}

// NewJSONField 创建JSON字段
func NewJSONField(name string) *Field {
	return &Field{
		Name:       name,
		Type:       FieldTypeJSON,
		Indexed:    true,
		Stored:     true,
		Fast:       false,
		Boost:      1.0,
		Tokenizer:  "default",
		RecordType: "position",
		JSONConfig: &JSONFieldConfig{
			ExpandDots: true,
			Paths:      []string{},
		},
	}
}

// NewVectorField 创建向量字段
func NewVectorField(name string, dimension int) *Field {
	return &Field{
		Name:       name,
		Type:       FieldTypeVector,
		Indexed:    true,
		Stored:     true,
		Fast:       true,
		Boost:      1.0,
		RecordType: "basic",
		VectorConfig: &VectorFieldConfig{
			Dimension: dimension,
			Metric:    "cosine",
		},
	}
}

// Schema 索引Schema
type Schema struct {
	Fields       []*Field
	FieldMap     map[string]*Field
	DefaultField string
}

// NewSchema 创建Schema
func NewSchema() *Schema {
	return &Schema{
		Fields:   make([]*Field, 0),
		FieldMap: make(map[string]*Field),
	}
}

// AddField 添加字段
func (s *Schema) AddField(field *Field) error {
	if _, exists := s.FieldMap[field.Name]; exists {
		return fmt.Errorf("field %s already exists", field.Name)
	}

	s.Fields = append(s.Fields, field)
	s.FieldMap[field.Name] = field

	return nil
}

// GetField 获取字段
func (s *Schema) GetField(name string) (*Field, bool) {
	field, ok := s.FieldMap[name]
	return field, ok
}

// SetDefaultField 设置默认字段
func (s *Schema) SetDefaultField(name string) error {
	if _, ok := s.FieldMap[name]; !ok {
		return fmt.Errorf("field %s not found", name)
	}
	s.DefaultField = name
	return nil
}

// Document 文档
type Document struct {
	ID     int64
	Fields map[string]FieldValue
}

// FieldValue 字段值
type FieldValue struct {
	Type  FieldType
	Value interface{}
}

// NewDocument 创建文档
func NewDocument(id int64) *Document {
	return &Document{
		ID:     id,
		Fields: make(map[string]FieldValue),
	}
}

// AddText 添加文本值
func (d *Document) AddText(field, value string) {
	d.Fields[field] = FieldValue{
		Type:  FieldTypeText,
		Value: value,
	}
}

// AddNumeric 添加数值
func (d *Document) AddNumeric(field string, value float64) {
	d.Fields[field] = FieldValue{
		Type:  FieldTypeNumeric,
		Value: value,
	}
}

// AddBoolean 添加布尔值
func (d *Document) AddBoolean(field string, value bool) {
	d.Fields[field] = FieldValue{
		Type:  FieldTypeBoolean,
		Value: value,
	}
}

// AddDatetime 添加日期时间
func (d *Document) AddDatetime(field string, value time.Time) {
	d.Fields[field] = FieldValue{
		Type:  FieldTypeDatetime,
		Value: value,
	}
}

// AddJSON 添加JSON
func (d *Document) AddJSON(field string, value map[string]interface{}) {
	d.Fields[field] = FieldValue{
		Type:  FieldTypeJSON,
		Value: value,
	}
}

// AddVector 添加向量
func (d *Document) AddVector(field string, value []float32) {
	d.Fields[field] = FieldValue{
		Type:  FieldTypeVector,
		Value: value,
	}
}

// GetValue 获取字段值
func (d *Document) GetValue(field string) (FieldValue, bool) {
	v, ok := d.Fields[field]
	return v, ok
}

// GetText 获取文本值
func (d *Document) GetText(field string) (string, bool) {
	v, ok := d.Fields[field]
	if !ok || v.Type != FieldTypeText {
		return "", false
	}
	return v.Value.(string), true
}

// GetNumeric 获取数值
func (d *Document) GetNumeric(field string) (float64, bool) {
	v, ok := d.Fields[field]
	if !ok || v.Type != FieldTypeNumeric {
		return 0, false
	}
	switch val := v.Value.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

// GetBoolean 获取布尔值
func (d *Document) GetBoolean(field string) (bool, bool) {
	v, ok := d.Fields[field]
	if !ok || v.Type != FieldTypeBoolean {
		return false, false
	}
	return v.Value.(bool), true
}

// GetDatetime 获取日期时间
func (d *Document) GetDatetime(field string) (time.Time, bool) {
	v, ok := d.Fields[field]
	if !ok || v.Type != FieldTypeDatetime {
		return time.Time{}, false
	}
	return v.Value.(time.Time), true
}

// GetJSON 获取JSON
func (d *Document) GetJSON(field string) (map[string]interface{}, bool) {
	v, ok := d.Fields[field]
	if !ok || v.Type != FieldTypeJSON {
		return nil, false
	}
	return v.Value.(map[string]interface{}), true
}

// GetVector 获取向量
func (d *Document) GetVector(field string) ([]float32, bool) {
	v, ok := d.Fields[field]
	if !ok || v.Type != FieldTypeVector {
		return nil, false
	}
	return v.Value.([]float32), true
}

// ValueToString 将字段值转换为字符串
func (fv FieldValue) ValueToString() string {
	switch fv.Type {
	case FieldTypeText:
		return fv.Value.(string)
	case FieldTypeNumeric:
		switch v := fv.Value.(type) {
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		case int:
			return strconv.Itoa(v)
		case int64:
			return strconv.FormatInt(v, 10)
		}
	case FieldTypeBoolean:
		return strconv.FormatBool(fv.Value.(bool))
	case FieldTypeDatetime:
		return fv.Value.(time.Time).Format(time.RFC3339)
	case FieldTypeJSON:
		b, _ := json.Marshal(fv.Value)
		return string(b)
	}
	return ""
}

// ParseValue 解析字符串为字段值
func ParseValue(fieldType FieldType, value string) (FieldValue, error) {
	switch fieldType {
	case FieldTypeText:
		return FieldValue{Type: fieldType, Value: value}, nil

	case FieldTypeNumeric:
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return FieldValue{Type: fieldType, Value: f}, nil
		} else {
			return FieldValue{}, err
		}

	case FieldTypeBoolean:
		if b, err := strconv.ParseBool(value); err == nil {
			return FieldValue{Type: fieldType, Value: b}, nil
		} else {
			return FieldValue{}, err
		}

	case FieldTypeDatetime:
		if t, err := time.Parse(time.RFC3339, value); err == nil {
			return FieldValue{Type: fieldType, Value: t}, nil
		} else {
			return FieldValue{}, err
		}

	case FieldTypeJSON:
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(value), &m); err == nil {
			return FieldValue{Type: fieldType, Value: m}, nil
		} else {
			return FieldValue{}, err
		}

	default:
		return FieldValue{}, fmt.Errorf("unsupported field type: %s", fieldType)
	}
}

// FieldSchemaBuilder Schema构建器
type FieldSchemaBuilder struct {
	field *Field
}

// NewFieldSchemaBuilder 创建字段Schema构建器
func NewFieldSchemaBuilder(name string, fieldType FieldType) *FieldSchemaBuilder {
	var field *Field

	switch fieldType {
	case FieldTypeText:
		field = NewTextField(name)
	case FieldTypeNumeric:
		field = NewNumericField(name)
	case FieldTypeBoolean:
		field = NewBooleanField(name)
	case FieldTypeDatetime:
		field = NewDatetimeField(name)
	case FieldTypeJSON:
		field = NewJSONField(name)
	default:
		field = &Field{Name: name, Type: fieldType}
	}

	return &FieldSchemaBuilder{field: field}
}

// Indexed 设置是否索引
func (b *FieldSchemaBuilder) Indexed(indexed bool) *FieldSchemaBuilder {
	b.field.Indexed = indexed
	return b
}

// Stored 设置是否存储
func (b *FieldSchemaBuilder) Stored(stored bool) *FieldSchemaBuilder {
	b.field.Stored = stored
	return b
}

// Fast 设置Fast选项
func (b *FieldSchemaBuilder) Fast(fast bool) *FieldSchemaBuilder {
	b.field.Fast = fast
	return b
}

// Boost 设置权重
func (b *FieldSchemaBuilder) Boost(boost float64) *FieldSchemaBuilder {
	b.field.Boost = boost
	return b
}

// WithTokenizer 设置分词器
func (b *FieldSchemaBuilder) WithTokenizer(tokenizer string) *FieldSchemaBuilder {
	b.field.Tokenizer = tokenizer
	return b
}

// Build 构建字段
func (b *FieldSchemaBuilder) Build() *Field {
	return b.field
}

// SchemaBuilder Schema构建器
type SchemaBuilder struct {
	schema *Schema
}

// NewSchemaBuilder 创建Schema构建器
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		schema: NewSchema(),
	}
}

// AddField 添加字段
func (b *SchemaBuilder) AddField(field *Field) *SchemaBuilder {
	b.schema.AddField(field)
	return b
}

// AddTextField 添加文本字段
func (b *SchemaBuilder) AddTextField(name string, fn func(*FieldSchemaBuilder)) *SchemaBuilder {
	builder := NewFieldSchemaBuilder(name, FieldTypeText)
	if fn != nil {
		fn(builder)
	}
	b.schema.AddField(builder.Build())
	return b
}

// AddNumericField 添加数值字段
func (b *SchemaBuilder) AddNumericField(name string, fn func(*FieldSchemaBuilder)) *SchemaBuilder {
	builder := NewFieldSchemaBuilder(name, FieldTypeNumeric)
	if fn != nil {
		fn(builder)
	}
	b.schema.AddField(builder.Build())
	return b
}

// Build 构建Schema
func (b *SchemaBuilder) Build() *Schema {
	return b.schema
}
