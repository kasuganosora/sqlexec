package datasource

import "strconv"

// FieldType 字段类型
type FieldType string

const (
	TypeString  FieldType = "string"
	TypeInt     FieldType = "int"
	TypeFloat   FieldType = "float"
	TypeBoolean FieldType = "boolean"
	TypeDate    FieldType = "date"
)

// Config 配置
type Config struct {
	Tables map[string]*TableConfig
}

// TableConfig 表配置
type TableConfig struct {
	Name      string     `json:"name"`
	Type      string     `json:"type"` // api, json, csv, xlsx
	Fields    []Field    `json:"fields"`
	APIConfig *APIConfig `json:"api_config,omitempty"`
	FilePath  string     `json:"file_path,omitempty"`
	SheetName string     `json:"sheet_name,omitempty"` // 用于xlsx
	RowCount  int64      `json:"row_count"`
}

// Field 字段定义
type Field struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Nullable bool      `json:"nullable"`
}

// APIConfig API配置
type APIConfig struct {
	URL         string            `json:"url"`
	Method      string            `json:"method"`
	Headers     map[string]string `json:"headers"`
	QueryParams map[string]string `json:"query_params"`
	Fields      []Field           `json:"fields"`
}

// Row 表示一行数据
type Row map[string]interface{}

// OrderBy 排序条件
type OrderBy struct {
	Field     string
	Direction string // ASC 或 DESC
}

// QueryType 查询类型
type QueryType int

const (
	QueryTypeSelect QueryType = iota
	QueryTypeInsert
	QueryTypeUpdate
	QueryTypeDelete
	QueryTypeShow
	QueryTypeDescribe
)

// JoinType 连接类型
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
)

// Condition 查询条件
type Condition struct {
	Field    string
	Operator string
	Value    interface{}
	Subquery *Query // 子查询
}

// Join 连接信息
type Join struct {
	Type          JoinType
	Table         string
	Condition     string
	Subquery      *Query // 子查询
	SubqueryAlias string // 子查询别名
}

// Query 查询信息
type Query struct {
	Type          QueryType
	Table         string
	Fields        []string
	Joins         []Join
	Where         []Condition
	GroupBy       []string
	Having        []Condition
	OrderBy       []OrderBy
	Limit         int
	Offset        int
	Subquery      *Query // 子查询
	SubqueryAlias string // 子查询别名
	Union         *Query // UNION查询
	UnionType     string // UNION类型：ALL/DISTINCT
	Intersect     *Query // INTERSECT查询
	Except        *Query // EXCEPT查询
}

// toFloat64 将任意类型转换为 float64
func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 0
}
