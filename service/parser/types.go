package parser

import (
	"fmt"
)

// SQLType SQL 语句类型
type SQLType string

const (
	SQLTypeSelect    SQLType = "SELECT"
	SQLTypeInsert    SQLType = "INSERT"
	SQLTypeUpdate    SQLType = "UPDATE"
	SQLTypeDelete    SQLType = "DELETE"
	SQLTypeCreate    SQLType = "CREATE"
	SQLTypeDrop      SQLType = "DROP"
	SQLTypeAlter     SQLType = "ALTER"
	SQLTypeTruncate  SQLType = "TRUNCATE"
	SQLTypeBegin    SQLType = "BEGIN"
	SQLTypeCommit   SQLType = "COMMIT"
	SQLTypeRollback SQLType = "ROLLBACK"
	SQLTypeUnknown   SQLType = "UNKNOWN"
	
	// 排序方向
	SortAsc  = "ASC"
	SortDesc = "DESC"
)

// SQLStatement SQL 语句
type SQLStatement struct {
	Type      SQLType             `json:"type"`
	RawSQL    string              `json:"raw_sql"`
	Select    *SelectStatement    `json:"select,omitempty"`
	Insert    *InsertStatement    `json:"insert,omitempty"`
	Update    *UpdateStatement    `json:"update,omitempty"`
	Delete    *DeleteStatement    `json:"delete,omitempty"`
	Create    *CreateStatement    `json:"create,omitempty"`
	Drop      *DropStatement      `json:"drop,omitempty"`
	Alter     *AlterStatement     `json:"alter,omitempty"`
	Begin     *TransactionStatement `json:"begin,omitempty"`
	Commit    *TransactionStatement `json:"commit,omitempty"`
	Rollback  *TransactionStatement `json:"rollback,omitempty"`
}

// SelectStatement SELECT 语句
type SelectStatement struct {
	Distinct   bool            `json:"distinct"`
	Columns    []SelectColumn  `json:"columns"`
	From       string          `json:"from"`
	Joins      []JoinInfo      `json:"joins,omitempty"`
	Where      *Expression     `json:"where,omitempty"`
	GroupBy    []string        `json:"group_by,omitempty"`
	Having     *Expression     `json:"having,omitempty"`
	OrderBy    []OrderByItem   `json:"order_by,omitempty"`
	Limit      *int64          `json:"limit,omitempty"`
	Offset     *int64          `json:"offset,omitempty"`
}

// InsertStatement INSERT 语句
type InsertStatement struct {
	Table      string         `json:"table"`
	Columns    []string       `json:"columns,omitempty"`
	Values     [][]interface{} `json:"values"`
	OnDuplicate *UpdateStatement `json:"on_duplicate,omitempty"`
}

// UpdateStatement UPDATE 语句
type UpdateStatement struct {
	Table   string            `json:"table"`
	Set     map[string]interface{} `json:"set"`
	Where   *Expression       `json:"where,omitempty"`
	OrderBy []OrderByItem     `json:"order_by,omitempty"`
	Limit   *int64            `json:"limit,omitempty"`
}

// DeleteStatement DELETE 语句
type DeleteStatement struct {
	Table   string        `json:"table"`
	Where   *Expression   `json:"where,omitempty"`
	OrderBy []OrderByItem `json:"order_by,omitempty"`
	Limit   *int64        `json:"limit,omitempty"`
}

// CreateStatement CREATE 语句
type CreateStatement struct {
	Type      string           `json:"type"` // TABLE, DATABASE, INDEX, etc.
	Name      string           `json:"name"`
	Columns   []ColumnInfo     `json:"columns,omitempty"`
	Options   map[string]interface{} `json:"options,omitempty"`
}

// DropStatement DROP 语句
type DropStatement struct {
	Type      string `json:"type"` // TABLE, DATABASE, INDEX, etc.
	Name      string `json:"name"`
	IfExists  bool   `json:"if_exists"`
}

// AlterStatement ALTER 语句
type AlterStatement struct {
	Type      string            `json:"type"` // TABLE, etc.
	Name      string            `json:"name"`
	Actions   []AlterAction     `json:"actions,omitempty"`
}

// AlterAction ALTER 操作
type AlterAction struct {
	Type     string            `json:"type"` // ADD, DROP, MODIFY, CHANGE, etc.
	Column   *ColumnInfo       `json:"column,omitempty"`
	OldName  string            `json:"old_name,omitempty"`
	NewName  string            `json:"new_name,omitempty"`
}

// SelectColumn SELECT �?
type SelectColumn struct {
	Name      string      `json:"name"`
	Alias     string      `json:"alias,omitempty"`
	Table     string      `json:"table,omitempty"`
	Expr      *Expression `json:"expr,omitempty"`
	IsWildcard bool       `json:"is_wildcard"` // 是否�?*
}

// JoinInfo JOIN 信息
type JoinInfo struct {
	Type      JoinType   `json:"type"`
	Table     string     `json:"table"`
	Alias     string     `json:"alias,omitempty"`
	Condition *Expression `json:"condition,omitempty"`
}

// JoinType JOIN 类型
type JoinType string

const (
	JoinTypeInner  JoinType = "INNER"
	JoinTypeLeft   JoinType = "LEFT"
	JoinTypeRight  JoinType = "RIGHT"
	JoinTypeFull   JoinType = "FULL"
	JoinTypeCross  JoinType = "CROSS"
)

// TransactionStatement 事务语句
type TransactionStatement struct {
	Level string `json:"level,omitempty"` // 隔离级别：READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
}

// Expression 表达�?
type Expression struct {
	Type      ExprType         `json:"type"`
	Column    string           `json:"column,omitempty"`
	Value     interface{}      `json:"value,omitempty"`
	Operator  string           `json:"operator,omitempty"`
	Left      *Expression      `json:"left,omitempty"`
	Right     *Expression      `json:"right,omitempty"`
	Args      []Expression     `json:"args,omitempty"`
	Function  string           `json:"function,omitempty"`
}

// ExprType 表达式类�?
type ExprType string

const (
	ExprTypeColumn    ExprType = "COLUMN"
	ExprTypeValue     ExprType = "VALUE"
	ExprTypeOperator  ExprType = "OPERATOR"
	ExprTypeFunction  ExprType = "FUNCTION"
	ExprTypeList      ExprType = "LIST"
)

// OrderByItem 排序�?
type OrderByItem struct {
	Column    string `json:"column"`
	Direction string `json:"direction"` // ASC, DESC
}

// ColumnInfo 列信息（用于 DDL�?
type ColumnInfo struct {
	Name      string      `json:"name"`
	Type      string      `json:"type"`
	Nullable  bool        `json:"nullable"`
	Primary   bool        `json:"primary"`
	Default   interface{} `json:"default,omitempty"`
	AutoInc   bool        `json:"auto_increment"`
	Unique    bool        `json:"unique"`
	Comment   string      `json:"comment,omitempty"`
}

// ParseResult 解析结果
type ParseResult struct {
	Statement *SQLStatement `json:"statement"`
	Success   bool          `json:"success"`
	Error     string        `json:"error,omitempty"`
}

// ParserError 解析错误
type ParserError struct {
	SQL     string `json:"sql"`
	Message string `json:"message"`
	Pos     int    `json:"pos"`
}

func (e *ParserError) Error() string {
	return fmt.Sprintf("SQL parse error at position %d: %s", e.Pos, e.Message)
}
