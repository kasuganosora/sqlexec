package parser

import (
	"fmt"
)

// SQLType SQL 语句类型
type SQLType string

const (
	SQLTypeSelect      SQLType = "SELECT"
	SQLTypeInsert      SQLType = "INSERT"
	SQLTypeUpdate      SQLType = "UPDATE"
	SQLTypeDelete      SQLType = "DELETE"
	SQLTypeCreate      SQLType = "CREATE"
	SQLTypeCreateView  SQLType = "CREATE VIEW"
	SQLTypeDrop        SQLType = "DROP"
	SQLTypeDropView    SQLType = "DROP VIEW"
	SQLTypeAlter       SQLType = "ALTER"
	// Note: TiDB does not support ALTER VIEW, so SQLTypeAlterView is deprecated
	// SQLTypeAlterView   SQLType = "ALTER VIEW"
	SQLTypeTruncate    SQLType = "TRUNCATE"
	SQLTypeShow        SQLType = "SHOW"
	SQLTypeDescribe    SQLType = "DESCRIBE"
	SQLTypeExplain     SQLType = "EXPLAIN"
	SQLTypeBegin       SQLType = "BEGIN"
	SQLTypeCommit      SQLType = "COMMIT"
	SQLTypeRollback    SQLType = "ROLLBACK"
	SQLTypeUse         SQLType = "USE"
	SQLTypeCreateUser  SQLType = "CREATE USER"
	SQLTypeDropUser    SQLType = "DROP USER"
	SQLTypeGrant       SQLType = "GRANT"
	SQLTypeRevoke      SQLType = "REVOKE"
	SQLTypeSetPasswd   SQLType = "SET PASSWORD"
	SQLTypeUnknown     SQLType = "UNKNOWN"
)

// 排序方向
const (
	SortAsc  = "ASC"
	SortDesc = "DESC"
)

// SQLStatement SQL 语句
type SQLStatement struct {
	Type         SQLType               `json:"type"`
	RawSQL       string                `json:"raw_sql"`
	Select       *SelectStatement       `json:"select,omitempty"`
	Insert       *InsertStatement       `json:"insert,omitempty"`
	Update       *UpdateStatement       `json:"update,omitempty"`
	Delete       *DeleteStatement       `json:"delete,omitempty"`
	Create       *CreateStatement       `json:"create,omitempty"`
	CreateView   *CreateViewStatement   `json:"create_view,omitempty"`
	Drop         *DropStatement         `json:"drop,omitempty"`
	DropView     *DropViewStatement     `json:"drop_view,omitempty"`
	Alter        *AlterStatement        `json:"alter,omitempty"`
	// Note: TiDB does not support ALTER VIEW, so AlterView field is deprecated
	// AlterView    *AlterViewStatement    `json:"alter_view,omitempty"`
	CreateIndex   *CreateIndexStatement  `json:"create_index,omitempty"`
	DropIndex    *DropIndexStatement    `json:"drop_index,omitempty"`
	Show         *ShowStatement        `json:"show,omitempty"`
	Describe     *DescribeStatement    `json:"describe,omitempty"`
	Explain      *ExplainStatement     `json:"explain,omitempty"`
	Begin        *TransactionStatement `json:"begin,omitempty"`
	Commit       *TransactionStatement `json:"commit,omitempty"`
	Rollback     *TransactionStatement `json:"rollback,omitempty"`
	Use          *UseStatement        `json:"use,omitempty"`
	CreateUser   *CreateUserStatement `json:"create_user,omitempty"`
	DropUser     *DropUserStatement   `json:"drop_user,omitempty"`
	Grant        *GrantStatement      `json:"grant,omitempty"`
	Revoke       *RevokeStatement     `json:"revoke,omitempty"`
	SetPassword  *SetPasswordStatement `json:"set_password,omitempty"`
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
	Hints      string          `json:"hints,omitempty"` // Raw hints string from SQL comment
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

// CreateIndexStatement CREATE INDEX 语句
type CreateIndexStatement struct {
	IndexName  string   `json:"index_name"`
	TableName  string   `json:"table_name"`
	ColumnName string   `json:"column_name"`
	IndexType  string   `json:"index_type"` // BTREE, HASH, FULLTEXT, VECTOR
	Unique     bool     `json:"unique"`
	IfExists   bool   `json:"if_exists"`
	
	// Vector Index 配置
	IsVectorIndex   bool              `json:"is_vector_index,omitempty"`
	VectorIndexType string            `json:"vector_index_type,omitempty"` // hnsw, ivf_flat, flat
	VectorMetric    string            `json:"vector_metric,omitempty"`     // cosine, l2, inner_product
	VectorDim       int               `json:"vector_dim,omitempty"`
	VectorParams    map[string]interface{} `json:"vector_params,omitempty"`
}

// DropIndexStatement DROP INDEX 语句
type DropIndexStatement struct {
	IndexName string `json:"index_name"`
	TableName string `json:"table_name"`
	IfExists  bool   `json:"if_exists"`
}

// ShowStatement SHOW 语句
type ShowStatement struct {
	Type   string `json:"type"` // TABLES, DATABASES, COLUMNS, PROCESSLIST, etc.
	Table  string `json:"table,omitempty"`
	Where string `json:"where,omitempty"`
	Like   string `json:"like,omitempty"`
	Full   bool   `json:"full,omitempty"` // SHOW FULL PROCESSLIST
}

// DescribeStatement DESCRIBE 语句
type DescribeStatement struct {
	Table  string `json:"table"`
	Column string `json:"column,omitempty"`
}

// ExplainStatement EXPLAIN 语句
type ExplainStatement struct {
	Query      *SelectStatement `json:"query,omitempty"`      // The query to explain
	TargetSQL  string          `json:"target_sql,omitempty"` // Raw SQL string
	Format     string          `json:"format,omitempty"`    // Format type (e.g., "TREE", "JSON")
	Analyze    bool            `json:"analyze,omitempty"` // EXPLAIN ANALYZE
}

// UseStatement USE 语句
type UseStatement struct {
	Database string `json:"database"` // 数据库名
}

// SelectColumn SELECT 列
type SelectColumn struct {
	Name      string `json:"name"`
	Alias     string `json:"alias,omitempty"`
	Table     string `json:"table,omitempty"`
	Expr      *Expression `json:"expr,omitempty"`
	IsWildcard bool       `json:"is_wildcard"` // 是否是 *
}

// JoinInfo JOIN 信息
type JoinInfo struct {
	Type      JoinType   `json:"type"`
	Table     string `json:"table"`
	Alias     string `json:"alias,omitempty"`
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

// Expression 表达式
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

// ExprType 表达式类型
type ExprType string

const (
	ExprTypeColumn    ExprType = "COLUMN"
	ExprTypeValue     ExprType = "VALUE"
	ExprTypeOperator  ExprType = "OPERATOR"
	ExprTypeFunction  ExprType = "FUNCTION"
	ExprTypeList      ExprType = "LIST"
)

// OrderByItem 排序项
type OrderByItem struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`              // ASC, DESC
	Collation string `json:"collation,omitempty"` // COLLATE clause (optional)
}

// ForeignKeyInfo 外键信息
type ForeignKeyInfo struct {
	RefTable    string `json:"ref_table"`
	RefColumn   string `json:"ref_column"`
	OnDelete    string `json:"on_delete,omitempty"`
	OnUpdate    string `json:"on_update,omitempty"`
}

// ColumnInfo 列信息（用于 DDL）
type ColumnInfo struct {
	Name         string           `json:"name"`
	Type         string           `json:"type"`
	Nullable     bool             `json:"nullable"`
	Primary      bool             `json:"primary"`
	Default      interface{}      `json:"default,omitempty"`
	Unique       bool             `json:"unique,omitempty"`
	AutoInc      bool             `json:"auto_increment,omitempty"`
	ForeignKey   *ForeignKeyInfo  `json:"foreign_key,omitempty"`
	Comment      string           `json:"comment,omitempty"`
	
	// Generated Columns 支持
	IsGenerated      bool     `json:"is_generated,omitempty"`    // 是否为生成列
	GeneratedType    string   `json:"generated_type,omitempty"`    // "STORED" (第一阶段) 或 "VIRTUAL" (第二阶段)
	GeneratedExpr    string   `json:"generated_expr,omitempty"`      // 表达式字符串
	GeneratedDepends []string `json:"generated_depends,omitempty"` // 依赖的列名
	
	// Vector Columns 支持
	VectorDim  int    `json:"vector_dim,omitempty"`   // 向量维度
	VectorType string `json:"vector_type,omitempty"`  // 向量类型（如 "float32"）
}

// IsVectorType 检查是否为向量类型
func (c ColumnInfo) IsVectorType() bool {
	return c.VectorDim > 0 || c.Type == "VECTOR"
}

// CreateUserStatement CREATE USER 语句
type CreateUserStatement struct {
	Username    string `json:"username"`
	Host        string `json:"host,omitempty"`        // Default is '%'
	Password    string `json:"password,omitempty"`    // IDENTIFIED BY
	IfNotExists bool   `json:"if_not_exists"`
}

// DropUserStatement DROP USER 语句
type DropUserStatement struct {
	Username  string `json:"username"`
	Host      string `json:"host,omitempty"`  // Default is '%'
	IfExists  bool   `json:"if_exists"`
}

// GrantStatement GRANT 语句
type GrantStatement struct {
	Privileges   []string           `json:"privileges"`
	On           string             `json:"on"`             // e.g., "db.*", "db.table"
	To           string             `json:"to"`             // e.g., "'user'@'host'"
	WithGrantOption bool               `json:"with_grant_option"`
}

// RevokeStatement REVOKE 语句
type RevokeStatement struct {
	Privileges []string `json:"privileges"`
	On         string   `json:"on"`   // e.g., "db.*", "db.table"
	From       string   `json:"from"` // e.g., "'user'@'host'"
}

// SetPasswordStatement SET PASSWORD 语句
type SetPasswordStatement struct {
	Username    string `json:"username"`
	Host        string `json:"host,omitempty"` // Default is '%'
	NewPassword string `json:"new_password"` // PASSWORD('password')
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

// CreateViewStatement CREATE VIEW 语句
type CreateViewStatement struct {
	OrReplace    bool               `json:"or_replace"`
	Algorithm    string             `json:"algorithm,omitempty"`    // UNDEFINED, MERGE, TEMPTABLE
	Definer      string             `json:"definer,omitempty"`      // 'user'@'host'
	Security     string             `json:"security,omitempty"`     // DEFINER, INVOKER
	Name         string             `json:"name"`
	ColumnList   []string           `json:"column_list,omitempty"`
	Select       *SelectStatement   `json:"select"`
	CheckOption  string             `json:"check_option,omitempty"` // NONE, CASCADED, LOCAL
}

// DropViewStatement DROP VIEW 语句
type DropViewStatement struct {
	Views     []string `json:"views"`      // 视图名称列表
	IfExists  bool     `json:"if_exists"`  // IF EXISTS
	Restrict  bool     `json:"restrict"`   // RESTRICT (TiDB 不支持，保留用于兼容性)
	Cascade   bool     `json:"cascade"`    // CASCADE (TiDB 不支持，保留用于兼容性)
}

// Note: TiDB does not support ALTER VIEW statement
// The following AlterViewStatement is kept for compatibility but not used
/*
// AlterViewStatement ALTER VIEW 语句
type AlterViewStatement struct {
	Algorithm    string             `json:"algorithm,omitempty"`    // UNDEFINED, MERGE, TEMPTABLE
	Definer      string             `json:"definer,omitempty"`      // 'user'@'host'
	Security     string             `json:"security,omitempty"`     // DEFINER, INVOKER
	Name         string             `json:"name"`
	ColumnList   []string           `json:"column_list,omitempty"`
	Select       *SelectStatement   `json:"select"`
	CheckOption  string             `json:"check_option,omitempty"` // NONE, CASCADED, LOCAL
}
*/


