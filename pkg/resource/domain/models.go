package domain

// DataSourceType 数据源类型
type DataSourceType string

// String 返回数据源类型的字符串表示
func (t DataSourceType) String() string {
	return string(t)
}

const (
	// DataSourceTypeMemory 内存数据源
	DataSourceTypeMemory DataSourceType = "memory"
	// DataSourceTypeMySQL MySQL数据源
	DataSourceTypeMySQL DataSourceType = "mysql"
	// DataSourceTypePostgreSQL PostgreSQL数据源
	DataSourceTypePostgreSQL DataSourceType = "postgresql"
	// DataSourceTypeSQLite SQLite数据源
	DataSourceTypeSQLite DataSourceType = "sqlite"
	// DataSourceTypeCSV CSV文件数据源
	DataSourceTypeCSV DataSourceType = "csv"
	// DataSourceTypeExcel Excel文件数据源
	DataSourceTypeExcel DataSourceType = "excel"
	// DataSourceTypeJSON JSON文件数据源
	DataSourceTypeJSON DataSourceType = "json"
	// DataSourceTypeParquet Parquet文件数据源
	DataSourceTypeParquet DataSourceType = "parquet"
)

// DataSourceConfig 数据源配置
type DataSourceConfig struct {
	Type     DataSourceType         `json:"type"`
	Name     string                 `json:"name"`
	Host     string                 `json:"host,omitempty"`
	Port     int                    `json:"port,omitempty"`
	Username string                 `json:"username,omitempty"`
	Password string                 `json:"password,omitempty"`
	Database string                 `json:"database,omitempty"`
	Writable  bool                   `json:"writable,omitempty"` // 是否可写，默认true
	Options  map[string]interface{} `json:"options,omitempty"`
}

// TableInfo 表信息
type TableInfo struct {
	Name       string                 `json:"name"`
	Schema     string                 `json:"schema,omitempty"`
	Columns    []ColumnInfo            `json:"columns"`
	Temporary  bool                   `json:"temporary,omitempty"` // 是否是临时表
	Atts       map[string]interface{} `json:"atts,omitempty"`       // 表属性
}

// ColumnInfo 列信息
type ColumnInfo struct {
	Name         string           `json:"name"`
	Type         string           `json:"type"`
	Nullable     bool             `json:"nullable"`
	Primary      bool             `json:"primary"`
	Default      string           `json:"default,omitempty"`
	Unique       bool             `json:"unique,omitempty"`          // 唯一约束
	AutoIncrement bool            `json:"auto_increment,omitempty"` // 自动递增
	ForeignKey   *ForeignKeyInfo  `json:"foreign_key,omitempty"`   // 外键约束
	
	// Generated Columns 支持
	IsGenerated      bool     `json:"is_generated,omitempty"`    // 是否为生成列
	GeneratedType    string   `json:"generated_type,omitempty"`    // "STORED" (第一阶段) 或 "VIRTUAL" (第二阶段)
	GeneratedExpr    string   `json:"generated_expr,omitempty"`     // 表达式字符串
	GeneratedDepends []string `json:"generated_depends,omitempty"` // 依赖的列名
}

// ForeignKeyInfo 外键信息
type ForeignKeyInfo struct {
	Table    string `json:"table"`              // 引用的表
	Column   string `json:"column"`             // 引用的列
	OnDelete string `json:"on_delete,omitempty"`  // 删除策略：CASCADE, SET NULL, NO ACTION
	OnUpdate string `json:"on_update,omitempty"`  // 更新策略
}

// Row 行数据
type Row map[string]interface{}

// 行属性内部键名，不对外暴露
const (
	rowAttsKey = "__atts__" // 行属性键名，保留字段，禁止用户访问
)

// SetRowAttributes 设置行属性（内部使用）
func SetRowAttributes(row Row, atts map[string]any) {
	row[rowAttsKey] = atts
}

// GetRowAttributes 获取行属性（内部使用）
func GetRowAttributes(row Row) (map[string]any, bool) {
	atts, ok := row[rowAttsKey].(map[string]any)
	return atts, ok
}

// QueryResult 查询结果
type QueryResult struct {
	Columns []ColumnInfo `json:"columns"`
	Rows    []Row        `json:"rows"`
	Total   int64        `json:"total"`
}

// Filter 查询过滤器
type Filter struct {
	Field      string      `json:"field"`
	Operator   string      `json:"operator"` // =, !=, >, <, >=, <=, LIKE, IN, BETWEEN
	Value      interface{} `json:"value"`
	LogicOp    string      `json:"logic_op,omitempty"`    // AND, OR (用于多条件组合)
	SubFilters []Filter    `json:"sub_filters,omitempty"` // 子过滤器（用于逻辑组合）
}

// QueryOptions 查询选项
type QueryOptions struct {
	Filters      []Filter `json:"filters,omitempty"`
	OrderBy      string   `json:"order_by,omitempty"`
	Order        string   `json:"order,omitempty"` // ASC, DESC
	Limit        int      `json:"limit,omitempty"`
	Offset       int      `json:"offset,omitempty"`
	SelectAll    bool     `json:"select_all,omitempty"`     // 是否是 select *
	SelectColumns []string `json:"select_columns,omitempty"` // 指定要查询的列（列裁剪）
	User         string   `json:"user,omitempty"`         // 当前用户名（用于权限检查）
}

// InsertOptions 插入选项
type InsertOptions struct {
	Replace bool `json:"replace,omitempty"` // 如果存在则替换
}

// UpdateOptions 更新选项
type UpdateOptions struct {
	Upsert bool `json:"upsert,omitempty"` // 如果不存在则插入
}

// DeleteOptions 删除选项
type DeleteOptions struct {
	Force bool `json:"force,omitempty"` // 强制删除
}

// TransactionOptions 事务选项
type TransactionOptions struct {
	IsolationLevel string `json:"isolation_level,omitempty"` // READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
	ReadOnly       bool   `json:"read_only,omitempty"`        // 只读事务
}

// ConstraintType 约束类型
type ConstraintType string

const (
	ConstraintTypeUnique     ConstraintType = "unique"
	ConstraintTypeForeignKey ConstraintType = "foreign_key"
	ConstraintTypeCheck      ConstraintType = "check"
	ConstraintTypePrimaryKey  ConstraintType = "primary_key"
)

// IndexType 索引类型
type IndexType string

const (
	IndexTypeBTree    IndexType = "btree"
	IndexTypeHash     IndexType = "hash"
	IndexTypeFullText IndexType = "fulltext"
)

// ForeignKeyReference 外键引用信息
type ForeignKeyReference struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
}

// Constraint 约束信息
type Constraint struct {
	Name       string                `json:"name"`
	Type       ConstraintType         `json:"type"`
	Columns    []string              `json:"columns"`
	Table      string                `json:"table"`
	References *ForeignKeyReference  `json:"references,omitempty"`
	CheckExpr  string                `json:"check_expr,omitempty"`
	Enabled    bool                  `json:"enabled"`
}

// Index 索引信息
type Index struct {
	Name     string      `json:"name"`
	Table    string      `json:"table"`
	Columns  []string    `json:"columns"`
	Type     IndexType   `json:"type"`
	Unique   bool        `json:"unique"`
	Primary  bool        `json:"primary"`
	Enabled  bool        `json:"enabled"`
}

// Schema 模式信息
type Schema struct {
	Name        string       `json:"name"`
	Tables      []*TableInfo `json:"tables"`
	Indexes     []*Index     `json:"indexes"`
	Constraints []*Constraint `json:"constraints"`
}

// ViewAlgorithm 视图算法类型
type ViewAlgorithm string

const (
	ViewAlgorithmUndefined ViewAlgorithm = "UNDEFINED"
	ViewAlgorithmMerge    ViewAlgorithm = "MERGE"
	ViewAlgorithmTempTable ViewAlgorithm = "TEMPTABLE"
)

// ViewSecurity 视图安全类型
type ViewSecurity string

const (
	ViewSecurityDefiner  ViewSecurity = "DEFINER"
	ViewSecurityInvoker ViewSecurity = "INVOKER"
)

// ViewCheckOption 视图检查选项
type ViewCheckOption string

const (
	ViewCheckOptionNone      ViewCheckOption = "NONE"
	ViewCheckOptionCascaded  ViewCheckOption = "CASCADED"
	ViewCheckOptionLocal     ViewCheckOption = "LOCAL"
)

// ViewInfo 视图元数据
type ViewInfo struct {
	Algorithm   ViewAlgorithm   `json:"algorithm"`    // MERGE, TEMPTABLE, UNDEFINED
	Definer     string          `json:"definer"`      // 'user'@'host'
	Security    ViewSecurity    `json:"security"`     // DEFINER, INVOKER
	SelectStmt  string          `json:"select_stmt"`  // 视图定义的 SELECT 语句
	CheckOption ViewCheckOption `json:"check_option"` // NONE, CASCADED, LOCAL
	Cols        []string        `json:"cols"`         // 视图列名列表
	Updatable   bool            `json:"updatable"`    // 是否可更新
	Charset     string          `json:"charset,omitempty"`     // 客户端字符集
	Collate     string          `json:"collate,omitempty"`     // 连接排序规则
}

// 视图相关常量
const (
	ViewMetaKey  = "__view__" // 视图元数据在 TableInfo.Atts 中的键名
	MaxViewDepth = 10         // 视图嵌套最大深度
)
