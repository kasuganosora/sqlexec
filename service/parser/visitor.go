package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

const (
	SetNames  = "SET NAMES"
	SetCharset = "SET CHARSET"
)

// SQLInfo 提取�?SQL 信息
type SQLInfo struct {
	Tables     []string // 涉及的表�?
	Columns    []string // 涉及的列�?
	Databases  []string // 涉及的数据库�?
	WhereExpr  ast.ExprNode // WHERE 条件表达�?
	LimitExpr  *ast.Limit   // LIMIT 表达�?
	OrderByItems []*ast.ByItem // ORDER BY 子句
	GroupByItems []*ast.ByItem    // GROUP BY 子句
	Having     ast.ExprNode      // HAVING 子句
	IsSelect   bool
	IsInsert   bool
	IsUpdate   bool
	IsDelete   bool
	IsDDL      bool
}

// SQLVisitor AST 访问器，用于提取 SQL 信息
type SQLVisitor struct {
	info *SQLInfo
}

// NewSQLVisitor 创建新的 SQL 访问�?
func NewSQLVisitor() *SQLVisitor {
	return &SQLVisitor{
		info: &SQLInfo{
			Tables:    make([]string, 0),
			Columns:   make([]string, 0),
			Databases: make([]string, 0),
		},
	}
}

// GetInfo 获取提取�?SQL 信息
func (v *SQLVisitor) GetInfo() *SQLInfo {
	return v.info
}

// Enter 进入节点
func (v *SQLVisitor) Enter(n ast.Node) (ast.Node, bool) {
	switch node := n.(type) {
	case *ast.TableName:
		// 提取表名
		v.info.Tables = append(v.info.Tables, node.Name.String())
	case *ast.ColumnName:
		// 提取列名
		if node.Name.String() != "" {
			v.info.Columns = append(v.info.Columns, node.Name.String())
		}
	case *ast.SelectStmt:
		v.info.IsSelect = true
	case *ast.InsertStmt:
		v.info.IsInsert = true
	case *ast.UpdateStmt:
		v.info.IsUpdate = true
	case *ast.DeleteStmt:
		v.info.IsDelete = true
	case *ast.CreateTableStmt, *ast.DropTableStmt, *ast.CreateDatabaseStmt, *ast.DropDatabaseStmt:
		v.info.IsDDL = true
	}
	return n, false
}

// Leave 离开节点
func (v *SQLVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

// ExtractSQLInfo 提取 SQL 信息
func ExtractSQLInfo(stmt ast.StmtNode) *SQLInfo {
	visitor := NewSQLVisitor()
	stmt.Accept(visitor)
	
	// 提取更多详细信息
	if selectStmt, ok := stmt.(*ast.SelectStmt); ok {
		visitor.info.WhereExpr = selectStmt.Where
		visitor.info.LimitExpr = selectStmt.Limit
		if selectStmt.OrderBy != nil {
			visitor.info.OrderByItems = selectStmt.OrderBy.Items
		}
		if selectStmt.GroupBy != nil {
			visitor.info.GroupByItems = selectStmt.GroupBy.Items
		}
		visitor.info.Having = nil // Having 需要特殊处�?
	}
	
	return visitor.GetInfo()
}

// ExtractTableNames 提取表名
func ExtractTableNames(stmt ast.StmtNode) []string {
	info := ExtractSQLInfo(stmt)
	return info.Tables
}

// ExtractColumnNames 提取列名
func ExtractColumnNames(stmt ast.StmtNode) []string {
	info := ExtractSQLInfo(stmt)
	return info.Columns
}

// ExtractDatabaseNames 提取数据库名
func ExtractDatabaseNames(stmt ast.StmtNode) []string {
	info := ExtractSQLInfo(stmt)
	return info.Databases
}

// TableVisitor 表名访问�?
type TableVisitor struct {
	tables []string
}

// NewTableVisitor 创建表名访问�?
func NewTableVisitor() *TableVisitor {
	return &TableVisitor{
		tables: make([]string, 0),
	}
}

// GetTables 获取表名列表
func (v *TableVisitor) GetTables() []string {
	return v.tables
}

// Enter 进入节点
func (v *TableVisitor) Enter(n ast.Node) (ast.Node, bool) {
	if table, ok := n.(*ast.TableName); ok {
		tableName := table.Name.String()
		if tableName != "" {
			v.tables = append(v.tables, tableName)
		}
	}
	return n, false
}

// Leave 离开节点
func (v *TableVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

// ColumnVisitor 列名访问�?
type ColumnVisitor struct {
	columns []string
}

// NewColumnVisitor 创建列名访问�?
func NewColumnVisitor() *ColumnVisitor {
	return &ColumnVisitor{
		columns: make([]string, 0),
	}
}

// GetColumns 获取列名列表
func (v *ColumnVisitor) GetColumns() []string {
	return v.columns
}

// Enter 进入节点
func (v *ColumnVisitor) Enter(n ast.Node) (ast.Node, bool) {
	if col, ok := n.(*ast.ColumnName); ok {
		colName := col.Name.String()
		if colName != "" {
			v.columns = append(v.columns, colName)
		}
	}
	return n, false
}

// Leave 离开节点
func (v *ColumnVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}
