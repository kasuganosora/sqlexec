package parser

import (
	"fmt"
	"log"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// StmtHandler SQL 语句处理器接口
type StmtHandler interface {
	Handle(stmt ast.StmtNode) (result interface{}, err error)
}

// HandlerChain 处理器链
type HandlerChain struct {
	handlers map[string]StmtHandler
	defaultHandler StmtHandler
}

// NewHandlerChain 创建新的处理器链
func NewHandlerChain() *HandlerChain {
	return &HandlerChain{
		handlers: make(map[string]StmtHandler),
	}
}

// RegisterHandler 注册处理器
func (c *HandlerChain) RegisterHandler(stmtType string, handler StmtHandler) {
	c.handlers[stmtType] = handler
}

// SetDefaultHandler 设置默认处理器
func (c *HandlerChain) SetDefaultHandler(handler StmtHandler) {
	c.defaultHandler = handler
}

// Handle 处理 SQL 语句
func (c *HandlerChain) Handle(stmt ast.StmtNode) (interface{}, error) {
	if stmt == nil {
		return nil, fmt.Errorf("SQL 语句为空")
	}

	stmtType := GetStmtType(stmt)
	log.Printf("SQL 语句类型: %s", stmtType)

	// 查找对应的处理器
	handler, ok := c.handlers[stmtType]
	if !ok {
		// 使用默认处理器
		if c.defaultHandler != nil {
			return c.defaultHandler.Handle(stmt)
		}
		return nil, fmt.Errorf("不支持的 SQL 语句类型: %s", stmtType)
	}

	return handler.Handle(stmt)
}

// QueryHandler SELECT 查询处理器
type QueryHandler struct{}

// NewQueryHandler 创建查询处理器
func NewQueryHandler() *QueryHandler {
	return &QueryHandler{}
}

// Handle 处理 SELECT 语句
func (h *QueryHandler) Handle(stmt ast.StmtNode) (interface{}, error) {
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("不是 SELECT 语句")
	}

	log.Printf("处理 SELECT 语句")
	
	// 提取查询信息
	info := ExtractSQLInfo(stmt)
	log.Printf("涉及表: %v", info.Tables)
	log.Printf("涉及列: %v", info.Columns)

	// 返回查询结果
	return &QueryResult{
		Type:     "SELECT",
		Tables:   info.Tables,
		Columns:  info.Columns,
		Stmt:     selectStmt,
	}, nil
}

// DMLHandler DML 语句处理器（INSERT/UPDATE/DELETE）
type DMLHandler struct{}

// NewDMLHandler 创建 DML 处理器
func NewDMLHandler() *DMLHandler {
	return &DMLHandler{}
}

// Handle 处理 DML 语句
func (h *DMLHandler) Handle(stmt ast.StmtNode) (interface{}, error) {
	info := ExtractSQLInfo(stmt)

	var stmtType string
	switch stmt.(type) {
	case *ast.InsertStmt:
		stmtType = "INSERT"
	case *ast.UpdateStmt:
		stmtType = "UPDATE"
	case *ast.DeleteStmt:
		stmtType = "DELETE"
	default:
		stmtType = "UNKNOWN"
	}

	log.Printf("处理 %s 语句", stmtType)
	log.Printf("涉及表: %v", info.Tables)
	log.Printf("涉及列: %v", info.Columns)

	return &DMLResult{
		Type:    stmtType,
		Tables:  info.Tables,
		Columns: info.Columns,
		Affected: 1, // 默认影响行数
	}, nil
}

// DDLHandler DDL 语句处理器（CREATE/DROP/ALTER）
type DDLHandler struct{}

// NewDDLHandler 创建 DDL 处理器
func NewDDLHandler() *DDLHandler {
	return &DDLHandler{}
}

// Handle 处理 DDL 语句
func (h *DDLHandler) Handle(stmt ast.StmtNode) (interface{}, error) {
	info := ExtractSQLInfo(stmt)

	var stmtType string
	switch stmt.(type) {
	case *ast.CreateTableStmt:
		stmtType = "CREATE_TABLE"
	case *ast.DropTableStmt:
		stmtType = "DROP_TABLE"
	case *ast.CreateDatabaseStmt:
		stmtType = "CREATE_DATABASE"
	case *ast.DropDatabaseStmt:
		stmtType = "DROP_DATABASE"
	case *ast.AlterTableStmt:
		stmtType = "ALTER_TABLE"
	case *ast.TruncateTableStmt:
		stmtType = "TRUNCATE_TABLE"
	default:
		stmtType = "UNKNOWN"
	}

	log.Printf("处理 %s 语句", stmtType)
	log.Printf("涉及表: %v", info.Tables)
	log.Printf("涉及数据库: %v", info.Databases)

	return &DDLResult{
		Type:      stmtType,
		Tables:    info.Tables,
		Databases: info.Databases,
	}, nil
}

// SetHandler SET 语句处理器
type SetHandler struct{}

// NewSetHandler 创建 SET 处理器
func NewSetHandler() *SetHandler {
	return &SetHandler{}
}

// Handle 处理 SET 语句
func (h *SetHandler) Handle(stmt ast.StmtNode) (interface{}, error) {
	setStmt, ok := stmt.(*ast.SetStmt)
	if !ok {
		return nil, fmt.Errorf("不是 SET 语句")
	}

	log.Printf("处理 SET 语句，变量数量: %d", len(setStmt.Variables))

	vars := make(map[string]interface{})
	for _, variable := range setStmt.Variables {
		varName := variable.Name
		if varName == "" {
			varName = SetNames
		}
		
		varValue := ""
		if variable.Value != nil {
			varValue = fmt.Sprintf("%v", variable.Value)
		}

		vars[varName] = varValue
		log.Printf("设置变量: %s = %s", varName, varValue)
	}

	return &SetResult{
		Type:  "SET",
		Vars:  vars,
		Count: len(setStmt.Variables),
	}, nil
}

// ShowHandler SHOW 语句处理器
type ShowHandler struct{}

// NewShowHandler 创建 SHOW 处理器
func NewShowHandler() *ShowHandler {
	return &ShowHandler{}
}

// Handle 处理 SHOW 语句
func (h *ShowHandler) Handle(stmt ast.StmtNode) (interface{}, error) {
	showStmt, ok := stmt.(*ast.ShowStmt)
	if !ok {
		return nil, fmt.Errorf("不是 SHOW 语句")
	}

	log.Printf("处理 SHOW 语句")

	info := ExtractSQLInfo(stmt)

	return &ShowResult{
		Type:   "SHOW",
		ShowTp: fmt.Sprintf("%v", showStmt.Tp),
		Tables: info.Tables,
	}, nil
}

// UseHandler USE 语句处理器
type UseHandler struct{}

// NewUseHandler 创建 USE 处理器
func NewUseHandler() *UseHandler {
	return &UseHandler{}
}

// Handle 处理 USE 语句
func (h *UseHandler) Handle(stmt ast.StmtNode) (interface{}, error) {
	useStmt, ok := stmt.(*ast.UseStmt)
	if !ok {
		return nil, fmt.Errorf("不是 USE 语句")
	}

	dbName := useStmt.DBName

	log.Printf("处理 USE 语句，切换到数据库: %s", dbName)

	return &UseResult{
		Type:     "USE",
		Database: dbName,
	}, nil
}

// DefaultHandler 默认处理器
type DefaultHandler struct{}

// NewDefaultHandler 创建默认处理器
func NewDefaultHandler() *DefaultHandler {
	return &DefaultHandler{}
}

// Handle 处理未知类型语句
func (h *DefaultHandler) Handle(stmt ast.StmtNode) (interface{}, error) {
	stmtType := GetStmtType(stmt)
	log.Printf("使用默认处理器处理语句: %s", stmtType)
	
	return &DefaultResult{
		Type: stmtType,
		Stmt: stmt,
	}, nil
}

// 查询结果
type QueryResult struct {
	Type    string
	Tables  []string
	Columns []string
	Stmt    *ast.SelectStmt
}

// DML 结果
type DMLResult struct {
	Type     string
	Tables   []string
	Columns  []string
	Affected int64
}

// DDL 结果
type DDLResult struct {
	Type      string
	Tables    []string
	Databases []string
}

// SET 结果
type SetResult struct {
	Type  string
	Vars  map[string]interface{}
	Count int
}

// SHOW 结果
type ShowResult struct {
	Type   string
	ShowTp string
	Tables []string
}

// USE 结果
type UseResult struct {
	Type     string
	Database string
}

// 默认结果
type DefaultResult struct {
	Type string
	Stmt ast.StmtNode
}
