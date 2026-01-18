package parser

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// Parser SQL 解析器，封装 TiDB parser
type Parser struct {
	parser *parser.Parser
}

// NewParser 创建新的 SQL 解析�?
func NewParser() *Parser {
	return &Parser{
		parser: parser.New(),
	}
}

// ParseSQL 解析 SQL 语句，返�?AST 节点列表
func (p *Parser) ParseSQL(sql string) ([]ast.StmtNode, error) {
	stmtNodes, warnings, err := p.parser.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("解析 SQL 失败: %w", err)
	}
	if len(warnings) > 0 {
		// 记录警告信息
		for _, warn := range warnings {
			fmt.Printf("解析警告: %s\n", warn.Error())
		}
	}
	return stmtNodes, nil
}

// ParseOneStmt 解析单条 SQL 语句
func (p *Parser) ParseOneStmt(sql string) (ast.StmtNode, error) {
	stmts, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("未解析到 SQL 语句")
	}
	return stmts[0], nil
}

// ParseOneStmtText 解析 SQL 文本（去除注释和空白�?
func (p *Parser) ParseOneStmtText(sql string) (ast.StmtNode, error) {
	// 去除首尾空白
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("SQL 语句为空")
	}
	return p.ParseOneStmt(sql)
}

// ParseSelectStmt 解析 SELECT 语句
func (p *Parser) ParseSelectStmt(sql string) (*ast.SelectStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("不是 SELECT 语句")
	}
	return selectStmt, nil
}

// ParseInsertStmt 解析 INSERT 语句
func (p *Parser) ParseInsertStmt(sql string) (*ast.InsertStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	insertStmt, ok := stmt.(*ast.InsertStmt)
	if !ok {
		return nil, fmt.Errorf("不是 INSERT 语句")
	}
	return insertStmt, nil
}

// ParseUpdateStmt 解析 UPDATE 语句
func (p *Parser) ParseUpdateStmt(sql string) (*ast.UpdateStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	updateStmt, ok := stmt.(*ast.UpdateStmt)
	if !ok {
		return nil, fmt.Errorf("不是 UPDATE 语句")
	}
	return updateStmt, nil
}

// ParseDeleteStmt 解析 DELETE 语句
func (p *Parser) ParseDeleteStmt(sql string) (*ast.DeleteStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	deleteStmt, ok := stmt.(*ast.DeleteStmt)
	if !ok {
		return nil, fmt.Errorf("不是 DELETE 语句")
	}
	return deleteStmt, nil
}

// ParseSetStmt 解析 SET 语句
func (p *Parser) ParseSetStmt(sql string) (*ast.SetStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	setStmt, ok := stmt.(*ast.SetStmt)
	if !ok {
		return nil, fmt.Errorf("不是 SET 语句")
	}
	return setStmt, nil
}

// ParseShowStmt 解析 SHOW 语句
func (p *Parser) ParseShowStmt(sql string) (*ast.ShowStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	showStmt, ok := stmt.(*ast.ShowStmt)
	if !ok {
		return nil, fmt.Errorf("不是 SHOW 语句")
	}
	return showStmt, nil
}

// ParseUseStmt 解析 USE 语句
func (p *Parser) ParseUseStmt(sql string) (*ast.UseStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	useStmt, ok := stmt.(*ast.UseStmt)
	if !ok {
		return nil, fmt.Errorf("不是 USE 语句")
	}
	return useStmt, nil
}

// ParseCreateTableStmt 解析 CREATE TABLE 语句
func (p *Parser) ParseCreateTableStmt(sql string) (*ast.CreateTableStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	createTableStmt, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("不是 CREATE TABLE 语句")
	}
	return createTableStmt, nil
}

// ParseDropTableStmt 解析 DROP TABLE 语句
func (p *Parser) ParseDropTableStmt(sql string) (*ast.DropTableStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	dropTableStmt, ok := stmt.(*ast.DropTableStmt)
	if !ok {
		return nil, fmt.Errorf("不是 DROP TABLE 语句")
	}
	return dropTableStmt, nil
}

// ParseCreateDatabaseStmt 解析 CREATE DATABASE 语句
func (p *Parser) ParseCreateDatabaseStmt(sql string) (*ast.CreateDatabaseStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	createDBStmt, ok := stmt.(*ast.CreateDatabaseStmt)
	if !ok {
		return nil, fmt.Errorf("不是 CREATE DATABASE 语句")
	}
	return createDBStmt, nil
}

// ParseDropDatabaseStmt 解析 DROP DATABASE 语句
func (p *Parser) ParseDropDatabaseStmt(sql string) (*ast.DropDatabaseStmt, error) {
	stmt, err := p.ParseOneStmt(sql)
	if err != nil {
		return nil, err
	}
	
	dropDBStmt, ok := stmt.(*ast.DropDatabaseStmt)
	if !ok {
		return nil, fmt.Errorf("不是 DROP DATABASE 语句")
	}
	return dropDBStmt, nil
}

// GetStmtType 获取 SQL 语句类型
func GetStmtType(stmt ast.StmtNode) string {
	if stmt == nil {
		return "UNKNOWN"
	}
	
	switch stmt.(type) {
	case *ast.SelectStmt:
		return "SELECT"
	case *ast.InsertStmt:
		return "INSERT"
	case *ast.UpdateStmt:
		return "UPDATE"
	case *ast.DeleteStmt:
		return "DELETE"
	case *ast.SetStmt:
		return "SET"
	case *ast.ShowStmt:
		return "SHOW"
	case *ast.UseStmt:
		return "USE"
	case *ast.CreateTableStmt:
		return "CREATE_TABLE"
	case *ast.DropTableStmt:
		return "DROP_TABLE"
	case *ast.CreateDatabaseStmt:
		return "CREATE_DATABASE"
	case *ast.DropDatabaseStmt:
		return "DROP_DATABASE"
	case *ast.AlterTableStmt:
		return "ALTER_TABLE"
	case *ast.TruncateTableStmt:
		return "TRUNCATE_TABLE"
	case *ast.BeginStmt:
		return "BEGIN"
	case *ast.CommitStmt:
		return "COMMIT"
	case *ast.RollbackStmt:
		return "ROLLBACK"
	default:
		return "UNKNOWN"
	}
}

// IsWriteOperation 判断是否为写操作
func IsWriteOperation(stmt ast.StmtNode) bool {
	switch stmt.(type) {
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt,
		*ast.CreateTableStmt, *ast.DropTableStmt, *ast.CreateDatabaseStmt,
		*ast.DropDatabaseStmt, *ast.AlterTableStmt, *ast.TruncateTableStmt:
		return true
	default:
		return false
	}
}

// IsReadOperation 判断是否为读操作
func IsReadOperation(stmt ast.StmtNode) bool {
	switch stmt.(type) {
	case *ast.SelectStmt, *ast.ShowStmt:
		return true
	default:
		return false
	}
}

// IsTransactionOperation 判断是否为事务操�?
func IsTransactionOperation(stmt ast.StmtNode) bool {
	switch stmt.(type) {
	case *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt:
		return true
	default:
		return false
	}
}
