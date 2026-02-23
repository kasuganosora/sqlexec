package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// SQLAdapter SQL 解析适配器
type SQLAdapter struct {
	parser *parser.Parser
}

// simplifyTypeName 简化类型名，移除长度和精度说明
// 例如: DECIMAL(10,2) -> DECIMAL, VARCHAR(255) -> VARCHAR
func simplifyTypeName(fullType string) string {
	// 查找第一个括号
	if idx := strings.Index(fullType, "("); idx != -1 {
		return fullType[:idx]
	}
	return fullType
}

// NewSQLAdapter 创建 SQL 适配器
func NewSQLAdapter() *SQLAdapter {
	return &SQLAdapter{
		parser: parser.New(),
	}
}

// Parse 解析 SQL 语句
func (a *SQLAdapter) Parse(sql string) (*ParseResult, error) {
	// 预处理 SQL：将 WITH 子句转换为 COMMENT 子句
	preprocessedSQL := preprocessWithClause(sql)

	stmtNodes, _, err := a.parser.Parse(preprocessedSQL, "", "")
	if err != nil {
		return &ParseResult{
			Success: false,
			Error:   err.Error(),
		}, fmt.Errorf("parse SQL failed: %w", err)
	}

	if len(stmtNodes) == 0 {
		return &ParseResult{
			Success: false,
			Error:   "no statements found",
		}, fmt.Errorf("no statements found")
	}

	// 只处理第一条语句
	stmt := stmtNodes[0]
	statement, err := a.convertToStatement(stmt)
	if err != nil {
		return &ParseResult{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	return &ParseResult{
		Statement: statement,
		Success:   true,
	}, nil
}

// ParseMulti 解析多条 SQL 语句
func (a *SQLAdapter) ParseMulti(sql string) ([]*ParseResult, error) {
	stmtNodes, _, err := a.parser.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse SQL failed: %w", err)
	}

	results := make([]*ParseResult, 0, len(stmtNodes))
	for _, stmt := range stmtNodes {
		statement, err := a.convertToStatement(stmt)
		if err != nil {
			results = append(results, &ParseResult{
				Success: false,
				Error:   err.Error(),
			})
			continue
		}
		results = append(results, &ParseResult{
			Statement: statement,
			Success:   true,
		})
	}

	return results, nil
}

// convertToStatement 将 AST 节点转换为 SQLStatement
func (a *SQLAdapter) convertToStatement(node ast.StmtNode) (*SQLStatement, error) {
	stmt := &SQLStatement{
		RawSQL: node.Text(),
	}

	switch stmtNode := node.(type) {
	case *ast.SelectStmt:
		stmt.Type = SQLTypeSelect
		selectStmt, err := a.convertSelectStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Select = selectStmt

	case *ast.InsertStmt:
		stmt.Type = SQLTypeInsert
		insertStmt, err := a.convertInsertStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Insert = insertStmt

	case *ast.UpdateStmt:
		stmt.Type = SQLTypeUpdate
		updateStmt, err := a.convertUpdateStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Update = updateStmt

	case *ast.DeleteStmt:
		stmt.Type = SQLTypeDelete
		deleteStmt, err := a.convertDeleteStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Delete = deleteStmt

	case *ast.CreateTableStmt:
		stmt.Type = SQLTypeCreate
		createStmt, err := a.convertCreateTableStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Create = createStmt

	case *ast.DropTableStmt:
		// TiDB uses DropTableStmt for both DROP TABLE and DROP VIEW
		// Check IsView field to distinguish
		if stmtNode.IsView {
			stmt.Type = SQLTypeDropView
			dropViewStmt, err := a.convertDropViewStmt(stmtNode)
			if err != nil {
				return nil, err
			}
			stmt.DropView = dropViewStmt
		} else {
			stmt.Type = SQLTypeDrop
			dropStmt, err := a.convertDropTableStmt(stmtNode)
			if err != nil {
				return nil, err
			}
			stmt.Drop = dropStmt
		}

	case *ast.TruncateTableStmt:
		stmt.Type = SQLTypeTruncate
		stmt.Drop = &DropStatement{
			Type:     "TRUNCATE",
			Name:     stmtNode.Table.Name.String(),
			IfExists: false,
		}

	case *ast.AlterTableStmt:
		stmt.Type = SQLTypeAlter
		alterStmt, err := a.convertAlterTableStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Alter = alterStmt

	case *ast.CreateIndexStmt:
		stmt.Type = SQLTypeCreate
		createIndexStmt, err := a.convertCreateIndexStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.CreateIndex = createIndexStmt

	case *ast.DropIndexStmt:
		stmt.Type = SQLTypeDrop
		dropIndexStmt, err := a.convertDropIndexStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.DropIndex = dropIndexStmt

	case *ast.UseStmt:
		stmt.Type = SQLTypeUse
		useStmt := a.convertUseStmt(stmtNode)
		stmt.Use = useStmt

	case *ast.ShowStmt:
		stmt.Type = SQLTypeShow
		showStmt, err := a.convertShowStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Show = showStmt

	case *ast.ExplainStmt:
		// DESCRIBE/DESC statements are parsed as ExplainStmt with a ShowStmt inside
		if showStmt, ok := stmtNode.Stmt.(*ast.ShowStmt); ok && showStmt.Tp == ast.ShowColumns {
			stmt.Type = SQLTypeDescribe
			describeStmt, err := a.convertDescribeFromShowStmt(showStmt)
			if err != nil {
				return nil, err
			}
			stmt.Describe = describeStmt
		} else {
			// Regular EXPLAIN statement
			stmt.Type = SQLTypeExplain
			explainStmt := &ExplainStatement{
				TargetSQL: stmt.RawSQL,
				Format:    "TREE", // Default format
				Analyze:   strings.HasPrefix(strings.ToUpper(stmt.RawSQL), "EXPLAIN ANALYZE"),
			}

			// Try to extract the inner SELECT statement
			if stmtNode.Stmt != nil {
				if selectStmt, ok := stmtNode.Stmt.(*ast.SelectStmt); ok {
					selectStmt, err := a.convertSelectStmt(selectStmt)
					if err != nil {
						return nil, err
					}
					explainStmt.Query = selectStmt
				}
			}

			stmt.Explain = explainStmt
		}

	case *ast.CreateViewStmt:
		stmt.Type = SQLTypeCreateView
		createViewStmt, err := a.convertCreateViewStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.CreateView = createViewStmt

	case *ast.SetStmt:
		stmt.Type = SQLTypeSet
		setStmt := a.convertSetStmt(stmtNode)
		stmt.Set = setStmt

	default:
		stmt.Type = SQLTypeUnknown
	}

	return stmt, nil
}

// convertSelectStmt 转换 SELECT 语句
func (a *SQLAdapter) convertSelectStmt(stmt *ast.SelectStmt) (*SelectStatement, error) {
	selectStmt := &SelectStatement{
		Distinct: stmt.Distinct,
	}

	// 解析 SELECT 列
	if stmt.Fields != nil {
		selectStmt.Columns = make([]SelectColumn, 0, len(stmt.Fields.Fields))
		for _, field := range stmt.Fields.Fields {
			col, err := a.convertSelectField(field)
			if err != nil {
				return nil, err
			}
			selectStmt.Columns = append(selectStmt.Columns, *col)
		}
	}

	// 解析 FROM
	if stmt.From != nil && stmt.From.TableRefs != nil {
		// 从 Join 的左表获取主表名
		if tableSource, ok := stmt.From.TableRefs.Left.(*ast.TableSource); ok {
			if tableName, ok := tableSource.Source.(*ast.TableName); ok {
				// Preserve full qualified table name (schema.table)
				fullName := tableName.Name.String()
				if tableName.Schema.String() != "" {
					fullName = tableName.Schema.String() + "." + fullName
				}
				selectStmt.From = fullName
			}
		}

		// 解析 JOIN（从右表）
		if stmt.From.TableRefs.Right != nil {
			selectStmt.Joins = a.convertJoinTree(stmt.From.TableRefs.Right)
		}
	}

	// 解析 WHERE
	if stmt.Where != nil {
		expr, err := a.convertExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		selectStmt.Where = expr
	}

	// 解析 GROUP BY
	if stmt.GroupBy != nil {
		selectStmt.GroupBy = make([]string, 0, len(stmt.GroupBy.Items))
		for _, item := range stmt.GroupBy.Items {
			if col, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				selectStmt.GroupBy = append(selectStmt.GroupBy, col.Name.Name.String())
			}
		}
	}

	// 解析 HAVING
	if stmt.Having != nil {
		expr, err := a.convertExpression(stmt.Having.Expr)
		if err != nil {
			return nil, err
		}
		selectStmt.Having = expr
	}

	// 解析 ORDER BY
	if stmt.OrderBy != nil {
		selectStmt.OrderBy = make([]OrderByItem, 0, len(stmt.OrderBy.Items))
		for _, item := range stmt.OrderBy.Items {
			direction := "ASC"
			if item.Desc {
				direction = "DESC"
			}
			// Handle column name expressions
			if col, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				selectStmt.OrderBy = append(selectStmt.OrderBy, OrderByItem{
					Column:    col.Name.Name.String(),
					Direction: direction,
				})
			} else if funcCall, ok := item.Expr.(*ast.FuncCallExpr); ok {
				// Handle function expressions like vec_cosine_distance(...)
				selectStmt.OrderBy = append(selectStmt.OrderBy, OrderByItem{
					Column:    extractFuncCallString(funcCall),
					Direction: direction,
				})
			}
		}
	}

	// 解析 LIMIT
	if stmt.Limit != nil {
		// 处理LIMIT值
		if stmt.Limit.Count != nil {
			// 尝试多种方式提取值
			if valExpr, ok := stmt.Limit.Count.(ast.ValueExpr); ok {
				val := valExpr.GetValue()
				// 确保转换为int64
				var i64 int64
				switch v := val.(type) {
				case int:
					i64 = int64(v)
				case int8:
					i64 = int64(v)
				case int16:
					i64 = int64(v)
				case int32:
					i64 = int64(v)
				case int64:
					i64 = v
				case uint:
					i64 = int64(v)
				case uint8:
					i64 = int64(v)
				case uint16:
					i64 = int64(v)
				case uint32:
					i64 = int64(v)
				case uint64:
					i64 = int64(v)
				}
				selectStmt.Limit = &i64
			}
		}
		// 处理OFFSET值
		if stmt.Limit.Offset != nil {
			if valExpr, ok := stmt.Limit.Offset.(ast.ValueExpr); ok {
				val := valExpr.GetValue()
				// 确保转换为int64
				var i64 int64
				switch v := val.(type) {
				case int:
					i64 = int64(v)
				case int8:
					i64 = int64(v)
				case int16:
					i64 = int64(v)
				case int32:
					i64 = int64(v)
				case int64:
					i64 = v
				case uint:
					i64 = int64(v)
				case uint8:
					i64 = int64(v)
				case uint16:
					i64 = int64(v)
				case uint32:
					i64 = int64(v)
				case uint64:
					i64 = int64(v)
				}
				selectStmt.Offset = &i64
			}
		}
	}

	return selectStmt, nil
}

// convertJoinTree 递归转换 JOIN 树
func (a *SQLAdapter) convertJoinTree(node ast.ResultSetNode) []JoinInfo {
	result := make([]JoinInfo, 0)

	if node == nil {
		return result
	}

	switch n := node.(type) {
	case *ast.Join:
		// 先处理左子树的 JOIN（如果有）
		if leftJoins := a.convertJoinTree(n.Left); len(leftJoins) > 0 {
			result = append(result, leftJoins...)
		}

		// 处理当前 JOIN
		joinType := JoinTypeInner
		switch n.Tp {
		case ast.LeftJoin:
			joinType = JoinTypeLeft
		case ast.RightJoin:
			joinType = JoinTypeRight
		case ast.CrossJoin:
			joinType = JoinTypeCross
		}

		joinInfo := JoinInfo{
			Type: joinType,
		}

		// 从右表获取表名
		if tableSource, ok := n.Right.(*ast.TableSource); ok {
			if tableName, ok := tableSource.Source.(*ast.TableName); ok {
				joinInfo.Table = tableName.Name.String()
				if tableSource.AsName.L != "" {
					joinInfo.Alias = tableSource.AsName.String()
				}
			}
		}

		// 解析 ON 条件
		if n.On != nil && n.On.Expr != nil {
			expr, _ := a.convertExpression(n.On.Expr)
			joinInfo.Condition = expr
		}

		result = append(result, joinInfo)

		// 处理右子树的 JOIN（如果有）
		if rightJoins := a.convertJoinTree(n.Right); len(rightJoins) > 0 {
			result = append(result, rightJoins...)
		}
	}

	return result
}

// convertInsertStmt 转换 INSERT 语句
func (a *SQLAdapter) convertInsertStmt(stmt *ast.InsertStmt) (*InsertStatement, error) {
	// 从 TableRefsClause 获取表名
	var tableName string
	if stmt.Table != nil && stmt.Table.TableRefs != nil {
		if tableSource, ok := stmt.Table.TableRefs.Left.(*ast.TableSource); ok {
			if tableNameNode, ok := tableSource.Source.(*ast.TableName); ok {
				tableName = tableNameNode.Name.String()
			}
		}
	}

	insertStmt := &InsertStatement{
		Table: tableName,
	}

	// 解析列名
	if stmt.Columns != nil {
		insertStmt.Columns = make([]string, 0, len(stmt.Columns))
		for _, col := range stmt.Columns {
			insertStmt.Columns = append(insertStmt.Columns, col.Name.String())
		}
	}

	// 解析值列表 (Lists 是 [][]ExprNode)
	insertStmt.Values = make([][]interface{}, 0, len(stmt.Lists))
	for _, rowExprs := range stmt.Lists {
		rowValues := make([]interface{}, 0, len(rowExprs))
		for _, expr := range rowExprs {
			val, err := a.extractValue(expr)
			if err != nil {
				// 如果无法提取值，跳过
				rowValues = append(rowValues, nil)
				continue
			}
			rowValues = append(rowValues, val)
		}
		insertStmt.Values = append(insertStmt.Values, rowValues)
	}

	return insertStmt, nil
}

// convertUpdateStmt 转换 UPDATE 语句
func (a *SQLAdapter) convertUpdateStmt(stmt *ast.UpdateStmt) (*UpdateStatement, error) {
	// 从 TableRefsClause 获取表名
	var tableName string
	if stmt.TableRefs != nil && stmt.TableRefs.TableRefs != nil {
		if tableSource, ok := stmt.TableRefs.TableRefs.Left.(*ast.TableSource); ok {
			if tableNameNode, ok := tableSource.Source.(*ast.TableName); ok {
				tableName = tableNameNode.Name.String()
			}
		}
	}

	updateStmt := &UpdateStatement{
		Table: tableName,
		Set:   make(map[string]interface{}),
	}

	// 解析 SET 子句 (List 是 []*Assignment)
	for _, assign := range stmt.List {
		col := assign.Column.Name.String()
		val, err := a.extractValue(assign.Expr)
		if err != nil {
			continue
		}
		updateStmt.Set[col] = val
	}

	// 解析 WHERE
	if stmt.Where != nil {
		expr, err := a.convertExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		updateStmt.Where = expr
	}

	// 解析 ORDER BY
	if stmt.Order != nil {
		updateStmt.OrderBy = make([]OrderByItem, 0, len(stmt.Order.Items))
		for _, item := range stmt.Order.Items {
			direction := "ASC"
			if item.Desc {
				direction = "DESC"
			}
			if col, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				updateStmt.OrderBy = append(updateStmt.OrderBy, OrderByItem{
					Column:    col.Name.Name.String(),
					Direction: direction,
				})
			}
		}
	}

	// 解析 LIMIT
	if stmt.Limit != nil {
		if stmt.Limit.Count != nil {
			val, err := a.extractValue(stmt.Limit.Count)
			if err == nil {
				if v, ok := val.(int64); ok {
					updateStmt.Limit = &v
				}
			}
		}
	}

	return updateStmt, nil
}

// convertDeleteStmt 转换 DELETE 语句
func (a *SQLAdapter) convertDeleteStmt(stmt *ast.DeleteStmt) (*DeleteStatement, error) {
	// 从 TableRefsClause 获取表名
	var tableName string
	if stmt.TableRefs != nil && stmt.TableRefs.TableRefs != nil {
		if tableSource, ok := stmt.TableRefs.TableRefs.Left.(*ast.TableSource); ok {
			if tableNameNode, ok := tableSource.Source.(*ast.TableName); ok {
				tableName = tableNameNode.Name.String()
			}
		}
	}

	deleteStmt := &DeleteStatement{
		Table: tableName,
	}

	// 解析 WHERE
	if stmt.Where != nil {
		expr, err := a.convertExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		deleteStmt.Where = expr
	}

	// 解析 ORDER BY
	if stmt.Order != nil {
		deleteStmt.OrderBy = make([]OrderByItem, 0, len(stmt.Order.Items))
		for _, item := range stmt.Order.Items {
			direction := "ASC"
			if item.Desc {
				direction = "DESC"
			}
			if col, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				deleteStmt.OrderBy = append(deleteStmt.OrderBy, OrderByItem{
					Column:    col.Name.Name.String(),
					Direction: direction,
				})
			}
		}
	}

	// 解析 LIMIT
	if stmt.Limit != nil {
		if stmt.Limit.Count != nil {
			val, err := a.extractValue(stmt.Limit.Count)
			if err == nil {
				if v, ok := val.(int64); ok {
					deleteStmt.Limit = &v
				}
			}
		}
	}

	return deleteStmt, nil
}

// convertCreateTableStmt 转换 CREATE TABLE 语句
func (a *SQLAdapter) convertCreateTableStmt(stmt *ast.CreateTableStmt) (*CreateStatement, error) {
	createStmt := &CreateStatement{
		Type:    "TABLE",
		Name:    stmt.Table.Name.String(),
		Columns: make([]ColumnInfo, 0),
		Options: make(map[string]interface{}),
	}

	// Check for db.table format
	if stmt.Table.Schema.String() != "" {
		createStmt.Database = stmt.Table.Schema.String()
	}

	// 解析列定义
	for _, col := range stmt.Cols {
		colInfo := ColumnInfo{
			Name:     col.Name.Name.String(),
			Type:     simplifyTypeName(col.Tp.String()),
			Nullable: true, // 默认可空
			Default:  nil,
		}

		// 检查是否为 VECTOR 类型
		colTypeStr := col.Tp.String()
		if isVectorType(colTypeStr) {
			colInfo.Type = "VECTOR"
			colInfo.VectorDim = extractVectorDimension(colTypeStr)
			colInfo.VectorType = "float32"
		}

		// 从 Options 解析列属性
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionNotNull, ast.ColumnOptionPrimaryKey:
				colInfo.Nullable = false
				colInfo.Primary = opt.Tp == ast.ColumnOptionPrimaryKey
			case ast.ColumnOptionDefaultValue:
				if opt.Expr != nil {
					val, _ := a.extractValue(opt.Expr)
					colInfo.Default = val
				}
			case ast.ColumnOptionAutoIncrement:
				colInfo.AutoInc = true
			case ast.ColumnOptionUniqKey:
				colInfo.Unique = true
			case ast.ColumnOptionGenerated:
				// 解析生成列
				colInfo.IsGenerated = true

				// 根据 Stored 字段判断生成列类型
				// Stored == true 表示 STORED 类型，false 表示 VIRTUAL 类型（默认）
				if opt.Stored {
					colInfo.GeneratedType = "STORED"
				} else {
					colInfo.GeneratedType = "VIRTUAL"
				}

				// 提取表达式字符串
				if opt.Expr != nil {
					colInfo.GeneratedExpr = opt.Expr.Text()

					// 提取依赖的列名
					colInfo.GeneratedDepends = a.extractColumnNames(opt.Expr)
				}
			}
		}

		createStmt.Columns = append(createStmt.Columns, colInfo)
	}

	// Parse table options (ENGINE, COMMENT, etc.)
	for _, opt := range stmt.Options {
		if opt.Tp == ast.TableOptionEngine {
			engineName := strings.ToUpper(opt.StrValue)
			createStmt.Options["engine"] = strings.ToLower(opt.StrValue)
			if engineName == "PERSISTENT" {
				createStmt.Persistent = true
			}
		}
		if opt.Tp == ast.TableOptionComment {
			createStmt.Options["comment"] = opt.StrValue
		}
	}

	return createStmt, nil
}

// isVectorType 检查是否为 VECTOR 类型
func isVectorType(typeStr string) bool {
	upperType := strings.ToUpper(typeStr)
	return strings.HasPrefix(upperType, "VECTOR") || strings.HasPrefix(upperType, "ARRAY<")
}

// extractVectorDimension 从 VECTOR(dim) 中提取维度
func extractVectorDimension(typeStr string) int {
	// 处理 VECTOR(dim) 格式
	start := strings.Index(typeStr, "(")
	end := strings.Index(typeStr, ")")
	if start != -1 && end != -1 && end > start+1 {
		dimStr := typeStr[start+1 : end]
		var dim int
		if _, err := fmt.Sscanf(dimStr, "%d", &dim); err == nil {
			return dim
		}
	}
	// 处理 ARRAY<FLOAT, dim> 格式
	if strings.Contains(strings.ToUpper(typeStr), "ARRAY<") {
		parts := strings.Split(typeStr, ",")
		if len(parts) == 2 {
			var dim int
			if _, err := fmt.Sscanf(parts[1], "%d>", &dim); err == nil {
				return dim
			}
		}
	}
	return 0
}

// convertDropTableStmt 转换 DROP TABLE 语句
func (a *SQLAdapter) convertDropTableStmt(stmt *ast.DropTableStmt) (*DropStatement, error) {
	dropStmt := &DropStatement{
		Type:     "TABLE",
		IfExists: stmt.IfExists,
	}

	// Support multiple tables: DROP TABLE t1, t2, t3
	if len(stmt.Tables) > 0 {
		dropStmt.Name = stmt.Tables[0].Name.String()
		if len(stmt.Tables) > 1 {
			dropStmt.Names = make([]string, len(stmt.Tables))
			for i, t := range stmt.Tables {
				dropStmt.Names[i] = t.Name.String()
			}
		}
	}

	return dropStmt, nil
}

// convertAlterTableStmt 转换 ALTER TABLE 语句
func (a *SQLAdapter) convertAlterTableStmt(stmt *ast.AlterTableStmt) (*AlterStatement, error) {
	alterStmt := &AlterStatement{
		Type:    "TABLE",
		Name:    stmt.Table.Name.String(),
		Actions: make([]AlterAction, 0),
	}

	for _, spec := range stmt.Specs {
		action := AlterAction{
			Type: fmt.Sprintf("%d", int(spec.Tp)),
		}

		if spec.NewColumnName != nil {
			action.OldName = spec.OldColumnName.Name.String()
			action.NewName = spec.NewColumnName.Name.String()
		}

		alterStmt.Actions = append(alterStmt.Actions, action)
	}

	return alterStmt, nil
}

// convertSelectField 转换 SELECT 字段
func (a *SQLAdapter) convertSelectField(field *ast.SelectField) (*SelectColumn, error) {
	col := &SelectColumn{}

	// 检查通配符：使用字符串检查
	if field.WildCard != nil {
		col.IsWildcard = true
		col.Name = "*"
		return col, nil
	}

	// 处理列名
	if expr, ok := field.Expr.(*ast.ColumnNameExpr); ok {
		col.Name = expr.Name.Name.String()
		col.Table = expr.Name.Table.String()
		col.IsWildcard = expr.Name.Name.String() == "*"
	} else if varExpr, ok := field.Expr.(*ast.VariableExpr); ok {
		// 系统/会话变量
		if varExpr.IsSystem {
			col.Name = "@@" + varExpr.Name
		} else {
			col.Name = "@" + varExpr.Name
		}
	}

	// 处理函数调用 - 将表达式转换为 Expression
	if field.Expr != nil {
		if expr, err := a.convertExpression(field.Expr); err == nil {
			col.Expr = expr
			// 如果是函数调用且没有设置名称，使用函数名作为列名
			if col.Name == "" && expr.Type == ExprTypeFunction {
				col.Name = expr.Function
			}
		}
	}

	if field.AsName.L != "" {
		col.Alias = field.AsName.String()
	}

	return col, nil
}

// convertExpression 转换表达式
func (a *SQLAdapter) convertExpression(node ast.ExprNode) (*Expression, error) {
	expr := &Expression{}

	switch n := node.(type) {
	case *ast.BinaryOperationExpr:
		expr.Type = ExprTypeOperator
		expr.Operator = n.Op.String()
		left, _ := a.convertExpression(n.L)
		right, _ := a.convertExpression(n.R)
		expr.Left = left
		expr.Right = right

	case *ast.ColumnNameExpr:
		expr.Type = ExprTypeColumn
		expr.Column = n.Name.Name.String()
		if n.Name.Table.L != "" {
			if n.Name.Schema.L != "" {
				expr.Column = n.Name.Schema.String() + "." + n.Name.Table.String() + "." + expr.Column
			} else {
				expr.Column = n.Name.Table.String() + "." + expr.Column
			}
		}

	case ast.ValueExpr:
		// ValueExpr 是接口类型
		expr.Type = ExprTypeValue
		expr.Value = n.GetValue()

	case *ast.FuncCallExpr:
		expr.Type = ExprTypeFunction
		expr.Function = n.FnName.String()
		args := make([]Expression, 0)
		for _, arg := range n.Args {
			converted, _ := a.convertExpression(arg)
			args = append(args, *converted)
		}
		expr.Args = args

	case *ast.AggregateFuncExpr:
		// Aggregate functions such as COUNT(*), SUM(col), AVG(col), etc.
		expr.Type = ExprTypeFunction
		expr.Function = strings.ToUpper(n.F)
		// Store the function name in Value so that parseAggregationFunction can read it.
		expr.Value = n.F
		args := make([]Expression, 0, len(n.Args))
		for _, arg := range n.Args {
			converted, _ := a.convertExpression(arg)
			if converted != nil {
				args = append(args, *converted)
			}
		}
		expr.Args = args

	case *ast.PatternLikeOrIlikeExpr:
		// 处理 LIKE 表达式
		expr.Type = ExprTypeOperator
		if n.Not {
			expr.Operator = "NOT LIKE"
		} else {
			expr.Operator = "LIKE"
		}
		left, _ := a.convertExpression(n.Expr)
		right, _ := a.convertExpression(n.Pattern)
		expr.Left = left
		expr.Right = right

	case *ast.BetweenExpr:
		// 处理 BETWEEN 表达式
		expr.Type = ExprTypeOperator
		if n.Not {
			expr.Operator = "NOT BETWEEN"
		} else {
			expr.Operator = "BETWEEN"
		}
		left, _ := a.convertExpression(n.Expr)
		expr.Left = left
		// 存储两个边界值
		leftBound, _ := a.convertExpression(n.Left)
		rightBound, _ := a.convertExpression(n.Right)
		// 将 BETWEEN 转换为一个包含两个值的列表
		// expr.Left: 列, expr.Right: [min, max]
		expr.Right = &Expression{
			Type:  ExprTypeValue,
			Value: []interface{}{leftBound, rightBound},
		}

	case *ast.PatternInExpr:
		// 处理 IN 表达式
		expr.Type = ExprTypeOperator
		if n.Not {
			expr.Operator = "NOT IN"
		} else {
			expr.Operator = "IN"
		}
		left, _ := a.convertExpression(n.Expr)
		expr.Left = left
		// 提取 IN 列表中的所有值
		values := make([]interface{}, 0, len(n.List))
		for _, item := range n.List {
			if valExpr, ok := item.(ast.ValueExpr); ok {
				values = append(values, valExpr.GetValue())
			} else {
				// 复杂表达式，尝试转换
				converted, _ := a.convertExpression(item)
				if converted != nil && converted.Type == ExprTypeValue {
					values = append(values, converted.Value)
				}
			}
		}
		expr.Right = &Expression{
			Type:  ExprTypeValue,
			Value: values,
		}

	case *ast.IsNullExpr:
		// 处理 IS NULL / IS NOT NULL 表达式
		expr.Type = ExprTypeOperator
		if n.Not {
			expr.Operator = "IS NOT NULL"
		} else {
			expr.Operator = "IS NULL"
		}
		left, _ := a.convertExpression(n.Expr)
		expr.Left = left

	case *ast.ParenthesesExpr:
		// 括号表达式，直接返回内部表达式的转换结果
		innerExpr, err := a.convertExpression(n.Expr)
		if err != nil {
			return nil, err
		}
		return innerExpr, nil

	case *ast.VariableExpr:
		// 系统变量或会话变量：@@var_name 或 @var_name
		expr.Type = ExprTypeColumn
		if n.IsSystem {
			expr.Column = "@@" + n.Name
		} else {
			expr.Column = "@" + n.Name
		}

	default:
		expr.Type = ExprTypeValue
	}

	return expr, nil
}

// extractValue 提取值
func (a *SQLAdapter) extractValue(node ast.ExprNode) (interface{}, error) {
	if node == nil {
		return nil, fmt.Errorf("node is nil")
	}

	// 尝试转换为ValueExpr
	if valExpr, ok := node.(ast.ValueExpr); ok {
		val := valExpr.GetValue()

		// 处理TiDB的特殊类型（如MyDecimal）
		if val != nil {
			convertedVal, err := convertTiDBValue(val)
			if err == nil {
				return convertedVal, nil
			}
			// 如果转换失败，返回原始值
			return val, nil
		}

		return val, nil
	}

	// 如果不是ValueExpr，可能是其他表达式类型
	// 对于LIMIT，我们可能需要使用不同的方法
	return nil, fmt.Errorf("not a value expression: %T", node)
}

// convertTiDBValue 转换TiDB的内部类型为标准Go类型
func convertTiDBValue(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	// 首先尝试类型断言
	switch v := val.(type) {
	case bool:
		// 保持bool类型，不要转换为float64
		return v, nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
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
		if v <= uint64(math.MaxInt64) {
			return int64(v), nil
		}
		return v, nil
	case string:
		// 尝试解析为float64（针对DECIMAL字符串）
		if f, err := parseDecimalString(v); err == nil {
			return f, nil
		}
		return v, nil
	default:
		// 如果标准类型断言失败，尝试通过String()方法处理
		// 这处理TiDB的特殊类型如MyDecimal
		type Stringer interface {
			String() string
		}
		if stringer, ok := val.(Stringer); ok {
			strVal := stringer.String()
			// 尝试将字符串解析为float64
			if f, err := parseDecimalString(strVal); err == nil {
				return f, nil
			}
			// 如果解析失败，返回原始值
			return val, nil
		}

		// 无法转换，返回原始值
		return val, nil
	}
}

// parseDecimalString 尝试解析DECIMAL字符串为float64
func parseDecimalString(s string) (float64, error) {
	// 使用Go的strconv解析
	f, err := strconv.ParseFloat(s, 64)
	return f, err
}

// extractColumnNames 从表达式中提取列名
func (a *SQLAdapter) extractColumnNames(expr ast.ExprNode) []string {
	names := make(map[string]bool)
	a.collectColumnNames(expr, names)

	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	return result
}

// collectColumnNames 递归收集表达式中的列名
func (a *SQLAdapter) collectColumnNames(expr ast.ExprNode, names map[string]bool) {
	if expr == nil {
		return
	}

	switch n := expr.(type) {
	case *ast.ColumnNameExpr:
		// 列名表达式
		colName := n.Name.Name.String()
		names[colName] = true

	case *ast.BinaryOperationExpr:
		// 二元运算表达式，递归处理左右操作数
		a.collectColumnNames(n.L, names)
		a.collectColumnNames(n.R, names)

	case *ast.UnaryOperationExpr:
		// 一元运算表达式，递归处理操作数
		a.collectColumnNames(n.V, names)

	case *ast.FuncCallExpr:
		// 函数调用表达式，递归处理所有参数
		for _, arg := range n.Args {
			a.collectColumnNames(arg, names)
		}

	case *ast.ParenthesesExpr:
		// 括号表达式，递归处理内部表达式
		a.collectColumnNames(n.Expr, names)

	case *ast.PatternLikeOrIlikeExpr:
		// LIKE 表达式，递归处理
		a.collectColumnNames(n.Expr, names)
		a.collectColumnNames(n.Pattern, names)

	case *ast.BetweenExpr:
		// BETWEEN 表达式，递归处理
		a.collectColumnNames(n.Expr, names)
		a.collectColumnNames(n.Left, names)
		a.collectColumnNames(n.Right, names)

	case *ast.CaseExpr:
		// CASE 表达式，递归处理
		for _, item := range n.WhenClauses {
			a.collectColumnNames(item.Expr, names)
			a.collectColumnNames(item.Result, names)
		}
		a.collectColumnNames(n.ElseClause, names)
	}
}

// convertShowStmt 转换 SHOW 语句
func (a *SQLAdapter) convertShowStmt(stmt *ast.ShowStmt) (*ShowStatement, error) {
	showStmt := &ShowStatement{}

	// 获取 SHOW 类型
	switch stmt.Tp {
	case ast.ShowTables:
		showStmt.Type = "TABLES"
	case ast.ShowDatabases:
		showStmt.Type = "DATABASES"
	case ast.ShowColumns:
		showStmt.Type = "COLUMNS"
		if stmt.Table != nil {
			showStmt.Table = stmt.Table.Name.String()
		}
	case ast.ShowCreateTable:
		showStmt.Type = "CREATE_TABLE"
		if stmt.Table != nil {
			showStmt.Table = stmt.Table.Name.String()
		}
	case ast.ShowProcessList:
		showStmt.Type = "PROCESSLIST"
		showStmt.Full = stmt.Full
	case ast.ShowVariables:
		showStmt.Type = "VARIABLES"
	case ast.ShowStatus:
		showStmt.Type = "STATUS"
	default:
		showStmt.Type = "UNKNOWN"
	}

	// 处理 LIKE 子句 - Pattern 是 *PatternLikeOrIlikeExpr 类型
	// 其中 Pattern.Pattern 是实际的模式表达式 (ValueExpr)
	if stmt.Pattern != nil && stmt.Pattern.Pattern != nil {
		// 尝试获取字符串值
		// Pattern.Pattern 可能是 *test_driver.ValueExpr，其 GetDatumString() 方法可以获取值
		if getter, ok := stmt.Pattern.Pattern.(interface{ GetDatumString() string }); ok {
			showStmt.Like = getter.GetDatumString()
		} else {
			// fallback to OriginalText
			showStmt.Like = stmt.Pattern.Pattern.OriginalText()
		}
	}

	// 处理 WHERE 子句
	if stmt.Where != nil {
		showStmt.Where = stmt.Where.OriginalText()
	}

	return showStmt, nil
}

// convertDescribeFromShowStmt 从 ShowStmt 转换 DESCRIBE 语句
// DESCRIBE/DESC 语句被 TiDB parser 解析为 ExplainStmt，其中包含一个 ShowStmt
func (a *SQLAdapter) convertDescribeFromShowStmt(stmt *ast.ShowStmt) (*DescribeStatement, error) {
	describeStmt := &DescribeStatement{}

	// 获取表名
	if stmt.Table != nil {
		describeStmt.Table = stmt.Table.Name.String()
	}

	// 获取列名（如果有）
	if stmt.Column != nil {
		describeStmt.Column = stmt.Column.Name.String()
	}

	return describeStmt, nil
}

// convertUseStmt 转换 USE 语句
func (a *SQLAdapter) convertUseStmt(stmt *ast.UseStmt) *UseStatement {
	// UseStmt.DBName is a CIStr (C identifier string)
	// Convert it to regular Go string
	dbName := string(stmt.DBName)

	useStmt := &UseStatement{
		Database: dbName,
	}
	return useStmt
}

// convertCreateIndexStmt 转换 CREATE INDEX 语句
func (a *SQLAdapter) convertCreateIndexStmt(stmt *ast.CreateIndexStmt) (*CreateIndexStatement, error) {
	createIndexStmt := &CreateIndexStatement{
		IndexName: stmt.IndexName,
		IfExists:  stmt.IfNotExists,
		Unique:    stmt.KeyType == ast.IndexKeyTypeUnique,
	}

	// 获取表名
	if stmt.Table != nil {
		createIndexStmt.TableName = stmt.Table.Name.String()
	}

	// 获取索引类型（BTREE, HASH, FULLTEXT, VECTOR）
	// 默认为 BTREE
	createIndexStmt.IndexType = "BTREE"

	// 检查是否为向量索引（通过 KeyType 或 USING 子句）
	indexType := "btree"
	isVectorIndex := false

	// 优先检查 TiDB 的 KeyType
	if stmt.KeyType == ast.IndexKeyTypeVector {
		isVectorIndex = true
		createIndexStmt.IsVectorIndex = true
		createIndexStmt.IndexType = "VECTOR"
		// 从 IndexOption.Tp 提取向量索引类型
		if stmt.IndexOption != nil && stmt.IndexOption.Tp != 0 {
			tpStr := stmt.IndexOption.Tp.String()
			tpLower := strings.ToLower(tpStr)
			// 检查是否是支持的向量索引类型
			switch tpLower {
			case "hnsw", "flat", "ivf_flat", "ivf_sq8", "ivf_pq",
				"hnsw_sq", "hnsw_pq", "ivf_rabitq", "hnsw_prq", "aisaq":
				createIndexStmt.VectorIndexType = tpLower
			}
		}
	} else if stmt.IndexOption != nil && stmt.IndexOption.Tp != 0 {
		// 根据 TiDB 的索引类型常量转换
		switch stmt.IndexOption.Tp {
		case ast.IndexTypeHash:
			indexType = "hash"
		case ast.IndexTypeBtree:
			indexType = "btree"
		case ast.IndexTypeRtree:
			indexType = "rtree"
		}
		createIndexStmt.IndexType = strings.ToUpper(indexType)
	}

	// 如果不是 TiDB KeyType 向量索引，检查 USING 子句
	if !isVectorIndex {
		// 检查 USING 子句中是否有向量索引类型
		if stmt.IndexOption != nil && stmt.IndexOption.ParserName.O != "" {
			usingType := strings.ToLower(stmt.IndexOption.ParserName.O)
			switch usingType {
			case "flat", "vector_flat",
				"hnsw", "vector_hnsw",
				"ivf_flat", "vector_ivf_flat",
				"ivf_sq8", "vector_ivf_sq8",
				"ivf_pq", "vector_ivf_pq",
				"hnsw_sq", "vector_hnsw_sq",
				"hnsw_pq", "vector_hnsw_pq",
				"ivf_rabitq", "vector_ivf_rabitq",
				"hnsw_prq", "vector_hnsw_prq",
				"aisaq", "vector_aisaq":
				createIndexStmt.IsVectorIndex = true
				createIndexStmt.VectorIndexType = usingType
				createIndexStmt.IndexType = "VECTOR"
			}
		}

		// 如果 USING 子句没有指定，检查索引名是否包含向量索引标记
		if !createIndexStmt.IsVectorIndex {
			if strings.Contains(strings.ToUpper(stmt.IndexName), "VECTOR") ||
				strings.Contains(strings.ToUpper(stmt.IndexName), "HNSW") ||
				strings.Contains(strings.ToUpper(stmt.IndexName), "IVF") ||
				strings.Contains(strings.ToUpper(stmt.IndexName), "FLAT") {
				createIndexStmt.IsVectorIndex = true
				createIndexStmt.VectorIndexType = "hnsw" // 默认
				createIndexStmt.IndexType = "VECTOR"
			}
		}
	}

	// 解析 WITH/COMMENT 子句中的参数
	if createIndexStmt.IsVectorIndex && stmt.IndexOption != nil && stmt.IndexOption.Comment != "" {
		// 解析 COMMENT 中的 JSON 参数，如 WITH (metric='cosine', dim=128) 或 JSON 格式
		params := parseWithClause(stmt.IndexOption.Comment)
		if params != nil {
			createIndexStmt.VectorParams = params

			// 从参数中提取度量类型
			if metric, ok := params["metric"].(string); ok {
				createIndexStmt.VectorMetric = metric
			} else {
				createIndexStmt.VectorMetric = "cosine" // 默认
			}

			// 从参数中提取维度（支持 int 和 float64）
			if dim, ok := params["dim"].(int); ok {
				createIndexStmt.VectorDim = dim
			} else if dim, ok := params["dim"].(float64); ok {
				createIndexStmt.VectorDim = int(dim)
			}
		}
	}

	// 获取列名（从 IndexPartSpecifications）- 支持复合索引
	if len(stmt.IndexPartSpecifications) > 0 {
		columns := make([]string, 0, len(stmt.IndexPartSpecifications))

		for _, spec := range stmt.IndexPartSpecifications {
			if spec.Column != nil {
				// 传统列索引
				columns = append(columns, spec.Column.Name.String())
			} else if spec.Expr != nil {
				// TiDB 向量索引语法：CREATE VECTOR INDEX idx ((VEC_COSINE_DISTANCE(embedding)))
				// 解析表达式提取列名和度量类型
				columnName, metric, err := extractVectorDistanceFunc(spec.Expr)
				if err != nil {
					return nil, fmt.Errorf("invalid vector index expression: %w", err)
				}
				columns = append(columns, columnName)
				// 如果之前没有设置度量类型，使用函数名推导的度量类型
				if createIndexStmt.VectorMetric == "" {
					createIndexStmt.VectorMetric = metric
				}
				// 标记为向量索引
				createIndexStmt.IsVectorIndex = true
				if createIndexStmt.IndexType != "VECTOR" {
					createIndexStmt.IndexType = "VECTOR"
				}
			} else {
				// 如果 Column 和 Expr 都为 nil，报错
				return nil, fmt.Errorf("invalid index specification: column is required")
			}
		}

		// 设置列名
		createIndexStmt.Columns = columns
	} else {
		// 如果没有 IndexPartSpecifications，返回错误
		return nil, fmt.Errorf("CREATE INDEX requires at least one column")
	}

	return createIndexStmt, nil
}

// extractFuncCallString extracts a string representation of a function call for ORDER BY
func extractFuncCallString(funcCall *ast.FuncCallExpr) string {
	var sb strings.Builder
	sb.WriteString(strings.ToLower(funcCall.FnName.String()))
	sb.WriteString("(")
	for i, arg := range funcCall.Args {
		if i > 0 {
			sb.WriteString(", ")
		}
		switch a := arg.(type) {
		case *ast.ColumnNameExpr:
			sb.WriteString(a.Name.Name.String())
		case ast.ValueExpr:
			sb.WriteString(fmt.Sprintf("'%v'", a.GetValue()))
		default:
			sb.WriteString(fmt.Sprintf("%v", arg))
		}
	}
	sb.WriteString(")")
	return sb.String()
}

// extractVectorDistanceFunc 从表达式中提取向量距离函数的信息
// 支持 TiDB 语法：VEC_COSINE_DISTANCE(embedding), VEC_L2_DISTANCE(embedding), VEC_INNER_PRODUCT(embedding)
func extractVectorDistanceFunc(expr ast.ExprNode) (columnName string, metric string, err error) {
	// 检查是否为函数调用表达式
	funcExpr, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return "", "", fmt.Errorf("not a function expression")
	}

	// 获取函数名
	funcName := strings.ToUpper(funcExpr.FnName.String())
	var metricType string

	// 解析距离度量类型
	switch funcName {
	case "VEC_COSINE_DISTANCE":
		metricType = "cosine"
	case "VEC_L2_DISTANCE":
		metricType = "l2"
	case "VEC_INNER_PRODUCT":
		metricType = "inner_product"
	default:
		return "", "", fmt.Errorf("unsupported vector distance function: %s", funcName)
	}

	// 提取列名（函数的第一个参数）
	if len(funcExpr.Args) == 0 {
		return "", "", fmt.Errorf("vector distance function requires column name argument")
	}

	// 第一个参数应该是列名
	colExpr, ok := funcExpr.Args[0].(*ast.ColumnNameExpr)
	if !ok {
		return "", "", fmt.Errorf("vector distance function argument must be a column name")
	}

	columnName = colExpr.Name.String()
	return columnName, metricType, nil
}

// parseWithClause 解析 WITH 子句中的参数
// 格式: "metric='cosine', dim=128, M=16, ef=200"
func parseWithClause(comment string) map[string]interface{} {
	params := make(map[string]interface{})

	// 清理字符串
	comment = strings.Trim(comment, " ")
	if comment == "" {
		return params
	}

	// 分割参数
	parts := strings.Split(comment, ",")
	for _, part := range parts {
		part = strings.Trim(part, " ")
		// 分割键值
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			key := strings.Trim(kv[0], " ")
			value := strings.Trim(kv[1], " ")

			// 移除引号
			value = strings.Trim(value, "'\"")

			// 尝试解析为数字
			if intVal, err := strconv.Atoi(value); err == nil {
				// 先尝试解析为整数
				params[key] = intVal
			} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
				// 尝试解析为浮点数
				params[key] = floatVal
			} else {
				// 作为字符串保存
				params[key] = value
			}
		}
	}

	return params
}

// convertDropIndexStmt 转换 DROP INDEX 语句
func (a *SQLAdapter) convertDropIndexStmt(stmt *ast.DropIndexStmt) (*DropIndexStatement, error) {
	dropIndexStmt := &DropIndexStatement{
		IndexName: stmt.IndexName,
		IfExists:  stmt.IfExists,
	}

	// 获取表名
	if stmt.Table != nil {
		dropIndexStmt.TableName = stmt.Table.Name.String()
	}

	return dropIndexStmt, nil
}

// convertCreateViewStmt 转换 CREATE VIEW 语句
func (a *SQLAdapter) convertCreateViewStmt(stmt *ast.CreateViewStmt) (*CreateViewStatement, error) {
	createViewStmt := &CreateViewStatement{
		OrReplace: stmt.OrReplace,
		Name:      stmt.ViewName.Name.String(),
	}

	// 解析 Algorithm - ViewAlgorithm has String() method, no Valid() needed
	createViewStmt.Algorithm = stmt.Algorithm.String()

	// 解析 Definer
	if stmt.Definer != nil {
		if stmt.Definer.CurrentUser {
			createViewStmt.Definer = "CURRENT_USER"
		} else {
			username := stmt.Definer.Username
			hostname := stmt.Definer.Hostname
			if hostname != "" {
				createViewStmt.Definer = fmt.Sprintf("'%s'@'%s'", username, hostname)
			} else {
				createViewStmt.Definer = fmt.Sprintf("'%s'", username)
			}
		}
	}

	// 解析 Security - ViewSecurity has String() method, no Valid() needed
	createViewStmt.Security = stmt.Security.String()

	// 解析列名列表
	if len(stmt.Cols) > 0 {
		createViewStmt.ColumnList = make([]string, 0, len(stmt.Cols))
		for _, col := range stmt.Cols {
			createViewStmt.ColumnList = append(createViewStmt.ColumnList, col.String())
		}
	}

	// 解析 SELECT 语句
	if stmt.Select != nil {
		selectAst, ok := stmt.Select.(*ast.SelectStmt)
		if !ok {
			return nil, fmt.Errorf("invalid SELECT statement in CREATE VIEW")
		}
		selectStmt, err := a.convertSelectStmt(selectAst)
		if err != nil {
			return nil, err
		}
		createViewStmt.Select = selectStmt
	}

	// 解析 CheckOption - ViewCheckOption has String() method, no Valid() needed
	// Note: TiDB uses Local and Cascaded, we convert to standard naming
	checkOpt := stmt.CheckOption.String()
	if checkOpt == "Local" {
		createViewStmt.CheckOption = "LOCAL"
	} else if checkOpt == "Cascaded" {
		createViewStmt.CheckOption = "CASCADED"
	} else {
		createViewStmt.CheckOption = "NONE"
	}

	return createViewStmt, nil
}

// convertDropViewStmt 转换 DROP VIEW 语句（基于 DropTableStmt）
func (a *SQLAdapter) convertDropViewStmt(stmt *ast.DropTableStmt) (*DropViewStatement, error) {
	dropViewStmt := &DropViewStatement{
		IfExists: stmt.IfExists,
	}

	// 解析视图名称列表
	if len(stmt.Tables) > 0 {
		dropViewStmt.Views = make([]string, 0, len(stmt.Tables))
		for _, table := range stmt.Tables {
			dropViewStmt.Views = append(dropViewStmt.Views, table.Name.String())
		}
	}

	// TiDB 的 DROP VIEW 不支持 CASCADE/RESTRICT 选项
	// 但保留字段以兼容其他数据库
	dropViewStmt.Restrict = false
	dropViewStmt.Cascade = false

	return dropViewStmt, nil
}

// convertSetStmt converts SET statement (SET NAMES, SET CHARACTER SET, etc.)
func (a *SQLAdapter) convertSetStmt(stmt *ast.SetStmt) *SetStatement {
	setStmt := &SetStatement{
		Variables: make(map[string]string),
	}

	for _, varAssign := range stmt.Variables {
		// Check for SET NAMES charset
		if strings.ToUpper(varAssign.Name) == "NAMES" {
			setStmt.Type = "NAMES"
			if varAssign.Value != nil {
				if val, err := a.extractValue(varAssign.Value); err == nil {
					if str, ok := val.(string); ok {
						setStmt.Value = str
					}
				}
			}
			return setStmt
		}

		// Check for SET CHARACTER SET charset
		if strings.ToUpper(varAssign.Name) == "CHARACTER SET" || strings.ToUpper(varAssign.Name) == "CHARSET" {
			setStmt.Type = "CHARACTER SET"
			if varAssign.Value != nil {
				if val, err := a.extractValue(varAssign.Value); err == nil {
					if str, ok := val.(string); ok {
						setStmt.Value = str
					}
				}
			}
			return setStmt
		}

		// Regular SET variable = value
		setStmt.Type = "VARIABLE"
		varName := varAssign.Name
		if varAssign.IsGlobal {
			varName = "GLOBAL " + varName
		} else if varAssign.IsSystem {
			varName = "SESSION " + varName
		}

		if varAssign.Value != nil {
			if val, err := a.extractValue(varAssign.Value); err == nil {
				switch v := val.(type) {
				case string:
					setStmt.Variables[varName] = v
				case int64:
					setStmt.Variables[varName] = fmt.Sprintf("%d", v)
				case float64:
					setStmt.Variables[varName] = fmt.Sprintf("%v", v)
				default:
					setStmt.Variables[varName] = fmt.Sprintf("%v", v)
				}
			}
		}
	}

	return setStmt
}
