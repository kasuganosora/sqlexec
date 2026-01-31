package parser

import (
	"fmt"
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
	stmtNodes, _, err := a.parser.Parse(sql, "", "")
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
		stmt.Type = SQLTypeDrop
		dropStmt, err := a.convertDropTableStmt(stmtNode)
		if err != nil {
			return nil, err
		}
		stmt.Drop = dropStmt

	case *ast.TruncateTableStmt:
		stmt.Type = SQLTypeDrop
		stmt.Drop = &DropStatement{
			Type:     "TABLE",
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
			if col, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				selectStmt.OrderBy = append(selectStmt.OrderBy, OrderByItem{
					Column:    col.Name.Name.String(),
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

	// 解析列定义
	for _, col := range stmt.Cols {
		colInfo := ColumnInfo{
			Name:     col.Name.Name.String(),
			Type:     simplifyTypeName(col.Tp.String()),
			Nullable: true, // 默认可空
			Default:  nil,
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

	return createStmt, nil
}

// convertDropTableStmt 转换 DROP TABLE 语句
func (a *SQLAdapter) convertDropTableStmt(stmt *ast.DropTableStmt) (*DropStatement, error) {
	return &DropStatement{
		Type:     "TABLE",
		Name:     stmt.Tables[0].Name.String(),
		IfExists: stmt.IfExists,
	}, nil
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
		col.Table = expr.Name.Schema.String()
		col.IsWildcard = expr.Name.Name.String() == "*"
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
		if n.Name.Schema.L != "" {
			expr.Column = n.Name.Schema.String() + "." + expr.Column
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
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
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
	default:
		showStmt.Type = "UNKNOWN"
	}

	// 处理 LIKE 子句
	if stmt.Pattern != nil {
		showStmt.Like = stmt.Pattern.OriginalText()
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

	// 获取索引类型（BTREE, HASH, FULLTEXT）
	// 默认为 BTREE
	if stmt.IndexOption != nil {
		createIndexStmt.IndexType = "BTREE" // 默认值
		// 注意：TiDB 的 IndexOption 可能包含索引类型信息
		// 这里简化处理，实际可能需要更复杂的解析
	} else {
		createIndexStmt.IndexType = "BTREE"
	}

	// 获取列名（从 IndexPartSpecifications）
	if len(stmt.IndexPartSpecifications) > 0 {
		// 获取第一个列名（简化处理，不支持多列索引）
		spec := stmt.IndexPartSpecifications[0]
		if spec.Column != nil {
			createIndexStmt.ColumnName = spec.Column.Name.String()
		} else {
			// 如果 Column 为 nil，可能是表达式索引或其他情况
			return nil, fmt.Errorf("invalid index specification: column is required")
		}
	} else {
		// 如果没有 IndexPartSpecifications，返回错误
		return nil, fmt.Errorf("CREATE INDEX requires at least one column")
	}

	return createIndexStmt, nil
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
