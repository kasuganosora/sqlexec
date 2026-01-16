package datasource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// Parser SQL解析器
type Parser struct {
	functionManager *FunctionManager
}

// NewParser 创建解析器
func NewParser(functionManager *FunctionManager) *Parser {
	return &Parser{
		functionManager: functionManager,
	}
}

// Parse 解析SQL语句
func (p *Parser) Parse(sql string) (*Query, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("解析SQL失败: %v", err)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return p.parseSelect(stmt)
	case *sqlparser.Show:
		return p.parseShow(stmt)
	default:
		return nil, fmt.Errorf("不支持的SQL语句类型")
	}
}

// parseSelect 解析SELECT语句
func (p *Parser) parseSelect(stmt *sqlparser.Select) (*Query, error) {
	query := &Query{
		Type: QueryTypeSelect,
	}

	// 解析表名
	if len(stmt.From) == 0 {
		return nil, fmt.Errorf("缺少FROM子句")
	}
	tableExpr := stmt.From[0]
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch expr := table.Expr.(type) {
		case sqlparser.TableName:
			query.Table = expr.Name.String()
		}
	}

	// 解析字段
	for _, expr := range stmt.SelectExprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("不支持的表达式类型: %T", expr)
		}
		colName, ok := aliasedExpr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("不支持的表达式类型: %T", aliasedExpr.Expr)
		}
		query.Fields = append(query.Fields, colName.Name.String())
	}

	// 解析JOIN
	for i := 1; i < len(stmt.From); i++ {
		join, ok := stmt.From[i].(*sqlparser.JoinTableExpr)
		if !ok {
			continue
		}
		joinInfo := Join{}
		switch join.Join {
		case "join":
			joinInfo.Type = JoinTypeInner
		case "left join":
			joinInfo.Type = JoinTypeLeft
		case "right join":
			joinInfo.Type = JoinTypeRight
		default:
			return nil, fmt.Errorf("不支持的JOIN类型: %v", join.Join)
		}
		// 解析JOIN的表名
		if at, ok := join.RightExpr.(*sqlparser.AliasedTableExpr); ok {
			switch expr := at.Expr.(type) {
			case sqlparser.TableName:
				joinInfo.Table = expr.Name.String()
			}
		}
		// 解析JOIN条件
		if join.On != nil {
			joinInfo.Condition = sqlparser.String(join.On)
		}
		query.Joins = append(query.Joins, joinInfo)
	}

	// 解析WHERE
	if stmt.Where != nil {
		conditions, err := p.parseWhere(stmt.Where)
		if err != nil {
			return nil, err
		}
		query.Where = conditions
	}

	// 解析GROUP BY
	for _, expr := range stmt.GroupBy {
		colName, ok := expr.(*sqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("不支持的表达式类型: %T", expr)
		}
		query.GroupBy = append(query.GroupBy, colName.Name.String())
	}

	// 解析HAVING
	if stmt.Having != nil {
		fmt.Printf("[DEBUG] HAVING表达式类型: %T\n", stmt.Having.Expr)
		fmt.Printf("[DEBUG] HAVING表达式内容: %+v\n", stmt.Having.Expr)
		fmt.Printf("[DEBUG] HAVING表达式字符串: %s\n", sqlparser.String(stmt.Having.Expr))
		having, err := p.parseWhere(&sqlparser.Where{Expr: stmt.Having.Expr})
		if err != nil {
			return nil, fmt.Errorf("解析HAVING条件失败: %v", err)
		}
		query.Having = having
	}

	// 解析 ORDER BY 子句
	for _, ob := range stmt.OrderBy {
		col, ok := ob.Expr.(*sqlparser.ColName)
		if !ok {
			continue
		}
		field := col.Name.String()
		direction := "ASC"
		if ob.Direction == "desc" || ob.Direction == "DESC" {
			direction = "DESC"
		}
		query.OrderBy = append(query.OrderBy, OrderBy{
			Field:     field,
			Direction: direction,
		})
	}

	// 解析LIMIT
	if stmt.Limit != nil {
		if stmt.Limit.Offset != nil {
			if offset, ok := stmt.Limit.Offset.(*sqlparser.SQLVal); ok {
				val := string(offset.Val)
				query.Offset = parseInt(val)
			}
		}
		if stmt.Limit.Rowcount != nil {
			if limit, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok {
				val := string(limit.Val)
				query.Limit = parseInt(val)
			}
		}
	}

	return query, nil
}

// parseInt 辅助函数
func parseInt(val string) int {
	var i int
	fmt.Sscanf(val, "%d", &i)
	return i
}

// parseWhere 解析WHERE条件
func (p *Parser) parseWhere(where *sqlparser.Where) ([]Condition, error) {
	var conditions []Condition

	switch expr := where.Expr.(type) {
	case *sqlparser.ComparisonExpr:
		fmt.Printf("[DEBUG] ComparisonExpr左操作数类型: %T\n", expr.Left)
		fmt.Printf("[DEBUG] ComparisonExpr右操作数类型: %T\n", expr.Right)
		fmt.Printf("[DEBUG] ComparisonExpr左操作数内容: %+v\n", expr.Left)
		fmt.Printf("[DEBUG] ComparisonExpr右操作数内容: %+v\n", expr.Right)
		condition := Condition{}
		switch left := expr.Left.(type) {
		case *sqlparser.ColName:
			condition.Field = left.Name.String()
		case *sqlparser.FuncExpr:
			funcName := left.Name.String()
			args := make([]string, 0)
			for _, arg := range left.Exprs {
				aliased, ok := arg.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, fmt.Errorf("不支持的函数参数类型: %T", arg)
				}
				switch ex := aliased.Expr.(type) {
				case *sqlparser.ColName:
					args = append(args, ex.Name.String())
				case *sqlparser.SQLVal:
					args = append(args, string(ex.Val))
				case *sqlparser.FuncExpr:
					args = append(args, sqlparser.String(ex))
				default:
					return nil, fmt.Errorf("不支持的表达式类型: %T", ex)
				}
			}
			condition.Field = fmt.Sprintf("%s(%s)", funcName, strings.Join(args, ","))
		}
		condition.Operator = string(expr.Operator)
		switch right := expr.Right.(type) {
		case *sqlparser.SQLVal:
			switch right.Type {
			case sqlparser.IntVal:
				val, _ := strconv.ParseInt(string(right.Val), 10, 64)
				condition.Value = val
			case sqlparser.FloatVal:
				val, _ := strconv.ParseFloat(string(right.Val), 64)
				condition.Value = val
			case sqlparser.StrVal:
				condition.Value = string(right.Val)
			default:
				condition.Value = string(right.Val)
			}
		case *sqlparser.ColName:
			condition.Value = right.Name.String()
		case *sqlparser.FuncExpr:
			condition.Value = sqlparser.String(right)
		}
		conditions = append(conditions, condition)
	case *sqlparser.AndExpr:
		leftConditions, err := p.parseWhere(&sqlparser.Where{Expr: expr.Left})
		if err != nil {
			return nil, err
		}
		rightConditions, err := p.parseWhere(&sqlparser.Where{Expr: expr.Right})
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, leftConditions...)
		conditions = append(conditions, rightConditions...)
	case *sqlparser.OrExpr:
		leftConditions, err := p.parseWhere(&sqlparser.Where{Expr: expr.Left})
		if err != nil {
			return nil, err
		}
		rightConditions, err := p.parseWhere(&sqlparser.Where{Expr: expr.Right})
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, leftConditions...)
		conditions = append(conditions, rightConditions...)
	case *sqlparser.FuncExpr:
		fmt.Printf("[DEBUG] FuncExpr类型: %T\n", expr)
		fmt.Printf("[DEBUG] FuncExpr内容: %+v\n", expr)
		fmt.Printf("[DEBUG] FuncExpr字符串: %s\n", sqlparser.String(expr))
		// 处理聚合函数
		if len(expr.Exprs) == 1 {
			// 获取函数参数
			arg := expr.Exprs[0]
			switch arg := arg.(type) {
			case *sqlparser.AliasedExpr:
				if colName, ok := arg.Expr.(*sqlparser.ColName); ok {
					return []Condition{{
						Field:    colName.Name.String(),
						Operator: expr.Name.String(),
						Value:    nil,
					}}, nil
				}
				return nil, fmt.Errorf("不支持的函数参数类型: %T", arg.Expr)
			default:
				return nil, fmt.Errorf("不支持的函数参数类型: %T", arg)
			}
		}
		return nil, fmt.Errorf("不支持的函数表达式: %s", sqlparser.String(expr))
	default:
		typeName := reflect.TypeOf(expr).String()
		fmt.Printf("[DEBUG] parseWhere default分支，表达式类型: %s\n", typeName)
		if typeName == "*sqlparser.AliasedExpr" {
			val := reflect.ValueOf(expr)
			field := val.Elem().FieldByName("Expr")
			if field.IsValid() && field.CanInterface() {
				fmt.Printf("[DEBUG] AliasedExpr.Expr 字段类型: %T\n", field.Interface())
				nextExpr, ok := field.Interface().(sqlparser.Expr)
				if ok {
					fmt.Printf("[DEBUG] 反射递归处理AliasedExpr: %T\n", nextExpr)
					return p.parseWhere(&sqlparser.Where{Expr: nextExpr})
				}
			}
		}
		return nil, fmt.Errorf("暂不支持的WHERE条件类型: %T", expr)
	}
	return conditions, nil
}

// parseShow 解析SHOW语句
func (p *Parser) parseShow(stmt *sqlparser.Show) (*Query, error) {
	query := &Query{
		Type: QueryTypeShow,
	}

	showType := sqlparser.String(stmt)
	switch {
	case strings.Contains(showType, "SHOW TABLES"):
		query.Table = "tables"
	case strings.Contains(showType, "SHOW DATABASES"):
		query.Table = "databases"
	case strings.Contains(showType, "SHOW COLUMNS"):
		query.Table = strings.TrimPrefix(showType, "SHOW COLUMNS FROM ")
	case strings.Contains(showType, "SHOW INDEX"):
		query.Table = strings.TrimPrefix(showType, "SHOW INDEX FROM ")
	case strings.Contains(showType, "SHOW STATUS"):
		query.Table = "status"
	case strings.Contains(showType, "SHOW VARIABLES"):
		query.Table = "variables"
		if strings.Contains(showType, "LIKE") {
			parts := strings.Split(showType, "LIKE")
			if len(parts) > 1 {
				query.Where = []Condition{{
					Field:    "Variable_name",
					Operator: "LIKE",
					Value:    strings.TrimSpace(parts[1]),
				}}
			}
		}
	default:
		return nil, fmt.Errorf("不支持的SHOW类型: %s", showType)
	}

	return query, nil
}
