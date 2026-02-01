package parser

import (
	"context"
	"fmt"
	"strings"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
)

// QueryBuilder 查询构建器
type QueryBuilder struct {
	dataSource domain.DataSource
}

// NewQueryBuilder 创建查询构建器
func NewQueryBuilder(dataSource domain.DataSource) *QueryBuilder {
	return &QueryBuilder{
		dataSource: dataSource,
	}
}

// BuildAndExecute 构建并执行 SQL 语句
func (b *QueryBuilder) BuildAndExecute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	adapter := NewSQLAdapter()
	result, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse SQL failed: %w", err)
	}

	if !result.Success {
		return nil, fmt.Errorf("parse failed: %s", result.Error)
	}

	return b.ExecuteStatement(ctx, result.Statement)
}

// ExecuteStatement 执行解析后的语句
func (b *QueryBuilder) ExecuteStatement(ctx context.Context, stmt *SQLStatement) (*domain.QueryResult, error) {
	switch stmt.Type {
	case SQLTypeSelect:
		return b.executeSelect(ctx, stmt.Select)
	case SQLTypeInsert:
		return b.executeInsert(ctx, stmt.Insert)
	case SQLTypeUpdate:
		return b.executeUpdate(ctx, stmt.Update)
	case SQLTypeDelete:
		return b.executeDelete(ctx, stmt.Delete)
	case SQLTypeCreate:
		// 优先处理 CREATE INDEX
		if stmt.CreateIndex != nil {
			return b.executeCreateIndex(ctx, stmt.CreateIndex)
		}
		return b.executeCreate(ctx, stmt.Create)
	case SQLTypeDrop:
		// 优先处理 DROP INDEX
		if stmt.DropIndex != nil {
			return b.executeDropIndex(ctx, stmt.DropIndex)
		}
		return b.executeDrop(ctx, stmt.Drop)
	case SQLTypeAlter:
		return b.executeAlter(ctx, stmt.Alter)
	default:
		return nil, fmt.Errorf("unsupported SQL type: %s", stmt.Type)
	}
}

// executeSelect 执行 SELECT
func (b *QueryBuilder) executeSelect(ctx context.Context, stmt *SelectStatement) (*domain.QueryResult, error) {
	// 构建 QueryOptions
	options := &domain.QueryOptions{}

	// 检查是否是 select *
	isSelectAll := false
	for _, col := range stmt.Columns {
		if col.IsWildcard {
			isSelectAll = true
			break
		}
	}
	options.SelectAll = isSelectAll

	// 处理 WHERE 条件
	if stmt.Where != nil {
		options.Filters = b.convertExpressionToFilters(stmt.Where)
	}

	// 处理 ORDER BY
	if len(stmt.OrderBy) > 0 {
		options.OrderBy = stmt.OrderBy[0].Column
		options.Order = stmt.OrderBy[0].Direction
	}

	// 处理 LIMIT
	if stmt.Limit != nil {
		options.Limit = int(*stmt.Limit)
	}

	// 处理 OFFSET
	if stmt.Offset != nil {
		options.Offset = int(*stmt.Offset)
	}

	// 执行查询
	result, err := b.dataSource.Query(ctx, stmt.From, options)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// 如果是 select *，需要确保返回的行数据不包含隐藏字段
	if isSelectAll {
		// 数据源层已经过滤了 _ttl 字段，这里再次确保
		// 构建新的行数据，只包含列定义中的字段
		filteredRows := make([]domain.Row, 0, len(result.Rows))
		for _, row := range result.Rows {
			filteredRow := make(domain.Row)
			for _, col := range result.Columns {
				if val, exists := row[col.Name]; exists {
					filteredRow[col.Name] = val
				}
			}
			filteredRows = append(filteredRows, filteredRow)
		}
		result.Rows = filteredRows
		return result, nil
	}

	// 如果不是 select *，则需要根据 SELECT 的列来过滤结果
	if len(stmt.Columns) > 0 {
		// 构建列名列表
		selectedColumns := make([]string, 0, len(stmt.Columns))
		for _, col := range stmt.Columns {
			// 跳过空列名和以 _ 开头的列名
			if len(col.Name) > 0 && col.Name[0] != '_' {
				selectedColumns = append(selectedColumns, col.Name)
			}
		}

		// 如果没有有效的列名，则使用数据源返回的列
		if len(selectedColumns) == 0 {
			return result, nil
		}

		// 构建新的列定义
		newColumns := make([]domain.ColumnInfo, 0, len(selectedColumns))
		for _, colName := range selectedColumns {
			// 查找对应的列定义
			found := false
			for _, col := range result.Columns {
				if col.Name == colName {
					newColumns = append(newColumns, col)
					found = true
					break
				}
			}
			// 如果没有找到列定义（比如 _ttl 这种隐藏字段），则创建一个基本的列定义
			if !found {
				newColumns = append(newColumns, domain.ColumnInfo{
					Name:     colName,
					Type:     "int64",
					Nullable: true,
					Primary:  false,
				})
			}
		}

		// 过滤行数据，只保留选择的列
		filteredRows := make([]domain.Row, 0, len(result.Rows))
		for _, row := range result.Rows {
			filteredRow := make(domain.Row)
			for _, colName := range selectedColumns {
				if val, exists := row[colName]; exists {
					filteredRow[colName] = val
				}
			}
			filteredRows = append(filteredRows, filteredRow)
		}

		// 更新结果
		result.Columns = newColumns
		result.Rows = filteredRows
	}

	// TODO: 处理 JOIN
	// TODO: 处理聚合函数
	// TODO: 处理 GROUP BY
	// TODO: 处理 HAVING

	return result, nil
}

// executeInsert 执行 INSERT
func (b *QueryBuilder) executeInsert(ctx context.Context, stmt *InsertStatement) (*domain.QueryResult, error) {
	// 检查数据源是否可写
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is read-only, INSERT operation not allowed")
	}

	// 获取表信息（用于过滤生成列）
	tableInfo, err := b.dataSource.GetTableInfo(ctx, stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	// 转换值为行数据，并过滤生成列
	rows := make([]domain.Row, 0, len(stmt.Values))
	for _, values := range stmt.Values {
		row := make(domain.Row)

		// 如果没有指定列名，使用表结构的列顺序
		columns := stmt.Columns
		if len(columns) == 0 {
			// 从表结构获取列名
			for _, col := range tableInfo.Columns {
				columns = append(columns, col.Name)
			}
		}

		for i, col := range columns {
			if i < len(values) {
				row[col] = values[i]
			}
		}
		// 过滤生成列（不允许显式插入）
		filteredRow := generated.FilterGeneratedColumns(row, tableInfo)
		rows = append(rows, filteredRow)
	}

	options := &domain.InsertOptions{
		Replace: false,
	}

	affected, err := b.dataSource.Insert(ctx, stmt.Table, rows, options)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	return &domain.QueryResult{
		Total: affected,
	}, nil
}

// executeUpdate 执行 UPDATE
func (b *QueryBuilder) executeUpdate(ctx context.Context, stmt *UpdateStatement) (*domain.QueryResult, error) {
	// 检查数据源是否可写
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is read-only, UPDATE operation not allowed")
	}

	// 获取表信息（用于过滤生成列）
	tableInfo, err := b.dataSource.GetTableInfo(ctx, stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	// 转换 WHERE 条件
	var filters []domain.Filter
	if stmt.Where != nil {
		filters = b.convertExpressionToFilters(stmt.Where)
	}

	// 转换更新数据，并过滤生成列
	updates := make(domain.Row)
	for col, val := range stmt.Set {
		updates[col] = val
	}
	// 过滤生成列（不允许显式更新）
	filteredUpdates := generated.FilterGeneratedColumns(updates, tableInfo)

	options := &domain.UpdateOptions{
		Upsert: false,
	}

	affected, err := b.dataSource.Update(ctx, stmt.Table, filters, filteredUpdates, options)
	if err != nil {
		return nil, fmt.Errorf("update failed: %w", err)
	}

	return &domain.QueryResult{
		Total: affected,
	}, nil
}

// executeDelete 执行 DELETE
func (b *QueryBuilder) executeDelete(ctx context.Context, stmt *DeleteStatement) (*domain.QueryResult, error) {
	// 检查数据源是否可写
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is read-only, DELETE operation not allowed")
	}

	// 转换 WHERE 条件
	var filters []domain.Filter
	if stmt.Where != nil {
		filters = b.convertExpressionToFilters(stmt.Where)
	}

	options := &domain.DeleteOptions{
		Force: false,
	}

	affected, err := b.dataSource.Delete(ctx, stmt.Table, filters, options)
	if err != nil {
		return nil, fmt.Errorf("delete failed: %w", err)
	}

	return &domain.QueryResult{
		Total: affected,
	}, nil
}

// executeCreate 执行 CREATE
func (b *QueryBuilder) executeCreate(ctx context.Context, stmt *CreateStatement) (*domain.QueryResult, error) {
	// 检查数据源是否可写
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is read-only, CREATE operation not allowed")
	}

	if stmt.Type == "TABLE" {
		tableInfo := &domain.TableInfo{
			Name:    stmt.Name,
			Columns: make([]domain.ColumnInfo, 0, len(stmt.Columns)),
		}

		for _, col := range stmt.Columns {
			tableInfo.Columns = append(tableInfo.Columns, domain.ColumnInfo{
				Name:         col.Name,
				Type:         col.Type,
				Nullable:     col.Nullable,
				Primary:      col.Primary,
				Default:      fmt.Sprintf("%v", col.Default),
				Unique:       col.Unique,
				AutoIncrement: col.AutoInc,
				// Generated column support
				IsGenerated:      col.IsGenerated,
				GeneratedType:    col.GeneratedType,
				GeneratedExpr:    col.GeneratedExpr,
				GeneratedDepends: col.GeneratedDepends,
			})
		}

		err := b.dataSource.CreateTable(ctx, tableInfo)
		if err != nil {
			return nil, fmt.Errorf("create table failed: %w", err)
		}

		return &domain.QueryResult{
			Total: 0,
		}, nil
	}

	return nil, fmt.Errorf("unsupported create type: %s", stmt.Type)
}

// executeDrop 执行 DROP
func (b *QueryBuilder) executeDrop(ctx context.Context, stmt *DropStatement) (*domain.QueryResult, error) {
	// 检查数据源是否可写
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is read-only, DROP operation not allowed")
	}

	if stmt.Type == "TABLE" {
		err := b.dataSource.DropTable(ctx, stmt.Name)
		if err != nil {
			return nil, fmt.Errorf("drop table failed: %w", err)
		}

		return &domain.QueryResult{
			Total: 0,
		}, nil
	}

	return nil, fmt.Errorf("unsupported drop type: %s", stmt.Type)
}

// executeAlter 执行 ALTER
func (b *QueryBuilder) executeAlter(ctx context.Context, stmt *AlterStatement) (*domain.QueryResult, error) {
	return nil, fmt.Errorf("ALTER TABLE is not currently supported")
}

// executeCreateIndex 执行 CREATE INDEX
func (b *QueryBuilder) executeCreateIndex(ctx context.Context, stmt *CreateIndexStatement) (*domain.QueryResult, error) {
	// 检查数据源是否可写
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is read-only, CREATE INDEX operation not allowed")
	}

	// 检查数据源是否支持索引操作
	indexManager, ok := b.dataSource.(interface {
		CreateIndex(tableName, columnName string, indexType string, unique bool) error
	})
	if !ok {
		return nil, fmt.Errorf("data source does not support CREATE INDEX")
	}

	// 转换索引类型
	var idxType string
	switch strings.ToUpper(stmt.IndexType) {
	case "BTREE", "B-TREE":
		idxType = "btree"
	case "HASH":
		idxType = "hash"
	case "FULLTEXT", "FULL-TEXT":
		idxType = "fulltext"
	default:
		idxType = "btree" // 默认使用 btree
	}

	// 调用数据源的 CreateIndex 方法
	err := indexManager.CreateIndex(stmt.TableName, stmt.ColumnName, idxType, stmt.Unique)
	if err != nil {
		return nil, fmt.Errorf("create index failed: %w", err)
	}

	return &domain.QueryResult{
		Total: 0,
	}, nil
}

// executeDropIndex 执行 DROP INDEX
func (b *QueryBuilder) executeDropIndex(ctx context.Context, stmt *DropIndexStatement) (*domain.QueryResult, error) {
	// 检查数据源是否可写
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is read-only, DROP INDEX operation not allowed")
	}

	// 检查数据源是否支持索引操作
	indexManager, ok := b.dataSource.(interface {
		DropIndex(tableName, indexName string) error
	})
	if !ok {
		return nil, fmt.Errorf("data source does not support DROP INDEX")
	}

	// 调用数据源的 DropIndex 方法
	err := indexManager.DropIndex(stmt.TableName, stmt.IndexName)
	if err != nil {
		return nil, fmt.Errorf("drop index failed: %w", err)
	}

	return &domain.QueryResult{
		Total: 0,
	}, nil
}

// convertExpressionToFilters 将表达式转换为过滤器列表
func (b *QueryBuilder) convertExpressionToFilters(expr *Expression) []domain.Filter {
	return b.convertExpressionToFiltersInternal(expr, false)
}

// convertExpressionToFiltersInternal 内部递归函数
func (b *QueryBuilder) convertExpressionToFiltersInternal(expr *Expression, isInOr bool) []domain.Filter {
	filters := make([]domain.Filter, 0)

	if expr == nil {
		return filters
	}

	switch expr.Type {
	case ExprTypeOperator:
		if expr.Left != nil && expr.Right != nil {
			if expr.Operator == "and" || expr.Operator == "or" {
				leftFilters := b.convertExpressionToFiltersInternal(expr.Left, expr.Operator == "or")
				rightFilters := b.convertExpressionToFiltersInternal(expr.Right, expr.Operator == "or")

				if len(leftFilters) > 0 || len(rightFilters) > 0 {
					logicOp := strings.ToUpper(expr.Operator)
					filters = append(filters, domain.Filter{
						LogicOp:    logicOp,
						SubFilters: append(leftFilters, rightFilters...),
					})
				}
				return filters
			}

			if expr.Left.Type == ExprTypeColumn && expr.Right.Type == ExprTypeValue {
				operator := b.convertOperator(expr.Operator)
				value := b.convertValue(expr.Right.Value)
				filters = append(filters, domain.Filter{
					Field:    expr.Left.Column,
					Operator: operator,
					Value:    value,
				})
				return filters
			}

			// 处理 BETWEEN 操作符
			if expr.Left.Type == ExprTypeColumn && expr.Right.Type == ExprTypeValue {
				if expr.Operator == "BETWEEN" || expr.Operator == "NOT BETWEEN" {
					// expr.Right.Value 应该是 [min, max] 的数组
					if valueSlice, ok := expr.Right.Value.([]interface{}); ok && len(valueSlice) >= 2 {
						// 获取左边界和右边界的值
						minValue := b.extractExpressionValue(valueSlice[0])
						maxValue := b.extractExpressionValue(valueSlice[1])
						if minValue != nil && maxValue != nil {
							operator := expr.Operator
							filters = append(filters, domain.Filter{
								Field:    expr.Left.Column,
								Operator: operator,
								Value:    []interface{}{minValue, maxValue},
							})
							return filters
						}
					}
				}
			}
		}

	case ExprTypeFunction:
		if expr.Function == "in" {
			if len(expr.Args) > 1 {
				if expr.Args[0].Type == ExprTypeColumn {
					values := make([]interface{}, 0)
					for i := 1; i < len(expr.Args); i++ {
						if expr.Args[i].Type == ExprTypeValue {
							values = append(values, b.convertValue(expr.Args[i].Value))
						}
					}
					filters = append(filters, domain.Filter{
						Field:    expr.Args[0].Column,
						Operator: "IN",
						Value:    values,
					})
					return filters
				}
			}
		}

	case ExprTypeColumn:
		if expr.Type == ExprTypeColumn {
			filters = append(filters, domain.Filter{
				Field:    expr.Column,
				Operator: "=",
				Value:    true,
			})
			return filters
		}
	}

	return filters
}

// convertOperator 转换操作符
func (b *QueryBuilder) convertOperator(op string) string {
	switch op {
	case "==":
		return "="
	case "!=":
		return "!="
	case "eq", "EQ": // TiDB Parser使用小写"eq"
		return "="
	case "ne", "NE": // TiDB Parser使用小写"ne"
		return "!="
	case ">", "gt", "GT":
		return ">"
	case "<", "lt", "LT":
		return "<"
	case ">=", "ge", "GE":
		return ">="
	case "<=", "le", "LE":
		return "<="
	case "like", "LIKE":
		return "LIKE"
	case "in", "IN":
		return "IN"
	case "between", "BETWEEN":
		return "BETWEEN"
	case "and", "AND": // TiDB Parser使用小写"and"
		return "AND"
	case "or", "OR": // TiDB Parser使用小写"or"
		return "OR"
	default:
		return op
	}
}

// convertValue 转换值，进行类型验证和转换
func (b *QueryBuilder) convertValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case int:
		// 显式处理int类型
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		return v
	case bool:
		return v
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = b.convertValue(item)
		}
		return result
	default:
		return fmt.Sprintf("%v", v)
	}
}

// extractExpressionValue 从表达式或值中提取实际值
func (b *QueryBuilder) extractExpressionValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	// 如果是 Expression 类型，提取其值
	if expr, ok := val.(*Expression); ok && expr != nil {
		if expr.Type == ExprTypeValue {
			return expr.Value
		}
		// 递归处理嵌套表达式
		if expr.Left != nil {
			return b.extractExpressionValue(expr.Left)
		}
	}

	return b.convertValue(val)
}
