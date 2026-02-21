package parser

import (
	"context"
	"fmt"
	"strings"
	"time"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
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
	case SQLTypeCreateView:
		return b.executeCreateView(ctx, stmt.CreateView)
	case SQLTypeDropView:
		return b.executeDropView(ctx, stmt.DropView)
	// Note: SQLTypeAlterView is not supported by TiDB
	default:
		return nil, fmt.Errorf("unsupported SQL type: %s", stmt.Type)
	}
}

// executeSelect 执行 SELECT
func (b *QueryBuilder) executeSelect(ctx context.Context, stmt *SelectStatement) (*domain.QueryResult, error) {
	// 构建 QueryOptions
	options := &domain.QueryOptions{}

	// 从上下文中获取用户信息（用于权限检查）
	if user, ok := ctx.Value("user").(string); ok {
		options.User = user
	}

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
			// 跳过空列名
			if len(col.Name) > 0 {
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

		// Check if table is a view and validate with CHECK OPTION
		if viewInfo, isView := b.getViewInfo(tableInfo); isView {
			validator := NewCheckOptionValidator(viewInfo)
			if err := validator.ValidateInsert(filteredRow); err != nil {
				return nil, fmt.Errorf("view check option failed: %w", err)
			}
		}

		rows = append(rows, filteredRow)
	}

	options := &domain.InsertOptions{
		Replace: false,
	}

	affected, err := b.dataSource.Insert(ctx, stmt.Table, rows, options)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	// Get last insert ID from auto-increment column
	var lastInsertID int64
	tableInfo2, _ := b.dataSource.GetTableInfo(ctx, stmt.Table)
	if tableInfo2 != nil {
		for _, col := range tableInfo2.Columns {
			if col.AutoIncrement {
				// Find the last inserted row's ID
				if len(rows) > 0 {
					if val, ok := rows[len(rows)-1][col.Name]; ok {
						switch v := val.(type) {
						case int64:
							lastInsertID = v
						case int:
							lastInsertID = int64(v)
						case float64:
							lastInsertID = int64(v)
						}
					}
				}
				break
			}
		}
	}

	return &domain.QueryResult{
		Total: affected,
		Rows: []domain.Row{
			{"rows_affected": affected, "last_insert_id": lastInsertID},
		},
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

	// Check if table is a view and validate with CHECK OPTION
	if viewInfo, isView := b.getViewInfo(tableInfo); isView {
		validator := NewCheckOptionValidator(viewInfo)

		// Get rows that would be updated to validate them
		queryResult, err := b.dataSource.Query(ctx, stmt.Table, &domain.QueryOptions{
			Filters: b.convertExpressionToFilters(stmt.Where),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query rows for check option validation: %w", err)
		}

		// Validate each row would still satisfy view definition after update
		for _, row := range queryResult.Rows {
			if err := validator.ValidateUpdate(row, filteredUpdates); err != nil {
				return nil, fmt.Errorf("view check option failed: %w", err)
			}
		}
	}

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

		// Handle PERSISTENT option for hybrid data source
		if stmt.Persistent {
			// Check if data source supports EnablePersistence (HybridDataSource)
			type persistenceEnabler interface {
				EnablePersistence(ctx context.Context, tableName string) error
			}
			if pe, ok := b.dataSource.(persistenceEnabler); ok {
				if err := pe.EnablePersistence(ctx, stmt.Name); err != nil {
					return nil, fmt.Errorf("failed to enable persistence: %w", err)
				}
			}
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
		// Support multiple tables: DROP TABLE t1, t2, t3
		tables := stmt.Names
		if len(tables) == 0 && stmt.Name != "" {
			tables = []string{stmt.Name}
		}

		for _, tableName := range tables {
			err := b.dataSource.DropTable(ctx, tableName)
			if err != nil {
				// If IF EXISTS, continue on error
				if !stmt.IfExists {
					return nil, fmt.Errorf("drop table '%s' failed: %w", tableName, err)
				}
			}
		}

		return &domain.QueryResult{
			Total: int64(len(tables)),
		}, nil
	}

	if stmt.Type == "TRUNCATE" {
		tableName := stmt.Name
		if tableName == "" {
			return nil, fmt.Errorf("TRUNCATE TABLE requires a table name")
		}
		err := b.dataSource.TruncateTable(ctx, tableName)
		if err != nil {
			return nil, fmt.Errorf("truncate table '%s' failed: %w", tableName, err)
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

	// 如果是向量索引，使用专门的创建方法
	if stmt.IsVectorIndex {
		return b.executeCreateVectorIndex(ctx, stmt)
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

	// 使用支持多列索引的接口
	indexManager, ok := b.dataSource.(interface {
		CreateIndexWithColumns(tableName string, columnNames []string, indexType string, unique bool) error
	})
	if !ok {
		return nil, fmt.Errorf("data source does not support CREATE INDEX")
	}

	err := indexManager.CreateIndexWithColumns(stmt.TableName, stmt.Columns, idxType, stmt.Unique)
	if err != nil {
		return nil, fmt.Errorf("create index failed: %w", err)
	}
	return &domain.QueryResult{Total: 0}, nil
}

// executeCreateVectorIndex 执行 CREATE VECTOR INDEX
func (b *QueryBuilder) executeCreateVectorIndex(ctx context.Context, stmt *CreateIndexStatement) (*domain.QueryResult, error) {
	// 检查数据源是否支持向量索引操作
	vectorIndexManager, ok := b.dataSource.(interface {
		CreateVectorIndex(tableName, columnName string, metricType string, indexType string, dimension int, params map[string]interface{}) error
	})
	if !ok {
		// 尝试使用 memory.IndexManager 接口
		indexManager, ok := b.dataSource.(interface {
			CreateVectorIndex(tableName, columnName string, metricType memory.VectorMetricType, indexType memory.IndexType, dimension int, params map[string]interface{}) (memory.VectorIndex, error)
		})
		if !ok {
			return nil, fmt.Errorf("data source does not support CREATE VECTOR INDEX")
		}
		
		// 转换参数
		metricType := convertToVectorMetricType(stmt.VectorMetric)
		indexType := convertToVectorIndexType(stmt.VectorIndexType)
		dimension := stmt.VectorDim
		
		// 合并参数
		params := make(map[string]interface{})
		if stmt.VectorParams != nil {
			for k, v := range stmt.VectorParams {
				params[k] = v
			}
		}
		
		// 调用向量索引创建方法
		if len(stmt.Columns) == 0 {
			return nil, fmt.Errorf("vector index requires at least one column")
		}
		_, err := indexManager.CreateVectorIndex(stmt.TableName, stmt.Columns[0], metricType, indexType, dimension, params)
		if err != nil {
			return nil, fmt.Errorf("create vector index failed: %w", err)
		}

		return &domain.QueryResult{
			Total: 0,
		}, nil
	}

	// 调用数据源的 CreateVectorIndex 方法
	if len(stmt.Columns) == 0 {
		return nil, fmt.Errorf("vector index requires at least one column")
	}
	err := vectorIndexManager.CreateVectorIndex(
		stmt.TableName,
		stmt.Columns[0],
		stmt.VectorMetric,
		stmt.VectorIndexType,
		stmt.VectorDim,
		stmt.VectorParams,
	)
	if err != nil {
		return nil, fmt.Errorf("create vector index failed: %w", err)
	}

	return &domain.QueryResult{
		Total: 0,
	}, nil
}

// convertToVectorMetricType 转换度量类型字符串为枚举值
func convertToVectorMetricType(metric string) memory.VectorMetricType {
	switch strings.ToLower(metric) {
	case "l2", "euclidean":
		return memory.VectorMetricL2
	case "ip", "inner_product", "inner":
		return memory.VectorMetricIP
	default:
		return memory.VectorMetricCosine
	}
}

// convertToVectorIndexType 转换索引类型字符串为枚举值
func convertToVectorIndexType(indexType string) memory.IndexType {
	switch strings.ToLower(indexType) {
	case "flat", "vector_flat":
		return memory.IndexTypeVectorFlat
	case "hnsw", "vector_hnsw":
		return memory.IndexTypeVectorHNSW
	case "ivf_flat", "vector_ivf_flat":
		return memory.IndexTypeVectorIVFFlat
	case "ivf_sq8", "vector_ivf_sq8":
		return memory.IndexTypeVectorIVFSQ8
	case "ivf_pq", "vector_ivf_pq":
		return memory.IndexTypeVectorIVFPQ
	case "hnsw_sq", "vector_hnsw_sq":
		return memory.IndexTypeVectorHNSWSQ
	case "hnsw_pq", "vector_hnsw_pq":
		return memory.IndexTypeVectorHNSWPQ
	case "ivf_rabitq", "vector_ivf_rabitq":
		return memory.IndexTypeVectorIVFRabitQ
	case "hnsw_prq", "vector_hnsw_prq":
		return memory.IndexTypeVectorHNSWPRQ
	case "aisaq", "vector_aisaq":
		return memory.IndexTypeVectorAISAQ
	default:
		return memory.IndexTypeVectorHNSW
	}
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
		// 处理 IS NULL / IS NOT NULL（一元运算符，只有 Left，没有 Right）
		if expr.Left != nil && expr.Right == nil {
			op := strings.ToUpper(expr.Operator)
			if (op == "IS NULL" || op == "ISNULL") && expr.Left.Type == ExprTypeColumn {
				filters = append(filters, domain.Filter{
					Field:    expr.Left.Column,
					Operator: "IS NULL",
					Value:    nil,
				})
				return filters
			}
			if (op == "IS NOT NULL" || op == "ISNOTNULL") && expr.Left.Type == ExprTypeColumn {
				filters = append(filters, domain.Filter{
					Field:    expr.Left.Column,
					Operator: "IS NOT NULL",
					Value:    nil,
				})
				return filters
			}
		}

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
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999999")
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

// getViewInfo checks if a table is a view and returns its metadata
func (b *QueryBuilder) getViewInfo(tableInfo *domain.TableInfo) (*domain.ViewInfo, bool) {
	if tableInfo.Atts == nil {
		return nil, false
	}

	viewMeta, exists := tableInfo.Atts[domain.ViewMetaKey]
	if !exists {
		return nil, false
	}

	viewInfo, ok := viewMeta.(domain.ViewInfo)
	if !ok {
		return nil, false
	}

	return &viewInfo, true
}

// executeCreateView 执行 CREATE VIEW
func (b *QueryBuilder) executeCreateView(ctx context.Context, stmt *CreateViewStatement) (*domain.QueryResult, error) {
	// Check if data source is writable
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is not writable")
	}

	// Build view metadata
	viewInfo := domain.ViewInfo{
		Algorithm:   parseViewAlgorithm(stmt.Algorithm),
		Definer:     stmt.Definer,
		Security:    parseViewSecurity(stmt.Security),
		SelectStmt:  "", // Will be set from SELECT
		CheckOption: parseViewCheckOption(stmt.CheckOption),
		Cols:        stmt.ColumnList,
		Updatable:   true, // Will be recalculated
	}

	// Serialize SELECT statement to string
	if stmt.Select != nil {
		// Reconstruct SELECT SQL from parsed statement
		viewInfo.SelectStmt = b.buildSelectSQL(stmt.Select)
	}

	// Create table info for the view
	tableInfo := domain.TableInfo{
		Name:      stmt.Name,
		Schema:    "", // Use default schema
		Columns:   []domain.ColumnInfo{},
		Atts:      map[string]interface{}{
			domain.ViewMetaKey: viewInfo,
		},
	}

	// Determine column definitions from SELECT
	if stmt.Select != nil && len(stmt.Select.Columns) > 0 {
		tableInfo.Columns = make([]domain.ColumnInfo, 0, len(stmt.Select.Columns))
		for _, col := range stmt.Select.Columns {
			colType := "text"
			colName := col.Name
			if colName == "" && col.Alias != "" {
				colName = col.Alias
			}

			tableInfo.Columns = append(tableInfo.Columns, domain.ColumnInfo{
				Name:     colName,
				Type:     colType,
				Nullable: true,
			})
		}
	}

	// Create the view (as a table in the data source)
	err := b.dataSource.CreateTable(ctx, &tableInfo)
	if err != nil {
		// Check if table already exists
		if stmt.OrReplace {
			// Try to drop first
			b.dataSource.DropTable(ctx, stmt.Name)
			err = b.dataSource.CreateTable(ctx, &tableInfo)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to create view: %w", err)
		}
	}
	
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "result", Type: "text", Nullable: true},
		},
		Rows:    []domain.Row{{"result": "OK"}},
		Total:   1,
	}, nil
}

// executeDropView 执行 DROP VIEW
func (b *QueryBuilder) executeDropView(ctx context.Context, stmt *DropViewStatement) (*domain.QueryResult, error) {
	// Check if data source is writable
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is not writable")
	}
	
	// Drop each view
	results := make([]domain.Row, 0)
	for _, viewName := range stmt.Views {
		err := b.dataSource.DropTable(ctx, viewName)
		if err != nil {
			if !stmt.IfExists {
				return nil, fmt.Errorf("failed to drop view '%s': %w", viewName, err)
			}
			// IF EXISTS specified, continue to next view
		} else {
			results = append(results, domain.Row{
				"view": viewName,
				"status": "dropped",
			})
		}
	}
	
	if len(results) == 0 {
		return &domain.QueryResult{
			Columns: []domain.ColumnInfo{
				{Name: "result", Type: "text", Nullable: true},
			},
			Rows:    []domain.Row{},
			Total:   0,
		}, nil
	}
	
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "view", Type: "text", Nullable: true},
			{Name: "status", Type: "text", Nullable: true},
		},
		Rows:  results,
		Total: int64(len(results)),
	}, nil
}

// Note: executeAlterView is not supported by TiDB and has been removed
// The following code is kept commented for reference but should not be used
/*
func (b *QueryBuilder) executeAlterView(ctx context.Context, stmt *AlterViewStatement) (*domain.QueryResult, error) {
	// Check if data source is writable
	if !b.dataSource.IsWritable() {
		return nil, fmt.Errorf("data source is not writable")
	}
	
	// For simplicity, ALTER VIEW is implemented as DROP + CREATE
	// 1. Check if view exists
	_, err := b.dataSource.GetTableInfo(ctx, stmt.Name)
	if err != nil {
		return nil, fmt.Errorf("view '%s' does not exist", stmt.Name)
	}

	// 2. Drop the view
	err = b.dataSource.DropTable(ctx, stmt.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to drop view for ALTER: %w", err)
	}
	
	// 3. Create the new view
	createStmt := &CreateViewStatement{
		Name:        stmt.Name,
		ColumnList:  stmt.ColumnList,
		Select:      stmt.Select,
		Algorithm:    stmt.Algorithm,
		Definer:      stmt.Definer,
		Security:     stmt.Security,
		CheckOption:  stmt.CheckOption,
	}
	
	return b.executeCreateView(ctx, createStmt)
}
*/

// buildSelectSQL builds a SELECT SQL string from a SelectStatement
func (b *QueryBuilder) buildSelectSQL(stmt *SelectStatement) string {
	sql := "SELECT"

	// DISTINCT
	if stmt.Distinct {
		sql += " DISTINCT"
	}

	// Columns
	if len(stmt.Columns) > 0 {
		colNames := make([]string, 0, len(stmt.Columns))
		for _, col := range stmt.Columns {
			name := col.Name
			if name == "" {
				name = "*"
			}
			if col.Alias != "" {
				name += " AS " + col.Alias
			}
			colNames = append(colNames, name)
		}
		sql += " " + strings.Join(colNames, ", ")
	}

	// FROM
	if stmt.From != "" {
		sql += " FROM " + stmt.From
	}

	// WHERE
	if stmt.Where != nil {
		sql += " WHERE " + b.buildExpressionSQL(stmt.Where)
	}

	// GROUP BY
	if len(stmt.GroupBy) > 0 {
		sql += " GROUP BY " + strings.Join(stmt.GroupBy, ", ")
	}

	// HAVING
	if stmt.Having != nil {
		sql += " HAVING " + b.buildExpressionSQL(stmt.Having)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderItems := make([]string, 0, len(stmt.OrderBy))
		for _, item := range stmt.OrderBy {
			orderItems = append(orderItems, item.Column+" "+item.Direction)
		}
		sql += " ORDER BY " + strings.Join(orderItems, ", ")
	}

	// LIMIT
	if stmt.Limit != nil {
		sql += fmt.Sprintf(" LIMIT %d", *stmt.Limit)
	}

	// OFFSET
	if stmt.Offset != nil {
		sql += fmt.Sprintf(" OFFSET %d", *stmt.Offset)
	}

	return sql
}

// buildExpressionSQL builds an expression SQL string from an Expression
func (b *QueryBuilder) buildExpressionSQL(expr *Expression) string {
	if expr == nil {
		return ""
	}

	switch expr.Type {
	case ExprTypeColumn:
		return expr.Column

	case ExprTypeValue:
		return fmt.Sprintf("%v", expr.Value)

	case ExprTypeOperator:
		left := b.buildExpressionSQL(expr.Left)
		right := b.buildExpressionSQL(expr.Right)
		if expr.Operator == "and" || expr.Operator == "or" {
			return fmt.Sprintf("(%s) %s (%s)", left, strings.ToUpper(expr.Operator), right)
		}
		return fmt.Sprintf("%s %s %s", left, strings.ToUpper(expr.Operator), right)

	case ExprTypeFunction:
		args := make([]string, 0, len(expr.Args))
		for _, arg := range expr.Args {
			args = append(args, b.buildExpressionSQL(&arg))
		}
		return fmt.Sprintf("%s(%s)", strings.ToUpper(expr.Function), strings.Join(args, ", "))

	default:
		return ""
	}
}

// parseViewAlgorithm 解析视图算法字符串为 ViewAlgorithm 类型
func parseViewAlgorithm(algorithm string) domain.ViewAlgorithm {
	switch strings.ToUpper(algorithm) {
	case "UNDEFINED":
		return domain.ViewAlgorithmUndefined
	case "MERGE":
		return domain.ViewAlgorithmMerge
	case "TEMPTABLE":
		return domain.ViewAlgorithmTempTable
	default:
		return domain.ViewAlgorithmUndefined
	}
}

// parseViewSecurity 解析视图安全类型字符串为 ViewSecurity 类型
func parseViewSecurity(security string) domain.ViewSecurity {
	switch strings.ToUpper(security) {
	case "DEFINER":
		return domain.ViewSecurityDefiner
	case "INVOKER":
		return domain.ViewSecurityInvoker
	default:
		return domain.ViewSecurityDefiner
	}
}

// parseViewCheckOption 解析视图检查选项字符串为 ViewCheckOption 类型
func parseViewCheckOption(checkOption string) domain.ViewCheckOption {
	switch strings.ToUpper(checkOption) {
	case "NONE":
		return domain.ViewCheckOptionNone
	case "CASCADED":
		return domain.ViewCheckOptionCascaded
	case "LOCAL":
		return domain.ViewCheckOptionLocal
	default:
		return domain.ViewCheckOptionNone
	}
}
