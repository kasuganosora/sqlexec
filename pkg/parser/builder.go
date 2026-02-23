package parser

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	domain "github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/generated"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/utils"
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

	// Detect if post-processing is needed
	hasAggregates := b.hasAggregateFunctions(stmt.Columns)
	hasGroupBy := len(stmt.GroupBy) > 0
	hasJoins := len(stmt.Joins) > 0

	// Only apply ORDER BY/LIMIT/OFFSET at dataSource level if no post-processing needed
	if !hasAggregates && !hasGroupBy && !hasJoins {
		if len(stmt.OrderBy) > 0 {
			options.OrderBy = stmt.OrderBy[0].Column
			options.Order = stmt.OrderBy[0].Direction
		}
		if stmt.Limit != nil {
			options.Limit = int(*stmt.Limit)
		}
		if stmt.Offset != nil {
			options.Offset = int(*stmt.Offset)
		}
	}

	// 执行查询
	result, err := b.dataSource.Query(ctx, stmt.From, options)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// =========================================================================
	// 处理 JOIN
	// =========================================================================
	if hasJoins {
		mainTableName := stmt.From
		// Prefix main table rows with table name to avoid column name conflicts
		prefixedRows := make([]domain.Row, 0, len(result.Rows))
		for _, row := range result.Rows {
			newRow := make(domain.Row)
			for k, v := range row {
				newRow[k] = v
				newRow[mainTableName+"."+k] = v
			}
			prefixedRows = append(prefixedRows, newRow)
		}
		currentRows := prefixedRows

		// Cache join table results to avoid duplicate queries
		joinResultCache := make(map[string]*domain.QueryResult)

		for _, join := range stmt.Joins {
			joinTableName := join.Table
			joinAlias := join.Alias
			if joinAlias == "" {
				joinAlias = joinTableName
			}

			// Query the join table (fetch all rows, no filters)
			joinResult, err := b.dataSource.Query(ctx, joinTableName, &domain.QueryOptions{SelectAll: true})
			if err != nil {
				return nil, fmt.Errorf("join query on table '%s' failed: %w", joinTableName, err)
			}
			joinResultCache[joinTableName] = joinResult

			// Prefix join table rows with table name and alias
			joinRows := make([]domain.Row, 0, len(joinResult.Rows))
			for _, row := range joinResult.Rows {
				newRow := make(domain.Row)
				for k, v := range row {
					newRow[joinTableName+"."+k] = v
					if joinAlias != joinTableName {
						newRow[joinAlias+"."+k] = v
					}
				}
				joinRows = append(joinRows, newRow)
			}

			// Merge rows based on join type
			currentRows = b.performJoin(currentRows, joinRows, join, joinTableName, joinAlias, joinResult.Columns)
		}

		result.Rows = currentRows
		result.Total = int64(len(currentRows))

		// Update column info to include joined table columns with table prefix.
		// Re-use cached results instead of re-querying the data source.
		joinedColumns := make([]domain.ColumnInfo, 0)
		for _, col := range result.Columns {
			joinedColumns = append(joinedColumns, domain.ColumnInfo{
				Name: mainTableName + "." + col.Name, Type: col.Type, Nullable: col.Nullable, Primary: col.Primary,
			})
		}
		for _, join := range stmt.Joins {
			joinTableName := join.Table
			if cached, ok := joinResultCache[joinTableName]; ok {
				for _, col := range cached.Columns {
					joinedColumns = append(joinedColumns, domain.ColumnInfo{
						Name: joinTableName + "." + col.Name, Type: col.Type, Nullable: col.Nullable,
					})
				}
			}
		}
		result.Columns = joinedColumns
	}

	// =========================================================================
	// 处理 GROUP BY + 聚合函数 + HAVING
	// =========================================================================
	if hasGroupBy {
		// Group rows by the specified columns
		groups := b.groupRows(result.Rows, stmt.GroupBy)

		// Compute aggregates for each group
		groupedRows := make([]domain.Row, 0, len(groups))
		for _, groupRows := range groups {
			row := make(domain.Row)
			// Copy group-by column values from the first row in the group
			for _, gbCol := range stmt.GroupBy {
				if len(groupRows) > 0 {
					row[gbCol] = b.getColumnValue(groupRows[0], gbCol)
				}
			}
			// Compute aggregate columns
			for _, col := range stmt.Columns {
				if col.Expr != nil && col.Expr.Type == ExprTypeFunction && b.isAggregateFunction(col.Expr.Function) {
					val := b.computeAggregate(col.Expr.Function, col.Expr.Args, groupRows)
					outputName := col.Alias
					if outputName == "" {
						outputName = col.Name
					}
					if outputName != "" {
						row[outputName] = val
					}
				}
			}
			groupedRows = append(groupedRows, row)
		}

		// 处理 HAVING - filter groups after aggregation
		if stmt.Having != nil {
			filteredGroups := make([]domain.Row, 0)
			i := 0
			for _, groupRows := range groups {
				if i >= len(groupedRows) {
					break
				}
				if b.evaluateHavingExpression(stmt.Having, groupRows) {
					filteredGroups = append(filteredGroups, groupedRows[i])
				}
				i++
			}
			groupedRows = filteredGroups
		}

		result.Rows = groupedRows
		result.Total = int64(len(groupedRows))

		// Build column info for the result
		newColumns := make([]domain.ColumnInfo, 0)
		for _, gbCol := range stmt.GroupBy {
			newColumns = append(newColumns, domain.ColumnInfo{Name: gbCol, Type: "text", Nullable: true})
		}
		for _, col := range stmt.Columns {
			if col.Expr != nil && col.Expr.Type == ExprTypeFunction && b.isAggregateFunction(col.Expr.Function) {
				outputName := col.Alias
				if outputName == "" {
					outputName = col.Name
				}
				if outputName != "" {
					newColumns = append(newColumns, domain.ColumnInfo{Name: outputName, Type: "float64", Nullable: true})
				}
			}
		}
		result.Columns = newColumns

		return result, nil
	}

	// 处理聚合函数（无 GROUP BY 的情况）
	if hasAggregates {
		aggRow := make(domain.Row)
		for _, col := range stmt.Columns {
			if col.Expr != nil && col.Expr.Type == ExprTypeFunction && b.isAggregateFunction(col.Expr.Function) {
				val := b.computeAggregate(col.Expr.Function, col.Expr.Args, result.Rows)
				outputName := col.Alias
				if outputName == "" {
					outputName = col.Name
				}
				if outputName != "" {
					aggRow[outputName] = val
				}
			}
		}

		newColumns := make([]domain.ColumnInfo, 0)
		for _, col := range stmt.Columns {
			if col.Expr != nil && col.Expr.Type == ExprTypeFunction && b.isAggregateFunction(col.Expr.Function) {
				outputName := col.Alias
				if outputName == "" {
					outputName = col.Name
				}
				if outputName != "" {
					newColumns = append(newColumns, domain.ColumnInfo{Name: outputName, Type: "float64", Nullable: true})
				}
			}
		}

		return &domain.QueryResult{
			Columns: newColumns,
			Rows:    []domain.Row{aggRow},
			Total:   1,
		}, nil
	}

	// =========================================================================
	// Column selection (non-aggregate, non-group-by case)
	// =========================================================================

	// 如果是 select *，需要确保返回的行数据不包含隐藏字段
	if isSelectAll {
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
		selectedColumns := make([]string, 0, len(stmt.Columns))
		for _, col := range stmt.Columns {
			if len(col.Name) > 0 {
				selectedColumns = append(selectedColumns, col.Name)
			}
		}

		if len(selectedColumns) == 0 {
			return result, nil
		}

		newColumns := make([]domain.ColumnInfo, 0, len(selectedColumns))
		for _, colName := range selectedColumns {
			found := false
			for _, col := range result.Columns {
				if col.Name == colName {
					newColumns = append(newColumns, col)
					found = true
					break
				}
			}
			if !found {
				newColumns = append(newColumns, domain.ColumnInfo{
					Name:     colName,
					Type:     "int64",
					Nullable: true,
					Primary:  false,
				})
			}
		}

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

		result.Columns = newColumns
		result.Rows = filteredRows
	}

	return result, nil
}

// =============================================================================
// JOIN helper methods
// =============================================================================

// performJoin merges left and right row sets based on join type and condition
func (b *QueryBuilder) performJoin(leftRows []domain.Row, rightRows []domain.Row, join JoinInfo, joinTableName, joinAlias string, rightColumns []domain.ColumnInfo) []domain.Row {
	switch join.Type {
	case JoinTypeCross:
		return b.performCrossJoin(leftRows, rightRows)
	case JoinTypeInner:
		return b.performInnerJoin(leftRows, rightRows, join.Condition)
	case JoinTypeLeft:
		return b.performLeftJoin(leftRows, rightRows, join.Condition, joinTableName, joinAlias, rightColumns)
	case JoinTypeRight:
		return b.performRightJoin(leftRows, rightRows, join.Condition, joinTableName, joinAlias, rightColumns)
	case JoinTypeFull:
		left := b.performLeftJoin(leftRows, rightRows, join.Condition, joinTableName, joinAlias, rightColumns)
		rightUnmatched := b.getUnmatchedRightRows(leftRows, rightRows, join.Condition)
		return append(left, rightUnmatched...)
	default:
		return b.performInnerJoin(leftRows, rightRows, join.Condition)
	}
}

// performCrossJoin returns the Cartesian product of left and right rows
func (b *QueryBuilder) performCrossJoin(leftRows, rightRows []domain.Row) []domain.Row {
	result := make([]domain.Row, 0, len(leftRows)*len(rightRows))
	for _, left := range leftRows {
		for _, right := range rightRows {
			result = append(result, b.mergeRows(left, right))
		}
	}
	return result
}

// performInnerJoin returns rows where the join condition matches
func (b *QueryBuilder) performInnerJoin(leftRows, rightRows []domain.Row, condition *Expression) []domain.Row {
	// Try hash join for simple equality conditions (O(n+m) vs O(n*m))
	if leftCol, rightCol, ok := b.extractEqualityColumns(condition); ok {
		return b.hashInnerJoin(leftRows, rightRows, leftCol, rightCol)
	}
	// Fallback to nested loop for complex conditions
	result := make([]domain.Row, 0)
	for _, left := range leftRows {
		for _, right := range rightRows {
			merged := b.mergeRows(left, right)
			if b.evaluateJoinCondition(merged, condition) {
				result = append(result, merged)
			}
		}
	}
	return result
}

// hashInnerJoin performs an inner join using a hash map on the right side
func (b *QueryBuilder) hashInnerJoin(leftRows, rightRows []domain.Row, leftCol, rightCol string) []domain.Row {
	// Build hash table on right rows (typically smaller or equal)
	hashTable := make(map[string][]domain.Row)
	for _, right := range rightRows {
		key := fmt.Sprintf("%v", right[rightCol])
		hashTable[key] = append(hashTable[key], right)
	}

	result := make([]domain.Row, 0, len(leftRows))
	for _, left := range leftRows {
		key := fmt.Sprintf("%v", left[leftCol])
		if matches, ok := hashTable[key]; ok {
			for _, right := range matches {
				result = append(result, b.mergeRows(left, right))
			}
		}
	}
	return result
}

// extractEqualityColumns extracts left and right column names from a simple equality condition.
// Returns ("", "", false) for non-simple-equality conditions.
func (b *QueryBuilder) extractEqualityColumns(condition *Expression) (string, string, bool) {
	if condition == nil {
		return "", "", false
	}
	if condition.Type != ExprTypeOperator {
		return "", "", false
	}
	if strings.ToLower(condition.Operator) != "=" {
		return "", "", false
	}
	if condition.Left == nil || condition.Right == nil {
		return "", "", false
	}
	if condition.Left.Type != ExprTypeColumn || condition.Right.Type != ExprTypeColumn {
		return "", "", false
	}
	return condition.Left.Column, condition.Right.Column, true
}

// performLeftJoin returns all left rows with matching right rows; unmatched left rows get null right columns
func (b *QueryBuilder) performLeftJoin(leftRows, rightRows []domain.Row, condition *Expression, joinTableName, joinAlias string, rightColumns []domain.ColumnInfo) []domain.Row {
	result := make([]domain.Row, 0, len(leftRows))
	for _, left := range leftRows {
		matched := false
		for _, right := range rightRows {
			merged := b.mergeRows(left, right)
			if b.evaluateJoinCondition(merged, condition) {
				result = append(result, merged)
				matched = true
			}
		}
		if !matched {
			nullRow := b.mergeRowWithNulls(left, rightColumns, joinTableName, joinAlias)
			result = append(result, nullRow)
		}
	}
	return result
}

// performRightJoin returns all right rows with matching left rows; unmatched right rows get null left columns
func (b *QueryBuilder) performRightJoin(leftRows, rightRows []domain.Row, condition *Expression, joinTableName, joinAlias string, rightColumns []domain.ColumnInfo) []domain.Row {
	result := make([]domain.Row, 0, len(rightRows))
	// Collect left table column keys from first left row to build NULL row
	var leftColKeys []string
	if len(leftRows) > 0 {
		for k := range leftRows[0] {
			leftColKeys = append(leftColKeys, k)
		}
	}
	for _, right := range rightRows {
		matched := false
		for _, left := range leftRows {
			merged := b.mergeRows(left, right)
			if b.evaluateJoinCondition(merged, condition) {
				result = append(result, merged)
				matched = true
			}
		}
		if !matched {
			// Build a merged row with NULL for all left columns
			nullRow := make(domain.Row, len(right)+len(leftColKeys))
			for _, k := range leftColKeys {
				nullRow[k] = nil
			}
			for k, v := range right {
				nullRow[k] = v
			}
			result = append(result, nullRow)
		}
	}
	return result
}

// getUnmatchedRightRows returns right rows that don't match any left row (for FULL JOIN).
// Unmatched right rows are returned with NULL for all left-side columns.
func (b *QueryBuilder) getUnmatchedRightRows(leftRows, rightRows []domain.Row, condition *Expression) []domain.Row {
	// Collect left table column keys from first left row to build NULL row
	var leftColKeys []string
	if len(leftRows) > 0 {
		for k := range leftRows[0] {
			leftColKeys = append(leftColKeys, k)
		}
	}
	result := make([]domain.Row, 0)
	for _, right := range rightRows {
		matched := false
		for _, left := range leftRows {
			merged := b.mergeRows(left, right)
			if b.evaluateJoinCondition(merged, condition) {
				matched = true
				break
			}
		}
		if !matched {
			nullRow := make(domain.Row, len(right)+len(leftColKeys))
			for _, k := range leftColKeys {
				nullRow[k] = nil
			}
			for k, v := range right {
				nullRow[k] = v
			}
			result = append(result, nullRow)
		}
	}
	return result
}

// mergeRows merges two rows into one, with right overwriting on conflict
func (b *QueryBuilder) mergeRows(left, right domain.Row) domain.Row {
	merged := make(domain.Row, len(left)+len(right))
	for k, v := range left {
		merged[k] = v
	}
	for k, v := range right {
		merged[k] = v
	}
	return merged
}

// mergeRowWithNulls creates a merged row with left data and nil for right table columns
func (b *QueryBuilder) mergeRowWithNulls(left domain.Row, rightColumns []domain.ColumnInfo, joinTableName, joinAlias string) domain.Row {
	merged := make(domain.Row, len(left)+len(rightColumns)*2)
	for k, v := range left {
		merged[k] = v
	}
	for _, col := range rightColumns {
		merged[joinTableName+"."+col.Name] = nil
		if joinAlias != joinTableName {
			merged[joinAlias+"."+col.Name] = nil
		}
	}
	return merged
}

// evaluateJoinCondition evaluates a join condition expression against a merged row
func (b *QueryBuilder) evaluateJoinCondition(row domain.Row, condition *Expression) bool {
	if condition == nil {
		return true
	}

	switch condition.Type {
	case ExprTypeOperator:
		op := strings.ToLower(condition.Operator)

		if op == "and" {
			return b.evaluateJoinCondition(row, condition.Left) && b.evaluateJoinCondition(row, condition.Right)
		}
		if op == "or" {
			return b.evaluateJoinCondition(row, condition.Left) || b.evaluateJoinCondition(row, condition.Right)
		}

		if condition.Left == nil || condition.Right == nil {
			return false
		}

		leftVal := b.resolveExprValue(row, condition.Left)
		rightVal := b.resolveExprValue(row, condition.Right)

		sqlOp := b.convertOperator(op)
		result, err := utils.CompareValues(leftVal, rightVal, sqlOp)
		if err != nil {
			return false
		}
		return result

	default:
		return true
	}
}

// resolveExprValue resolves an expression to a concrete value from a row
func (b *QueryBuilder) resolveExprValue(row domain.Row, expr *Expression) interface{} {
	if expr == nil {
		return nil
	}
	switch expr.Type {
	case ExprTypeColumn:
		if val, exists := row[expr.Column]; exists {
			return val
		}
		return nil
	case ExprTypeValue:
		return expr.Value
	default:
		return nil
	}
}

// getColumnValue resolves a column name from a row, trying both direct and prefixed matches
func (b *QueryBuilder) getColumnValue(row domain.Row, colName string) interface{} {
	if val, exists := row[colName]; exists {
		return val
	}
	suffix := "." + colName
	for k, v := range row {
		if strings.HasSuffix(k, suffix) {
			return v
		}
	}
	return nil
}

// =============================================================================
// Aggregation helper methods
// =============================================================================

// hasAggregateFunctions checks if any select column contains an aggregate function
func (b *QueryBuilder) hasAggregateFunctions(columns []SelectColumn) bool {
	for _, col := range columns {
		if col.Expr != nil && col.Expr.Type == ExprTypeFunction && b.isAggregateFunction(col.Expr.Function) {
			return true
		}
	}
	return false
}

// isAggregateFunction checks if a function name is an aggregate function
func (b *QueryBuilder) isAggregateFunction(funcName string) bool {
	switch strings.ToUpper(funcName) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

// computeAggregate computes an aggregate function value over a set of rows
func (b *QueryBuilder) computeAggregate(funcName string, args []Expression, rows []domain.Row) interface{} {
	switch strings.ToUpper(funcName) {
	case "COUNT":
		return b.computeCount(args, rows)
	case "SUM":
		return b.computeSum(args, rows)
	case "AVG":
		return b.computeAvg(args, rows)
	case "MIN":
		return b.computeMin(args, rows)
	case "MAX":
		return b.computeMax(args, rows)
	default:
		return nil
	}
}

// computeCount computes COUNT(*) or COUNT(column)
func (b *QueryBuilder) computeCount(args []Expression, rows []domain.Row) int64 {
	if len(args) == 0 || args[0].Type == ExprTypeValue {
		return int64(len(rows))
	}
	if args[0].Type == ExprTypeColumn {
		colName := args[0].Column
		count := int64(0)
		for _, row := range rows {
			if b.getColumnValue(row, colName) != nil {
				count++
			}
		}
		return count
	}
	return int64(len(rows))
}

// computeSum computes SUM(column)
func (b *QueryBuilder) computeSum(args []Expression, rows []domain.Row) float64 {
	if len(args) == 0 || args[0].Type != ExprTypeColumn {
		return 0
	}
	colName := args[0].Column
	sum := float64(0)
	for _, row := range rows {
		if val := b.getColumnValue(row, colName); val != nil {
			if f, err := utils.ToFloat64(val); err == nil {
				sum += f
			}
		}
	}
	return sum
}

// computeAvg computes AVG(column)
func (b *QueryBuilder) computeAvg(args []Expression, rows []domain.Row) float64 {
	if len(args) == 0 || args[0].Type != ExprTypeColumn || len(rows) == 0 {
		return 0
	}
	colName := args[0].Column
	sum := float64(0)
	count := 0
	for _, row := range rows {
		if val := b.getColumnValue(row, colName); val != nil {
			if f, err := utils.ToFloat64(val); err == nil {
				sum += f
				count++
			}
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// computeMin computes MIN(column)
func (b *QueryBuilder) computeMin(args []Expression, rows []domain.Row) interface{} {
	if len(args) == 0 || args[0].Type != ExprTypeColumn || len(rows) == 0 {
		return nil
	}
	colName := args[0].Column
	var minVal interface{}
	minFloat := math.MaxFloat64
	hasValue := false
	for _, row := range rows {
		if val := b.getColumnValue(row, colName); val != nil {
			if f, err := utils.ToFloat64(val); err == nil {
				if !hasValue || f < minFloat {
					minFloat = f
					minVal = val
					hasValue = true
				}
			}
		}
	}
	if !hasValue {
		return nil
	}
	return minVal
}

// computeMax computes MAX(column)
func (b *QueryBuilder) computeMax(args []Expression, rows []domain.Row) interface{} {
	if len(args) == 0 || args[0].Type != ExprTypeColumn || len(rows) == 0 {
		return nil
	}
	colName := args[0].Column
	var maxVal interface{}
	maxFloat := -math.MaxFloat64
	hasValue := false
	for _, row := range rows {
		if val := b.getColumnValue(row, colName); val != nil {
			if f, err := utils.ToFloat64(val); err == nil {
				if !hasValue || f > maxFloat {
					maxFloat = f
					maxVal = val
					hasValue = true
				}
			}
		}
	}
	if !hasValue {
		return nil
	}
	return maxVal
}

// =============================================================================
// GROUP BY helper methods
// =============================================================================

// groupRows groups rows by the specified columns, preserving insertion order
func (b *QueryBuilder) groupRows(rows []domain.Row, groupByCols []string) [][]domain.Row {
	type groupEntry struct {
		key  string
		rows []domain.Row
	}
	groupMap := make(map[string]*groupEntry)
	var orderedKeys []string

	for _, row := range rows {
		key := b.buildGroupKey(row, groupByCols)
		entry, exists := groupMap[key]
		if !exists {
			entry = &groupEntry{key: key, rows: make([]domain.Row, 0)}
			groupMap[key] = entry
			orderedKeys = append(orderedKeys, key)
		}
		entry.rows = append(entry.rows, row)
	}

	result := make([][]domain.Row, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		result = append(result, groupMap[key].rows)
	}
	return result
}

// buildGroupKey builds a string key for grouping by concatenating column values
func (b *QueryBuilder) buildGroupKey(row domain.Row, groupByCols []string) string {
	parts := make([]string, len(groupByCols))
	for i, col := range groupByCols {
		val := b.getColumnValue(row, col)
		parts[i] = fmt.Sprintf("%v", val)
	}
	return strings.Join(parts, "\x00")
}

// =============================================================================
// HAVING helper methods
// =============================================================================

// evaluateHavingExpression evaluates a HAVING expression against a group of rows
func (b *QueryBuilder) evaluateHavingExpression(expr *Expression, groupRows []domain.Row) bool {
	if expr == nil {
		return true
	}

	switch expr.Type {
	case ExprTypeOperator:
		op := strings.ToLower(expr.Operator)

		if op == "and" {
			return b.evaluateHavingExpression(expr.Left, groupRows) && b.evaluateHavingExpression(expr.Right, groupRows)
		}
		if op == "or" {
			return b.evaluateHavingExpression(expr.Left, groupRows) || b.evaluateHavingExpression(expr.Right, groupRows)
		}

		leftVal := b.resolveHavingExprValue(expr.Left, groupRows)
		rightVal := b.resolveHavingExprValue(expr.Right, groupRows)

		sqlOp := b.convertOperator(op)
		result, err := utils.CompareValues(leftVal, rightVal, sqlOp)
		if err != nil {
			return false
		}
		return result

	case ExprTypeFunction:
		if b.isAggregateFunction(expr.Function) {
			val := b.computeAggregate(expr.Function, expr.Args, groupRows)
			return b.isTruthyValue(val)
		}
		return true

	default:
		return true
	}
}

// resolveHavingExprValue resolves a HAVING expression to a value, computing aggregates as needed
func (b *QueryBuilder) resolveHavingExprValue(expr *Expression, groupRows []domain.Row) interface{} {
	if expr == nil {
		return nil
	}
	switch expr.Type {
	case ExprTypeFunction:
		if b.isAggregateFunction(expr.Function) {
			return b.computeAggregate(expr.Function, expr.Args, groupRows)
		}
		return nil
	case ExprTypeColumn:
		if len(groupRows) > 0 {
			return b.getColumnValue(groupRows[0], expr.Column)
		}
		return nil
	case ExprTypeValue:
		return expr.Value
	default:
		return nil
	}
}

// isTruthyValue checks if a value is truthy
func (b *QueryBuilder) isTruthyValue(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	case int:
		return v != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	default:
		return true
	}
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
				Name:          col.Name,
				Type:          col.Type,
				Nullable:      col.Nullable,
				Primary:       col.Primary,
				Default:       fmt.Sprintf("%v", col.Default),
				Unique:        col.Unique,
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
		Name:    stmt.Name,
		Schema:  "", // Use default schema
		Columns: []domain.ColumnInfo{},
		Atts: map[string]interface{}{
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
		Rows:  []domain.Row{{"result": "OK"}},
		Total: 1,
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
				"view":   viewName,
				"status": "dropped",
			})
		}
	}

	if len(results) == 0 {
		return &domain.QueryResult{
			Columns: []domain.ColumnInfo{
				{Name: "result", Type: "text", Nullable: true},
			},
			Rows:  []domain.Row{},
			Total: 0,
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
