package planning

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/optimizer"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// convertToLogicalPlan convert SQL statement to logical plan
func (o *Optimizer) convertToLogicalPlan(stmt *parser.SQLStatement) (optimizer.LogicalPlan, error) {
	switch stmt.Type {
	case parser.SQLTypeSelect:
		return o.convertSelect(stmt.Select)
	case parser.SQLTypeInsert:
		return o.convertInsert(stmt.Insert)
	case parser.SQLTypeUpdate:
		return o.convertUpdate(stmt.Update)
	case parser.SQLTypeDelete:
		return o.convertDelete(stmt.Delete)
	default:
		return nil, fmt.Errorf("unsupported SQL type: %s", stmt.Type)
	}
}

// convertSelect convert SELECT statement
func (o *Optimizer) convertSelect(stmt *parser.SelectStatement) (optimizer.LogicalPlan, error) {
	fmt.Println("  [DEBUG] convertSelect: 开始转换, 表名:", stmt.From)

	// Handle queries without FROM clause (e.g., SELECT DATABASE())
	if stmt.From == "" {
		fmt.Println("  [DEBUG] convertSelect: 无 FROM 子句，使用常量数据源")
		// Create a virtual table with one row and one column
		virtualTableInfo := &domain.TableInfo{
			Name:    "dual",
			Columns: []domain.ColumnInfo{},
		}

		var logicalPlan optimizer.LogicalPlan = optimizer.NewLogicalDataSource("dual", virtualTableInfo)

		// Apply WHERE condition (Selection)
		if stmt.Where != nil {
			conditions := o.extractConditions(stmt.Where)
			logicalPlan = optimizer.NewLogicalSelection(conditions, logicalPlan)
		}

		// Apply GROUP BY (Aggregate)
		if len(stmt.GroupBy) > 0 {
			aggFuncs := o.extractAggFuncs(stmt.Columns)
			logicalPlan = optimizer.NewLogicalAggregate(aggFuncs, stmt.GroupBy, logicalPlan)
		}

		// Apply ORDER BY (Sort)
		if len(stmt.OrderBy) > 0 {
			sortItems := make([]parser.OrderItem, len(stmt.OrderBy))
			for i, item := range stmt.OrderBy {
				sortItems[i] = parser.OrderItem{
					Expr: parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: item.Column,
					},
					Direction: item.Direction,
				}
			}
			// Create pointer slice for NewLogicalSort
			sortItemsPtr := make([]*parser.OrderItem, len(sortItems))
			for i := range sortItems {
				sortItemsPtr[i] = &sortItems[i]
			}
			logicalPlan = optimizer.NewLogicalSort(sortItemsPtr, logicalPlan)
		}

		// Apply LIMIT (Limit)
		if stmt.Limit != nil {
			limit := *stmt.Limit
			offset := int64(0)
			if stmt.Offset != nil {
				offset = *stmt.Offset
			}
			logicalPlan = optimizer.NewLogicalLimit(limit, offset, logicalPlan)
		}

		return logicalPlan, nil
	}

	// 1. Create DataSource
	tableInfo, err := o.dataSource.GetTableInfo(context.Background(), stmt.From)
	if err != nil {
		fmt.Println("  [DEBUG] convertSelect: GetTableInfo 失败:", err)
		return nil, fmt.Errorf("get table info failed: %w", err)
	}
	fmt.Println("  [DEBUG] convertSelect: GetTableInfo 成功, 列数:", len(tableInfo.Columns))

	var logicalPlan optimizer.LogicalPlan = optimizer.NewLogicalDataSource(stmt.From, tableInfo)
	fmt.Println("  [DEBUG] convertSelect: LogicalDataSource 创建完成")

	// 2. Apply WHERE condition (Selection)
	if stmt.Where != nil {
		conditions := o.extractConditions(stmt.Where)
		logicalPlan = optimizer.NewLogicalSelection(conditions, logicalPlan)
	}

	// 3. Apply GROUP BY (Aggregate)
	if len(stmt.GroupBy) > 0 {
		aggFuncs := o.extractAggFuncs(stmt.Columns)
		logicalPlan = optimizer.NewLogicalAggregate(aggFuncs, stmt.GroupBy, logicalPlan)
	}

	// 4. Apply ORDER BY (Sort)
	if len(stmt.OrderBy) > 0 {
		sortItems := make([]parser.OrderItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			sortItems[i] = parser.OrderItem{
				Expr: parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: item.Column,
				},
				Direction: item.Direction,
			}
		}
		// Create pointer slice for NewLogicalSort
		sortItemsPtr := make([]*parser.OrderItem, len(sortItems))
		for i := range sortItems {
			sortItemsPtr[i] = &sortItems[i]
		}
		logicalPlan = optimizer.NewLogicalSort(sortItemsPtr, logicalPlan)
	}

	// 5. Apply LIMIT (Limit)
	if stmt.Limit != nil {
		limit := *stmt.Limit
		offset := int64(0)
		if stmt.Offset != nil {
			offset = *stmt.Offset
		}
		logicalPlan = optimizer.NewLogicalLimit(limit, offset, logicalPlan)
	}

	// 6. Apply SELECT columns (Projection)
	fmt.Printf("  [DEBUG] convertSelect: SELECT列数量: %d, IsWildcard=%v\n", len(stmt.Columns), isWildcard(stmt.Columns))
	if len(stmt.Columns) > 0 {
		fmt.Printf("  [DEBUG] convertSelect: cols[0].Name='%s'\n", stmt.Columns[0].Name)
	}
	if len(stmt.Columns) > 0 && !isWildcard(stmt.Columns) {
		fmt.Println("  [DEBUG] convertSelect: 创建Projection")
		exprs := make([]*parser.Expression, len(stmt.Columns))
		aliases := make([]string, len(stmt.Columns))
		for i, col := range stmt.Columns {
			fmt.Printf("  [DEBUG] convertSelect: 列%d: Name='%s', Alias='%s'\n", i, col.Name, col.Alias)
			exprs[i] = &parser.Expression{
				Type:   parser.ExprTypeColumn,
				Column: col.Name,
			}
			if col.Alias != "" {
				aliases[i] = col.Alias
			} else {
				aliases[i] = col.Name
			}
		}
		logicalPlan = optimizer.NewLogicalProjection(exprs, aliases, logicalPlan)
	}

	return logicalPlan, nil
}

// convertInsert convert INSERT statement
func (o *Optimizer) convertInsert(stmt *parser.InsertStatement) (optimizer.LogicalPlan, error) {
	// Verify table exists
	_, err := o.dataSource.GetTableInfo(context.Background(), stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist: %v", stmt.Table, err)
	}

	// Convert Values to parser.Expression
	values := make([][]parser.Expression, len(stmt.Values))
	for i, row := range stmt.Values {
		values[i] = make([]parser.Expression, len(row))
		for j, val := range row {
			values[i][j] = o.valueToExpression(val)
		}
	}

	// Create LogicalInsert
	insertPlan := optimizer.NewLogicalInsert(stmt.Table, stmt.Columns, values)

	// Handle ON DUPLICATE KEY UPDATE
	if stmt.OnDuplicate != nil {
		updatePlan, err := o.convertUpdate(stmt.OnDuplicate)
		if err != nil {
			return nil, fmt.Errorf("failed to convert ON DUPLICATE KEY UPDATE: %v", err)
		}
		if logicalUpdate, ok := updatePlan.(*optimizer.LogicalUpdate); ok {
			insertPlan.SetOnDuplicate(logicalUpdate)
		}
	}

	fmt.Printf("  [DEBUG] convertInsert: INSERT into %s with %d rows\n", stmt.Table, len(values))
	return insertPlan, nil
}

// convertUpdate convert UPDATE statement
func (o *Optimizer) convertUpdate(stmt *parser.UpdateStatement) (optimizer.LogicalPlan, error) {
	// Verify table exists
	_, err := o.dataSource.GetTableInfo(context.Background(), stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist: %v", stmt.Table, err)
	}

	// Convert SET clause
	set := make(map[string]parser.Expression)
	for col, val := range stmt.Set {
		set[col] = o.valueToExpression(val)
	}

	// Create LogicalUpdate
	updatePlan := optimizer.NewLogicalUpdate(stmt.Table, set)

	// Set WHERE condition
	updatePlan.SetWhere(stmt.Where)

	// Set ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderItems := make([]*parser.OrderItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			orderItems[i] = &parser.OrderItem{
				Expr: parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: item.Column,
				},
				Direction: item.Direction,
			}
		}
		updatePlan.SetOrderBy(orderItems)
	}

	// Set LIMIT
	updatePlan.SetLimit(stmt.Limit)

	fmt.Printf("  [DEBUG] convertUpdate: UPDATE %s with %d columns\n", stmt.Table, len(set))
	return updatePlan, nil
}

// convertDelete convert DELETE statement
func (o *Optimizer) convertDelete(stmt *parser.DeleteStatement) (optimizer.LogicalPlan, error) {
	// Verify table exists
	_, err := o.dataSource.GetTableInfo(context.Background(), stmt.Table)
	if err != nil {
		return nil, fmt.Errorf("table '%s' does not exist: %v", stmt.Table, err)
	}

	// Create LogicalDelete
	deletePlan := optimizer.NewLogicalDelete(stmt.Table)

	// Set WHERE condition
	deletePlan.SetWhere(stmt.Where)

	// Set ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderItems := make([]*parser.OrderItem, len(stmt.OrderBy))
		for i, item := range stmt.OrderBy {
			orderItems[i] = &parser.OrderItem{
				Expr: parser.Expression{
					Type:   parser.ExprTypeColumn,
					Column: item.Column,
				},
				Direction: item.Direction,
			}
		}
		deletePlan.SetOrderBy(orderItems)
	}

	// Set LIMIT
	deletePlan.SetLimit(stmt.Limit)

	fmt.Printf("  [DEBUG] convertDelete: DELETE from %s\n", stmt.Table)
	return deletePlan, nil
}

// isWildcard check if it's a wildcard
func isWildcard(cols []parser.SelectColumn) bool {
	if len(cols) == 1 && cols[0].IsWildcard {
		return true
	}
	return false
}
