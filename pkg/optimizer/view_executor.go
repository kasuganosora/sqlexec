package optimizer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ViewExecutor handles view execution (TEMPTABLE algorithm)
type ViewExecutor struct {
	dataSource domain.DataSource
}

// NewViewExecutor creates a new view executor
func NewViewExecutor(dataSource domain.DataSource) *ViewExecutor {
	return &ViewExecutor{
		dataSource: dataSource,
	}
}

// ExecuteAsTempTable executes a view query and stores result in a temporary table
func (ve *ViewExecutor) ExecuteAsTempTable(ctx context.Context, viewInfo *domain.ViewInfo, outerQuery *parser.SelectStatement) (*domain.QueryResult, error) {
	// Check view algorithm
	if viewInfo.Algorithm != "" && viewInfo.Algorithm != domain.ViewAlgorithmUndefined && viewInfo.Algorithm != domain.ViewAlgorithmTempTable {
		return nil, fmt.Errorf("view does not use TEMPTABLE algorithm: %s", viewInfo.Algorithm)
	}

	// Parse view's SELECT statement
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(viewInfo.SelectStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse view SELECT: %w", err)
	}

	if !parseResult.Success {
		return nil, fmt.Errorf("view SELECT parse failed: %s", parseResult.Error)
	}

	if parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("view definition is not a SELECT statement")
	}

	viewSelect := parseResult.Statement.Select

	// Execute view's SELECT to get data
	builder := parser.NewQueryBuilder(ve.dataSource)
	result, err := builder.ExecuteStatement(ctx, &parser.SQLStatement{
		Type:   parser.SQLTypeSelect,
		Select: viewSelect,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute view query: %w", err)
	}

	if result == nil {
		return &domain.QueryResult{
			Columns: []domain.ColumnInfo{},
			Rows:    []domain.Row{},
			Total:   0,
		}, nil
	}
	if result.Rows == nil {
		return &domain.QueryResult{
			Columns: result.Columns,
			Rows:    []domain.Row{},
			Total:   0,
		}, nil
	}

	// Apply outer query filters/limits to result
	filteredResult := ve.applyOuterQuery(result, outerQuery, viewInfo)

	return filteredResult, nil
}

// applyOuterQuery applies outer query filters, limits, etc. to view result
func (ve *ViewExecutor) applyOuterQuery(viewResult *domain.QueryResult, outerQuery *parser.SelectStatement, viewInfo *domain.ViewInfo) *domain.QueryResult {
	filtered := &domain.QueryResult{
		Columns: viewResult.Columns,
		Rows:    make([]domain.Row, 0, len(viewResult.Rows)),
		Total:   viewResult.Total,
	}

	// Apply WHERE from outer query
	if outerQuery.Where != nil {
		for _, row := range viewResult.Rows {
			matches, err := ve.evaluateWhere(row, outerQuery.Where)
			if err == nil && matches {
				filtered.Rows = append(filtered.Rows, row)
			}
		}
		filtered.Total = int64(len(filtered.Rows))
	} else {
		filtered.Rows = viewResult.Rows
	}

	// Apply ORDER BY
	if len(outerQuery.OrderBy) > 0 {
		ve.sortRows(filtered.Rows, outerQuery.OrderBy)
	}

	// Apply LIMIT/OFFSET
	if outerQuery.Limit != nil {
		start := 0
		if outerQuery.Offset != nil {
			start = int(*outerQuery.Offset)
		}
		if start < 0 {
			start = 0
		}

		end := start + int(*outerQuery.Limit)
		if end > len(filtered.Rows) {
			end = len(filtered.Rows)
		}

		if start < len(filtered.Rows) {
			filtered.Rows = filtered.Rows[start:end]
		} else {
			filtered.Rows = []domain.Row{}
		}
		filtered.Total = int64(len(filtered.Rows))
	}

	// Apply column selection
	if len(outerQuery.Columns) > 0 && !ve.isSelectAll(outerQuery.Columns) {
		filtered = ve.selectColumns(filtered, outerQuery.Columns, viewInfo)
	}

	return filtered
}

// evaluateWhere evaluates a WHERE expression against a row
func (ve *ViewExecutor) evaluateWhere(row domain.Row, expr *parser.Expression) (bool, error) {
	if expr == nil {
		return true, nil
	}

	// Evaluate expression based on type
	switch expr.Type {
	case parser.ExprTypeColumn:
		// Check if column exists and has a truthy value
		val, exists := row[expr.Column]
		if !exists {
			return false, nil
		}
		return ve.isTruthy(val), nil

	case parser.ExprTypeValue:
		// Constant value
		return ve.isTruthy(expr.Value), nil

	case parser.ExprTypeOperator:
		// Binary operation
		return ve.evaluateOperator(row, expr)

	case parser.ExprTypeFunction:
		// Function call - not fully implemented
		return true, nil

	default:
		return true, nil
	}
}

// evaluateOperator evaluates a binary operator expression
func (ve *ViewExecutor) evaluateOperator(row domain.Row, expr *parser.Expression) (bool, error) {
	if expr.Operator == "" {
		return true, nil
	}

	// Handle logical operators
	op := strings.ToUpper(expr.Operator)
	switch op {
	case "AND":
		left, _ := ve.evaluateWhere(row, expr.Left)
		right, _ := ve.evaluateWhere(row, expr.Right)
		return left && right, nil

	case "OR":
		left, _ := ve.evaluateWhere(row, expr.Left)
		right, _ := ve.evaluateWhere(row, expr.Right)
		return left || right, nil
	}

	// For comparison operators, extract column and value values
	if expr.Left == nil || expr.Right == nil {
		return false, nil
	}

	leftValue, err := ve.extractValue(row, expr.Left)
	if err != nil {
		return false, err
	}

	rightValue, err := ve.extractValue(row, expr.Right)
	if err != nil {
		return false, err
	}

	// Perform comparison
	return ve.compareValues(leftValue, rightValue, op)
}

// extractValue extracts a value from an expression
func (ve *ViewExecutor) extractValue(row domain.Row, expr *parser.Expression) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}

	switch expr.Type {
	case parser.ExprTypeValue:
		return expr.Value, nil

	case parser.ExprTypeColumn:
		val, exists := row[expr.Column]
		if !exists {
			return nil, nil
		}
		return val, nil

	case parser.ExprTypeFunction:
		// Not implemented
		return nil, nil

	default:
		return nil, nil
	}
}

// compareValues compares two values
func (ve *ViewExecutor) compareValues(left, right interface{}, operator string) (bool, error) {
	// Handle nil values
	if left == nil || right == nil {
		// NULL comparisons always return false (except IS NULL)
		if operator == "!=" {
			return left != right, nil
		}
		return false, nil
	}

	// Try to convert to float64 for numeric comparison
	leftFloat, leftOk := ve.toFloat64(left)
	rightFloat, rightOk := ve.toFloat64(right)

	if leftOk && rightOk {
		// Numeric comparison
		switch operator {
		case "=":
			return leftFloat == rightFloat, nil
		case "!=":
			return leftFloat != rightFloat, nil
		case ">":
			return leftFloat > rightFloat, nil
		case "<":
			return leftFloat < rightFloat, nil
		case ">=":
			return leftFloat >= rightFloat, nil
		case "<=":
			return leftFloat <= rightFloat, nil
		}
	}

	// String comparison
	leftStr, leftOk := left.(string)
	rightStr, rightOk := right.(string)

	if leftOk && rightOk {
		switch operator {
		case "=":
			return leftStr == rightStr, nil
		case "!=":
			return leftStr != rightStr, nil
		case ">":
			return leftStr > rightStr, nil
		case "<":
			return leftStr < rightStr, nil
		case ">=":
			return leftStr >= rightStr, nil
		case "<=":
			return leftStr <= rightStr, nil
		case "LIKE":
			// Simple LIKE implementation (case-insensitive)
			pattern := strings.ToLower(rightStr)
			text := strings.ToLower(leftStr)
			return strings.Contains(text, pattern), nil
		}
	}

	return false, nil
}

// toFloat64 converts a value to float64 if possible
func (ve *ViewExecutor) toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

// isTruthy checks if a value is truthy
func (ve *ViewExecutor) isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case string:
		return v != ""
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
	default:
		return true
	}
}

// sortRows sorts rows by ORDER BY clause
func (ve *ViewExecutor) sortRows(rows []domain.Row, orderBy []parser.OrderByItem) {
	if len(orderBy) == 0 {
		return
	}

	// Use first ORDER BY item (simplified)
	orderByItem := orderBy[0]

	sort.SliceStable(rows, func(i, j int) bool {
		val1, exists1 := rows[i][orderByItem.Column]
		val2, exists2 := rows[j][orderByItem.Column]

		if !exists1 && !exists2 {
			return false
		}
		if !exists1 {
			return false
		}
		if !exists2 {
			return true
		}

		// Compare values
		cmp := ve.compareForSort(val1, val2)
		if orderByItem.Direction == "DESC" {
			return cmp > 0
		}
		return cmp < 0
	})
}

// compareForSort compares two values for sorting
func (ve *ViewExecutor) compareForSort(val1, val2 interface{}) int {
	// Handle nil values
	if val1 == nil && val2 == nil {
		return 0
	}
	if val1 == nil {
		return -1
	}
	if val2 == nil {
		return 1
	}

	// Try numeric comparison
	if num1, ok1 := ve.toFloat64(val1); ok1 {
		if num2, ok2 := ve.toFloat64(val2); ok2 {
			if num1 < num2 {
				return -1
			}
			if num1 > num2 {
				return 1
			}
			return 0
		}
	}

	// String comparison
	if str1, ok1 := val1.(string); ok1 {
		if str2, ok2 := val2.(string); ok2 {
			if str1 < str2 {
				return -1
			}
			if str1 > str2 {
				return 1
			}
			return 0
		}
	}

	return 0
}

// isSelectAll checks if column list is SELECT *
func (ve *ViewExecutor) isSelectAll(cols []parser.SelectColumn) bool {
	for _, col := range cols {
		if col.IsWildcard {
			return true
		}
	}
	return false
}

// selectColumns selects specific columns from result
func (ve *ViewExecutor) selectColumns(result *domain.QueryResult, columns []parser.SelectColumn, viewInfo *domain.ViewInfo) *domain.QueryResult {
	// Build column name mapping
	colMap := make(map[string]bool)
	for _, col := range columns {
		if col.Name != "" {
			colMap[col.Name] = true
		}
		if col.Alias != "" {
			colMap[col.Alias] = true
		}
	}

	// If no specific columns selected, return all
	if len(colMap) == 0 {
		return result
	}

	selected := &domain.QueryResult{
		Columns: make([]domain.ColumnInfo, 0),
		Rows:    make([]domain.Row, 0, len(result.Rows)),
		Total:   result.Total,
	}

	// Build selected columns
	for _, colInfo := range result.Columns {
		if colMap[colInfo.Name] {
			selected.Columns = append(selected.Columns, colInfo)
		}
	}

	// If no columns selected, add requested columns with default info
	if len(selected.Columns) == 0 {
		for colName := range colMap {
			selected.Columns = append(selected.Columns, domain.ColumnInfo{
				Name:     colName,
				Type:     "text",
				Nullable: true,
			})
		}
	}

	// Build selected rows
	for _, row := range result.Rows {
		selectedRow := make(domain.Row)
		for colName := range colMap {
			if val, exists := row[colName]; exists {
				selectedRow[colName] = val
			}
		}
		selected.Rows = append(selected.Rows, selectedRow)
	}

	return selected
}
