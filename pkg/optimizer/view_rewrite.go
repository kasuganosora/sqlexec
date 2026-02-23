package optimizer

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// ViewRewriter handles view query rewriting using the MERGE algorithm
type ViewRewriter struct {
	viewDepth int
	maxDepth  int
}

// NewViewRewriter creates a new view rewriter
func NewViewRewriter() *ViewRewriter {
	return &ViewRewriter{
		viewDepth: 0,
		maxDepth:  domain.MaxViewDepth,
	}
}

// Rewrite merges outer query with view definition using MERGE algorithm
func (vr *ViewRewriter) Rewrite(outerQuery *parser.SelectStatement, viewInfo *domain.ViewInfo) (*parser.SelectStatement, error) {
	// Check view algorithm
	if viewInfo.Algorithm != "" && viewInfo.Algorithm != domain.ViewAlgorithmUndefined && viewInfo.Algorithm != domain.ViewAlgorithmMerge {
		return nil, fmt.Errorf("view does not use MERGE algorithm: %s", viewInfo.Algorithm)
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

	// Check view depth to prevent infinite recursion
	vr.viewDepth++
	if vr.viewDepth > vr.maxDepth {
		return nil, fmt.Errorf("maximum view nesting depth exceeded: %d", vr.maxDepth)
	}

	// Create merged SELECT statement
	merged := vr.mergeSelectStatements(outerQuery, viewSelect, viewInfo)

	return merged, nil
}

// mergeSelectStatements merges outer query with view SELECT
func (vr *ViewRewriter) mergeSelectStatements(outer, view *parser.SelectStatement, viewInfo *domain.ViewInfo) *parser.SelectStatement {
	merged := &parser.SelectStatement{
		Distinct: view.Distinct, // Keep view's DISTINCT
	}

	// Merge SELECT list
	if len(outer.Columns) > 0 && !vr.isSelectAll(outer.Columns) {
		// Outer query specifies columns - use those with proper column mapping
		merged.Columns = vr.mergeSelectColumns(outer.Columns, view.Columns, viewInfo)
	} else {
		// Outer query uses SELECT * - use view's columns
		merged.Columns = view.Columns
	}

	// Merge FROM clause - use view's FROM
	merged.From = view.From
	if view.From == "" {
		// If view doesn't have FROM (e.g., SELECT 1+1), create a merged FROM
		merged.From = vr.buildMergedFrom(view, outer)
	}

	// Merge JOINs
	if len(view.Joins) > 0 {
		merged.Joins = view.Joins
	}
	if len(outer.Joins) > 0 {
		merged.Joins = append(merged.Joins, outer.Joins...)
	}

	// Merge WHERE clauses
	merged.Where = vr.mergeWhereClauses(outer.Where, view.Where)

	// Merge GROUP BY
	if len(outer.GroupBy) > 0 {
		// Outer query has GROUP BY - use it
		merged.GroupBy = outer.GroupBy
	} else {
		// Use view's GROUP BY
		merged.GroupBy = view.GroupBy
	}

	// Merge HAVING
	merged.Having = vr.mergeHavingClauses(outer.Having, view.Having)

	// Merge ORDER BY - outer query takes precedence
	if len(outer.OrderBy) > 0 {
		merged.OrderBy = outer.OrderBy
	} else {
		merged.OrderBy = view.OrderBy
	}

	// Merge LIMIT/OFFSET
	if outer.Limit != nil {
		merged.Limit = outer.Limit
	} else {
		merged.Limit = view.Limit
	}

	if outer.Offset != nil {
		merged.Offset = outer.Offset
	} else {
		merged.Offset = view.Offset
	}

	return merged
}

// mergeSelectColumns merges outer and view SELECT columns
func (vr *ViewRewriter) mergeSelectColumns(outerCols, viewCols []parser.SelectColumn, viewInfo *domain.ViewInfo) []parser.SelectColumn {
	// If view has explicit column list, map outer columns to view columns
	if len(viewInfo.Cols) > 0 {
		return vr.mapColumnsByViewDefinition(outerCols, viewInfo.Cols)
	}

	// Otherwise, directly use outer columns mapped to view columns
	merged := make([]parser.SelectColumn, 0, len(outerCols))

	for _, outerCol := range outerCols {
		// Find corresponding column in view
		var viewCol *parser.SelectColumn
		for _, vc := range viewCols {
			if vc.Name == outerCol.Name || vc.Alias == outerCol.Name {
				viewCol = &vc
				break
			}
		}

		if viewCol != nil {
			// Use view column definition but with outer alias
			newCol := *viewCol
			if outerCol.Alias != "" {
				newCol.Alias = outerCol.Alias
			}
			merged = append(merged, newCol)
		} else if outerCol.Expr != nil {
			// Expression or column not in view - use as-is
			merged = append(merged, outerCol)
		}
	}

	return merged
}

// mapColumnsByViewDefinition maps outer columns to view's column list
func (vr *ViewRewriter) mapColumnsByViewDefinition(outerCols []parser.SelectColumn, viewCols []string) []parser.SelectColumn {
	merged := make([]parser.SelectColumn, 0, len(outerCols))

	for _, outerCol := range outerCols {
		// Check if outer column name is in view's column list
		found := false
		for i, viewColName := range viewCols {
			if outerCol.Name == viewColName || outerCol.Alias == viewColName {
				// Map to the corresponding view column
				merged = append(merged, parser.SelectColumn{
					Name:  viewColName,
					Alias: outerCol.Alias,
					Table: outerCol.Table,
					Expr: &parser.Expression{
						Type:   parser.ExprTypeColumn,
						Column: viewColName,
					},
				})
				found = true
				break
			} else if i < len(viewCols) && outerCol.Name == fmt.Sprintf("col%d", i+1) {
				// Position-based mapping (SELECT col1, col2...)
				merged = append(merged, parser.SelectColumn{
					Name:  viewColName,
					Alias: outerCol.Alias,
				})
				found = true
				break
			}
		}

		if !found {
			// Column not found in view - use as-is
			merged = append(merged, outerCol)
		}
	}

	return merged
}

// buildMergedFrom builds FROM clause when view doesn't have one
func (vr *ViewRewriter) buildMergedFrom(view, outer *parser.SelectStatement) string {
	if view.From != "" {
		return view.From
	}
	if outer.From != "" {
		return outer.From
	}
	return ""
}

// mergeWhereClauses merges WHERE clauses from outer and view
func (vr *ViewRewriter) mergeWhereClauses(outerWhere, viewWhere *parser.Expression) *parser.Expression {
	if outerWhere == nil && viewWhere == nil {
		return nil
	}

	if outerWhere == nil {
		return viewWhere
	}

	if viewWhere == nil {
		return outerWhere
	}

	// Combine with AND
	return &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "AND",
		Left:     viewWhere,
		Right:    outerWhere,
	}
}

// mergeHavingClauses merges HAVING clauses
func (vr *ViewRewriter) mergeHavingClauses(outerHaving, viewHaving *parser.Expression) *parser.Expression {
	if outerHaving == nil && viewHaving == nil {
		return nil
	}

	if outerHaving == nil {
		return viewHaving
	}

	if viewHaving == nil {
		return outerHaving
	}

	// Combine with AND
	return &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "AND",
		Left:     viewHaving,
		Right:    outerHaving,
	}
}

// isSelectAll checks if the column list is SELECT *
func (vr *ViewRewriter) isSelectAll(cols []parser.SelectColumn) bool {
	if len(cols) == 1 && cols[0].IsWildcard {
		return true
	}
	for _, col := range cols {
		if col.IsWildcard {
			return true
		}
	}
	return false
}

// IsUpdatable checks if a view is updatable based on its definition
func IsUpdatable(viewInfo *domain.ViewInfo) bool {
	// Simplified implementation - check for non-updatable patterns
	if viewInfo == nil {
		return true // Default to updatable
	}

	// Check for aggregates in SELECT statement
	aggregates := []string{"count(", "sum(", "avg(", "min(", "max(", "group_concat(", "std(", "stddev("}
	selectStmt := viewInfo.SelectStmt
	if selectStmt == "" {
		return true
	}

	lowerStmt := strings.ToLower(selectStmt)
	for _, agg := range aggregates {
		if strings.Contains(lowerStmt, strings.ToLower(agg)) {
			return false // Contains aggregate function
		}
	}

	// Check for DISTINCT
	if strings.Contains(lowerStmt, "distinct ") {
		return false
	}

	// Check for GROUP BY
	if strings.Contains(lowerStmt, "group by ") {
		return false
	}

	// Check for HAVING
	if strings.Contains(lowerStmt, "having ") {
		return false
	}

	// Check for UNION
	if strings.Contains(lowerStmt, "union ") {
		return false
	}

	// Check for subqueries (simplified)
	if strings.Contains(lowerStmt, "select ") && strings.Contains(lowerStmt, " from ") && strings.Contains(lowerStmt, " where ") {
		// This is a heuristic check
		return false
	}

	return true
}
