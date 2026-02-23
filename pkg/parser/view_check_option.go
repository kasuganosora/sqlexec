package parser

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// ViewResolver is a function that resolves a table name to its ViewInfo.
// Returns nil if the table is not a view. This allows the CheckOptionValidator
// to traverse parent views for CASCADED check option validation.
type ViewResolver func(tableName string) *domain.ViewInfo

// maxCascadeDepth limits recursion for CASCADED checks to prevent infinite
// loops caused by circular view references.
const maxCascadeDepth = 10

// CheckOptionValidator validates INSERT/UPDATE operations against WITH CHECK OPTION
type CheckOptionValidator struct {
	viewInfo     *domain.ViewInfo
	viewResolver ViewResolver
}

// NewCheckOptionValidator creates a new CHECK OPTION validator
func NewCheckOptionValidator(viewInfo *domain.ViewInfo) *CheckOptionValidator {
	return &CheckOptionValidator{
		viewInfo: viewInfo,
	}
}

// NewCheckOptionValidatorWithResolver creates a CHECK OPTION validator with a
// ViewResolver for cascaded parent view checks. The resolver is called to look
// up parent views by table name.
func NewCheckOptionValidatorWithResolver(viewInfo *domain.ViewInfo, resolver ViewResolver) *CheckOptionValidator {
	return &CheckOptionValidator{
		viewInfo:     viewInfo,
		viewResolver: resolver,
	}
}

// ValidateInsert validates an INSERT operation against view's CHECK OPTION
func (cv *CheckOptionValidator) ValidateInsert(row domain.Row) error {
	// Check if view has CHECK OPTION
	if cv.viewInfo.CheckOption == domain.ViewCheckOptionNone {
		return nil // No CHECK OPTION specified
	}

	// Parse view's SELECT statement to get WHERE clause
	whereClause, err := cv.extractViewWhereClause()
	if err != nil {
		return fmt.Errorf("failed to extract view WHERE clause: %w", err)
	}

	if whereClause == nil {
		return nil // No WHERE clause to validate
	}

	// Check if row satisfies WHERE clause
	if !cv.satisfiesWhereClause(row, whereClause) {
		return fmt.Errorf("check option failed: row does not satisfy view's WHERE clause")
	}

	// If CASCADED, also check against parent views
	if cv.viewInfo.CheckOption == domain.ViewCheckOptionCascaded {
		if err := cv.validateCascaded(row, 0); err != nil {
			return err
		}
	}

	return nil
}

// ValidateUpdate validates an UPDATE operation against view's CHECK OPTION
func (cv *CheckOptionValidator) ValidateUpdate(row domain.Row, updates domain.Row) error {
	// Check if view has CHECK OPTION
	if cv.viewInfo.CheckOption == domain.ViewCheckOptionNone {
		return nil
	}

	// Create updated row by applying updates
	updatedRow := cv.applyUpdates(row, updates)

	// Parse view's SELECT statement to get WHERE clause
	whereClause, err := cv.extractViewWhereClause()
	if err != nil {
		return fmt.Errorf("failed to extract view WHERE clause: %w", err)
	}

	if whereClause == nil {
		return nil
	}

	// Check if updated row satisfies WHERE clause
	if !cv.satisfiesWhereClause(updatedRow, whereClause) {
		return fmt.Errorf("check option failed: updated row does not satisfy view's WHERE clause")
	}

	// If CASCADED, also check against parent views
	if cv.viewInfo.CheckOption == domain.ViewCheckOptionCascaded {
		if err := cv.validateCascaded(updatedRow, 0); err != nil {
			return err
		}
	}

	return nil
}

// validateCascaded recursively validates a row against parent views'
// WHERE clauses. It parses the current view's SELECT to extract the FROM
// table, resolves that table as a view (if it is one via the ViewResolver),
// and checks its WHERE clause. If the parent view is also CASCADED, it
// recurses further up the view chain.
func (cv *CheckOptionValidator) validateCascaded(row domain.Row, depth int) error {
	if depth >= maxCascadeDepth {
		// Prevent infinite recursion from circular view references
		return nil
	}

	if cv.viewResolver == nil {
		// No resolver available: cannot look up parent views.
		// This is expected when the validator is used without DataSource access.
		return nil
	}

	// Extract the FROM table name from the current view's SELECT
	parentTableName := cv.extractFromTableName(cv.viewInfo.SelectStmt)
	if parentTableName == "" {
		return nil
	}

	// Resolve the parent table as a view
	parentViewInfo := cv.viewResolver(parentTableName)
	if parentViewInfo == nil {
		// Parent is a base table, not a view - no further checks needed
		return nil
	}

	// Parse the parent view's WHERE clause and validate the row against it
	parentValidator := &CheckOptionValidator{
		viewInfo:     parentViewInfo,
		viewResolver: cv.viewResolver,
	}

	parentWhere, err := parentValidator.extractViewWhereClause()
	if err != nil {
		return fmt.Errorf("failed to extract parent view WHERE clause: %w", err)
	}

	if parentWhere != nil {
		if !parentValidator.satisfiesWhereClause(row, parentWhere) {
			return fmt.Errorf("check option failed: row does not satisfy parent view's WHERE clause")
		}
	}

	// If the parent view also uses CASCADED, recurse into its parent
	if parentViewInfo.CheckOption == domain.ViewCheckOptionCascaded {
		return parentValidator.validateCascaded(row, depth+1)
	}

	return nil
}

// extractFromTableName extracts the FROM table name from a SELECT statement
// using the parser. Returns empty string if parsing fails or there is no FROM.
func (cv *CheckOptionValidator) extractFromTableName(selectStmt string) string {
	if selectStmt == "" {
		return ""
	}

	adapter := NewSQLAdapter()
	parseResult, err := adapter.Parse(selectStmt)
	if err != nil || !parseResult.Success || parseResult.Statement.Select == nil {
		return ""
	}

	return parseResult.Statement.Select.From
}

// extractViewWhereClause extracts WHERE clause from view's SELECT statement
func (cv *CheckOptionValidator) extractViewWhereClause() (*Expression, error) {
	if cv.viewInfo.SelectStmt == "" {
		return nil, nil
	}

	// Parse SELECT statement
	adapter := NewSQLAdapter()
	parseResult, err := adapter.Parse(cv.viewInfo.SelectStmt)
	if err != nil {
		return nil, err
	}

	if !parseResult.Success || parseResult.Statement.Select == nil {
		return nil, fmt.Errorf("invalid view SELECT statement")
	}

	// Return WHERE clause
	whereClause := parseResult.Statement.Select.Where

	return whereClause, nil
}

// satisfiesWhereClause checks if a row satisfies WHERE clause
func (cv *CheckOptionValidator) satisfiesWhereClause(row domain.Row, expr *Expression) bool {
	if expr == nil {
		return true
	}

	// Evaluate expression
	result, err := cv.evaluateExpression(row, expr)
	if err != nil {
		// Fail-safe: reject the operation if validation cannot be performed
		return false
	}
	return result
}

// evaluateExpression evaluates an expression against a row
func (cv *CheckOptionValidator) evaluateExpression(row domain.Row, expr *Expression) (bool, error) {
	if expr == nil {
		return true, nil
	}

	switch expr.Type {
	case ExprTypeColumn:
		// Column reference - check if it exists and has a truthy value
		val, exists := row[expr.Column]
		if !exists {
			return false, nil
		}
		return cv.isTruthy(val), nil

	case ExprTypeValue:
		// Constant value
		return cv.isTruthy(expr.Value), nil

	case ExprTypeOperator:
		// Binary or logical operator
		return cv.evaluateOperator(row, expr)

	case ExprTypeFunction:
		// Function call - simplified implementation
		// For common functions like IS NULL, IS NOT NULL
		if strings.ToUpper(expr.Function) == "ISNULL" {
			if len(expr.Args) > 0 {
				arg := expr.Args[0]
				val, _ := cv.extractValue(row, &arg)
				return val == nil, nil
			}
		}
		return true, nil

	default:
		return true, nil
	}
}

// evaluateOperator evaluates a binary operator expression
func (cv *CheckOptionValidator) evaluateOperator(row domain.Row, expr *Expression) (bool, error) {
	if expr.Operator == "" {
		return true, nil
	}

	// Handle logical operators
	op := strings.ToUpper(expr.Operator)
	switch op {
	case "AND":
		left, _ := cv.evaluateExpression(row, expr.Left)
		right, _ := cv.evaluateExpression(row, expr.Right)
		return left && right, nil

	case "OR":
		left, _ := cv.evaluateExpression(row, expr.Left)
		right, _ := cv.evaluateExpression(row, expr.Right)
		return left || right, nil
	}

	// For comparison operators, extract left and right values
	if expr.Left == nil || expr.Right == nil {
		return false, nil
	}

	leftValue, err := cv.extractValue(row, expr.Left)
	if err != nil {
		return false, err
	}

	rightValue, err := cv.extractValue(row, expr.Right)
	if err != nil {
		return false, err
	}

	// Perform comparison
	return cv.compareValues(leftValue, rightValue, op)
}

// extractValue extracts a value from an expression
func (cv *CheckOptionValidator) extractValue(row domain.Row, expr *Expression) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}

	switch expr.Type {
	case ExprTypeValue:
		return expr.Value, nil

	case ExprTypeColumn:
		val, exists := row[expr.Column]
		if !exists {
			return nil, nil
		}
		return val, nil

	case ExprTypeFunction:
		// Not fully implemented
		return nil, nil

	default:
		return nil, nil
	}
}

// compareValues compares two values (using utils package)
func (cv *CheckOptionValidator) compareValues(left, right interface{}, operator string) (bool, error) {
	return utils.CompareValues(left, right, operator)
}

// isTruthy checks if a value is truthy
func (cv *CheckOptionValidator) isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case bool:
		return v
	case string:
		return v != ""
	case int:
		return v != 0
	case int8:
		return v != 0
	case int16:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case uint:
		return v != 0
	case uint8:
		return v != 0
	case uint16:
		return v != 0
	case uint32:
		return v != 0
	case uint64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	default:
		return true
	}
}

// applyUpdates applies updates to a row
func (cv *CheckOptionValidator) applyUpdates(row domain.Row, updates domain.Row) domain.Row {
	// Create a copy of the row
	result := make(domain.Row)
	for k, v := range row {
		result[k] = v
	}

	// Apply updates
	for k, v := range updates {
		result[k] = v
	}

	return result
}
