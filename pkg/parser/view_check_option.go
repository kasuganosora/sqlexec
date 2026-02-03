package parser

import (
	"fmt"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// CheckOptionValidator validates INSERT/UPDATE operations against WITH CHECK OPTION
type CheckOptionValidator struct {
	viewInfo *domain.ViewInfo
}

// NewCheckOptionValidator creates a new CHECK OPTION validator
func NewCheckOptionValidator(viewInfo *domain.ViewInfo) *CheckOptionValidator {
	return &CheckOptionValidator{
		viewInfo: viewInfo,
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

	// If CASCADED, also check against parent views (simplified)
	if cv.viewInfo.CheckOption == domain.ViewCheckOptionCascaded {
		// TODO: Implement cascaded check to parent views
		// For now, treat same as LOCAL
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
		// TODO: Implement cascaded check to parent views
	}

	return nil
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
		// Log error but return true to allow operation
		return true
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
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true
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
