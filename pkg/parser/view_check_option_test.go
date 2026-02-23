package parser

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckOptionValidator_NoCheckOption(t *testing.T) {
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18",
		CheckOption: domain.ViewCheckOptionNone,
	}

	cv := NewCheckOptionValidator(viewInfo)

	// No check option means all inserts/updates should pass
	row := domain.Row{"name": "Alice", "age": 10}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)

	err = cv.ValidateUpdate(domain.Row{"name": "Bob", "age": 20}, domain.Row{"age": 10})
	assert.NoError(t, err)
}

func TestCheckOptionValidator_LocalCheckOption_PassingRow(t *testing.T) {
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	cv := NewCheckOptionValidator(viewInfo)

	// Row satisfies WHERE age > 18
	row := domain.Row{"name": "Alice", "age": 25}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)
}

func TestCheckOptionValidator_LocalCheckOption_FailingRow(t *testing.T) {
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	cv := NewCheckOptionValidator(viewInfo)

	// Row does NOT satisfy WHERE age > 18
	row := domain.Row{"name": "Alice", "age": 10}
	err := cv.ValidateInsert(row)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "check option failed")
}

func TestCheckOptionValidator_CascadedCheckOption_NoResolver(t *testing.T) {
	// CASCADED without a resolver behaves like LOCAL
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	cv := NewCheckOptionValidator(viewInfo)
	// No resolver set - cascaded defaults to local-only check

	// Row satisfies local WHERE
	row := domain.Row{"name": "Alice", "age": 25}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)

	// Row does NOT satisfy local WHERE
	row = domain.Row{"name": "Bob", "age": 10}
	err = cv.ValidateInsert(row)
	assert.Error(t, err)
}

func TestCheckOptionValidator_CascadedCheckOption_WithResolver(t *testing.T) {
	// This view: SELECT * FROM base_view WHERE age < 65
	// Parent view (base_view): SELECT * FROM users WHERE age > 18
	// CASCADED means row must satisfy BOTH conditions: age > 18 AND age < 65

	parentViewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18",
		CheckOption: domain.ViewCheckOptionNone,
	}

	childViewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM base_view WHERE age < 65",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	resolver := func(tableName string) *domain.ViewInfo {
		if tableName == "base_view" {
			return parentViewInfo
		}
		return nil
	}

	cv := NewCheckOptionValidatorWithResolver(childViewInfo, resolver)

	// Row satisfies both: age > 18 AND age < 65
	row := domain.Row{"name": "Alice", "age": 30}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)

	// Row satisfies child WHERE (age < 65) but NOT parent (age > 18)
	row = domain.Row{"name": "Bob", "age": 10}
	err = cv.ValidateInsert(row)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "check option failed")

	// Row satisfies parent WHERE (age > 18) but NOT child (age < 65)
	row = domain.Row{"name": "Charlie", "age": 70}
	err = cv.ValidateInsert(row)
	assert.Error(t, err)
}

func TestCheckOptionValidator_CascadedUpdate_WithResolver(t *testing.T) {
	parentViewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18",
		CheckOption: domain.ViewCheckOptionNone,
	}

	childViewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM base_view WHERE age < 65",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	resolver := func(tableName string) *domain.ViewInfo {
		if tableName == "base_view" {
			return parentViewInfo
		}
		return nil
	}

	cv := NewCheckOptionValidatorWithResolver(childViewInfo, resolver)

	// Update that satisfies both conditions
	err := cv.ValidateUpdate(domain.Row{"name": "Alice", "age": 30}, domain.Row{"age": 40})
	assert.NoError(t, err)

	// Update that violates parent (age > 18)
	err = cv.ValidateUpdate(domain.Row{"name": "Alice", "age": 30}, domain.Row{"age": 10})
	assert.Error(t, err)
}

func TestCheckOptionValidator_CascadedNested_WithResolver(t *testing.T) {
	// Three levels: grandparent -> parent -> child
	// grandparent: SELECT * FROM users WHERE status = 'active'
	// parent: SELECT * FROM gp_view WHERE age > 18
	// child: SELECT * FROM parent_view WHERE age < 65

	grandparentView := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE status = 'active'",
		CheckOption: domain.ViewCheckOptionNone,
	}

	parentView := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM gp_view WHERE age > 18",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	childView := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM parent_view WHERE age < 65",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	resolver := func(tableName string) *domain.ViewInfo {
		switch tableName {
		case "gp_view":
			return grandparentView
		case "parent_view":
			return parentView
		default:
			return nil
		}
	}

	cv := NewCheckOptionValidatorWithResolver(childView, resolver)

	// Satisfies all: status='active', age>18, age<65
	row := domain.Row{"name": "Alice", "age": 30, "status": "active"}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)

	// Fails grandparent (status != 'active')
	row = domain.Row{"name": "Bob", "age": 30, "status": "inactive"}
	err = cv.ValidateInsert(row)
	assert.Error(t, err)
}

func TestCheckOptionValidator_NoWhereClause(t *testing.T) {
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	cv := NewCheckOptionValidator(viewInfo)

	// No WHERE clause means all rows pass
	row := domain.Row{"name": "Alice", "age": 10}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)
}

func TestCheckOptionValidator_EmptySelectStmt(t *testing.T) {
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	cv := NewCheckOptionValidator(viewInfo)

	row := domain.Row{"name": "Alice"}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)
}

func TestCheckOptionValidator_MaxDepthProtection(t *testing.T) {
	// Circular reference protection: view A references view B which references view A
	viewA := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM view_b WHERE a > 1",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	viewB := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM view_a WHERE b > 2",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	resolver := func(tableName string) *domain.ViewInfo {
		switch tableName {
		case "view_a":
			return viewA
		case "view_b":
			return viewB
		default:
			return nil
		}
	}

	cv := NewCheckOptionValidatorWithResolver(viewA, resolver)

	// Should not infinite loop; max depth protection stops recursion
	row := domain.Row{"a": 5, "b": 5}
	err := cv.ValidateInsert(row)
	// Even if there's a depth error, it shouldn't panic
	require.NotPanics(t, func() {
		_ = cv.ValidateInsert(row)
	})
	_ = err // Result may vary depending on depth limit behavior
}

func TestExtractFromTableName(t *testing.T) {
	cv := NewCheckOptionValidator(&domain.ViewInfo{})

	// Simple SELECT
	name := cv.extractFromTableName("SELECT * FROM users WHERE age > 18")
	assert.Equal(t, "users", name)

	// With schema
	name = cv.extractFromTableName("SELECT * FROM my_view WHERE x = 1")
	assert.Equal(t, "my_view", name)

	// Empty
	name = cv.extractFromTableName("")
	assert.Equal(t, "", name)

	// No FROM
	name = cv.extractFromTableName("SELECT 1")
	assert.Equal(t, "", name)
}

// ---------------------------------------------------------------------------
// New unit tests
// ---------------------------------------------------------------------------

func TestCheckOptionValidator_IsTruthy(t *testing.T) {
	cv := NewCheckOptionValidator(&domain.ViewInfo{})

	// nil is falsy
	assert.False(t, cv.isTruthy(nil))

	// bool
	assert.True(t, cv.isTruthy(true))
	assert.False(t, cv.isTruthy(false))

	// string
	assert.True(t, cv.isTruthy("hello"))
	assert.False(t, cv.isTruthy(""))

	// int types - zero and non-zero
	assert.False(t, cv.isTruthy(int(0)))
	assert.True(t, cv.isTruthy(int(42)))

	assert.False(t, cv.isTruthy(int8(0)))
	assert.True(t, cv.isTruthy(int8(1)))

	assert.False(t, cv.isTruthy(int16(0)))
	assert.True(t, cv.isTruthy(int16(-1)))

	assert.False(t, cv.isTruthy(int32(0)))
	assert.True(t, cv.isTruthy(int32(100)))

	assert.False(t, cv.isTruthy(int64(0)))
	assert.True(t, cv.isTruthy(int64(999)))

	// uint types - zero and non-zero
	assert.False(t, cv.isTruthy(uint(0)))
	assert.True(t, cv.isTruthy(uint(1)))

	assert.False(t, cv.isTruthy(uint8(0)))
	assert.True(t, cv.isTruthy(uint8(255)))

	assert.False(t, cv.isTruthy(uint16(0)))
	assert.True(t, cv.isTruthy(uint16(65535)))

	assert.False(t, cv.isTruthy(uint32(0)))
	assert.True(t, cv.isTruthy(uint32(1)))

	assert.False(t, cv.isTruthy(uint64(0)))
	assert.True(t, cv.isTruthy(uint64(1)))

	// float types - zero and non-zero
	assert.False(t, cv.isTruthy(float32(0)))
	assert.True(t, cv.isTruthy(float32(3.14)))

	assert.False(t, cv.isTruthy(float64(0)))
	assert.True(t, cv.isTruthy(float64(-0.5)))

	// Unknown type (struct) falls into default → truthy
	assert.True(t, cv.isTruthy(struct{}{}))
}

func TestCheckOptionValidator_EvaluateExpression_FunctionISNULL(t *testing.T) {
	cv := NewCheckOptionValidator(&domain.ViewInfo{})

	// ISNULL with nil column value → true
	expr := &Expression{
		Type:     ExprTypeFunction,
		Function: "ISNULL",
		Args: []Expression{
			{Type: ExprTypeColumn, Column: "email"},
		},
	}
	row := domain.Row{"email": nil}
	result, err := cv.evaluateExpression(row, expr)
	require.NoError(t, err)
	assert.True(t, result, "ISNULL should return true for nil value")

	// ISNULL with missing column (extractValue returns nil) → true
	row2 := domain.Row{"name": "Alice"}
	result, err = cv.evaluateExpression(row2, expr)
	require.NoError(t, err)
	assert.True(t, result, "ISNULL should return true for missing column")

	// ISNULL with non-nil value → false
	row3 := domain.Row{"email": "alice@example.com"}
	result, err = cv.evaluateExpression(row3, expr)
	require.NoError(t, err)
	assert.False(t, result, "ISNULL should return false for non-nil value")
}

func TestCheckOptionValidator_EvaluateExpression_AndOr(t *testing.T) {
	cv := NewCheckOptionValidator(&domain.ViewInfo{})
	row := domain.Row{}

	trueExpr := &Expression{Type: ExprTypeValue, Value: true}
	falseExpr := &Expression{Type: ExprTypeValue, Value: false}

	// true AND true → true
	expr := &Expression{
		Type:     ExprTypeOperator,
		Operator: "AND",
		Left:     trueExpr,
		Right:    trueExpr,
	}
	result, err := cv.evaluateExpression(row, expr)
	require.NoError(t, err)
	assert.True(t, result, "true AND true should be true")

	// true AND false → false
	expr = &Expression{
		Type:     ExprTypeOperator,
		Operator: "AND",
		Left:     trueExpr,
		Right:    falseExpr,
	}
	result, err = cv.evaluateExpression(row, expr)
	require.NoError(t, err)
	assert.False(t, result, "true AND false should be false")

	// true OR false → true
	expr = &Expression{
		Type:     ExprTypeOperator,
		Operator: "OR",
		Left:     trueExpr,
		Right:    falseExpr,
	}
	result, err = cv.evaluateExpression(row, expr)
	require.NoError(t, err)
	assert.True(t, result, "true OR false should be true")

	// false OR false → false
	expr = &Expression{
		Type:     ExprTypeOperator,
		Operator: "OR",
		Left:     falseExpr,
		Right:    falseExpr,
	}
	result, err = cv.evaluateExpression(row, expr)
	require.NoError(t, err)
	assert.False(t, result, "false OR false should be false")
}

func TestCheckOptionValidator_ApplyUpdates(t *testing.T) {
	cv := NewCheckOptionValidator(&domain.ViewInfo{})

	original := domain.Row{
		"name":   "Alice",
		"age":    25,
		"status": "active",
	}
	updates := domain.Row{
		"age":   30,
		"email": "alice@example.com",
	}

	merged := cv.applyUpdates(original, updates)

	// Original keys preserved when not in updates
	assert.Equal(t, "Alice", merged["name"])
	assert.Equal(t, "active", merged["status"])

	// Update keys overwrite original
	assert.Equal(t, 30, merged["age"])

	// New keys from updates are added
	assert.Equal(t, "alice@example.com", merged["email"])

	// Original row is not modified (copy semantics)
	assert.Equal(t, 25, original["age"])
	_, hasEmail := original["email"]
	assert.False(t, hasEmail, "original row should not have the 'email' key")
}

func TestCheckOptionValidator_CompareValues(t *testing.T) {
	cv := NewCheckOptionValidator(&domain.ViewInfo{})

	// = operator
	result, err := cv.compareValues(10, 10, "=")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(10, 20, "=")
	require.NoError(t, err)
	assert.False(t, result)

	// != operator
	result, err = cv.compareValues(10, 20, "!=")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(10, 10, "!=")
	require.NoError(t, err)
	assert.False(t, result)

	// > operator
	result, err = cv.compareValues(20, 10, ">")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(10, 20, ">")
	require.NoError(t, err)
	assert.False(t, result)

	// < operator
	result, err = cv.compareValues(10, 20, "<")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(20, 10, "<")
	require.NoError(t, err)
	assert.False(t, result)

	// >= operator
	result, err = cv.compareValues(20, 10, ">=")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(10, 10, ">=")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(9, 10, ">=")
	require.NoError(t, err)
	assert.False(t, result)

	// <= operator
	result, err = cv.compareValues(10, 20, "<=")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(10, 10, "<=")
	require.NoError(t, err)
	assert.True(t, result)

	result, err = cv.compareValues(11, 10, "<=")
	require.NoError(t, err)
	assert.False(t, result)
}

func TestCheckOptionValidator_ExtractValue_AllTypes(t *testing.T) {
	cv := NewCheckOptionValidator(&domain.ViewInfo{})
	row := domain.Row{"name": "Alice", "age": 30}

	// ExprTypeValue - returns the literal value
	val, err := cv.extractValue(row, &Expression{Type: ExprTypeValue, Value: 42})
	require.NoError(t, err)
	assert.Equal(t, 42, val)

	// ExprTypeColumn - column exists
	val, err = cv.extractValue(row, &Expression{Type: ExprTypeColumn, Column: "name"})
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)

	// ExprTypeColumn - column does not exist
	val, err = cv.extractValue(row, &Expression{Type: ExprTypeColumn, Column: "missing"})
	require.NoError(t, err)
	assert.Nil(t, val)

	// ExprTypeFunction - returns nil (not fully implemented)
	val, err = cv.extractValue(row, &Expression{Type: ExprTypeFunction, Function: "NOW"})
	require.NoError(t, err)
	assert.Nil(t, val)

	// nil expression - returns nil
	val, err = cv.extractValue(row, nil)
	require.NoError(t, err)
	assert.Nil(t, val)

	// Unknown/default type - returns nil
	val, err = cv.extractValue(row, &Expression{Type: ExprType("UNKNOWN_TYPE")})
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestCheckOptionValidator_ValidateInsert_ComplexWhere(t *testing.T) {
	// View with WHERE age > 18 AND status = 'active'
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18 AND status = 'active'",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	cv := NewCheckOptionValidator(viewInfo)

	// Row satisfies both conditions → pass
	row := domain.Row{"name": "Alice", "age": 25, "status": "active"}
	err := cv.ValidateInsert(row)
	assert.NoError(t, err)

	// Row fails age condition (age <= 18) → fail
	row = domain.Row{"name": "Bob", "age": 16, "status": "active"}
	err = cv.ValidateInsert(row)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "check option failed")

	// Row fails status condition → fail
	row = domain.Row{"name": "Charlie", "age": 25, "status": "inactive"}
	err = cv.ValidateInsert(row)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "check option failed")

	// Row fails both conditions → fail
	row = domain.Row{"name": "Dave", "age": 10, "status": "inactive"}
	err = cv.ValidateInsert(row)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "check option failed")
}

func TestCheckOptionValidator_ValidateUpdate_AppliesUpdates(t *testing.T) {
	// View with WHERE age > 18
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE age > 18",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	cv := NewCheckOptionValidator(viewInfo)

	// Original row has age=25 (satisfies), update sets age=15 (violates) → should fail
	originalRow := domain.Row{"name": "Alice", "age": 25}
	updates := domain.Row{"age": 15}
	err := cv.ValidateUpdate(originalRow, updates)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "check option failed")

	// Confirm original row was not modified by the validation
	assert.Equal(t, 25, originalRow["age"])

	// Original row has age=25, update sets age=30 (still satisfies) → should pass
	updates = domain.Row{"age": 30}
	err = cv.ValidateUpdate(originalRow, updates)
	assert.NoError(t, err)

	// Original row has age=10 (violates), update sets age=25 (satisfies) → should pass
	originalRow2 := domain.Row{"name": "Bob", "age": 10}
	updates = domain.Row{"age": 25}
	err = cv.ValidateUpdate(originalRow2, updates)
	assert.NoError(t, err)

	// Update only non-WHERE columns; original satisfies → should pass
	originalRow3 := domain.Row{"name": "Charlie", "age": 25}
	updates = domain.Row{"name": "Charles"}
	err = cv.ValidateUpdate(originalRow3, updates)
	assert.NoError(t, err)
}
