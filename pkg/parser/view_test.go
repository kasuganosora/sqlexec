package parser

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestCheckOptionValidator(t *testing.T) {
	// Test with NO CHECK OPTION
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT id, name FROM users WHERE active = 1",
		CheckOption: domain.ViewCheckOptionNone,
	}

	validator := NewCheckOptionValidator(viewInfo)

	// Should pass since no CHECK OPTION
	row := domain.Row{"id": 1, "name": "Alice", "active": 1}
	err := validator.ValidateInsert(row)
	t.Logf("Test 1: ValidateInsert with NO CHECK OPTION, row=%v, err=%v", row, err)
	if err != nil {
		t.Errorf("ValidateInsert should not fail with NO CHECK OPTION: %v", err)
	}

	// Test with CASCADED CHECK OPTION
	viewInfo.CheckOption = domain.ViewCheckOptionCascaded
	validator = NewCheckOptionValidator(viewInfo)

	// Should pass - row satisfies WHERE clause
	t.Logf("Test 2: ValidateInsert with CASCADED, valid row (active=1)")
	err = validator.ValidateInsert(row)
	t.Logf("Test 2 result: err=%v", err)
	if err != nil {
		t.Errorf("ValidateInsert should pass for valid row: %v", err)
	}

	// Should fail - row doesn't satisfy WHERE clause
	t.Logf("Test 3: ValidateInsert with CASCADED, invalid row (active=0)")
	row = domain.Row{"id": 2, "name": "Bob", "active": 0}
	err = validator.ValidateInsert(row)
	t.Logf("Test 3 result: err=%v", err)
	if err == nil {
		t.Error("ValidateInsert should fail for row that doesn't satisfy WHERE clause")
	}
}

func TestCheckOptionValidatorUpdate(t *testing.T) {
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT id, name FROM users WHERE active = 1",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	validator := NewCheckOptionValidator(viewInfo)

	oldRow := domain.Row{"id": 1, "name": "Alice", "active": 1}

	// Valid update - keeps active = 1
	updates := domain.Row{"name": "Alice Updated"}
	err := validator.ValidateUpdate(oldRow, updates)
	if err != nil {
		t.Errorf("ValidateUpdate should pass for valid update: %v", err)
	}

	// Invalid update - changes active to 0
	updates = domain.Row{"active": 0}
	err = validator.ValidateUpdate(oldRow, updates)
	if err == nil {
		t.Error("ValidateUpdate should fail for update that violates WHERE clause")
	}
}

func TestCheckOptionValidatorNoWhere(t *testing.T) {
	// Test with no WHERE clause - should always pass
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT id, name FROM users",
		CheckOption: domain.ViewCheckOptionLocal,
	}

	validator := NewCheckOptionValidator(viewInfo)
	row := domain.Row{"id": 1, "name": "Alice"}

	err := validator.ValidateInsert(row)
	if err != nil {
		t.Errorf("ValidateInsert should pass when view has no WHERE clause: %v", err)
	}
}

func TestBuildSelectSQL(t *testing.T) {
	builder := &QueryBuilder{dataSource: nil}

	// Test basic SELECT
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "id"},
			{Name: "name"},
		},
		From: "users",
	}

	sql := builder.buildSelectSQL(stmt)
	expected := "SELECT id, name FROM users"
	if sql != expected {
		t.Errorf("buildSelectSQL = %q, want %q", sql, expected)
	}

	// Test SELECT with WHERE
	stmt.Where = &Expression{
		Type:     ExprTypeColumn,
		Column:   "active",
		Operator: "=",
		Value:    1,
	}

	sql = builder.buildSelectSQL(stmt)
	expected = "SELECT id, name FROM users WHERE active"
	if sql != expected {
		t.Errorf("buildSelectSQL with WHERE = %q, want %q", sql, expected)
	}

	// Test SELECT with ORDER BY
	stmt.Where = nil
	stmt.OrderBy = []OrderByItem{
		{Column: "id", Direction: "DESC"},
	}

	sql = builder.buildSelectSQL(stmt)
	expected = "SELECT id, name FROM users ORDER BY id DESC"
	if sql != expected {
		t.Errorf("buildSelectSQL with ORDER BY = %q, want %q", sql, expected)
	}

	// Test SELECT with LIMIT
	limit := int64(10)
	stmt.Limit = &limit

	sql = builder.buildSelectSQL(stmt)
	expected = "SELECT id, name FROM users ORDER BY id DESC LIMIT 10"
	if sql != expected {
		t.Errorf("buildSelectSQL with LIMIT = %q, want %q", sql, expected)
	}

	// Test SELECT with DISTINCT
	stmt.Limit = nil
	stmt.OrderBy = nil
	stmt.Distinct = true

	sql = builder.buildSelectSQL(stmt)
	expected = "SELECT DISTINCT id, name FROM users"
	if sql != expected {
		t.Errorf("buildSelectSQL with DISTINCT = %q, want %q", sql, expected)
	}
}

func TestBuildExpressionSQL(t *testing.T) {
	builder := &QueryBuilder{dataSource: nil}

	// Test column reference
	expr := &Expression{
		Type:   ExprTypeColumn,
		Column: "id",
	}

	sql := builder.buildExpressionSQL(expr)
	expected := "id"
	if sql != expected {
		t.Errorf("buildExpressionSQL(column) = %q, want %q", sql, expected)
	}

	// Test value
	expr = &Expression{
		Type:  ExprTypeValue,
		Value: 1,
	}

	sql = builder.buildExpressionSQL(expr)
	expected = "1"
	if sql != expected {
		t.Errorf("buildExpressionSQL(value) = %q, want %q", sql, expected)
	}

	// Test operator
	expr = &Expression{
		Type:     ExprTypeOperator,
		Operator: "and",
		Left: &Expression{
			Type:   ExprTypeColumn,
			Column: "active",
		},
		Right: &Expression{
			Type:  ExprTypeValue,
			Value: 1,
		},
	}

	sql = builder.buildExpressionSQL(expr)
	expected = "(active) AND (1)"
	if sql != expected {
		t.Errorf("buildExpressionSQL(operator) = %q, want %q", sql, expected)
	}
}

func TestGetViewInfo(t *testing.T) {
	builder := &QueryBuilder{dataSource: nil}

	// Test with view table
	tableInfo := &domain.TableInfo{
		Name: "user_view",
		Atts: map[string]interface{}{
			domain.ViewMetaKey: domain.ViewInfo{
				SelectStmt: "SELECT * FROM users",
			},
		},
	}

	viewInfo, isView := builder.getViewInfo(tableInfo)
	if !isView {
		t.Error("Expected table to be a view")
	}

	if viewInfo == nil {
		t.Error("Expected viewInfo to be non-nil")
	}

	// Test with non-view table
	tableInfo = &domain.TableInfo{
		Name: "users",
		Atts: map[string]interface{}{},
	}

	viewInfo, isView = builder.getViewInfo(tableInfo)
	if isView {
		t.Error("Expected table to NOT be a view")
	}

	if viewInfo != nil {
		t.Error("Expected viewInfo to be nil for non-view table")
	}
}

func TestSelectColumnAlias(t *testing.T) {
	// Test SelectColumn with alias
	col := SelectColumn{
		Name:  "id",
		Alias: "user_id",
	}

	if col.Alias != "user_id" {
		t.Errorf("SelectColumn.Alias = %q, want %q", col.Alias, "user_id")
	}

	if col.Name != "id" {
		t.Errorf("SelectColumn.Name = %q, want %q", col.Name, "id")
	}
}

func TestExpressionTypes(t *testing.T) {
	// Test different expression types
	exprs := []struct {
		expr     *Expression
		typeName string
	}{
		{
			&Expression{Type: ExprTypeColumn, Column: "id"},
			"ExprTypeColumn",
		},
		{
			&Expression{Type: ExprTypeValue, Value: 42},
			"ExprTypeValue",
		},
		{
			&Expression{Type: ExprTypeOperator, Operator: "="},
			"ExprTypeOperator",
		},
		{
			&Expression{Type: ExprTypeFunction, Function: "COUNT"},
			"ExprTypeFunction",
		},
	}

	for _, tc := range exprs {
		var typeName string
		switch tc.expr.Type {
		case ExprTypeColumn:
			typeName = "ExprTypeColumn"
		case ExprTypeValue:
			typeName = "ExprTypeValue"
		case ExprTypeOperator:
			typeName = "ExprTypeOperator"
		case ExprTypeFunction:
			typeName = "ExprTypeFunction"
		default:
			typeName = "Unknown"
		}

		if typeName != tc.typeName {
			t.Errorf("Expression type mismatch: got %q, want %q", typeName, tc.typeName)
		}
	}
}

func TestViewConstants(t *testing.T) {
	// Test view-related constants
	if domain.ViewMetaKey != "__view__" {
		t.Errorf("ViewMetaKey = %q, want %q", domain.ViewMetaKey, "__view__")
	}

	if domain.MaxViewDepth != 10 {
		t.Errorf("MaxViewDepth = %d, want %d", domain.MaxViewDepth, 10)
	}

	if domain.ViewAlgorithmMerge != "MERGE" {
		t.Errorf("ViewAlgorithmMerge = %q, want %q", domain.ViewAlgorithmMerge, "MERGE")
	}

	if domain.ViewAlgorithmTempTable != "TEMPTABLE" {
		t.Errorf("ViewAlgorithmTempTable = %q, want %q", domain.ViewAlgorithmTempTable, "TEMPTABLE")
	}

	if domain.ViewSecurityDefiner != "DEFINER" {
		t.Errorf("ViewSecurityDefiner = %q, want %q", domain.ViewSecurityDefiner, "DEFINER")
	}

	if domain.ViewSecurityInvoker != "INVOKER" {
		t.Errorf("ViewSecurityInvoker = %q, want %q", domain.ViewSecurityInvoker, "INVOKER")
	}

	if domain.ViewCheckOptionNone != "NONE" {
		t.Errorf("ViewCheckOptionNone = %q, want %q", domain.ViewCheckOptionNone, "NONE")
	}

	if domain.ViewCheckOptionCascaded != "CASCADED" {
		t.Errorf("ViewCheckOptionCascaded = %q, want %q", domain.ViewCheckOptionCascaded, "CASCADED")
	}

	if domain.ViewCheckOptionLocal != "LOCAL" {
		t.Errorf("ViewCheckOptionLocal = %q, want %q", domain.ViewCheckOptionLocal, "LOCAL")
	}
}

func TestViewInfoSerialization(t *testing.T) {
	viewInfo := domain.ViewInfo{
		Algorithm:   domain.ViewAlgorithmMerge,
		Definer:     "CURRENT_USER",
		Security:    domain.ViewSecurityDefiner,
		SelectStmt:  "SELECT id, name FROM users",
		CheckOption: domain.ViewCheckOptionLocal,
		Cols:        []string{"id", "name"},
		Updatable:   true,
	}

	// Verify all fields are set correctly
	if viewInfo.Algorithm != domain.ViewAlgorithmMerge {
		t.Errorf("Algorithm = %q, want %q", viewInfo.Algorithm, domain.ViewAlgorithmMerge)
	}

	if viewInfo.Definer != "CURRENT_USER" {
		t.Errorf("Definer = %q, want %q", viewInfo.Definer, "CURRENT_USER")
	}

	if viewInfo.Security != domain.ViewSecurityDefiner {
		t.Errorf("Security = %q, want %q", viewInfo.Security, domain.ViewSecurityDefiner)
	}

	if viewInfo.CheckOption != domain.ViewCheckOptionLocal {
		t.Errorf("CheckOption = %q, want %q", viewInfo.CheckOption, domain.ViewCheckOptionLocal)
	}

	if len(viewInfo.Cols) != 2 {
		t.Errorf("Cols length = %d, want %d", len(viewInfo.Cols), 2)
	}

	if !viewInfo.Updatable {
		t.Error("Updatable should be true")
	}
}

func TestContextWithValue(t *testing.T) {
	ctx := context.Background()

	// Test setting user in context
	user := "testuser"
	ctx = context.WithValue(ctx, "user", user)

	retrieved, ok := ctx.Value("user").(string)
	if !ok {
		t.Error("Failed to retrieve user from context")
	}

	if retrieved != user {
		t.Errorf("Retrieved user = %q, want %q", retrieved, user)
	}

	// Test context without user
	ctx = context.Background()
	_, ok = ctx.Value("user").(string)
	if ok {
		t.Error("Should not have user in context")
	}
}
