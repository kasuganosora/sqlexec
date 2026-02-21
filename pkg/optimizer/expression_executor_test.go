package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// TestNewExpressionExecutor tests the constructor
func TestNewExpressionExecutor(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	if executor == nil {
		t.Fatal("Expected executor to be created")
	}

	if executor.currentDB != "test_db" {
		t.Errorf("Expected currentDB to be 'test_db', got '%s'", executor.currentDB)
	}
}

// TestSetCurrentDB tests setting the current database
func TestSetCurrentDB(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("initial_db", nil, exprEvaluator)

	executor.SetCurrentDB("new_db")

	if executor.currentDB != "new_db" {
		t.Errorf("Expected currentDB to be 'new_db', got '%s'", executor.currentDB)
	}
}

// TestGenerateColumnName tests column name generation from expressions
func TestGenerateColumnName(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	tests := []struct {
		name     string
		expr     *parser.Expression
		expected string
	}{
		{
			name:     "value expression",
			expr:     &parser.Expression{Type: parser.ExprTypeValue, Value: "test_value"},
			expected: "test_value",
		},
		{
			name:     "nil value expression",
			expr:     &parser.Expression{Type: parser.ExprTypeValue, Value: nil},
			expected: "NULL",
		},
		{
			name:     "function expression",
			expr:     &parser.Expression{Type: parser.ExprTypeFunction, Function: "COUNT"},
			expected: "COUNT()",
		},
		{
			name:     "column expression",
			expr:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
			expected: "id",
		},
		{
			name:     "system variable expression",
			expr:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "@@version"},
			expected: "@@version",
		},
		{
			name:     "session variable expression",
			expr:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "@my_var"},
			expected: "@my_var",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.generateColumnName(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected column name '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestOperatorToSQL tests operator name to SQL symbol conversion
func TestOperatorToSQL(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	tests := []struct {
		name     string
		operator string
		expected string
	}{
		{"plus", "plus", "+"},
		{"minus", "minus", "-"},
		{"multiply", "mul", "*"},
		{"divide", "div", "/"},
		{"equal", "eq", "="},
		{"not equal", "neq", "!="},
		{"greater than", "gt", ">"},
		{"greater than or equal", "gte", ">="},
		{"less than", "lt", "<"},
		{"less than or equal", "lte", "<="},
		{"and", "and", " AND "},
		{"or", "or", " OR "},
		{"not", "not", "NOT "},
		{"like", "like", " LIKE "},
		{"unknown", "unknown_op", "unknown_op"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.operatorToSQL(tt.operator)
			if result != tt.expected {
				t.Errorf("Expected operator symbol '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestGenerateOperatorColumnName tests operator expression column name generation
func TestGenerateOperatorColumnName(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	tests := []struct {
		name     string
		expr     *parser.Expression
		expected string
	}{
		{
			name: "binary operator",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "plus",
				Left:     &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 2},
			},
			expected: "1+2",
		},
		{
			name: "unary operator",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "not",
				Left:     &parser.Expression{Type: parser.ExprTypeValue, Value: true},
			},
			expected: "NOT true",
		},
		{
			name: "right only operator",
			expr: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "minus",
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 5},
			},
			expected: "-5",
		},
		{
			name:     "empty operator",
			expr:     &parser.Expression{Type: parser.ExprTypeOperator, Operator: ""},
			expected: "expr",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.generateOperatorColumnName(tt.expr)
			if result != tt.expected {
				t.Errorf("Expected column name '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestEvaluateVariable tests variable evaluation
func TestEvaluateVariable(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	tests := []struct {
		name        string
		varName     string
		expectError bool
	}{
		{
			name:        "system variable with @@",
			varName:     "@@version",
			expectError: false,
		},
		{
			name:        "system variable with GLOBAL scope",
			varName:     "@@GLOBAL.version",
			expectError: false,
		},
		{
			name:        "system variable with SESSION scope",
			varName:     "@@SESSION.version",
			expectError: false,
		},
		{
			name:        "session variable with @",
			varName:     "@my_var",
			expectError: true,
		},
		{
			name:        "invalid variable",
			varName:     "invalid_var",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executor.evaluateVariable(tt.varName)
			if tt.expectError && err == nil {
				t.Errorf("Expected error for variable '%s'", tt.varName)
			}
			if !tt.expectError && err != nil {
				t.Errorf("Did not expect error for variable '%s': %v", tt.varName, err)
			}
		})
	}
}

// TestEvaluateSystemVariable tests system variable evaluation
func TestEvaluateSystemVariable(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	tests := []struct {
		name     string
		varName  string
		expected interface{}
		wantErr  bool
	}{
		{"version comment", "VERSION_COMMENT", "sqlexec MySQL-compatible database", false},
		{"version", "VERSION", "8.0.34-sqlexec", false},
		{"port", "PORT", "3307", false},
		{"hostname", "HOSTNAME", "localhost", false},
		{"datadir", "DATADIR", "/var/lib/mysql/", false},
		{"server_id", "SERVER_ID", "1", false},
		{"unknown variable", "UNKNOWN_VAR", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.evaluateSystemVariable(tt.varName)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for variable '%s'", tt.varName)
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error for variable '%s': %v", tt.varName, err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestEvaluateSessionVariable tests session variable evaluation
func TestEvaluateSessionVariable(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	_, err := executor.evaluateSessionVariable("@my_var")
	if err == nil {
		t.Error("Expected error for session variable (not supported)")
	}
}

// TestInferType tests type inference
func TestInferType(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	executor := NewExpressionExecutor("test_db", nil, exprEvaluator)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"nil value", nil, "null"},
		{"int value", 42, "int"},
		{"int64 value", int64(42), "int"},
		{"float value", 3.14, "float"},
		{"bool value", true, "bool"},
		{"string value", "test", "string"},
		{"unknown value", []int{1, 2, 3}, "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := executor.inferType(tt.value)
			if result != tt.expected {
				t.Errorf("Expected type '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestEvaluateComparisonOperators tests comparison operators (gt, lt, gte, lte, eq, neq)
func TestEvaluateComparisonOperators(t *testing.T) {
	exprEvaluator := NewExpressionEvaluatorWithoutAPI()
	ctx := NewSimpleExpressionContext(nil)

	tests := []struct {
		name     string
		operator string
		left     interface{}
		right    interface{}
		expected bool
	}{
		// Greater than (gt)
		{"gt - 5 > 3", "gt", 5, 3, true},
		{"gt - 3 > 5", "gt", 3, 5, false},
		{"gt - 5 > 5", "gt", 5, 5, false},
		{"> - 5 > 3", ">", 5, 3, true},
		{"> - 3 > 5", ">", 3, 5, false},

		// Less than (lt)
		{"lt - 3 < 5", "lt", 3, 5, true},
		{"lt - 5 < 3", "lt", 5, 3, false},
		{"lt - 5 < 5", "lt", 5, 5, false},
		{"< - 3 < 5", "<", 3, 5, true},
		{"< - 5 < 3", "<", 5, 3, false},

		// Greater than or equal (gte)
		{"gte - 5 >= 3", "gte", 5, 3, true},
		{"gte - 3 >= 5", "gte", 3, 5, false},
		{"gte - 5 >= 5", "gte", 5, 5, true},
		{">= - 5 >= 3", ">=", 5, 3, true},
		{">= - 3 >= 5", ">=", 3, 5, false},

		// Less than or equal (lte)
		{"lte - 3 <= 5", "lte", 3, 5, true},
		{"lte - 5 <= 3", "lte", 5, 3, false},
		{"lte - 5 <= 5", "lte", 5, 5, true},
		{"<= - 3 <= 5", "<=", 3, 5, true},
		{"<= - 5 <= 3", "<=", 5, 3, false},

		// Equal (eq)
		{"eq - 5 = 5", "eq", 5, 5, true},
		{"eq - 5 = 3", "eq", 5, 3, false},
		{"= - 5 = 5", "=", 5, 5, true},
		{"= - 5 = 3", "=", 5, 3, false},

		// Not equal (neq)
		{"neq - 5 != 3", "neq", 5, 3, true},
		{"neq - 5 != 5", "neq", 5, 5, false},
		{"!= - 5 != 3", "!=", 5, 3, true},
		{"!= - 5 != 5", "!=", 5, 5, false},
		{"<> - 5 <> 3", "<>", 5, 3, true},
		{"<> - 5 <> 5", "<>", 5, 5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: tt.operator,
				Left:     &parser.Expression{Type: parser.ExprTypeValue, Value: tt.left},
				Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: tt.right},
			}

			result, err := exprEvaluator.Evaluate(expr, ctx)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			boolResult, ok := result.(bool)
			if !ok {
				t.Errorf("Expected bool result, got %T", result)
				return
			}

			if boolResult != tt.expected {
				t.Errorf("Expected %v, got %v for %v %s %v", tt.expected, boolResult, tt.left, tt.operator, tt.right)
			}
		})
	}
}
