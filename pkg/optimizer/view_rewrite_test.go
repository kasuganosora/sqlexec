package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestNewViewRewriter tests creating a view rewriter
func TestNewViewRewriter(t *testing.T) {
	rewriter := NewViewRewriter()

	if rewriter == nil {
		t.Fatal("Expected rewriter to be created")
	}

	if rewriter.viewDepth != 0 {
		t.Errorf("Expected viewDepth to be 0, got %d", rewriter.viewDepth)
	}

	if rewriter.maxDepth != domain.MaxViewDepth {
		t.Errorf("Expected maxDepth to be %d, got %d", domain.MaxViewDepth, rewriter.maxDepth)
	}
}

// TestRewrite_InvalidAlgorithm tests rewriting with invalid algorithm
func TestRewrite_InvalidAlgorithm(t *testing.T) {
	rewriter := NewViewRewriter()

	outerQuery := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "id"},
		},
	}

	viewInfo := &domain.ViewInfo{
		SelectStmt: "SELECT id FROM users",
		Algorithm:  domain.ViewAlgorithmTempTable, // Invalid for MERGE rewriter
	}

	_, err := rewriter.Rewrite(outerQuery, viewInfo)
	if err == nil {
		t.Error("Expected error for non-MERGE algorithm")
	}

	if err == nil || err.Error() != "view does not use MERGE algorithm: TEMPTABLE" {
		t.Errorf("Expected algorithm error, got: %v", err)
	}
}

// TestRewrite_InvalidViewSelect tests rewriting with invalid view SELECT
func TestRewrite_InvalidViewSelect(t *testing.T) {
	rewriter := NewViewRewriter()

	outerQuery := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "id"},
		},
	}

	viewInfo := &domain.ViewInfo{
		SelectStmt: "INVALID SQL",
		Algorithm:  domain.ViewAlgorithmMerge,
	}

	_, err := rewriter.Rewrite(outerQuery, viewInfo)
	if err == nil {
		t.Error("Expected error for invalid view SELECT")
	}
}

// TestRewrite_MaximumViewDepth tests exceeding maximum view depth
func TestRewrite_MaximumViewDepth(t *testing.T) {
	rewriter := NewViewRewriter()

	outerQuery := &parser.SelectStatement{
		Columns: []parser.SelectColumn{
			{Name: "id"},
		},
	}

	viewInfo := &domain.ViewInfo{
		SelectStmt: "SELECT id FROM users",
		Algorithm:  domain.ViewAlgorithmMerge,
	}

	// Set view depth to maximum
	rewriter.viewDepth = domain.MaxViewDepth

	_, err := rewriter.Rewrite(outerQuery, viewInfo)
	if err == nil {
		t.Error("Expected error for exceeding maximum view depth")
	}

	if err == nil || err.Error() != "maximum view nesting depth exceeded: 10" {
		t.Errorf("Expected depth error, got: %v", err)
	}
}

// TestMergeWhereClauses tests merging WHERE clauses
func TestMergeWhereClauses(t *testing.T) {
	rewriter := NewViewRewriter()

	tests := []struct {
		name       string
		outerWhere *parser.Expression
		viewWhere  *parser.Expression
		wantNil    bool
		wantAnd    bool
	}{
		{
			name:       "both nil",
			outerWhere: nil,
			viewWhere:  nil,
			wantNil:    true,
		},
		{
			name:       "outer nil",
			outerWhere: nil,
			viewWhere: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
			},
			wantNil: false,
			wantAnd: false,
		},
		{
			name: "view nil",
			outerWhere: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
			},
			viewWhere: nil,
			wantNil:   false,
			wantAnd:   false,
		},
		{
			name: "both non-nil",
			outerWhere: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
			},
			viewWhere: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
			},
			wantNil: false,
			wantAnd: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriter.mergeWhereClauses(tt.outerWhere, tt.viewWhere)
			if tt.wantNil {
				if result != nil {
					t.Error("Expected nil result")
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if tt.wantAnd && result.Operator != "AND" {
					t.Errorf("Expected AND operator, got %s", result.Operator)
				}
			}
		})
	}
}

// TestMergeHavingClauses tests merging HAVING clauses
func TestMergeHavingClauses(t *testing.T) {
	rewriter := NewViewRewriter()

	tests := []struct {
		name        string
		outerHaving *parser.Expression
		viewHaving  *parser.Expression
		wantNil     bool
		wantAnd     bool
	}{
		{
			name:        "both nil",
			outerHaving: nil,
			viewHaving:  nil,
			wantNil:     true,
		},
		{
			name:        "outer nil",
			outerHaving: nil,
			viewHaving: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
			},
			wantNil: false,
			wantAnd: false,
		},
		{
			name: "both non-nil",
			outerHaving: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "eq",
			},
			viewHaving: &parser.Expression{
				Type:     parser.ExprTypeOperator,
				Operator: "gt",
			},
			wantNil: false,
			wantAnd: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriter.mergeHavingClauses(tt.outerHaving, tt.viewHaving)
			if tt.wantNil {
				if result != nil {
					t.Error("Expected nil result")
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
					return
				}
				if tt.wantAnd && result.Operator != "AND" {
					t.Errorf("Expected AND operator, got %s", result.Operator)
				}
			}
		})
	}
}

// TestIsSelectAll tests checking for SELECT *
func TestIsSelectAll(t *testing.T) {
	rewriter := NewViewRewriter()

	tests := []struct {
		name     string
		cols     []parser.SelectColumn
		expected bool
	}{
		{
			name:     "empty columns",
			cols:     []parser.SelectColumn{},
			expected: false,
		},
		{
			name: "single wildcard",
			cols: []parser.SelectColumn{
				{IsWildcard: true},
			},
			expected: true,
		},
		{
			name: "specific columns",
			cols: []parser.SelectColumn{
				{Name: "id"},
				{Name: "name"},
			},
			expected: false,
		},
		{
			name: "wildcard with other columns",
			cols: []parser.SelectColumn{
				{IsWildcard: true},
				{Name: "id"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriter.isSelectAll(tt.cols)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestBuildMergedFrom tests building FROM clause
func TestBuildMergedFrom(t *testing.T) {
	rewriter := NewViewRewriter()

	tests := []struct {
		name     string
		view     *parser.SelectStatement
		outer    *parser.SelectStatement
		expected string
	}{
		{
			name:     "both empty",
			view:     &parser.SelectStatement{},
			outer:    &parser.SelectStatement{},
			expected: "",
		},
		{
			name: "view has FROM",
			view: &parser.SelectStatement{
				From: "users",
			},
			outer:    &parser.SelectStatement{},
			expected: "users",
		},
		{
			name: "outer has FROM",
			view: &parser.SelectStatement{},
			outer: &parser.SelectStatement{
				From: "orders",
			},
			expected: "orders",
		},
		{
			name: "both have FROM - view takes precedence",
			view: &parser.SelectStatement{
				From: "users",
			},
			outer: &parser.SelectStatement{
				From: "orders",
			},
			expected: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriter.buildMergedFrom(tt.view, tt.outer)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestMapColumnsByViewDefinition tests mapping columns by view definition
func TestMapColumnsByViewDefinition(t *testing.T) {
	rewriter := NewViewRewriter()

	outerCols := []parser.SelectColumn{
		{Name: "user_id", Alias: "uid"},
		{Name: "user_name"},
		{Name: "email"},
	}

	viewCols := []string{"user_id", "user_name", "email"}

	result := rewriter.mapColumnsByViewDefinition(outerCols, viewCols)

	if len(result) != len(outerCols) {
		t.Errorf("Expected %d columns, got %d", len(outerCols), len(result))
	}

	// Check that column names are mapped to view column names
	if result[0].Name != "user_id" {
		t.Errorf("Expected column name 'user_id', got '%s'", result[0].Name)
	}
	if result[0].Alias != "uid" {
		t.Errorf("Expected alias 'uid', got '%s'", result[0].Alias)
	}
}

// TestIsUpdatable tests checking if a view is updatable
func TestIsUpdatable(t *testing.T) {
	tests := []struct {
		name     string
		viewInfo *domain.ViewInfo
		expected bool
	}{
		{
			name:     "nil view info",
			viewInfo: nil,
			expected: true,
		},
		{
			name: "empty SELECT",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "",
			},
			expected: true,
		},
		{
			name: "simple SELECT",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT id, name FROM users",
			},
			expected: true,
		},
		{
			name: "SELECT with COUNT",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT COUNT(*) FROM users",
			},
			expected: false,
		},
		{
			name: "SELECT with SUM",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT SUM(amount) FROM orders",
			},
			expected: false,
		},
		{
			name: "SELECT with DISTINCT",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT DISTINCT name FROM users",
			},
			expected: false,
		},
		{
			name: "SELECT with GROUP BY",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT category, COUNT(*) FROM users GROUP BY category",
			},
			expected: false,
		},
		{
			name: "SELECT with HAVING",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT id, COUNT(*) FROM users HAVING COUNT(*) > 1",
			},
			expected: false,
		},
		{
			name: "SELECT with UNION",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT id FROM users UNION SELECT id FROM admins",
			},
			expected: false,
		},
		{
			name: "SELECT with subquery (heuristic)",
			viewInfo: &domain.ViewInfo{
				SelectStmt: "SELECT id FROM (SELECT id FROM users) AS t WHERE t.id > 10",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsUpdatable(tt.viewInfo)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestMergeSelectColumns tests merging SELECT columns
func TestMergeSelectColumns(t *testing.T) {
	rewriter := NewViewRewriter()

	outerCols := []parser.SelectColumn{
		{Name: "id"},
		{Name: "name"},
	}

	viewCols := []parser.SelectColumn{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	}

	viewInfo := &domain.ViewInfo{
		Cols: []string{},
	}

	result := rewriter.mergeSelectColumns(outerCols, viewCols, viewInfo)

	if len(result) != len(outerCols) {
		t.Errorf("Expected %d columns, got %d", len(outerCols), len(result))
	}
}

// TestMergeSelectColumns_WithViewColumnList tests merging with view column list
func TestMergeSelectColumns_WithViewColumnList(t *testing.T) {
	rewriter := NewViewRewriter()

	outerCols := []parser.SelectColumn{
		{Name: "user_id", Alias: "uid"},
		{Name: "user_name", Alias: "uname"},
	}

	viewCols := []parser.SelectColumn{}

	viewInfo := &domain.ViewInfo{
		Cols: []string{"user_id", "user_name", "email"},
	}

	result := rewriter.mergeSelectColumns(outerCols, viewCols, viewInfo)

	if len(result) != len(outerCols) {
		t.Errorf("Expected %d columns, got %d", len(outerCols), len(result))
	}

	// Check that aliases are preserved
	if result[0].Alias != "uid" {
		t.Errorf("Expected alias 'uid', got '%s'", result[0].Alias)
	}
}
