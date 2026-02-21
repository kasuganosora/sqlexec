package parser

import (
	"testing"
	"time"
)

// =============================================================================
// P1-1: SelectColumn.Table uses Schema instead of Table (adapter.go:823)
// For `SELECT t.id FROM t`, the table qualifier "t" is in Table, not Schema.
// =============================================================================

func TestSelectColumn_TableQualifier(t *testing.T) {
	adapter := NewSQLAdapter()
	result, err := adapter.Parse("SELECT t.id, t.name FROM users AS t")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if !result.Success || result.Statement.Select == nil {
		t.Fatalf("Parse failed or no SELECT statement")
	}

	for _, col := range result.Statement.Select.Columns {
		if col.Name == "id" || col.Name == "name" {
			// Table qualifier should be "t" (from the alias), not empty
			// Note: TiDB parser resolves table aliases in column references
			if col.Table == "" {
				t.Errorf("Column %q has empty Table qualifier, expected table alias", col.Name)
			}
		}
	}
}

// =============================================================================
// P1-2: convertExpression loses table qualifier for qualified column names
// adapter.go:866-870 checks Schema instead of Table for column prefixes.
// For WHERE t.id = 5, the column becomes just "id" instead of "t.id".
// =============================================================================

func TestConvertExpression_TableQualifiedColumn(t *testing.T) {
	adapter := NewSQLAdapter()
	result, err := adapter.Parse("SELECT * FROM users WHERE users.id = 5")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if !result.Success || result.Statement.Select == nil {
		t.Fatalf("Parse failed or no SELECT statement")
	}

	where := result.Statement.Select.Where
	if where == nil {
		t.Fatalf("No WHERE clause")
	}

	// The left side should have the table qualifier "users.id", not just "id"
	if where.Left != nil && where.Left.Type == ExprTypeColumn {
		col := where.Left.Column
		if col == "id" {
			t.Errorf("Column lost table qualifier: got %q, expected %q", col, "users.id")
		}
	}
}

// =============================================================================
// P1-4: ParseWindowFrame skips UNBOUNDED FOLLOWING end bound (window.go:110)
// When end == BoundUnboundedFollowing, frame.End is not set (remains nil).
// =============================================================================

func TestParseWindowFrame_UnboundedFollowing(t *testing.T) {
	frame := ParseWindowFrame(
		FrameModeRows,
		BoundCurrentRow,
		Expression{},
		BoundUnboundedFollowing,
		Expression{},
	)

	if frame.End == nil {
		t.Errorf("ParseWindowFrame: End is nil for BoundUnboundedFollowing, should be set")
	}

	if frame.End != nil && frame.End.Type != BoundUnboundedFollowing {
		t.Errorf("ParseWindowFrame: End.Type = %d, expected BoundUnboundedFollowing(%d)",
			frame.End.Type, BoundUnboundedFollowing)
	}
}

// =============================================================================
// P1-5: LAG/LEAD validation rejects valid 3-arg form (window.go:263)
// SQL standard: LAG(expr, offset, default) accepts 1-3 arguments.
// Current code rejects 3 arguments.
// =============================================================================

func TestValidateWindowExpression_LagLead3Args(t *testing.T) {
	// LAG with 3 arguments should be valid
	lag3 := &WindowExpression{
		FuncName: "LAG",
		Args: []Expression{
			{Type: ExprTypeColumn, Column: "value"},
			{Type: ExprTypeValue, Value: 1},
			{Type: ExprTypeValue, Value: 0}, // default value
		},
		Spec: &WindowSpec{
			OrderBy: []OrderItem{{Expr: Expression{Type: ExprTypeColumn, Column: "id"}, Direction: "ASC"}},
		},
	}

	err := ValidateWindowExpression(lag3)
	if err != nil {
		t.Errorf("LAG with 3 args should be valid, got error: %v", err)
	}

	// LEAD with 3 arguments should also be valid
	lead3 := &WindowExpression{
		FuncName: "LEAD",
		Args: []Expression{
			{Type: ExprTypeColumn, Column: "value"},
			{Type: ExprTypeValue, Value: 1},
			{Type: ExprTypeValue, Value: nil}, // default NULL
		},
		Spec: &WindowSpec{
			OrderBy: []OrderItem{{Expr: Expression{Type: ExprTypeColumn, Column: "id"}, Direction: "ASC"}},
		},
	}

	err = ValidateWindowExpression(lead3)
	if err != nil {
		t.Errorf("LEAD with 3 args should be valid, got error: %v", err)
	}
}

// =============================================================================
// P1-6: RECOMMEND INDEX Parse uppercases query content (recommend.go:33)
// sql = strings.ToUpper(sql) destroys case of string literals in the query.
// =============================================================================

func TestRecommendIndexParse_PreservesQueryCase(t *testing.T) {
	parser := NewRecommendIndexParser()

	// The original SQL string - we need to test the RUN action with FOR
	stmt, err := parser.Parse(`RECOMMEND INDEX RUN FOR 'SELECT * FROM users WHERE name = "John"'`)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Action != "RUN" {
		t.Errorf("Expected action RUN, got %s", stmt.Action)
	}

	// The query should preserve the original case (e.g., "John" not "JOHN")
	if stmt.Query != "" {
		// Check that the query wasn't completely uppercased
		if stmt.Query == `SELECT * FROM USERS WHERE NAME = "JOHN"` {
			t.Errorf("Query was incorrectly uppercased: %s", stmt.Query)
		}
	}
}

// =============================================================================
// P1-7: parseDuration rejects plain integer milliseconds (hints_parser.go)
// MAX_EXECUTION_TIME(1000) is a common TiDB hint (plain ms), but the regex
// requires a unit suffix like ms/s/m/h.
// =============================================================================

func TestParseDuration_PlainInteger(t *testing.T) {
	hp := NewHintsParser()

	// Plain integer (milliseconds, common TiDB format)
	duration, err := hp.parseDuration("1000")
	if err != nil {
		t.Errorf("parseDuration(\"1000\") should accept plain integer as ms, got error: %v", err)
	}
	if duration != 1000*time.Millisecond {
		t.Errorf("parseDuration(\"1000\") = %v, expected 1000ms", duration)
	}

	// With explicit ms suffix should still work
	duration2, err := hp.parseDuration("500ms")
	if err != nil {
		t.Errorf("parseDuration(\"500ms\") failed: %v", err)
	}
	if duration2 != 500*time.Millisecond {
		t.Errorf("parseDuration(\"500ms\") = %v, expected 500ms", duration2)
	}
}

// =============================================================================
// P1-10: convertJoinTree appends empty-table JoinInfo (adapter.go:466)
// When n.Right is not a TableSource, joinInfo.Table is empty but still appended.
// =============================================================================

func TestConvertJoinTree_NoEmptyJoinInfo(t *testing.T) {
	adapter := NewSQLAdapter()

	// Simple two-table JOIN
	result, err := adapter.Parse("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if result.Statement.Select == nil {
		t.Fatalf("No SELECT statement")
	}

	// Check that all JoinInfo entries have non-empty Table names
	for i, join := range result.Statement.Select.Joins {
		if join.Table == "" {
			t.Errorf("JoinInfo[%d] has empty Table name", i)
		}
	}
}

// =============================================================================
// P1-3: Duplicate vector index detection code (adapter.go:1311-1341)
// The USING clause and index name checks are duplicated, running twice.
// This is a code quality issue that could cause incorrect behavior.
// Test that the first detection works correctly without the duplicate block.
// =============================================================================

func TestConvertCreateIndexStmt_NoDuplicateVectorDetection(t *testing.T) {
	adapter := NewSQLAdapter()

	// A regular index should NOT be detected as vector
	result, err := adapter.Parse("CREATE INDEX idx_name ON users(name)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if result.Statement.CreateIndex != nil && result.Statement.CreateIndex.IsVectorIndex {
		t.Errorf("Regular index should not be detected as vector index")
	}
}
