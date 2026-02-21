package parser

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// =============================================================================
// P0-1: TRUNCATE TABLE incorrectly maps to SQLTypeDrop instead of SQLTypeTruncate
// adapter.go convertToStatement: TruncateTableStmt sets stmt.Type = SQLTypeDrop
// This causes TRUNCATE TABLE to be treated as DROP TABLE, destroying the schema.
// =============================================================================

func TestTruncateTableShouldNotMapToDrop(t *testing.T) {
	adapter := NewSQLAdapter()
	result, err := adapter.Parse("TRUNCATE TABLE users")
	if err != nil {
		t.Fatalf("Failed to parse TRUNCATE TABLE: %v", err)
	}

	if !result.Success {
		t.Fatalf("Parse failed: %s", result.Error)
	}

	// TRUNCATE should NOT be SQLTypeDrop
	if result.Statement.Type == SQLTypeDrop {
		t.Errorf("TRUNCATE TABLE was incorrectly mapped to SQLTypeDrop (would DROP the table!)")
	}

	// TRUNCATE should be SQLTypeTruncate
	if result.Statement.Type != SQLTypeTruncate {
		t.Errorf("Expected SQLTypeTruncate, got %s", result.Statement.Type)
	}
}

// =============================================================================
// P0-2: isTruthy in view_check_option.go treats zero int/float as truthy
// The Go type-switch `case int, int8, ... : return true` preserves interface{},
// so int(0) and float64(0.0) incorrectly return true.
// This breaks CHECK OPTION validation for rows with zero values.
// =============================================================================

func TestIsTruthy_ZeroIntShouldBeFalse(t *testing.T) {
	cv := &CheckOptionValidator{
		viewInfo: &domain.ViewInfo{},
	}

	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		// Zero values should be falsy
		{"int(0)", int(0), false},
		{"int8(0)", int8(0), false},
		{"int16(0)", int16(0), false},
		{"int32(0)", int32(0), false},
		{"int64(0)", int64(0), false},
		{"uint(0)", uint(0), false},
		{"uint8(0)", uint8(0), false},
		{"uint16(0)", uint16(0), false},
		{"uint32(0)", uint32(0), false},
		{"uint64(0)", uint64(0), false},
		{"float32(0)", float32(0), false},
		{"float64(0)", float64(0), false},

		// Non-zero values should be truthy
		{"int(1)", int(1), true},
		{"int64(42)", int64(42), true},
		{"float64(3.14)", float64(3.14), true},
		{"float64(-1)", float64(-1.0), true},

		// Nil should be falsy
		{"nil", nil, false},

		// Bool values
		{"true", true, true},
		{"false", false, false},

		// String values
		{"empty string", "", false},
		{"non-empty string", "hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cv.isTruthy(tt.value)
			if result != tt.expected {
				t.Errorf("isTruthy(%v) = %v, expected %v", tt.value, result, tt.expected)
			}
		})
	}
}

func TestCheckOptionValidator_ZeroValueShouldFail(t *testing.T) {
	// View: SELECT * FROM users WHERE active = 1
	viewInfo := &domain.ViewInfo{
		SelectStmt:  "SELECT * FROM users WHERE active = 1",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	cv := NewCheckOptionValidator(viewInfo)

	// Row with active=0 should fail CHECK OPTION
	row := domain.Row{"id": int64(1), "name": "test", "active": int64(0)}
	err := cv.ValidateInsert(row)
	if err == nil {
		t.Errorf("Expected CHECK OPTION to fail for active=0, but it passed")
	}

	// Row with active=1 should pass
	row2 := domain.Row{"id": int64(2), "name": "test2", "active": int64(1)}
	err = cv.ValidateInsert(row2)
	if err != nil {
		t.Errorf("Expected CHECK OPTION to pass for active=1, got error: %v", err)
	}
}
