package parser

import (
	"testing"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func TestCheckOptionValidatorDetailed(t *testing.T) {
	t.Log("=== Testing with NO CHECK OPTION ===")
	viewInfo1 := &domain.ViewInfo{
		SelectStmt:  "SELECT id, name FROM users WHERE active = 1",
		CheckOption: domain.ViewCheckOptionNone,
	}

	validator1 := NewCheckOptionValidator(viewInfo1)

	// Should pass since no CHECK OPTION
	row1 := domain.Row{"id": 1, "name": "Alice", "active": 1}
	err1 := validator1.ValidateInsert(row1)
	t.Logf("ValidateInsert with NO CHECK OPTION: row=%v, err=%v", row1, err1)
	if err1 != nil {
		t.Errorf("ValidateInsert should not fail with NO CHECK OPTION: %v", err1)
	}

	t.Log("\n=== Testing with CASCADED CHECK OPTION ===")
	// Test with CASCADED CHECK OPTION
	viewInfo2 := &domain.ViewInfo{
		SelectStmt:  "SELECT id, name FROM users WHERE active = 1",
		CheckOption: domain.ViewCheckOptionCascaded,
	}

	validator2 := NewCheckOptionValidator(viewInfo2)

	// Should pass - row satisfies WHERE clause
	t.Log("Test 1: Valid row (active=1)")
	row2 := domain.Row{"id": 1, "name": "Alice", "active": 1}
	err2 := validator2.ValidateInsert(row2)
	t.Logf("ValidateInsert result: err=%v", err2)
	if err2 != nil {
		t.Errorf("ValidateInsert should pass for valid row: %v", err2)
	}

	// Should fail - row doesn't satisfy WHERE clause
	t.Log("Test 2: Invalid row (active=0)")
	row3 := domain.Row{"id": 2, "name": "Bob", "active": 0}
	err3 := validator2.ValidateInsert(row3)
	t.Logf("ValidateInsert result: err=%v", err3)
	if err3 == nil {
		t.Error("ValidateInsert should fail for row that doesn't satisfy WHERE clause")
	}

	t.Log("\n=== Testing ValidateUpdate ===")
	// Test ValidateUpdate
	oldRow := domain.Row{"id": 1, "name": "Alice", "active": 1}
	
	// Valid update - only name changes, active stays 1
	updates1 := domain.Row{"name": "Alice Updated"}
	t.Log("Test 1: Valid UPDATE (name changed, active stays 1)")
	err4 := validator2.ValidateUpdate(oldRow, updates1)
	t.Logf("ValidateUpdate result (name change): err=%v", err4)
	if err4 != nil {
		t.Errorf("Expected no error for valid update (name only), got: %v", err4)
	}

	// Invalid update - active changes to 0
	updates2 := domain.Row{"active": 0}
	t.Log("Test 2: Invalid UPDATE (active changed to 0)")
	err5 := validator2.ValidateUpdate(oldRow, updates2)
	t.Logf("ValidateUpdate result (active change): err=%v", err5)
	if err5 == nil {
		t.Error("Expected error for invalid update (active to 0), got nil")
	}

	// Valid update - both name and active change, but active stays 1
	updates3 := domain.Row{"name": "Bob Updated", "active": 1}
	t.Log("Test 3: Valid UPDATE (both changed, but active stays 1)")
	err6 := validator2.ValidateUpdate(oldRow, updates3)
	t.Logf("ValidateUpdate result (both changed, active stays 1): err=%v", err6)
	if err6 != nil {
		t.Errorf("Expected no error for valid update, got: %v", err6)
	}
}
