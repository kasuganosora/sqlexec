package optimizer

import (
	"context"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/parser"
)

// TestNewShowExecutor tests the ShowExecutor constructor
func TestNewShowExecutor(t *testing.T) {
	executor := NewShowExecutor("test_db", nil, nil)

	if executor == nil {
		t.Fatal("Expected executor to be created")
	}

	if executor.currentDB != "test_db" {
		t.Errorf("Expected currentDB to be 'test_db', got '%s'", executor.currentDB)
	}
}

// TestSetCurrentDB_ShowExecutor tests setting the current database
func TestSetCurrentDB_ShowExecutor(t *testing.T) {
	executor := NewShowExecutor("initial_db", nil, nil)

	executor.SetCurrentDB("new_db")

	if executor.currentDB != "new_db" {
		t.Errorf("Expected currentDB to be 'new_db', got '%s'", executor.currentDB)
	}
}

// TestExecuteShow tests executing SHOW statements
func TestExecuteShow(t *testing.T) {
	executor := NewShowExecutor("test_db", nil, nil)
	ctx := context.Background()

	tests := []struct {
		name        string
		showType    string
		expectError bool
	}{
		{
			name:        "SHOW PROCESSLIST",
			showType:    "PROCESSLIST",
			expectError: false, // Should not error
		},
		{
			name:        "unsupported SHOW type",
			showType:    "UNSUPPORTED",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			showStmt := &parser.ShowStatement{
				Type: tt.showType,
			}

			_, err := executor.ExecuteShow(ctx, showStmt)
			if tt.expectError && err == nil {
				t.Errorf("Expected error for SHOW %s", tt.showType)
			}
			if !tt.expectError && err != nil {
				t.Errorf("Did not expect error for SHOW %s: %v", tt.showType, err)
			}
		})
	}
}

// TestExecuteShowColumns tests SHOW COLUMNS execution
func TestExecuteShowColumns(t *testing.T) {
	executor := NewShowExecutor("test_db", nil, nil)
	ctx := context.Background()

	tests := []struct {
		name        string
		showStmt    *parser.ShowStatement
		expectError bool
	}{
		{
			name: "SHOW COLUMNS without table name",
			showStmt: &parser.ShowStatement{
				Type: "COLUMNS",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executor.executeShowColumns(ctx, tt.showStmt)
			if tt.expectError && err == nil {
				t.Errorf("Expected error")
			}
		})
	}
}

// TestExecuteShowProcessList tests SHOW PROCESSLIST execution
func TestExecuteShowProcessList(t *testing.T) {
	executor := NewShowExecutor("test_db", nil, nil)
	ctx := context.Background()

	tests := []struct {
		name string
		full bool
	}{
		{"SHOW PROCESSLIST (not full)", false},
		{"SHOW FULL PROCESSLIST", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.executeShowProcessList(ctx, tt.full)
			if err != nil {
				t.Errorf("Did not expect error: %v", err)
				return
			}

			// Check that result has correct structure
			if result == nil {
				t.Fatal("Expected result to be non-nil")
			}

			// Check columns
			if len(result.Columns) == 0 {
				t.Error("Expected columns in result")
			}

			// Verify column names
			expectedColumns := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
			for i, expectedCol := range expectedColumns {
				if i >= len(result.Columns) {
					t.Errorf("Missing column %s", expectedCol)
					continue
				}
				if result.Columns[i].Name != expectedCol {
					t.Errorf("Expected column %s, got %s", expectedCol, result.Columns[i].Name)
				}
			}

			// Result should not error even with empty process list
		})
	}
}

// TestShowProcessListWithData tests SHOW PROCESSLIST with process list provider
func TestShowProcessListWithData(t *testing.T) {
	// Set up a process list provider
	processListProvider = func() []interface{} {
		return []interface{}{
			map[string]interface{}{
				"ThreadID": uint32(1),
				"SQL":      "SELECT * FROM users",
				"Duration": time.Duration(5 * time.Second),
				"Status":   "running",
				"User":     "test_user",
				"Host":     "192.168.1.1:3306",
				"DB":       "test_db",
			},
			map[string]interface{}{
				"ThreadID": uint32(2),
				"SQL":      "SELECT COUNT(*) FROM orders",
				"Duration": time.Duration(10 * time.Second),
				"Status":   "timeout",
				"User":     "admin",
				"Host":     "localhost",
				"DB":       "test_db",
			},
		}
	}
	defer func() { processListProvider = nil }()

	executor := NewShowExecutor("test_db", nil, nil)
	ctx := context.Background()

	tests := []struct {
		name           string
		full           bool
		expectedRows   int
		checkTruncation bool
	}{
		{
			name:           "not full - check truncation",
			full:           false,
			expectedRows:   2,
			checkTruncation: true,
		},
		{
			name:           "full - no truncation",
			full:           true,
			expectedRows:   2,
			checkTruncation: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.executeShowProcessList(ctx, tt.full)
			if err != nil {
				t.Errorf("Did not expect error: %v", err)
				return
			}

			if len(result.Rows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(result.Rows))
			}

			if tt.checkTruncation && len(result.Rows) > 0 {
				// Check that Info field is truncated when not full
				info, ok := result.Rows[0]["Info"].(string)
				if ok && len(info) > 100 {
					t.Errorf("Expected Info to be truncated to 100 chars when not full, got %d chars", len(info))
				}
			}

			if !tt.full && len(result.Rows) > 0 {
				info, ok := result.Rows[0]["Info"].(string)
				if ok && len(info) > 100 {
					t.Errorf("Expected Info to be truncated when not full, got %d chars", len(info))
				}
			}
		})
	}
}

// TestProcessListProviderIntegration tests process list provider integration
func TestProcessListProviderIntegration(t *testing.T) {
	// Test with nil provider
	processListProvider = nil
	executor := NewShowExecutor("test_db", nil, nil)
	ctx := context.Background()

	result, err := executor.executeShowProcessList(ctx, false)
	if err != nil {
		t.Errorf("Did not expect error with nil provider: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows with nil provider, got %d", len(result.Rows))
	}

	// Test with provider that returns empty list
	processListProvider = func() []interface{} {
		return []interface{}{}
	}

	result, err = executor.executeShowProcessList(ctx, false)
	if err != nil {
		t.Errorf("Did not expect error with empty provider: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows with empty provider, got %d", len(result.Rows))
	}

	// Reset
	processListProvider = nil
}

// TestExecuteShowVariables tests SHOW VARIABLES execution
func TestExecuteShowVariables(t *testing.T) {
	executor := NewShowExecutor("test_db", nil, nil)
	ctx := context.Background()

	tests := []struct {
		name          string
		showStmt      *parser.ShowStatement
		expectedVars  []string
		expectMinRows int
	}{
		{
			name: "SHOW VARIABLES - all",
			showStmt: &parser.ShowStatement{
				Type: "VARIABLES",
			},
			expectedVars:  []string{"version", "port", "hostname"},
			expectMinRows: 10,
		},
		{
			name: "SHOW VARIABLES LIKE 'version%'",
			showStmt: &parser.ShowStatement{
				Type: "VARIABLES",
				Like: "'version%'",
			},
			expectedVars:  []string{"version", "version_comment"},
			expectMinRows: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.executeShowVariables(ctx, tt.showStmt)
			if err != nil {
				t.Errorf("Did not expect error: %v", err)
				return
			}

			if result == nil {
				t.Fatal("Expected result to be non-nil")
			}

			// Check columns
			if len(result.Columns) != 2 {
				t.Errorf("Expected 2 columns, got %d", len(result.Columns))
			}
			if result.Columns[0].Name != "Variable_name" {
				t.Errorf("Expected first column 'Variable_name', got '%s'", result.Columns[0].Name)
			}
			if result.Columns[1].Name != "Value" {
				t.Errorf("Expected second column 'Value', got '%s'", result.Columns[1].Name)
			}

			// Check minimum rows
			if len(result.Rows) < tt.expectMinRows {
				t.Errorf("Expected at least %d rows, got %d", tt.expectMinRows, len(result.Rows))
			}

			// Check expected variables exist
			for _, expectedVar := range tt.expectedVars {
				found := false
				for _, row := range result.Rows {
					if varName, ok := row["Variable_name"].(string); ok && varName == expectedVar {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected variable '%s' not found in result", expectedVar)
				}
			}
		})
	}
}

// TestExecuteShowStatus tests SHOW STATUS execution
func TestExecuteShowStatus(t *testing.T) {
	executor := NewShowExecutor("test_db", nil, nil)
	ctx := context.Background()

	tests := []struct {
		name          string
		showStmt      *parser.ShowStatement
		expectedVars  []string
		expectMinRows int
	}{
		{
			name: "SHOW STATUS - all",
			showStmt: &parser.ShowStatement{
				Type: "STATUS",
			},
			expectedVars:  []string{"Threads_connected", "Threads_running", "Queries"},
			expectMinRows: 5,
		},
		{
			name: "SHOW STATUS LIKE 'Threads%'",
			showStmt: &parser.ShowStatement{
				Type: "STATUS",
				Like: "'Threads%'",
			},
			expectedVars:  []string{"Threads_connected", "Threads_running"},
			expectMinRows: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.executeShowStatus(ctx, tt.showStmt)
			if err != nil {
				t.Errorf("Did not expect error: %v", err)
				return
			}

			if result == nil {
				t.Fatal("Expected result to be non-nil")
			}

			// Check columns
			if len(result.Columns) != 2 {
				t.Errorf("Expected 2 columns, got %d", len(result.Columns))
			}
			if result.Columns[0].Name != "Variable_name" {
				t.Errorf("Expected first column 'Variable_name', got '%s'", result.Columns[0].Name)
			}
			if result.Columns[1].Name != "Value" {
				t.Errorf("Expected second column 'Value', got '%s'", result.Columns[1].Name)
			}

			// Check minimum rows
			if len(result.Rows) < tt.expectMinRows {
				t.Errorf("Expected at least %d rows, got %d", tt.expectMinRows, len(result.Rows))
			}

			// Check expected variables exist
			for _, expectedVar := range tt.expectedVars {
				found := false
				for _, row := range result.Rows {
					if varName, ok := row["Variable_name"].(string); ok && varName == expectedVar {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected status variable '%s' not found in result", expectedVar)
				}
			}
		})
	}
}

// TestMatchLike tests the matchLike helper function
func TestMatchLike(t *testing.T) {
	tests := []struct {
		s       string
		pattern string
		expect  bool
	}{
		{"version", "version", true},
		{"version_comment", "version%", true},
		{"version", "ver%", true},
		{"version", "%sion", true},
		{"version", "%vers%", true},
		{"port", "port", true},
		{"port", "por%", true},
		{"port", "p%", true},
		{"port", "%t", true},
		{"port", "xyz", false},
		{"port", "xyz%", false},
		{"port", "%xyz", false},
		{"max_connections", "max_%", true},
		{"max_connections", "%connections", true},
	}

	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.pattern, func(t *testing.T) {
			result := matchLike(tt.s, tt.pattern)
			if result != tt.expect {
				t.Errorf("matchLike(%q, %q) = %v, expected %v", tt.s, tt.pattern, result, tt.expect)
			}
		})
	}
}
