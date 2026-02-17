package api

import (
	"testing"
	"time"
)

func TestBindParams(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		params   []interface{}
		expected string
		wantErr  bool
	}{
		{
			name:     "no parameters",
			sql:      "SELECT * FROM users",
			params:   []interface{}{},
			expected: "SELECT * FROM users",
			wantErr:  false,
		},
		{
			name:     "single int parameter",
			sql:      "SELECT * FROM users WHERE id = ?",
			params:   []interface{}{42},
			expected: "SELECT * FROM users WHERE id = 42",
			wantErr:  false,
		},
		{
			name:     "single string parameter",
			sql:      "SELECT * FROM users WHERE name = ?",
			params:   []interface{}{"John"},
			expected: "SELECT * FROM users WHERE name = 'John'",
			wantErr:  false,
		},
		{
			name:     "string with single quote",
			sql:      "SELECT * FROM users WHERE name = ?",
			params:   []interface{}{"O'Reilly"},
			expected: "SELECT * FROM users WHERE name = 'O''Reilly'",
			wantErr:  false,
		},
		{
			name:     "multiple parameters",
			sql:      "SELECT * FROM users WHERE id = ? AND name = ? AND age > ?",
			params:   []interface{}{1, "John", 30},
			expected: "SELECT * FROM users WHERE id = 1 AND name = 'John' AND age > 30",
			wantErr:  false,
		},
		{
			name:     "null parameter",
			sql:      "SELECT * FROM users WHERE name = ?",
			params:   []interface{}{nil},
			expected: "SELECT * FROM users WHERE name = NULL",
			wantErr:  false,
		},
		{
			name:     "bool true",
			sql:      "SELECT * FROM users WHERE active = ?",
			params:   []interface{}{true},
			expected: "SELECT * FROM users WHERE active = TRUE",
			wantErr:  false,
		},
		{
			name:     "bool false",
			sql:      "SELECT * FROM users WHERE active = ?",
			params:   []interface{}{false},
			expected: "SELECT * FROM users WHERE active = FALSE",
			wantErr:  false,
		},
		{
			name:     "float parameter",
			sql:      "SELECT * FROM users WHERE price = ?",
			params:   []interface{}{19.99},
			expected: "SELECT * FROM users WHERE price = 19.99",
			wantErr:  false,
		},
		{
			name:     "byte array parameter",
			sql:      "SELECT * FROM users WHERE data = ?",
			params:   []interface{}{[]byte{0x01, 0x02, 0x03}},
			expected: "SELECT * FROM users WHERE data = 0x010203",
			wantErr:  false,
		},
		{
			name:     "parameter count mismatch - too few",
			sql:      "SELECT * FROM users WHERE id = ? AND name = ?",
			params:   []interface{}{1},
			expected: "",
			wantErr:  true,
		},
		{
			name:     "parameter count mismatch - too many",
			sql:      "SELECT * FROM users WHERE id = ?",
			params:   []interface{}{1, 2},
			expected: "",
			wantErr:  true,
		},
		{
			name:     "insert with parameters",
			sql:      "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
			params:   []interface{}{1, "John", 30},
			expected: "INSERT INTO users (id, name, age) VALUES (1, 'John', 30)",
			wantErr:  false,
		},
		{
			name:     "update with parameters",
			sql:      "UPDATE users SET name = ?, age = ? WHERE id = ?",
			params:   []interface{}{"Jane", 25, 1},
			expected: "UPDATE users SET name = 'Jane', age = 25 WHERE id = 1",
			wantErr:  false,
		},
		{
			name:     "delete with parameters",
			sql:      "DELETE FROM users WHERE id = ?",
			params:   []interface{}{1},
			expected: "DELETE FROM users WHERE id = 1",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bindParams(tt.sql, tt.params)
			if tt.wantErr {
				if err == nil {
					t.Errorf("bindParams() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("bindParams() unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("bindParams() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParamToString(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "nil",
			value:    nil,
			expected: "NULL",
		},
		{
			name:     "string",
			value:    "hello",
			expected: "'hello'",
		},
		{
			name:     "string with quote",
			value:    "it's",
			expected: "'it''s'",
		},
		{
			name:     "int",
			value:    42,
			expected: "42",
		},
		{
			name:     "int64",
			value:    int64(9223372036854775807),
			expected: "9223372036854775807",
		},
		{
			name:     "uint",
			value:    uint(42),
			expected: "42",
		},
		{
			name:     "float64",
			value:    3.14,
			expected: "3.14",
		},
		{
			name:     "bool true",
			value:    true,
			expected: "TRUE",
		},
		{
			name:     "bool false",
			value:    false,
			expected: "FALSE",
		},
		{
			name:     "bytes",
			value:    []byte{0x01, 0x02},
			expected: "0x0102",
		},
		{
			name:     "time.Time",
			value:    time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC),
			expected: "'2024-01-15 10:30:00.123456789'",
		},
		{
			name:     "time.Time without nanoseconds",
			value:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			expected: "'2024-01-15 10:30:00'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParamToString(tt.value)
			if result != tt.expected {
				t.Errorf("ParamToString() = %v, want %v", result, tt.expected)
			}
		})
	}
}
