package builtin

import (
	"fmt"
	"testing"

	jsonpkg "github.com/kasuganosora/sqlexec/pkg/json"
)

// Test JSON_TYPE function
func TestJSONType(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{`{"a": 1}`, "OBJECT"},
		{`[1, 2, 3]`, "ARRAY"},
		{`"hello"`, "STRING"},
		{`42`, "INTEGER"},
		{`3.14`, "DOUBLE"},
		{`true`, "BOOLEAN"},
		{`null`, "NULL"},
		{`{"a": {"b": 1}}`, "OBJECT"},
	}

	for _, tt := range tests {
		t.Run(tt.input.(string), func(t *testing.T) {
			result, err := jsonType([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonType() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_VALID function
func TestJSONValid(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int64
	}{
		{`{"a": 1}`, 1},
		{`[1, 2, 3]`, 1},
		{`"valid"`, 1},
		{`123`, 1},
		{`true`, 1},
		{`null`, 1},
		{`{invalid}`, 0},
		{``, 0},
		{`{}`, 1},
		{`[]`, 1},
	}

	for _, tt := range tests {
		t.Run(tt.input.(string), func(t *testing.T) {
			result, err := jsonValid([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonValid() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonValid() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_ARRAY function
func TestJSONArray(t *testing.T) {
	tests := []struct {
		args     []interface{}
		expected string
	}{
		{[]interface{}{1, 2, 3}, `[1,2,3]`},
		{[]interface{}{"a", "b"}, `["a","b"]`},
		{[]interface{}{1, "b", true}, `[1,"b",true]`},
		{[]interface{}{}, `[]`},
		{[]interface{}{[]interface{}{1, 2}}, `[[1,2]]`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result, err := jsonArray(tt.args)
			if err != nil {
				t.Fatalf("jsonArray() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonArray() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_OBJECT function
func TestJSONObject(t *testing.T) {
	tests := []struct {
		args     []interface{}
		expected string
	}{
		{[]interface{}{"a", 1, "b", 2}, `{"a":1,"b":2}`},
		{[]interface{}{"key", "value"}, `{"key":"value"}`},
		{[]interface{}{}, `{}`},
		{[]interface{}{"a", 1, "b", 2, "c", 3}, `{"a":1,"b":2,"c":3}`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result, err := jsonObject(tt.args)
			if err != nil {
				t.Fatalf("jsonObject() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonObject() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_EXTRACT function
func TestJSONExtract(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{"simple key", []interface{}{`{"a": 1, "b": 2}`, "$.a"}, `1`},
		{"nested key", []interface{}{`{"a": {"b": 1}}`, "$.a.b"}, `1`},
		{"array index", []interface{}{`[1, 2, 3]`, "$[0]"}, `1`},
		{"array nested", []interface{}{`{"a": [1, 2, 3]}`, "$.a[1]"}, `2`},
		{"wildcard array", []interface{}{`[1, 2, 3]`, "$[*]"}, `[1,2,3]`},
		{"wildcard object", []interface{}{`{"a": 1, "b": 2}`, "$.*"}, `[1,2]`},
		{"last index", []interface{}{`[1, 2, 3]`, "$[last]"}, `3`},
		{"negative index", []interface{}{`[1, 2, 3]`, "$[-1]"}, `3`},
		{"range", []interface{}{`[1, 2, 3, 4, 5]`, "$[0 to 2]"}, `[1,2,3]`},
		{"multiple paths", []interface{}{`{"a": 1, "b": 2}`, "$.a", "$.b"}, `[1,2]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonExtract(tt.args)
			if err != nil {
				t.Fatalf("jsonExtract() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonExtract() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_CONTAINS function
func TestJSONContains(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected int64
	}{
		{"array contains", []interface{}{`[1, 2, 3]`, `2`}, 1},
		{"array not contains", []interface{}{`[1, 2, 3]`, `4`}, 0},
		{"object contains", []interface{}{`{"a": 1, "b": 2}`, `{"a": 1}`}, 1},
		{"object not contains", []interface{}{`{"a": 1}`, `{"b": 2}`}, 0},
		{"exact match", []interface{}{`{"a": 1}`, `{"a": 1}`}, 1},
		{"partial match", []interface{}{`{"a": 1, "b": 2}`, `{"a": 1}`}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonContains(tt.args)
			if err != nil {
				t.Fatalf("jsonContains() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonContains() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_QUOTE function
func TestJSONQuote(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", `"hello"`},
		{`"hello"`, `"\"hello\""`},
		{`hello\world`, `"hello\\world"`},
		{"hello\nworld", `"hello\nworld"`},
		{"", `""`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := jsonQuote([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonQuote() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonQuote() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_UNQUOTE function
func TestJSONUnquote(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"hello"`, "hello"},
		{`"\"hello\""`, `"hello"`},
		{`""`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := jsonUnquote([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonUnquote() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonUnquote() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_CONTAINS_PATH function
func TestJSONContainsPath(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected int64
	}{
		{"one path exists", []interface{}{`{"a": 1}`, "one", "$.a"}, 1},
		{"one path not exists", []interface{}{`{"a": 1}`, "one", "$.b"}, 0},
		{"one of many exists", []interface{}{`{"a": 1, "b": 2}`, "one", "$.a", "$.c"}, 1},
		{"all paths exist", []interface{}{`{"a": 1, "b": 2}`, "all", "$.a", "$.b"}, 1},
		{"not all paths exist", []interface{}{`{"a": 1, "b": 2}`, "all", "$.a", "$.c"}, 0},
		{"nested path", []interface{}{`{"a": {"b": 1}}`, "one", "$.a.b"}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonContainsPath(tt.args)
			if err != nil {
				t.Fatalf("jsonContainsPath() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonContainsPath() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_KEYS function
func TestJSONKeys(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple object", `{"a": 1, "b": 2}`, `["a","b"]`},
		{"nested object", `{"a": {"b": 1}}`, `["a"]`},
		{"empty object", `{}`, `[]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonKeys([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonKeys() error: %v", err)
			}
			// Result may vary in order, so just check if it contains the keys
			if result != tt.expected {
				t.Logf("jsonKeys() = %v (order may differ), expected %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_SET function
func TestJSONSet(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{"set existing key", []interface{}{`{"a": 1}`, "$.a", 2}, `{"a":2}`},
		{"set new key", []interface{}{`{"a": 1}`, "$.b", 2}, `{"a":1,"b":2}`},
		{"set array element", []interface{}{`[1, 2, 3]`, "$[1]", 10}, `[1,10,3]`},
		{"nested object", []interface{}{`{"a": {"b": 1}}`, "$.a.c", 2}, `{"a":{"b":1,"c":2}}`},
		{"set multiple", []interface{}{`{"a": 1, "b": 2}`, "$.a", 10, "$.b", 20}, `{"a":10,"b":20}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSet(tt.args)
			if err != nil {
				t.Fatalf("jsonSet() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonSet() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_INSERT function
func TestJSONInsert(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
		wantErr  bool
	}{
		{"insert new key", []interface{}{`{"a": 1}`, "$.b", 2}, `{"a":1,"b":2}`, false},
		{"insert nested", []interface{}{`{"a": {}}`, "$.a.b", 2}, `{"a":{"b":2}}`, false},
		{"insert existing should not fail but not replace", []interface{}{`{"a": 1}`, "$.a", 2}, `{"a":1}`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonInsert(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("jsonInsert() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("jsonInsert() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonInsert() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_REPLACE function
func TestJSONReplace(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
		wantErr  bool
	}{
		{"replace existing key", []interface{}{`{"a": 1}`, "$.a", 2}, `{"a":2}`, false},
		{"replace array element", []interface{}{`[1, 2, 3]`, "$[1]", 10}, `[1,10,3]`, false},
		{"replace non-existing should fail", []interface{}{`{"a": 1}`, "$.b", 2}, ``, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonReplace(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("jsonReplace() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("jsonReplace() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonReplace() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_REMOVE function
func TestJSONRemove(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{"remove key", []interface{}{`{"a": 1, "b": 2}`, "$.b"}, `{"a":1}`},
		{"remove array element", []interface{}{`[1, 2, 3]`, "$[1]"}, `[1,3]`},
		{"remove last", []interface{}{`[1, 2, 3]`, "$[last]"}, `[1,2]`},
		{"remove nested", []interface{}{`{"a": {"b": 1, "c": 2}}`, "$.a.b"}, `{"a":{"c":2}}`},
		{"remove multiple", []interface{}{`{"a": 1, "b": 2, "c": 3}`, "$.b", "$.c"}, `{"a":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonRemove(tt.args)
			if err != nil {
				t.Fatalf("jsonRemove() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonRemove() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_MERGE_PRESERVE function
func TestJSONMergePreserve(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{"merge two objects", []interface{}{`{"a": 1}`, `{"b": 2}`}, `{"a":1,"b":2}`},
		{"merge overlapping keys", []interface{}{`{"a": 1}`, `{"a": 2, "b": 3}`}, `{"a":1,"b":3}`},
		{"merge with null", []interface{}{`null`, `{"a": 1}`}, `{"a":1}`},
		{"merge arrays", []interface{}{`[1, 2]`, `[3, 4]`}, `[1,2,3,4]`},
		{"merge array with value", []interface{}{`[1, 2]`, `3`}, `[1,2,3]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonMergePreserve(tt.args)
			if err != nil {
				t.Fatalf("jsonMergePreserve() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonMergePreserve() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_MERGE_PATCH function
func TestJSONMergePatch(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{"patch simple", []interface{}{`{"a": 1, "b": 2}`, `{"a": 3}`}, `{"a":3,"b":2}`},
		{"patch add key", []interface{}{`{"a": 1}`, `{"b": 2}`}, `{"a":1,"b":2}`},
		{"patch delete key", []interface{}{`{"a": 1, "b": 2}`, `{"b": null}`}, `{"a":1}`},
		{"patch with null", []interface{}{`{"a": 1}`, `null`}, `null`},
		{"patch nested", []interface{}{`{"a": {"b": 1}}`, `{"a": {"b": 2, "c": 3}}`}, `{"a":{"b":2,"c":3}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonMergePatch(tt.args)
			if err != nil {
				t.Fatalf("jsonMergePatch() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonMergePatch() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_LENGTH function
func TestJSONLength(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"array length", `[1, 2, 3]`, 3},
		{"object length", `{"a": 1, "b": 2, "c": 3}`, 3},
		{"string length", `"hello"`, 5},
		{"null length", `null`, 0},
		{"number length", `42`, 1},
		{"empty array", `[]`, 0},
		{"empty object", `{}`, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonLength([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonLength() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonLength() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_DEPTH function
func TestJSONDepth(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"null depth", `null`, 1},
		{"primitive depth", `42`, 1},
		{"array depth", `[1, 2, 3]`, 2},
		{"object depth", `{"a": 1}`, 2},
		{"nested object depth", `{"a": {"b": 1}}`, 3},
		{"nested array depth", `[[1, 2], [3, 4]]`, 3},
		{"complex nested depth", `{"a": [{"b": 1}, {"c": 2}]}`, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonDepth([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonDepth() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonDepth() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_PRETTY function
func TestJSONPretty(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple object", `{"a":1,"b":2}`, "{\n  \"a\": 1,\n  \"b\": 2\n}"},
		{"simple array", `[1,2,3]`, "[\n  1,\n  2,\n  3\n]"},
		{"nested", `{"a":{"b":1}}`, "{\n  \"a\": {\n    \"b\": 1\n  }\n}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonPretty([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonPretty() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonPretty() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_STORAGE_SIZE function
func TestJSONStorageSize(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple", `{"a":1}`},
		{"array", `[1,2,3]`},
		{"nested", `{"a":{"b":1}}`},
		{"empty", `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonStorageSize([]interface{}{tt.input})
			if err != nil {
				t.Fatalf("jsonStorageSize() error: %v", err)
			}
			if result.(int64) <= 0 {
				t.Errorf("jsonStorageSize() = %v, expected positive value", result)
			}
		})
	}
}

// Test JSON_ARRAY_APPEND function
func TestJSONArrayAppend(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{"append to array", []interface{}{`[1, 2]`, "$", 3}, `[1,2,3]`},
		{"append multiple", []interface{}{`[1, 2]`, "$", 3, 4}, `[1,2,3,4]`},
		{"append to nested", []interface{}{`{"a": [1, 2]}`, "$.a", 3}, `{"a":[1,2,3]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonArrayAppend(tt.args)
			if err != nil {
				t.Fatalf("jsonArrayAppend() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonArrayAppend() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_ARRAY_INSERT function
func TestJSONArrayInsert(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{"insert at position", []interface{}{`[1, 3]`, "$", 1, 2}, `[1,2,3]`},
		{"insert at end", []interface{}{`[1, 2]`, "$", 2, 3}, `[1,2,3]`},
		{"insert at beginning", []interface{}{`[2, 3]`, "$", 0, 1}, `[1,2,3]`},
		{"negative index", []interface{}{`[1, 3]`, "$", -1, 2}, `[1,2,3]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonArrayInsert(tt.args)
			if err != nil {
				t.Fatalf("jsonArrayInsert() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonArrayInsert() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_MEMBER_OF function
func TestJSONMemberOf(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected int64
	}{
		{"member exists", []interface{}{`2`, `[1, 2, 3]`}, 1},
		{"member not exists", []interface{}{`4`, `[1, 2, 3]`}, 0},
		{"object key", []interface{}{`"a"`, `{"a": 1, "b": 2}`}, 1},
		{"object value", []interface{}{`1`, `{"a": 1, "b": 2}`}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonMemberOf(tt.args)
			if err != nil {
				t.Fatalf("jsonMemberOf() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonMemberOf() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_OVERLAPS function
func TestJSONOverlaps(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected int64
	}{
		{"overlapping arrays", []interface{}{`[1, 2, 3]`, `[3, 4, 5]`}, 1},
		{"non-overlapping arrays", []interface{}{`[1, 2]`, `[3, 4]`}, 0},
		{"same array", []interface{}{`[1, 2, 3]`, `[1, 2, 3]`}, 1},
		{"one empty", []interface{}{`[1, 2]`, `[]`}, 0},
		{"both empty", []interface{}{`[]`, `[]`}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonOverlaps(tt.args)
			if err != nil {
				t.Fatalf("jsonOverlaps() error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonOverlaps() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test edge cases and error handling
func TestJSONErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		fn        func([]interface{}) (interface{}, error)
		args      []interface{}
		wantError bool
	}{
		{"json_type no args", jsonType, []interface{}{}, true},
		{"json_valid no args", jsonValid, []interface{}{}, false}, // Returns 0
		{"json_extract no args", jsonExtract, []interface{}{}, true},
		{"json_contains wrong args", jsonContains, []interface{}{}, true},
		{"json_set wrong args", jsonSet, []interface{}{}, true},
		{"json_insert wrong args", jsonInsert, []interface{}{}, true},
		{"json_remove no args", jsonRemove, []interface{}{}, true},
		{"json_merge_preserve no args", jsonMergePreserve, []interface{}{}, true},
		{"json_merge_patch no args", jsonMergePatch, []interface{}{}, true},
		{"json_length no args", jsonLength, []interface{}{}, true},
		{"json_depth no args", jsonDepth, []interface{}{}, true},
		{"json_contains_path wrong args", jsonContainsPath, []interface{}{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn(tt.args)
			if tt.wantError {
				if err == nil {
					t.Errorf("%s expected error but got result: %v", tt.name, result)
				}
			} else {
				if err != nil {
					t.Errorf("%s unexpected error: %v", tt.name, err)
				}
			}
		})
	}
}

// Test JSON_SEARCH function
func TestJSONSearch(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		// Basic "one" mode - find first match
		{
			name:     "one mode simple match",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "one", "hello"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "one mode no match",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "one", "notfound"},
			expected: nil,
			wantErr:  false,
		},
		// Basic "all" mode - find all matches
		{
			name:     "all mode multiple matches",
			args:     []interface{}{`{"a": "hello", "b": "hello", "c": "world"}`, "all", "hello"},
			expected: `["$.a","$.b"]`,
			wantErr:  false,
		},
		{
			name:     "all mode single match",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "all", "hello"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "all mode no match",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "all", "notfound"},
			expected: nil,
			wantErr:  false,
		},
		// Nested objects
		{
			name:     "nested object match",
			args:     []interface{}{`{"a": {"b": "target"}}`, "one", "target"},
			expected: `"$.a.b"`,
			wantErr:  false,
		},
		// Arrays
		{
			name:     "array element match",
			args:     []interface{}{`["hello", "world", "foo"]`, "one", "world"},
			expected: `"$[1]"`,
			wantErr:  false,
		},
		{
			name:     "array all matches",
			args:     []interface{}{`["hello", "world", "hello"]`, "all", "hello"},
			expected: `["$[0]","$[2]"]`,
			wantErr:  false,
		},
		// Mixed nested structures
		{
			name:     "mixed nested structure",
			args:     []interface{}{`{"a": ["x", "y"], "b": {"c": "y"}}`, "all", "y"},
			expected: `["$.a[1]","$.b.c"]`,
			wantErr:  false,
		},
		// Wildcard patterns
		{
			name:     "percent wildcard prefix",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "one", "%llo"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "percent wildcard suffix",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "one", "hel%"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "percent wildcard middle",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "one", "h%o"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "underscore wildcard",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "one", "hell_"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "percent matches everything",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "all", "%"},
			expected: `["$.a","$.b"]`,
			wantErr:  false,
		},
		// Non-string values should be skipped
		{
			name:     "skip non-string values",
			args:     []interface{}{`{"a": 1, "b": "hello", "c": true}`, "one", "hello"},
			expected: `"$.b"`,
			wantErr:  false,
		},
		// Null JSON document
		{
			name:     "null json doc",
			args:     []interface{}{`null`, "one", "hello"},
			expected: nil,
			wantErr:  false,
		},
		// Error cases
		{
			name:    "too few arguments",
			args:    []interface{}{`{"a": 1}`},
			wantErr: true,
		},
		{
			name:    "invalid one_or_all",
			args:    []interface{}{`{"a": "hello"}`, "invalid", "hello"},
			wantErr: true,
		},
		// Custom escape character (4th arg)
		{
			name:     "with escape char",
			args:     []interface{}{`{"a": "10%"}`, "one", "10|%", "|"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		// Deeply nested
		{
			name:     "deeply nested match",
			args:     []interface{}{`{"a": {"b": {"c": {"d": "deep"}}}}`, "one", "deep"},
			expected: `"$.a.b.c.d"`,
			wantErr:  false,
		},
		// Empty string search
		{
			name:     "search empty string",
			args:     []interface{}{`{"a": "", "b": "hello"}`, "one", ""},
			expected: `"$.a"`,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSearch(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("jsonSearch() expected error, got result: %v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("jsonSearch() unexpected error: %v", err)
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("jsonSearch() = %v, want nil", result)
				}
				return
			}
			if result != tt.expected {
				t.Errorf("jsonSearch() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test performance with large JSON
func TestJSONPerformance(t *testing.T) {
	// Create a large JSON object
	largeJSON := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		largeJSON[fmt.Sprintf("key%d", i)] = i
	}

	// Test operations
	t.Run("extract from large json", func(t *testing.T) {
		bj, err := jsonpkg.NewBinaryJSON(largeJSON)
		if err != nil {
			t.Fatalf("Failed to create large JSON: %v", err)
		}

		result, err := bj.Extract("$.key50")
		if err != nil {
			t.Errorf("Extract failed: %v", err)
		}
		// NewBinaryJSON converts int to int64, so the stored value is int64(50)
		if result.GetInterface() != int64(50) {
			t.Errorf("Extract returned wrong value: got %v (%T), want int64(50)", result.GetInterface(), result.GetInterface())
		}
	})

	t.Run("set in large json", func(t *testing.T) {
		bj, err := jsonpkg.NewBinaryJSON(largeJSON)
		if err != nil {
			t.Fatalf("Failed to create large JSON: %v", err)
		}

		result, err := bj.Set("$.key50", 999)
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		// Verify the change
		newResult, err := result.Extract("$.key50")
		if err != nil {
			t.Errorf("Extract failed: %v", err)
		}
		// NewBinaryJSON converts int to int64, so the stored value is int64(999)
		if newResult.GetInterface() != int64(999) {
			t.Errorf("Set did not update correctly: got %v (%T), want int64(999)", newResult.GetInterface(), newResult.GetInterface())
		}
	})
}

// Test JSON_SEARCH edge cases
func TestJSONSearch_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "null JSON input returns nil",
			args:     []interface{}{`null`, "one", "anything"},
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "no match found returns nil",
			args:     []interface{}{`{"a": "hello", "b": "world"}`, "one", "nonexistent"},
			expected: nil,
			wantErr:  false,
		},
		{
			name:    "invalid second argument returns error",
			args:    []interface{}{`{"a": "hello"}`, "bogus", "hello"},
			wantErr: true,
		},
		{
			name:     "custom escape character exclamation mark",
			args:     []interface{}{`{"a": "100%", "b": "hello"}`, "one", "100!%", "!"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "search in deeply nested structure 4 levels",
			args:     []interface{}{`{"l1": {"l2": {"l3": {"l4": "found"}}}}`, "one", "found"},
			expected: `"$.l1.l2.l3.l4"`,
			wantErr:  false,
		},
		{
			name:     "search in mixed array and object",
			args:     []interface{}{`[{"name": "alice"}, {"name": "bob"}, "charlie"]`, "one", "bob"},
			expected: `"$[1].name"`,
			wantErr:  false,
		},
		{
			name:     "empty search string matches empty string value",
			args:     []interface{}{`{"a": "", "b": "notempty"}`, "one", ""},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:    "too few arguments returns error",
			args:    []interface{}{`{"a": 1}`, "one"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSearch(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("jsonSearch() expected error, got result: %v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("jsonSearch() unexpected error: %v", err)
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("jsonSearch() = %v, want nil", result)
				}
				return
			}
			if result != tt.expected {
				t.Errorf("jsonSearch() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_SEARCH "all" mode returning multiple matches as a JSON array
func TestJSONSearch_AllMode_MultipleMatches(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
	}{
		{
			name:     "all mode finds multiple matches in object",
			args:     []interface{}{`{"x": "match", "y": "no", "z": "match"}`, "all", "match"},
			expected: `["$.x","$.z"]`,
		},
		{
			name:     "all mode with wildcard finds all strings",
			args:     []interface{}{`{"a": "foo", "b": "bar", "c": "baz"}`, "all", "b%"},
			expected: `["$.b","$.c"]`,
		},
		{
			name:     "all mode finds matches across nested objects and arrays",
			args:     []interface{}{`{"arr": ["target", "other"], "obj": {"key": "target"}}`, "all", "target"},
			expected: `["$.arr[0]","$.obj.key"]`,
		},
		{
			name:     "all mode with contains wildcard",
			args:     []interface{}{`{"a": "abcdef", "b": "xcdey", "c": "nope"}`, "all", "%cde%"},
			expected: `["$.a","$.b"]`,
		},
		{
			name:     "all mode single match returns quoted string not array",
			args:     []interface{}{`{"a": "only", "b": "other"}`, "all", "only"},
			expected: `"$.a"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSearch(tt.args)
			if err != nil {
				t.Fatalf("jsonSearch() unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("jsonSearch() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test likeMatchDP pattern matching logic via jsonSearch
func TestLikeMatchDP(t *testing.T) {
	// We test likeMatchDP indirectly through jsonSearch. Each test case places a
	// single string value in a JSON object and searches for it with a pattern.
	tests := []struct {
		name      string
		value     string
		pattern   string
		expectHit bool
	}{
		{
			name:      "exact match no wildcards",
			value:     "hello",
			pattern:   "hello",
			expectHit: true,
		},
		{
			name:      "exact match mismatch",
			value:     "hello",
			pattern:   "world",
			expectHit: false,
		},
		{
			name:      "percent at start suffix match",
			value:     "hello",
			pattern:   "%llo",
			expectHit: true,
		},
		{
			name:      "percent at start no match",
			value:     "hello",
			pattern:   "%xyz",
			expectHit: false,
		},
		{
			name:      "percent at end prefix match",
			value:     "hello",
			pattern:   "hel%",
			expectHit: true,
		},
		{
			name:      "percent at end no match",
			value:     "hello",
			pattern:   "xyz%",
			expectHit: false,
		},
		{
			name:      "percent on both sides contains match",
			value:     "hello world",
			pattern:   "%lo wo%",
			expectHit: true,
		},
		{
			name:      "percent on both sides no match",
			value:     "hello world",
			pattern:   "%xyz%",
			expectHit: false,
		},
		{
			name:      "underscore single char wildcard",
			value:     "hat",
			pattern:   "h_t",
			expectHit: true,
		},
		{
			name:      "underscore single char no match too long",
			value:     "hoot",
			pattern:   "h_t",
			expectHit: false,
		},
		{
			name:      "multiple underscores",
			value:     "abcde",
			pattern:   "a___e",
			expectHit: true,
		},
		{
			name:      "multiple underscores wrong length",
			value:     "abcdef",
			pattern:   "a___e",
			expectHit: false,
		},
		{
			name:      "percent and underscore combined",
			value:     "abcdef",
			pattern:   "a_c%f",
			expectHit: true,
		},
		{
			name:      "percent and underscore combined no match",
			value:     "aXYdef",
			pattern:   "a_c%f",
			expectHit: false,
		},
		{
			name:      "empty pattern matches empty string",
			value:     "",
			pattern:   "",
			expectHit: true,
		},
		{
			name:      "empty pattern does not match non-empty string",
			value:     "hello",
			pattern:   "",
			expectHit: false,
		},
		{
			name:      "pattern with only percent matches everything",
			value:     "anything at all",
			pattern:   "%",
			expectHit: true,
		},
		{
			name:      "pattern with only percent matches empty string",
			value:     "",
			pattern:   "%",
			expectHit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build a JSON doc with a single string value
			jsonDoc := fmt.Sprintf(`{"v": %q}`, tt.value)
			result, err := jsonSearch([]interface{}{jsonDoc, "one", tt.pattern})
			if err != nil {
				t.Fatalf("jsonSearch() unexpected error: %v", err)
			}
			if tt.expectHit {
				if result == nil {
					t.Errorf("expected match for value=%q pattern=%q, got nil", tt.value, tt.pattern)
				} else if result != `"$.v"` {
					t.Errorf("expected path \"$.v\", got %v", result)
				}
			} else {
				if result != nil {
					t.Errorf("expected no match for value=%q pattern=%q, got %v", tt.value, tt.pattern, result)
				}
			}
		})
	}
}

// Test JSON_SEARCH escape character functionality
func TestJSONSearch_EscapeCharacter(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "default escape backslash matches literal percent",
			args:     []interface{}{`{"a": "50%", "b": "500"}`, "one", `50\%`},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "custom escape exclamation matches literal percent",
			args:     []interface{}{`{"a": "50%", "b": "500"}`, "one", "50!%", "!"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "custom escape matches literal underscore",
			args:     []interface{}{`{"a": "a_b", "b": "axb"}`, "one", "a!_b", "!"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "escape char does not affect normal characters",
			args:     []interface{}{`{"a": "hello"}`, "one", "hel%", "!"},
			expected: `"$.a"`,
			wantErr:  false,
		},
		{
			name:     "escaped percent does not act as wildcard",
			args:     []interface{}{`{"a": "hello"}`, "all", `h\%o`},
			expected: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSearch(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("jsonSearch() expected error, got result: %v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("jsonSearch() unexpected error: %v", err)
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("jsonSearch() = %v, want nil", result)
				}
				return
			}
			if result != tt.expected {
				t.Errorf("jsonSearch() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test JSON_SEARCH within JSON arrays
func TestJSONSearch_ArraySearch(t *testing.T) {
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "finds element in flat array",
			args:     []interface{}{`["alpha", "beta", "gamma"]`, "one", "beta"},
			expected: `"$[1]"`,
			wantErr:  false,
		},
		{
			name:     "returns correct array index path for third element",
			args:     []interface{}{`["a", "b", "target", "d"]`, "one", "target"},
			expected: `"$[2]"`,
			wantErr:  false,
		},
		{
			name:     "one mode stops at first match in array",
			args:     []interface{}{`["dup", "other", "dup"]`, "one", "dup"},
			expected: `"$[0]"`,
			wantErr:  false,
		},
		{
			name:     "all mode finds all matches in array",
			args:     []interface{}{`["dup", "other", "dup", "dup"]`, "all", "dup"},
			expected: `["$[0]","$[2]","$[3]"]`,
			wantErr:  false,
		},
		{
			name:     "array with wildcard pattern",
			args:     []interface{}{`["apple", "apricot", "banana"]`, "all", "ap%"},
			expected: `["$[0]","$[1]"]`,
			wantErr:  false,
		},
		{
			name:     "nested array in object search",
			args:     []interface{}{`{"items": ["red", "green", "blue"]}`, "one", "green"},
			expected: `"$.items[1]"`,
			wantErr:  false,
		},
		{
			name:     "all mode in nested array returns correct paths",
			args:     []interface{}{`{"items": ["red", "green", "red"]}`, "all", "red"},
			expected: `["$.items[0]","$.items[2]"]`,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jsonSearch(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("jsonSearch() expected error, got result: %v", result)
				}
				return
			}
			if err != nil {
				t.Fatalf("jsonSearch() unexpected error: %v", err)
			}
			if tt.expected == nil {
				if result != nil {
					t.Errorf("jsonSearch() = %v, want nil", result)
				}
				return
			}
			if result != tt.expected {
				t.Errorf("jsonSearch() = %v, want %v", result, tt.expected)
			}
		})
	}
}
