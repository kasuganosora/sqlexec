package json

import (
	"testing"
)

func TestParseJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid object string", `{"a": 1, "b": "test"}`, false},
		{"valid array string", `[1, 2, 3]`, false},
		{"valid string", `"hello"`, false},
		{"valid number", `42`, false},
		{"valid boolean", `true`, false},
		{"valid null", `null`, false},
		{"invalid json", `{invalid}`, true},
		{"empty string", ``, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseJSON() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("ParseJSON() unexpected error: %v", err)
				return
			}
			// TypeCode 0 (TypeLiteral) is valid for boolean and null values
			if bj.Value == nil && bj.TypeCode != 0 {
				t.Errorf("ParseJSON() null value should have TypeLiteral type code")
			}
		})
	}
}



func TestBinaryJSON_Type(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"object", `{"a": 1}`, "OBJECT"},
		{"array", `[1, 2, 3]`, "ARRAY"},
		{"string", `"hello"`, "STRING"},
		{"integer", `42`, "INTEGER"}, // JSON numbers are parsed as float64 by encoding/json, then converted to INTEGER if whole number
		{"float", `3.14`, "DOUBLE"},
		{"boolean", `true`, "BOOLEAN"},
		{"null", `null`, "NULL"},
		{"nested object", `{"a": {"b": 1}}`, "OBJECT"},
		{"nested array", `[{"a": 1}, {"b": 2}]`, "ARRAY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.input)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}
			got := bj.Type()
			if got != tt.want {
				t.Errorf("BinaryJSON.Type() = %v, want %v", got, tt.want)
			}
		})
	}
}



func TestBinaryJSON_Extract(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		path      string
		wantErr   bool
		wantValue interface{}
	}{
		{"simple key", `{"a": 1}`, "$.a", false, 1.0},
		{"nested key", `{"a": {"b": 1}}`, "$.a.b", false, 1.0},
		{"array index", `[1, 2, 3]`, "$[0]", false, 1.0},
		{"array nested", `{"a": [1, 2, 3]}`, "$.a[1]", false, 2.0},
		{"array wildcard", `[1, 2, 3]`, "$[*]", false, []interface{}{1.0, 2.0, 3.0}},
		{"wildcard keys", `{"a": 1, "b": 2}`, "$.*", false, []interface{}{1.0, 2.0}},
		{"last index", `[1, 2, 3]`, "$[last]", false, 3.0},
		{"negative index", `[1, 2, 3]`, "$[-1]", false, 3.0},
		{"range", `[1, 2, 3, 4, 5]`, "$[0 to 2]", false, []interface{}{1.0, 2.0, 3.0}},
		{"invalid path", `{"a": 1}`, "$.x", true, nil},
		{"out of bounds", `[1, 2, 3]`, "$[10]", true, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := bj.Extract(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Extract() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Extract() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("Extract() = %v (type %T), want %v (type %T)", got, got, tt.wantValue, tt.wantValue)
			}
		})
	}
}

func TestBinaryJSON_Set(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		path      string
		value     interface{}
		wantValue interface{}
	}{
		{"set existing key", `{"a": 1}`, "$.a", 2, map[string]interface{}{"a": 2.0}},
		{"set new key", `{"a": 1}`, "$.b", 2, map[string]interface{}{"a": 1.0, "b": 2.0}},
		{"set array element", `[1, 2, 3]`, "$[1]", 10, []interface{}{1.0, 10.0, 3.0}},
		{"nested object", `{"a": {"b": 1}}`, "$.a.c", 2, map[string]interface{}{"a": map[string]interface{}{"b": 1.0, "c": 2.0}}},
		{"nested array", `{"a": [1, 2, 3]}`, "$.a[1]", 10, map[string]interface{}{"a": []interface{}{1.0, 10.0, 3.0}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := bj.Set(tt.path, tt.value)
			if err != nil {
				t.Errorf("Set() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("Set() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestBinaryJSON_Insert(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		path      string
		value     interface{}
		wantValue interface{}
		wantErr   bool
	}{
		{"insert new key", `{"a": 1}`, "$.b", 2, map[string]interface{}{"a": 1.0, "b": 2.0}, false},
		{"insert existing key should fail", `{"a": 1}`, "$.a", 2, nil, true},
		{"insert nested", `{"a": {}}`, "$.a.b", 2, map[string]interface{}{"a": map[string]interface{}{"b": 2.0}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := bj.Insert(tt.path, tt.value)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Insert() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Insert() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("Insert() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestBinaryJSON_Replace(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		path      string
		value     interface{}
		wantValue interface{}
		wantErr   bool
	}{
		{"replace existing key", `{"a": 1}`, "$.a", 2, map[string]interface{}{"a": 2.0}, false},
		{"replace non-existing should fail", `{"a": 1}`, "$.b", 2, nil, true},
		{"replace array element", `[1, 2, 3]`, "$[1]", 10, []interface{}{1.0, 10.0, 3.0}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := bj.Replace(tt.path, tt.value)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Replace() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Replace() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("Replace() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestBinaryJSON_Remove(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		path      string
		wantValue interface{}
	}{
		{"remove key", `{"a": 1, "b": 2}`, "$.b", map[string]interface{}{"a": 1.0}},
		{"remove array element", `[1, 2, 3]`, "$[1]", []interface{}{1.0, 3.0}},
		{"remove last", `[1, 2, 3]`, "$[last]", []interface{}{1.0, 2.0}},
		{"remove nested", `{"a": {"b": 1, "c": 2}}`, "$.a.b", map[string]interface{}{"a": map[string]interface{}{"c": 2.0}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := bj.Remove(tt.path)
			if err != nil {
				t.Errorf("Remove() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("Remove() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestBinaryJSON_RemoveMultiple(t *testing.T) {
	bj, err := ParseJSON(`{"a": 1, "b": 2, "c": 3}`)
	if err != nil {
		t.Fatalf("ParseJSON() error: %v", err)
	}

	result, err := bj.Remove("$.b", "$.c")
	if err != nil {
		t.Fatalf("Remove() error: %v", err)
	}

	got := result.GetInterface()
	wantValue := map[string]interface{}{"a": 1.0}
	if !equalJSON(got, wantValue) {
		t.Errorf("Remove() = %v, want %v", got, wantValue)
	}
}


func TestBinaryJSON_Merge(t *testing.T) {
	tests := []struct {
		name      string
		json1     string
		json2     interface{}
		wantValue interface{}
	}{
		{"merge two objects", `{"a": 1}`, `{"b": 2}`, map[string]interface{}{"a": 1.0, "b": 2.0}},
		{"merge overlapping keys", `{"a": 1}`, `{"a": 2, "b": 3}`, map[string]interface{}{"a": 1.0, "b": 3.0}},
		{"merge null with value", `null`, `{"a": 1}`, map[string]interface{}{"a": 1.0}},
		{"merge value with null", `{"a": 1}`, `null`, map[string]interface{}{"a": 1.0}},
		{"merge arrays", `[1, 2]`, `[3, 4]`, []interface{}{1.0, 2.0, 3.0, 4.0}},
		{"merge array with value", `[1, 2]`, `3`, []interface{}{1.0, 2.0, 3.0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj1, err := ParseJSON(tt.json1)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := bj1.Merge(tt.json2)
			if err != nil {
				t.Errorf("Merge() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("Merge() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestBinaryJSON_Patch(t *testing.T) {
	tests := []struct {
		name      string
		json1     string
		json2     string
		wantValue interface{}
	}{
		{"patch simple", `{"a": 1, "b": 2}`, `{"a": 3}`, map[string]interface{}{"a": 3.0, "b": 2.0}},
		{"patch add key", `{"a": 1}`, `{"b": 2}`, map[string]interface{}{"a": 1.0, "b": 2.0}},
		{"patch delete key", `{"a": 1, "b": 2}`, `{"b": null}`, map[string]interface{}{"a": 1.0}},
		{"patch null patch", `{"a": 1}`, `null`, nil},
		{"patch nested", `{"a": {"b": 1}}`, `{"a": {"b": 2, "c": 3}}`, map[string]interface{}{"a": map[string]interface{}{"b": 2.0, "c": 3.0}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj1, err := ParseJSON(tt.json1)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := bj1.Patch(tt.json2)
			if err != nil {
				t.Errorf("Patch() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("Patch() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestLength(t *testing.T) {
	tests := []struct {
		name  string
		json  string
		want  int
		wantErr bool
	}{
		{"array length", `[1, 2, 3]`, 3, false},
		{"object length", `{"a": 1, "b": 2, "c": 3}`, 3, false},
		{"string length", `"hello"`, 5, false},
		{"null length", `null`, 0, false},
		{"number length", `42`, 1, false},
		{"empty array", `[]`, 0, false},
		{"empty object", `{}`, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			got, err := Length(bj)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Length() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Length() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Length() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDepth(t *testing.T) {
	tests := []struct {
		name string
		json string
		want int
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
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			got, err := Depth(bj)
			if err != nil {
				t.Errorf("Depth() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Depth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeys(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		wantValue []interface{}
		wantErr   bool
	}{
		{"simple object", `{"a": 1, "b": 2}`, []interface{}{"a", "b"}, false},
		{"nested object", `{"a": {"b": 1}}`, []interface{}{"a"}, false},
		{"empty object", `{}`, []interface{}{}, false},
		{"array should fail", `[1, 2, 3]`, nil, true},
		{"string should fail", `"hello"`, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := Keys(bj)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Keys() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("Keys() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			gotArray, ok := got.([]interface{})
			if !ok {
				t.Errorf("Keys() returned non-array: %T", got)
				return
			}

			// Check that all expected keys are present (order may vary)
			if len(gotArray) != len(tt.wantValue) {
				t.Errorf("Keys() length = %v, want %v", len(gotArray), len(tt.wantValue))
				return
			}

			gotMap := make(map[interface{}]bool)
			for _, k := range gotArray {
				gotMap[k] = true
			}

			for _, wantKey := range tt.wantValue {
				if !gotMap[wantKey] {
					t.Errorf("Keys() missing key %v", wantKey)
				}
			}
		})
	}
}

func TestArrayAppend(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		args      []interface{}
		wantValue interface{}
		wantErr   bool
	}{
		{"append to array", `[1, 2]`, []interface{}{"$", 3}, []interface{}{1.0, 2.0, 3.0}, false},
		{"append multiple", `[1, 2]`, []interface{}{"$", 3, 4}, []interface{}{1.0, 2.0, 3.0, 4.0}, false},
		{"append to nested", `{"a": [1, 2]}`, []interface{}{"$.a", 3}, map[string]interface{}{"a": []interface{}{1.0, 2.0, 3.0}}, false},
		{"non-array should fail", `{"a": 1}`, []interface{}{"$.a", 2}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := ArrayAppend(bj, tt.args...)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ArrayAppend() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("ArrayAppend() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("ArrayAppend() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestArrayInsert(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		args      []interface{}
		wantValue interface{}
		wantErr   bool
	}{
		{"insert at position", `[1, 3]`, []interface{}{"$", 1, 2}, []interface{}{1.0, 2.0, 3.0}, false},
		{"insert at end", `[1, 2]`, []interface{}{"$", 2, 3}, []interface{}{1.0, 2.0, 3.0}, false},
		{"insert at beginning", `[2, 3]`, []interface{}{"$", 0, 1}, []interface{}{1.0, 2.0, 3.0}, false},
		{"negative index", `[1, 3]`, []interface{}{"$", -1, 2}, []interface{}{1.0, 2.0, 3.0}, false},
		{"out of bounds should fail", `[1, 2]`, []interface{}{"$", 10, 3}, nil, true},
		{"non-array should fail", `{"a": 1}`, []interface{}{"$.a", 1, 2}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			result, err := ArrayInsert(bj, tt.args...)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ArrayInsert() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("ArrayInsert() unexpected error: %v", err)
				return
			}

			got := result.GetInterface()
			if !equalJSON(got, tt.wantValue) {
				t.Errorf("ArrayInsert() = %v, want %v", got, tt.wantValue)
			}
		})
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		name  string
		source interface{}
		target interface{}
		want  bool
	}{
		{"array contains element", `[1, 2, 3]`, `2`, true},
		{"array not contains", `[1, 2, 3]`, `4`, false},
		{"object contains key", `{"a": 1, "b": 2}`, `{"a": 1}`, true},
		{"object not contains", `{"a": 1}`, `{"b": 2}`, false},
		{"exact match", `{"a": 1}`, `{"a": 1}`, true},
		{"partial match", `{"a": 1, "b": 2}`, `{"a": 1}`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Contains(tt.source, tt.target)
			if err != nil {
				t.Errorf("Contains() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContainsPath(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		oneOrAll  string
		paths     []string
		want      bool
	}{
		{"one path exists", `{"a": 1}`, "one", []string{"$.a"}, true},
		{"one path not exists", `{"a": 1}`, "one", []string{"$.b"}, false},
		{"one of many exists", `{"a": 1, "b": 2}`, "one", []string{"$.a", "$.c"}, true},
		{"all paths exist", `{"a": 1, "b": 2}`, "all", []string{"$.a", "$.b"}, true},
		{"not all paths exist", `{"a": 1, "b": 2}`, "all", []string{"$.a", "$.c"}, false},
		{"nested path", `{"a": {"b": 1}}`, "one", []string{"$.a.b"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bj, err := ParseJSON(tt.json)
			if err != nil {
				t.Fatalf("ParseJSON() error: %v", err)
			}

			got, err := ContainsPath(bj, tt.oneOrAll, tt.paths...)
			if err != nil {
				t.Errorf("ContainsPath() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("ContainsPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemberOf(t *testing.T) {
	tests := []struct {
		name      string
		target    interface{}
		container interface{}
		want      bool
	}{
		{"member exists", `2`, `[1, 2, 3]`, true},
		{"member not exists", `4`, `[1, 2, 3]`, false},
		{"object key member", `"a"`, `{"a": 1, "b": 2}`, true},
		{"object value member", `1`, `{"a": 1, "b": 2}`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MemberOf(tt.target, tt.container)
			if err != nil {
				t.Errorf("MemberOf() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("MemberOf() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOverlaps(t *testing.T) {
	tests := []struct {
		name string
		a    interface{}
		b    interface{}
		want bool
	}{
		{"overlapping arrays", `[1, 2, 3]`, `[3, 4, 5]`, true},
		{"non-overlapping arrays", `[1, 2]`, `[3, 4]`, false},
		{"same array", `[1, 2, 3]`, `[1, 2, 3]`, true},
		{"one empty", `[1, 2]`, `[]`, false},
		{"both empty", `[]`, `[]`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Overlaps(tt.a, tt.b)
			if err != nil {
				t.Errorf("Overlaps() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Overlaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuote(t *testing.T) {
	tests := []struct {
		name string
		input string
		want string
	}{
		{"simple string", "hello", `"hello"`},
		{"with quote", `"hello"`, `"\"hello\""`},
		{"with backslash", `hello\world`, `"hello\\world"`},
		{"with newline", "hello\nworld", `"hello\nworld"`},
		{"empty string", "", `""`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Quote(tt.input)
			if got != tt.want {
				t.Errorf("Quote() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUnquote(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple string", `"hello"`, "hello"},
		{"with escape", `"\"hello\""`, `"hello"`},
		{"empty string", `""`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Unquote(tt.input)
			if err != nil {
				t.Errorf("Unquote() unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("Unquote() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Helper function to compare JSON values
func equalJSON(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Type assertion and comparison
	switch aVal := a.(type) {
	case float64:
		if bVal, ok := b.(float64); ok {
			return aVal == bVal
		}
		// Also compare with int types (JSON numbers may be float64 or int64)
		if bVal, ok := b.(int64); ok {
			return float64(bVal) == aVal
		}
	case int64:
		if bVal, ok := b.(int64); ok {
			return aVal == bVal
		}
		// Also compare with float64 (JSON numbers may be float64 or int64)
		if bVal, ok := b.(float64); ok {
			return aVal == int64(bVal)
		}
	case string:
		if bVal, ok := b.(string); ok {
			return aVal == bVal
		}
	case bool:
		if bVal, ok := b.(bool); ok {
			return aVal == bVal
		}
	case []interface{}:
		if bVal, ok := b.([]interface{}); ok {
			if len(aVal) != len(bVal) {
				return false
			}
			for i := range aVal {
				if !equalJSON(aVal[i], bVal[i]) {
					return false
				}
			}
			return true
		}
	case map[string]interface{}:
		if bVal, ok := b.(map[string]interface{}); ok {
			if len(aVal) != len(bVal) {
				return false
			}
			for k, v := range aVal {
				if bv, ok := bVal[k]; !ok || !equalJSON(v, bv) {
					return false
				}
			}
			return true
		}
	}

	return false
}
