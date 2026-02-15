package builtin

import (
	"strings"
	"testing"
	"time"
)

func TestTypeOf(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{nil, "NULL"},
		{true, "BOOLEAN"},
		{42, "INTEGER"},
		{int64(42), "INTEGER"},
		{3.14, "DOUBLE"},
		{float32(3.14), "DOUBLE"},
		{"hello", "VARCHAR"},
		{[]byte("data"), "BLOB"},
		{time.Now(), "TIMESTAMP"},
		{[]interface{}{1, 2}, "LIST"},
		{map[string]interface{}{"a": 1}, "STRUCT"},
	}
	for _, tt := range tests {
		result, err := sysTypeOf([]interface{}{tt.input})
		if err != nil {
			t.Errorf("sysTypeOf(%v) error = %v", tt.input, err)
			continue
		}
		if result != tt.want {
			t.Errorf("sysTypeOf(%v) = %v, want %v", tt.input, result, tt.want)
		}
	}
}

func TestVersion(t *testing.T) {
	result, err := sysVersion(nil)
	if err != nil {
		t.Fatalf("sysVersion() error = %v", err)
	}
	s, ok := result.(string)
	if !ok {
		t.Fatalf("sysVersion() returned %T, want string", result)
	}
	if !strings.Contains(s, "SQLExec") {
		t.Errorf("sysVersion() = %s, expected to contain 'SQLExec'", s)
	}
}

func TestCurrentDatabase(t *testing.T) {
	result, err := sysCurrentDatabase(nil)
	if err != nil {
		t.Fatalf("sysCurrentDatabase() error = %v", err)
	}
	if result != "default" {
		t.Errorf("sysCurrentDatabase() = %v, want 'default'", result)
	}
}

func TestUUID(t *testing.T) {
	result, err := sysUUID(nil)
	if err != nil {
		t.Fatalf("sysUUID() error = %v", err)
	}
	s, ok := result.(string)
	if !ok {
		t.Fatalf("sysUUID() returned %T, want string", result)
	}
	// UUID v4 format: 8-4-4-4-12
	if len(s) != 36 {
		t.Errorf("sysUUID() length = %d, want 36", len(s))
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		t.Errorf("sysUUID() format invalid: %s", s)
	}
	// Version must be 4
	if s[14] != '4' {
		t.Errorf("sysUUID() version byte = %c, want '4'", s[14])
	}

	// Generate two UUIDs and ensure they're different
	result2, _ := sysUUID(nil)
	if result == result2 {
		t.Error("two UUID calls returned the same value")
	}
}

func TestSetseed(t *testing.T) {
	result, err := sysSetseed([]interface{}{0.5})
	if err != nil {
		t.Fatalf("sysSetseed() error = %v", err)
	}
	if result != nil {
		t.Errorf("sysSetseed() = %v, want nil", result)
	}

	// Test with int
	_, err = sysSetseed([]interface{}{42})
	if err != nil {
		t.Errorf("sysSetseed(42) error = %v", err)
	}

	// Test with nil
	_, err = sysSetseed([]interface{}{nil})
	if err != nil {
		t.Errorf("sysSetseed(nil) error = %v", err)
	}
}

func TestSleep(t *testing.T) {
	start := time.Now()
	result, err := sysSleep([]interface{}{0.01}) // 10ms
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("sysSleep() error = %v", err)
	}
	if result != int64(0) {
		t.Errorf("sysSleep() = %v, want 0", result)
	}
	if elapsed < 5*time.Millisecond {
		t.Errorf("sysSleep(0.01) returned too fast: %v", elapsed)
	}
}

func TestSystemFunctions_Registration(t *testing.T) {
	funcs := []string{"typeof", "version", "current_database", "current_schema", "uuid", "gen_random_uuid", "setseed", "sleep"}
	for _, name := range funcs {
		fn, ok := GetGlobal(name)
		if !ok {
			t.Errorf("function %s not registered", name)
			continue
		}
		if fn.Category != "system" {
			t.Errorf("function %s category = %s, want system", name, fn.Category)
		}
	}
}
