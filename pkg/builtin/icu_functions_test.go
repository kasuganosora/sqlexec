package builtin

import (
	"testing"
)

func TestIcuSortKey(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantNil bool
		wantErr bool
	}{
		{"basic string", []interface{}{"hello", "utf8mb4_unicode_ci"}, false, false},
		{"empty string", []interface{}{"", "utf8mb4_unicode_ci"}, false, false},
		{"binary collation", []interface{}{"hello", "utf8mb4_bin"}, false, false},
		{"nil input", []interface{}{nil, "utf8mb4_unicode_ci"}, true, false},
		{"too few args", []interface{}{"hello"}, false, true},
		{"no args", []interface{}{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuSortKey(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			s, ok := result.(string)
			if !ok {
				t.Errorf("expected string, got %T", result)
			}
			if len(s) == 0 && tt.args[0] != nil && tt.args[0] != "" {
				t.Error("expected non-empty sort key")
			}
		})
	}
}

func TestIcuCollation(t *testing.T) {
	result, err := icuCollation([]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "utf8mb4_unicode_ci" {
		t.Errorf("collation() = %v, want utf8mb4_unicode_ci", result)
	}
}

func TestIcuUnicodeNormalize(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantNil bool
		wantErr bool
	}{
		{"NFC default", []interface{}{"hello"}, "hello", false, false},
		{"NFC explicit", []interface{}{"hello", "NFC"}, "hello", false, false},
		{"NFD form", []interface{}{"hello", "NFD"}, "hello", false, false},
		{"NFKC form", []interface{}{"hello", "NFKC"}, "hello", false, false},
		{"NFKD form", []interface{}{"hello", "NFKD"}, "hello", false, false},
		{"case insensitive form name", []interface{}{"hello", "nfc"}, "hello", false, false},
		{"unsupported form", []interface{}{"hello", "INVALID"}, "", false, true},
		{"nil input", []interface{}{nil}, "", true, false},
		{"no args", []interface{}{}, "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuUnicodeNormalize(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if result.(string) != tt.want {
				t.Errorf("unicode_normalize(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestIcuStripAccents(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantNil bool
		wantErr bool
	}{
		{"cafe", []interface{}{"café"}, "cafe", false, false},
		{"plain ascii", []interface{}{"hello"}, "hello", false, false},
		{"empty string", []interface{}{""}, "", false, false},
		{"nil input", []interface{}{nil}, "", true, false},
		{"no args", []interface{}{}, "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuStripAccents(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if result.(string) != tt.want {
				t.Errorf("strip_accents(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestIcuCompare(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantNil bool
		wantErr bool
	}{
		{"equal strings", []interface{}{"hello", "hello"}, 0, false, false},
		{"a < b", []interface{}{"a", "b"}, -1, false, false},
		{"b > a", []interface{}{"b", "a"}, 1, false, false},
		{"case insensitive equal", []interface{}{"Hello", "hello", "utf8mb4_unicode_ci"}, 0, false, false},
		{"with explicit collation", []interface{}{"a", "b", "utf8mb4_general_ci"}, -1, false, false},
		{"nil first arg", []interface{}{nil, "hello"}, 0, true, false},
		{"nil second arg", []interface{}{"hello", nil}, 0, true, false},
		{"too few args", []interface{}{"hello"}, 0, false, true},
		{"no args", []interface{}{}, 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuCompare(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if result.(int64) != tt.want {
				t.Errorf("icu_compare(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestIcuTransliterate(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantNil bool
		wantErr bool
	}{
		{"Latin-ASCII", []interface{}{"café", "Latin-ASCII"}, "cafe", false, false},
		{"Upper", []interface{}{"hello", "Upper"}, "HELLO", false, false},
		{"Lower", []interface{}{"HELLO", "Lower"}, "hello", false, false},
		{"unknown rule", []interface{}{"hello", "Unknown"}, "", false, true},
		{"nil input", []interface{}{nil, "Upper"}, "", true, false},
		{"nil rule", []interface{}{"hello", nil}, "", false, true},
		{"no args", []interface{}{}, "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuTransliterate(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if result.(string) != tt.want {
				t.Errorf("transliterate(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestIcuNFC(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantNil bool
		wantErr bool
	}{
		{"basic string", []interface{}{"hello"}, false, false},
		{"empty string", []interface{}{""}, false, false},
		{"nil input", []interface{}{nil}, true, false},
		{"no args", []interface{}{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuNFC(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if _, ok := result.(string); !ok {
				t.Errorf("expected string result, got %T", result)
			}
		})
	}
}

func TestIcuNFD(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantNil bool
		wantErr bool
	}{
		{"basic string", []interface{}{"hello"}, false, false},
		{"empty string", []interface{}{""}, false, false},
		{"nil input", []interface{}{nil}, true, false},
		{"no args", []interface{}{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuNFD(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if _, ok := result.(string); !ok {
				t.Errorf("expected string result, got %T", result)
			}
		})
	}
}

func TestIcuNFKC(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantNil bool
		wantErr bool
	}{
		{"basic string", []interface{}{"hello"}, false, false},
		{"empty string", []interface{}{""}, false, false},
		{"nil input", []interface{}{nil}, true, false},
		{"no args", []interface{}{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuNFKC(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if _, ok := result.(string); !ok {
				t.Errorf("expected string result, got %T", result)
			}
		})
	}
}

func TestIcuNFKD(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		wantNil bool
		wantErr bool
	}{
		{"basic string", []interface{}{"hello"}, false, false},
		{"empty string", []interface{}{""}, false, false},
		{"nil input", []interface{}{nil}, true, false},
		{"no args", []interface{}{}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := icuNFKD(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if _, ok := result.(string); !ok {
				t.Errorf("expected string result, got %T", result)
			}
		})
	}
}

func TestNFKCDecomposition(t *testing.T) {
	// The fi ligature (ﬁ) should decompose to "fi" under NFKC
	result, err := icuNFKC([]interface{}{"ﬁ"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.(string) != "fi" {
		t.Errorf("nfkc(ﬁ) = %q, want %q", result, "fi")
	}
}

func TestStripAccentsUnicode(t *testing.T) {
	result, err := icuStripAccents([]interface{}{"café"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.(string) != "cafe" {
		t.Errorf("strip_accents(\"café\") = %q, want %q", result, "cafe")
	}
}

func TestIcuFunctionsRegistered(t *testing.T) {
	names := []string{
		"icu_sort_key",
		"collation",
		"unicode_normalize",
		"strip_accents",
		"icu_compare",
		"transliterate",
		"nfc",
		"nfd",
		"nfkc",
		"nfkd",
	}

	for _, name := range names {
		fn, exists := GetGlobal(name)
		if !exists {
			t.Errorf("function %s should be registered", name)
			continue
		}
		if fn.Category != "icu" {
			t.Errorf("function %s category = %q, want %q", name, fn.Category, "icu")
		}
	}
}

func TestIcuSortKeyDeterministic(t *testing.T) {
	r1, err := icuSortKey([]interface{}{"hello", "utf8mb4_unicode_ci"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r2, err := icuSortKey([]interface{}{"hello", "utf8mb4_unicode_ci"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r1.(string) != r2.(string) {
		t.Errorf("sort key should be deterministic: %v != %v", r1, r2)
	}
}

func TestIcuCompareDefaultCollation(t *testing.T) {
	// Without explicit collation, should use utf8mb4_unicode_ci (case-insensitive)
	result, err := icuCompare([]interface{}{"Hello", "hello"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.(int64) != 0 {
		t.Errorf("icu_compare(\"Hello\", \"hello\") = %v, want 0 (case insensitive)", result)
	}
}
