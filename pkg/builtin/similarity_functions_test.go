package builtin

import (
	"math"
	"testing"
)

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want int64
	}{
		{"kitten-sitting", "kitten", "sitting", 3},
		{"empty-empty", "", "", 0},
		{"empty-abc", "", "abc", 3},
		{"abc-empty", "abc", "", 3},
		{"same", "hello", "hello", 0},
		{"single-char", "a", "b", 1},
		{"flaw-lawn", "flaw", "lawn", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := similarityLevenshtein([]interface{}{tt.a, tt.b})
			if err != nil {
				t.Fatalf("levenshtein(%q, %q) error = %v", tt.a, tt.b, err)
			}
			if result != tt.want {
				t.Errorf("levenshtein(%q, %q) = %v, want %v", tt.a, tt.b, result, tt.want)
			}
		})
	}
}

func TestDamerauLevenshtein(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want int64
	}{
		{"ca-abc", "ca", "abc", 3},
		{"empty-empty", "", "", 0},
		{"empty-abc", "", "abc", 3},
		{"same", "hello", "hello", 0},
		{"transposition", "ab", "ba", 1},
		{"single-sub", "a", "b", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := similarityDamerauLevenshtein([]interface{}{tt.a, tt.b})
			if err != nil {
				t.Fatalf("damerau_levenshtein(%q, %q) error = %v", tt.a, tt.b, err)
			}
			if result != tt.want {
				t.Errorf("damerau_levenshtein(%q, %q) = %v, want %v", tt.a, tt.b, result, tt.want)
			}
		})
	}
}

func TestHamming(t *testing.T) {
	tests := []struct {
		name    string
		a, b    string
		want    int64
		wantErr bool
	}{
		{"karolin-kathrin", "karolin", "kathrin", 3, false},
		{"same", "hello", "hello", 0, false},
		{"all-diff", "abc", "xyz", 3, false},
		{"empty-empty", "", "", 0, false},
		{"diff-length", "abc", "ab", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := similarityHamming([]interface{}{tt.a, tt.b})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("hamming(%q, %q) expected error, got nil", tt.a, tt.b)
				}
				return
			}
			if err != nil {
				t.Fatalf("hamming(%q, %q) error = %v", tt.a, tt.b, err)
			}
			if result != tt.want {
				t.Errorf("hamming(%q, %q) = %v, want %v", tt.a, tt.b, result, tt.want)
			}
		})
	}
}

func TestJaccard(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want float64
	}{
		{"night-nacht", "night", "nacht", 0.1428},
		{"same", "hello", "hello", 1.0},
		{"empty-empty", "", "", 1.0},
		{"a-b", "a", "b", 1.0}, // single chars have no bigrams, both empty sets
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := similarityJaccard([]interface{}{tt.a, tt.b})
			if err != nil {
				t.Fatalf("jaccard(%q, %q) error = %v", tt.a, tt.b, err)
			}
			f := result.(float64)
			if math.Abs(f-tt.want) > 0.01 {
				t.Errorf("jaccard(%q, %q) = %v, want %v", tt.a, tt.b, f, tt.want)
			}
		})
	}
}

func TestJaroSimilarity(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want float64
	}{
		{"martha-marhta", "martha", "marhta", 0.944},
		{"same", "hello", "hello", 1.0},
		{"empty-empty", "", "", 1.0},
		{"empty-a", "", "a", 0.0},
		{"a-empty", "a", "", 0.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := similarityJaro([]interface{}{tt.a, tt.b})
			if err != nil {
				t.Fatalf("jaro_similarity(%q, %q) error = %v", tt.a, tt.b, err)
			}
			f := result.(float64)
			if math.Abs(f-tt.want) > 0.01 {
				t.Errorf("jaro_similarity(%q, %q) = %v, want %v", tt.a, tt.b, f, tt.want)
			}
		})
	}
}

func TestJaroWinklerSimilarity(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want float64
	}{
		{"martha-marhta", "martha", "marhta", 0.961},
		{"same", "hello", "hello", 1.0},
		{"empty-empty", "", "", 1.0},
		{"empty-a", "", "a", 0.0},
		{"dwayne-duane", "dwayne", "duane", 0.84},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := similarityJaroWinkler([]interface{}{tt.a, tt.b})
			if err != nil {
				t.Fatalf("jaro_winkler_similarity(%q, %q) error = %v", tt.a, tt.b, err)
			}
			f := result.(float64)
			if math.Abs(f-tt.want) > 0.01 {
				t.Errorf("jaro_winkler_similarity(%q, %q) = %v, want %v", tt.a, tt.b, f, tt.want)
			}
		})
	}
}

// TestChr tests the chr/char function
func TestChr(t *testing.T) {
	tests := []struct {
		name string
		arg  interface{}
		want string
	}{
		{"A", int64(65), "A"},
		{"a", int64(97), "a"},
		{"zero", int64(0), "\x00"},
		{"emoji", int64(128512), "\U0001F600"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringChr([]interface{}{tt.arg})
			if err != nil {
				t.Fatalf("chr(%v) error = %v", tt.arg, err)
			}
			if result != tt.want {
				t.Errorf("chr(%v) = %v, want %v", tt.arg, result, tt.want)
			}
		})
	}
}

// TestUnicode tests the unicode function
func TestUnicode(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want int64
	}{
		{"A", "A", 65},
		{"a", "a", 97},
		{"hello", "hello", 104},
		{"empty", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringUnicode([]interface{}{tt.arg})
			if err != nil {
				t.Fatalf("unicode(%q) error = %v", tt.arg, err)
			}
			if result != tt.want {
				t.Errorf("unicode(%q) = %v, want %v", tt.arg, result, tt.want)
			}
		})
	}
}

// TestTranslate tests the translate function
func TestTranslate(t *testing.T) {
	tests := []struct {
		name        string
		s, from, to string
		want        string
	}{
		{"basic", "hello", "el", "ip", "hippo"},
		{"no-match", "hello", "xyz", "abc", "hello"},
		{"delete-extra", "hello", "hel", "xy", "xyo"},
		{"empty-string", "", "abc", "xyz", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringTranslate([]interface{}{tt.s, tt.from, tt.to})
			if err != nil {
				t.Fatalf("translate(%q, %q, %q) error = %v", tt.s, tt.from, tt.to, err)
			}
			if result != tt.want {
				t.Errorf("translate(%q, %q, %q) = %v, want %v", tt.s, tt.from, tt.to, result, tt.want)
			}
		})
	}
}

// TestStartsWith tests the starts_with function
func TestStartsWith(t *testing.T) {
	tests := []struct {
		name   string
		s, pre string
		want   bool
	}{
		{"match", "hello", "he", true},
		{"no-match", "hello", "lo", false},
		{"empty-prefix", "hello", "", true},
		{"empty-string", "", "he", false},
		{"exact", "hello", "hello", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringStartsWith([]interface{}{tt.s, tt.pre})
			if err != nil {
				t.Fatalf("starts_with(%q, %q) error = %v", tt.s, tt.pre, err)
			}
			if result != tt.want {
				t.Errorf("starts_with(%q, %q) = %v, want %v", tt.s, tt.pre, result, tt.want)
			}
		})
	}
}

// TestEndsWith tests the ends_with function
func TestEndsWith(t *testing.T) {
	tests := []struct {
		name   string
		s, suf string
		want   bool
	}{
		{"match", "hello", "lo", true},
		{"no-match", "hello", "he", false},
		{"empty-suffix", "hello", "", true},
		{"empty-string", "", "lo", false},
		{"exact", "hello", "hello", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringEndsWith([]interface{}{tt.s, tt.suf})
			if err != nil {
				t.Fatalf("ends_with(%q, %q) error = %v", tt.s, tt.suf, err)
			}
			if result != tt.want {
				t.Errorf("ends_with(%q, %q) = %v, want %v", tt.s, tt.suf, result, tt.want)
			}
		})
	}
}

// TestContains tests the contains function
func TestContains(t *testing.T) {
	tests := []struct {
		name   string
		s, sub string
		want   bool
	}{
		{"match", "hello world", "world", true},
		{"no-match", "hello world", "xyz", false},
		{"empty-sub", "hello", "", true},
		{"empty-string", "", "hello", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringContains([]interface{}{tt.s, tt.sub})
			if err != nil {
				t.Fatalf("contains(%q, %q) error = %v", tt.s, tt.sub, err)
			}
			if result != tt.want {
				t.Errorf("contains(%q, %q) = %v, want %v", tt.s, tt.sub, result, tt.want)
			}
		})
	}
}

// TestFormat tests the format/printf function
func TestFormat(t *testing.T) {
	tests := []struct {
		name string
		args []interface{}
		want string
	}{
		{"string-arg", []interface{}{"Hello %s", "world"}, "Hello world"},
		{"int-arg", []interface{}{"Count: %d", 42}, "Count: 42"},
		{"no-args", []interface{}{"plain"}, "plain"},
		{"multi-args", []interface{}{"%s is %d", "age", 30}, "age is 30"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringFormat(tt.args)
			if err != nil {
				t.Fatalf("format(%v) error = %v", tt.args, err)
			}
			if result != tt.want {
				t.Errorf("format(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

// TestUrlEncode tests the url_encode function
func TestUrlEncode(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want string
	}{
		{"space", "hello world", "hello+world"},
		{"special", "a=1&b=2", "a%3D1%26b%3D2"},
		{"empty", "", ""},
		{"no-encoding", "hello", "hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringURLEncode([]interface{}{tt.arg})
			if err != nil {
				t.Fatalf("url_encode(%q) error = %v", tt.arg, err)
			}
			if result != tt.want {
				t.Errorf("url_encode(%q) = %v, want %v", tt.arg, result, tt.want)
			}
		})
	}
}

// TestUrlDecode tests the url_decode function
func TestUrlDecode(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		want    string
		wantErr bool
	}{
		{"space", "hello+world", "hello world", false},
		{"special", "a%3D1%26b%3D2", "a=1&b=2", false},
		{"empty", "", "", false},
		{"no-decoding", "hello", "hello", false},
		{"invalid", "%zz", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringURLDecode([]interface{}{tt.arg})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("url_decode(%q) expected error, got nil", tt.arg)
				}
				return
			}
			if err != nil {
				t.Fatalf("url_decode(%q) error = %v", tt.arg, err)
			}
			if result != tt.want {
				t.Errorf("url_decode(%q) = %v, want %v", tt.arg, result, tt.want)
			}
		})
	}
}
