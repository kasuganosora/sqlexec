package util

import (
	"testing"
)

// TestStartsWith 测试StartsWith函数
func TestStartsWith(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		prefix   string
		expected bool
	}{
		{"match", "hello world", "hello", true},
		{"no match", "hello world", "world", false},
		{"full string", "hello", "hello", true},
		{"empty prefix", "hello", "", true},
		{"empty string", "", "hello", false},
		{"both empty", "", "", true},
		{"prefix longer than string", "hi", "hello", false},
		{"case sensitive", "Hello", "hello", false},
		{"partial match", "hello world", "hell", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StartsWith(tt.s, tt.prefix)
			if result != tt.expected {
				t.Errorf("StartsWith(%q, %q) = %v, expected %v", tt.s, tt.prefix, result, tt.expected)
			}
		})
	}
}

// TestEndsWith 测试EndsWith函数
func TestEndsWith(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		suffix   string
		expected bool
	}{
		{"match", "hello world", "world", true},
		{"no match", "hello world", "hello", false},
		{"full string", "hello", "hello", true},
		{"empty suffix", "hello", "", true},
		{"empty string", "", "hello", false},
		{"both empty", "", "", true},
		{"suffix longer than string", "hi", "hello", false},
		{"case sensitive", "hello", "Hello", false},
		{"partial match", "hello world", "orld", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EndsWith(tt.s, tt.suffix)
			if result != tt.expected {
				t.Errorf("EndsWith(%q, %q) = %v, expected %v", tt.s, tt.suffix, result, tt.expected)
			}
		})
	}
}

// TestContainsSimple 测试ContainsSimple函数
func TestContainsSimple(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected bool
	}{
		{"contains", "hello world", "world", true},
		{"not contains", "hello world", "goodbye", false},
		{"empty substring", "hello", "", true},
		{"full match", "hello", "hello", true},
		{"empty string", "", "hello", false},
		{"both empty", "", "", true},
		{"substring longer", "hi", "hello", false},
		{"case sensitive", "Hello World", "hello", false},
		{"multiple occurrences", "hello hello hello", "hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsSimple(tt.s, tt.substr)
			if result != tt.expected {
				t.Errorf("ContainsSimple(%q, %q) = %v, expected %v", tt.s, tt.substr, result, tt.expected)
			}
		})
	}
}

// TestFindSubstring 测试FindSubstring函数
func TestFindSubstring(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected int
	}{
		{"found", "hello world", "world", 6},
		{"not found", "hello world", "goodbye", -1},
		{"at start", "hello world", "hello", 0},
		{"at end", "hello world", "world", 6},
		{"empty substring", "hello", "", 0},
		{"empty string", "", "hello", -1},
		{"both empty", "", "", 0},
		{"substring longer", "hi", "hello", -1},
		{"multiple occurrences", "hello hello hello", "hello", 0},
		{"middle", "hello world", "lo wo", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FindSubstring(tt.s, tt.substr)
			if result != tt.expected {
				t.Errorf("FindSubstring(%q, %q) = %v, expected %v", tt.s, tt.substr, result, tt.expected)
			}
		})
	}
}

// TestContains 测试Contains函数
func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		substr   string
		expected bool
	}{
		{"exact match", "hello", "hello", true},
		{"wildcard start", "hello", "*llo", true},
		{"wildcard end", "hello", "hel*", true},
		{"wildcard both", "hello", "*ell*", true},
		{"wildcard only", "hello", "*", true},
		{"percent wildcard", "hello", "%", true},
		{"no match", "hello", "world", false},
		{"empty pattern", "hello", "", true}, // empty pattern matches any string
		{"empty string", "", "hello", false},
		{"both empty", "", "", true}, // empty string matches empty string
		{"wildcard with percent", "hello", "h%o", false},
		{"complex pattern", "helloworld", "*wo*", true},
		{"pattern not found", "hello", "*x*", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Contains(tt.s, tt.substr)
			if result != tt.expected {
				t.Errorf("Contains(%q, %q) = %v, expected %v", tt.s, tt.substr, result, tt.expected)
			}
		})
	}
}

// TestReplaceAll 测试ReplaceAll函数
func TestReplaceAll(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		old      string
		new      string
		expected string
	}{
		{"simple replace", "hello world", "world", "there", "hello there"},
		{"multiple occurrences", "hello hello hello", "hello", "hi", "hi hi hi"},
		{"no occurrence", "hello world", "goodbye", "hi", "hello world"},
		{"empty old", "hello", "", "hi", "hello"}, // empty old returns original string
		{"empty new", "hello world", "world", "", "hello "},
		{"empty string", "", "x", "y", ""},
		{"replace with empty", "hello", "hello", "", ""},
		{"case sensitive", "Hello hello", "hello", "hi", "Hello hi"},
		{"partial replace", "hello world", "lo", "LO", "helLO world"}, // ReplaceAll only replaces exact match "lo", not "rl"
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ReplaceAll(tt.s, tt.old, tt.new)
			if result != tt.expected {
				t.Errorf("ReplaceAll(%q, %q, %q) = %q, expected %q", tt.s, tt.old, tt.new, result, tt.expected)
			}
		})
	}
}

// TestContainsTable 测试ContainsTable函数
func TestContainsTable(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		table    string
		expected bool
	}{
		{"exact match", "SELECT * FROM users", "users", true},
		{"no match", "SELECT * FROM products", "users", false},
		{"contains word", "SELECT * FROM users WHERE id = 1", "users", true},
		{"different case", "SELECT * FROM Users", "users", true},
		{"partial match should not work", "SELECT * FROM users_list", "users", false},
		{"empty query", "", "users", false},
		{"empty table", "SELECT * FROM users", "", false},
		{"both empty", "", "", false},
		{"table in JOIN", "SELECT * FROM products JOIN users ON", "users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsTable(tt.query, tt.table)
			if result != tt.expected {
				t.Errorf("ContainsTable(%q, %q) = %v, expected %v", tt.query, tt.table, result, tt.expected)
			}
		})
	}
}

// TestContainsWord 测试ContainsWord函数
func TestContainsWord(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		word     string
		expected bool
	}{
		{"word at start", "users and products", "users", true},
		{"word at end", "products and users", "users", true},
		{"word in middle", "products users items", "users", true},
		{"word not present", "products and items", "users", false},
		{"partial match should not work", "users_list", "users", false},
		{"case insensitive", "USERS and products", "users", true},
		{"empty string", "", "users", false},
		{"empty word", "products and users", "", false},
		{"both empty", "", "", false},
		{"word with underscore", "users_items", "users", false},
		// ContainsWord uses various separators for word boundary detection
		{"word after comma matched", "products,users,items", "users", true},     // comma is a separator
		{"word after semicolon matched", "products;users;items", "users", true}, // semicolon is a separator
		{"word in parentheses matched", "(users)", "users", true},               // parentheses are separators
		{"newline boundaries matched", "products\nusers", "users", true},        // newline is a separator
		{"newline before word", "products\n users", "users", true},
		{"newline after word", "products \nusers", "users", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsWord(tt.str, tt.word)
			if result != tt.expected {
				t.Errorf("ContainsWord(%q, %q) = %v, expected %v", tt.str, tt.word, result, tt.expected)
			}
		})
	}
}

// TestJoinWith 测试JoinWith函数
func TestJoinWith(t *testing.T) {
	tests := []struct {
		name     string
		strs     []string
		sep      string
		expected string
	}{
		{"comma separated", []string{"a", "b", "c"}, ",", "a,b,c"},
		{"space separated", []string{"hello", "world"}, " ", "hello world"},
		{"empty separator", []string{"a", "b", "c"}, "", "abc"},
		{"empty slice", []string{}, ",", ""},
		{"single element", []string{"hello"}, ",", "hello"},
		{"empty strings", []string{"", "", ""}, ",", ",,"},
		{"multi-char separator", []string{"a", "b", "c"}, "||", "a||b||c"},
		{"newlines", []string{"a", "b", "c"}, "\n", "a\nb\nc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := JoinWith(tt.strs, tt.sep)
			if result != tt.expected {
				t.Errorf("JoinWith(%v, %q) = %q, expected %q", tt.strs, tt.sep, result, tt.expected)
			}
		})
	}
}
