package utils

import "testing"

func TestParseInt(t *testing.T) {
	tests := []struct {
		s        string
		def      int
		expected int
	}{
		{"123", 0, 123},
		{"abc", 0, 0},
		{"", 10, 10},
		{"-5", 0, -5},
		{"0", 99, 0},
	}

	for _, tt := range tests {
		result := ParseInt(tt.s, tt.def)
		if result != tt.expected {
			t.Errorf("ParseInt(%q, %d) = %d, want %d", tt.s, tt.def, result, tt.expected)
		}
	}
}

func TestParseInt64(t *testing.T) {
	tests := []struct {
		s        string
		def      int64
		expected int64
	}{
		{"123", 0, 123},
		{"abc", 99, 99},
		{"", 10, 10},
		{"-5", 0, -5},
	}

	for _, tt := range tests {
		result := ParseInt64(tt.s, tt.def)
		if result != tt.expected {
			t.Errorf("ParseInt64(%q, %d) = %d, want %d", tt.s, tt.def, result, tt.expected)
		}
	}
}

func TestParseFloat64(t *testing.T) {
	tests := []struct {
		s        string
		def      float64
		expected float64
	}{
		{"3.14", 0, 3.14},
		{"abc", 1.0, 1.0},
		{"", 2.5, 2.5},
		{"-5.5", 0, -5.5},
	}

	for _, tt := range tests {
		result := ParseFloat64(tt.s, tt.def)
		if result != tt.expected {
			t.Errorf("ParseFloat64(%q, %v) = %v, want %v", tt.s, tt.def, result, tt.expected)
		}
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		s        string
		def      bool
		expected bool
	}{
		{"true", false, true},
		{"false", true, false},
		{"1", false, true},
		{"0", true, false},
		{"invalid", true, true},
		{"", false, false},
	}

	for _, tt := range tests {
		result := ParseBool(tt.s, tt.def)
		if result != tt.expected {
			t.Errorf("ParseBool(%q, %v) = %v, want %v", tt.s, tt.def, result, tt.expected)
		}
	}
}

func TestParseIntStrict(t *testing.T) {
	val, err := ParseIntStrict("123")
	if err != nil || val != 123 {
		t.Errorf("ParseIntStrict(\"123\") = (%d, %v), want (123, nil)", val, err)
	}

	_, err = ParseIntStrict("abc")
	if err == nil {
		t.Error("ParseIntStrict(\"abc\") should return error")
	}
}

func TestParseBoolStrict(t *testing.T) {
	val, err := ParseBoolStrict("true")
	if err != nil || val != true {
		t.Errorf("ParseBoolStrict(\"true\") = (%v, %v), want (true, nil)", val, err)
	}

	_, err = ParseBoolStrict("invalid")
	if err == nil {
		t.Error("ParseBoolStrict(\"invalid\") should return error")
	}
}
