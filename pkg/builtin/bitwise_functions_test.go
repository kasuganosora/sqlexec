package builtin

import (
	"testing"
)

func TestBitCount(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"zero", []interface{}{int64(0)}, 0, false},
		{"one", []interface{}{int64(1)}, 1, false},
		{"seven", []interface{}{int64(7)}, 3, false},
		{"255", []interface{}{int64(255)}, 8, false},
		{"power of 2", []interface{}{int64(1024)}, 1, false},
		{"no args", []interface{}{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseBitCount(tt.args)
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
			if result.(int64) != tt.want {
				t.Errorf("bitwiseBitCount(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestGetBit(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"5 bit 0", []interface{}{int64(5), int64(0)}, 1, false},
		{"5 bit 1", []interface{}{int64(5), int64(1)}, 0, false},
		{"5 bit 2", []interface{}{int64(5), int64(2)}, 1, false},
		{"0 bit 0", []interface{}{int64(0), int64(0)}, 0, false},
		{"8 bit 3", []interface{}{int64(8), int64(3)}, 1, false},
		{"negative pos", []interface{}{int64(5), int64(-1)}, 0, true},
		{"pos too large", []interface{}{int64(5), int64(64)}, 0, true},
		{"no args", []interface{}{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseGetBit(tt.args)
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
			if result.(int64) != tt.want {
				t.Errorf("bitwiseGetBit(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestSetBit(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"set bit 1 of 5", []interface{}{int64(5), int64(1), int64(1)}, 7, false},
		{"clear bit 0 of 5", []interface{}{int64(5), int64(0), int64(0)}, 4, false},
		{"set bit 0 of 0", []interface{}{int64(0), int64(0), int64(1)}, 1, false},
		{"clear bit 0 of 0", []interface{}{int64(0), int64(0), int64(0)}, 0, false},
		{"set already set bit", []interface{}{int64(5), int64(2), int64(1)}, 5, false},
		{"invalid val", []interface{}{int64(5), int64(0), int64(2)}, 0, true},
		{"negative pos", []interface{}{int64(5), int64(-1), int64(1)}, 0, true},
		{"pos too large", []interface{}{int64(5), int64(64), int64(1)}, 0, true},
		{"no args", []interface{}{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseSetBit(tt.args)
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
			if result.(int64) != tt.want {
				t.Errorf("bitwiseSetBit(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestBitLength(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    int64
		wantErr bool
	}{
		{"hello", []interface{}{"hello"}, 40, false},
		{"empty", []interface{}{""}, 0, false},
		{"single char", []interface{}{"a"}, 8, false},
		{"number as string", []interface{}{123}, 24, false}, // "123" is 3 bytes
		{"no args", []interface{}{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bitwiseBitLength(tt.args)
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
			if result.(int64) != tt.want {
				t.Errorf("bitwiseBitLength(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

// Test set then get roundtrip
func TestBitSetGetRoundtrip(t *testing.T) {
	// Start with 0, set bit 3 to 1
	result, err := bitwiseSetBit([]interface{}{int64(0), int64(3), int64(1)})
	if err != nil {
		t.Fatalf("set_bit error: %v", err)
	}
	// Should be 8
	if result.(int64) != 8 {
		t.Fatalf("set_bit(0, 3, 1) = %v, want 8", result)
	}
	// Get bit 3 of result
	bit, err := bitwiseGetBit([]interface{}{result, int64(3)})
	if err != nil {
		t.Fatalf("get_bit error: %v", err)
	}
	if bit.(int64) != 1 {
		t.Errorf("get_bit(8, 3) = %v, want 1", bit)
	}
}

// Ensure bitwise functions are registered in the global registry.
func TestBitwiseFunctionsRegistered(t *testing.T) {
	names := []string{"bit_count", "get_bit", "set_bit", "bit_length"}

	for _, name := range names {
		fn, exists := GetGlobal(name)
		if !exists {
			t.Errorf("function %s should be registered", name)
			continue
		}
		if fn.Category != "bitwise" {
			t.Errorf("function %s category = %q, want %q", name, fn.Category, "bitwise")
		}
	}
}

func TestBitCountAllOnes(t *testing.T) {
	// All 64 bits set
	result, err := bitwiseBitCount([]interface{}{int64(-1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.(int64) != 64 {
		t.Errorf("bit_count(-1) = %v, want 64", result)
	}
}
