package builtin

import (
	"encoding/hex"
	"testing"
)

func TestEncodingHex(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"string hello", []interface{}{"hello"}, "68656c6c6f", false},
		{"empty string", []interface{}{""}, "", false},
		{"int 255", []interface{}{int64(255)}, "FF", false},
		{"int 0", []interface{}{int64(0)}, "0", false},
		{"int 16", []interface{}{int(16)}, "10", false},
		{"bytes", []interface{}{[]byte{0xDE, 0xAD}}, "dead", false},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingHex(tt.args)
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
			if result.(string) != tt.want {
				t.Errorf("encodingHex(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestEncodingUnhex(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"valid hex", []interface{}{"68656c6c6f"}, "hello", false},
		{"empty", []interface{}{""}, "", false},
		{"invalid hex", []interface{}{"xyz"}, "", true},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingUnhex(tt.args)
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
			if string(result.([]byte)) != tt.want {
				t.Errorf("encodingUnhex(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestBase64(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"hello", []interface{}{"hello"}, "aGVsbG8=", false},
		{"empty", []interface{}{""}, "", false},
		{"bytes", []interface{}{[]byte("world")}, "d29ybGQ=", false},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingToBase64(tt.args)
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
			if result.(string) != tt.want {
				t.Errorf("encodingToBase64(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestFromBase64(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"hello", []interface{}{"aGVsbG8="}, "hello", false},
		{"empty", []interface{}{""}, "", false},
		{"invalid", []interface{}{"!!!invalid!!!"}, "", true},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingFromBase64(tt.args)
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
			if string(result.([]byte)) != tt.want {
				t.Errorf("encodingFromBase64(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestBin(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"10", []interface{}{int64(10)}, "1010", false},
		{"0", []interface{}{int64(0)}, "0", false},
		{"1", []interface{}{int64(1)}, "1", false},
		{"255", []interface{}{int64(255)}, "11111111", false},
		{"negative", []interface{}{int64(-1)}, "-1", false},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingBin(tt.args)
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
			if result.(string) != tt.want {
				t.Errorf("encodingBin(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestMd5(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"hello", []interface{}{"hello"}, "5d41402abc4b2a76b9719d911017c592", false},
		{"empty", []interface{}{""}, "d41d8cd98f00b204e9800998ecf8427e", false},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingMd5(tt.args)
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
			if result.(string) != tt.want {
				t.Errorf("encodingMd5(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestSha1(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"hello", []interface{}{"hello"}, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d", false},
		{"empty", []interface{}{""}, "da39a3ee5e6b4b0d3255bfef95601890afd80709", false},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingSha1(tt.args)
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
			if result.(string) != tt.want {
				t.Errorf("encodingSha1(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestSha2(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"sha256 hello", []interface{}{"hello", int64(256)}, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", false},
		{"sha512 hello", []interface{}{"hello", int64(512)}, "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043", false},
		{"unsupported bits", []interface{}{"hello", int64(384)}, "", true},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingSha2(tt.args)
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
			if result.(string) != tt.want {
				t.Errorf("encodingSha2(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestHash(t *testing.T) {
	// Hash should return a consistent int64 value for the same input
	result1, err := encodingHash([]interface{}{"hello"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result2, err := encodingHash([]interface{}{"hello"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result1.(int64) != result2.(int64) {
		t.Errorf("hash should be deterministic: %v != %v", result1, result2)
	}

	// Different inputs should (very likely) produce different hashes
	result3, err := encodingHash([]interface{}{"world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result1.(int64) == result3.(int64) {
		t.Error("hash of different strings should differ")
	}

	// Error case
	_, err = encodingHash([]interface{}{})
	if err == nil {
		t.Error("expected error for no args")
	}
}

func TestEncodingEncode(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"hex encode", []interface{}{"hello", "hex"}, "68656c6c6f", false},
		{"base64 encode", []interface{}{"hello", "base64"}, "aGVsbG8=", false},
		{"unsupported charset", []interface{}{"hello", "utf8"}, "", true},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingEncode(tt.args)
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
			if result.(string) != tt.want {
				t.Errorf("encodingEncode(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestEncodingDecode(t *testing.T) {
	tests := []struct {
		name    string
		args    []interface{}
		want    string
		wantErr bool
	}{
		{"hex decode", []interface{}{"68656c6c6f", "hex"}, "hello", false},
		{"base64 decode", []interface{}{"aGVsbG8=", "base64"}, "hello", false},
		{"unsupported charset", []interface{}{"data", "utf8"}, "", true},
		{"invalid hex", []interface{}{"xyz", "hex"}, "", true},
		{"invalid base64", []interface{}{"!!!invalid!!!", "base64"}, "", true},
		{"no args", []interface{}{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingDecode(tt.args)
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
			if string(result.([]byte)) != tt.want {
				t.Errorf("encodingDecode(%v) = %v, want %v", tt.args, result, tt.want)
			}
		})
	}
}

func TestHexRoundtrip(t *testing.T) {
	// Encode then decode should yield the original
	original := "Hello, World!"
	encoded, err := encodingHex([]interface{}{original})
	if err != nil {
		t.Fatalf("hex encode error: %v", err)
	}
	decoded, err := encodingUnhex([]interface{}{encoded})
	if err != nil {
		t.Fatalf("hex decode error: %v", err)
	}
	if string(decoded.([]byte)) != original {
		t.Errorf("roundtrip failed: got %q, want %q", decoded, original)
	}
}

func TestBase64Roundtrip(t *testing.T) {
	original := "Hello, World!"
	encoded, err := encodingToBase64([]interface{}{original})
	if err != nil {
		t.Fatalf("base64 encode error: %v", err)
	}
	decoded, err := encodingFromBase64([]interface{}{encoded})
	if err != nil {
		t.Fatalf("base64 decode error: %v", err)
	}
	if string(decoded.([]byte)) != original {
		t.Errorf("roundtrip failed: got %q, want %q", decoded, original)
	}
}

func TestEncodingHexWithVariousIntTypes(t *testing.T) {
	tests := []struct {
		name string
		arg  interface{}
		want string
	}{
		{"int32", int32(255), "FF"},
		{"uint64", uint64(256), "100"},
		{"uint", uint(10), "A"},
		{"int8", int8(15), "F"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodingHex([]interface{}{tt.arg})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.(string) != tt.want {
				t.Errorf("encodingHex(%v) = %v, want %v", tt.arg, result, tt.want)
			}
		})
	}
}

// Ensure the encoding functions are registered in the global registry.
func TestEncodingFunctionsRegistered(t *testing.T) {
	names := []string{
		"hex", "unhex", "to_base64", "base64", "from_base64",
		"bin", "md5", "sha1", "sha2", "hash", "encode", "decode",
	}

	for _, name := range names {
		fn, exists := GetGlobal(name)
		if !exists {
			t.Errorf("function %s should be registered", name)
			continue
		}
		if fn.Category != "encoding" {
			t.Errorf("function %s category = %q, want %q", name, fn.Category, "encoding")
		}
	}
}

// Verify hex encode output matches encoding/hex package directly.
func TestHexConsistency(t *testing.T) {
	input := "test data 123"
	expected := hex.EncodeToString([]byte(input))
	result, err := encodingHex([]interface{}{input})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.(string) != expected {
		t.Errorf("hex(%q) = %q, want %q", input, result, expected)
	}
}
