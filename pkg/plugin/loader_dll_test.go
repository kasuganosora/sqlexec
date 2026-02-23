//go:build windows

package plugin

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestCStringToGoString(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"empty string", []byte{0}, ""},
		{"hello", []byte{'h', 'e', 'l', 'l', 'o', 0}, "hello"},
		{"single char", []byte{'A', 0}, "A"},
		{"with spaces", []byte{'a', ' ', 'b', 0}, "a b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ptr := uintptr(unsafe.Pointer(&tt.input[0]))
			result := cStringToGoString(ptr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCStringToGoString_NullPtr(t *testing.T) {
	result := cStringToGoString(0)
	assert.Equal(t, "", result)
}
