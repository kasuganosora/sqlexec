package utils

import "testing"

func TestMin(t *testing.T) {
	tests := []struct {
		a, b, expected int
	}{
		{1, 2, 1},
		{2, 1, 1},
		{5, 5, 5},
		{-1, 1, -1},
		{0, 0, 0},
	}

	for _, tt := range tests {
		result := Min(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("Min(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		a, b, expected int
	}{
		{1, 2, 2},
		{2, 1, 2},
		{5, 5, 5},
		{-1, 1, 1},
		{0, 0, 0},
	}

	for _, tt := range tests {
		result := Max(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("Max(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestAbs(t *testing.T) {
	tests := []struct {
		input, expected int
	}{
		{1, 1},
		{-1, 1},
		{0, 0},
		{-100, 100},
		{100, 100},
	}

	for _, tt := range tests {
		result := Abs(tt.input)
		if result != tt.expected {
			t.Errorf("Abs(%d) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestClamp(t *testing.T) {
	tests := []struct {
		value, minVal, maxVal, expected int
	}{
		{5, 0, 10, 5},
		{-5, 0, 10, 0},
		{15, 0, 10, 10},
		{5, 5, 5, 5},
	}

	for _, tt := range tests {
		result := Clamp(tt.value, tt.minVal, tt.maxVal)
		if result != tt.expected {
			t.Errorf("Clamp(%d, %d, %d) = %d, want %d", tt.value, tt.minVal, tt.maxVal, result, tt.expected)
		}
	}
}

func TestMinInt64(t *testing.T) {
	tests := []struct {
		a, b, expected int64
	}{
		{1, 2, 1},
		{2, 1, 1},
		{-1, 1, -1},
	}

	for _, tt := range tests {
		result := MinInt64(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("MinInt64(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestMaxInt64(t *testing.T) {
	tests := []struct {
		a, b, expected int64
	}{
		{1, 2, 2},
		{2, 1, 2},
		{-1, 1, 1},
	}

	for _, tt := range tests {
		result := MaxInt64(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("MaxInt64(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestAbsFloat(t *testing.T) {
	tests := []struct {
		input, expected float64
	}{
		{1.5, 1.5},
		{-1.5, 1.5},
		{0, 0},
		{-0.001, 0.001},
	}

	for _, tt := range tests {
		result := AbsFloat(tt.input)
		if result != tt.expected {
			t.Errorf("AbsFloat(%f) = %f, want %f", tt.input, result, tt.expected)
		}
	}
}
