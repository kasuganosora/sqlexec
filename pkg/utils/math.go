package utils

// Min returns the smaller of two values
func Min[T Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// Max returns the larger of two values
func Max[T Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// Abs returns the absolute value
func Abs[T Signed](a T) T {
	if a < 0 {
		return -a
	}
	return a
}

// AbsFloat returns the absolute value of a float64
func AbsFloat(a float64) float64 {
	if a < 0 {
		return -a
	}
	return a
}

// Clamp constrains a value to be within a range [min, max]
func Clamp[T Ordered](value, minVal, maxVal T) T {
	if value < minVal {
		return minVal
	}
	if value > maxVal {
		return maxVal
	}
	return value
}

// MinInt returns the smaller of two int values (for compatibility)
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MaxInt returns the larger of two int values (for compatibility)
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinInt64 returns the smaller of two int64 values (for compatibility)
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// MaxInt64 returns the larger of two int64 values (for compatibility)
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// AbsInt returns the absolute value of an int
func AbsInt(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

// AbsInt64 returns the absolute value of an int64
func AbsInt64(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}
