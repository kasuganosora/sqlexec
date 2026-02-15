package builtin

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Bug #1: LPAD/RPAD with negative length causes panic
// LPAD('hello', -1, 'x') should return empty string (MySQL returns NULL or '')
// ============================================================

func TestLPad_NegativeLength(t *testing.T) {
	result, err := stringLPad([]interface{}{"hello", int64(-1), "x"})
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestLPad_ZeroLength(t *testing.T) {
	result, err := stringLPad([]interface{}{"hello", int64(0), "x"})
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestRPad_NegativeLength(t *testing.T) {
	result, err := stringRPad([]interface{}{"hello", int64(-1), "x"})
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestRPad_ZeroLength(t *testing.T) {
	result, err := stringRPad([]interface{}{"hello", int64(0), "x"})
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

// ============================================================
// Bug #2: MOD(a, 0) returns NaN instead of NULL
// MySQL: SELECT MOD(10, 0) → NULL
// ============================================================

func TestMod_DivisionByZero(t *testing.T) {
	result, err := mathMod([]interface{}{int64(10), int64(0)})
	require.NoError(t, err)
	assert.Nil(t, result, "MOD(10, 0) should return NULL")
}

func TestMod_DivisionByZeroFloat(t *testing.T) {
	result, err := mathMod([]interface{}{10.5, 0.0})
	require.NoError(t, err)
	assert.Nil(t, result, "MOD(10.5, 0.0) should return NULL")
}

func TestMod_Normal(t *testing.T) {
	result, err := mathMod([]interface{}{int64(10), int64(3)})
	require.NoError(t, err)
	assert.Equal(t, 1.0, result)
}

// ============================================================
// Bug #3: REPEAT / SPACE with extremely large count → OOM
// Should cap or return error for unreasonable counts
// ============================================================

func TestRepeat_HugeCount(t *testing.T) {
	// Should not panic or OOM; should return error or empty
	result, err := stringRepeat([]interface{}{"a", int64(1 << 30)}) // 1GB
	if err != nil {
		// Error is acceptable
		return
	}
	// If no error, result should be capped
	str, ok := result.(string)
	assert.True(t, ok)
	assert.LessOrEqual(t, len(str), 10*1024*1024, "REPEAT should cap output size")
}

func TestSpace_HugeCount(t *testing.T) {
	result, err := stringSpace([]interface{}{int64(1 << 30)})
	if err != nil {
		return
	}
	str, ok := result.(string)
	assert.True(t, ok)
	assert.LessOrEqual(t, len(str), 10*1024*1024, "SPACE should cap output size")
}

func TestRepeat_Normal(t *testing.T) {
	result, err := stringRepeat([]interface{}{"ab", int64(3)})
	require.NoError(t, err)
	assert.Equal(t, "ababab", result)
}

// ============================================================
// Bug #4: SUBSTRING with negative start
// MySQL: SUBSTRING('hello', -2) → 'lo' (counts from end)
// MySQL: SUBSTRING('hello', 0) → '' (empty)
// ============================================================

func TestSubstring_NegativeStart(t *testing.T) {
	// MySQL: SUBSTRING('hello', -2) → 'lo'
	result, err := stringSubstring([]interface{}{"hello", int64(-2)})
	require.NoError(t, err)
	assert.Equal(t, "lo", result)
}

func TestSubstring_NegativeStartWithLength(t *testing.T) {
	// MySQL: SUBSTRING('hello', -3, 2) → 'll'
	result, err := stringSubstring([]interface{}{"hello", int64(-3), int64(2)})
	require.NoError(t, err)
	assert.Equal(t, "ll", result)
}

func TestSubstring_ZeroStart(t *testing.T) {
	// MySQL: SUBSTRING('hello', 0) → ''
	result, err := stringSubstring([]interface{}{"hello", int64(0)})
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestSubstring_NormalStart(t *testing.T) {
	// MySQL: SUBSTRING('hello', 2) → 'ello'
	result, err := stringSubstring([]interface{}{"hello", int64(2)})
	require.NoError(t, err)
	assert.Equal(t, "ello", result)
}

func TestSubstring_NormalStartWithLength(t *testing.T) {
	// MySQL: SUBSTRING('hello', 2, 3) → 'ell'
	result, err := stringSubstring([]interface{}{"hello", int64(2), int64(3)})
	require.NoError(t, err)
	assert.Equal(t, "ell", result)
}

// ============================================================
// Bug #5: NULLIF uses fmt.Sprintf for comparison
// Should use proper numeric-aware comparison
// ============================================================

func TestNullIf_SameInts(t *testing.T) {
	result, err := controlNullIf([]interface{}{int64(1), int64(1)})
	require.NoError(t, err)
	assert.Nil(t, result, "NULLIF(1, 1) should return NULL")
}

func TestNullIf_DifferentInts(t *testing.T) {
	result, err := controlNullIf([]interface{}{int64(1), int64(2)})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result)
}

func TestNullIf_IntAndFloat_SameValue(t *testing.T) {
	// MySQL: NULLIF(1, 1.0) → NULL
	result, err := controlNullIf([]interface{}{int64(1), float64(1.0)})
	require.NoError(t, err)
	assert.Nil(t, result, "NULLIF(1, 1.0) should return NULL")
}

func TestNullIf_BothNil(t *testing.T) {
	result, err := controlNullIf([]interface{}{nil, nil})
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestNullIf_OneNil(t *testing.T) {
	result, err := controlNullIf([]interface{}{int64(1), nil})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result)
}

// ============================================================
// Bug #6: TRUNCATE overflow for large values
// TRUNCATE(1e18, 2) causes int64 overflow
// ============================================================

func TestTruncate_LargeValue(t *testing.T) {
	// TRUNCATE(1e15, 2) should not overflow
	result, err := mathTruncate([]interface{}{1e15, int64(2)})
	require.NoError(t, err)
	val, ok := result.(float64)
	require.True(t, ok)
	assert.False(t, math.IsInf(val, 0), "TRUNCATE should not return Inf")
	assert.False(t, math.IsNaN(val), "TRUNCATE should not return NaN")
}

func TestTruncate_NegativeDecimals(t *testing.T) {
	// MySQL: TRUNCATE(12345, -2) → 12300
	result, err := mathTruncate([]interface{}{float64(12345), int64(-2)})
	require.NoError(t, err)
	assert.Equal(t, float64(12300), result)
}

func TestTruncate_Normal(t *testing.T) {
	result, err := mathTruncate([]interface{}{1.999, int64(1)})
	require.NoError(t, err)
	assert.Equal(t, 1.9, result)
}

// ============================================================
// Additional edge case tests
// ============================================================

func TestLeft_NilArg(t *testing.T) {
	result, err := stringLeft([]interface{}{nil, int64(3)})
	require.NoError(t, err)
	assert.Equal(t, "", result) // nil → empty string via toString
}

func TestRight_NilArg(t *testing.T) {
	result, err := stringRight([]interface{}{nil, int64(3)})
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestLPad_NormalPadding(t *testing.T) {
	result, err := stringLPad([]interface{}{"hi", int64(5), "xy"})
	require.NoError(t, err)
	assert.Equal(t, "xyxhi", result)
}

func TestRPad_NormalPadding(t *testing.T) {
	result, err := stringRPad([]interface{}{"hi", int64(5), "xy"})
	require.NoError(t, err)
	assert.Equal(t, "hixyx", result)
}

func TestLPad_TruncateWhenLonger(t *testing.T) {
	// LPAD('hello', 3, 'x') → 'hel' (truncate to length)
	result, err := stringLPad([]interface{}{"hello", int64(3), "x"})
	require.NoError(t, err)
	assert.Equal(t, "hel", result)
}

func TestRPad_TruncateWhenLonger(t *testing.T) {
	// RPAD('hello', 3, 'x') → 'hel'
	result, err := stringRPad([]interface{}{"hello", int64(3), "x"})
	require.NoError(t, err)
	assert.Equal(t, "hel", result)
}
