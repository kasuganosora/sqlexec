package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==========================================================================
// Bug 1 (P1): ArrayInsert doesn't update parent structure
// ArrayInsert on nested path returns only the modified array,
// not the full document with the array updated at the path.
// Compare with ArrayAppend which correctly uses setRecursive.
// ==========================================================================

func TestBug1_ArrayInsert_NestedPath(t *testing.T) {
	// Create: {"data": {"items": [1, 2, 3]}}
	bj, err := ParseJSON(`{"data": {"items": [1, 2, 3]}}`)
	require.NoError(t, err)

	// Insert 99 at position 1 in $.data.items
	result, err := ArrayInsert(bj, "$.data.items", 1, 99)
	require.NoError(t, err)

	// Result should be the full document, not just the array
	assert.True(t, result.IsObject(), "result should be an object, got %s", result.Type())

	// Verify the nested array was updated
	items, err := result.Extract("$.data.items")
	require.NoError(t, err)
	assert.True(t, items.IsArray())

	arr, err := items.GetArray()
	require.NoError(t, err)
	assert.Len(t, arr, 4)
	// arr should be [1, 99, 2, 3] — JSON integers may be float64 or int64 internally
	assert.EqualValues(t, 1, arr[0])
	assert.EqualValues(t, 99, arr[1])
	assert.EqualValues(t, 2, arr[2])
	assert.EqualValues(t, 3, arr[3])
}

func TestBug1_ArrayInsert_RootPath_StillWorks(t *testing.T) {
	// Root path should still work correctly
	bj, err := ParseJSON(`[1, 2, 3]`)
	require.NoError(t, err)

	result, err := ArrayInsert(bj, "$", 0, 99)
	require.NoError(t, err)

	assert.True(t, result.IsArray())
	arr, err := result.GetArray()
	require.NoError(t, err)
	assert.Len(t, arr, 4)
	assert.Equal(t, int64(99), arr[0])
}

// ==========================================================================
// Bug 2 (P1): setRecursive path creation loses parent context
// When creating intermediate paths that don't exist, the function
// returns only the subtree, not the full reconstructed parent object.
// ==========================================================================

func TestBug2_SetRecursive_CreateNestedPath(t *testing.T) {
	// Start with: {"x": 1}
	bj, err := ParseJSON(`{"x": 1}`)
	require.NoError(t, err)

	// Set $.a.b = "hello" — key "a" doesn't exist, should be created
	result, err := bj.Set("$.a.b", "hello")
	require.NoError(t, err)

	// Result should be: {"x": 1, "a": {"b": "hello"}}
	assert.True(t, result.IsObject(), "result should be an object, got %s", result.Type())

	// Verify "x" is preserved
	xVal, err := result.Extract("$.x")
	require.NoError(t, err)
	xInt, err := xVal.GetInt64()
	require.NoError(t, err)
	assert.Equal(t, int64(1), xInt)

	// Verify "a.b" was created
	bVal, err := result.Extract("$.a.b")
	require.NoError(t, err)
	bStr, err := bVal.GetString()
	require.NoError(t, err)
	assert.Equal(t, "hello", bStr)
}

func TestBug2_SetRecursive_CreateDeeplyNestedPath(t *testing.T) {
	bj, err := ParseJSON(`{}`)
	require.NoError(t, err)

	result, err := bj.Set("$.a.b.c", 42)
	require.NoError(t, err)

	assert.True(t, result.IsObject())

	val, err := result.Extract("$.a.b.c")
	require.NoError(t, err)
	intVal, err := val.GetInt64()
	require.NoError(t, err)
	assert.Equal(t, int64(42), intVal)
}

// ==========================================================================
// Bug 3 (P2): Merge unsafe type assertion
// When bj is scalar and other is array, the code uses
// other.GetInterface().([]interface{}) without safe assertion.
// ==========================================================================

func TestBug3_Merge_ScalarWithArray(t *testing.T) {
	// This tests the code path at merge.go:63
	bj, err := NewBinaryJSON("hello")
	require.NoError(t, err)

	other, err := NewBinaryJSON([]interface{}{1, 2, 3})
	require.NoError(t, err)

	// Should not panic — merge scalar with array wraps both
	result, err := bj.Merge(other.GetInterface())
	require.NoError(t, err)

	// Result should be ["hello", 1, 2, 3]
	assert.True(t, result.IsArray())
	arr, err := result.GetArray()
	require.NoError(t, err)
	assert.Len(t, arr, 4)
}
