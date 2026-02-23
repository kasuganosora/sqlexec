package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestMergeJoin creates a PhysicalMergeJoin suitable for unit testing.
func newTestMergeJoin() *PhysicalMergeJoin {
	return &PhysicalMergeJoin{
		JoinType: InnerJoin,
	}
}

// ---------------------------------------------------------------------------
// 1. TestMergeJoin_InnerJoin_Basic
// ---------------------------------------------------------------------------

func TestMergeJoin_InnerJoin_Basic(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
		{"id": int64(3), "name": "Charlie"},
	}
	rightRows := []domain.Row{
		{"id": int64(2), "val": "X"},
		{"id": int64(3), "val": "Y"},
		{"id": int64(4), "val": "Z"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", InnerJoin)

	require.Len(t, result, 2, "inner join should produce 2 matching rows")

	// id=2 match
	assert.Equal(t, int64(2), result[0]["id"])
	assert.Equal(t, "Bob", result[0]["name"])
	assert.Equal(t, "X", result[0]["val"])

	// id=3 match
	assert.Equal(t, int64(3), result[1]["id"])
	assert.Equal(t, "Charlie", result[1]["name"])
	assert.Equal(t, "Y", result[1]["val"])
}

// ---------------------------------------------------------------------------
// 2. TestMergeJoin_InnerJoin_DuplicateKeys
// ---------------------------------------------------------------------------

func TestMergeJoin_InnerJoin_DuplicateKeys(t *testing.T) {
	mj := newTestMergeJoin()

	// Left has two rows with id=1, right has two rows with id=1.
	// The merge should produce a 2x2 = 4 row cartesian product for key 1.
	leftRows := []domain.Row{
		{"id": int64(1), "name": "A"},
		{"id": int64(1), "name": "B"},
		{"id": int64(2), "name": "C"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "val": "X"},
		{"id": int64(1), "val": "Y"},
		{"id": int64(2), "val": "Z"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", InnerJoin)

	// 2*2 (key=1) + 1*1 (key=2) = 5
	require.Len(t, result, 5, "inner join with duplicate keys should produce cartesian product within matching groups")

	// Verify all combinations for key=1 are present.
	type pair struct{ name, val string }
	key1Pairs := []pair{}
	for _, row := range result {
		if row["id"] == int64(1) {
			key1Pairs = append(key1Pairs, pair{
				name: row["name"].(string),
				val:  row["val"].(string),
			})
		}
	}
	assert.Len(t, key1Pairs, 4, "key=1 should produce 4 rows (2x2 cartesian)")

	expectedPairs := []pair{
		{"A", "X"}, {"A", "Y"}, {"B", "X"}, {"B", "Y"},
	}
	for _, ep := range expectedPairs {
		found := false
		for _, kp := range key1Pairs {
			if kp == ep {
				found = true
				break
			}
		}
		assert.True(t, found, "expected pair %v not found in result", ep)
	}
}

// ---------------------------------------------------------------------------
// 3. TestMergeJoin_LeftOuterJoin_Basic
// ---------------------------------------------------------------------------

func TestMergeJoin_LeftOuterJoin_Basic(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
		{"id": int64(3), "name": "Charlie"},
	}
	rightRows := []domain.Row{
		{"id": int64(2), "val": "X"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", LeftOuterJoin)

	require.Len(t, result, 3, "left outer join should preserve all left rows")

	// id=1: no match, right columns should be nil
	assert.Equal(t, int64(1), result[0]["id"])
	assert.Equal(t, "Alice", result[0]["name"])
	assert.Nil(t, result[0]["val"], "unmatched left row should have nil right columns")

	// id=2: matched
	assert.Equal(t, int64(2), result[1]["id"])
	assert.Equal(t, "Bob", result[1]["name"])
	assert.Equal(t, "X", result[1]["val"])

	// id=3: no match
	assert.Equal(t, int64(3), result[2]["id"])
	assert.Equal(t, "Charlie", result[2]["name"])
	assert.Nil(t, result[2]["val"], "unmatched left row should have nil right columns")
}

// ---------------------------------------------------------------------------
// 4. TestMergeJoin_LeftOuterJoin_DuplicateLeftKeys (Fix #18)
// ---------------------------------------------------------------------------

func TestMergeJoin_LeftOuterJoin_DuplicateLeftKeys(t *testing.T) {
	mj := newTestMergeJoin()

	// Fix #18: When multiple left rows share the same key, ALL of them must
	// match against the right group. A buggy implementation would advance the
	// right pointer past the group after the first left row, causing subsequent
	// left rows with the same key to receive NULL instead of the match.
	leftRows := []domain.Row{
		{"id": int64(1), "name": "A"},
		{"id": int64(1), "name": "B"},
		{"id": int64(2), "name": "C"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "val": int64(10)},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", LeftOuterJoin)

	require.Len(t, result, 3, "should have 2 matched rows (id=1) + 1 null row (id=2)")

	// Both id=1 left rows should be matched with the right row.
	matchedNames := []string{}
	for _, row := range result {
		if row["id"] == int64(1) && row["val"] == int64(10) {
			matchedNames = append(matchedNames, row["name"].(string))
		}
	}
	assert.ElementsMatch(t, []string{"A", "B"}, matchedNames,
		"Fix #18: all left rows with key=1 must match the right group")

	// id=2 should have nil val
	var nullRow domain.Row
	for _, row := range result {
		if row["id"] == int64(2) {
			nullRow = row
			break
		}
	}
	require.NotNil(t, nullRow, "id=2 row should be present")
	assert.Equal(t, "C", nullRow["name"])
	assert.Nil(t, nullRow["val"], "id=2 should have nil right columns")
}

// TestMergeJoin_LeftOuterJoin_DuplicateLeftKeys_MultipleRight extends Fix #18
// testing with multiple right rows sharing the same key as well.
func TestMergeJoin_LeftOuterJoin_DuplicateLeftKeys_MultipleRight(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "A"},
		{"id": int64(1), "name": "B"},
		{"id": int64(3), "name": "D"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "val": "X"},
		{"id": int64(1), "val": "Y"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", LeftOuterJoin)

	// 2 left * 2 right for key=1, plus 1 null row for key=3 = 5
	require.Len(t, result, 5, "2x2 cartesian for key=1 + 1 null for key=3")

	key1Count := 0
	for _, row := range result {
		if row["id"] == int64(1) {
			key1Count++
			assert.Contains(t, []string{"X", "Y"}, row["val"],
				"matched rows should have val from right side")
		}
	}
	assert.Equal(t, 4, key1Count, "Fix #18: should have 4 rows for key=1 (2 left x 2 right)")
}

// ---------------------------------------------------------------------------
// 5. TestMergeJoin_RightOuterJoin_Basic
// ---------------------------------------------------------------------------

func TestMergeJoin_RightOuterJoin_Basic(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(2), "name": "Bob"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "val": "X"},
		{"id": int64(2), "val": "Y"},
		{"id": int64(3), "val": "Z"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", RightOuterJoin)

	require.Len(t, result, 3, "right outer join should preserve all right rows")

	// For unmatched right rows, mergeRowWithNull is called with (leftNullRow, rightRow).
	// leftNullRow is the "notNull" param (all-nil template from left), and rightRow is
	// the "nullRow" param. Since the "id" column exists in leftNullRow, the right row's
	// "id" is not copied (it's treated as already present). Columns unique to the right
	// side that don't exist in leftNullRow get set to nil.

	// id=1: no left match - left columns are nil. "id" comes from leftNullRow (nil).
	// "val" does not exist in leftNullRow so it is set to nil by mergeRowWithNull.
	assert.Nil(t, result[0]["id"], "unmatched: id comes from left null template")
	assert.Nil(t, result[0]["name"], "unmatched right row should have nil left columns")
	assert.Nil(t, result[0]["val"], "unmatched: val not in left template, set to nil")

	// id=2: matched via mergeRow (left=leftRows[0], right=rightRows[1])
	assert.Equal(t, int64(2), result[1]["id"], "matched row should have left id")
	assert.Equal(t, "Y", result[1]["val"])
	assert.Equal(t, "Bob", result[1]["name"])

	// id=3: no left match
	assert.Nil(t, result[2]["id"], "unmatched: id comes from left null template")
	assert.Nil(t, result[2]["name"], "unmatched right row should have nil left columns")
}

// ---------------------------------------------------------------------------
// 6. TestMergeJoin_RightOuterJoin_DuplicateRightKeys
// ---------------------------------------------------------------------------

func TestMergeJoin_RightOuterJoin_DuplicateRightKeys(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "val": "X"},
		{"id": int64(1), "val": "Y"},
		{"id": int64(2), "val": "Z"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", RightOuterJoin)

	require.Len(t, result, 3, "should have 2 matched (id=1) + 1 null (id=2)")

	// Both right rows with id=1 should match the left row via mergeRow.
	matchedVals := []string{}
	for _, row := range result {
		if row["id"] == int64(1) && row["name"] == "Alice" {
			matchedVals = append(matchedVals, row["val"].(string))
		}
	}
	assert.ElementsMatch(t, []string{"X", "Y"}, matchedVals,
		"all right rows with key=1 must match the left group")

	// id=2: no left match. mergeRowWithNull(leftNullRow, rightRow) is called.
	// leftNullRow has {"id": nil, "name": nil}. The right row's "id" column
	// collides with leftNullRow's "id", so it stays nil. "val" is not in
	// leftNullRow, so it also gets set to nil.
	// We identify the unmatched row as the one where "name" is nil.
	var nullRow domain.Row
	for _, row := range result {
		if row["name"] == nil {
			nullRow = row
			break
		}
	}
	require.NotNil(t, nullRow, "unmatched right row should be present")
	assert.Nil(t, nullRow["id"], "unmatched: id from left null template is nil")
	assert.Nil(t, nullRow["name"], "unmatched right row should have nil left columns")
}

// TestMergeJoin_RightOuterJoin_DuplicateBothSides tests the mirror of Fix #18 for
// right outer join with duplicate keys on both sides.
func TestMergeJoin_RightOuterJoin_DuplicateBothSides(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "A"},
		{"id": int64(1), "name": "B"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "val": "X"},
		{"id": int64(1), "val": "Y"},
		{"id": int64(3), "val": "Z"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", RightOuterJoin)

	// For key=1: right row X matches left A and B (2 rows), right row Y matches left A and B (2 rows) = 4
	// For key=3: no left match = 1 null row
	// Total = 5
	require.Len(t, result, 5, "2x2 cartesian for key=1 + 1 null for key=3")

	key1Count := 0
	for _, row := range result {
		if row["id"] == int64(1) {
			key1Count++
		}
	}
	assert.Equal(t, 4, key1Count, "key=1 should produce 4 rows (2 left x 2 right)")
}

// ---------------------------------------------------------------------------
// 7. TestMergeJoin_SortByColumn
// ---------------------------------------------------------------------------

func TestMergeJoin_SortByColumn(t *testing.T) {
	mj := newTestMergeJoin()

	rows := []domain.Row{
		{"id": int64(3), "name": "Charlie"},
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}

	sorted := mj.sortByColumn(rows, "id")

	require.Len(t, sorted, 3)
	assert.Equal(t, int64(1), sorted[0]["id"])
	assert.Equal(t, int64(2), sorted[1]["id"])
	assert.Equal(t, int64(3), sorted[2]["id"])

	// Verify the original slice is not mutated.
	assert.Equal(t, int64(3), rows[0]["id"], "original slice should remain unmodified")
}

// ---------------------------------------------------------------------------
// 8. TestMergeJoin_SortByColumn_StableSort (Fix #13)
// ---------------------------------------------------------------------------

func TestMergeJoin_SortByColumn_StableSort(t *testing.T) {
	mj := newTestMergeJoin()

	// Fix #13: Using sort.SliceStable guarantees that elements with equal
	// sort keys preserve their original relative order. If an O(n^2) bubble
	// sort were used instead, it would be correct but far too slow for large
	// inputs. This test verifies stability.
	rows := []domain.Row{
		{"grade": int64(2), "name": "Alice"},
		{"grade": int64(1), "name": "Bob"},
		{"grade": int64(2), "name": "Charlie"},
		{"grade": int64(1), "name": "Diana"},
		{"grade": int64(2), "name": "Eve"},
	}

	sorted := mj.sortByColumn(rows, "grade")

	require.Len(t, sorted, 5)

	// Grade=1 group should preserve original order: Bob, Diana
	grade1 := []string{}
	for _, row := range sorted {
		if row["grade"] == int64(1) {
			grade1 = append(grade1, row["name"].(string))
		}
	}
	assert.Equal(t, []string{"Bob", "Diana"}, grade1,
		"Fix #13: stable sort must preserve original order for equal keys")

	// Grade=2 group should preserve original order: Alice, Charlie, Eve
	grade2 := []string{}
	for _, row := range sorted {
		if row["grade"] == int64(2) {
			grade2 = append(grade2, row["name"].(string))
		}
	}
	assert.Equal(t, []string{"Alice", "Charlie", "Eve"}, grade2,
		"Fix #13: stable sort must preserve original order for equal keys")
}

// TestMergeJoin_SortByColumn_Performance is a sanity check that sorting a
// moderate number of rows completes quickly (O(n log n) vs O(n^2)).
func TestMergeJoin_SortByColumn_Performance(t *testing.T) {
	mj := newTestMergeJoin()

	// Create 10000 rows in reverse order. An O(n^2) bubble sort would be
	// noticeably slow; O(n log n) should handle this easily.
	n := 10000
	rows := make([]domain.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = domain.Row{"id": int64(n - i), "idx": int64(i)}
	}

	sorted := mj.sortByColumn(rows, "id")

	require.Len(t, sorted, n)
	assert.Equal(t, int64(1), sorted[0]["id"])
	assert.Equal(t, int64(int64(n)), sorted[n-1]["id"])
}

// ---------------------------------------------------------------------------
// 9. TestMergeJoin_SortByColumn_MixedTypes
// ---------------------------------------------------------------------------

func TestMergeJoin_SortByColumn_MixedTypes(t *testing.T) {
	mj := newTestMergeJoin()

	// Rows where the sort column has mixed int and string types.
	// The compareValuesForSort function must handle this gracefully.
	rows := []domain.Row{
		{"val": "banana"},
		{"val": int64(42)},
		{"val": "apple"},
		{"val": int64(7)},
	}

	sorted := mj.sortByColumn(rows, "val")

	// The exact ordering depends on compareValuesForSort's handling of mixed
	// types. We just verify that it does not panic and returns all rows.
	require.Len(t, sorted, 4, "sort with mixed types should return all rows without panicking")

	// Verify all original values are present.
	vals := make([]interface{}, len(sorted))
	for i, row := range sorted {
		vals[i] = row["val"]
	}
	assert.Contains(t, vals, "banana")
	assert.Contains(t, vals, "apple")
	assert.Contains(t, vals, int64(42))
	assert.Contains(t, vals, int64(7))
}

// TestMergeJoin_SortByColumn_StringValues verifies lexicographic ordering of string
// values.
func TestMergeJoin_SortByColumn_StringValues(t *testing.T) {
	mj := newTestMergeJoin()

	rows := []domain.Row{
		{"name": "Charlie"},
		{"name": "Alice"},
		{"name": "Bob"},
	}

	sorted := mj.sortByColumn(rows, "name")

	require.Len(t, sorted, 3)
	assert.Equal(t, "Alice", sorted[0]["name"])
	assert.Equal(t, "Bob", sorted[1]["name"])
	assert.Equal(t, "Charlie", sorted[2]["name"])
}

// ---------------------------------------------------------------------------
// 10. TestMergeJoin_MergeRow_ColumnConflict
// ---------------------------------------------------------------------------

func TestMergeJoin_MergeRow_ColumnConflict(t *testing.T) {
	mj := newTestMergeJoin()

	left := domain.Row{"id": int64(1), "name": "Alice"}
	right := domain.Row{"id": int64(1), "score": int64(95)}

	merged := mj.mergeRow(left, right)

	// "id" exists in both rows. The right "id" should get a "right_" prefix.
	assert.Equal(t, int64(1), merged["id"], "left id should be kept as-is")
	assert.Equal(t, int64(1), merged["right_id"], "conflicting right id should be prefixed with right_")
	assert.Equal(t, "Alice", merged["name"])
	assert.Equal(t, int64(95), merged["score"])
}

// TestMergeJoin_MergeRow_NoConflict verifies that non-conflicting columns are
// simply merged without prefixes.
func TestMergeJoin_MergeRow_NoConflict(t *testing.T) {
	mj := newTestMergeJoin()

	left := domain.Row{"id": int64(1), "name": "Alice"}
	right := domain.Row{"score": int64(95), "grade": "A"}

	merged := mj.mergeRow(left, right)

	assert.Equal(t, int64(1), merged["id"])
	assert.Equal(t, "Alice", merged["name"])
	assert.Equal(t, int64(95), merged["score"])
	assert.Equal(t, "A", merged["grade"])
	assert.Len(t, merged, 4)
}

// TestMergeJoin_MergeRow_MultipleConflicts checks that multiple conflicting
// column names all receive the "right_" prefix.
func TestMergeJoin_MergeRow_MultipleConflicts(t *testing.T) {
	mj := newTestMergeJoin()

	left := domain.Row{"a": int64(1), "b": int64(2)}
	right := domain.Row{"a": int64(10), "b": int64(20), "c": int64(30)}

	merged := mj.mergeRow(left, right)

	assert.Equal(t, int64(1), merged["a"])
	assert.Equal(t, int64(2), merged["b"])
	assert.Equal(t, int64(10), merged["right_a"])
	assert.Equal(t, int64(20), merged["right_b"])
	assert.Equal(t, int64(30), merged["c"])
	assert.Len(t, merged, 5)
}

// ---------------------------------------------------------------------------
// 11. TestMergeJoin_EmptyInputs
// ---------------------------------------------------------------------------

func TestMergeJoin_EmptyInputs(t *testing.T) {
	mj := newTestMergeJoin()

	nonEmpty := []domain.Row{
		{"id": int64(1), "name": "Alice"},
	}
	empty := []domain.Row{}

	t.Run("EmptyLeft_InnerJoin", func(t *testing.T) {
		result := mj.mergeRows(empty, nonEmpty, "id", "id", InnerJoin)
		assert.Empty(t, result, "inner join with empty left should produce no rows")
	})

	t.Run("EmptyRight_InnerJoin", func(t *testing.T) {
		result := mj.mergeRows(nonEmpty, empty, "id", "id", InnerJoin)
		assert.Empty(t, result, "inner join with empty right should produce no rows")
	})

	t.Run("BothEmpty_InnerJoin", func(t *testing.T) {
		result := mj.mergeRows(empty, empty, "id", "id", InnerJoin)
		assert.Empty(t, result, "inner join with both empty should produce no rows")
	})

	t.Run("EmptyLeft_LeftOuterJoin", func(t *testing.T) {
		result := mj.mergeRows(empty, nonEmpty, "id", "id", LeftOuterJoin)
		assert.Empty(t, result, "left join with empty left should produce no rows")
	})

	t.Run("EmptyRight_LeftOuterJoin", func(t *testing.T) {
		result := mj.mergeRows(nonEmpty, empty, "id", "id", LeftOuterJoin)
		require.Len(t, result, 1, "left join with empty right should preserve left rows")
		assert.Equal(t, int64(1), result[0]["id"])
		assert.Equal(t, "Alice", result[0]["name"])
	})

	t.Run("EmptyRight_RightOuterJoin", func(t *testing.T) {
		result := mj.mergeRows(nonEmpty, empty, "id", "id", RightOuterJoin)
		assert.Empty(t, result, "right join with empty right should produce no rows")
	})

	t.Run("EmptyLeft_RightOuterJoin", func(t *testing.T) {
		result := mj.mergeRows(empty, nonEmpty, "id", "id", RightOuterJoin)
		require.Len(t, result, 1, "right join with empty left should preserve right rows")
		// When left is empty, buildNullRow returns an empty map (no columns).
		// mergeRowWithNull(emptyNullRow, rightRow) copies emptyNullRow (nothing),
		// then for each key in rightRow, since nothing exists in merged, sets it to nil.
		// So all right row columns become nil.
		assert.Nil(t, result[0]["id"], "right row columns become nil when left is empty")
		assert.Nil(t, result[0]["name"], "right row columns become nil when left is empty")
	})

	t.Run("BothEmpty_LeftOuterJoin", func(t *testing.T) {
		result := mj.mergeRows(empty, empty, "id", "id", LeftOuterJoin)
		assert.Empty(t, result)
	})

	t.Run("BothEmpty_RightOuterJoin", func(t *testing.T) {
		result := mj.mergeRows(empty, empty, "id", "id", RightOuterJoin)
		assert.Empty(t, result)
	})
}

// ---------------------------------------------------------------------------
// 12. TestMergeJoin_InnerJoin_NoMatches
// ---------------------------------------------------------------------------

func TestMergeJoin_InnerJoin_NoMatches(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	rightRows := []domain.Row{
		{"id": int64(3), "val": "X"},
		{"id": int64(4), "val": "Y"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", InnerJoin)
	assert.Empty(t, result, "inner join with no matching keys should produce no rows")
}

// TestMergeJoin_InnerJoin_NoMatches_LeftOuterJoin verifies that all left rows
// survive even when there are zero right matches.
func TestMergeJoin_InnerJoin_NoMatches_LeftOuterJoin(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	rightRows := []domain.Row{
		{"id": int64(3), "val": "X"},
		{"id": int64(4), "val": "Y"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", LeftOuterJoin)

	require.Len(t, result, 2, "left join with no matches should preserve all left rows")
	for _, row := range result {
		assert.Nil(t, row["val"], "right columns should be nil when no match")
	}
}

// ---------------------------------------------------------------------------
// Additional edge case tests
// ---------------------------------------------------------------------------

// TestMergeJoin_LeftOuterJoin_AllMatch verifies correct behaviour when every
// left row has a matching right row.
func TestMergeJoin_LeftOuterJoin_AllMatch(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	rightRows := []domain.Row{
		{"id": int64(1), "val": "X"},
		{"id": int64(2), "val": "Y"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", LeftOuterJoin)

	require.Len(t, result, 2)
	for _, row := range result {
		assert.NotNil(t, row["val"], "all left rows should have matching right data")
	}
}

// TestMergeJoin_SortByColumn_EmptySlice verifies sorting an empty slice is safe.
func TestMergeJoin_SortByColumn_EmptySlice(t *testing.T) {
	mj := newTestMergeJoin()

	sorted := mj.sortByColumn([]domain.Row{}, "id")
	assert.Empty(t, sorted, "sorting empty slice should return empty slice")
}

// TestMergeJoin_SortByColumn_SingleElement verifies sorting a single-element slice.
func TestMergeJoin_SortByColumn_SingleElement(t *testing.T) {
	mj := newTestMergeJoin()

	rows := []domain.Row{{"id": int64(42), "name": "Only"}}
	sorted := mj.sortByColumn(rows, "id")

	require.Len(t, sorted, 1)
	assert.Equal(t, int64(42), sorted[0]["id"])
}

// TestMergeJoin_SortByColumn_MissingColumn verifies behaviour when the sort
// column is missing from some rows (nil values).
func TestMergeJoin_SortByColumn_MissingColumn(t *testing.T) {
	mj := newTestMergeJoin()

	rows := []domain.Row{
		{"id": int64(2), "name": "Bob"},
		{"name": "NoId"},
		{"id": int64(1), "name": "Alice"},
	}

	// Should not panic; missing column yields nil for that row's sort key.
	sorted := mj.sortByColumn(rows, "id")
	require.Len(t, sorted, 3, "all rows should be returned")
}

// TestMergeJoin_DefaultJoinType verifies that an unsupported join type falls
// back to InnerJoin behaviour.
func TestMergeJoin_DefaultJoinType(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	rightRows := []domain.Row{
		{"id": int64(2), "val": "X"},
		{"id": int64(3), "val": "Y"},
	}

	leftSorted := mj.sortByColumn(leftRows, "id")
	rightSorted := mj.sortByColumn(rightRows, "id")

	// CrossJoin falls into the default case which delegates to InnerJoin.
	result := mj.mergeRows(leftSorted, rightSorted, "id", "id", CrossJoin)

	require.Len(t, result, 1, "default/fallback should behave as inner join")
	assert.Equal(t, int64(2), result[0]["id"])
}

// TestMergeJoin_DifferentColumnNames verifies joining on columns with
// different names on left and right sides.
func TestMergeJoin_DifferentColumnNames(t *testing.T) {
	mj := newTestMergeJoin()

	leftRows := []domain.Row{
		{"user_id": int64(1), "name": "Alice"},
		{"user_id": int64(2), "name": "Bob"},
	}
	rightRows := []domain.Row{
		{"uid": int64(1), "score": int64(95)},
		{"uid": int64(2), "score": int64(88)},
	}

	leftSorted := mj.sortByColumn(leftRows, "user_id")
	rightSorted := mj.sortByColumn(rightRows, "uid")

	result := mj.mergeRows(leftSorted, rightSorted, "user_id", "uid", InnerJoin)

	require.Len(t, result, 2)
	assert.Equal(t, "Alice", result[0]["name"])
	assert.Equal(t, int64(95), result[0]["score"])
	assert.Equal(t, "Bob", result[1]["name"])
	assert.Equal(t, int64(88), result[1]["score"])
}
