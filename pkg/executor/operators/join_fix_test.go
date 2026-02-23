package operators

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeTestJoinOp creates a HashJoinOperator with a single join condition on
// the given left/right column names.
func makeTestJoinOp(joinType types.JoinType, leftResult, rightResult *domain.QueryResult, leftCol, rightCol string) *HashJoinOperator {
	return &HashJoinOperator{
		BaseOperator: &BaseOperator{
			children: []Operator{
				&mockChildOperator{result: leftResult},
				&mockChildOperator{result: rightResult},
			},
		},
		config: &plan.HashJoinConfig{
			JoinType: joinType,
			LeftCond: &types.JoinCondition{
				Left: &types.Expression{Type: "column", Column: leftCol},
			},
			RightCond: &types.JoinCondition{
				Left: &types.Expression{Type: "column", Column: rightCol},
			},
		},
	}
}

// Standard test data: users (left) and orders (right).
func usersResult() *domain.QueryResult {
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "TEXT"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Charlie"},
		},
	}
}

func ordersResult() *domain.QueryResult {
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "user_id", Type: "INT"},
			{Name: "amount", Type: "FLOAT"},
		},
		Rows: []domain.Row{
			{"user_id": int64(1), "amount": float64(100)},
			{"user_id": int64(1), "amount": float64(200)},
			{"user_id": int64(3), "amount": float64(300)},
			{"user_id": int64(4), "amount": float64(400)},
		},
	}
}

// emptyResult returns a QueryResult with columns but zero rows.
func emptyResult(cols []domain.ColumnInfo) *domain.QueryResult {
	return &domain.QueryResult{
		Columns: cols,
		Rows:    []domain.Row{},
	}
}

// ---------------------------------------------------------------------------
// Fix #1 -- All JOIN types work
// ---------------------------------------------------------------------------

func TestHashJoin_InnerJoin_Basic(t *testing.T) {
	op := makeTestJoinOp(types.InnerJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// user_id 1 matches twice, user_id 3 matches once = 3 rows total
	require.Len(t, result.Rows, 3)

	// All rows must have both left and right columns.
	for _, row := range result.Rows {
		assert.Contains(t, row, "id")
		assert.Contains(t, row, "name")
		assert.Contains(t, row, "user_id")
		assert.Contains(t, row, "amount")
	}

	// Verify the matched values
	var amounts []float64
	for _, row := range result.Rows {
		amounts = append(amounts, row["amount"].(float64))
	}
	assert.ElementsMatch(t, []float64{100, 200, 300}, amounts)
}

func TestHashJoin_LeftJoin_WithUnmatched(t *testing.T) {
	op := makeTestJoinOp(types.LeftOuterJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// user 1 -> 2 rows, user 2 -> 1 row (null right), user 3 -> 1 row = 4 rows
	require.Len(t, result.Rows, 4)

	// Find the unmatched row (Bob, id=2)
	var unmatched domain.Row
	for _, row := range result.Rows {
		if row["name"] == "Bob" {
			unmatched = row
			break
		}
	}
	require.NotNil(t, unmatched, "Bob should appear in LEFT JOIN result")
	assert.Nil(t, unmatched["user_id"], "unmatched right columns should be nil")
	assert.Nil(t, unmatched["amount"], "unmatched right columns should be nil")
}

func TestHashJoin_RightJoin_WithUnmatched(t *testing.T) {
	op := makeTestJoinOp(types.RightOuterJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// user_id 1 -> 2 rows matched (Alice), user_id 3 -> 1 row matched (Charlie),
	// user_id 4 -> 1 row unmatched = 4 rows
	require.Len(t, result.Rows, 4)

	// Find the unmatched row (user_id=4, no matching user)
	var unmatched domain.Row
	for _, row := range result.Rows {
		if row["user_id"] == int64(4) || row["right_user_id"] == int64(4) {
			unmatched = row
			break
		}
	}
	require.NotNil(t, unmatched, "order with user_id=4 should appear in RIGHT JOIN result")
	assert.Nil(t, unmatched["id"], "unmatched left columns should be nil")
	assert.Nil(t, unmatched["name"], "unmatched left columns should be nil")
}

func TestHashJoin_FullOuterJoin(t *testing.T) {
	op := makeTestJoinOp(types.FullOuterJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// Matched: user 1 x 2 orders + user 3 x 1 order = 3
	// Left unmatched: user 2 (Bob) = 1
	// Right unmatched: order user_id=4 = 1
	// Total = 5
	require.Len(t, result.Rows, 5)

	// Check that Bob (id=2) is present with null right side
	var bobRow domain.Row
	for _, row := range result.Rows {
		if row["name"] == "Bob" {
			bobRow = row
			break
		}
	}
	require.NotNil(t, bobRow, "Bob should appear in FULL OUTER JOIN")
	assert.Nil(t, bobRow["user_id"], "Bob's right columns should be nil")

	// Check that order with user_id=4 is present with null left side
	var orphanOrder domain.Row
	for _, row := range result.Rows {
		uid, ok1 := row["user_id"]
		ruid, ok2 := row["right_user_id"]
		if (ok1 && uid == int64(4)) || (ok2 && ruid == int64(4)) {
			orphanOrder = row
			break
		}
	}
	require.NotNil(t, orphanOrder, "order user_id=4 should appear in FULL OUTER JOIN")
	assert.Nil(t, orphanOrder["id"], "orphan order left columns should be nil")
	assert.Nil(t, orphanOrder["name"], "orphan order left columns should be nil")
}

func TestHashJoin_CrossJoin(t *testing.T) {
	op := makeTestJoinOp(types.CrossJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// Cross join: 3 users x 4 orders = 12 rows
	require.Len(t, result.Rows, 12)
}

func TestHashJoin_SemiJoin(t *testing.T) {
	op := makeTestJoinOp(types.SemiJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// SEMI JOIN returns left rows that have at least one match on the right.
	// Users 1 and 3 match, user 2 does not. Result = 2 rows.
	require.Len(t, result.Rows, 2)

	// Rows should only contain left-side columns.
	for _, row := range result.Rows {
		assert.Contains(t, row, "id")
		assert.Contains(t, row, "name")
		assert.NotContains(t, row, "user_id", "SEMI JOIN should not include right columns")
		assert.NotContains(t, row, "amount", "SEMI JOIN should not include right columns")
	}

	names := []interface{}{result.Rows[0]["name"], result.Rows[1]["name"]}
	assert.ElementsMatch(t, []interface{}{"Alice", "Charlie"}, names)
}

func TestHashJoin_AntiSemiJoin(t *testing.T) {
	op := makeTestJoinOp(types.AntiSemiJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// ANTI-SEMI JOIN returns left rows with NO match on the right.
	// Only user 2 (Bob) has no orders.
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Bob", result.Rows[0]["name"])
}

// ---------------------------------------------------------------------------
// Fix #14 -- Type-aware hash key
// ---------------------------------------------------------------------------

func TestHashKey_TypeAwareness(t *testing.T) {
	// int64(1) and string("1") must produce different keys.
	keyInt := hashKey(int64(1))
	keyStr := hashKey("1")
	assert.NotEqual(t, keyInt, keyStr, "int64(1) and string('1') must hash differently")

	// float64(1.0) and string("1") must produce different keys.
	keyFloat := hashKey(float64(1.0))
	assert.NotEqual(t, keyFloat, keyStr, "float64(1.0) and string('1') must hash differently")

	// int64(1) and float64(1.0) should also differ (different type prefixes).
	assert.NotEqual(t, keyInt, keyFloat, "int64(1) and float64(1.0) must hash differently")

	// bool values should produce distinct keys.
	keyTrue := hashKey(true)
	keyFalse := hashKey(false)
	assert.NotEqual(t, keyTrue, keyFalse, "true and false must hash differently")
	assert.NotEqual(t, keyTrue, keyStr, "bool(true) and string('1') must hash differently")

	// Same type, same value should produce equal keys.
	assert.Equal(t, hashKey(int64(42)), hashKey(int64(42)))
	assert.Equal(t, hashKey("hello"), hashKey("hello"))
	assert.Equal(t, hashKey(float64(3.14)), hashKey(float64(3.14)))
}

func TestHashKey_NilValue(t *testing.T) {
	keyNil := hashKey(nil)
	assert.NotEmpty(t, keyNil, "nil should produce a non-empty key")

	// nil should differ from every non-nil value
	assert.NotEqual(t, keyNil, hashKey(int64(0)))
	assert.NotEqual(t, keyNil, hashKey(""))
	assert.NotEqual(t, keyNil, hashKey(false))
	assert.NotEqual(t, keyNil, hashKey(float64(0)))

	// Two nils should produce the same key (deterministic).
	assert.Equal(t, keyNil, hashKey(nil))
}

// ---------------------------------------------------------------------------
// Fix #21 -- Multi-condition JOIN
// ---------------------------------------------------------------------------

func TestMultiHashKey_MultipleColumns(t *testing.T) {
	row := domain.Row{
		"a": int64(1),
		"b": "hello",
		"c": float64(2.5),
	}

	// Single column should match hashKey directly.
	assert.Equal(t, hashKey(int64(1)), multiHashKey(row, []string{"a"}))

	// Multi-column key should be a pipe-separated concatenation.
	multiKey := multiHashKey(row, []string{"a", "b"})
	expected := hashKey(int64(1)) + "|" + hashKey("hello")
	assert.Equal(t, expected, multiKey)

	// Order matters.
	reverseKey := multiHashKey(row, []string{"b", "a"})
	assert.NotEqual(t, multiKey, reverseKey, "column order must affect the key")

	// Three columns.
	triKey := multiHashKey(row, []string{"a", "b", "c"})
	expectedTri := hashKey(int64(1)) + "|" + hashKey("hello") + "|" + hashKey(float64(2.5))
	assert.Equal(t, expectedTri, triKey)
}

func TestHashJoin_MultiCondition(t *testing.T) {
	// Left: employees with dept_id and location_id
	leftResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "emp_name", Type: "TEXT"},
			{Name: "dept_id", Type: "INT"},
			{Name: "location_id", Type: "INT"},
		},
		Rows: []domain.Row{
			{"emp_name": "Alice", "dept_id": int64(1), "location_id": int64(10)},
			{"emp_name": "Bob", "dept_id": int64(1), "location_id": int64(20)},
			{"emp_name": "Charlie", "dept_id": int64(2), "location_id": int64(10)},
		},
	}

	// Right: departments with dept_id and location_id
	rightResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "dept_id", Type: "INT"},
			{Name: "location_id", Type: "INT"},
			{Name: "dept_name", Type: "TEXT"},
		},
		Rows: []domain.Row{
			{"dept_id": int64(1), "location_id": int64(10), "dept_name": "Engineering-NYC"},
			{"dept_id": int64(2), "location_id": int64(20), "dept_name": "Marketing-LA"},
		},
	}

	op := &HashJoinOperator{
		BaseOperator: &BaseOperator{
			children: []Operator{
				&mockChildOperator{result: leftResult},
				&mockChildOperator{result: rightResult},
			},
		},
		config: &plan.HashJoinConfig{
			JoinType: types.InnerJoin,
			LeftConds: []*types.JoinCondition{
				{Left: &types.Expression{Type: "column", Column: "dept_id"}},
				{Left: &types.Expression{Type: "column", Column: "location_id"}},
			},
			RightConds: []*types.JoinCondition{
				{Left: &types.Expression{Type: "column", Column: "dept_id"}},
				{Left: &types.Expression{Type: "column", Column: "location_id"}},
			},
		},
	}

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// Only Alice matches (dept_id=1, location_id=10).
	// Bob has (1, 20) - no match. Charlie has (2, 10) - no match.
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "Alice", result.Rows[0]["emp_name"])
	assert.Equal(t, "Engineering-NYC", result.Rows[0]["dept_name"])
}

// ---------------------------------------------------------------------------
// Column conflict handling -- mergeRowPair
// ---------------------------------------------------------------------------

func TestMergeRowPair_ColumnConflict(t *testing.T) {
	left := domain.Row{"id": int64(1), "name": "Alice"}
	right := domain.Row{"id": int64(100), "value": "order1"}

	merged := mergeRowPair(left, right, 4)

	// Left "id" should be preserved as-is.
	assert.Equal(t, int64(1), merged["id"])
	// Conflicting right "id" should be prefixed with "right_".
	assert.Equal(t, int64(100), merged["right_id"])
	// Non-conflicting columns should be present normally.
	assert.Equal(t, "Alice", merged["name"])
	assert.Equal(t, "order1", merged["value"])
}

func TestMergeRowWithNullRight(t *testing.T) {
	left := domain.Row{"id": int64(1), "name": "Alice"}
	rightCols := []domain.ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "amount", Type: "FLOAT"},
	}

	merged := mergeRowWithNullRight(left, rightCols)

	// Left columns preserved.
	assert.Equal(t, int64(1), merged["id"])
	assert.Equal(t, "Alice", merged["name"])
	// Conflicting right column "id" -> "right_id" with nil value.
	assert.Nil(t, merged["right_id"])
	// Non-conflicting right column.
	assert.Nil(t, merged["amount"])
}

func TestMergeRowWithNullLeft(t *testing.T) {
	right := domain.Row{"id": int64(100), "amount": float64(500)}
	leftCols := []domain.ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "TEXT"},
	}

	merged := mergeRowWithNullLeft(right, leftCols)

	// Left columns should be nil.
	assert.Nil(t, merged["id"])
	assert.Nil(t, merged["name"])
	// The right column "id" conflicts with left "id" (already set to nil),
	// so it becomes "right_id".
	assert.Equal(t, int64(100), merged["right_id"])
	// Non-conflicting right column.
	assert.Equal(t, float64(500), merged["amount"])
}

// ---------------------------------------------------------------------------
// mergeColumnInfos
// ---------------------------------------------------------------------------

func TestMergeColumnInfos_SemiJoin(t *testing.T) {
	leftCols := []domain.ColumnInfo{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "TEXT"},
	}
	rightCols := []domain.ColumnInfo{
		{Name: "user_id", Type: "INT"},
		{Name: "amount", Type: "FLOAT"},
	}

	// SEMI JOIN should return only left columns.
	merged := mergeColumnInfos(leftCols, rightCols, types.SemiJoin)
	assert.Equal(t, leftCols, merged)

	// ANTI-SEMI JOIN should also return only left columns.
	merged = mergeColumnInfos(leftCols, rightCols, types.AntiSemiJoin)
	assert.Equal(t, leftCols, merged)
}

func TestMergeColumnInfos_ConflictHandling(t *testing.T) {
	leftCols := []domain.ColumnInfo{
		{Name: "id", Type: "INT", Nullable: false},
		{Name: "name", Type: "TEXT", Nullable: false},
	}
	rightCols := []domain.ColumnInfo{
		{Name: "id", Type: "INT", Nullable: true},
		{Name: "amount", Type: "FLOAT", Nullable: false},
	}

	merged := mergeColumnInfos(leftCols, rightCols, types.InnerJoin)
	require.Len(t, merged, 4)

	// Left columns first, unchanged.
	assert.Equal(t, "id", merged[0].Name)
	assert.Equal(t, "INT", merged[0].Type)
	assert.False(t, merged[0].Nullable)

	assert.Equal(t, "name", merged[1].Name)

	// Conflicting right "id" becomes "right_id", preserving original type & nullable.
	assert.Equal(t, "right_id", merged[2].Name)
	assert.Equal(t, "INT", merged[2].Type)
	assert.True(t, merged[2].Nullable)

	// Non-conflicting right column stays as-is.
	assert.Equal(t, "amount", merged[3].Name)
}

func TestMergeColumnInfos_NoConflict(t *testing.T) {
	leftCols := []domain.ColumnInfo{
		{Name: "id", Type: "INT"},
	}
	rightCols := []domain.ColumnInfo{
		{Name: "amount", Type: "FLOAT"},
	}

	merged := mergeColumnInfos(leftCols, rightCols, types.InnerJoin)
	require.Len(t, merged, 2)
	assert.Equal(t, "id", merged[0].Name)
	assert.Equal(t, "amount", merged[1].Name)
}

// ---------------------------------------------------------------------------
// Empty table edge cases
// ---------------------------------------------------------------------------

func TestHashJoin_EmptyTables(t *testing.T) {
	emptyUsers := emptyResult(usersResult().Columns)
	emptyOrders := emptyResult(ordersResult().Columns)

	t.Run("inner join both empty", func(t *testing.T) {
		op := makeTestJoinOp(types.InnerJoin, emptyUsers, emptyOrders, "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		assert.Empty(t, result.Rows)
	})

	t.Run("inner join left empty", func(t *testing.T) {
		op := makeTestJoinOp(types.InnerJoin, emptyUsers, ordersResult(), "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		assert.Empty(t, result.Rows)
	})

	t.Run("inner join right empty", func(t *testing.T) {
		op := makeTestJoinOp(types.InnerJoin, usersResult(), emptyOrders, "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		assert.Empty(t, result.Rows)
	})

	t.Run("left join right empty", func(t *testing.T) {
		op := makeTestJoinOp(types.LeftOuterJoin, usersResult(), emptyOrders, "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		// All 3 left rows appear with null right columns.
		require.Len(t, result.Rows, 3)
	})

	t.Run("right join left empty", func(t *testing.T) {
		op := makeTestJoinOp(types.RightOuterJoin, emptyUsers, ordersResult(), "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		// All 4 right rows appear with null left columns.
		require.Len(t, result.Rows, 4)
	})

	t.Run("full outer join both empty", func(t *testing.T) {
		op := makeTestJoinOp(types.FullOuterJoin, emptyUsers, emptyOrders, "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		assert.Empty(t, result.Rows)
	})

	t.Run("cross join with empty left", func(t *testing.T) {
		op := makeTestJoinOp(types.CrossJoin, emptyUsers, ordersResult(), "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		assert.Empty(t, result.Rows)
	})

	t.Run("semi join right empty", func(t *testing.T) {
		op := makeTestJoinOp(types.SemiJoin, usersResult(), emptyOrders, "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		assert.Empty(t, result.Rows, "SEMI JOIN with empty right should return no rows")
	})

	t.Run("anti-semi join right empty", func(t *testing.T) {
		op := makeTestJoinOp(types.AntiSemiJoin, usersResult(), emptyOrders, "id", "user_id")
		result, err := op.Execute(context.Background())
		require.NoError(t, err)
		// All left rows have no match, so all are returned.
		require.Len(t, result.Rows, 3)
	})
}

// ---------------------------------------------------------------------------
// Additional edge cases
// ---------------------------------------------------------------------------

func TestHashJoin_DuplicateKeysOnBothSides(t *testing.T) {
	// Both sides have duplicate join keys -- result should be the cartesian
	// product within each matching key.
	left := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "k", Type: "INT"}, {Name: "lv", Type: "TEXT"}},
		Rows: []domain.Row{
			{"k": int64(1), "lv": "L1"},
			{"k": int64(1), "lv": "L2"},
		},
	}
	right := &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "k", Type: "INT"}, {Name: "rv", Type: "TEXT"}},
		Rows: []domain.Row{
			{"k": int64(1), "rv": "R1"},
			{"k": int64(1), "rv": "R2"},
		},
	}

	op := makeTestJoinOp(types.InnerJoin, left, right, "k", "k")
	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// 2 left x 2 right = 4 matched rows.
	require.Len(t, result.Rows, 4)
}

func TestHashJoin_SemiJoin_NoDuplicateLeftRows(t *testing.T) {
	// Even though the right side has two matching rows for user_id=1,
	// SEMI JOIN should return each left row at most once.
	op := makeTestJoinOp(types.SemiJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// Users 1 and 3 match. User 1 matches twice on the right, but should
	// appear only once in the result.
	require.Len(t, result.Rows, 2)

	ids := []interface{}{result.Rows[0]["id"], result.Rows[1]["id"]}
	assert.ElementsMatch(t, []interface{}{int64(1), int64(3)}, ids)
}

func TestHashKey_IntVariants(t *testing.T) {
	// int and int64 with the same numeric value should produce the same key
	// because the hash function normalises both under the "i:" prefix.
	keyInt := hashKey(int(42))
	keyInt64 := hashKey(int64(42))
	assert.Equal(t, keyInt, keyInt64, "int(42) and int64(42) should hash the same")
}

func TestHashJoin_HashJoinType_TreatedAsInner(t *testing.T) {
	// types.HashJoin is handled in the same branch as InnerJoin.
	op := makeTestJoinOp(types.HashJoin, usersResult(), ordersResult(), "id", "user_id")

	result, err := op.Execute(context.Background())
	require.NoError(t, err)

	// Same result as INNER JOIN: 3 matched rows.
	require.Len(t, result.Rows, 3)
}
