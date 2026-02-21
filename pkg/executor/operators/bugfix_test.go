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

// ==========================================================================
// Bug 1 (P0): Aggregate alias mismatch — default alias uses len(groups[key])
// which changes as aggregates are added, so each row writes to a DIFFERENT
// key. This produces completely wrong aggregate results for multi-row groups.
// ==========================================================================

// mockChildOperator returns a fixed QueryResult and satisfies Operator interface
type mockChildOperator struct {
	result *domain.QueryResult
}

func (m *mockChildOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return m.result, nil
}

func (m *mockChildOperator) GetChildren() []Operator {
	return nil
}

func (m *mockChildOperator) GetSchema() []domain.ColumnInfo {
	return m.result.Columns
}

func TestBug1_AggregateAlias_MultiRowCount(t *testing.T) {
	// Two rows in the same group should give COUNT=3, not COUNT=1
	childResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "category", Type: "TEXT"},
			{Name: "value", Type: "INT"},
		},
		Rows: []domain.Row{
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": int64(20)},
			{"category": "A", "value": int64(30)},
		},
	}

	op := &AggregateOperator{
		BaseOperator: &BaseOperator{
			children: []Operator{&mockChildOperator{result: childResult}},
		},
		config: &plan.AggregateConfig{
			GroupByCols: []string{"category"},
			AggFuncs: []*types.AggregationItem{
				{
					Type:  types.Count,
					Alias: "", // empty alias triggers the bug
					Expr:  &types.Expression{Type: "column", Column: "value"},
				},
			},
		},
	}

	result, err := op.Execute(context.Background())
	require.NoError(t, err)
	require.Len(t, result.Rows, 1) // one group "A"

	row := result.Rows[0]
	// BUG: With broken alias generation, each row writes to a different key
	// ("agg_1", "agg_2", "agg_3"), so no key has count=3.
	// The output column expects "agg_0" (from len(outputColumns) formula).
	// After fix: "agg_0" should have count=3.
	countVal, ok := row["agg_0"]
	assert.True(t, ok, "expected key 'agg_0' in result row, got keys: %v", row)
	assert.Equal(t, 3, countVal, "COUNT should be 3 for 3 rows in group A")
}

func TestBug1_AggregateAlias_MultiRowSum(t *testing.T) {
	// Three rows in the same group: SUM should be 60
	childResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "category", Type: "TEXT"},
			{Name: "value", Type: "INT"},
		},
		Rows: []domain.Row{
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": int64(20)},
			{"category": "A", "value": int64(30)},
		},
	}

	op := &AggregateOperator{
		BaseOperator: &BaseOperator{
			children: []Operator{&mockChildOperator{result: childResult}},
		},
		config: &plan.AggregateConfig{
			GroupByCols: []string{"category"},
			AggFuncs: []*types.AggregationItem{
				{
					Type:  types.Sum,
					Alias: "", // empty alias triggers the bug
					Expr:  &types.Expression{Type: "column", Column: "value"},
				},
			},
		},
	}

	result, err := op.Execute(context.Background())
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	row := result.Rows[0]
	sumVal, ok := row["agg_0"]
	assert.True(t, ok, "expected key 'agg_0' in result row, got keys: %v", row)
	assert.Equal(t, float64(60), sumVal, "SUM should be 60 for values 10+20+30")
}

func TestBug1_AggregateAlias_Avg(t *testing.T) {
	// Three rows: AVG should be 20.0
	childResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "category", Type: "TEXT"},
			{Name: "value", Type: "INT"},
		},
		Rows: []domain.Row{
			{"category": "A", "value": int64(10)},
			{"category": "A", "value": int64(20)},
			{"category": "A", "value": int64(30)},
		},
	}

	op := &AggregateOperator{
		BaseOperator: &BaseOperator{
			children: []Operator{&mockChildOperator{result: childResult}},
		},
		config: &plan.AggregateConfig{
			GroupByCols: []string{"category"},
			AggFuncs: []*types.AggregationItem{
				{
					Type:  types.Avg,
					Alias: "", // empty alias triggers the bug
					Expr:  &types.Expression{Type: "column", Column: "value"},
				},
			},
		},
	}

	result, err := op.Execute(context.Background())
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	row := result.Rows[0]
	avgVal, ok := row["agg_0"]
	assert.True(t, ok, "expected key 'agg_0' in result row, got keys: %v", row)
	assert.Equal(t, float64(20), avgVal, "AVG should be 20.0 for values 10+20+30")
}

// ==========================================================================
// Bug 2 (P2): Hash join type collision — int(1) and string("1") produce
// the same hash key, causing false join matches.
// ==========================================================================

func TestBug2_HashJoin_TypeCollision(t *testing.T) {
	// Left table has int join column, right table has string join column.
	// int(1) and string("1") should NOT match.
	leftResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "left_val", Type: "TEXT"},
		},
		Rows: []domain.Row{
			{"id": int64(1), "left_val": "left_a"},
		},
	}
	rightResult := &domain.QueryResult{
		Columns: []domain.ColumnInfo{
			{Name: "ref_id", Type: "TEXT"},
			{Name: "right_val", Type: "TEXT"},
		},
		Rows: []domain.Row{
			{"ref_id": "1", "right_val": "right_b"}, // string "1", not int 1
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
			LeftCond: &types.JoinCondition{
				Left: &types.Expression{Type: "column", Column: "id"},
			},
			RightCond: &types.JoinCondition{
				Left: &types.Expression{Type: "column", Column: "ref_id"},
			},
		},
	}

	result, err := op.Execute(context.Background())
	require.NoError(t, err)
	// BUG: Currently matches because fmt.Sprintf("%v", int64(1)) == fmt.Sprintf("%v", "1") == "1"
	// After fix, int64(1) → "int64:1" and "1" → "string:1", so no match.
	assert.Len(t, result.Rows, 0, "int(1) should not match string('1') in join")
}
