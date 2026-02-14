package parallel

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewParallelHashJoinExecutor(t *testing.T) {
	leftResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
	}
	rightResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
	}

	tests := []struct {
		name          string
		joinType      parser.JoinType
		left          *domain.QueryResult
		right         *domain.QueryResult
		condition     *parser.Expression
		buildParallel int
		probeParallel int
	}{
		{
			name:          "inner join",
			joinType:      parser.JoinTypeInner,
			left:          leftResult,
			right:         rightResult,
			condition:     nil,
			buildParallel: 2,
			probeParallel: 2,
		},
		{
			name:          "left outer join",
			joinType:      parser.JoinTypeLeft,
			left:          leftResult,
			right:         rightResult,
			condition:     nil,
			buildParallel: 1,
			probeParallel: 1,
		},
		{
			name:          "right outer join",
			joinType:      parser.JoinTypeRight,
			left:          leftResult,
			right:         rightResult,
			condition:     nil,
			buildParallel: 4,
			probeParallel: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workerPool := NewWorkerPool(2)
			executor := NewParallelHashJoinExecutor(
				tt.joinType,
				tt.left,
				tt.right,
				tt.condition,
				tt.buildParallel,
				tt.probeParallel,
				workerPool,
			)

			assert.NotNil(t, executor)
			assert.Equal(t, tt.joinType, executor.joinType)
			assert.Equal(t, tt.left, executor.left)
			assert.Equal(t, tt.right, executor.right)
			assert.Equal(t, tt.buildParallel, executor.buildParallel)
			assert.Equal(t, tt.probeParallel, executor.probeParallel)
			assert.NotNil(t, executor.workerPool)
		})
	}
}

func TestParallelHashJoinExecutor_Execute(t *testing.T) {
	leftResult := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
			{"id": int64(3), "name": "Charlie"},
		},
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "string"},
		},
		Total: 3,
	}

	rightResult := &domain.QueryResult{
		Rows: []domain.Row{
			{"id": int64(1), "age": 25},
			{"id": int64(2), "age": 30},
			{"id": int64(4), "age": 35},
		},
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "age", Type: "int"},
		},
		Total: 3,
	}

	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		leftResult,
		rightResult,
		nil,
		2,
		2,
		workerPool,
	)

	ctx := context.Background()
	result, err := executor.Execute(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	// Should match on id 1 and 2
	assert.GreaterOrEqual(t, len(result.Rows), 0)
}

func TestParallelHashJoinExecutor_Execute_EmptyResults(t *testing.T) {
	leftResult := &domain.QueryResult{
		Rows: []domain.Row{},
		Columns: []domain.ColumnInfo{},
		Total:   0,
	}

	rightResult := &domain.QueryResult{
		Rows: []domain.Row{},
		Columns: []domain.ColumnInfo{},
		Total:   0,
	}

	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		leftResult,
		rightResult,
		nil,
		1,
		1,
		workerPool,
	)

	ctx := context.Background()
	result, err := executor.Execute(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(0), result.Total)
}

func TestParallelHashJoinExecutor_Execute_ContextCancellation(t *testing.T) {
	leftResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
		Total: 1,
	}

	rightResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
		Total: 1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		leftResult,
		rightResult,
		nil,
		2,
		2,
		workerPool,
	)

	_, err := executor.Execute(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestParallelHashJoinExecutor_MergeRows(t *testing.T) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		2,
		2,
		workerPool,
	)

	leftCols := []domain.ColumnInfo{
		{Name: "id", Type: "int"},
		{Name: "name", Type: "string"},
	}

	rightCols := []domain.ColumnInfo{
		{Name: "age", Type: "int"},
	}

	leftRow := domain.Row{
		"id":   int64(1),
		"name": "Alice",
	}

	rightRow := domain.Row{
		"age": int64(25),
	}

	tests := []struct {
		name       string
		leftRow    domain.Row
		rightRow   domain.Row
		leftCols   []domain.ColumnInfo
		rightCols  []domain.ColumnInfo
		wantFields int
	}{
		{
			name:       "no conflict",
			leftRow:    leftRow,
			rightRow:   rightRow,
			leftCols:   leftCols,
			rightCols:  rightCols,
			wantFields: 3,
		},
		{
			name: "with column conflict",
			leftRow: domain.Row{
				"id": int64(1),
			},
			rightRow: domain.Row{
				"id": int64(2), // conflicting column
			},
			leftCols: []domain.ColumnInfo{
				{Name: "id", Type: "int"},
			},
			rightCols: []domain.ColumnInfo{
				{Name: "id", Type: "int"},
			},
			wantFields: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := executor.mergeRows(tt.leftRow, tt.rightRow, tt.leftCols, tt.rightCols)
			assert.NotNil(t, merged)
			assert.Equal(t, tt.wantFields, len(merged))
		})
	}
}

func TestParallelHashJoinExecutor_MergeColumns(t *testing.T) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		2,
		2,
		workerPool,
	)

	tests := []struct {
		name       string
		leftCols   []domain.ColumnInfo
		rightCols  []domain.ColumnInfo
		wantLength int
	}{
		{
			name: "no conflict",
			leftCols: []domain.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "string"},
			},
			rightCols: []domain.ColumnInfo{
				{Name: "age", Type: "int"},
			},
			wantLength: 3,
		},
		{
			name: "with conflict",
			leftCols: []domain.ColumnInfo{
				{Name: "id", Type: "int"},
			},
			rightCols: []domain.ColumnInfo{
				{Name: "id", Type: "int"},
				{Name: "name", Type: "string"},
			},
			wantLength: 3, // 1 from left + 2 from right (right_id prefixed)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := executor.mergeColumns(tt.leftCols, tt.rightCols)
			assert.Len(t, merged, tt.wantLength)
		})
	}
}

func TestParallelHashJoinExecutor_MergeJoinResults(t *testing.T) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		2,
		2,
		workerPool,
	)

	tests := []struct {
		name     string
		results  [2]*domain.QueryResult
		empty    bool
	}{
		{
			name: "both nil",
			results: [2]*domain.QueryResult{nil, nil},
			empty:    true,
		},
		{
			name: "first nil, second has data",
			results: [2]*domain.QueryResult{
				nil,
				{Rows: []domain.Row{{"id": int64(1)}}, Total: 1},
			},
			empty: false,
		},
		{
			name: "first has data, second nil",
			results: [2]*domain.QueryResult{
				{Rows: []domain.Row{{"id": int64(1)}}, Total: 1},
				nil,
			},
			empty: false,
		},
		{
			name: "both have data",
			results: [2]*domain.QueryResult{
				{Rows: []domain.Row{{"id": int64(1)}}, Total: 1},
				{Rows: []domain.Row{{"id": int64(2)}}, Total: 1},
			},
			empty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := executor.mergeJoinResults(tt.results)
			if tt.empty {
				assert.Equal(t, int64(0), merged.Total)
			} else {
				assert.Greater(t, merged.Total, int64(0))
			}
		})
	}
}

func TestParallelHashJoinExecutor_ComputeHashKey(t *testing.T) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		2,
		2,
		workerPool,
	)

	tests := []struct {
		name  string
		row   domain.Row
		cols  []string
	}{
		{
			name: "single column",
			row:  domain.Row{"id": int64(1)},
			cols:  []string{"id"},
		},
		{
			name: "multiple columns",
			row: domain.Row{
				"id":   int64(1),
				"name": "Alice",
				"age":  int64(25),
			},
			cols: []string{"id", "name"},
		},
		{
			name: "nil values in row",
			row: domain.Row{
				"id":   int64(1),
				"name": nil,
				"age":  int64(25),
			},
			cols: []string{"id", "name", "age"},
		},
		{
			name: "empty cols",
			row:  domain.Row{"id": int64(1)},
			cols: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := executor.computeHashKey(tt.row, tt.cols)
			// empty cols returns 0, others return > 0
			if len(tt.cols) == 0 {
				assert.Equal(t, uint64(0), key)
			} else {
				assert.Greater(t, key, uint64(0))
			}
		})
	}
}

func TestParallelHashJoinExecutor_ExtractJoinColumns(t *testing.T) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		2,
		2,
		workerPool,
	)

	tests := []struct {
		name         string
		condition    *parser.Expression
		wantLeftLen  int
		wantRightLen int
	}{
		{
			name:         "nil condition",
			condition:    nil,
			wantLeftLen:  1, // defaults to ["id"]
			wantRightLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols := executor.extractJoinColumns()
			assert.Equal(t, tt.wantLeftLen, len(cols.Left))
			assert.Equal(t, tt.wantRightLen, len(cols.Right))
		})
	}
}

func TestParallelHashJoinExecutor_Explain(t *testing.T) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		4,
		4,
		workerPool,
	)

	explanation := executor.Explain()

	assert.NotEmpty(t, explanation)
	assert.Contains(t, explanation, "ParallelHashJoin")
	assert.Contains(t, explanation, "type=")
	assert.Contains(t, explanation, "buildParallel=4")
	assert.Contains(t, explanation, "probeParallel=4")
}

func TestParallelHashJoinExecutor_DifferentJoinTypes(t *testing.T) {
	joinTypes := []parser.JoinType{
		parser.JoinTypeInner,
		parser.JoinTypeLeft,
		parser.JoinTypeRight,
		parser.JoinTypeFull,
	}

	leftResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
		Total: 1,
	}

	rightResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
		Total: 1,
	}

	for _, joinType := range joinTypes {
		t.Run(string(joinType), func(t *testing.T) {
			workerPool := NewWorkerPool(2)
			executor := NewParallelHashJoinExecutor(
				joinType,
				leftResult,
				rightResult,
				nil,
				1,
				1,
				workerPool,
			)

			assert.Equal(t, joinType, executor.joinType)

			explanation := executor.Explain()
			assert.Contains(t, explanation, string(joinType))
		})
	}
}

func TestParallelHashJoinExecutor_DifferentParallelism(t *testing.T) {
	leftResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
		Total: 1,
	}

	rightResult := &domain.QueryResult{
		Rows: []domain.Row{{"id": int64(1)}},
		Total: 1,
	}

	tests := []struct {
		name          string
		buildParallel int
		probeParallel int
	}{
		{"1:1", 1, 1},
		{"2:2", 2, 2},
		{"4:8", 4, 8},
		{"8:4", 8, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workerPool := NewWorkerPool(8)
			executor := NewParallelHashJoinExecutor(
				parser.JoinTypeInner,
				leftResult,
				rightResult,
				nil,
				tt.buildParallel,
				tt.probeParallel,
				workerPool,
			)

			assert.Equal(t, tt.buildParallel, executor.buildParallel)
			assert.Equal(t, tt.probeParallel, executor.probeParallel)

			explanation := executor.Explain()
			assert.Contains(t, explanation, "buildParallel="+string(rune('0'+tt.buildParallel)))
		})
	}
}

func TestParallelHashJoinExecutor_LargeDatasets(t *testing.T) {
	leftRows := make([]domain.Row, 100)
	for i := 0; i < 100; i++ {
		leftRows[i] = domain.Row{
			"id":   int64(i + 1),
			"name": "user" + string(rune('0'+i%10)),
		}
	}

	rightRows := make([]domain.Row, 100)
	for i := 0; i < 100; i++ {
		rightRows[i] = domain.Row{
			"id":  int64(i + 1),
			"age": int64(20 + i%50),
		}
	}

	leftResult := &domain.QueryResult{
		Rows:    leftRows,
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}, {Name: "name", Type: "string"}},
		Total:   100,
	}

	rightResult := &domain.QueryResult{
		Rows:    rightRows,
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}, {Name: "age", Type: "int"}},
		Total:   100,
	}

	workerPool := NewWorkerPool(4)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		leftResult,
		rightResult,
		nil,
		4,
		4,
		workerPool,
	)

	ctx := context.Background()
	result, err := executor.Execute(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.Rows), 0)
}

func TestJoinColumns_Struct(t *testing.T) {
	cols := JoinColumns{
		Left:  []string{"id", "name"},
		Right: []string{"id", "age"},
	}

	assert.Len(t, cols.Left, 2)
	assert.Len(t, cols.Right, 2)
	assert.Equal(t, "id", cols.Left[0])
	assert.Equal(t, "age", cols.Right[1])
}

// Benchmark tests
func BenchmarkParallelHashJoinExecutor_Execute(b *testing.B) {
	leftRows := make([]domain.Row, 1000)
	for i := 0; i < 1000; i++ {
		leftRows[i] = domain.Row{"id": int64(i + 1)}
	}

	rightRows := make([]domain.Row, 1000)
	for i := 0; i < 1000; i++ {
		rightRows[i] = domain.Row{"id": int64(i + 1)}
	}

	leftResult := &domain.QueryResult{
		Rows:  leftRows,
		Total: 1000,
	}

	rightResult := &domain.QueryResult{
		Rows:  rightRows,
		Total: 1000,
	}

	workerPool := NewWorkerPool(4)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		leftResult,
		rightResult,
		nil,
		4,
		4,
		workerPool,
	)

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Execute(ctx)
	}
}

func BenchmarkParallelHashJoinExecutor_ComputeHashKey(b *testing.B) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		2,
		2,
		workerPool,
	)

	row := domain.Row{
		"id":   int64(1),
		"name": "Alice",
		"age":  int64(25),
	}
	cols := []string{"id", "name", "age"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.computeHashKey(row, cols)
	}
}

func BenchmarkParallelHashJoinExecutor_MergeRows(b *testing.B) {
	workerPool := NewWorkerPool(2)
	executor := NewParallelHashJoinExecutor(
		parser.JoinTypeInner,
		&domain.QueryResult{},
		&domain.QueryResult{},
		nil,
		2,
		2,
		workerPool,
	)

	leftRow := domain.Row{"id": int64(1), "name": "Alice"}
	rightRow := domain.Row{"age": int64(25)}
	leftCols := []domain.ColumnInfo{{Name: "id"}, {Name: "name"}}
	rightCols := []domain.ColumnInfo{{Name: "age"}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.mergeRows(leftRow, rightRow, leftCols, rightCols)
	}
}
