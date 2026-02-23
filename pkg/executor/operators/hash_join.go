package operators

import (
	"context"
	"fmt"
	"strconv"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/types"
)

// HashJoinOperator Hash Join算子
type HashJoinOperator struct {
	*BaseOperator
	config *plan.HashJoinConfig
}

// NewHashJoinOperator 创建Hash Join算子
func NewHashJoinOperator(p *plan.Plan, das dataaccess.Service) (*HashJoinOperator, error) {
	config, ok := p.Config.(*plan.HashJoinConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for HashJoin: %T", p.Config)
	}

	base := NewBaseOperator(p, das)

	// 构建子算子
	buildFn := func(childPlan *plan.Plan) (Operator, error) {
		return buildOperator(childPlan, das)
	}
	if err := base.BuildChildOperators(buildFn); err != nil {
		return nil, err
	}

	return &HashJoinOperator{
		BaseOperator: base,
		config:       config,
	}, nil
}

// hashKey builds a type-aware hash key for a value to avoid type collisions
// (e.g., int64(1) vs string("1") must produce different keys).
func hashKey(val interface{}) string {
	if val == nil {
		return "nil:"
	}
	switch v := val.(type) {
	case int64:
		return "i:" + strconv.FormatInt(v, 10)
	case int:
		return "i:" + strconv.Itoa(v)
	case float64:
		return "f:" + strconv.FormatFloat(v, 'g', -1, 64)
	case string:
		return "s:" + v
	case bool:
		if v {
			return "b:1"
		}
		return "b:0"
	default:
		return fmt.Sprintf("%T:%v", val, val)
	}
}

// multiHashKey builds a hash key for multiple join columns.
func multiHashKey(row domain.Row, cols []string) string {
	if len(cols) == 1 {
		return hashKey(row[cols[0]])
	}
	key := ""
	for i, col := range cols {
		if i > 0 {
			key += "|"
		}
		key += hashKey(row[col])
	}
	return key
}

// mergeRowPair merges a left row and a right row, prefixing conflicting column
// names from the right side with "right_".
func mergeRowPair(left, right domain.Row, totalCols int) domain.Row {
	merged := make(domain.Row, totalCols)
	for k, v := range left {
		merged[k] = v
	}
	for k, v := range right {
		if _, exists := merged[k]; exists {
			merged["right_"+k] = v
		} else {
			merged[k] = v
		}
	}
	return merged
}

// mergeRowWithNullRight creates a row with left data and nil for every right column.
func mergeRowWithNullRight(left domain.Row, rightCols []domain.ColumnInfo) domain.Row {
	merged := make(domain.Row, len(left)+len(rightCols))
	for k, v := range left {
		merged[k] = v
	}
	for _, col := range rightCols {
		if _, exists := merged[col.Name]; exists {
			merged["right_"+col.Name] = nil
		} else {
			merged[col.Name] = nil
		}
	}
	return merged
}

// mergeRowWithNullLeft creates a row with nil for every left column and right data.
func mergeRowWithNullLeft(right domain.Row, leftCols []domain.ColumnInfo) domain.Row {
	merged := make(domain.Row, len(right)+len(leftCols))
	for _, col := range leftCols {
		merged[col.Name] = nil
	}
	for k, v := range right {
		if _, exists := merged[k]; exists {
			merged["right_"+k] = v
		} else {
			merged[k] = v
		}
	}
	return merged
}

// Execute 执行Hash Join，支持 INNER / LEFT / RIGHT / FULL OUTER / CROSS / SEMI / ANTI-SEMI JOIN
func (op *HashJoinOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	if len(op.children) < 2 {
		return nil, fmt.Errorf("HashJoin requires 2 children, got %d", len(op.children))
	}

	leftChild := op.children[0]
	rightChild := op.children[1]

	leftResult, err := leftChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute left child failed: %w", err)
	}

	rightResult, err := rightChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute right child failed: %w", err)
	}

	// Extract join column names (support multi-condition via LeftConds/RightConds)
	leftJoinCols := op.getLeftJoinCols()
	rightJoinCols := op.getRightJoinCols()
	hasCondition := len(leftJoinCols) > 0 && len(rightJoinCols) > 0

	totalCols := len(leftResult.Columns) + len(rightResult.Columns)
	joinType := op.config.JoinType

	// Build hash table from right side
	hashTable := make(map[string][]domain.Row)
	if hasCondition {
		for _, rightRow := range rightResult.Rows {
			key := multiHashKey(rightRow, rightJoinCols)
			hashTable[key] = append(hashTable[key], rightRow)
		}
	}

	var joinedRows []domain.Row

	switch joinType {
	case types.InnerJoin, types.HashJoin:
		joinedRows = op.executeInnerJoin(leftResult, hashTable, leftJoinCols, hasCondition, rightResult, totalCols)

	case types.LeftOuterJoin:
		joinedRows = op.executeLeftJoin(leftResult, rightResult, hashTable, leftJoinCols, hasCondition, totalCols)

	case types.RightOuterJoin:
		joinedRows = op.executeRightJoin(leftResult, rightResult, hashTable, rightJoinCols, leftJoinCols, hasCondition, totalCols)

	case types.FullOuterJoin:
		joinedRows = op.executeFullOuterJoin(leftResult, rightResult, hashTable, leftJoinCols, rightJoinCols, hasCondition, totalCols)

	case types.CrossJoin:
		joinedRows = op.executeCrossJoin(leftResult, rightResult, totalCols)

	case types.SemiJoin:
		joinedRows = op.executeSemiJoin(leftResult, hashTable, leftJoinCols, hasCondition, rightResult)

	case types.AntiSemiJoin:
		joinedRows = op.executeAntiSemiJoin(leftResult, hashTable, leftJoinCols, hasCondition, rightResult)

	default:
		joinedRows = op.executeInnerJoin(leftResult, hashTable, leftJoinCols, hasCondition, rightResult, totalCols)
	}

	mergedColumns := mergeColumnInfos(leftResult.Columns, rightResult.Columns, joinType)

	return &domain.QueryResult{
		Columns: mergedColumns,
		Rows:    joinedRows,
	}, nil
}

// getLeftJoinCols extracts left join column names from config.
func (op *HashJoinOperator) getLeftJoinCols() []string {
	var cols []string
	if len(op.config.LeftConds) > 0 {
		for _, cond := range op.config.LeftConds {
			if cond != nil && cond.Left != nil && cond.Left.Column != "" {
				cols = append(cols, cond.Left.Column)
			}
		}
		return cols
	}
	if op.config.LeftCond != nil && op.config.LeftCond.Left != nil && op.config.LeftCond.Left.Column != "" {
		return []string{op.config.LeftCond.Left.Column}
	}
	return nil
}

// getRightJoinCols extracts right join column names from config.
func (op *HashJoinOperator) getRightJoinCols() []string {
	var cols []string
	if len(op.config.RightConds) > 0 {
		for _, cond := range op.config.RightConds {
			if cond != nil && cond.Left != nil && cond.Left.Column != "" {
				cols = append(cols, cond.Left.Column)
			}
		}
		return cols
	}
	if op.config.RightCond != nil && op.config.RightCond.Left != nil && op.config.RightCond.Left.Column != "" {
		return []string{op.config.RightCond.Left.Column}
	}
	return nil
}

func (op *HashJoinOperator) executeInnerJoin(leftResult *domain.QueryResult, hashTable map[string][]domain.Row, leftJoinCols []string, hasCondition bool, rightResult *domain.QueryResult, totalCols int) []domain.Row {
	rows := make([]domain.Row, 0, len(leftResult.Rows))
	if hasCondition {
		for _, leftRow := range leftResult.Rows {
			key := multiHashKey(leftRow, leftJoinCols)
			if matchedRows, ok := hashTable[key]; ok {
				for _, rightRow := range matchedRows {
					rows = append(rows, mergeRowPair(leftRow, rightRow, totalCols))
				}
			}
		}
	} else {
		rows = op.executeCrossJoin(leftResult, rightResult, totalCols)
	}
	return rows
}

func (op *HashJoinOperator) executeLeftJoin(leftResult, rightResult *domain.QueryResult, hashTable map[string][]domain.Row, leftJoinCols []string, hasCondition bool, totalCols int) []domain.Row {
	rows := make([]domain.Row, 0, len(leftResult.Rows))
	if hasCondition {
		for _, leftRow := range leftResult.Rows {
			key := multiHashKey(leftRow, leftJoinCols)
			if matchedRows, ok := hashTable[key]; ok {
				for _, rightRow := range matchedRows {
					rows = append(rows, mergeRowPair(leftRow, rightRow, totalCols))
				}
			} else {
				rows = append(rows, mergeRowWithNullRight(leftRow, rightResult.Columns))
			}
		}
	} else {
		for _, leftRow := range leftResult.Rows {
			for _, rightRow := range rightResult.Rows {
				rows = append(rows, mergeRowPair(leftRow, rightRow, totalCols))
			}
		}
	}
	return rows
}

func (op *HashJoinOperator) executeRightJoin(leftResult, rightResult *domain.QueryResult, hashTable map[string][]domain.Row, rightJoinCols, leftJoinCols []string, hasCondition bool, totalCols int) []domain.Row {
	rows := make([]domain.Row, 0, len(rightResult.Rows))
	if hasCondition {
		leftHashTable := make(map[string][]domain.Row)
		for _, leftRow := range leftResult.Rows {
			key := multiHashKey(leftRow, leftJoinCols)
			leftHashTable[key] = append(leftHashTable[key], leftRow)
		}
		for _, rightRow := range rightResult.Rows {
			key := multiHashKey(rightRow, rightJoinCols)
			if matchedRows, ok := leftHashTable[key]; ok {
				for _, leftRow := range matchedRows {
					rows = append(rows, mergeRowPair(leftRow, rightRow, totalCols))
				}
			} else {
				rows = append(rows, mergeRowWithNullLeft(rightRow, leftResult.Columns))
			}
		}
	} else {
		for _, leftRow := range leftResult.Rows {
			for _, rightRow := range rightResult.Rows {
				rows = append(rows, mergeRowPair(leftRow, rightRow, totalCols))
			}
		}
	}
	return rows
}

func (op *HashJoinOperator) executeFullOuterJoin(leftResult, rightResult *domain.QueryResult, hashTable map[string][]domain.Row, leftJoinCols, rightJoinCols []string, hasCondition bool, totalCols int) []domain.Row {
	rows := make([]domain.Row, 0, len(leftResult.Rows)+len(rightResult.Rows))
	if !hasCondition {
		return op.executeCrossJoin(leftResult, rightResult, totalCols)
	}

	rightMatched := make(map[int]bool)

	for _, leftRow := range leftResult.Rows {
		key := multiHashKey(leftRow, leftJoinCols)
		if matchedRows, ok := hashTable[key]; ok {
			for _, rightRow := range matchedRows {
				rows = append(rows, mergeRowPair(leftRow, rightRow, totalCols))
			}
			for ri, rightRow := range rightResult.Rows {
				rkey := multiHashKey(rightRow, rightJoinCols)
				if rkey == key {
					rightMatched[ri] = true
				}
			}
		} else {
			rows = append(rows, mergeRowWithNullRight(leftRow, rightResult.Columns))
		}
	}

	for ri, rightRow := range rightResult.Rows {
		if !rightMatched[ri] {
			rows = append(rows, mergeRowWithNullLeft(rightRow, leftResult.Columns))
		}
	}

	return rows
}

func (op *HashJoinOperator) executeCrossJoin(leftResult, rightResult *domain.QueryResult, totalCols int) []domain.Row {
	rows := make([]domain.Row, 0, len(leftResult.Rows)*len(rightResult.Rows))
	for _, leftRow := range leftResult.Rows {
		for _, rightRow := range rightResult.Rows {
			rows = append(rows, mergeRowPair(leftRow, rightRow, totalCols))
		}
	}
	return rows
}

func (op *HashJoinOperator) executeSemiJoin(leftResult *domain.QueryResult, hashTable map[string][]domain.Row, leftJoinCols []string, hasCondition bool, rightResult *domain.QueryResult) []domain.Row {
	rows := make([]domain.Row, 0)
	if hasCondition {
		for _, leftRow := range leftResult.Rows {
			key := multiHashKey(leftRow, leftJoinCols)
			if _, ok := hashTable[key]; ok {
				rows = append(rows, leftRow)
			}
		}
	} else {
		if len(rightResult.Rows) > 0 {
			rows = append(rows, leftResult.Rows...)
		}
	}
	return rows
}

func (op *HashJoinOperator) executeAntiSemiJoin(leftResult *domain.QueryResult, hashTable map[string][]domain.Row, leftJoinCols []string, hasCondition bool, rightResult *domain.QueryResult) []domain.Row {
	rows := make([]domain.Row, 0)
	if hasCondition {
		for _, leftRow := range leftResult.Rows {
			key := multiHashKey(leftRow, leftJoinCols)
			if _, ok := hashTable[key]; !ok {
				rows = append(rows, leftRow)
			}
		}
	} else {
		if len(rightResult.Rows) == 0 {
			rows = append(rows, leftResult.Rows...)
		}
	}
	return rows
}

// mergeColumnInfos builds the output column list, handling conflicts and join type.
func mergeColumnInfos(leftCols, rightCols []domain.ColumnInfo, joinType types.JoinType) []domain.ColumnInfo {
	if joinType == types.SemiJoin || joinType == types.AntiSemiJoin {
		return leftCols
	}

	merged := make([]domain.ColumnInfo, 0, len(leftCols)+len(rightCols))
	merged = append(merged, leftCols...)
	leftColNames := make(map[string]bool, len(leftCols))
	for _, col := range leftCols {
		leftColNames[col.Name] = true
	}
	for _, col := range rightCols {
		if leftColNames[col.Name] {
			merged = append(merged, domain.ColumnInfo{
				Name:     "right_" + col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		} else {
			merged = append(merged, col)
		}
	}
	return merged
}

// buildOperator 构建算子的辅助函数
func buildOperator(p *plan.Plan, das dataaccess.Service) (Operator, error) {
	switch p.Type {
	case plan.TypeTableScan:
		return NewTableScanOperator(p, das)
	case plan.TypeSelection:
		return NewSelectionOperator(p, das)
	case plan.TypeProjection:
		return NewProjectionOperator(p, das)
	case plan.TypeLimit:
		return NewLimitOperator(p, das)
	case plan.TypeAggregate:
		return NewAggregateOperator(p, das)
	case plan.TypeHashJoin:
		return NewHashJoinOperator(p, das)
	case plan.TypeSort:
		return NewSortOperator(p, das)
	case plan.TypeInsert:
		return NewInsertOperator(p, das)
	case plan.TypeUpdate:
		return NewUpdateOperator(p, das)
	case plan.TypeDelete:
		return NewDeleteOperator(p, das)
	case plan.TypeUnion:
		return NewUnionOperator(p, das)
	default:
		return nil, fmt.Errorf("unsupported plan type: %s", p.Type)
	}
}
