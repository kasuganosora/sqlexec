package operators

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
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

// Execute 执行Hash Join
func (op *HashJoinOperator) Execute(ctx context.Context) (*domain.QueryResult, error) {
	// 获取左右子算子
	if len(op.children) < 2 {
		return nil, fmt.Errorf("HashJoin requires 2 children, got %d", len(op.children))
	}

	leftChild := op.children[0]
	rightChild := op.children[1]

	// 执行子算子
	leftResult, err := leftChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute left child failed: %w", err)
	}

	rightResult, err := rightChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("execute right child failed: %w", err)
	}

	// Build hash table from right side
	hashTable := make(map[string][]domain.Row)
	joinCol := ""
	if op.config.LeftCond != nil && op.config.LeftCond.Left != nil {
		joinCol = op.config.LeftCond.Left.Column
	}
	rightJoinCol := ""
	if op.config.RightCond != nil && op.config.RightCond.Left != nil {
		rightJoinCol = op.config.RightCond.Left.Column
	}

	if joinCol != "" && rightJoinCol != "" {
		// Hash join with condition
		for _, rightRow := range rightResult.Rows {
			key := fmt.Sprintf("%T:%v", rightRow[rightJoinCol], rightRow[rightJoinCol])
			hashTable[key] = append(hashTable[key], rightRow)
		}
	}

	totalCols := len(leftResult.Columns) + len(rightResult.Columns)
	joinedRows := make([]domain.Row, 0, len(leftResult.Rows))
	if joinCol != "" && rightJoinCol != "" {
		// Probe hash table with left side
		for _, leftRow := range leftResult.Rows {
			key := fmt.Sprintf("%T:%v", leftRow[joinCol], leftRow[joinCol])
			if matchedRows, ok := hashTable[key]; ok {
				for _, rightRow := range matchedRows {
					merged := make(domain.Row, totalCols)
					for k, v := range leftRow {
						merged[k] = v
					}
					for k, v := range rightRow {
						if _, exists := merged[k]; exists {
							merged["right_"+k] = v
						} else {
							merged[k] = v
						}
					}
					joinedRows = append(joinedRows, merged)
				}
			}
		}
	} else {
		// Fallback: cross join when no condition
		for _, leftRow := range leftResult.Rows {
			for _, rightRow := range rightResult.Rows {
				merged := make(domain.Row, totalCols)
				for k, v := range leftRow {
					merged[k] = v
				}
				for k, v := range rightRow {
					if _, exists := merged[k]; exists {
						merged["right_"+k] = v
					} else {
						merged[k] = v
					}
				}
				joinedRows = append(joinedRows, merged)
			}
		}
	}

	// Merge columns from both sides
	mergedColumns := make([]domain.ColumnInfo, 0, len(leftResult.Columns)+len(rightResult.Columns))
	mergedColumns = append(mergedColumns, leftResult.Columns...)
	leftColNames := make(map[string]bool)
	for _, col := range leftResult.Columns {
		leftColNames[col.Name] = true
	}
	for _, col := range rightResult.Columns {
		if leftColNames[col.Name] {
			mergedColumns = append(mergedColumns, domain.ColumnInfo{
				Name:     "right_" + col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		} else {
			mergedColumns = append(mergedColumns, col)
		}
	}

	return &domain.QueryResult{
		Columns: mergedColumns,
		Rows:    joinedRows,
	}, nil
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
