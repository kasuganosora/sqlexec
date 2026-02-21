package optimizer

import (
	"context"
	"fmt"
	"maps"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/utils"
)

// PhysicalMergeJoin 物理归并连接
// 基于两路归并排序的连接算法，适合有序数据
type PhysicalMergeJoin struct {
	JoinType   JoinType
	Conditions  []*JoinCondition
	cost        float64
	children    []PhysicalPlan
}

// NewPhysicalMergeJoin 创建物理归并连接
func NewPhysicalMergeJoin(joinType JoinType, left, right PhysicalPlan, conditions []*JoinCondition) *PhysicalMergeJoin {
	leftRows := int64(1000) // 假设
	rightRows := int64(1000) // 假设

	// Merge Join 成本 = 合并两个有序序列
	// 时间复杂度: O(n + m)
	leftCost := left.Cost()
	rightCost := right.Cost()
	mergeCost := float64(leftRows+rightRows) * 0.05
	cost := leftCost + rightCost + mergeCost

	return &PhysicalMergeJoin{
		JoinType:  joinType,
		Conditions:  conditions,
		cost:       cost,
		children:    []PhysicalPlan{left, right},
	}
}

// Children 获取子节点
func (p *PhysicalMergeJoin) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalMergeJoin) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalMergeJoin) Schema() []ColumnInfo {
	columns := []ColumnInfo{}
	if len(p.children) > 0 {
		columns = append(columns, p.children[0].Schema()...)
	}
	if len(p.children) > 1 {
		columns = append(columns, p.children[1].Schema()...)
	}
	return columns
}

// Cost 返回执行成本
func (p *PhysicalMergeJoin) Cost() float64 {
	return p.cost
}

// Execute 执行归并连接
// DEPRECATED: 执行逻辑已迁移到 pkg/executor 包，此方法保留仅为兼容性
func (p *PhysicalMergeJoin) Execute(ctx context.Context) (*domain.QueryResult, error) {
	return nil, fmt.Errorf("PhysicalMergeJoin.Execute is deprecated. Please use pkg/executor instead")
}

// sortByColumn 按指定列排序行数据
func (p *PhysicalMergeJoin) sortByColumn(rows []domain.Row, column string) []domain.Row {
	// 使用稳定的排序算法
	sorted := make([]domain.Row, len(rows))
	copy(sorted, rows)

	// 简单冒泡排序（实际应该用更高效的算法）
	for i := 0; i < len(sorted); i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			leftVal := sorted[j][column]
			rightVal := sorted[j+1][column]

			if compareValuesForSort(leftVal, rightVal) > 0 {
				// 交换
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	return sorted
}

// mergeRows 使用两路归并算法合并已排序的行
// 正确处理重复键：对于匹配的键组产生笛卡尔积
func (p *PhysicalMergeJoin) mergeRows(
	leftRows, rightRows []domain.Row,
	leftCol, rightCol string,
	joinType JoinType,
) []domain.Row {
	i, j := 0, 0
	leftCount := len(leftRows)
	rightCount := len(rightRows)

	output := make([]domain.Row, 0, leftCount+rightCount)

	switch joinType {
	case InnerJoin:
		for i < leftCount && j < rightCount {
			leftVal := leftRows[i][leftCol]
			rightVal := rightRows[j][rightCol]

			cmp := compareValuesForSort(leftVal, rightVal)
			if cmp < 0 {
				i++
			} else if cmp > 0 {
				j++
			} else {
				// 找到匹配：确定右侧重复组的范围
				jStart := j
				for j < rightCount && compareValuesForSort(leftRows[i][leftCol], rightRows[j][rightCol]) == 0 {
					j++
				}
				// 对左侧每一行与右侧整组产生笛卡尔积
				for i < leftCount && compareValuesForSort(leftRows[i][leftCol], rightRows[jStart][rightCol]) == 0 {
					for k := jStart; k < j; k++ {
						output = append(output, p.mergeRow(leftRows[i], rightRows[k]))
					}
					i++
				}
			}
		}

	case LeftOuterJoin:
		rightNullRow := p.buildNullRow(rightRows, rightCol)
		for i < leftCount {
			leftVal := leftRows[i][leftCol]

			// 推进右指针到 >= leftVal
			for j < rightCount && compareValuesForSort(rightRows[j][rightCol], leftVal) < 0 {
				j++
			}

			// 检查是否有匹配
			matchFound := false
			for k := j; k < rightCount && compareValuesForSort(rightRows[k][rightCol], leftVal) == 0; k++ {
				output = append(output, p.mergeRow(leftRows[i], rightRows[k]))
				matchFound = true
			}

			if !matchFound {
				output = append(output, p.mergeRowWithNull(leftRows[i], rightNullRow))
			}

			i++
		}

	case RightOuterJoin:
		leftNullRow := p.buildNullRow(leftRows, leftCol)
		for j < rightCount {
			rightVal := rightRows[j][rightCol]

			// 推进左指针到 >= rightVal
			for i < leftCount && compareValuesForSort(leftRows[i][leftCol], rightVal) < 0 {
				i++
			}

			// 检查是否有匹配
			matchFound := false
			for k := i; k < leftCount && compareValuesForSort(leftRows[k][leftCol], rightVal) == 0; k++ {
				output = append(output, p.mergeRow(leftRows[k], rightRows[j]))
				matchFound = true
			}

			if !matchFound {
				output = append(output, p.mergeRowWithNull(leftNullRow, rightRows[j]))
			}

			j++
		}

	default:
		return p.mergeRows(leftRows, rightRows, leftCol, rightCol, InnerJoin)
	}

	return output
}

// mergeRow 合并两行数据
func (p *PhysicalMergeJoin) mergeRow(left, right domain.Row) domain.Row {
	merged := make(domain.Row)

	// 添加左行数据
	maps.Copy(merged, left)

	// 添加右行数据
	for k, v := range right {
		// 检查列名冲突
		if _, exists := merged[k]; exists {
			merged["right_"+k] = v
		} else {
			merged[k] = v
		}
	}

	return merged
}

// buildNullRow 构建一个NULL行模板，用于LEFT/RIGHT JOIN无匹配时
// 如果rows非空，使用第一行的列名作为模板；否则使用Schema获取列信息
func (p *PhysicalMergeJoin) buildNullRow(rows []domain.Row, col string) domain.Row {
	nullRow := make(domain.Row)
	if len(rows) > 0 {
		for k := range rows[0] {
			nullRow[k] = nil
		}
	}
	return nullRow
}

// mergeRowWithNull 合并行数据，一边为NULL
func (p *PhysicalMergeJoin) mergeRowWithNull(notNull, nullRow domain.Row) domain.Row {
	merged := make(domain.Row)

	// 添加非NULL行的数据
	maps.Copy(merged, notNull)

	// 添加NULL行的数据（全部为NULL）
	for k := range nullRow {
		if _, exists := merged[k]; !exists {
			merged[k] = nil
		}
	}

	return merged
}

// getJoinColumns 从连接条件中获取列名
func getJoinColumns(conditions []*JoinCondition) (string, string) {
	if len(conditions) == 0 {
		return "", ""
	}

	// 简化：取第一个条件的字符串表示
	if conditions[0].Left != nil {
		leftStr := fmt.Sprintf("%v", conditions[0].Left)
		if conditions[0].Right != nil {
			rightStr := fmt.Sprintf("%v", conditions[0].Right)
			return leftStr, rightStr
		}
		return leftStr, ""
	}
	return "", ""
}

// compareValuesForSort 为归并排序比较两个值
// 返回 -1: a < b, 0: a == b, 1: a > b
func compareValuesForSort(a, b interface{}) int {
	return utils.CompareValuesForSort(a, b)
}

// Explain 返回计划说明
func (p *PhysicalMergeJoin) Explain() string {
	return fmt.Sprintf("MergeJoin(type=%s, cost=%.2f)", p.JoinType, p.cost)
}
