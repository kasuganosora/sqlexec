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
func (p *PhysicalMergeJoin) mergeRows(
	leftRows, rightRows []domain.Row,
	leftCol, rightCol string,
	joinType JoinType,
) []domain.Row {
	
	// 使用归并排序算法
	i, j := 0, 0
	leftCount := len(leftRows)
	rightCount := len(rightRows)

	output := make([]domain.Row, 0, leftCount+rightCount)

	switch joinType {
	case InnerJoin:
		// INNER JOIN: 只有两边都匹配的行
		for i < leftCount && j < rightCount {
			leftVal := leftRows[i][leftCol]
			rightVal := rightRows[j][rightCol]

			cmp := compareValuesForSort(leftVal, rightVal)
			if cmp < 0 {
				// 左值小，推进左指针
				i++
			} else if cmp > 0 {
				// 右值小，推进右指针
				j++
			} else {
				// 相等，合并行并推进两个指针
				output = append(output, p.mergeRow(leftRows[i], rightRows[j]))
				i++
				j++
			}
		}

	case LeftOuterJoin:
		// LEFT JOIN: 左表所有行，右表匹配的行
		// 构建右侧NULL行模板（用于无匹配时）
		rightNullRow := p.buildNullRow(rightRows, rightCol)
		for i < leftCount {
			leftRow := leftRows[i]
			leftVal := leftRow[leftCol]

			// 在右表中查找匹配
			matchFound := false
			for j < rightCount {
				rightVal := rightRows[j][rightCol]

				if compareValuesForSort(leftVal, rightVal) == 0 {
					output = append(output, p.mergeRow(leftRow, rightRows[j]))
					matchFound = true
					break
				} else if compareValuesForSort(leftVal, rightVal) < 0 {
					// 右表的值已经更大，不需要继续查找
					break
				}
				j++
			}

			if !matchFound {
				// 没有匹配，左行 + 右NULL
				output = append(output, p.mergeRowWithNull(leftRow, rightNullRow))
			}

			i++
		}

	case RightOuterJoin:
		// RIGHT JOIN: 右表所有行，左表匹配的行
		// 构建左侧NULL行模板（用于无匹配时）
		leftNullRow := p.buildNullRow(leftRows, leftCol)
		for j < rightCount {
			rightRow := rightRows[j]
			rightVal := rightRow[rightCol]

			// 在左表中查找匹配
			matchFound := false
			for i < leftCount {
				leftVal := leftRows[i][leftCol]

				if compareValuesForSort(leftVal, rightVal) == 0 {
					output = append(output, p.mergeRow(leftRows[i], rightRow))
					matchFound = true
					break
				} else if compareValuesForSort(leftVal, rightVal) < 0 {
					// 左表的值已经更大，不需要继续查找
					break
				}
				i++
			}

			if !matchFound {
				// 没有匹配，左NULL + 右行
				output = append(output, p.mergeRowWithNull(leftNullRow, rightRow))
			}

			j++
		}

	default:
		// 其他JOIN类型：默认为INNER JOIN
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
