package optimizer

import (
	"context"
	"fmt"

	"mysql-proxy/mysql/resource"
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
func (p *PhysicalMergeJoin) Execute(ctx context.Context) (*resource.QueryResult, error) {
	if len(p.children) != 2 {
		return nil, fmt.Errorf("MergeJoin requires exactly 2 children")
	}

	// 1. 执行左表和右表
	leftResult, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("left table execute error: %w", err)
	}

	rightResult, err := p.children[1].Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("right table execute error: %w", err)
	}

	// 2. 获取连接条件
	leftJoinCol, rightJoinCol := getJoinColumns(p.Conditions)
	if leftJoinCol == "" || rightJoinCol == "" {
		return nil, fmt.Errorf("invalid join conditions")
	}

	// 3. 对两边数据进行排序（如果是有序数据可以跳过这一步）
	leftRows := p.sortByColumn(leftResult.Rows, leftJoinCol)
	rightRows := p.sortByColumn(rightResult.Rows, rightJoinCol)

	// 4. 执行两路归并
	output := p.mergeRows(leftRows, rightRows, leftJoinCol, rightJoinCol, p.JoinType)

	// 5. 合并列信息
	columns := []resource.ColumnInfo{}
	columns = append(columns, leftResult.Columns...)
	for _, col := range rightResult.Columns {
		// 检查列名冲突
		conflict := false
		for _, leftCol := range leftResult.Columns {
			if leftCol.Name == col.Name {
				conflict = true
				break
			}
		}
		if conflict {
			columns = append(columns, resource.ColumnInfo{
				Name:     "right_" + col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		} else {
			columns = append(columns, col)
		}
	}

	return &resource.QueryResult{
		Columns: columns,
		Rows:    output,
		Total:    int64(len(output)),
	}, nil
}

// sortByColumn 按指定列排序行数据
func (p *PhysicalMergeJoin) sortByColumn(rows []resource.Row, column string) []resource.Row {
	// 使用稳定的排序算法
	sorted := make([]resource.Row, len(rows))
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
	leftRows, rightRows []resource.Row,
	leftCol, rightCol string,
	joinType JoinType,
) []resource.Row {
	
	// 使用归并排序算法
	i, j := 0, 0
	leftCount := len(leftRows)
	rightCount := len(rightRows)

	output := make([]resource.Row, 0, leftCount+rightCount)

	switch joinType {
	case JoinTypeInner:
		// INNER JOIN: 只有两边都有的行
		for i < leftCount && j < rightCount {
			leftVal := leftRows[i][leftCol]
			rightVal := rightRows[j][rightCol]

			cmp := compareValuesForSort(leftVal, rightVal)
			if cmp < 0 {
				// 左值小，取左行
				output = append(output, p.mergeRow(leftRows[i], rightRows[j]))
				i++
			} else if cmp > 0 {
				// 右值小，取右行
				output = append(output, p.mergeRow(leftRows[i], rightRows[j]))
				j++
			} else {
				// 相等，合并行并推进两个指针
				output = append(output, p.mergeRow(leftRows[i], rightRows[j]))
				i++
				j++
			}
		}

	case JoinTypeLeft:
		// LEFT JOIN: 左表所有行，右表匹配的行
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
				output = append(output, p.mergeRowWithNull(leftRow, rightRows[0]))
			}

			i++
		}

	case JoinTypeRight:
		// RIGHT JOIN: 右表所有行，左表匹配的行
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
				output = append(output, p.mergeRowWithNull(leftRows[0], rightRow))
			}

			j++
		}

	default:
		// 其他JOIN类型：默认为INNER JOIN
		return p.mergeRows(leftRows, rightRows, leftCol, rightCol, JoinTypeInner)
	}

	return output
}

// mergeRow 合并两行数据
func (p *PhysicalMergeJoin) mergeRow(left, right resource.Row) resource.Row {
	merged := make(resource.Row)

	// 添加左行数据
	for k, v := range left {
		merged[k] = v
	}

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

// mergeRowWithNull 合并行数据，一边为NULL
func (p *PhysicalMergeJoin) mergeRowWithNull(notNull, nullRow resource.Row) resource.Row {
	merged := make(resource.Row)

	// 添加非NULL行的数据
	for k, v := range notNull {
		merged[k] = v
	}

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

	// 简化：取第一个条件
	return conditions[0].Left, conditions[0].Right
}

// compareValuesForSort 为归并排序比较两个值
// 返回 -1: a < b, 0: a == b, 1: a > b
func compareValuesForSort(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 尝试数值比较
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// 字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// Explain 返回计划说明
func (p *PhysicalMergeJoin) Explain() string {
	return fmt.Sprintf("MergeJoin(type=%s, cost=%.2f)", p.JoinType, p.cost)
}
