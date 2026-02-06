package optimizer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// PhysicalSort 物理排序
type PhysicalSort struct {
	OrderByItems []*OrderByItem
	cost         float64
	children     []PhysicalPlan
}

// NewPhysicalSort 创建物理排序
func NewPhysicalSort(orderByItems []*OrderByItem, child PhysicalPlan) *PhysicalSort {
	inputCost := child.Cost()
	// 排序成本估算：n * log(n)，n是输入行数
	// 假设1000行
	inputRows := int64(1000)
	sortCost := float64(inputRows) * float64(log2(float64(inputRows))) * 0.01
	cost := inputCost + sortCost

	return &PhysicalSort{
		OrderByItems: orderByItems,
		cost:         cost,
		children:     []PhysicalPlan{child},
	}
}

// log2 计算以2为底的对数
func log2(x float64) float64 {
	if x <= 0 {
		return 0
	}
	return math.Log2(x)
}

// Children 获取子节点
func (p *PhysicalSort) Children() []PhysicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *PhysicalSort) SetChildren(children ...PhysicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *PhysicalSort) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// Cost 返回执行成本
func (p *PhysicalSort) Cost() float64 {
	return p.cost
}

// Execute 执行排序
func (p *PhysicalSort) Execute(ctx context.Context) (*domain.QueryResult, error) {
	if len(p.children) == 0 {
		return nil, fmt.Errorf("PhysicalSort has no child")
	}

	// 执行子节点
	input, err := p.children[0].Execute(ctx)
	if err != nil {
		return nil, err
	}

	if len(p.OrderByItems) == 0 {
		// 没有排序条件，直接返回
		return input, nil
	}

	// 复制行以避免修改原始数据
	rows := make([]domain.Row, len(input.Rows))
	copy(rows, input.Rows)

	// 排序
	sort.Slice(rows, func(i, j int) bool {
		for _, item := range p.OrderByItems {
			leftVal := rows[i][item.Column]
			rightVal := rows[j][item.Column]

			// 比较两个值
			cmp := compareValues(leftVal, rightVal)
			if cmp != 0 {
				// DESC 需要反转比较结果
				if item.Direction == "DESC" {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		// 所有排序列都相等，保持原顺序
		return i < j
	})

	return &domain.QueryResult{
		Columns: input.Columns,
		Rows:    rows,
		Total:   input.Total,
	}, nil
}

// Explain 返回计划说明
func (p *PhysicalSort) Explain() string {
	var items strings.Builder
	for i, item := range p.OrderByItems {
		if i > 0 {
			items.WriteString(", ")
		}
		direction := "ASC"
		if item.Direction == "DESC" {
			direction = "DESC"
		}
		items.WriteString(fmt.Sprintf("%s %s", item.Column, direction))
	}
	return fmt.Sprintf("Sort(%s, cost=%.2f)", items.String(), p.cost)
}
