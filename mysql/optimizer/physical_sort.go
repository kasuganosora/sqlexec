package optimizer

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"

	"mysql-proxy/mysql/resource"
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
func (p *PhysicalSort) Execute(ctx context.Context) (*resource.QueryResult, error) {
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
	rows := make([]resource.Row, len(input.Rows))
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
				if item.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		// 所有排序列都相等，保持原顺序
		return i < j
	})

	return &resource.QueryResult{
		Columns: input.Columns,
		Rows:    rows,
		Total:   input.Total,
	}, nil
}

// compareValues 比较两个值
// 返回 -1: a < b, 0: a == b, 1: a > b
func compareValues(a, b interface{}) int {
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
	aNum, aOk := toNumber(a)
	bNum, bOk := toNumber(b)
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

// toNumber 尝试将值转换为float64
func toNumber(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int, int8, int16, int32, int64:
		return float64(reflect.ValueOf(v).Int()), true
	case uint, uint8, uint16, uint32, uint64:
		return float64(reflect.ValueOf(v).Uint()), true
	case float32, float64:
		return reflect.ValueOf(v).Float(), true
	default:
		return 0, false
	}
}

// Explain 返回计划说明
func (p *PhysicalSort) Explain() string {
	items := ""
	for i, item := range p.OrderByItems {
		if i > 0 {
			items += ", "
		}
		direction := "ASC"
		if item.Desc {
			direction = "DESC"
		}
		items += fmt.Sprintf("%s %s", item.Column, direction)
	}
	return fmt.Sprintf("Sort(%s, cost=%.2f)", items, p.cost)
}
