package optimizer

// LogicalSort 逻辑排序
type LogicalSort struct {
	OrderBy  []OrderByItem
	children []LogicalPlan
}

// NewLogicalSort 创建逻辑排序
func NewLogicalSort(orderBy []OrderByItem, child LogicalPlan) *LogicalSort {
	return &LogicalSort{
		OrderBy:  orderBy,
		children: []LogicalPlan{child},
	}
}

// Children 获取子节点
func (p *LogicalSort) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalSort) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalSort) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// GetOrderByItems 返回排序列表
func (p *LogicalSort) GetOrderByItems() []*OrderByItem {
	result := make([]*OrderByItem, 0, len(p.OrderBy))
	for i := range p.OrderBy {
		result = append(result, &p.OrderBy[i])
	}
	return result
}

// Explain 返回计划说明
func (p *LogicalSort) Explain() string {
	items := ""
	orderByItems := p.GetOrderByItems()
	for i, item := range orderByItems {
		if i > 0 {
			items += ", "
		}
		items += item.Column + " " + item.Direction
	}
	return "Sort(" + items + ")"
}
