package optimizer

// LogicalUnion 逻辑联合
type LogicalUnion struct {
	children  []LogicalPlan
	unionType string
	all       bool
}

// NewLogicalUnion 创建逻辑联合
func NewLogicalUnion(children ...LogicalPlan) *LogicalUnion {
	return &LogicalUnion{
		children:  children,
		unionType: "UNION",
		all:       false,
	}
}

// Children 获取子节点
func (p *LogicalUnion) Children() []LogicalPlan {
	return p.children
}

// SetChildren 设置子节点
func (p *LogicalUnion) SetChildren(children ...LogicalPlan) {
	p.children = children
}

// Schema 返回输出列
func (p *LogicalUnion) Schema() []ColumnInfo {
	if len(p.children) > 0 {
		return p.children[0].Schema()
	}
	return []ColumnInfo{}
}

// GetUnionType 返回UNION类型
func (p *LogicalUnion) GetUnionType() string {
	return p.unionType
}

// GetAll 返回是否包含重复行
func (p *LogicalUnion) GetAll() bool {
	return p.all
}

// Explain 返回计划说明
func (p *LogicalUnion) Explain() string {
	return "Union"
}
