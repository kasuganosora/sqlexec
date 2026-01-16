package datasource

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// PlanType 计划类型
type PlanType int

const (
	PlanTypeTableScan PlanType = iota
	PlanTypeIndexScan
	PlanTypeNestedLoopJoin
	PlanTypeHashJoin
	PlanTypeSort
	PlanTypeGroup
	PlanTypeFilter
	PlanTypeLimit
	PlanTypeSubquery
	PlanTypeExists
	PlanTypeIn
	PlanTypeUnion
	PlanTypeIntersect
	PlanTypeExcept
)

// PlanNode 计划节点
type PlanNode struct {
	Type       PlanType
	Table      string
	Fields     []string
	Conditions []Condition
	Children   []*PlanNode
	Cost       float64
	Rows       int64
	Time       time.Duration
	Subquery   *Query // 子查询
	UnionType  string // UNION类型：ALL/DISTINCT
	Alias      string // 子查询别名
}

// Planner 查询计划生成器
type Planner struct {
	configManager *ConfigManager
	stats         *QueryStats
}

// QueryStats 查询统计
type QueryStats struct {
	TotalTime     time.Duration
	ParseTime     time.Duration
	ValidateTime  time.Duration
	OptimizeTime  time.Duration
	PlanTime      time.Duration
	ExecuteTime   time.Duration
	RowsProcessed int64
	MemoryUsed    int64
	CacheHits     int64
	CacheMisses   int64
}

// NewPlanner 创建计划生成器
func NewPlanner(configManager *ConfigManager) *Planner {
	return &Planner{
		configManager: configManager,
		stats:         &QueryStats{},
	}
}

// GeneratePlan 生成查询计划
func (p *Planner) GeneratePlan(query *Query) (*PlanNode, error) {
	start := time.Now()
	defer func() {
		p.stats.PlanTime = time.Since(start)
	}()

	// 创建根节点
	root := &PlanNode{
		Type:  PlanTypeTableScan,
		Table: query.Table,
	}

	// 添加字段
	root.Fields = query.Fields

	// 处理子查询
	if query.Subquery != nil {
		subqueryNode, err := p.GeneratePlan(query.Subquery)
		if err != nil {
			return nil, err
		}
		subqueryNode.Type = PlanTypeSubquery
		subqueryNode.Alias = query.SubqueryAlias
		root.Children = append(root.Children, subqueryNode)
	}

	// 处理WHERE条件
	if len(query.Where) > 0 {
		filterNode := &PlanNode{
			Type:       PlanTypeFilter,
			Conditions: query.Where,
		}
		// 处理WHERE中的子查询
		for _, cond := range query.Where {
			if cond.Subquery != nil {
				subqueryNode, err := p.GeneratePlan(cond.Subquery)
				if err != nil {
					return nil, err
				}
				switch cond.Operator {
				case "EXISTS":
					subqueryNode.Type = PlanTypeExists
				case "IN":
					subqueryNode.Type = PlanTypeIn
				default:
					subqueryNode.Type = PlanTypeSubquery
				}
				filterNode.Children = append(filterNode.Children, subqueryNode)
			}
		}
		filterNode.Children = append(filterNode.Children, root)
		root = filterNode
	}

	// 处理JOIN
	for _, join := range query.Joins {
		joinNode := &PlanNode{
			Type:  PlanTypeNestedLoopJoin,
			Table: join.Table,
		}
		// 处理JOIN中的子查询
		if join.Subquery != nil {
			subqueryNode, err := p.GeneratePlan(join.Subquery)
			if err != nil {
				return nil, err
			}
			subqueryNode.Type = PlanTypeSubquery
			subqueryNode.Alias = join.SubqueryAlias
			joinNode.Children = append(joinNode.Children, subqueryNode)
		}
		joinNode.Children = append(joinNode.Children, root)
		root = joinNode
	}

	// 处理UNION
	if query.Union != nil {
		unionNode := &PlanNode{
			Type:      PlanTypeUnion,
			UnionType: query.UnionType,
		}
		unionQuery, err := p.GeneratePlan(query.Union)
		if err != nil {
			return nil, err
		}
		unionNode.Children = append(unionNode.Children, root, unionQuery)
		root = unionNode
	}

	// 处理INTERSECT
	if query.Intersect != nil {
		intersectNode := &PlanNode{
			Type: PlanTypeIntersect,
		}
		intersectQuery, err := p.GeneratePlan(query.Intersect)
		if err != nil {
			return nil, err
		}
		intersectNode.Children = append(intersectNode.Children, root, intersectQuery)
		root = intersectNode
	}

	// 处理EXCEPT
	if query.Except != nil {
		exceptNode := &PlanNode{
			Type: PlanTypeExcept,
		}
		exceptQuery, err := p.GeneratePlan(query.Except)
		if err != nil {
			return nil, err
		}
		exceptNode.Children = append(exceptNode.Children, root, exceptQuery)
		root = exceptNode
	}

	// 处理GROUP BY
	if len(query.GroupBy) > 0 {
		groupNode := &PlanNode{
			Type:   PlanTypeGroup,
			Fields: query.GroupBy,
		}
		groupNode.Children = append(groupNode.Children, root)
		root = groupNode
	}

	// 处理HAVING
	if len(query.Having) > 0 {
		havingNode := &PlanNode{
			Type:       PlanTypeFilter,
			Conditions: query.Having,
		}
		// 处理HAVING中的子查询
		for _, cond := range query.Having {
			if cond.Subquery != nil {
				subqueryNode, err := p.GeneratePlan(cond.Subquery)
				if err != nil {
					return nil, err
				}
				subqueryNode.Type = PlanTypeSubquery
				havingNode.Children = append(havingNode.Children, subqueryNode)
			}
		}
		havingNode.Children = append(havingNode.Children, root)
		root = havingNode
	}

	// 处理ORDER BY
	if len(query.OrderBy) > 0 {
		sortNode := &PlanNode{
			Type: PlanTypeSort,
		}
		sortNode.Children = append(sortNode.Children, root)
		root = sortNode
	}

	// 处理LIMIT
	if query.Limit > 0 {
		limitNode := &PlanNode{
			Type: PlanTypeLimit,
		}
		limitNode.Children = append(limitNode.Children, root)
		root = limitNode
	}

	// 估算成本
	p.estimateCost(root)

	return root, nil
}

// estimateCost 估算计划成本
func (p *Planner) estimateCost(node *PlanNode) {
	switch node.Type {
	case PlanTypeTableScan:
		// 表扫描成本
		tableConfig, ok := p.configManager.GetTable("test", node.Table)
		if ok {
			node.Rows = int64(tableConfig.RowCount)
			node.Cost = float64(node.Rows) * 0.1 // 假设每行扫描成本为0.1
		}
	case PlanTypeIndexScan:
		// 索引扫描成本
		node.Cost = float64(node.Rows) * 0.05 // 假设每行索引扫描成本为0.05
	case PlanTypeNestedLoopJoin:
		// 嵌套循环连接成本
		if len(node.Children) == 2 {
			left := node.Children[0]
			right := node.Children[1]
			node.Rows = left.Rows * right.Rows
			node.Cost = left.Cost + right.Cost + float64(node.Rows)*0.2
		}
	case PlanTypeHashJoin:
		// 哈希连接成本
		if len(node.Children) == 2 {
			left := node.Children[0]
			right := node.Children[1]
			node.Rows = left.Rows + right.Rows
			node.Cost = left.Cost + right.Cost + float64(node.Rows)*0.1
		}
	case PlanTypeSort:
		// 排序成本
		if len(node.Children) > 0 {
			child := node.Children[0]
			node.Rows = child.Rows
			node.Cost = child.Cost + float64(node.Rows)*math.Log2(float64(node.Rows))*0.1
		}
	case PlanTypeGroup:
		// 分组成本
		if len(node.Children) > 0 {
			child := node.Children[0]
			node.Rows = child.Rows / 10 // 假设分组后行数减少90%
			node.Cost = child.Cost + float64(child.Rows)*0.15
		}
	case PlanTypeFilter:
		// 过滤成本
		if len(node.Children) > 0 {
			child := node.Children[0]
			node.Rows = child.Rows / 2 // 假设过滤后行数减少50%
			node.Cost = child.Cost + float64(child.Rows)*0.05
		}
	case PlanTypeLimit:
		// LIMIT成本
		if len(node.Children) > 0 {
			child := node.Children[0]
			node.Rows = child.Rows
			node.Cost = child.Cost
		}
	case PlanTypeSubquery:
		// 子查询成本
		if len(node.Children) > 0 {
			child := node.Children[0]
			node.Rows = child.Rows
			node.Cost = child.Cost * 1.1 // 子查询额外开销10%
		}
	case PlanTypeExists:
		// EXISTS子查询成本
		if len(node.Children) > 0 {
			child := node.Children[0]
			node.Rows = 1                // EXISTS只返回布尔值
			node.Cost = child.Cost * 0.5 // EXISTS可以提前终止
		}
	case PlanTypeIn:
		// IN子查询成本
		if len(node.Children) > 0 {
			child := node.Children[0]
			node.Rows = child.Rows
			node.Cost = child.Cost * 1.2 // IN子查询需要构建哈希表
		}
	case PlanTypeUnion:
		// UNION成本
		if len(node.Children) == 2 {
			left := node.Children[0]
			right := node.Children[1]
			if node.UnionType == "DISTINCT" {
				node.Rows = left.Rows + right.Rows
				node.Cost = left.Cost + right.Cost + float64(node.Rows)*0.2 // 去重开销
			} else {
				node.Rows = left.Rows + right.Rows
				node.Cost = left.Cost + right.Cost
			}
		}
	case PlanTypeIntersect:
		// INTERSECT成本
		if len(node.Children) == 2 {
			left := node.Children[0]
			right := node.Children[1]
			node.Rows = int64Min(left.Rows, right.Rows)
			node.Cost = left.Cost + right.Cost + float64(left.Rows+right.Rows)*0.3
		}
	case PlanTypeExcept:
		// EXCEPT成本
		if len(node.Children) == 2 {
			left := node.Children[0]
			right := node.Children[1]
			node.Rows = left.Rows
			node.Cost = left.Cost + right.Cost + float64(left.Rows+right.Rows)*0.3
		}
	}

	// 递归估算子节点成本
	for _, child := range node.Children {
		p.estimateCost(child)
	}
}

// GetStats 获取查询统计
func (p *Planner) GetStats() *QueryStats {
	return p.stats
}

// ResetStats 重置统计
func (p *Planner) ResetStats() {
	p.stats = &QueryStats{}
}

// UpdateStats 更新统计
func (p *Planner) UpdateStats(stats *QueryStats) {
	p.stats = stats
}

// String 返回计划的可读表示
func (p *PlanNode) String() string {
	var result string
	switch p.Type {
	case PlanTypeTableScan:
		result = fmt.Sprintf("TableScan(%s)", p.Table)
	case PlanTypeIndexScan:
		result = fmt.Sprintf("IndexScan(%s)", p.Table)
	case PlanTypeNestedLoopJoin:
		result = fmt.Sprintf("NestedLoopJoin(%s)", p.Table)
	case PlanTypeHashJoin:
		result = fmt.Sprintf("HashJoin(%s)", p.Table)
	case PlanTypeSort:
		result = fmt.Sprintf("Sort(%v)", p.Fields)
	case PlanTypeGroup:
		result = fmt.Sprintf("Group(%v)", p.Fields)
	case PlanTypeFilter:
		result = fmt.Sprintf("Filter(%v)", p.Conditions)
	case PlanTypeLimit:
		result = "Limit"
	case PlanTypeSubquery:
		result = fmt.Sprintf("Subquery(%s)", p.Alias)
	case PlanTypeExists:
		result = "Exists"
	case PlanTypeIn:
		result = "In"
	case PlanTypeUnion:
		result = fmt.Sprintf("Union(%s)", p.UnionType)
	case PlanTypeIntersect:
		result = "Intersect"
	case PlanTypeExcept:
		result = "Except"
	}

	if len(p.Children) > 0 {
		result += " -> ["
		for i, child := range p.Children {
			if i > 0 {
				result += ", "
			}
			result += child.String()
		}
		result += "]"
	}

	result += fmt.Sprintf(" (cost=%.2f, rows=%d)", p.Cost, p.Rows)
	return result
}

func int64Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// optimizeOrderBy 优化 ORDER BY
func (p *Planner) optimizeOrderBy(orderBy []OrderBy) []OrderBy {
	if len(orderBy) == 0 {
		return nil
	}

	// 复制并优化 ORDER BY
	optimized := make([]OrderBy, len(orderBy))
	for i, order := range orderBy {
		// 移除字段名中的表名前缀
		field := order.Field
		if strings.HasSuffix(field, ".*") {
			field = field[:len(field)-2]
		}
		optimized[i] = OrderBy{
			Field:     field,
			Direction: order.Direction,
		}
	}
	return optimized
}

// Optimize 优化查询
func (p *Planner) Optimize(query *Query) *Query {
	optimized := &Query{
		Type:    query.Type,
		Table:   query.Table,
		Fields:  make([]string, len(query.Fields)),
		Joins:   make([]Join, len(query.Joins)),
		Where:   make([]Condition, len(query.Where)),
		GroupBy: make([]string, len(query.GroupBy)),
		Having:  make([]Condition, len(query.Having)),
		OrderBy: make([]OrderBy, len(query.OrderBy)),
		Limit:   query.Limit,
		Offset:  query.Offset,
	}

	// 复制字段
	copy(optimized.Fields, query.Fields)
	copy(optimized.Joins, query.Joins)
	copy(optimized.Where, query.Where)
	copy(optimized.GroupBy, query.GroupBy)
	copy(optimized.Having, query.Having)
	copy(optimized.OrderBy, query.OrderBy)

	// 优化 ORDER BY
	if len(query.OrderBy) > 0 {
		optimized.OrderBy = p.optimizeOrderBy(query.OrderBy)
	}

	return optimized
}
