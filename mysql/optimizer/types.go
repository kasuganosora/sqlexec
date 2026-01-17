package optimizer

import (
	"context"
	"fmt"

	"mysql-proxy/mysql/parser"
	"mysql-proxy/mysql/resource"
)

// LogicalPlan 逻辑计划接口
type LogicalPlan interface {
	// Children 获取子节点
	Children() []LogicalPlan

	// SetChildren 设置子节点
	SetChildren(children ...LogicalPlan)

	// Schema 返回输出列
	Schema() []ColumnInfo

	// Explain 返回计划说明
	Explain() string
}

// PhysicalPlan 物理计划接口
type PhysicalPlan interface {
	// Children 获取子节点
	Children() []PhysicalPlan

	// SetChildren 设置子节点
	SetChildren(children ...PhysicalPlan)

	// Schema 返回输出列
	Schema() []ColumnInfo

	// Cost 返回执行成本
	Cost() float64

	// Execute 执行计划
	Execute(ctx context.Context) (*resource.QueryResult, error)

	// Explain 返回计划说明
	Explain() string
}

// ColumnInfo 列信息
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	// 可以扩展更多字段
}

// JoinType 连接类型
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftOuterJoin
	RightOuterJoin
	FullOuterJoin
)

// String 返回 JoinType 的字符串表示
func (jt JoinType) String() string {
	switch jt {
	case InnerJoin:
		return "INNER JOIN"
	case LeftOuterJoin:
		return "LEFT OUTER JOIN"
	case RightOuterJoin:
		return "RIGHT OUTER JOIN"
	case FullOuterJoin:
		return "FULL OUTER JOIN"
	default:
		return "UNKNOWN"
	}
}

// AggregationType 聚合函数类型
type AggregationType int

const (
	Count AggregationType = iota
	Sum
	Avg
	Max
	Min
)

// String 返回 AggregationType 的字符串表示
func (at AggregationType) String() string {
	switch at {
	case Count:
		return "COUNT"
	case Sum:
		return "SUM"
	case Avg:
		return "AVG"
	case Max:
		return "MAX"
	case Min:
		return "MIN"
	default:
		return "UNKNOWN"
	}
}

// AggregationItem 聚合项
type AggregationItem struct {
	Type     AggregationType
	Expr     *parser.Expression
	Alias    string
	Distinct bool
}

// JoinCondition 连接条件
type JoinCondition struct {
	Left    *parser.Expression
	Right   *parser.Expression
	Operator string
}

// Statistics 统计信息（简化版）
type Statistics struct {
	RowCount   int64
	UniqueKeys int64
	NullCount  int64
}

// OptimizationContext 优化上下文
type OptimizationContext struct {
	DataSource resource.DataSource
	TableInfo map[string]*resource.TableInfo
	Stats     map[string]*Statistics
	CostModel CostModel
}

// CostModel 成本模型
type CostModel interface {
	// ScanCost 计算扫描成本
	ScanCost(tableName string, rowCount int64) float64

	// FilterCost 计算过滤成本
	FilterCost(inputRows int64, selectivity float64) float64

	// JoinCost 计算连接成本
	JoinCost(leftRows, rightRows int64, joinType JoinType) float64

	// AggregateCost 计算聚合成本
	AggregateCost(inputRows int64, groupByCols int) float64

	// ProjectCost 计算投影成本
	ProjectCost(inputRows int64, projCols int) float64
}

// DefaultCostModel 默认成本模型
type DefaultCostModel struct {
	CPUFactor     float64
	IoFactor      float64
	MemoryFactor   float64
}

// NewDefaultCostModel 创建默认成本模型
func NewDefaultCostModel() *DefaultCostModel {
	return &DefaultCostModel{
		CPUFactor:   0.01,
		IoFactor:    0.1,
		MemoryFactor: 0.001,
	}
}

// ScanCost 计算扫描成本
func (cm *DefaultCostModel) ScanCost(tableName string, rowCount int64) float64 {
	// 成本 = IO 读取 + CPU 处理
	return float64(rowCount)*cm.IoFactor + float64(rowCount)*cm.CPUFactor
}

// FilterCost 计算过滤成本
func (cm *DefaultCostModel) FilterCost(inputRows int64, selectivity float64) float64 {
	// 成本 = 读取所有行 + 比较成本
	outputRows := float64(inputRows) * selectivity
	return float64(inputRows)*cm.CPUFactor + outputRows
}

// JoinCost 计算连接成本
func (cm *DefaultCostModel) JoinCost(leftRows, rightRows int64, joinType JoinType) float64 {
	// 假设使用 hash join
	// 成本 = 构建 hash + 探测 hash
	buildCost := float64(leftRows) * cm.CPUFactor
	probeCost := float64(rightRows) * cm.CPUFactor
	memoryCost := float64(leftRows) * cm.MemoryFactor
	return buildCost + probeCost + memoryCost
}

// AggregateCost 计算聚合成本
func (cm *DefaultCostModel) AggregateCost(inputRows int64, groupByCols int) float64 {
	// 成本 = 分组 + 聚合
	groupCost := float64(inputRows) * cm.CPUFactor * float64(groupByCols)
	aggCost := float64(inputRows) * cm.CPUFactor
	return groupCost + aggCost
}

// ProjectCost 计算投影成本
func (cm *DefaultCostModel) ProjectCost(inputRows int64, projCols int) float64 {
	// 成本 = 计算每个表达式
	return float64(inputRows) * float64(projCols) * cm.CPUFactor
}

// OptimizationRule 优化规则接口
type OptimizationRule interface {
	// Name 规则名称
	Name() string

	// Match 检查规则是否匹配
	Match(plan LogicalPlan) bool

	// Apply 应用规则，返回优化后的计划
	Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error)
}

// RuleSet 规则集合
type RuleSet []OptimizationRule

// Apply 应用所有规则
func (rs RuleSet) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	current := plan
	changed := true

	// 迭代应用规则，直到不再变化
	for changed {
		changed = false
		for _, rule := range rs {
			if rule.Match(current) {
				newPlan, err := rule.Apply(ctx, current, optCtx)
				if err != nil {
					return nil, fmt.Errorf("rule %s failed: %w", rule.Name(), err)
				}
				if newPlan != nil {
					current = newPlan
					changed = true
				}
			}
		}
	}

	return current, nil
}
