package optimizer

import (
	"context"
	"fmt"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
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
	CrossJoin
	SemiJoin
	AntiSemiJoin
	HashJoin // Hash join algorithm
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
	case CrossJoin:
		return "CROSS JOIN"
	case SemiJoin:
		return "SEMI JOIN"
	case AntiSemiJoin:
		return "ANTI SEMI JOIN"
	case HashJoin:
		return "HASH JOIN"
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
	Left     *parser.Expression
	Right    *parser.Expression
	Operator string
}

// LimitInfo Limit信息
type LimitInfo struct {
	Limit  int64
	Offset int64
}

// OrderByItem 排序项
type OrderByItem struct {
	Column    string
	Direction string // "ASC" or "DESC"
}

// Statistics 统计信息（简化版）
type Statistics struct {
	RowCount   int64
	UniqueKeys int64
	NullCount  int64
}

// OptimizationContext 优化上下文
type OptimizationContext struct {
	DataSource domain.DataSource
	TableInfo  map[string]*domain.TableInfo
	Stats      map[string]*Statistics
	CostModel  CostModel
	Hints      *OptimizerHints // 添加优化器 hints
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
	CPUFactor    float64
	IoFactor     float64
	MemoryFactor float64
}

// NewDefaultCostModel 创建默认成本模型
func NewDefaultCostModel() *DefaultCostModel {
	return &DefaultCostModel{
		CPUFactor:    0.01,
		IoFactor:     0.1,
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

// OptimizerHints 优化器 hints（TiDB 兼容）
type OptimizerHints struct {
	// JOIN hints
	HashJoinTables     []string
	MergeJoinTables    []string
	INLJoinTables      []string
	INLHashJoinTables  []string
	INLMergeJoinTables []string
	NoHashJoinTables   []string
	NoMergeJoinTables  []string
	NoIndexJoinTables  []string
	LeadingOrder       []string
	StraightJoin       bool

	// INDEX hints
	UseIndex     map[string][]string // table -> index list
	ForceIndex   map[string][]string // table -> index list
	IgnoreIndex  map[string][]string // table -> index list
	OrderIndex   map[string]string   // table -> index name
	NoOrderIndex map[string]string   // table -> index name

	// AGG hints
	HashAgg      bool
	StreamAgg    bool
	MPP1PhaseAgg bool
	MPP2PhaseAgg bool

	// Subquery hints
	SemiJoinRewrite bool
	NoDecorrelate   bool
	UseTOJA         bool

	// Global hints
	QBName                string
	MaxExecutionTime      time.Duration
	MemoryQuota           int64
	ReadConsistentReplica bool
	ResourceGroup         string
}

// HintAwareRule 支持 hints 的优化规则接口
type HintAwareRule interface {
	OptimizationRule

	// ApplyWithHints 应用带有 hints 的规则
	ApplyWithHints(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext, hints *OptimizerHints) (LogicalPlan, error)
}

// AggregationAlgorithm 聚合算法类型
type AggregationAlgorithm int

const (
	// HashAggAlgorithm 哈希聚合
	HashAggAlgorithm AggregationAlgorithm = iota
	// StreamAggAlgorithm 流式聚合
	StreamAggAlgorithm
	// MPP1PhaseAggAlgorithm MPP 单阶段聚合
	MPP1PhaseAggAlgorithm
	// MPP2PhaseAggAlgorithm MPP 两阶段聚合
	MPP2PhaseAggAlgorithm
)

// String 返回聚合算法的字符串表示
func (aa AggregationAlgorithm) String() string {
	switch aa {
	case HashAggAlgorithm:
		return "HASH_AGG"
	case StreamAggAlgorithm:
		return "STREAM_AGG"
	case MPP1PhaseAggAlgorithm:
		return "MPP_1PHASE_AGG"
	case MPP2PhaseAggAlgorithm:
		return "MPP_2PHASE_AGG"
	default:
		return "UNKNOWN"
	}
}

// HypotheticalIndexStats 虚拟索引统计信息
type HypotheticalIndexStats struct {
	NDV           int64   // Number of Distinct Values
	Selectivity   float64 // 选择性（0-1）
	EstimatedSize int64   // 预估索引大小（字节）
	NullFraction  float64 // NULL 值比例
	Correlation   float64 // 列相关性因子
}

// HypotheticalIndex 虚拟索引
type HypotheticalIndex struct {
	ID        string
	TableName string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
	Stats     *HypotheticalIndexStats
	CreatedAt time.Time
}

// IndexRecommendation 索引推荐
type IndexRecommendation struct {
	TableName        string
	Columns          []string
	EstimatedBenefit float64 // 收益（0-1）
	EstimatedCost    float64 // 成本降低百分比
	Reason           string
	CreateStatement  string
	RecommendationID string
}

// IndexCandidate 索引候选
type IndexCandidate struct {
	TableName string
	Columns   []string
	Priority  int    // 优先级（WHERE=4, JOIN=3, GROUP=2, ORDER=1）
	Source    string // 来源（WHERE, JOIN, GROUP, ORDER）
	Unique    bool   // 是否唯一索引
	IndexType string // 索引类型：BTREE, FULLTEXT, SPATIAL
}

// IndexType 索引类型常量
const (
	IndexTypeBTree    = "BTREE"
	IndexTypeFullText = "FULLTEXT"
	IndexTypeSpatial  = "SPATIAL"
)

// FullTextIndexCandidate 全文索引候选
type FullTextIndexCandidate struct {
	TableName string
	Columns   []string // 支持的列类型：TEXT, VARCHAR
	MinLength int      // 最小词长度（默认 4）
	StopWords []string // 停用词列表
}

// SpatialIndexCandidate 空间索引候选
type SpatialIndexCandidate struct {
	TableName    string
	ColumnName   string // 支持的列类型：GEOMETRY, POINT, LINESTRING, POLYGON
	IndexSubType string // 具体子类型：POINT, LINESTRING, POLYGON, MULTIPOLYGON
}

// SpatialFunction 空间函数类型
const (
	SpatialFuncContains   = "ST_Contains"
	SpatialFuncIntersects = "ST_Intersects"
	SpatialFuncWithin     = "ST_Within"
	SpatialFuncOverlaps   = "ST_Overlaps"
	SpatialFuncTouches    = "ST_Touches"
	SpatialFuncCrosses    = "ST_Crosses"
	SpatialFuncDistance   = "ST_Distance"
	SpatialFuncArea       = "ST_Area"
	SpatialFuncLength     = "ST_Length"
	SpatialFuncBuffer     = "ST_Buffer"
)

// FullTextFunction 全文函数类型
const (
	FullTextFuncMatchAgainst = "MATCH_AGAINST"
	FullTextFuncFulltext     = "FULLTEXT"
)

// RuleSet 规则集合
type RuleSet []OptimizationRule

// Apply 应用所有规则
func (rs RuleSet) Apply(ctx context.Context, plan LogicalPlan, optCtx *OptimizationContext) (LogicalPlan, error) {
	debugln("  [DEBUG] RuleSet.Apply: 开始, 当前计划:", plan.Explain())
	current := plan
	changed := true
	maxIterations := 10 // 防止无限循环
	iterations := 0

	// 迭代应用规则，直到不再变化
	for changed && iterations < maxIterations {
		changed = false
		iterations++
		debugln("  [DEBUG] RuleSet.Apply: 迭代", iterations)

		for _, rule := range rs {
			if rule.Match(current) {
				debugln("  [DEBUG] RuleSet.Apply: 匹配规则", rule.Name())
				newPlan, err := rule.Apply(ctx, current, optCtx)
				if err != nil {
					return nil, fmt.Errorf("rule %s failed: %w", rule.Name(), err)
				}
				if newPlan != nil && newPlan != current {
					current = newPlan
					changed = true
					debugln("  [DEBUG] RuleSet.Apply: 规则", rule.Name(), "应用成功")
				}
			}
		}

		// 递归应用到子节点
		children := current.Children()
		if len(children) > 0 {
			debugln("  [DEBUG] RuleSet.Apply: 递归处理子节点, 数量:", len(children))
			for i, child := range children {
				newChild, err := rs.Apply(ctx, child, optCtx)
				if err != nil {
					return nil, err
				}
				if newChild != child {
					debugln("  [DEBUG] RuleSet.Apply: 子节点", i, "已更新")
					allChildren := current.Children()
					allChildren[i] = newChild
					current.SetChildren(allChildren...)
					changed = true
				}
			}
		}
	}

	debugln("  [DEBUG] RuleSet.Apply: 完成, 总迭代次数:", iterations)
	return current, nil
}
