package cost

import (
	"fmt"
	"math"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// JoinType JOIN类型（独立定义以避免循环导入）
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftOuterJoin
	RightOuterJoin
	FullOuterJoin
)

// SimpleCardinalityEstimator 简单的基数估算器（默认实现）
type SimpleCardinalityEstimator struct{}

func (s *SimpleCardinalityEstimator) EstimateTableScan(tableName string) int64 {
	return 10000 // 默认值
}

func (s *SimpleCardinalityEstimator) EstimateFilter(tableName string, filters []domain.Filter) int64 {
	return 1000 // 默认值
}

// CardinalityEstimator 基数估算器接口（cost 包内部接口）
type CardinalityEstimator interface {
	EstimateTableScan(tableName string) int64
	EstimateFilter(tableName string, filters []domain.Filter) int64
}

// AdaptiveCostModel 自适应成本模型
// 基于硬件配置、统计信息动态调整成本估算
type AdaptiveCostModel struct {
	hardware     *HardwareProfile
	factors      *AdaptiveCostFactor
	estimator    CardinalityEstimator
	cacheHitInfo *CacheHitInfo
}

// CacheHitInfo 缓存命中信息（用于动态调整）
type CacheHitInfo struct {
	TableHitRates map[string]float64 // 表级缓存命中率
	LastUpdate    time.Time
}

// NewAdaptiveCostModel 创建自适应成本模型
func NewAdaptiveCostModel(estimator CardinalityEstimator) *AdaptiveCostModel {
	hardware := DetectHardwareProfile()

	return &AdaptiveCostModel{
		hardware:     hardware,
		factors:      hardware.CalculateCostFactors(),
		estimator:    estimator,
		cacheHitInfo: &CacheHitInfo{
			TableHitRates: make(map[string]float64),
			LastUpdate:    time.Now(),
		},
	}
}

// NewEnhancedCostModel 创建增强成本模型（别名）
func NewEnhancedCostModel(hardware *HardwareProfile) *AdaptiveCostModel {
	if hardware == nil {
		hardware = DetectHardwareProfile()
	}

	estimator := &SimpleCardinalityEstimator{}

	return &AdaptiveCostModel{
		hardware:     hardware,
		factors:      hardware.CalculateCostFactors(),
		estimator:    estimator,
		cacheHitInfo: &CacheHitInfo{
			TableHitRates: make(map[string]float64),
			LastUpdate:    time.Now(),
		},
	}
}

// ScanCost 计算扫描成本（增强版）
func (acm *AdaptiveCostModel) ScanCost(tableName string, rowCount int64, useIndex bool) float64 {
	if rowCount <= 0 {
		return 0
	}

	// 基础IO成本
	baseCost := float64(rowCount) * acm.factors.IOFactor

	// 使用索引时，调整成本
	if useIndex {
		// 索引扫描成本 = 索引高度 + 叶子节点访问
		indexHeight := acm.estimateIndexHeight(tableName)
		leafAccess := float64(rowCount) * acm.factors.IOFactor * 0.1
		
		// 索引通常比全表扫描快10倍
		indexCost := (float64(indexHeight) * acm.factors.CPUFactor) + leafAccess
		return math.Min(baseCost, indexCost * 0.1)
	}

	// 考虑缓存命中率
	cacheHitRate := acm.getTableCacheHitRate(tableName)
	if cacheHitRate > 0 {
		// 缓存命中时，成本大幅降低
		cachedCost := baseCost * (1.0 - cacheHitRate)
		return math.Min(baseCost, cachedCost)
	}

	return baseCost
}

// FilterCost 计算过滤成本（增强版）
func (acm *AdaptiveCostModel) FilterCost(inputRows int64, selectivity float64, filters []domain.Filter) float64 {
	outputRows := float64(inputRows) * selectivity

	// CPU比较成本
	comparisonCost := outputRows * acm.factors.CPUFactor

	// 内存成本：过滤需要加载行到内存
	memoryCost := float64(inputRows) * acm.factors.MemoryFactor * 0.01

	// 如果使用了索引，降低成本
	for _, filter := range filters {
		if acm.canUseIndex(filter.Field) {
			// 索引过滤成本更低
			comparisonCost *= 0.3
		}
	}

	return comparisonCost + memoryCost
}

// JoinCost 计算连接成本（增强版）
func (acm *AdaptiveCostModel) JoinCost(left interface{}, right interface{}, joinType JoinType, conditions []*parser.Expression) float64 {
	leftRows := acm.estimatePlanRowCount(left)
	rightRows := acm.estimatePlanRowCount(right)

	if leftRows == 0 || rightRows == 0 {
		return 0
	}

	switch joinType {
	case InnerJoin:
		// Hash Join成本
		buildCost := acm.buildHashTableCost(leftRows)
		probeCost := acm.probeHashTableCost(leftRows, rightRows, conditions)
		return buildCost + probeCost

	case LeftOuterJoin:
		// Left Outer Join
		buildCost := acm.buildHashTableCost(leftRows)
		probeCost := acm.probeHashTableCost(leftRows, rightRows, conditions)
		materializeCost := float64(leftRows) * acm.factors.MemoryFactor * 0.01
		return buildCost + probeCost + materializeCost

	case RightOuterJoin:
		// Right Outer Join
		buildCost := acm.buildHashTableCost(rightRows)
		probeCost := acm.probeHashTableCost(rightRows, leftRows, conditions)
		materializeCost := float64(rightRows) * acm.factors.MemoryFactor * 0.01
		return buildCost + probeCost + materializeCost

	case FullOuterJoin:
		// Full Outer Join
		leftBuild := acm.buildHashTableCost(leftRows)
		rightBuild := acm.buildHashTableCost(rightRows)
		probeCost1 := acm.probeHashTableCost(leftRows, rightRows, conditions)
		probeCost2 := acm.probeHashTableCost(rightRows, leftRows, conditions)
		return leftBuild + rightBuild + probeCost1 + probeCost2

	default:
		// 默认使用Hash Join
		return acm.buildHashTableCost(int64(math.Min(float64(leftRows), float64(rightRows)))) +
			acm.probeHashTableCost(leftRows, rightRows, conditions)
	}
}

// ScanCostForJoinModel 用于join.CostModel接口的扫描成本计算
func (acm *AdaptiveCostModel) ScanCostForJoinModel(tableName string, rowCount int64, useIndex bool) float64 {
	return acm.ScanCost(tableName, rowCount, useIndex)
}

// AggregateCost 计算聚合成本（增强版）
func (acm *AdaptiveCostModel) AggregateCost(inputRows int64, groupByCols int, aggFuncs int) float64 {
	// 分组成本：每行 * 分组列数 * CPU因子
	groupingCost := float64(inputRows) * float64(groupByCols) * acm.factors.CPUFactor

	// 聚合函数成本
	aggregationCost := float64(inputRows) * float64(aggFuncs) * acm.factors.CPUFactor

	// 内存成本：哈希表构建
	hashTableCost := float64(inputRows) * acm.factors.MemoryFactor * 0.05

	// 排序成本（如果有ORDER BY）
	sortingCost := acm.estimateSortingCost(inputRows, groupByCols)

	return groupingCost + aggregationCost + hashTableCost + sortingCost
}

// ProjectCost 计算投影成本（增强版）
func (acm *AdaptiveCostModel) ProjectCost(inputRows int64, projCols int) float64 {
	// 基础投影成本
	baseCost := float64(inputRows) * float64(projCols) * acm.factors.CPUFactor

	// 如果投影包含表达式计算，增加额外成本
	expressionCost := float64(projCols) * acm.factors.CPUFactor * 0.5

	// 内存成本
	memoryCost := float64(inputRows) * float64(projCols) * acm.factors.MemoryFactor * 0.001

	return baseCost + expressionCost + memoryCost
}

// SortCost 计算排序成本
func (acm *AdaptiveCostModel) SortCost(inputRows int64) float64 {
	// 快速排序成本：n * log(n)
	if inputRows <= 1 {
		return 0
	}
	return float64(inputRows) * math.Log2(float64(inputRows)) * acm.factors.CPUFactor
}

// VectorSearchCost 计算向量搜索成本
func (acm *AdaptiveCostModel) VectorSearchCost(indexType string, rowCount int64, k int) float64 {
	if rowCount <= 0 {
		return 0
	}

	switch indexType {
	case "vector_hnsw", "hnsw":
		// HNSW索引成本：log(N) * k * ef_search
		// 近似为 O(log N) 的搜索复杂度
		logN := math.Log2(float64(rowCount))
		searchCost := logN * float64(k) * acm.factors.CPUFactor
		// 加上内存访问成本
		memoryCost := logN * float64(k) * acm.factors.MemoryFactor * 0.01
		return searchCost + memoryCost

	case "vector_flat", "flat":
		// Flat索引（暴力搜索）成本：N * k
		scanCost := float64(rowCount) * acm.factors.CPUFactor
		// 排序成本：k * log(k) * N（找到top k）
		sortCost := float64(k) * math.Log2(float64(k)) * acm.factors.CPUFactor
		return scanCost + sortCost

	case "vector_ivf_flat", "ivf_flat":
		// IVF-Flat索引成本：N/nlist * k
		// 假设 nlist = sqrt(N)
		nlist := math.Sqrt(float64(rowCount))
		scanCost := (float64(rowCount) / nlist) * acm.factors.CPUFactor
		return scanCost

	default:
		// 默认使用暴力搜索成本
		return float64(rowCount) * acm.factors.CPUFactor
	}
}

// estimateIndexHeight 估算索引高度
func (acm *AdaptiveCostModel) estimateIndexHeight(tableName string) int {
	// 基于B+树索引，高度 ≈ log2(行数)
	rowCount := acm.estimator.EstimateTableScan(tableName)
	if rowCount <= 0 {
		return 3
	}
	
	height := math.Ceil(math.Log2(float64(rowCount)))
	return int(math.Max(2, height))
}

// buildHashTableCost 计算哈希表构建成本
func (acm *AdaptiveCostModel) buildHashTableCost(rows int64) float64 {
	// 构建哈希表 = 计算哈希 + 插入哈希
	hashCost := float64(rows) * acm.factors.CPUFactor * 2.0
	memoryCost := float64(rows) * acm.factors.MemoryFactor * 0.01
	return hashCost + memoryCost
}

// probeHashTableCost 计算哈希探测成本
func (acm *AdaptiveCostModel) probeHashTableCost(buildRows, probeRows int64, conditions []*parser.Expression) float64 {
	// 探测成本 = 探测行数 * 条件数 * CPU因子
	probeCost := float64(probeRows) * float64(len(conditions)+1) * acm.factors.CPUFactor
	
	// 内存成本：加载探测表
	memoryCost := float64(probeRows) * acm.factors.MemoryFactor * 0.001

	return probeCost + memoryCost
}

// estimateSortingCost 估算排序成本
func (acm *AdaptiveCostModel) estimateSortingCost(rows int64, sortCols int) float64 {
	if rows <= 1 {
		return 0
	}
	
	// 外部排序成本：n * log(n) * (列数/10)
	logCost := math.Log2(float64(rows))
	return float64(rows) * logCost * float64(sortCols) * acm.factors.CPUFactor
}

// estimatePlanRowCount 估算计划的行数
func (acm *AdaptiveCostModel) estimatePlanRowCount(plan interface{}) int64 {
	// 简化实现，直接估算
	return 10000
}

// estimatePlanRowCountWithType 带类型断言的估算（如果需要）
func (acm *AdaptiveCostModel) estimatePlanRowCountWithType(plan interface{}) int64 {
	if ds, ok := plan.(interface{ GetTableName() string }); ok {
		if method := ds.GetTableName; method != nil {
			return acm.estimator.EstimateTableScan(method())
		}
	}

	if sel, ok := plan.(interface{ Children() []interface{} }); ok && len(sel.Children()) > 0 {
		// 简化：直接返回估算值
		return acm.estimator.EstimateTableScan("unknown")
	}

	// 默认
	return 10000
}

// canUseIndex 检查是否可以使用索引
func (acm *AdaptiveCostModel) canUseIndex(fieldName string) bool {
	// 简化：假设所有列都有索引
	// 实际应该从IndexManager检查
	return true
}

// getTableCacheHitRate 获取表的缓存命中率
func (acm *AdaptiveCostModel) getTableCacheHitRate(tableName string) float64 {
	if rate, exists := acm.cacheHitInfo.TableHitRates[tableName]; exists {
		return rate
	}
	return acm.hardware.EstimateCacheHitRate()
}

// UpdateCacheHitInfo 更新缓存命中信息
func (acm *AdaptiveCostModel) UpdateCacheHitInfo(tableName string, hitRate float64) {
	acm.cacheHitInfo.TableHitRates[tableName] = hitRate
	acm.cacheHitInfo.LastUpdate = time.Now()
}

// GetHardwareProfile 获取硬件配置
func (acm *AdaptiveCostModel) GetHardwareProfile() *HardwareProfile {
	return acm.hardware
}

// GetCostFactors 获取成本因子
func (acm *AdaptiveCostModel) GetCostFactors() *AdaptiveCostFactor {
	return acm.factors
}

// Explain 返回成本模型的说明
func (acm *AdaptiveCostModel) Explain() string {
	return fmt.Sprintf(
		"AdaptiveCostModel:\n"+
			"  IO Factor: %.4f\n"+
			"  CPU Factor: %.4f\n"+
			"  Memory Factor: %.4f\n"+
			"  Network Factor: %.4f\n"+
			"  Cache Hit Rate: %.2f%%",
		acm.factors.IOFactor,
		acm.factors.CPUFactor,
		acm.factors.MemoryFactor,
		acm.factors.NetworkFactor,
		acm.hardware.EstimateCacheHitRate()*100,
	)
}

// getTableName 获取表的名称（简化版）
func getTableName(plan interface{}) string {
	// 简化实现
	return "unknown_table"
}
