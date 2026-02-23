package index

import (
	"fmt"
	"math"
	"strings"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/statistics"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// IndexSelector 索引选择器
// 选择最优索引用于查询执行
type IndexSelector struct {
	estimator    statistics.CardinalityEstimator
	indexManager *IndexManager
}

// IndexManager 索引管理器
type IndexManager struct {
	indices map[string][]*Index // table_name -> indices
}

// Index 索引定义
type Index struct {
	Name        string
	TableName   string
	Columns     []string
	Unique      bool
	Primary     bool
	Cardinality int64 // 基数
	IndexType   IndexType
}

// IndexType 索引类型
type IndexType int

const (
	BTreeIndex IndexType = iota
	HashIndex
	BitmapIndex
	FullTextIndex
)

// NewIndexManager 创建索引管理器
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indices: make(map[string][]*Index),
	}
}

// NewIndexSelector 创建索引选择器
func NewIndexSelector(estimator statistics.CardinalityEstimator) *IndexSelector {
	return &IndexSelector{
		estimator:    estimator,
		indexManager: NewIndexManager(),
	}
}

// SelectBestIndex 选择最佳索引
func (is *IndexSelector) SelectBestIndex(tableName string, filters []domain.Filter, requiredCols []string) *IndexSelection {
	// 获取表的所有索引
	indices := is.indexManager.GetIndices(tableName)
	if len(indices) == 0 {
		return &IndexSelection{
			SelectedIndex: nil,
			Reason:        "No available index",
			Cost:          math.MaxFloat64,
		}
	}

	// 评估每个索引
	bestIndex := is.evaluateIndexes(tableName, filters, requiredCols, indices)

	return bestIndex
}

// evaluateIndexes 评估所有可用索引
func (is *IndexSelector) evaluateIndexes(tableName string, filters []domain.Filter, requiredCols []string, indices []*Index) *IndexSelection {
	var bestIndex *IndexSelection
	bestIndex = &IndexSelection{
		Cost: math.MaxFloat64,
	}

	for _, idx := range indices {
		evaluation := is.evaluateIndex(tableName, filters, requiredCols, idx)

		if evaluation.Cost < bestIndex.Cost {
			bestIndex = evaluation
		}
	}

	return bestIndex
}

// evaluateIndex 评估单个索引
func (is *IndexSelector) evaluateIndex(tableName string, filters []domain.Filter, requiredCols []string, idx *Index) *IndexSelection {
	// 1. 检查索引是否可用
	if !is.isIndexUsable(tableName, filters, idx) {
		return &IndexSelection{
			SelectedIndex: nil,
			Reason:        "Index not usable for given filters",
			Cost:          math.MaxFloat64,
		}
	}

	// 2. 计算索引扫描成本
	scanCost := is.estimateIndexScanCost(tableName, filters, idx)

	// 3. 检查索引覆盖性
	isCovering := is.isCoveringIndex(requiredCols, idx)

	// 4. 计算总成本
	totalCost := scanCost
	if !isCovering {
		// 非覆盖索引需要回表查询
		totalCost += is.estimateTableLookupCost(tableName)
	}

	// 5. 生成选择理由
	reason := is.generateSelectionReason(idx, filters, isCovering, scanCost)

	return &IndexSelection{
		SelectedIndex: idx,
		IsCovering:    isCovering,
		EstimatedRows: is.estimateIndexRows(tableName, filters, idx),
		Cost:          totalCost,
		Reason:        reason,
	}
}

// isIndexUsable 检查索引是否可用
func (is *IndexSelector) isIndexUsable(tableName string, filters []domain.Filter, idx *Index) bool {
	// 至少有一个过滤条件匹配索引的前导列
	for _, filter := range filters {
		for i, col := range idx.Columns {
			if filter.Field == col {
				// 找到匹配的列，检查是否是前导列
				if i == 0 {
					return true // 前导列匹配
				}
				// 简化：如果不是前导列，也可以使用（实际应该更严格）
				return true
			}
		}
	}

	return len(filters) == 0
}

// estimateIndexScanCost 估算索引扫描成本
func (is *IndexSelector) estimateIndexScanCost(tableName string, filters []domain.Filter, idx *Index) float64 {
	// 获取基估计
	stats, err := is.estimator.GetStatistics(tableName)
	if err != nil {
		// 使用默认估算
		return 100.0
	}

	// 估算选择性
	selectivity := is.estimateIndexSelectivity(filters, idx, stats)
	estimatedRows := float64(stats.RowCount) * selectivity

	// 索引扫描成本 = 索引高度 + 叶子节点访问
	indexHeight := is.estimateIndexHeight(idx, stats)
	leafAccess := estimatedRows * 0.01 // IO成本

	scanCost := float64(indexHeight) + leafAccess

	return scanCost
}

// estimateIndexRows 估算索引扫描的行数
func (is *IndexSelector) estimateIndexRows(tableName string, filters []domain.Filter, idx *Index) float64 {
	stats, err := is.estimator.GetStatistics(tableName)
	if err != nil {
		return 100.0
	}

	selectivity := is.estimateIndexSelectivity(filters, idx, stats)
	return float64(stats.RowCount) * selectivity
}

// estimateIndexSelectivity 估算索引的选择性
func (is *IndexSelector) estimateIndexSelectivity(filters []domain.Filter, idx *Index, stats *statistics.TableStatistics) float64 {
	if len(filters) == 0 {
		return 1.0
	}

	// 使用直方图估算选择性
	totalSelectivity := 1.0
	for _, filter := range filters {
		// 检查过滤条件是否在索引的列上
		colInIndex := false
		for _, col := range idx.Columns {
			if filter.Field == col {
				colInIndex = true
				break
			}
		}

		if colInIndex {
			// 使用列的直方图
			if histogram, exists := stats.Histograms[filter.Field]; exists {
				sel := histogram.EstimateSelectivity(filter)
				totalSelectivity *= sel
			} else if colStats, exists := stats.ColumnStats[filter.Field]; exists {
				// 回退到NDV
				sel := 1.0 / float64(colStats.DistinctCount)
				totalSelectivity *= sel
			}
		}
	}

	return totalSelectivity
}

// isCoveringIndex 检查是否是覆盖索引
func (is *IndexSelector) isCoveringIndex(requiredCols []string, idx *Index) bool {
	if len(requiredCols) == 0 {
		return false
	}

	// 检查所有需要的列是否都在索引中
	coveredCount := 0
	for _, reqCol := range requiredCols {
		for _, idxCol := range idx.Columns {
			if reqCol == idxCol {
				coveredCount++
				break
			}
		}
	}

	// 覆盖索引包含所有需要的列
	return coveredCount == len(requiredCols)
}

// estimateTableLookupCost 估算表查找成本
func (is *IndexSelector) estimateTableLookupCost(tableName string) float64 {
	// 随机表查找：通常需要10-20次IO
	return 15.0
}

// estimateIndexHeight 估算索引高度
func (is *IndexSelector) estimateIndexHeight(idx *Index, stats *statistics.TableStatistics) int {
	if idx.Cardinality > 0 {
		// 基于B+树的高度 ≈ log2(基数)
		height := math.Ceil(math.Log2(float64(idx.Cardinality)))
		return int(math.Max(2, height))
	}

	// 使用表的NDV
	for _, col := range idx.Columns {
		if colStats, exists := stats.ColumnStats[col]; exists && colStats.DistinctCount > 0 {
			height := math.Ceil(math.Log2(float64(colStats.DistinctCount)))
			return int(math.Max(2, height))
		}
	}

	return 3 // 默认高度
}

// generateSelectionReason 生成索引选择理由
func (is *IndexSelector) generateSelectionReason(idx *Index, filters []domain.Filter, isCovering bool, scanCost float64) string {
	reason := fmt.Sprintf("Selected index '%s'", idx.Name)

	if isCovering {
		reason += " (covering index)"
	}

	reason += fmt.Sprintf(" with estimated cost %.2f", scanCost)

	if len(filters) > 0 {
		reason += fmt.Sprintf(" for %d filter condition(s)", len(filters))
	}

	return reason
}

// AddIndex 添加索引定义
func (im *IndexManager) AddIndex(index *Index) {
	if im.indices == nil {
		im.indices = make(map[string][]*Index)
	}
	im.indices[index.TableName] = append(im.indices[index.TableName], index)
}

// GetIndices 获取表的所有索引
func (im *IndexManager) GetIndices(tableName string) []*Index {
	if im.indices == nil {
		return nil
	}
	if indices, exists := im.indices[tableName]; exists {
		return indices
	}
	return nil
}

// FindIndexByName 根据名称查找索引
func (im *IndexManager) FindIndexByName(tableName, indexName string) *Index {
	indices := im.GetIndices(tableName)
	for _, idx := range indices {
		if idx.Name == indexName {
			return idx
		}
	}
	return nil
}

// IndexSelection 索引选择结果
type IndexSelection struct {
	SelectedIndex *Index
	IsCovering    bool
	EstimatedRows float64
	Cost          float64
	Reason        string
}

// String 返回选择的字符串表示
func (is *IndexSelection) String() string {
	if is.SelectedIndex == nil {
		return fmt.Sprintf("NoIndexSelected(cost=%.2f, reason=%s)", is.Cost, is.Reason)
	}
	return fmt.Sprintf("IndexSelected(%s, covering=%v, cost=%.2f, reason=%s)",
		is.SelectedIndex.Name, is.IsCovering, is.Cost, is.Reason)
}

// Explain 返回详细的索引选择说明
func (is *IndexSelector) Explain(tableName string, filters []domain.Filter, requiredCols []string) string {
	selection := is.SelectBestIndex(tableName, filters, requiredCols)

	var explanation strings.Builder
	explanation.WriteString(fmt.Sprintf("=== Index Selection for '%s' ===\n", tableName))
	explanation.WriteString(fmt.Sprintf("Required Columns: %v\n", requiredCols))
	explanation.WriteString(fmt.Sprintf("Filters: %d\n\n", len(filters)))

	for i, filter := range filters {
		explanation.WriteString(fmt.Sprintf("  Filter %d: %s %s %v\n", i+1, filter.Field, filter.Operator, filter.Value))
	}

	explanation.WriteString(fmt.Sprintf("\nSelected: %s\n", selection.String()))
	explanation.WriteString(fmt.Sprintf("Estimated Rows: %.0f\n", selection.EstimatedRows))

	return explanation.String()
}
