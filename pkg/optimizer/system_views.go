package optimizer

import (
	"fmt"
	"sync"
	"time"
)

// IndexAdvisorResult 索引推荐结果
type IndexAdvisorResult struct {
	ID               string
	Database         string
	TableName        string
	IndexName        string
	IndexColumns     []string
	EstIndexSize     int64
	Reason           string
	CreateStatement  string
	EstimatedBenefit float64
	Timestamp        time.Time
}

// UnusedIndex 未使用的索引
type UnusedIndex struct {
	Database       string
	TableName      string
	IndexName      string
	IndexColumns   []string
	LastUsedAt     time.Time
	IndexSize      int64
	Reason         string
}

// HypotheticalIndexDisplay 虚拟索引显示信息
type HypotheticalIndexDisplay struct {
	ID             string
	TableName      string
	IndexColumns   []string
	IsUnique       bool
	Selectivity    float64
	EstimatedSize  int64
	CreatedAt      time.Time
}

// SystemViews 系统视图管理器
type SystemViews struct {
	indexAdvisorResults []IndexAdvisorResult
	unusedIndexes       []UnusedIndex
	hypotheticalIndexes []HypotheticalIndexDisplay
	mu                  sync.RWMutex
	maxResults          int
}

// NewSystemViews 创建系统视图管理器
func NewSystemViews() *SystemViews {
	return &SystemViews{
		indexAdvisorResults: make([]IndexAdvisorResult, 0),
		unusedIndexes:       make([]UnusedIndex, 0),
		hypotheticalIndexes: make([]HypotheticalIndexDisplay, 0),
		maxResults:          1000,
	}
}

// AddIndexAdvisorResult 添加索引推荐结果
func (sv *SystemViews) AddIndexAdvisorResult(result IndexAdvisorResult) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	result.Timestamp = time.Now()
	sv.indexAdvisorResults = append(sv.indexAdvisorResults, result)

	// 限制结果数量
	if len(sv.indexAdvisorResults) > sv.maxResults {
		sv.indexAdvisorResults = sv.indexAdvisorResults[len(sv.indexAdvisorResults)-sv.maxResults:]
	}
}

// GetIndexAdvisorResults 获取索引推荐结果
func (sv *SystemViews) GetIndexAdvisorResults() []IndexAdvisorResult {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	// 返回副本
	results := make([]IndexAdvisorResult, len(sv.indexAdvisorResults))
	copy(results, sv.indexAdvisorResults)

	return results
}

// GetIndexAdvisorResultsForTable 获取指定表的索引推荐结果
func (sv *SystemViews) GetIndexAdvisorResultsForTable(tableName string) []IndexAdvisorResult {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	var results []IndexAdvisorResult
	for _, result := range sv.indexAdvisorResults {
		if result.TableName == tableName {
			results = append(results, result)
		}
	}

	return results
}

// ClearIndexAdvisorResults 清空索引推荐结果
func (sv *SystemViews) ClearIndexAdvisorResults() {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.indexAdvisorResults = make([]IndexAdvisorResult, 0)
}

// AddUnusedIndex 添加未使用的索引
func (sv *SystemViews) AddUnusedIndex(index UnusedIndex) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.unusedIndexes = append(sv.unusedIndexes, index)

	// 限制结果数量
	if len(sv.unusedIndexes) > sv.maxResults {
		sv.unusedIndexes = sv.unusedIndexes[len(sv.unusedIndexes)-sv.maxResults:]
	}
}

// GetUnusedIndexes 获取未使用的索引列表
func (sv *SystemViews) GetUnusedIndexes() []UnusedIndex {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	// 返回副本
	indexes := make([]UnusedIndex, len(sv.unusedIndexes))
	copy(indexes, sv.unusedIndexes)

	return indexes
}

// GetUnusedIndexesForTable 获取指定表的未使用索引
func (sv *SystemViews) GetUnusedIndexesForTable(tableName string) []UnusedIndex {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	var indexes []UnusedIndex
	for _, index := range sv.unusedIndexes {
		if index.TableName == tableName {
			indexes = append(indexes, index)
		}
	}

	return indexes
}

// ClearUnusedIndexes 清空未使用的索引列表
func (sv *SystemViews) ClearUnusedIndexes() {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.unusedIndexes = make([]UnusedIndex, 0)
}

// AddHypotheticalIndex 添加虚拟索引
func (sv *SystemViews) AddHypotheticalIndex(index HypotheticalIndexDisplay) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.hypotheticalIndexes = append(sv.hypotheticalIndexes, index)

	// 限制结果数量
	if len(sv.hypotheticalIndexes) > sv.maxResults {
		sv.hypotheticalIndexes = sv.hypotheticalIndexes[len(sv.hypotheticalIndexes)-sv.maxResults:]
	}
}

// GetHypotheticalIndexes 获取虚拟索引列表
func (sv *SystemViews) GetHypotheticalIndexes() []HypotheticalIndexDisplay {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	// 返回副本
	indexes := make([]HypotheticalIndexDisplay, len(sv.hypotheticalIndexes))
	copy(indexes, sv.hypotheticalIndexes)

	return indexes
}

// GetHypotheticalIndexesForTable 获取指定表的虚拟索引
func (sv *SystemViews) GetHypotheticalIndexesForTable(tableName string) []HypotheticalIndexDisplay {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	var indexes []HypotheticalIndexDisplay
	for _, index := range sv.hypotheticalIndexes {
		if index.TableName == tableName {
			indexes = append(indexes, index)
		}
	}

	return indexes
}

// ClearHypotheticalIndexes 清空虚拟索引列表
func (sv *SystemViews) ClearHypotheticalIndexes() {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.hypotheticalIndexes = make([]HypotheticalIndexDisplay, 0)
}

// ClearAll 清空所有数据
func (sv *SystemViews) ClearAll() {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.indexAdvisorResults = make([]IndexAdvisorResult, 0)
	sv.unusedIndexes = make([]UnusedIndex, 0)
	sv.hypotheticalIndexes = make([]HypotheticalIndexDisplay, 0)
}

// SetMaxResults 设置最大结果数量
func (sv *SystemViews) SetMaxResults(max int) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.maxResults = max
}

// GetMaxResults 获取最大结果数量
func (sv *SystemViews) GetMaxResults() int {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	return sv.maxResults
}

// GetStatistics 获取统计信息
func (sv *SystemViews) GetStatistics() map[string]int {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	return map[string]int{
		"index_advisor_results": len(sv.indexAdvisorResults),
		"unused_indexes":       len(sv.unusedIndexes),
		"hypothetical_indexes": len(sv.hypotheticalIndexes),
	}
}

// IndexAdvisorResultToRow 将索引推荐结果转换为行数据（用于信息模式视图）
func (sv *SystemViews) IndexAdvisorResultToRow(result IndexAdvisorResult) []interface{} {
	return []interface{}{
		result.ID,
		result.Database,
		result.TableName,
		result.IndexName,
		fmt.Sprintf("%v", result.IndexColumns),
		result.EstIndexSize,
		result.Reason,
		result.CreateStatement,
		result.EstimatedBenefit,
		result.Timestamp,
	}
}

// UnusedIndexToRow 将未使用索引转换为行数据
func (sv *SystemViews) UnusedIndexToRow(index UnusedIndex) []interface{} {
	return []interface{}{
		index.Database,
		index.TableName,
		index.IndexName,
		fmt.Sprintf("%v", index.IndexColumns),
		index.LastUsedAt,
		index.IndexSize,
		index.Reason,
	}
}

// HypotheticalIndexDisplayToRow 将虚拟索引转换为行数据
func (sv *SystemViews) HypotheticalIndexDisplayToRow(index HypotheticalIndexDisplay) []interface{} {
	return []interface{}{
		index.ID,
		index.TableName,
		fmt.Sprintf("%v", index.IndexColumns),
		index.IsUnique,
		index.Selectivity,
		index.EstimatedSize,
		index.CreatedAt,
	}
}

// QueryIndexAdvisorResults 查询索引推荐结果（模拟信息模式查询）
func (sv *SystemViews) QueryIndexAdvisorResults(filter map[string]interface{}) []IndexAdvisorResult {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	var results []IndexAdvisorResult

	for _, result := range sv.indexAdvisorResults {
		// 应用过滤器
		match := true

		if tableName, ok := filter["table_name"].(string); ok {
			if result.TableName != tableName {
				match = false
			}
		}

		if database, ok := filter["database"].(string); ok {
			if result.Database != database {
				match = false
			}
		}

		if minBenefit, ok := filter["min_benefit"].(float64); ok {
			if result.EstimatedBenefit < minBenefit {
				match = false
			}
		}

		if match {
			results = append(results, result)
		}
	}

	return results
}

// GetTopRecommendedIndexes 获取推荐索引排行
func (sv *SystemViews) GetTopRecommendedIndexes(limit int) []IndexAdvisorResult {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	// 按收益排序
	results := make([]IndexAdvisorResult, len(sv.indexAdvisorResults))
	copy(results, sv.indexAdvisorResults)

	// 简单排序
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i].EstimatedBenefit < results[j].EstimatedBenefit {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	// 限制返回数量
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results
}

// ConvertRecommendationsToSystemViews 将推荐结果转换为系统视图格式
func (sv *SystemViews) ConvertRecommendationsToSystemViews(
	recommendations []*IndexRecommendation,
	database string,
) []IndexAdvisorResult {
	results := make([]IndexAdvisorResult, 0, len(recommendations))

	for _, rec := range recommendations {
		result := IndexAdvisorResult{
			ID:               rec.RecommendationID,
			Database:         database,
			TableName:        rec.TableName,
			IndexName:        generateIndexName(rec.Columns),
			IndexColumns:     rec.Columns,
			EstIndexSize:     estimateIndexSize(rec.Columns),
			Reason:           rec.Reason,
			CreateStatement:  rec.CreateStatement,
			EstimatedBenefit: rec.EstimatedBenefit,
			Timestamp:        time.Now(),
		}

		results = append(results, result)
	}

	return results
}

// generateIndexName 生成索引名称
func generateIndexName(columns []string) string {
	if len(columns) == 0 {
		return "idx_auto"
	}

	name := "idx_"
	for i, col := range columns {
		if i > 0 {
			name += "_"
		}
		name += col
	}
	return name
}

// estimateIndexSize 估算索引大小（简化版）
func estimateIndexSize(columns []string) int64 {
	// 简化估算：假设每列 100 字节，10000 行
	baseSize := int64(100 * len(columns) * 10000)
	return baseSize
}

// SingleSystemViews 单例系统视图
var singleSystemViews *SystemViews
var svOnce sync.Once

// GetSystemViews 获取系统视图单例
func GetSystemViews() *SystemViews {
	svOnce.Do(func() {
		singleSystemViews = NewSystemViews()
	})
	return singleSystemViews
}
