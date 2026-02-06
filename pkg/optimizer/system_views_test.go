package optimizer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNewSystemViews 测试创建系统视图管理器
func TestNewSystemViews(t *testing.T) {
	sv := NewSystemViews()

	assert.NotNil(t, sv)
	assert.Equal(t, 0, len(sv.indexAdvisorResults))
	assert.Equal(t, 0, len(sv.unusedIndexes))
	assert.Equal(t, 0, len(sv.hypotheticalIndexes))
	assert.Equal(t, 1000, sv.maxResults)
}

// TestAddAndGetIndexAdvisorResults 测试添加和获取索引推荐结果
func TestAddAndGetIndexAdvisorResults(t *testing.T) {
	sv := NewSystemViews()

	result := IndexAdvisorResult{
		ID:               "test-1",
		Database:         "test_db",
		TableName:        "t1",
		IndexName:        "idx_a",
		IndexColumns:     []string{"a"},
		EstIndexSize:     1024,
		Reason:           "test",
		CreateStatement:  "CREATE INDEX idx_a ON t1(a)",
		EstimatedBenefit: 0.8,
	}

	sv.AddIndexAdvisorResult(result)

	results := sv.GetIndexAdvisorResults()

	assert.Equal(t, 1, len(results))
	assert.Equal(t, "test-1", results[0].ID)
	assert.Equal(t, "t1", results[0].TableName)
}

// TestGetIndexAdvisorResultsForTable 测试获取指定表的索引推荐结果
func TestGetIndexAdvisorResultsForTable(t *testing.T) {
	sv := NewSystemViews()

	result1 := IndexAdvisorResult{
		ID:        "test-1",
		TableName: "t1",
	}
	result2 := IndexAdvisorResult{
		ID:        "test-2",
		TableName: "t2",
	}

	sv.AddIndexAdvisorResult(result1)
	sv.AddIndexAdvisorResult(result2)

	results := sv.GetIndexAdvisorResultsForTable("t1")

	assert.Equal(t, 1, len(results))
	assert.Equal(t, "test-1", results[0].ID)
}

// TestClearIndexAdvisorResults 测试清空索引推荐结果
func TestClearIndexAdvisorResults(t *testing.T) {
	sv := NewSystemViews()

	result := IndexAdvisorResult{
		ID:        "test-1",
		TableName: "t1",
	}

	sv.AddIndexAdvisorResult(result)
	assert.Equal(t, 1, len(sv.GetIndexAdvisorResults()))

	sv.ClearIndexAdvisorResults()
	assert.Equal(t, 0, len(sv.GetIndexAdvisorResults()))
}

// TestAddAndGetUnusedIndexes 测试添加和获取未使用的索引
func TestAddAndGetUnusedIndexes(t *testing.T) {
	sv := NewSystemViews()

	index := UnusedIndex{
		Database:     "test_db",
		TableName:    "t1",
		IndexName:    "idx_old",
		IndexColumns: []string{"old_col"},
		LastUsedAt:   time.Now().Add(-30 * 24 * time.Hour),
		IndexSize:    2048,
		Reason:       "not used for 30 days",
	}

	sv.AddUnusedIndex(index)

	indexes := sv.GetUnusedIndexes()

	assert.Equal(t, 1, len(indexes))
	assert.Equal(t, "idx_old", indexes[0].IndexName)
}

// TestGetUnusedIndexesForTable 测试获取指定表的未使用索引
func TestGetUnusedIndexesForTable(t *testing.T) {
	sv := NewSystemViews()

	index1 := UnusedIndex{
		TableName: "t1",
		IndexName: "idx_a",
	}
	index2 := UnusedIndex{
		TableName: "t2",
		IndexName: "idx_b",
	}

	sv.AddUnusedIndex(index1)
	sv.AddUnusedIndex(index2)

	indexes := sv.GetUnusedIndexesForTable("t1")

	assert.Equal(t, 1, len(indexes))
	assert.Equal(t, "idx_a", indexes[0].IndexName)
}

// TestClearUnusedIndexes 测试清空未使用的索引列表
func TestClearUnusedIndexes(t *testing.T) {
	sv := NewSystemViews()

	index := UnusedIndex{
		TableName: "t1",
		IndexName: "idx_a",
	}

	sv.AddUnusedIndex(index)
	assert.Equal(t, 1, len(sv.GetUnusedIndexes()))

	sv.ClearUnusedIndexes()
	assert.Equal(t, 0, len(sv.GetUnusedIndexes()))
}

// TestAddAndGetHypotheticalIndexes 测试添加和获取虚拟索引
func TestAddAndGetHypotheticalIndexes(t *testing.T) {
	sv := NewSystemViews()

	index := HypotheticalIndexDisplay{
		ID:            "hyp_1",
		TableName:     "t1",
		IndexColumns:  []string{"a", "b"},
		IsUnique:      false,
		Selectivity:   0.1,
		EstimatedSize: 1024,
		CreatedAt:     time.Now(),
	}

	sv.AddHypotheticalIndex(index)

	indexes := sv.GetHypotheticalIndexes()

	assert.Equal(t, 1, len(indexes))
	assert.Equal(t, "hyp_1", indexes[0].ID)
}

// TestGetHypotheticalIndexesForTable 测试获取指定表的虚拟索引
func TestGetHypotheticalIndexesForTable(t *testing.T) {
	sv := NewSystemViews()

	index1 := HypotheticalIndexDisplay{
		ID:        "hyp_1",
		TableName: "t1",
	}
	index2 := HypotheticalIndexDisplay{
		ID:        "hyp_2",
		TableName: "t2",
	}

	sv.AddHypotheticalIndex(index1)
	sv.AddHypotheticalIndex(index2)

	indexes := sv.GetHypotheticalIndexesForTable("t1")

	assert.Equal(t, 1, len(indexes))
	assert.Equal(t, "hyp_1", indexes[0].ID)
}

// TestClearHypotheticalIndexes 测试清空虚拟索引列表
func TestClearHypotheticalIndexes(t *testing.T) {
	sv := NewSystemViews()

	index := HypotheticalIndexDisplay{
		ID:        "hyp_1",
		TableName: "t1",
	}

	sv.AddHypotheticalIndex(index)
	assert.Equal(t, 1, len(sv.GetHypotheticalIndexes()))

	sv.ClearHypotheticalIndexes()
	assert.Equal(t, 0, len(sv.GetHypotheticalIndexes()))
}

// TestClearAll 测试清空所有数据
func TestClearAll(t *testing.T) {
	sv := NewSystemViews()

	// 添加一些数据
	sv.AddIndexAdvisorResult(IndexAdvisorResult{ID: "1"})
	sv.AddUnusedIndex(UnusedIndex{IndexName: "1"})
	sv.AddHypotheticalIndex(HypotheticalIndexDisplay{ID: "1"})

	assert.Equal(t, 1, len(sv.GetIndexAdvisorResults()))
	assert.Equal(t, 1, len(sv.GetUnusedIndexes()))
	assert.Equal(t, 1, len(sv.GetHypotheticalIndexes()))

	sv.ClearAll()

	assert.Equal(t, 0, len(sv.GetIndexAdvisorResults()))
	assert.Equal(t, 0, len(sv.GetUnusedIndexes()))
	assert.Equal(t, 0, len(sv.GetHypotheticalIndexes()))
}

// TestSetAndGetMaxResults 测试设置和获取最大结果数量
func TestSetAndGetMaxResults(t *testing.T) {
	sv := NewSystemViews()

	sv.SetMaxResults(500)
	assert.Equal(t, 500, sv.GetMaxResults())
}

// TestGetStatistics 测试获取统计信息
func TestGetStatistics(t *testing.T) {
	sv := NewSystemViews()

	sv.AddIndexAdvisorResult(IndexAdvisorResult{ID: "1"})
	sv.AddUnusedIndex(UnusedIndex{IndexName: "1"})
	sv.AddHypotheticalIndex(HypotheticalIndexDisplay{ID: "1"})

	stats := sv.GetStatistics()

	assert.Equal(t, 1, stats["index_advisor_results"])
	assert.Equal(t, 1, stats["unused_indexes"])
	assert.Equal(t, 1, stats["hypothetical_indexes"])
}

// TestQueryIndexAdvisorResults 测试查询索引推荐结果
func TestQueryIndexAdvisorResults(t *testing.T) {
	sv := NewSystemViews()

	result1 := IndexAdvisorResult{
		ID:               "1",
		Database:         "db1",
		TableName:        "t1",
		EstimatedBenefit: 0.9,
	}
	result2 := IndexAdvisorResult{
		ID:               "2",
		Database:         "db2",
		TableName:        "t2",
		EstimatedBenefit: 0.5,
	}

	sv.AddIndexAdvisorResult(result1)
	sv.AddIndexAdvisorResult(result2)

	// 测试按表名过滤
	filter1 := map[string]interface{}{"table_name": "t1"}
	results1 := sv.QueryIndexAdvisorResults(filter1)
	assert.Equal(t, 1, len(results1))
	assert.Equal(t, "t1", results1[0].TableName)

	// 测试按数据库过滤
	filter2 := map[string]interface{}{"database": "db2"}
	results2 := sv.QueryIndexAdvisorResults(filter2)
	assert.Equal(t, 1, len(results2))
	assert.Equal(t, "db2", results2[0].Database)

	// 测试按收益过滤
	filter3 := map[string]interface{}{"min_benefit": 0.7}
	results3 := sv.QueryIndexAdvisorResults(filter3)
	assert.Equal(t, 1, len(results3))
	assert.GreaterOrEqual(t, results3[0].EstimatedBenefit, 0.7)
}

// TestGetTopRecommendedIndexes 测试获取推荐索引排行
func TestGetTopRecommendedIndexes(t *testing.T) {
	sv := NewSystemViews()

	result1 := IndexAdvisorResult{
		ID:               "1",
		EstimatedBenefit: 0.9,
	}
	result2 := IndexAdvisorResult{
		ID:               "2",
		EstimatedBenefit: 0.7,
	}
	result3 := IndexAdvisorResult{
		ID:               "3",
		EstimatedBenefit: 0.8,
	}

	sv.AddIndexAdvisorResult(result1)
	sv.AddIndexAdvisorResult(result2)
	sv.AddIndexAdvisorResult(result3)

	// 获取前 2 个
	top := sv.GetTopRecommendedIndexes(2)

	assert.Equal(t, 2, len(top))
	assert.Equal(t, 0.9, top[0].EstimatedBenefit)
	assert.Equal(t, 0.8, top[1].EstimatedBenefit)
}

// TestConvertRecommendationsToSystemViews 测试转换推荐结果为系统视图格式
func TestConvertRecommendationsToSystemViews(t *testing.T) {
	sv := NewSystemViews()

	recommendations := []*IndexRecommendation{
		{
			RecommendationID: "rec_1",
			TableName:        "t1",
			Columns:          []string{"a", "b"},
			EstimatedBenefit: 0.8,
			Reason:           "test",
			CreateStatement:  "CREATE INDEX idx_a_b ON t1(a,b)",
		},
		{
			RecommendationID: "rec_2",
			TableName:        "t1",
			Columns:          []string{"c"},
			EstimatedBenefit: 0.6,
			Reason:           "test",
			CreateStatement:  "CREATE INDEX idx_c ON t1(c)",
		},
	}

	results := sv.ConvertRecommendationsToSystemViews(recommendations, "test_db")

	assert.Equal(t, 2, len(results))
	assert.Equal(t, "rec_1", results[0].ID)
	assert.Equal(t, "test_db", results[0].Database)
	assert.Equal(t, "t1", results[0].TableName)
	assert.Equal(t, "idx_a_b", results[0].IndexName)
}

// TestGenerateIndexName 测试生成索引名称
func TestGenerateIndexName(t *testing.T) {
	// 测试单列
	name1 := generateIndexName([]string{"a"})
	assert.Equal(t, "idx_a", name1)

	// 测试多列
	name2 := generateIndexName([]string{"a", "b", "c"})
	assert.Equal(t, "idx_a_b_c", name2)

	// 测试空列
	name3 := generateIndexName([]string{})
	assert.Equal(t, "idx_auto", name3)
}

// TestEstimateIndexSizeForSystemViews 测试估算索引大小
func TestEstimateIndexSizeForSystemViews(t *testing.T) {
	size1 := estimateIndexSize([]string{"a"})
	assert.Greater(t, size1, int64(0))

	size2 := estimateIndexSize([]string{"a", "b", "c"})
	assert.Greater(t, size2, size1)
}

// TestToRowMethods 测试行转换方法
func TestToRowMethods(t *testing.T) {
	sv := NewSystemViews()

	// 测试 IndexAdvisorResultToRow
	result := IndexAdvisorResult{
		ID:               "1",
		Database:         "db1",
		TableName:        "t1",
		IndexName:        "idx_a",
		IndexColumns:     []string{"a"},
		EstIndexSize:     1024,
		Reason:           "test",
		CreateStatement:  "CREATE INDEX idx_a ON t1(a)",
		EstimatedBenefit: 0.8,
		Timestamp:        time.Now(),
	}

	row1 := sv.IndexAdvisorResultToRow(result)
	assert.Equal(t, 10, len(row1))
	assert.Equal(t, "1", row1[0])
	assert.Equal(t, "db1", row1[1])

	// 测试 UnusedIndexToRow
	unused := UnusedIndex{
		Database:     "db1",
		TableName:    "t1",
		IndexName:    "idx_old",
		IndexColumns: []string{"old"},
		LastUsedAt:   time.Now(),
		IndexSize:    2048,
		Reason:       "test",
	}

	row2 := sv.UnusedIndexToRow(unused)
	assert.Equal(t, 7, len(row2))
	assert.Equal(t, "db1", row2[0])
	assert.Equal(t, "idx_old", row2[2])

	// 测试 HypotheticalIndexDisplayToRow
	hypo := HypotheticalIndexDisplay{
		ID:            "hyp_1",
		TableName:     "t1",
		IndexColumns:  []string{"a"},
		IsUnique:      false,
		Selectivity:   0.1,
		EstimatedSize: 1024,
		CreatedAt:     time.Now(),
	}

	row3 := sv.HypotheticalIndexDisplayToRow(hypo)
	assert.Equal(t, 7, len(row3))
	assert.Equal(t, "hyp_1", row3[0])
}

// TestGetSystemViewsSingleton 测试单例模式
func TestGetSystemViewsSingleton(t *testing.T) {
	sv1 := GetSystemViews()
	sv2 := GetSystemViews()

	assert.Same(t, sv1, sv2)
}
