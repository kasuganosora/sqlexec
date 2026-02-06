package optimizer

import (
	"strings"
	"testing"
)

// TestNewIndexMerger 测试创建索引合并器
func TestNewIndexMerger(t *testing.T) {
	merger := NewIndexMerger(3)

	if merger == nil {
		t.Fatal("Expected non-nil merger")
	}

	if merger.maxIndexColumns != 3 {
		t.Errorf("Expected maxIndexColumns=3, got %d", merger.maxIndexColumns)
	}

	// 测试默认值
	mergerDefault := NewIndexMerger(0)
	if mergerDefault.maxIndexColumns != 3 {
		t.Errorf("Expected default maxIndexColumns=3, got %d", mergerDefault.maxIndexColumns)
	}
}

// TestCanMerge 测试索引合并检查
func TestCanMerge(t *testing.T) {
	merger := NewIndexMerger(3)

	idx1 := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a"},
		Unique:    false,
		Name:      "idx_a",
	}

	idx2 := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a", "b"},
		Unique:    false,
		Name:      "idx_ab",
	}

	// 相同表，非唯一，有共同前缀，应该可以合并
	if !merger.canMerge(idx1, idx2) {
		t.Error("Expected indexes to be mergeable")
	}

	// 不同表的索引
	idx3 := &MergerIndex{
		TableName: "other_table",
		Columns:   []string{"a"},
		Unique:    false,
		Name:      "idx_a_other",
	}

	if merger.canMerge(idx1, idx3) {
		t.Error("Expected indexes from different tables to not be mergeable")
	}

	// 唯一索引和非唯一索引
	idx4 := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a"},
		Unique:    true,
		Name:      "idx_a_unique",
	}

	if merger.canMerge(idx1, idx4) {
		t.Error("Expected unique and non-unique indexes to not be mergeable")
	}

	// 没有共同前缀的索引
	idx5 := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"c"},
		Unique:    false,
		Name:      "idx_c",
	}

	if merger.canMerge(idx1, idx5) {
		t.Error("Expected indexes without common prefix to not be mergeable")
	}
}

// TestIsContained 测试索引包含检查
func TestIsContained(t *testing.T) {
	merger := NewIndexMerger(3)

	// idx_ab 包含 idx_a
	idxA := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a"},
		Name:      "idx_a",
	}

	idxAB := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a", "b"},
		Name:      "idx_ab",
	}

	if !merger.isContained(idxA, idxAB) {
		t.Error("Expected idx_a to be contained in idx_ab")
	}

	if merger.isContained(idxAB, idxA) {
		t.Error("Expected idx_ab to not be contained in idx_a")
	}

	// 不同表
	idxOther := &MergerIndex{
		TableName: "other_table",
		Columns:   []string{"a"},
		Name:      "idx_a_other",
	}

	if merger.isContained(idxA, idxOther) {
		t.Error("Expected indexes from different tables to not be contained")
	}
}

// TestMergeIndexes 测试索引合并
func TestMergeIndexes(t *testing.T) {
	merger := NewIndexMerger(3)

	// 合并两个有共同前缀的索引
	idxA := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a"},
		Name:      "idx_a",
		Size:      1024,
	}

	idxB := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a", "b"},
		Name:      "idx_ab",
		Size:      2048,
	}

	merged := merger.MergeIndexes([]*MergerIndex{idxA, idxB})

	if merged == nil {
		t.Fatal("Expected non-nil merged index")
	}

	if merged.TableName != "test_table" {
		t.Errorf("Expected TableName=test_table, got %s", merged.TableName)
	}

	// 合并后的列应该是去重的
	if len(merged.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(merged.Columns))
	}

	if merged.Columns[0] != "a" || merged.Columns[1] != "b" {
		t.Errorf("Expected columns [a, b], got %v", merged.Columns)
	}

	// 合并后的索引大小
	if merged.Size != 3072 {
		t.Errorf("Expected Size=3072, got %d", merged.Size)
	}

	// 合并不同表的索引应该返回 nil
	idxOther := &MergerIndex{
		TableName: "other_table",
		Columns:   []string{"a"},
		Name:      "idx_a_other",
	}

	merged = merger.MergeIndexes([]*MergerIndex{idxA, idxOther})
	if merged != nil {
		t.Error("Expected nil when merging indexes from different tables")
	}
}

// TestCalculateMergeBenefit 测试计算合并收益
func TestCalculateMergeBenefit(t *testing.T) {
	merger := NewIndexMerger(3)

	idxA := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a"},
		Name:      "idx_a",
		Size:      1024,
	}

	idxB := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"b"},
		Name:      "idx_b",
		Size:      1024,
	}

	// 合并前：2 个索引，总大小 2048
	before := []*MergerIndex{idxA, idxB}

	// 合并后：1 个索引，假设大小 1500（考虑合并优化）
	merged := &MergerIndex{
		TableName: "test_table",
		Columns:   []string{"a", "b"},
		Name:      "idx_ab",
		Size:      1500,
	}
	after := []*MergerIndex{merged}

	benefit := merger.CalculateMergeBenefit(before, after)

	// 收益 = (2048 - 1500) / 2048 ≈ 0.267
	expectedBenefit := float64(2048-1500) / float64(2048)
	if benefit != expectedBenefit {
		t.Errorf("Expected benefit %f, got %f", expectedBenefit, benefit)
	}

	// 收益应该在 [0, 1] 范围内
	if benefit < 0 || benefit > 1 {
		t.Errorf("Expected benefit in [0, 1], got %f", benefit)
	}
}

// TestFindMergeableIndexes 测试查找可合并索引
func TestFindMergeableIndexes(t *testing.T) {
	merger := NewIndexMerger(3)

	// 创建测试索引
	existingIndexes := []*Index{
		{
			TableName:  "test_table",
			Columns:    []string{"a"},
			Name:       "idx_a",
			Cardinality: 1024,
		},
		{
			TableName:  "test_table",
			Columns:    []string{"a", "b"},
			Name:       "idx_ab",
			Cardinality: 2048,
		},
		{
			TableName:  "test_table",
			Columns:    []string{"c"},
			Name:       "idx_c",
			Cardinality: 1024,
		},
	}

	merges := merger.FindMergeableIndexes(existingIndexes, nil)

	// 应该找到 idx_a 和 idx_ab 的合并
	if len(merges) == 0 {
		t.Error("Expected at least one merge")
	}

	// 检查合并内容
	found := false
	for _, merge := range merges {
		if containsSlice(merge.SourceIndexes, "idx_a") || containsSlice(merge.SourceIndexes, "idx_ab") {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected to find merge involving idx_a and idx_ab")
	}
}

// containsSlice 检查字符串切片是否包含子串
func containsSlice(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// TestGetRecommendedMerges 测试获取推荐合并
func TestGetRecommendedMerges(t *testing.T) {
	merger := NewIndexMerger(3)

	existingIndexes := []*Index{
		{
			TableName:  "test_table",
			Columns:    []string{"a"},
			Name:       "idx_a",
			Cardinality: 1024,
		},
		{
			TableName:  "test_table",
			Columns:    []string{"a", "b"},
			Name:       "idx_ab",
			Cardinality: 2048,
		},
	}

	recommended := merger.GetRecommendedMerges(existingIndexes, nil)

	// 所有推荐应该有收益 >= 10%
	for _, rec := range recommended {
		if rec.Benefit < 0.1 {
			t.Errorf("Expected recommended merge benefit >= 0.1, got %f", rec.Benefit)
		}
	}
}

// TestGenerateMergeStatements 测试生成合并语句
func TestGenerateMergeStatements(t *testing.T) {
	merger := NewIndexMerger(3)

	merges := []IndexMerge{
		{
			SourceIndexes: []string{"idx_a", "idx_b"},
			MergedIndex: &MergerIndex{
				TableName: "test_table",
				Columns:   []string{"a", "b"},
				Name:      "idx_ab",
				Unique:    false,
			},
			Reduction:  2,
			SpaceSaved: 1024,
			Benefit:    0.2,
			Reason:     "Merge compatible indexes",
		},
	}

	statements := merger.GenerateMergeStatements(merges)

	// 应该有 3 个语句：1 个 CREATE, 2 个 DROP
	if len(statements) != 3 {
		t.Errorf("Expected 3 statements, got %d", len(statements))
	}

	// 检查 CREATE INDEX 语句
	createFound := false
	for _, stmt := range statements {
		if strings.Contains(stmt, "CREATE INDEX") && strings.Contains(stmt, "idx_ab") {
			createFound = true
			if !strings.Contains(stmt, "a, b") {
				t.Error("Expected CREATE statement to include columns")
			}
		}
	}

	if !createFound {
		t.Error("Expected CREATE INDEX statement not found")
	}

	// 检查 DROP INDEX 语句
	dropCount := 0
	for _, stmt := range statements {
		if strings.Contains(stmt, "DROP INDEX") {
			dropCount++
		}
	}

	if dropCount != 2 {
		t.Errorf("Expected 2 DROP statements, got %d", dropCount)
	}
}

// TestCalculateSpaceSavings 测试计算空间节省
func TestCalculateSpaceSavings(t *testing.T) {
	merger := NewIndexMerger(3)

	merges := []IndexMerge{
		{SpaceSaved: 1024},
		{SpaceSaved: 2048},
		{SpaceSaved: 512},
	}

	totalSaved := merger.CalculateSpaceSavings(merges)

	expected := int64(1024 + 2048 + 512)
	if totalSaved != expected {
		t.Errorf("Expected total saved %d, got %d", expected, totalSaved)
	}
}

// TestExplainMerge 测试解释合并操作
func TestExplainMerge(t *testing.T) {
	merger := NewIndexMerger(3)

	merge := IndexMerge{
		SourceIndexes: []string{"idx_a", "idx_b"},
		MergedIndex: &MergerIndex{
			TableName: "test_table",
			Columns:   []string{"a", "b"},
			Name:      "idx_ab",
		},
		Reduction:  2,
		SpaceSaved: 1024,
		Benefit:    0.25,
		Reason:     "Merge compatible indexes",
	}

	explanation := merger.ExplainMerge(merge)

	if explanation == "" {
		t.Error("Expected non-empty explanation")
	}

	// 检查关键信息
	if !strings.Contains(explanation, "Merge Reason") {
		t.Error("Expected explanation to contain merge reason")
	}

	if !strings.Contains(explanation, "Source Indexes") {
		t.Error("Expected explanation to contain source indexes")
	}

	if !strings.Contains(explanation, "Reduction") {
		t.Error("Expected explanation to contain reduction")
	}

	if !strings.Contains(explanation, "Space Saved") {
		t.Error("Expected explanation to contain space saved")
	}

	if !strings.Contains(explanation, "Benefit") {
		t.Error("Expected explanation to contain benefit")
	}
}

// TestRemoveDuplicates 测试去重
func TestRemoveDuplicates(t *testing.T) {
	merger := NewIndexMerger(3)

	columns := []string{"a", "b", "a", "c", "b", "d"}
	unique := merger.removeDuplicates(columns)

	expected := []string{"a", "b", "c", "d"}

	if len(unique) != len(expected) {
		t.Errorf("Expected %d unique columns, got %d", len(expected), len(unique))
	}

	for i, col := range expected {
		if unique[i] != col {
			t.Errorf("Expected column %d to be %s, got %s", i, col, unique[i])
		}
	}
}

// TestComplexMergeScenario 测试复杂合并场景
func TestComplexMergeScenario(t *testing.T) {
	merger := NewIndexMerger(4)

	// 创建多个有重叠的索引
	existingIndexes := []*Index{
		{
			TableName:  "orders",
			Columns:    []string{"customer_id"},
			Name:       "idx_customer_id",
			Cardinality: 1024,
		},
		{
			TableName:  "orders",
			Columns:    []string{"customer_id", "order_date"},
			Name:       "idx_customer_date",
			Cardinality: 2048,
		},
		{
			TableName:  "orders",
			Columns:    []string{"customer_id", "order_date", "status"},
			Name:       "idx_customer_date_status",
			Cardinality: 3072,
		},
		{
			TableName:  "orders",
			Columns:    []string{"status"},
			Name:       "idx_status",
			Cardinality: 1024,
		},
	}

	merges := merger.FindMergeableIndexes(existingIndexes, nil)

	// 应该找到多个合并机会
	if len(merges) == 0 {
		t.Error("Expected at least one merge in complex scenario")
	}

	// 检查合并的合理性
	for _, merge := range merges {
		// 合并后的索引不应该超过最大列数
		if merge.MergedIndex != nil && len(merge.MergedIndex.Columns) > merger.maxIndexColumns {
			t.Errorf("Merged index exceeds max columns: %d", len(merge.MergedIndex.Columns))
		}

		// 收益应该是正数
		if merge.Benefit < 0 {
			t.Errorf("Expected positive benefit, got %f", merge.Benefit)
		}
	}
}
