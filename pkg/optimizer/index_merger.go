package optimizer

import (
	"fmt"
	"sort"
	"strings"
)

// IndexMerger 索引合并器
// 用于识别和合并可以合并的索引
type IndexMerger struct {
	maxIndexColumns int
}

// NewIndexMerger 创建索引合并器
func NewIndexMerger(maxColumns int) *IndexMerger {
	if maxColumns <= 0 {
		maxColumns = 3 // 默认最多 3 列
	}
	return &IndexMerger{
		maxIndexColumns: maxColumns,
	}
}

// IndexMerge 索引合并结果
type IndexMerge struct {
	SourceIndexes []string     // 被合并的索引名称
	MergedIndex   *MergerIndex // 合并后的索引
	Benefit       float64      // 收益（成本降低比例）
	Reduction     int          // 减少的索引数量
	SpaceSaved    int64        // 节省的空间（字节）
	Reason        string       // 合并原因
}

// MergerIndex 索引定义（用于合并器）
type MergerIndex struct {
	TableName string
	Columns   []string
	Unique    bool
	Name      string
	Size      int64 // 索引大小（字节）
}

// FindMergeableIndexes 查找可以合并的索引
func (im *IndexMerger) FindMergeableIndexes(existingIndexes []*Index, candidates []*IndexCandidate) []IndexMerge {
	var merges []IndexMerge

	// 将现有索引转换为 MergerIndex
	var allIndexes []*MergerIndex
	for _, idx := range existingIndexes {
		allIndexes = append(allIndexes, &MergerIndex{
			TableName: idx.TableName,
			Columns:   idx.Columns,
			Unique:    idx.Unique,
			Name:      idx.Name,
			Size:      idx.Cardinality, // 使用 Cardinality 作为 Size 的近似值
		})
	}
	for _, candidate := range candidates {
		allIndexes = append(allIndexes, &MergerIndex{
			TableName: candidate.TableName,
			Columns:   candidate.Columns,
			Unique:    candidate.Unique,
			Name:      fmt.Sprintf("idx_%s", strings.Join(candidate.Columns, "_")),
			Size:      1024, // 默认大小
		})
	}

	// 查找前缀兼容的索引对
	merged := make(map[string]bool)
	for i, idx1 := range allIndexes {
		if merged[idx1.Name] {
			continue
		}

		for j, idx2 := range allIndexes {
			if i >= j {
				continue
			}
			if merged[idx2.Name] {
				continue
			}

			// 检查是否可以合并
			if im.canMerge(idx1, idx2) {
				merge := im.createMerge(idx1, idx2)
				merges = append(merges, *merge)
				merged[idx1.Name] = true
				merged[idx2.Name] = true
			}
		}
	}

	// 查找包含关系的索引
	for i, idx1 := range allIndexes {
		if merged[idx1.Name] {
			continue
		}

		for j, idx2 := range allIndexes {
			if i >= j {
				continue
			}
			if merged[idx2.Name] {
				continue
			}

			// 检查是否有包含关系
			if im.isContained(idx1, idx2) {
				// idx2 包含 idx1，可以删除 idx1
				merge := IndexMerge{
					SourceIndexes: []string{idx1.Name},
					MergedIndex:   idx2,
					Reduction:     1,
					SpaceSaved:    idx1.Size,
					Reason:        fmt.Sprintf("Index %s is covered by %s", idx1.Name, idx2.Name),
				}
				merge.Benefit = float64(merge.SpaceSaved) / float64(idx1.Size+idx2.Size)
				merges = append(merges, merge)
				merged[idx1.Name] = true
			} else if im.isContained(idx2, idx1) {
				// idx1 包含 idx2，可以删除 idx2
				merge := IndexMerge{
					SourceIndexes: []string{idx2.Name},
					MergedIndex:   idx1,
					Reduction:     1,
					SpaceSaved:    idx2.Size,
					Reason:        fmt.Sprintf("Index %s is covered by %s", idx2.Name, idx1.Name),
				}
				merge.Benefit = float64(merge.SpaceSaved) / float64(idx1.Size+idx2.Size)
				merges = append(merges, merge)
				merged[idx2.Name] = true
			}
		}
	}

	// 按收益排序
	sort.Slice(merges, func(i, j int) bool {
		return merges[i].Benefit > merges[j].Benefit
	})

	return merges
}

// canMerge 检查两个索引是否可以合并
func (im *IndexMerger) canMerge(idx1, idx2 *MergerIndex) bool {
	// 不同表的索引不能合并
	if idx1.TableName != idx2.TableName {
		return false
	}

	// 唯一索引不能和非唯一索引合并
	if idx1.Unique != idx2.Unique {
		return false
	}

	// 检查前缀兼容
	// 两个索引的前缀列相同，可以合并
	minLen := len(idx1.Columns)
	if len(idx2.Columns) < minLen {
		minLen = len(idx2.Columns)
	}

	// 找出共同的前缀列
	commonPrefix := 0
	for i := 0; i < minLen; i++ {
		if idx1.Columns[i] == idx2.Columns[i] {
			commonPrefix++
		} else {
			break
		}
	}

	// 需要至少有 1 个共同的前缀列
	if commonPrefix < 1 {
		return false
	}

	// 检查合并后的列数是否超过限制
	// 合并策略：去重后保持顺序
	mergedColumns := im.mergeColumns(idx1.Columns, idx2.Columns)
	if len(mergedColumns) > im.maxIndexColumns {
		return false
	}

	return true
}

// createMerge 创建合并操作
func (im *IndexMerger) createMerge(idx1, idx2 *MergerIndex) *IndexMerge {
	// 合并索引
	merged := im.MergeIndexes([]*MergerIndex{idx1, idx2})

	// 计算收益
	totalBefore := idx1.Size + idx2.Size
	benefit := float64(totalBefore-merged.Size) / float64(totalBefore)
	if benefit < 0 {
		benefit = 0
	}

	return &IndexMerge{
		SourceIndexes: []string{idx1.Name, idx2.Name},
		MergedIndex:   merged,
		Reduction:     1,
		SpaceSaved:    totalBefore - merged.Size,
		Reason:        fmt.Sprintf("Merge %s and %s", idx1.Name, idx2.Name),
		Benefit:       benefit,
	}
}

// isContained 检查 idx2 是否包含 idx1
func (im *IndexMerger) isContained(idx1, idx2 *MergerIndex) bool {
	// 不同表的索引
	if idx1.TableName != idx2.TableName {
		return false
	}

	// idx2 的列数必须 >= idx1 的列数
	if len(idx2.Columns) < len(idx1.Columns) {
		return false
	}

	// idx1 的所有列必须在 idx2 中，且顺序一致
	for i, col := range idx1.Columns {
		if idx2.Columns[i] != col {
			return false
		}
	}

	return true
}

// MergeIndexes 合并多个索引为一个索引
func (im *IndexMerger) MergeIndexes(indexes []*MergerIndex) *MergerIndex {
	if len(indexes) == 0 {
		return nil
	}

	if len(indexes) == 1 {
		return indexes[0]
	}

	// 检查所有索引是否属于同一个表
	tableName := indexes[0].TableName
	for _, idx := range indexes {
		if idx.TableName != tableName {
			return nil // 不同表不能合并
		}
	}

	// 合并列：去重并保持顺序
	var allColumns []string
	for _, idx := range indexes {
		allColumns = append(allColumns, idx.Columns...)
	}

	// 去重并保持顺序
	mergedColumns := im.removeDuplicates(allColumns)

	// 合并后的索引是否唯一（所有索引都是唯一）
	isUnique := true
	for _, idx := range indexes {
		if !idx.Unique {
			isUnique = false
			break
		}
	}

	// 合并大小（简单相加）
	totalSize := int64(0)
	for _, idx := range indexes {
		totalSize += idx.Size
	}

	// 生成合并后的索引名称
	name := fmt.Sprintf("idx_merged_%s", strings.Join(mergedColumns, "_"))

	return &MergerIndex{
		TableName: tableName,
		Columns:   mergedColumns,
		Unique:    isUnique,
		Name:      name,
		Size:      totalSize,
	}
}

// CalculateMergeBenefit 计算合并收益
func (im *IndexMerger) CalculateMergeBenefit(before []*MergerIndex, after []*MergerIndex) float64 {
	// 计算合并前的总大小
	totalBefore := int64(0)
	for _, idx := range before {
		totalBefore += idx.Size
	}

	if totalBefore == 0 {
		return 0.0
	}

	// 计算合并后的总大小
	totalAfter := int64(0)
	for _, idx := range after {
		totalAfter += idx.Size
	}

	// 收益 = 节省的空间 / 原始空间
	benefit := float64(totalBefore-totalAfter) / float64(totalBefore)

	// 限制收益范围
	if benefit < 0 {
		benefit = 0.0
	}
	if benefit > 1.0 {
		benefit = 1.0
	}

	return benefit
}

// mergeColumns 合并列（去重并保持顺序）
func (im *IndexMerger) mergeColumns(cols1, cols2 []string) []string {
	var merged []string
	seen := make(map[string]bool)

	// 添加第一个索引的列
	for _, col := range cols1 {
		if !seen[col] {
			merged = append(merged, col)
			seen[col] = true
		}
	}

	// 添加第二个索引的列（保留前缀部分）
	for i, col := range cols2 {
		if !seen[col] {
			// 检查是否是前缀部分
			isPrefix := false
			for j := 0; j < i; j++ {
				if seen[cols2[j]] {
					isPrefix = true
					break
				}
			}
			if isPrefix || i == 0 {
				merged = append(merged, col)
				seen[col] = true
			}
		}
	}

	return merged
}

// removeDuplicates 去重并保持顺序
func (im *IndexMerger) removeDuplicates(columns []string) []string {
	var result []string
	seen := make(map[string]bool)

	for _, col := range columns {
		if !seen[col] {
			result = append(result, col)
			seen[col] = true
		}
	}

	return result
}

// GetRecommendedMerges 获取推荐的合并操作
func (im *IndexMerger) GetRecommendedMerges(existingIndexes []*Index, candidates []*IndexCandidate) []*IndexMerge {
	merges := im.FindMergeableIndexes(existingIndexes, candidates)

	// 过滤收益较低的合并
	var recommended []*IndexMerge
	for _, merge := range merges {
		// 收益至少 10%
		if merge.Benefit >= 0.1 {
			recommended = append(recommended, &merge)
		}
	}

	return recommended
}

// GenerateMergeStatements 生成合并索引的 SQL 语句
func (im *IndexMerger) GenerateMergeStatements(merges []IndexMerge) []string {
	var statements []string

	for _, merge := range merges {
		if merge.MergedIndex == nil {
			continue
		}

		// 生成 CREATE INDEX 语句
		unique := ""
		if merge.MergedIndex.Unique {
			unique = "UNIQUE "
		}

		columns := strings.Join(merge.MergedIndex.Columns, ", ")
		stmt := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
			unique, merge.MergedIndex.Name, merge.MergedIndex.TableName, columns)

		statements = append(statements, stmt)

		// 生成 DROP INDEX 语句
		for _, sourceIdx := range merge.SourceIndexes {
			dropStmt := fmt.Sprintf("DROP INDEX %s", sourceIdx)
			statements = append(statements, dropStmt)
		}
	}

	return statements
}

// CalculateSpaceSavings 计算空间节省
func (im *IndexMerger) CalculateSpaceSavings(merges []IndexMerge) int64 {
	totalSaved := int64(0)
	for _, merge := range merges {
		totalSaved += merge.SpaceSaved
	}
	return totalSaved
}

// ExplainMerge 解释合并操作
func (im *IndexMerger) ExplainMerge(merge IndexMerge) string {
	var explanation strings.Builder
	explanation.WriteString(fmt.Sprintf("Merge Reason: %s\n", merge.Reason))
	explanation.WriteString(fmt.Sprintf("  Source Indexes: %s\n", strings.Join(merge.SourceIndexes, ", ")))
	if merge.MergedIndex != nil {
		explanation.WriteString(fmt.Sprintf("  Merged Index: %s on %s (%s)\n",
			merge.MergedIndex.Name, merge.MergedIndex.TableName,
			strings.Join(merge.MergedIndex.Columns, ", ")))
	}
	explanation.WriteString(fmt.Sprintf("  Reduction: %d indexes\n", merge.Reduction))
	explanation.WriteString(fmt.Sprintf("  Space Saved: %d bytes\n", merge.SpaceSaved))
	explanation.WriteString(fmt.Sprintf("  Benefit: %.2f%%\n", merge.Benefit*100))

	return explanation.String()
}
