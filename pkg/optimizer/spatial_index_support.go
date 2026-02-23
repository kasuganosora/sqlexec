package optimizer

import (
	"fmt"
	"regexp"
	"strings"
)

// SpatialIndexSupport 空间索引支持
type SpatialIndexSupport struct {
	// 支持的空间数据类型
	SupportedTypes []string
}

// NewSpatialIndexSupport 创建空间索引支持实例
func NewSpatialIndexSupport() *SpatialIndexSupport {
	return &SpatialIndexSupport{
		SupportedTypes: []string{
			"GEOMETRY", "POINT", "LINESTRING", "POLYGON",
			"MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON",
			"GEOMETRYCOLLECTION",
		},
	}
}

// IsSpatialExpression 检查表达式是否为空间索引相关表达式
func (sis *SpatialIndexSupport) IsSpatialExpression(expr string) bool {
	// 检查是否包含空间函数
	return sis.containsSpatialFunction(expr)
}

// containsSpatialFunction 检查表达式是否包含空间函数
func (sis *SpatialIndexSupport) containsSpatialFunction(expr string) bool {
	spatialFunctions := []string{
		SpatialFuncContains, SpatialFuncIntersects, SpatialFuncWithin,
		SpatialFuncOverlaps, SpatialFuncTouches, SpatialFuncCrosses,
		SpatialFuncDistance, SpatialFuncArea, SpatialFuncLength, SpatialFuncBuffer,
		"ST_CONTAINS", "ST_INTERSECTS", "ST_WITHIN",
		"ST_OVERLAPS", "ST_TOUCHES", "ST_CROSSES",
		"ST_DISTANCE", "ST_AREA", "ST_LENGTH", "ST_BUFFER",
		"ST_CONTAIN", "ST_INTERSECT", // 支持单数形式
	}

	upperExpr := strings.ToUpper(expr)
	for _, funcName := range spatialFunctions {
		if strings.Contains(upperExpr, funcName) {
			return true
		}
	}

	return false
}

// ExtractSpatialFunction 提取空间函数及其参数
func (sis *SpatialIndexSupport) ExtractSpatialFunction(expr string) (string, []string) {
	// 从表达式中提取最外层的函数调用
	expr = strings.TrimSpace(expr)

	// 找到第一个左括号
	openParen := strings.Index(expr, "(")
	if openParen == -1 {
		return "", []string{}
	}

	// 提取函数名
	funcName := strings.TrimSpace(expr[:openParen])
	if funcName == "" {
		return "", []string{}
	}

	// 查找对应的右括号（处理嵌套）
	depth := 1
	closeParen := -1
	for i := openParen + 1; i < len(expr); i++ {
		if expr[i] == '(' {
			depth++
		} else if expr[i] == ')' {
			depth--
			if depth == 0 {
				closeParen = i
				break
			}
		}
	}

	if closeParen == -1 {
		return "", []string{}
	}

	// 提取参数部分
	argsStr := strings.TrimSpace(expr[openParen+1 : closeParen])
	if argsStr == "" {
		return strings.ToUpper(funcName), []string{}
	}

	// 分割参数（考虑嵌套函数）
	var args []string
	depth = 0
	start := 0
	for i := 0; i < len(argsStr); i++ {
		if argsStr[i] == '(' {
			depth++
		} else if argsStr[i] == ')' {
			depth--
		} else if argsStr[i] == ',' && depth == 0 {
			arg := strings.TrimSpace(argsStr[start:i])
			if arg != "" {
				args = append(args, arg)
			}
			start = i + 1
		}
	}
	// 添加最后一个参数
	if start < len(argsStr) {
		arg := strings.TrimSpace(argsStr[start:])
		if arg != "" {
			args = append(args, arg)
		}
	}

	return strings.ToUpper(funcName), args
}

// IsColumnTypeCompatible 检查列类型是否支持空间索引
func (sis *SpatialIndexSupport) IsColumnTypeCompatible(columnType string) bool {
	upperType := strings.ToUpper(columnType)
	for _, supported := range sis.SupportedTypes {
		if upperType == supported || strings.HasPrefix(upperType, supported) {
			return true
		}
	}
	return false
}

// GetSpatialIndexSubType 获取空间索引的具体子类型
func (sis *SpatialIndexSupport) GetSpatialIndexSubType(columnType string) string {
	upperType := strings.ToUpper(columnType)

	switch {
	case strings.HasPrefix(upperType, "POINT"):
		return "POINT"
	case strings.HasPrefix(upperType, "LINESTRING"):
		return "LINESTRING"
	case strings.HasPrefix(upperType, "POLYGON"):
		return "POLYGON"
	case strings.HasPrefix(upperType, "MULTIPOINT"):
		return "MULTIPOINT"
	case strings.HasPrefix(upperType, "MULTILINESTRING"):
		return "MULTILINESTRING"
	case strings.HasPrefix(upperType, "MULTIPOLYGON"):
		return "MULTIPOLYGON"
	default:
		return "GEOMETRY"
	}
}

// ExtractSpatialIndexCandidates 从表达式中提取空间索引候选
func (sis *SpatialIndexSupport) ExtractSpatialIndexCandidates(
	tableName string,
	expression string,
	columnTypes map[string]string,
) []*IndexCandidate {
	var candidates []*IndexCandidate

	// 检查是否为空间表达式
	if !sis.IsSpatialExpression(expression) {
		return candidates
	}

	// 提取空间函数及其参数
	funcName, args := sis.ExtractSpatialFunction(expression)
	if funcName == "" || len(args) == 0 {
		return candidates
	}

	// 从参数中提取空间列引用
	spatialColumns := sis.extractSpatialColumns(args)

	// 为每个兼容的空间列创建索引候选
	for _, col := range spatialColumns {
		colType, exists := columnTypes[col]
		if !exists {
			continue
		}

		if sis.IsColumnTypeCompatible(colType) {
			// 计算优先级：距离查询优先级较低，范围查询优先级较高
			priority := 3
			if funcName == SpatialFuncDistance {
				priority = 2
			} else if funcName == SpatialFuncContains || funcName == SpatialFuncIntersects {
				priority = 4
			}

			candidate := &IndexCandidate{
				TableName: tableName,
				Columns:   []string{col},
				Priority:  priority,
				Source:    funcName,
				Unique:    false,
				IndexType: IndexTypeSpatial,
			}

			candidates = append(candidates, candidate)

			// 添加复合空间索引建议（如果需要多个空间列）
			if len(spatialColumns) > 1 {
				candidate.Columns = spatialColumns
				candidate.Priority = 3
			}
		}
	}

	return candidates
}

// extractSpatialColumns 从参数列表中提取空间列引用
func (sis *SpatialIndexSupport) extractSpatialColumns(args []string) []string {
	var columns []string

	// 匹配列引用模式：table.column 或 column
	pattern := `([a-zA-Z_][a-zA-Z0-9_]*)`
	re := regexp.MustCompile(pattern)

	for _, arg := range args {
		// 排除函数调用和常量
		if strings.Contains(arg, "(") || strings.Contains(arg, "'") {
			continue
		}

		matches := re.FindAllString(arg, -1)
		for _, match := range matches {
			// 过滤关键字
			if !sis.isKeyword(match) {
				columns = append(columns, match)
			}
		}
	}

	return columns
}

// isKeyword 检查是否为 SQL 关键字
func (sis *SpatialIndexSupport) isKeyword(word string) bool {
	keywords := []string{
		"ST", "GEOMETRY", "POINT", "LINESTRING", "POLYGON",
		"FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
	}

	upperWord := strings.ToUpper(word)
	for _, keyword := range keywords {
		if upperWord == keyword {
			return true
		}
	}
	return false
}

// EstimateSpatialIndexStats 估算空间索引统计信息
func (sis *SpatialIndexSupport) EstimateSpatialIndexStats(
	tableName string,
	columnName string,
	columnType string,
	rowCount int64,
) *HypotheticalIndexStats {
	if rowCount == 0 {
		return nil
	}

	subType := sis.GetSpatialIndexSubType(columnType)

	// 估算 R-Tree 节点数量
	// R-Tree 是空间索引的典型数据结构
	// 假设每个 R-Tree 节点可以存储 50-100 个条目
	var rTreeNodes int64
	var estimatedSize int64

	switch subType {
	case "POINT":
		// 点数据：每个点约 16-24 字节（坐标）
		pointSize := int64(20)
		dataSize := int64(rowCount) * pointSize
		// R-Tree 节点数 ≈ 数据量 / (节点容量 * 条目大小)
		rTreeNodes = (dataSize / 50) + 10
		estimatedSize = dataSize + rTreeNodes*100 // 额外的树结构开销

	case "LINESTRING":
		// 线数据：假设平均每条线 10 个点
		lineSize := int64(200)
		dataSize := int64(rowCount) * lineSize
		rTreeNodes = (dataSize / 50) + 10
		estimatedSize = dataSize + rTreeNodes*150

	case "POLYGON":
		// 面数据：假设平均每个面 20 个点
		polygonSize := int64(400)
		dataSize := int64(rowCount) * polygonSize
		rTreeNodes = (dataSize / 40) + 10
		estimatedSize = dataSize + rTreeNodes*200

	default:
		// GEOMETRY 类型：使用通用估计
		geometrySize := int64(300)
		dataSize := int64(rowCount) * geometrySize
		rTreeNodes = (dataSize / 45) + 10
		estimatedSize = dataSize + rTreeNodes*150
	}

	// 估算不同的空间区域数量（NDV）
	// 空间数据的唯一性比较复杂，这里简化估算
	ndv := rowCount / 50
	if ndv < 10 {
		ndv = 10
	}

	// 空间索引的选择性取决于查询范围
	// 假设平均选择性为 0.05
	selectivity := 0.05

	// NULL 值比例
	nullFraction := 0.02

	// 相关性因子
	correlation := 0.2

	stats := &HypotheticalIndexStats{
		NDV:           ndv,
		Selectivity:   selectivity,
		EstimatedSize: estimatedSize,
		NullFraction:  nullFraction,
		Correlation:   correlation,
	}

	return stats
}

// CalculateSpatialQueryBenefit 计算空间查询的收益
func (sis *SpatialIndexSupport) CalculateSpatialQueryBenefit(
	funcName string,
	rowCount int64,
	queryArea string,
) float64 {
	// 基础收益基于行数
	baseBenefit := float64(rowCount) / 50000.0

	// 根据空间函数类型调整收益
	var funcTypeFactor float64
	switch funcName {
	case SpatialFuncContains, SpatialFuncIntersects, SpatialFuncWithin:
		// 范围查询收益最高
		funcTypeFactor = 1.5
	case SpatialFuncOverlaps, SpatialFuncTouches:
		// 相邻查询收益中等
		funcTypeFactor = 1.2
	case SpatialFuncDistance:
		// 距离查询收益较低（因为需要计算所有距离）
		funcTypeFactor = 0.8
	default:
		funcTypeFactor = 1.0
	}

	// 查询范围影响：查询范围越小，选择性越高，收益越大
	var areaFactor float64
	switch queryArea {
	case "POINT":
		areaFactor = 1.5
	case "SMALL_RECT":
		areaFactor = 1.3
	case "MEDIUM_RECT":
		areaFactor = 1.0
	case "LARGE_RECT":
		areaFactor = 0.7
	default:
		areaFactor = 1.0
	}

	totalBenefit := baseBenefit * funcTypeFactor * areaFactor

	return totalBenefit
}

// GetSpatialIndexDDL 生成创建空间索引的 DDL
func (sis *SpatialIndexSupport) GetSpatialIndexDDL(
	tableName string,
	columnName string,
	indexName string,
) string {
	if indexName == "" {
		indexName = fmt.Sprintf("sp_%s_%s", tableName, columnName)
	}

	// SPATIAL 索引语法（MySQL 风格）
	ddl := fmt.Sprintf("CREATE SPATIAL INDEX %s ON %s(%s)",
		indexName, tableName, columnName)

	return ddl
}

// OptimizeSpatialQuery 优化空间查询建议
func (sis *SpatialIndexSupport) OptimizeSpatialQuery(
	query string,
	tables map[string]bool,
) []string {
	var suggestions []string

	// 检查是否有空间函数但没有空间索引
	if sis.IsSpatialExpression(query) {
		suggestions = append(suggestions,
			"Consider adding SPATIAL indexes for geometry columns used in spatial queries",
		)
	}

	// 检查是否使用了距离计算但没有边界框优化
	if strings.Contains(strings.ToUpper(query), SpatialFuncDistance) &&
		!strings.Contains(strings.ToUpper(query), "MBR") {
		suggestions = append(suggestions,
			"Consider using MBR (Minimum Bounding Rectangle) to optimize distance queries",
		)
	}

	return suggestions
}

// EstimateMBRSize 估算最小边界框大小（用于查询优化）
func (sis *SpatialIndexSupport) EstimateMBRSize(
	subType string,
	estimatedSize int64,
) float64 {
	// MBR 通常是实际几何对象大小的一部分
	var mbrRatio float64

	switch subType {
	case "POINT":
		mbrRatio = 0.1 // 点的 MBR 接近点本身
	case "LINESTRING":
		mbrRatio = 0.3 // 线的 MBR 可能比线大 3 倍
	case "POLYGON":
		mbrRatio = 0.5 // 面的 MBR 可能比面大 2 倍
	default:
		mbrRatio = 0.4
	}

	return float64(estimatedSize) * mbrRatio
}

// CanUseSpatialIndex 检查空间查询是否可以使用空间索引
func (sis *SpatialIndexSupport) CanUseSpatialIndex(funcName string, args []string) bool {
	// 这些空间函数可以利用空间索引
	indexableFunctions := []string{
		SpatialFuncContains, SpatialFuncIntersects, SpatialFuncWithin,
		SpatialFuncOverlaps, SpatialFuncTouches, SpatialFuncCrosses,
	}

	for _, indexable := range indexableFunctions {
		if funcName == indexable {
			return true
		}
	}

	// ST_Distance 函数在有 MBR 约束时也可以部分使用索引
	if funcName == SpatialFuncDistance {
		// 检查是否有 MBR 约束
		for _, arg := range args {
			if strings.Contains(strings.ToUpper(arg), "MBR") {
				return true
			}
		}
	}

	return false
}

// CalculateIndexSelectivity 计算空间索引的选择性
func (sis *SpatialIndexSupport) CalculateIndexSelectivity(
	funcName string,
	queryRange string,
	dataRange string,
) float64 {
	// 简化版本：基于查询范围与数据范围的比例
	// 实际实现需要更复杂的几何计算

	selectivity := 0.1

	// 范围查询的选择性取决于查询范围大小
	switch queryRange {
	case "POINT":
		selectivity = 0.01
	case "SMALL_RECT":
		selectivity = 0.05
	case "MEDIUM_RECT":
		selectivity = 0.15
	case "LARGE_RECT":
		selectivity = 0.30
	}

	// 某些函数的选择性更高
	if funcName == SpatialFuncWithin || funcName == SpatialFuncContains {
		selectivity *= 0.8
	}

	return selectivity
}

// ValidateSpatialFunction 验证空间函数的有效性
func (sis *SpatialIndexSupport) ValidateSpatialFunction(funcName string, args []string) error {
	// 验证函数名是否有效
	validFunctions := map[string]int{
		SpatialFuncContains:   2,
		SpatialFuncIntersects: 2,
		SpatialFuncWithin:     2,
		SpatialFuncOverlaps:   2,
		SpatialFuncTouches:    2,
		SpatialFuncCrosses:    2,
		SpatialFuncDistance:   2,
		SpatialFuncArea:       1,
		SpatialFuncLength:     1,
		SpatialFuncBuffer:     2,
	}

	expectedArgs, exists := validFunctions[funcName]
	if !exists {
		return fmt.Errorf("unknown spatial function: %s", funcName)
	}

	if len(args) != expectedArgs {
		return fmt.Errorf("%s expects %d arguments, got %d",
			funcName, expectedArgs, len(args))
	}

	return nil
}
