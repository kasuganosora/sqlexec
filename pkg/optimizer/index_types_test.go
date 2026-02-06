package optimizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFullTextIndexSupport 测试全文索引支持
func TestFullTextIndexSupport(t *testing.T) {
	fts := NewFullTextIndexSupport()

	// 测试配置
	assert.Equal(t, 4, fts.MinWordLength)
	assert.NotEmpty(t, fts.StopWords)

	// 测试 MATCH AGAINST 表达式识别
	matchExpr := "MATCH(content) AGAINST('search term')"
	assert.True(t, fts.IsFullTextExpression(matchExpr))

	// 测试 LIKE 表达式识别
	likeExpr := "content LIKE '%test%'"
	assert.True(t, fts.IsFullTextExpression(likeExpr))

	// 测试前缀 LIKE（不应使用全文索引）
	prefixExpr := "content LIKE 'test%'"
	assert.False(t, fts.IsFullTextExpression(prefixExpr))

	// 测试列类型兼容性
	assert.True(t, fts.IsColumnTypeCompatible("TEXT"))
	assert.True(t, fts.IsColumnTypeCompatible("VARCHAR"))
	assert.True(t, fts.IsColumnTypeCompatible("LONGTEXT"))
	assert.False(t, fts.IsColumnTypeCompatible("INT"))
	assert.False(t, fts.IsColumnTypeCompatible("BLOB"))

	// 测试停用词检测
	assert.True(t, fts.IsStopWord("the"))
	assert.True(t, fts.IsStopWord("AND"))
	assert.False(t, fts.IsStopWord("important"))

	// 测试分词
	tokens := fts.TokenizeText("The quick brown fox jumps over the lazy dog")
	assert.Contains(t, tokens, "quick")
	assert.Contains(t, tokens, "brown")
	assert.Contains(t, tokens, "jumps")
	assert.NotContains(t, tokens, "the") // 停用词
	assert.NotContains(t, tokens, "and") // 停用词
}

// TestFullTextIndexExtraction 测试全文索引提取
func TestFullTextIndexExtraction(t *testing.T) {
	fts := NewFullTextIndexSupport()

	tableName := "articles"
	expression := "MATCH(title, body) AGAINST('database optimization')"
	columnTypes := map[string]string{
		"title": "VARCHAR",
		"body":  "TEXT",
		"date":  "DATETIME",
	}

	candidates := fts.ExtractFullTextIndexCandidates(tableName, expression, columnTypes)

	// 应该提取出 title 和 body 的索引候选
	assert.GreaterOrEqual(t, len(candidates), 2)

	// 验证候选索引的类型
	for _, candidate := range candidates {
		assert.Equal(t, IndexTypeFullText, candidate.IndexType)
		assert.Equal(t, "FULLTEXT", candidate.Source)
		assert.Equal(t, tableName, candidate.TableName)
		assert.Greater(t, candidate.Priority, 0)
	}
}

// TestFullTextIndexStats 测试全文索引统计信息估算
func TestFullTextIndexStats(t *testing.T) {
	fts := NewFullTextIndexSupport()

	tableName := "articles"
	columns := []string{"title", "body"}
	rowCount := int64(100000)

	stats := fts.EstimateFullTextIndexStats(tableName, columns, rowCount)

	assert.NotNil(t, stats)
	assert.Greater(t, stats.NDV, int64(0))
	assert.Greater(t, stats.Selectivity, 0.0)
	assert.Greater(t, stats.EstimatedSize, int64(0))
	assert.Greater(t, stats.NullFraction, 0.0)
}

// TestFullTextSearchBenefit 测试全文搜索收益计算
func TestFullTextSearchBenefit(t *testing.T) {
	fts := NewFullTextIndexSupport()

	// 测试不同行数的收益
	benefit1 := fts.CalculateFullTextSearchBenefit(1000, 10, true)
	benefit2 := fts.CalculateFullTextSearchBenefit(100000, 10, true)

	assert.Greater(t, benefit2, benefit1)

	// 测试精确匹配 vs 模糊匹配
	exactBenefit := fts.CalculateFullTextSearchBenefit(10000, 10, true)
	fuzzyBenefit := fts.CalculateFullTextSearchBenefit(10000, 10, false)

	assert.Greater(t, exactBenefit, fuzzyBenefit)
}

// TestFullTextIndexDDL 测试全文索引 DDL 生成
func TestFullTextIndexDDL(t *testing.T) {
	fts := NewFullTextIndexSupport()

	ddl := fts.GetFullTextIndexDDL("articles", []string{"title", "body"}, "idx_title_body")

	assert.Contains(t, ddl, "CREATE FULLTEXT INDEX")
	assert.Contains(t, ddl, "idx_title_body")
	assert.Contains(t, ddl, "articles")
	assert.Contains(t, ddl, "title, body")
}

// TestSpatialIndexSupport 测试空间索引支持
func TestSpatialIndexSupport(t *testing.T) {
	sis := NewSpatialIndexSupport()

	// 测试支持的类型
	assert.NotNil(t, sis.SupportedTypes)
	assert.Contains(t, sis.SupportedTypes, "GEOMETRY")
	assert.Contains(t, sis.SupportedTypes, "POINT")
	assert.Contains(t, sis.SupportedTypes, "LINESTRING")
	assert.Contains(t, sis.SupportedTypes, "POLYGON")

	// 测试空间表达式识别
	containsExpr := "ST_Contains(geom, ST_GeomFromText('POINT(1 1)'))"
	assert.True(t, sis.IsSpatialExpression(containsExpr))

	distanceExpr := "ST_Distance(geom1, geom2) < 100"
	assert.True(t, sis.IsSpatialExpression(distanceExpr))

	normalExpr := "col1 = 5"
	assert.False(t, sis.IsSpatialExpression(normalExpr))

	// 测试列类型兼容性
	assert.True(t, sis.IsColumnTypeCompatible("GEOMETRY"))
	assert.True(t, sis.IsColumnTypeCompatible("POINT"))
	assert.True(t, sis.IsColumnTypeCompatible("POLYGON"))
	assert.False(t, sis.IsColumnTypeCompatible("INT"))
	assert.False(t, sis.IsColumnTypeCompatible("VARCHAR"))

	// 测试空间子类型检测
	assert.Equal(t, "POINT", sis.GetSpatialIndexSubType("POINT"))
	assert.Equal(t, "POLYGON", sis.GetSpatialIndexSubType("POLYGON"))
	assert.Equal(t, "LINESTRING", sis.GetSpatialIndexSubType("LINESTRING"))
	assert.Equal(t, "GEOMETRY", sis.GetSpatialIndexSubType("GEOMETRYCOLLECTION"))
}

// TestSpatialFunctionExtraction 测试空间函数提取
func TestSpatialFunctionExtraction(t *testing.T) {
	sis := NewSpatialIndexSupport()

	expr := "ST_Contains(geom, ST_GeomFromText('POINT(1 1)'))"
	funcName, args := sis.ExtractSpatialFunction(expr)

	assert.Equal(t, "ST_CONTAINS", funcName)
	assert.GreaterOrEqual(t, len(args), 2)
}

// TestSpatialIndexExtraction 测试空间索引提取
func TestSpatialIndexExtraction(t *testing.T) {
	sis := NewSpatialIndexSupport()

	tableName := "locations"
	expression := "ST_Within(geom, ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'))"
	columnTypes := map[string]string{
		"geom":     "GEOMETRY",
		"name":     "VARCHAR",
		"location": "POINT",
	}

	candidates := sis.ExtractSpatialIndexCandidates(tableName, expression, columnTypes)

	// 应该提取出 geom 和 location 的索引候选
	assert.GreaterOrEqual(t, len(candidates), 1)

	// 验证候选索引的类型
	for _, candidate := range candidates {
		assert.Equal(t, IndexTypeSpatial, candidate.IndexType)
		assert.Equal(t, tableName, candidate.TableName)
		assert.Greater(t, candidate.Priority, 0)
	}
}

// TestSpatialIndexStats 测试空间索引统计信息估算
func TestSpatialIndexStats(t *testing.T) {
	sis := NewSpatialIndexSupport()

	tableName := "locations"
	columnName := "geom"
	columnType := "GEOMETRY"
	rowCount := int64(50000)

	stats := sis.EstimateSpatialIndexStats(tableName, columnName, columnType, rowCount)

	assert.NotNil(t, stats)
	assert.Greater(t, stats.NDV, int64(0))
	assert.Greater(t, stats.Selectivity, 0.0)
	assert.Greater(t, stats.EstimatedSize, int64(0))
	assert.Greater(t, stats.NullFraction, 0.0)
}

// TestSpatialQueryBenefit 测试空间查询收益计算
func TestSpatialQueryBenefit(t *testing.T) {
	sis := NewSpatialIndexSupport()

	// 测试不同函数类型的收益
	containsBenefit := sis.CalculateSpatialQueryBenefit(SpatialFuncContains, 100000, "POINT")
	distanceBenefit := sis.CalculateSpatialQueryBenefit(SpatialFuncDistance, 100000, "POINT")

	assert.Greater(t, containsBenefit, distanceBenefit)

	// 测试不同查询范围的收益
	smallRectBenefit := sis.CalculateSpatialQueryBenefit(SpatialFuncIntersects, 10000, "SMALL_RECT")
	largeRectBenefit := sis.CalculateSpatialQueryBenefit(SpatialFuncIntersects, 10000, "LARGE_RECT")

	assert.Greater(t, smallRectBenefit, largeRectBenefit)
}

// TestSpatialIndexDDL 测试空间索引 DDL 生成
func TestSpatialIndexDDL(t *testing.T) {
	sis := NewSpatialIndexSupport()

	ddl := sis.GetSpatialIndexDDL("locations", "geom", "idx_geom")

	assert.Contains(t, ddl, "CREATE SPATIAL INDEX")
	assert.Contains(t, ddl, "idx_geom")
	assert.Contains(t, ddl, "locations")
	assert.Contains(t, ddl, "geom")
}

// TestCanUseSpatialIndex 测试空间索引可用性
func TestCanUseSpatialIndex(t *testing.T) {
	sis := NewSpatialIndexSupport()

	// 测试可使用空间索引的函数
	assert.True(t, sis.CanUseSpatialIndex(SpatialFuncContains, []string{"geom1", "geom2"}))
	assert.True(t, sis.CanUseSpatialIndex(SpatialFuncIntersects, []string{"geom1", "geom2"}))

	// 测试在特定条件下可以使用空间索引的函数
	assert.True(t, sis.CanUseSpatialIndex(SpatialFuncDistance, []string{"MBR(geom)", "point"}))
	assert.False(t, sis.CanUseSpatialIndex(SpatialFuncDistance, []string{"geom1", "geom2"}))
}

// TestValidateSpatialFunction 测试空间函数验证
func TestValidateSpatialFunction(t *testing.T) {
	sis := NewSpatialIndexSupport()

	// 测试有效的函数
	err1 := sis.ValidateSpatialFunction(SpatialFuncContains, []string{"geom1", "geom2"})
	assert.NoError(t, err1)

	err2 := sis.ValidateSpatialFunction(SpatialFuncArea, []string{"geom"})
	assert.NoError(t, err2)

	// 测试参数数量错误
	err3 := sis.ValidateSpatialFunction(SpatialFuncContains, []string{"geom"})
	assert.Error(t, err3)

	// 测试无效的函数
	err4 := sis.ValidateSpatialFunction("ST_Invalid", []string{"geom"})
	assert.Error(t, err4)
}

// TestIndexTypeConstants 测试索引类型常量
func TestIndexTypeConstants(t *testing.T) {
	assert.Equal(t, "BTREE", IndexTypeBTree)
	assert.Equal(t, "FULLTEXT", IndexTypeFullText)
	assert.Equal(t, "SPATIAL", IndexTypeSpatial)
}

// TestFullTextConstants 测试全文函数常量
func TestFullTextConstants(t *testing.T) {
	assert.Equal(t, "ST_Contains", SpatialFuncContains)
	assert.Equal(t, "ST_Intersects", SpatialFuncIntersects)
	assert.Equal(t, "MATCH_AGAINST", FullTextFuncMatchAgainst)
	assert.Equal(t, "FULLTEXT", FullTextFuncFulltext)
}

// TestIndexCandidateWithNewTypes 测试包含新类型的索引候选
func TestIndexCandidateWithNewTypes(t *testing.T) {
	// BTree 索引
	btreeIndex := &IndexCandidate{
		TableName: "users",
		Columns:   []string{"id"},
		Priority:  4,
		Source:    "WHERE",
		Unique:    true,
		IndexType: IndexTypeBTree,
	}

	assert.Equal(t, IndexTypeBTree, btreeIndex.IndexType)

	// 全文索引
	fulltextIndex := &IndexCandidate{
		TableName: "articles",
		Columns:   []string{"title"},
		Priority:  4,
		Source:    "FULLTEXT",
		Unique:    false,
		IndexType: IndexTypeFullText,
	}

	assert.Equal(t, IndexTypeFullText, fulltextIndex.IndexType)

	// 空间索引
	spatialIndex := &IndexCandidate{
		TableName: "locations",
		Columns:   []string{"geom"},
		Priority:  4,
		Source:    SpatialFuncWithin,
		Unique:    false,
		IndexType: IndexTypeSpatial,
	}

	assert.Equal(t, IndexTypeSpatial, spatialIndex.IndexType)
}

// TestMBRSizeEstimation 测试 MBR 大小估算
func TestMBRSizeEstimation(t *testing.T) {
	sis := NewSpatialIndexSupport()

	// 测试不同几何类型的 MBR 大小
	pointSize := sis.EstimateMBRSize("POINT", 100)
	lineSize := sis.EstimateMBRSize("LINESTRING", 500)
	polygonSize := sis.EstimateMBRSize("POLYGON", 1000)

	// 点的 MBR 应该最小
	assert.Less(t, pointSize, lineSize)
	assert.Less(t, pointSize, polygonSize)

	// 面的 MBR 应该比线大
	assert.Less(t, lineSize, polygonSize)
}

// TestIndexSelectivityCalculation 测试索引选择性计算
func TestIndexSelectivityCalculation(t *testing.T) {
	sis := NewSpatialIndexSupport()

	// 测试不同查询范围的选择性
	pointSelectivity := sis.CalculateIndexSelectivity(SpatialFuncWithin, "POINT", "LARGE_RECT")
	smallSelectivity := sis.CalculateIndexSelectivity(SpatialFuncWithin, "SMALL_RECT", "LARGE_RECT")
	largeSelectivity := sis.CalculateIndexSelectivity(SpatialFuncWithin, "LARGE_RECT", "LARGE_RECT")

	// 点查询的选择性应该最高
	assert.Less(t, pointSelectivity, smallSelectivity)
	assert.Less(t, pointSelectivity, largeSelectivity)

	// 大范围查询的选择性应该较低
	assert.Greater(t, largeSelectivity, smallSelectivity)
}
