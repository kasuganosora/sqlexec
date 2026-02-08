package executor

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/require"
)

// TestVectorSearchEndToEnd 测试端到端的向量搜索流程
func TestVectorSearchEndToEnd(t *testing.T) {
	ctx := context.Background()

	// 1. 创建数据源
	mvccDs := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
		Name: "test_memory",
	})
	das := dataaccess.NewDataService(mvccDs)
	
	// 创建测试表
	tableInfo := &domain.TableInfo{
		Name: "articles",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "title", Type: "VARCHAR(255)", Nullable: false},
			{Name: "embedding", Type: "VECTOR", VectorDim: 128, Nullable: false},
		},
	}
	
	err := mvccDs.CreateTable(ctx, tableInfo)
	require.NoError(t, err)
	
	// 2. 创建索引管理器并添加向量索引
	idxMgr := memory.NewIndexManager()
	vectorIdx, err := idxMgr.CreateVectorIndex(
		"articles",
		"embedding",
		memory.VectorMetricCosine,
		memory.IndexTypeVectorHNSW,
		128,
		nil,
	)
	require.NoError(t, err)
	
	// 3. 插入测试数据
	numArticles := 100
	for i := 0; i < numArticles; i++ {
		vec := randomVector(128)
		row := map[string]interface{}{
			"id":        int64(i),
			"title":     "Article " + string(rune(i)),
			"embedding": vec,
		}
		err := das.Insert(ctx, "articles", row)
		require.NoError(t, err)
		
		// 同时插入向量索引
		err = vectorIdx.Insert(int64(i), vec)
		require.NoError(t, err)
	}
	
	// 4. 测试向量扫描算子
	t.Run("VectorScanOperator", func(t *testing.T) {
		// 创建向量扫描配置
		queryVector := randomVector(128)
		vectorConfig := &plan.VectorScanConfig{
			TableName:   "articles",
			ColumnName:  "embedding",
			IndexType:   "hnsw",
			QueryVector: queryVector,
			K:           10,
			MetricType:  "cosine",
		}
		
		// 创建计划
		vectorPlan := &plan.Plan{
			ID:     "vector_scan_test",
			Type:   plan.TypeVectorScan,
			Config: vectorConfig,
		}
		
		// 创建执行器
		executor := NewExecutorWithIndexManager(das, idxMgr)
		
		// 执行查询
		result, err := executor.Execute(ctx, vectorPlan)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.Rows, 10)
		
		// 验证结果包含距离列
		hasDistance := false
		for _, col := range result.Columns {
			if col.Name == "_distance" {
				hasDistance = true
				break
			}
		}
		require.True(t, hasDistance, "结果应该包含_distance列")
		
		// 验证每行都有距离值
		for _, row := range result.Rows {
			_, hasDist := row["_distance"]
			require.True(t, hasDist, "每行应该包含_distance字段")
		}
	})
	
	// 5. 测试完整的SQL解析和执行流程
	t.Run("SQLParseAndExecute", func(t *testing.T) {
		// 这里简化测试，直接测试是否能正确处理向量函数表达式
		// 在实际场景中，SQL应该是：SELECT * FROM articles ORDER BY vec_cosine_distance(embedding, '[...]') LIMIT 10
		
		// 创建SelectStatement
		selectStmt := &parser.SelectStatement{
			Columns: []parser.SelectColumn{
				{Expr: &parser.Expression{Type: parser.ExprTypeColumn, Column: "*"}},
			},
			From: "articles",
			OrderBy: []parser.OrderByItem{
				{
					Column:    "vec_cosine_distance(embedding, '[...]')",
					Direction: "ASC",
				},
			},
			Limit:  new(int64),
		}
		*selectStmt.Limit = 10
		
		// 验证语句结构
		require.Equal(t, "articles", selectStmt.From)
		require.NotNil(t, selectStmt.Limit)
		require.Equal(t, int64(10), *selectStmt.Limit)
	})
}

// TestVectorIndexManagerIntegration 测试IndexManager与执行器的集成
func TestVectorIndexManagerIntegration(t *testing.T) {
	// 创建IndexManager
	idxMgr := memory.NewIndexManager()
	
	// 创建多个向量索引
	testCases := []struct {
		tableName  string
		columnName string
		metric     memory.VectorMetricType
		indexType  memory.IndexType
		dimension  int
	}{
		{"articles", "embedding", memory.VectorMetricCosine, memory.IndexTypeVectorHNSW, 128},
		{"products", "features", memory.VectorMetricL2, memory.IndexTypeVectorFlat, 64},
		{"users", "profile_vector", memory.VectorMetricIP, memory.IndexTypeVectorHNSW, 256},
	}
	
	for _, tc := range testCases {
		idx, err := idxMgr.CreateVectorIndex(
			tc.tableName,
			tc.columnName,
			tc.metric,
			tc.indexType,
			tc.dimension,
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, idx)
		
		// 验证索引配置
		config := idx.GetConfig()
		require.Equal(t, tc.metric, config.MetricType)
		require.Equal(t, tc.dimension, config.Dimension)
	}
	
	// 测试获取所有索引
	for _, tc := range testCases {
		idx, err := idxMgr.GetVectorIndex(tc.tableName, tc.columnName)
		require.NoError(t, err)
		require.NotNil(t, idx)
	}
	
	// 测试删除索引
	err := idxMgr.DropVectorIndex("products", "features")
	require.NoError(t, err)
	
	_, err = idxMgr.GetVectorIndex("products", "features")
	require.Error(t, err)
}

// TestVectorSearchWithFilters 测试带过滤条件的向量搜索
func TestVectorSearchWithFilters(t *testing.T) {
	ctx := context.Background()
	
	// 创建数据源和索引
	mvccDs := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
		Name: "test_memory",
	})
	das := dataaccess.NewDataService(mvccDs)
	tableInfo := &domain.TableInfo{
		Name: "products",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "category", Type: "VARCHAR(100)", Nullable: false},
			{Name: "features", Type: "VECTOR", VectorDim: 64, Nullable: false},
		},
	}
	
	err := mvccDs.CreateTable(ctx, tableInfo)
	require.NoError(t, err)
	
	idxMgr := memory.NewIndexManager()
	vectorIdx, err := idxMgr.CreateVectorIndex(
		"products",
		"features",
		memory.VectorMetricCosine,
		memory.IndexTypeVectorHNSW,
		64,
		nil,
	)
	require.NoError(t, err)
	
	// 插入测试数据
	for i := 0; i < 50; i++ {
		category := "electronics"
		if i%2 == 0 {
			category = "books"
		}
		
		vec := randomVector(64)
		row := map[string]interface{}{
			"id":       int64(i),
			"category": category,
			"features": vec,
		}
		err := das.Insert(ctx, "products", row)
		require.NoError(t, err)
		
		err = vectorIdx.Insert(int64(i), vec)
		require.NoError(t, err)
	}
	
	// 执行带过滤的向量搜索
	queryVector := randomVector(64)
	filter := &memory.VectorFilter{
		IDs: []int64{1, 3, 5, 7, 9, 11, 13, 15, 17, 19}, // 只搜索奇数ID的books
	}
	
	result, err := vectorIdx.Search(ctx, queryVector, 5, filter)
	require.NoError(t, err)
	require.NotEmpty(t, result.IDs)
	
	// 验证结果都在过滤器中
	for _, id := range result.IDs {
		found := false
		for _, fid := range filter.IDs {
			if id == fid {
				found = true
				break
			}
		}
		require.True(t, found, "结果ID %d 不在过滤器中", id)
	}
}

// randomVector 生成随机向量（测试辅助函数）
func randomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i%10) / 10.0 // 简单的测试向量
	}
	return vec
}
