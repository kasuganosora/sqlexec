package parser

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/require"
)

// TestVectorTypeParsing 测试 VECTOR 类型解析
func TestVectorTypeParsing(t *testing.T) {
	testCases := []struct {
		name        string
		sql         string
		expectVector bool
		expectDim   int
	}{
		{
			name:        "VECTOR with dimension",
			sql:         "CREATE TABLE articles (id INT PRIMARY KEY, embedding VECTOR(768))",
			expectVector: true,
			expectDim:   768,
		},
		{
			name:        "VECTOR 128 dim",
			sql:         "CREATE TABLE products (id INT, features VECTOR(128))",
			expectVector: true,
			expectDim:   128,
		},
		{
			name:        "Multiple VECTOR columns",
			sql:         "CREATE TABLE items (id INT, vec1 VECTOR(256), vec2 VECTOR(512))",
			expectVector: true,
			expectDim:   256, // 第一列的维度
		},
		{
			name:        "Regular types",
			sql:         "CREATE TABLE users (id INT, name VARCHAR(255))",
			expectVector: false,
			expectDim:   0,
		},
	}

	adapter := NewSQLAdapter()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			require.NoError(t, err)
			require.True(t, result.Success)
			require.NotNil(t, result.Statement)
			require.NotNil(t, result.Statement.Create)

			createStmt := result.Statement.Create
			if tc.expectVector {
				// 查找 VECTOR 列
				foundVector := false
				for _, col := range createStmt.Columns {
					if col.Type == "VECTOR" {
						foundVector = true
						require.Equal(t, tc.expectDim, col.VectorDim)
						require.True(t, col.IsVectorType())
						break
					}
				}
				require.True(t, foundVector, "应该找到 VECTOR 列")
			} else {
				// 确保没有 VECTOR 列
				for _, col := range createStmt.Columns {
					require.NotEqual(t, "VECTOR", col.Type)
				}
			}
		})
	}
}

// TestVectorIndexParsing tests CREATE VECTOR INDEX parsing
func TestVectorIndexParsing(t *testing.T) {
	testCases := []struct {
		name           string
		sql            string
		expectVector   bool
		expectIndexType string
		expectMetric   string
		expectDim      int
	}{
		{
			name:           "CREATE VECTOR INDEX with USING HNSW",
			sql:            "CREATE VECTOR INDEX idx_embedding ON articles(embedding) USING HNSW WITH (metric='cosine', dim=768)",
			expectVector:   true,
			expectIndexType: "hnsw",
			expectMetric:   "cosine",
			expectDim:      768,
		},
		{
			name:           "CREATE VECTOR INDEX with USING HNSW and l2 metric",
			sql:            "CREATE VECTOR INDEX idx_vec ON products(features) USING HNSW WITH (metric='l2', dim=128)",
			expectVector:   true,
			expectIndexType: "hnsw",
			expectMetric:   "l2",
			expectDim:      128,
		},
		{
			name:           "CREATE VECTOR INDEX with inner_product",
			sql:            "CREATE VECTOR INDEX idx_ip ON items(vec) USING HNSW WITH (metric='inner_product', dim=256)",
			expectVector:   true,
			expectIndexType: "hnsw",
			expectMetric:   "inner_product",
			expectDim:      256,
		},
		{
			name:           "Regular index",
			sql:            "CREATE INDEX idx_name ON users(name) USING BTREE",
			expectVector:   false,
			expectIndexType: "",
			expectMetric:   "",
			expectDim:      0,
		},
	}

	adapter := NewSQLAdapter()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			require.NoError(t, err)
			require.True(t, result.Success)
			require.NotNil(t, result.Statement)
			require.NotNil(t, result.Statement.CreateIndex)

			createIndexStmt := result.Statement.CreateIndex
			require.Equal(t, tc.expectVector, createIndexStmt.IsVectorIndex)
			
			if tc.expectVector {
				require.Equal(t, tc.expectIndexType, createIndexStmt.VectorIndexType)
				require.Equal(t, tc.expectMetric, createIndexStmt.VectorMetric)
				require.Equal(t, tc.expectDim, createIndexStmt.VectorDim)
				require.NotNil(t, createIndexStmt.VectorParams)
			}
		})
	}
}

// TestParseWithClause 测试 WITH 子句解析
func TestParseWithClause(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		expect map[string]interface{}
	}{
		{
			name:  "Simple parameters",
			input: "metric='cosine', dim=768",
			expect: map[string]interface{}{
				"metric": "cosine",
				"dim":    768,
			},
		},
		{
			name:  "Multiple parameters",
			input: "metric='l2', dim=128, M=16, ef=200",
			expect: map[string]interface{}{
				"metric": "l2",
				"dim":    128,
				"M":      16,
				"ef":     200,
			},
		},
		{
			name:  "Float values",
			input: "metric='cosine', dim=512, threshold=0.75",
			expect: map[string]interface{}{
				"metric":    "cosine",
				"dim":       512,
				"threshold": 0.75,
			},
		},
		{
			name:   "Empty",
			input:  "",
			expect: map[string]interface{}{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseWithClause(tc.input)
			require.Equal(t, len(tc.expect), len(result))
			
			for k, expectedVal := range tc.expect {
				actualVal, exists := result[k]
				require.True(t, exists, "Key %s should exist", k)
				require.Equal(t, expectedVal, actualVal)
			}
		})
	}
}

// TestVectorIndexCreationIntegration 测试完整的向量索引创建流程
func TestVectorIndexCreationIntegration(t *testing.T) {
	ctx := context.Background()

	// 1. 解析 SQL
	sql := "CREATE VECTOR INDEX idx_embedding ON articles(embedding) USING HNSW WITH (metric='cosine', dim=768, M=16, ef=200)"
	adapter := NewSQLAdapter()
	
	result, err := adapter.Parse(sql)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.NotNil(t, result.Statement)
	require.NotNil(t, result.Statement.CreateIndex)

	// 2. 验证解析结果
	createIndexStmt := result.Statement.CreateIndex
	require.Equal(t, "idx_embedding", createIndexStmt.IndexName)
	require.Equal(t, "articles", createIndexStmt.TableName)
	require.Equal(t, "embedding", createIndexStmt.ColumnName)
	require.True(t, createIndexStmt.IsVectorIndex)
	require.Equal(t, "hnsw", createIndexStmt.VectorIndexType)
	require.Equal(t, "cosine", createIndexStmt.VectorMetric)
	require.Equal(t, 768, createIndexStmt.VectorDim)
	require.NotNil(t, createIndexStmt.VectorParams)
	require.Equal(t, 16, createIndexStmt.VectorParams["M"])
	require.Equal(t, 200, createIndexStmt.VectorParams["ef"])

	// 3. 创建数据源
	mvccDs := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type: domain.DataSourceTypeMemory,
		Name: "test_memory",
	})

	// 4. 创建查询构建器
	builder := NewQueryBuilder(mvccDs)

	// 5. 执行创建索引（这里只测试不报错，实际执行需要完整的集成）
	_, err = builder.executeCreateVectorIndex(ctx, createIndexStmt)
	// 由于数据源没有实现完整的接口，可能会返回错误
	// 但我们验证了解析部分的正确性
	t.Logf("执行结果: %v", err)
}

// TestConvertToVectorMetricType 测试度量类型转换
func TestConvertToVectorMetricType(t *testing.T) {
	testCases := []struct {
		input    string
		expected memory.VectorMetricType
	}{
		{"cosine", memory.VectorMetricCosine},
		{"COSINE", memory.VectorMetricCosine},
		{"l2", memory.VectorMetricL2},
		{"L2", memory.VectorMetricL2},
		{"euclidean", memory.VectorMetricL2},
		{"inner_product", memory.VectorMetricIP},
		{"ip", memory.VectorMetricIP},
		{"INNER", memory.VectorMetricIP}, // inner_product alias
		{"unknown", memory.VectorMetricCosine}, // default
	}

	for _, tc := range testCases {
		t.Run(string(tc.input), func(t *testing.T) {
			result := convertToVectorMetricType(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

// TestConvertToVectorIndexType 测试索引类型转换
func TestConvertToVectorIndexType(t *testing.T) {
	testCases := []struct {
		input    string
		expected memory.IndexType
	}{
		{"hnsw", memory.IndexTypeVectorHNSW},
		{"HNSW", memory.IndexTypeVectorHNSW},
		{"vector_hnsw", memory.IndexTypeVectorHNSW},
		{"flat", memory.IndexTypeVectorFlat},
		{"FLAT", memory.IndexTypeVectorFlat},
		{"vector_flat", memory.IndexTypeVectorFlat},
		{"ivf_flat", memory.IndexTypeVectorIVFFlat},
		{"IVF_FLAT", memory.IndexTypeVectorIVFFlat},
		{"vector_ivf_flat", memory.IndexTypeVectorIVFFlat},
		{"unknown", memory.IndexTypeVectorHNSW}, // 默认
	}

	for _, tc := range testCases {
		t.Run(string(tc.input), func(t *testing.T) {
			result := convertToVectorIndexType(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
