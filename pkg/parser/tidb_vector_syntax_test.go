package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTiDBVectorIndexSyntax 测试 TiDB 向量索引语法支持
func TestTiDBVectorIndexSyntax(t *testing.T) {
	adapter := NewSQLAdapter()

	testCases := []struct {
		name            string
		sql             string
		expectVector    bool
		expectIndexType string
		expectMetric    string
		expectDim       int
		expectColumn    string
	}{
		{
			name:            "TiDB VEC_COSINE_DISTANCE",
			sql:             "CREATE VECTOR INDEX idx_emb ON articles((VEC_COSINE_DISTANCE(embedding)))",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "cosine",
			expectDim:       0, // 维度可能在其他地方获取
			expectColumn:    "embedding",
		},
		{
			name:            "TiDB VEC_L2_DISTANCE",
			sql:             "CREATE VECTOR INDEX idx_vec ON products((VEC_L2_DISTANCE(features)))",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "l2",
			expectColumn:    "features",
		},
		{
			name:            "TiDB VEC_INNER_PRODUCT",
			sql:             "CREATE VECTOR INDEX idx_ip ON items((VEC_INNER_PRODUCT(vec)))",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "inner_product",
			expectColumn:    "vec",
		},
		{
			name:            "TiDB VEC_COSINE_DISTANCE USING HNSW",
			sql:             "CREATE VECTOR INDEX idx_emb ON articles((VEC_COSINE_DISTANCE(embedding))) USING HNSW",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "cosine",
			expectColumn:    "embedding",
		},
		{
			name:            "TiDB mixed syntax - with COMMENT",
			sql:             "CREATE VECTOR INDEX idx_emb ON articles((VEC_COSINE_DISTANCE(embedding))) COMMENT 'dim=768, M=8'",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "cosine",
			expectDim:       768,
			expectColumn:    "embedding",
		},

		{
			name:            "Traditional vector index syntax (backward compatibility)",
			sql:             "CREATE VECTOR INDEX idx_emb ON articles(embedding) USING HNSW COMMENT 'metric=cosine, dim=768'",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "cosine",
			expectDim:       768,
			expectColumn:    "embedding",
		},
		{
			name:            "Regular BTREE index (not vector)",
			sql:             "CREATE INDEX idx_id ON users(id)",
			expectVector:    false,
			expectIndexType: "BTREE",
			expectColumn:    "id",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			require.NoError(t, err, "解析SQL应该成功: %s", tc.sql)
			require.True(t, result.Success, "解析应该成功")

			createIndexStmt := result.Statement.CreateIndex
			require.NotNil(t, createIndexStmt, "应该解析出 CreateIndexStatement")

			// 验证向量索引标识
			require.Equal(t, tc.expectVector, createIndexStmt.IsVectorIndex, "IsVectorIndex")

			// 验证索引类型
			require.Equal(t, tc.expectIndexType, createIndexStmt.IndexType, "IndexType")

			// 验证列名
			require.Equal(t, []string{tc.expectColumn}, createIndexStmt.Columns, "Columns")

			if tc.expectVector {
				// 验证度量类型
				require.Equal(t, tc.expectMetric, createIndexStmt.VectorMetric, "VectorMetric")

				// 如果期望有维度，验证维度
				if tc.expectDim > 0 {
					require.Equal(t, tc.expectDim, createIndexStmt.VectorDim, "VectorDim")
				}
			}
		})
	}
}

// TestTiDBVectorDistanceFunctions 测试向量距离函数解析
func TestTiDBVectorDistanceFunctions(t *testing.T) {
	testCases := []struct {
		name      string
		expr      string
		expectCol string
		expectMet string
	}{
		{
			name:      "VEC_COSINE_DISTANCE",
			expr:      "VEC_COSINE_DISTANCE(embedding)",
			expectCol: "embedding",
			expectMet: "cosine",
		},
		{
			name:      "VEC_L2_DISTANCE",
			expr:      "VEC_L2_DISTANCE(features)",
			expectCol: "features",
			expectMet: "l2",
		},
		{
			name:      "VEC_INNER_PRODUCT",
			expr:      "VEC_INNER_PRODUCT(vec)",
			expectCol: "vec",
			expectMet: "inner_product",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewSQLAdapter()
			sql := "CREATE VECTOR INDEX idx ON foo((" + tc.expr + "))"
			result, err := adapter.Parse(sql)
			require.NoError(t, err, "解析SQL应该成功: %s", sql)
			require.True(t, result.Success)

			createIndexStmt := result.Statement.CreateIndex
			require.NotNil(t, createIndexStmt)
			require.Equal(t, []string{tc.expectCol}, createIndexStmt.Columns, "列名应该正确")
			require.Equal(t, tc.expectMet, createIndexStmt.VectorMetric, "度量类型应该正确")
		})
	}
}

// TestWithClausePreprocess 测试 WITH 子句预处理
func TestWithClausePreprocess(t *testing.T) {
	adapter := NewSQLAdapter()

	testCases := []struct {
		name            string
		sql             string
		expectVector    bool
		expectIndexType string
		expectMetric    string
		expectDim       int
		expectColumn    string
	}{
		{
			name:            "WITH clause after USING HNSW",
			sql:             "CREATE VECTOR INDEX idx_emb ON articles(embedding) USING HNSW WITH (metric='cosine', dim=768, M=8)",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "cosine",
			expectDim:       768,
			expectColumn:    "embedding",
		},
		{
			name:            "WITH clause without USING (default HNSW)",
			sql:             "CREATE VECTOR INDEX idx_emb ON articles(embedding) WITH (metric='cosine', dim=512)",
			expectVector:    true,
			expectIndexType: "VECTOR",
			expectMetric:    "cosine",
			expectDim:       512,
			expectColumn:    "embedding",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := adapter.Parse(tc.sql)
			require.NoError(t, err, "解析SQL应该成功: %s", tc.sql)
			require.True(t, result.Success, "解析应该成功")

			createIndexStmt := result.Statement.CreateIndex
			require.NotNil(t, createIndexStmt, "应该解析出 CreateIndexStatement")

			// 验证向量索引标识
			require.Equal(t, tc.expectVector, createIndexStmt.IsVectorIndex, "IsVectorIndex")

			// 验证索引类型
			require.Equal(t, tc.expectIndexType, createIndexStmt.IndexType, "IndexType")

			// 验证列名
			require.Equal(t, []string{tc.expectColumn}, createIndexStmt.Columns, "Columns")

			if tc.expectVector {
				// 验证度量类型
				require.Equal(t, tc.expectMetric, createIndexStmt.VectorMetric, "VectorMetric")

				// 验证维度
				if tc.expectDim > 0 {
					require.Equal(t, tc.expectDim, createIndexStmt.VectorDim, "VectorDim")
				}
			}
		})
	}
}
