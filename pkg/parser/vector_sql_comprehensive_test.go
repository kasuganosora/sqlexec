package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVectorTypeParsingComprehensive 全面测试 VECTOR 类型解析
func TestVectorTypeParsingComprehensive(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		tableName string
		columns   int
		checkFunc func(*CreateStatement) bool
	}{
		{
			name:      "basic_vector_column",
			sql:       "CREATE TABLE articles (id INT, embedding VECTOR(768))",
			tableName: "articles",
			columns:   2,
			checkFunc: func(stmt *CreateStatement) bool {
				return stmt.Columns[1].Name == "embedding" && stmt.Columns[1].VectorDim == 768
			},
		},
		{
			name:      "vector_with_primary_key",
			sql:       "CREATE TABLE items (id INT PRIMARY KEY, vec VECTOR(128))",
			tableName: "items",
			columns:   2,
			checkFunc: func(stmt *CreateStatement) bool {
				return stmt.Columns[1].Name == "vec" && stmt.Columns[1].VectorDim == 128
			},
		},
		{
			name:      "vector_with_varchar",
			sql:       "CREATE TABLE docs (id INT, title VARCHAR(255), embedding VECTOR(512))",
			tableName: "docs",
			columns:   3,
			checkFunc: func(stmt *CreateStatement) bool {
				return len(stmt.Columns) == 3 &&
					stmt.Columns[2].Name == "embedding" &&
					stmt.Columns[2].VectorDim == 512
			},
		},
		{
			name:      "small_dimension",
			sql:       "CREATE TABLE vec (id INT, v VECTOR(16))",
			tableName: "vec",
			columns:   2,
			checkFunc: func(stmt *CreateStatement) bool {
				return stmt.Columns[1].Name == "v" && stmt.Columns[1].VectorDim == 16
			},
		},
		{
			name:      "large_dimension",
			sql:       "CREATE TABLE big (id INT, v VECTOR(4096))",
			tableName: "big",
			columns:   2,
			checkFunc: func(stmt *CreateStatement) bool {
				return stmt.Columns[1].Name == "v" && stmt.Columns[1].VectorDim == 4096
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewSQLAdapter()
			result, err := adapter.Parse(tc.sql)

			require.NoError(t, err, "解析SQL应该成功: %s", tc.sql)
			require.NotNil(t, result)
			require.True(t, result.Success, "解析应该成功")
			require.NotNil(t, result.Statement.Create)

			stmt := result.Statement.Create
			require.Equal(t, tc.tableName, stmt.Name, "表名不匹配")
			require.Len(t, stmt.Columns, tc.columns, "列数不匹配")

			if tc.checkFunc != nil {
				require.True(t, tc.checkFunc(stmt), "自定义检查失败")
			}

			t.Logf("✅ 解析成功: 表=%s, 列=%d", stmt.Name, len(stmt.Columns))
			for _, col := range stmt.Columns {
				if col.VectorDim > 0 {
					t.Logf("  - 向量列: %s, 维度=%d", col.Name, col.VectorDim)
				}
			}
		})
	}
}

// TestVectorIndexParsingComprehensive 全面测试 CREATE VECTOR INDEX 解析
func TestVectorIndexParsingComprehensive(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		indexName string
		tableName string
		column    string
		checkFunc func(*CreateIndexStatement) bool
	}{
		{
			name:      "basic_hnsw_index",
			sql:       "CREATE VECTOR INDEX idx_emb ON articles(embedding) USING HNSW WITH (metric='cosine', dim=768)",
			indexName: "idx_emb",
			tableName: "articles",
			column:    "embedding",
			checkFunc: func(stmt *CreateIndexStatement) bool {
				return stmt.IsVectorIndex &&
					stmt.VectorIndexType == "hnsw" &&
					stmt.VectorMetric == "cosine" &&
					stmt.VectorDim == 768
			},
		},
		{
			name:      "flat_index_l2",
			sql:       "CREATE VECTOR INDEX idx_feat ON products(features) USING HNSW WITH (metric='l2', dim=128)",
			indexName: "idx_feat",
			tableName: "products",
			column:    "features",
			checkFunc: func(stmt *CreateIndexStatement) bool {
				return stmt.IsVectorIndex &&
					stmt.VectorIndexType == "hnsw" &&
					stmt.VectorMetric == "l2" &&
					stmt.VectorDim == 128
			},
		},
		{
			name:      "inner_product_index",
			sql:       "CREATE VECTOR INDEX idx_ip ON items(vec) USING HNSW WITH (metric='inner_product', dim=256)",
			indexName: "idx_ip",
			tableName: "items",
			column:    "vec",
			checkFunc: func(stmt *CreateIndexStatement) bool {
				return stmt.IsVectorIndex &&
					stmt.VectorMetric == "inner_product" &&
					stmt.VectorDim == 256
			},
		},
		{
			name:      "index_with_params",
			sql:       "CREATE VECTOR INDEX idx_hnsw ON docs(emb) USING HNSW WITH (metric='cosine', dim=512, M=16, ef=200)",
			indexName: "idx_hnsw",
			tableName: "docs",
			column:    "emb",
			checkFunc: func(stmt *CreateIndexStatement) bool {
				if stmt.VectorParams == nil {
					return false
				}
				m, ok1 := stmt.VectorParams["M"]
				ef, ok2 := stmt.VectorParams["ef"]
				return ok1 && ok2 && m == 16 && ef == 200
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewSQLAdapter()
			result, err := adapter.Parse(tc.sql)

			require.NoError(t, err, "解析SQL应该成功: %s", tc.sql)
			require.NotNil(t, result)
			require.True(t, result.Success, "解析应该成功")
			require.NotNil(t, result.Statement.CreateIndex)

			stmt := result.Statement.CreateIndex
			require.Equal(t, tc.indexName, stmt.IndexName, "索引名不匹配")
			require.Equal(t, tc.tableName, stmt.TableName, "表名不匹配")
			require.Equal(t, []string{tc.column}, stmt.Columns, "列名不匹配")

			if tc.checkFunc != nil {
				require.True(t, tc.checkFunc(stmt), "自定义检查失败")
			}

			t.Logf("✅ 解析成功: 索引=%s, 表=%s, 列=%v",
				stmt.IndexName, stmt.TableName, stmt.Columns)
			t.Logf("  - 类型=%s, 度量=%s, 维度=%d, 向量索引=%v",
				stmt.IndexType, stmt.VectorMetric, stmt.VectorDim, stmt.IsVectorIndex)
			if stmt.VectorParams != nil {
				t.Logf("  - 参数: %+v", stmt.VectorParams)
			}
		})
	}
}

// TestVectorDistanceInOrderBy 全面测试 ORDER BY 向量距离函数解析
func TestVectorDistanceInOrderBy(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		checkFunc func(*SelectStatement) bool
	}{
		{
			name: "cosine_distance",
			sql:  "SELECT * FROM articles ORDER BY vec_cosine_distance(embedding, '[0.1, 0.2]') LIMIT 10",
			checkFunc: func(stmt *SelectStatement) bool {
				return len(stmt.OrderBy) > 0 && stringContains(stmt.OrderBy[0].Column, "vec_cosine_distance")
			},
		},
		{
			name: "l2_distance",
			sql:  "SELECT * FROM products ORDER BY vec_l2_distance(features, '[1.0, 2.0]') LIMIT 5",
			checkFunc: func(stmt *SelectStatement) bool {
				return len(stmt.OrderBy) > 0 && stringContains(stmt.OrderBy[0].Column, "vec_l2_distance")
			},
		},
		{
			name: "inner_product_distance",
			sql:  "SELECT * FROM items ORDER BY vec_inner_product_distance(vec, '[0.5, 0.5]') LIMIT 10",
			checkFunc: func(stmt *SelectStatement) bool {
				return len(stmt.OrderBy) > 0 && stringContains(stmt.OrderBy[0].Column, "vec_inner_product_distance")
			},
		},
		{
			name: "distance_with_filter",
			sql:  "SELECT * FROM articles WHERE category='tech' ORDER BY vec_cosine_distance(embedding, '[0.1, 0.2]') LIMIT 10",
			checkFunc: func(stmt *SelectStatement) bool {
				return len(stmt.OrderBy) > 0 &&
					stringContains(stmt.OrderBy[0].Column, "vec_cosine_distance") &&
					stmt.Where != nil
			},
		},
		{
			name: "distance_with_projection",
			sql:  "SELECT id, title FROM articles ORDER BY vec_cosine_distance(embedding, '[0.1, 0.2]') LIMIT 10",
			checkFunc: func(stmt *SelectStatement) bool {
				return len(stmt.OrderBy) > 0 &&
					stringContains(stmt.OrderBy[0].Column, "vec_cosine_distance") &&
					len(stmt.Columns) == 2
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewSQLAdapter()
			result, err := adapter.Parse(tc.sql)

			require.NoError(t, err, "解析SQL应该成功: %s", tc.sql)
			require.NotNil(t, result)
			require.True(t, result.Success, "解析应该成功")
			require.NotNil(t, result.Statement.Select)

			stmt := result.Statement.Select

			if tc.checkFunc != nil {
				require.True(t, tc.checkFunc(stmt), "自定义检查失败")
			}

			t.Logf("✅ 解析成功: FROM=%s, ORDER BY=%v",
				stmt.From, stmt.OrderBy)
			if len(stmt.OrderBy) > 0 {
				t.Logf("  - 距离函数: %s, 方向: %s",
					stmt.OrderBy[0].Column, stmt.OrderBy[0].Direction)
			}
			if stmt.Where != nil {
				t.Logf("  - WHERE条件存在")
			}
		})
	}
}

// TestCompleteVectorSearchSQLWorkflow 测试完整的向量搜索 SQL 工作流
func TestCompleteVectorSearchSQLWorkflow(t *testing.T) {
	workflow := []struct {
		step string
		sql  string
		desc string
	}{
		{
			step: "1",
			sql:  "CREATE TABLE articles (id INT PRIMARY KEY, title VARCHAR(255), embedding VECTOR(768))",
			desc: "创建包含向量列的表",
		},
		{
			step: "2",
			sql:  "CREATE VECTOR INDEX idx_emb ON articles(embedding) USING HNSW WITH (metric='cosine', dim=768, M=16, ef=200)",
			desc: "创建HNSW向量索引",
		},
		{
			step: "3",
			sql:  "INSERT INTO articles (id, title, embedding) VALUES (1, 'Test Article', '[0.1, 0.2, 0.3]')",
			desc: "插入测试数据",
		},
		{
			step: "4",
			sql:  "SELECT * FROM articles ORDER BY vec_cosine_distance(embedding, '[0.1, 0.2, 0.3]') LIMIT 10",
			desc: "执行向量搜索查询",
		},
		{
			step: "5",
			sql:  "SELECT id, title FROM articles WHERE category='tech' ORDER BY vec_cosine_distance(embedding, '[0.1, 0.2]') LIMIT 5",
			desc: "执行带过滤条件的向量搜索",
		},
	}

	adapter := NewSQLAdapter()

	for _, step := range workflow {
		t.Run("Step_"+step.step, func(t *testing.T) {
			t.Logf("执行步骤 %s: %s", step.step, step.desc)
			t.Logf("SQL: %s", step.sql)

			result, err := adapter.Parse(step.sql)
			require.NoError(t, err, "解析SQL应该成功")
			require.NotNil(t, result)
			require.True(t, result.Success, "解析应该成功")

			t.Logf("✅ 步骤 %s 完成", step.step)
		})
	}

	t.Log("\n✅ 完整向量搜索 SQL 工作流测试完成")
}

// 辅助函数
func stringContains(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
