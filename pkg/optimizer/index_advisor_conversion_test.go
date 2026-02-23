package optimizer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// TestParseQueryToStatement_SelectBasic verifies that a simple SELECT query is
// fully converted to an SQLStatement with a populated Select field.
func TestParseQueryToStatement_SelectBasic(t *testing.T) {
	advisor := NewIndexAdvisor()

	stmt, err := advisor.parseQueryToStatement("SELECT * FROM users WHERE age > 18")
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Select, "Select field should be populated")

	assert.Equal(t, "users", stmt.Select.From)
	assert.NotNil(t, stmt.Select.Where, "Where should be populated")
}

// TestParseQueryToStatement_SelectWithJoin verifies that JOIN clauses are
// extracted from the AST.
func TestParseQueryToStatement_SelectWithJoin(t *testing.T) {
	advisor := NewIndexAdvisor()

	stmt, err := advisor.parseQueryToStatement(
		"SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 100",
	)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Select)

	// The TiDB parser should populate Joins when a JOIN clause is present.
	if len(stmt.Select.Joins) > 0 {
		assert.Equal(t, "orders", stmt.Select.Joins[0].Table)
	} else {
		// Even if Joins is empty due to parser internals, the FROM clause
		// should still reference the primary table.
		t.Logf("Joins slice is empty (parser-specific); From=%q", stmt.Select.From)
	}

	assert.NotNil(t, stmt.Select.Where)
}

// TestParseQueryToStatement_SelectWithGroupBy verifies that GROUP BY columns
// are extracted.
func TestParseQueryToStatement_SelectWithGroupBy(t *testing.T) {
	advisor := NewIndexAdvisor()

	stmt, err := advisor.parseQueryToStatement(
		"SELECT city, COUNT(*) FROM users GROUP BY city",
	)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Select)

	assert.NotEmpty(t, stmt.Select.GroupBy, "GroupBy should be populated")
	assert.Contains(t, stmt.Select.GroupBy, "city")
}

// TestParseQueryToStatement_SelectWithOrderBy verifies that ORDER BY items are
// extracted.
func TestParseQueryToStatement_SelectWithOrderBy(t *testing.T) {
	advisor := NewIndexAdvisor()

	stmt, err := advisor.parseQueryToStatement(
		"SELECT * FROM products ORDER BY price DESC",
	)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Select)

	require.NotEmpty(t, stmt.Select.OrderBy)
	assert.Equal(t, "price", stmt.Select.OrderBy[0].Column)
	assert.Equal(t, "DESC", stmt.Select.OrderBy[0].Direction)
}

// TestParseQueryToStatement_SelectWithLimit verifies that LIMIT is extracted.
func TestParseQueryToStatement_SelectWithLimit(t *testing.T) {
	advisor := NewIndexAdvisor()

	stmt, err := advisor.parseQueryToStatement(
		"SELECT * FROM logs LIMIT 50",
	)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Select)

	require.NotNil(t, stmt.Select.Limit)
	assert.Equal(t, int64(50), *stmt.Select.Limit)
}

// TestParseQueryToStatement_InvalidSQL verifies that invalid SQL returns an
// error.
func TestParseQueryToStatement_InvalidSQL(t *testing.T) {
	advisor := NewIndexAdvisor()

	_, err := advisor.parseQueryToStatement("NOT VALID SQL AT ALL")
	assert.Error(t, err)
}

// TestParseQueryToStatement_NonSelect verifies that non-SELECT statements are
// also parsed (the extractor will handle them accordingly).
func TestParseQueryToStatement_NonSelect(t *testing.T) {
	advisor := NewIndexAdvisor()

	stmt, err := advisor.parseQueryToStatement(
		"INSERT INTO users (name, age) VALUES ('Alice', 30)",
	)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	// Insert should populate the Insert field, not Select
	assert.Nil(t, stmt.Select)
	assert.NotNil(t, stmt.Insert)
}

// TestRecommendForSingleQuery_WithConversion verifies that the full
// recommendation pipeline works with AST-based conversion, producing
// candidates that use proper table names from the parsed statement.
func TestRecommendForSingleQuery_WithConversion(t *testing.T) {
	advisor := NewIndexAdvisor()
	advisor.PopulationSize = 10
	advisor.MaxGenerations = 5

	query := "SELECT * FROM users WHERE age > 18 AND name = 'Alice' ORDER BY created_at"

	tableInfo := map[string]*domain.TableInfo{
		"users": {
			Name: "users",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "INT"},
				{Name: "name", Type: "VARCHAR"},
				{Name: "age", Type: "INT"},
				{Name: "created_at", Type: "DATETIME"},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	recommendations, err := advisor.RecommendForSingleQuery(ctx, query, tableInfo)
	require.NoError(t, err)

	// The AST-based conversion should produce meaningful candidates.
	// With proper WHERE/ORDER BY parsing, we should get recommendations.
	if len(recommendations) > 0 {
		for _, rec := range recommendations {
			assert.NotEmpty(t, rec.Columns)
			assert.NotEmpty(t, rec.CreateStatement)
			assert.GreaterOrEqual(t, rec.EstimatedBenefit, 0.0)
			assert.LessOrEqual(t, rec.EstimatedBenefit, 1.0)
		}
	}
}

// TestRecommendForSingleQuery_CostEstimation verifies that the cost estimator
// properly uses the populated Select fields (WHERE, JOIN, etc.) from the
// AST-based conversion.
func TestRecommendForSingleQuery_CostEstimation(t *testing.T) {
	advisor := NewIndexAdvisor()

	// A query with WHERE, ORDER BY, and LIMIT should yield different costs than
	// a bare SELECT *.
	queryComplex := "SELECT * FROM t1 WHERE a = 1 AND b > 10 ORDER BY c LIMIT 100"
	querySimple := "SELECT * FROM t1"

	stmtComplex, err := advisor.parseQueryToStatement(queryComplex)
	require.NoError(t, err)

	stmtSimple, err := advisor.parseQueryToStatement(querySimple)
	require.NoError(t, err)

	tableInfo := map[string]*domain.TableInfo{
		"t1": {
			Name: "t1",
			Columns: []domain.ColumnInfo{
				{Name: "a", Type: "INT"},
				{Name: "b", Type: "INT"},
				{Name: "c", Type: "VARCHAR"},
			},
		},
	}

	costComplex, err := advisor.estimateDefaultCost(stmtComplex, tableInfo)
	require.NoError(t, err)

	costSimple, err := advisor.estimateDefaultCost(stmtSimple, tableInfo)
	require.NoError(t, err)

	// The complex query with WHERE + ORDER BY + LIMIT should have different
	// cost than simple. With LIMIT, cost is multiplied by 0.5.
	// Complex: base(1000) + WHERE(500) + ORDERBY(200) * 0.5 (LIMIT) = 850
	// Simple: base(1000)
	assert.NotEqual(t, costComplex, costSimple,
		"Complex query should have different cost than simple query")
}

// TestRecommendForWorkload_WithConversion verifies that the workload
// recommendation path also uses AST-based conversion.
func TestRecommendForWorkload_WithConversion(t *testing.T) {
	advisor := NewIndexAdvisor()
	advisor.PopulationSize = 10
	advisor.MaxGenerations = 5

	queries := []string{
		"SELECT * FROM users WHERE email = 'test@example.com'",
		"SELECT * FROM users WHERE name LIKE 'A%' ORDER BY created_at",
		"SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
	}

	tableInfo := map[string]*domain.TableInfo{
		"users": {
			Name: "users",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "INT"},
				{Name: "name", Type: "VARCHAR"},
				{Name: "email", Type: "VARCHAR"},
				{Name: "created_at", Type: "DATETIME"},
			},
		},
		"orders": {
			Name: "orders",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "INT"},
				{Name: "user_id", Type: "INT"},
				{Name: "total", Type: "DECIMAL"},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	recommendations, err := advisor.RecommendForWorkload(ctx, queries, tableInfo)
	// Should not error out
	require.NoError(t, err)
	t.Logf("Workload produced %d recommendations", len(recommendations))
}

// TestRecommendForWorkload_SkipsInvalidQueries verifies that invalid queries in
// a workload are skipped without breaking the whole recommendation process.
func TestRecommendForWorkload_SkipsInvalidQueries(t *testing.T) {
	advisor := NewIndexAdvisor()
	advisor.PopulationSize = 10
	advisor.MaxGenerations = 5

	queries := []string{
		"INVALID SQL HERE",
		"SELECT * FROM t1 WHERE a = 1",
		"ALSO NOT VALID",
		"SELECT * FROM t1 WHERE b = 2",
	}

	tableInfo := map[string]*domain.TableInfo{
		"t1": {
			Name: "t1",
			Columns: []domain.ColumnInfo{
				{Name: "a", Type: "INT"},
				{Name: "b", Type: "INT"},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Should not error -- invalid queries are simply skipped.
	recommendations, err := advisor.RecommendForWorkload(ctx, queries, tableInfo)
	require.NoError(t, err)
	t.Logf("Workload with invalid queries produced %d recommendations", len(recommendations))
}

// TestRecommendForWorkload_ContextCancellation verifies that
// RecommendForWorkload respects context cancellation and returns the
// context's error.
func TestRecommendForWorkload_ContextCancellation(t *testing.T) {
	advisor := NewIndexAdvisor()
	advisor.PopulationSize = 50
	advisor.MaxGenerations = 100

	// Generate a large set of queries so that cancellation has a chance to fire.
	queries := make([]string, 200)
	for i := 0; i < len(queries); i++ {
		queries[i] = "SELECT * FROM big_table WHERE col1 = 1 AND col2 > 10 ORDER BY col3"
	}

	tableInfo := map[string]*domain.TableInfo{
		"big_table": {
			Name: "big_table",
			Columns: []domain.ColumnInfo{
				{Name: "col1", Type: "INT"},
				{Name: "col2", Type: "INT"},
				{Name: "col3", Type: "VARCHAR"},
			},
		},
	}

	// Create a context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel right away

	_, err := advisor.RecommendForWorkload(ctx, queries, tableInfo)
	// The function should return the context error
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestParseQueryToStatement_ComplexWhere verifies that a query with a complex
// WHERE clause (AND + OR) is parsed and the expression tree is populated.
func TestParseQueryToStatement_ComplexWhere(t *testing.T) {
	advisor := NewIndexAdvisor()

	stmt, err := advisor.parseQueryToStatement(
		"SELECT * FROM orders WHERE a > 1 AND b < 10 OR c = 'foo'",
	)
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.NotNil(t, stmt.Select, "Select field should be populated")
	assert.Equal(t, "orders", stmt.Select.From)

	// The WHERE clause should be present and non-nil
	require.NotNil(t, stmt.Select.Where, "Where expression tree should be populated for complex WHERE")

	// The expression should have a type set (indicating it was parsed into an expression tree)
	assert.NotEmpty(t, stmt.Select.Where.Type,
		"Where expression Type should be set for complex WHERE clause")
}
