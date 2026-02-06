package optimizer

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

func init() {
	// 避免 "imported and not used" 错误
	_ = domain.TableInfo{}
	_ = domain.ColumnInfo{}
}

// TestNewIndexCandidateExtractor 测试创建提取器
func TestNewIndexCandidateExtractor(t *testing.T) {
	extractor := NewIndexCandidateExtractor()
	if extractor == nil {
		t.Fatal("NewIndexCandidateExtractor returned nil")
	}
	if extractor.excludeTypes == nil {
		t.Error("Expected excludeTypes to be initialized")
	}
	if !extractor.excludeTypes["BLOB"] {
		t.Error("Expected BLOB to be in excludeTypes")
	}
	if !extractor.excludeTypes["TEXT"] {
		t.Error("Expected TEXT to be in excludeTypes")
	}
}

// TestExtractFromWhere 测试从 WHERE 子句提取
func TestExtractFromWhere(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	// 简单等值查询
	where1 := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
		Operator: "=",
		Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
	}
	candidates1 := extractor.extractFromWhere(where1)
	if len(candidates1) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates1))
	}
	if candidates1[0].Columns[0] != "id" {
		t.Errorf("Expected column 'id', got %s", candidates1[0].Columns[0])
	}
	if candidates1[0].Priority != 4 {
		t.Errorf("Expected priority 4, got %d", candidates1[0].Priority)
	}

	// 范围查询
	where2 := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "age"},
		Operator: ">",
		Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 18},
	}
	candidates2 := extractor.extractFromWhere(where2)
	if len(candidates2) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates2))
	}

	// LIKE 查询
	where3 := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "name"},
		Operator: "LIKE",
		Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: "John%"},
	}
	candidates3 := extractor.extractFromWhere(where3)
	if len(candidates3) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates3))
	}

	// 复合条件
	where4 := &parser.Expression{
		Type:     parser.ExprTypeOperator,
		Operator: "AND",
		Left: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "id"},
			Operator: "=",
			Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: 1},
		},
		Right: &parser.Expression{
			Type:     parser.ExprTypeOperator,
			Left:     &parser.Expression{Type: parser.ExprTypeColumn, Column: "status"},
			Operator: "=",
			Right:    &parser.Expression{Type: parser.ExprTypeValue, Value: "active"},
		},
	}
	candidates4 := extractor.extractFromWhere(where4)
	if len(candidates4) < 2 {
		t.Logf("Warning: Expected at least 2 candidates for AND condition, got %d", len(candidates4))
	}
}

// TestExtractFromOrderBy 测试从 ORDER BY 提取
func TestExtractFromOrderBy(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	// 单列排序
	orderBy1 := []parser.OrderByItem{
		{
			Column:    "id",
			Direction: "ASC",
		},
	}
	candidates1 := extractor.extractFromOrderBy(orderBy1)
	if len(candidates1) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates1))
	}
	if candidates1[0].Source != "ORDER" {
		t.Errorf("Expected source 'ORDER', got %s", candidates1[0].Source)
	}

	// 多列排序
	orderBy2 := []parser.OrderByItem{
		{
			Column:    "last_name",
			Direction: "ASC",
		},
		{
			Column:    "first_name",
			Direction: "ASC",
		},
	}
	candidates2 := extractor.extractFromOrderBy(orderBy2)
	if len(candidates2) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates2))
	}
	if len(candidates2[0].Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(candidates2[0].Columns))
	}

	// 空 ORDER BY
	candidates3 := extractor.extractFromOrderBy([]parser.OrderByItem{})
	if len(candidates3) != 0 {
		t.Errorf("Expected 0 candidates for empty ORDER BY, got %d", len(candidates3))
	}
}

// TestExtractFromGroupBy 测试从 GROUP BY 提取
func TestExtractFromGroupBy(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	// 单列分组
	groupBy1 := []string{"department_id"}
	candidates1 := extractor.extractFromGroupBy(groupBy1)
	if len(candidates1) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates1))
	}
	if candidates1[0].Source != "GROUP" {
		t.Errorf("Expected source 'GROUP', got %s", candidates1[0].Source)
	}

	// 多列分组
	groupBy2 := []string{"department_id", "role_id"}
	candidates2 := extractor.extractFromGroupBy(groupBy2)
	if len(candidates2) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates2))
	}
	if len(candidates2[0].Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(candidates2[0].Columns))
	}

	// 空 GROUP BY
	candidates3 := extractor.extractFromGroupBy([]string{})
	if len(candidates3) != 0 {
		t.Errorf("Expected 0 candidates for empty GROUP BY, got %d", len(candidates3))
	}
}

// TestExtractFromJoins 测试从 JOIN 条件提取
func TestExtractFromJoins(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	// 简单 JOIN
	joins1 := []parser.JoinInfo{
		{
			Type:      parser.JoinTypeInner,
			Table:     "orders",
			Condition: &parser.Expression{
				Left: &parser.Expression{Column: "users.id"},
				Operator: "=",
				Right: &parser.Expression{Column: "orders.user_id"},
			},
		},
	}
	candidates1 := extractor.extractFromJoins(joins1, nil)
	if len(candidates1) == 0 {
		t.Error("Expected at least 1 candidate from JOIN")
	}

	// 空 JOIN
	candidates2 := extractor.extractFromJoins([]parser.JoinInfo{}, nil)
	if len(candidates2) != 0 {
		t.Errorf("Expected 0 candidates for empty JOIN, got %d", len(candidates2))
	}
}

// TestExtractFromSQL 测试从完整 SQL 提取
func TestExtractFromSQL(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	// 简单 SELECT
	stmt1 := &parser.SQLStatement{
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "*"}},
			From:    "users",
			Where: &parser.Expression{
				Column:  "id",
				Operator: "=",
				Value:   1,
			},
		},
	}
	candidates1, err := extractor.ExtractFromSQL(stmt1, nil)
	if err != nil {
		t.Fatalf("ExtractFromSQL failed: %v", err)
	}
	if len(candidates1) == 0 {
		t.Error("Expected at least 1 candidate")
	}

	// 复杂 SQL
	stmt2 := &parser.SQLStatement{
		Select: &parser.SelectStatement{
			Columns: []parser.SelectColumn{{Name: "*"}},
			From:    "users",
			Where: &parser.Expression{
				Operator: "AND",
				Left: &parser.Expression{
					Column:  "age",
					Operator: ">",
					Value:   18,
				},
				Right: &parser.Expression{
					Column:  "status",
					Operator: "=",
					Value:   "active",
				},
			},
			OrderBy: []parser.OrderByItem{
				{Column: "name"},
			},
		},
	}
	candidates2, err := extractor.ExtractFromSQL(stmt2, nil)
	if err != nil {
		t.Fatalf("ExtractFromSQL failed: %v", err)
	}
	if len(candidates2) < 2 {
		t.Errorf("Expected at least 2 candidates, got %d", len(candidates2))
	}

	// 无效语句（非 SELECT）
	stmt3 := &parser.SQLStatement{}
	_, err = extractor.ExtractFromSQL(stmt3, nil)
	if err == nil {
		t.Error("Expected error for non-SELECT statement")
	}
}

// TestDeduplicateCandidates 测试去重
func TestDeduplicateCandidates(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	candidates := []*IndexCandidate{
		{Columns: []string{"id"}, Priority: 4, Source: "WHERE"},
		{Columns: []string{"id"}, Priority: 3, Source: "JOIN"},  // 重复
		{Columns: []string{"name"}, Priority: 2, Source: "GROUP"},
	}

	deduplicated := extractor.deduplicateCandidates(candidates)
	if len(deduplicated) != 2 {
		t.Errorf("Expected 2 unique candidates, got %d", len(deduplicated))
	}

	// 检查是否保留高优先级
	if deduplicated[0].Columns[0] != "id" {
		t.Errorf("Expected first candidate to be 'id', got %s", deduplicated[0].Columns[0])
	}
	if deduplicated[0].Priority != 4 {
		t.Errorf("Expected priority 4 for 'id', got %d", deduplicated[0].Priority)
	}
}

// TestMergeCandidates 测试合并候选
func TestMergeCandidates(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	candidates := []*IndexCandidate{
		{Columns: []string{"id", "name"}, Priority: 4, Source: "WHERE"},
		{Columns: []string{"id"}, Priority: 3, Source: "JOIN"},
		{Columns: []string{"email"}, Priority: 2, Source: "GROUP"},
	}

	merged := extractor.MergeCandidates(candidates)
	if len(merged) != 3 {
		t.Errorf("Expected 3 candidates after merge, got %d", len(merged))
	}

	// 验证排序
	if merged[0].Priority < merged[1].Priority {
		t.Error("Candidates should be sorted by priority (descending)")
	}
}

// TestExtractForTable 测试提取指定表的候选
func TestExtractForTable(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	candidates := []*IndexCandidate{
		{Columns: []string{"users.id"}, Priority: 4, Source: "WHERE"},
		{Columns: []string{"orders.user_id"}, Priority: 3, Source: "JOIN"},
		{Columns: []string{"id"}, Priority: 2, Source: "GROUP"},
	}

	// 提取 users 表的候选
	usersCandidates := extractor.ExtractForTable(candidates, "users")
	if len(usersCandidates) != 2 {  // "users.id" 和 "id"
		t.Errorf("Expected 2 candidates for 'users' table, got %d", len(usersCandidates))
	}
}

// TestIsIndexableComparison 测试可索引判断
func TestIsIndexableComparison(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	testCases := []struct {
		operator  string
		expected  bool
	}{
		{"=", true},
		{"!=", true},
		{">", true},
		{">=", true},
		{"<", true},
		{"<=", true},
		{"LIKE", true},
		{"IN", true},
		{"BETWEEN", true},
		{"NOT", false},
		{"AND", false},
		{"OR", false},
	}

	for _, tc := range testCases {
		expr := &parser.Expression{Operator: tc.operator}
		result := extractor.isIndexableComparison(expr)
		if result != tc.expected {
			t.Errorf("For operator '%s', expected %v, got %v", tc.operator, tc.expected, result)
		}
	}
}

// TestIsColumnTypeSupported 测试列类型支持
func TestIsColumnTypeSupported(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	supported := []string{"INT", "VARCHAR", "DATE", "DECIMAL"}
	unsupported := []string{"BLOB", "TEXT", "JSON", "LONGBLOB"}

	for _, typ := range supported {
		if !extractor.isColumnTypeSupported(typ) {
			t.Errorf("Expected type '%s' to be supported", typ)
		}
	}

	for _, typ := range unsupported {
		if extractor.isColumnTypeSupported(typ) {
			t.Errorf("Expected type '%s' to be unsupported", typ)
		}
	}
}

// TestParseColumnReference 测试解析列引用
func TestParseColumnReference(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	testCases := []struct {
		colRef    string
		tableName string
		colName   string
	}{
		{"id", "", "id"},
		{"users.id", "users", "id"},
		{"orders.user_id", "orders", "user_id"},
		{"", "", ""},
	}

	for _, tc := range testCases {
		tableName, colName := extractor.parseColumnReference(tc.colRef)
		if tableName != tc.tableName {
			t.Errorf("Expected table name '%s', got '%s'", tc.tableName, tableName)
		}
		if colName != tc.colName {
			t.Errorf("Expected column name '%s', got '%s'", tc.colName, colName)
		}
	}
}

// TestExtractorBuildCandidateKey 测试构建候选键
func TestExtractorBuildCandidateKey(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	candidate := &IndexCandidate{
		Columns: []string{"id", "name", "email"},
	}
	key := extractor.buildCandidateKey(candidate)
	if key != "id,name,email" {
		t.Errorf("Expected key 'id,name,email', got '%s'", key)
	}
}

// TestPriorityOrdering 测试优先级排序
func TestPriorityOrdering(t *testing.T) {
	extractor := NewIndexCandidateExtractor()

	candidates := []*IndexCandidate{
		{Columns: []string{"name"}, Priority: 1, Source: "ORDER"},
		{Columns: []string{"id"}, Priority: 4, Source: "WHERE"},
		{Columns: []string{"status"}, Priority: 2, Source: "GROUP"},
		{Columns: []string{"user_id"}, Priority: 3, Source: "JOIN"},
	}

	deduplicated := extractor.deduplicateCandidates(candidates)

	// 验证排序：WHERE (4) > JOIN (3) > GROUP (2) > ORDER (1)
	expectedOrder := []string{"id", "user_id", "status", "name"}
	for i, expectedCol := range expectedOrder {
		if i >= len(deduplicated) {
			t.Errorf("Expected at least %d candidates, got %d", i+1, len(deduplicated))
			break
		}
		if deduplicated[i].Columns[0] != expectedCol {
			t.Errorf("Expected column '%s' at position %d, got '%s'",
				expectedCol, i, deduplicated[i].Columns[0])
		}
	}
}
