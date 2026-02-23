package parser

import (
	"context"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// =============================================================================
// Mock DataSource for testing
// =============================================================================

type mockDataSource struct {
	tables map[string]*domain.TableInfo
	data   map[string][]domain.Row
}

func newMockDataSource() *mockDataSource {
	return &mockDataSource{
		tables: make(map[string]*domain.TableInfo),
		data:   make(map[string][]domain.Row),
	}
}

func (m *mockDataSource) addTable(name string, columns []domain.ColumnInfo, rows []domain.Row) {
	m.tables[name] = &domain.TableInfo{
		Name:    name,
		Columns: columns,
	}
	m.data[name] = rows
}

func (m *mockDataSource) Connect(ctx context.Context) error   { return nil }
func (m *mockDataSource) Close(ctx context.Context) error     { return nil }
func (m *mockDataSource) IsConnected() bool                   { return true }
func (m *mockDataSource) IsWritable() bool                    { return true }
func (m *mockDataSource) GetConfig() *domain.DataSourceConfig { return &domain.DataSourceConfig{} }
func (m *mockDataSource) GetTables(ctx context.Context) ([]string, error) {
	var names []string
	for name := range m.tables {
		names = append(names, name)
	}
	return names, nil
}
func (m *mockDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	info, ok := m.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return info, nil
}
func (m *mockDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	info, ok := m.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	rows := m.data[tableName]

	// Apply filters (simple implementation for testing)
	filteredRows := rows
	if options != nil && len(options.Filters) > 0 {
		filteredRows = make([]domain.Row, 0)
		for _, row := range rows {
			if m.matchFilters(row, options.Filters) {
				filteredRows = append(filteredRows, row)
			}
		}
	}

	return &domain.QueryResult{
		Columns: info.Columns,
		Rows:    filteredRows,
		Total:   int64(len(filteredRows)),
	}, nil
}

func (m *mockDataSource) matchFilters(row domain.Row, filters []domain.Filter) bool {
	for _, f := range filters {
		if f.LogicOp != "" {
			// handle AND/OR sub-filters
			if f.LogicOp == "AND" {
				for _, sf := range f.SubFilters {
					if !m.matchFilters(row, []domain.Filter{sf}) {
						return false
					}
				}
				return true
			}
			if f.LogicOp == "OR" {
				for _, sf := range f.SubFilters {
					if m.matchFilters(row, []domain.Filter{sf}) {
						return true
					}
				}
				return false
			}
		}
		val, exists := row[f.Field]
		if !exists {
			return false
		}
		switch f.Operator {
		case "=":
			if fmt.Sprintf("%v", val) != fmt.Sprintf("%v", f.Value) {
				return false
			}
		}
	}
	return true
}

func (m *mockDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	return 0, nil
}
func (m *mockDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	return 0, nil
}
func (m *mockDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	return 0, nil
}
func (m *mockDataSource) CreateTable(ctx context.Context, tableInfo *domain.TableInfo) error {
	return nil
}
func (m *mockDataSource) DropTable(ctx context.Context, tableName string) error { return nil }
func (m *mockDataSource) TruncateTable(ctx context.Context, tableName string) error {
	return nil
}
func (m *mockDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, nil
}

// =============================================================================
// Helper to create test fixtures
// =============================================================================

func setupUsersAndOrders() *mockDataSource {
	ds := newMockDataSource()

	ds.addTable("users", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "name", Type: "text"},
		{Name: "department", Type: "text"},
	}, []domain.Row{
		{"id": int64(1), "name": "Alice", "department": "Engineering"},
		{"id": int64(2), "name": "Bob", "department": "Engineering"},
		{"id": int64(3), "name": "Charlie", "department": "Sales"},
		{"id": int64(4), "name": "Diana", "department": "Sales"},
		{"id": int64(5), "name": "Eve", "department": "HR"},
	})

	ds.addTable("orders", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "user_id", Type: "int64"},
		{Name: "amount", Type: "float64"},
		{Name: "product", Type: "text"},
	}, []domain.Row{
		{"id": int64(1), "user_id": int64(1), "amount": float64(100), "product": "Widget"},
		{"id": int64(2), "user_id": int64(1), "amount": float64(200), "product": "Gadget"},
		{"id": int64(3), "user_id": int64(2), "amount": float64(150), "product": "Widget"},
		{"id": int64(4), "user_id": int64(3), "amount": float64(300), "product": "Gizmo"},
		{"id": int64(5), "user_id": int64(5), "amount": float64(50), "product": "Widget"},
	})

	return ds
}

// =============================================================================
// Tests for JOIN
// =============================================================================

func TestExecuteSelect_InnerJoin(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// Simulate: SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "users",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeInner,
				Table: "orders",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "users.id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "orders.user_id"},
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Users 1,2,3,5 have orders. User 4 (Diana) has no orders.
	// User 1 (Alice) has 2 orders, User 2 (Bob) 1, User 3 (Charlie) 1, User 5 (Eve) 1
	// Total inner join rows = 5
	if len(result.Rows) != 5 {
		t.Errorf("INNER JOIN: expected 5 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	// Verify that joined data is present (users.name and orders.amount should coexist)
	for _, row := range result.Rows {
		if _, hasName := row["users.name"]; !hasName {
			if _, hasName2 := row["name"]; !hasName2 {
				t.Errorf("INNER JOIN row missing 'name' or 'users.name': %v", row)
			}
		}
		if _, hasAmount := row["orders.amount"]; !hasAmount {
			if _, hasAmount2 := row["amount"]; !hasAmount2 {
				t.Errorf("INNER JOIN row missing 'amount' or 'orders.amount': %v", row)
			}
		}
	}
}

func TestExecuteSelect_LeftJoin(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// Simulate: SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id
	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "users",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeLeft,
				Table: "orders",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "users.id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "orders.user_id"},
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// LEFT JOIN: all users appear. Alice has 2 orders, Bob 1, Charlie 1, Diana 0, Eve 1
	// Rows: Alice(2) + Bob(1) + Charlie(1) + Diana(1 with nulls) + Eve(1) = 6
	if len(result.Rows) != 6 {
		t.Errorf("LEFT JOIN: expected 6 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	// Diana should appear with null order values
	foundDiana := false
	for _, row := range result.Rows {
		name := ""
		if v, ok := row["users.name"]; ok {
			name, _ = v.(string)
		} else if v, ok := row["name"]; ok {
			name, _ = v.(string)
		}
		if name == "Diana" {
			foundDiana = true
			// Orders columns should be nil for Diana
			if amt, ok := row["orders.amount"]; ok && amt != nil {
				t.Errorf("LEFT JOIN: Diana's orders.amount should be nil, got %v", amt)
			}
		}
	}
	if !foundDiana {
		t.Errorf("LEFT JOIN: Diana should appear in result but not found")
	}
}

func TestExecuteSelect_RightJoin(t *testing.T) {
	ds := setupUsersAndOrders()
	// Add an order with user_id that doesn't exist in users
	ds.data["orders"] = append(ds.data["orders"], domain.Row{
		"id": int64(6), "user_id": int64(99), "amount": float64(999), "product": "Mystery",
	})
	builder := NewQueryBuilder(ds)

	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "users",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeRight,
				Table: "orders",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "users.id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "orders.user_id"},
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// RIGHT JOIN: all orders appear. The extra order with user_id=99 should appear with null user cols.
	// 5 matching orders + 1 unmatched order = 6
	if len(result.Rows) != 6 {
		t.Errorf("RIGHT JOIN: expected 6 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}
}

func TestExecuteSelect_CrossJoin(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("colors", []domain.ColumnInfo{
		{Name: "color", Type: "text"},
	}, []domain.Row{
		{"color": "red"},
		{"color": "blue"},
	})
	ds.addTable("sizes", []domain.ColumnInfo{
		{Name: "size", Type: "text"},
	}, []domain.Row{
		{"size": "S"},
		{"size": "M"},
		{"size": "L"},
	})

	builder := NewQueryBuilder(ds)

	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "colors",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeCross,
				Table: "sizes",
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// CROSS JOIN: 2 colors * 3 sizes = 6 rows
	if len(result.Rows) != 6 {
		t.Errorf("CROSS JOIN: expected 6 rows, got %d", len(result.Rows))
	}
}

func TestExecuteSelect_JoinWithAlias(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// Simulate: SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id
	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "users",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeInner,
				Table: "orders",
				Alias: "o",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "users.id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "o.user_id"},
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	if len(result.Rows) != 5 {
		t.Errorf("JOIN with alias: expected 5 rows, got %d", len(result.Rows))
	}
}

// =============================================================================
// Tests for Aggregation (without GROUP BY)
// =============================================================================

func TestExecuteSelect_CountStar(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// SELECT COUNT(*) FROM users
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
		},
		From: "users",
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("COUNT(*): expected 1 row, got %d", len(result.Rows))
	}

	cnt, ok := result.Rows[0]["cnt"]
	if !ok {
		// try alternative key
		cnt, ok = result.Rows[0]["COUNT(*)"]
	}
	if !ok {
		t.Fatalf("COUNT(*): result row missing 'cnt' or 'COUNT(*)' key. Row: %v", result.Rows[0])
	}

	if toInt64(cnt) != 5 {
		t.Errorf("COUNT(*): expected 5, got %v", cnt)
	}
}

func TestExecuteSelect_SumAvgMinMax(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// SELECT SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "total",
				Name:  "SUM(amount)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "avg",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "average",
				Name:  "AVG(amount)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "min",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "minimum",
				Name:  "MIN(amount)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "max",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "maximum",
				Name:  "MAX(amount)",
			},
		},
		From: "orders",
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Aggregation: expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]

	// SUM: 100 + 200 + 150 + 300 + 50 = 800
	total := toFloat64(row["total"])
	if total != 800 {
		t.Errorf("SUM(amount): expected 800, got %v", total)
	}

	// AVG: 800 / 5 = 160
	avg := toFloat64(row["average"])
	if avg != 160 {
		t.Errorf("AVG(amount): expected 160, got %v", avg)
	}

	// MIN: 50
	min := toFloat64(row["minimum"])
	if min != 50 {
		t.Errorf("MIN(amount): expected 50, got %v", min)
	}

	// MAX: 300
	max := toFloat64(row["maximum"])
	if max != 300 {
		t.Errorf("MAX(amount): expected 300, got %v", max)
	}
}

func TestExecuteSelect_CountColumn(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("data", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "value", Type: "text", Nullable: true},
	}, []domain.Row{
		{"id": int64(1), "value": "a"},
		{"id": int64(2), "value": nil},
		{"id": int64(3), "value": "b"},
		{"id": int64(4), "value": nil},
	})

	builder := NewQueryBuilder(ds)

	// SELECT COUNT(value) FROM data -- should not count NULLs
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "cnt",
				Name:  "COUNT(value)",
			},
		},
		From: "data",
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("COUNT(col): expected 1 row, got %d", len(result.Rows))
	}

	cnt := toInt64(result.Rows[0]["cnt"])
	if cnt != 2 {
		t.Errorf("COUNT(value): expected 2 (non-null), got %d", cnt)
	}
}

// =============================================================================
// Tests for GROUP BY
// =============================================================================

func TestExecuteSelect_GroupBy(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// SELECT department, COUNT(*) as cnt FROM users GROUP BY department
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "department"},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
		},
		From:    "users",
		GroupBy: []string{"department"},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// 3 departments: Engineering(2), Sales(2), HR(1)
	if len(result.Rows) != 3 {
		t.Errorf("GROUP BY: expected 3 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	// Verify each group
	deptCounts := make(map[string]int64)
	for _, row := range result.Rows {
		dept := fmt.Sprintf("%v", row["department"])
		cnt := toInt64(row["cnt"])
		deptCounts[dept] = cnt
	}

	expected := map[string]int64{
		"Engineering": 2,
		"Sales":       2,
		"HR":          1,
	}
	for dept, expectedCnt := range expected {
		if deptCounts[dept] != expectedCnt {
			t.Errorf("GROUP BY: department=%s expected count=%d, got %d", dept, expectedCnt, deptCounts[dept])
		}
	}
}

func TestExecuteSelect_GroupByWithSum(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// SELECT product, SUM(amount) as total, COUNT(*) as cnt FROM orders GROUP BY product
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "product"},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "total",
				Name:  "SUM(amount)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
		},
		From:    "orders",
		GroupBy: []string{"product"},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Products: Widget(100+150+50=300, cnt=3), Gadget(200, cnt=1), Gizmo(300, cnt=1)
	if len(result.Rows) != 3 {
		t.Errorf("GROUP BY: expected 3 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	productTotals := make(map[string]float64)
	productCounts := make(map[string]int64)
	for _, row := range result.Rows {
		prod := fmt.Sprintf("%v", row["product"])
		productTotals[prod] = toFloat64(row["total"])
		productCounts[prod] = toInt64(row["cnt"])
	}

	if productTotals["Widget"] != 300 {
		t.Errorf("GROUP BY Widget SUM: expected 300, got %v", productTotals["Widget"])
	}
	if productTotals["Gadget"] != 200 {
		t.Errorf("GROUP BY Gadget SUM: expected 200, got %v", productTotals["Gadget"])
	}
	if productCounts["Widget"] != 3 {
		t.Errorf("GROUP BY Widget COUNT: expected 3, got %v", productCounts["Widget"])
	}
}

// =============================================================================
// Tests for HAVING
// =============================================================================

func TestExecuteSelect_Having(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// SELECT department, COUNT(*) as cnt FROM users GROUP BY department HAVING COUNT(*) >= 2
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "department"},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
		},
		From:    "users",
		GroupBy: []string{"department"},
		Having: &Expression{
			Type:     ExprTypeOperator,
			Operator: "ge",
			Left: &Expression{
				Type:     ExprTypeFunction,
				Function: "count",
				Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
			},
			Right: &Expression{
				Type:  ExprTypeValue,
				Value: int64(2),
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Only Engineering(2) and Sales(2) have COUNT >= 2. HR(1) is filtered out.
	if len(result.Rows) != 2 {
		t.Errorf("HAVING: expected 2 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	for _, row := range result.Rows {
		dept := fmt.Sprintf("%v", row["department"])
		if dept == "HR" {
			t.Errorf("HAVING: HR should be filtered out (COUNT=1 < 2)")
		}
	}
}

func TestExecuteSelect_HavingWithSum(t *testing.T) {
	ds := setupUsersAndOrders()
	builder := NewQueryBuilder(ds)

	// SELECT product, SUM(amount) as total FROM orders GROUP BY product HAVING SUM(amount) > 200
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "product"},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "total",
				Name:  "SUM(amount)",
			},
		},
		From:    "orders",
		GroupBy: []string{"product"},
		Having: &Expression{
			Type:     ExprTypeOperator,
			Operator: "gt",
			Left: &Expression{
				Type:     ExprTypeFunction,
				Function: "sum",
				Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
			},
			Right: &Expression{
				Type:  ExprTypeValue,
				Value: int64(200),
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Widget: SUM=300 > 200 -> included
	// Gadget: SUM=200, not > 200 -> excluded
	// Gizmo: SUM=300 > 200 -> included
	if len(result.Rows) != 2 {
		t.Errorf("HAVING SUM>200: expected 2 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	for _, row := range result.Rows {
		prod := fmt.Sprintf("%v", row["product"])
		if prod == "Gadget" {
			t.Errorf("HAVING: Gadget should be filtered out (SUM=200, not > 200)")
		}
	}
}

// =============================================================================
// Tests for combined features
// =============================================================================

func TestExecuteSelect_AggregateWithoutGroupBy_EmptyResult(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("empty_table", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "value", Type: "float64"},
	}, []domain.Row{})

	builder := NewQueryBuilder(ds)

	// SELECT COUNT(*) as cnt, SUM(value) as total FROM empty_table
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "total",
				Name:  "SUM(value)",
			},
		},
		From: "empty_table",
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// COUNT(*) on empty table should return 1 row with count=0
	if len(result.Rows) != 1 {
		t.Fatalf("Agg on empty table: expected 1 row, got %d", len(result.Rows))
	}

	cnt := toInt64(result.Rows[0]["cnt"])
	if cnt != 0 {
		t.Errorf("COUNT(*) on empty table: expected 0, got %d", cnt)
	}
}

// =============================================================================
// Test helper functions
// =============================================================================

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case nil:
		return 0
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case nil:
		return 0
	default:
		return 0
	}
}

// =============================================================================
// Additional tests for untested code paths
// =============================================================================

func TestExecuteSelect_FullJoin(t *testing.T) {
	ds := newMockDataSource()

	// Table "left_t" has ids 1, 2, 3
	ds.addTable("left_t", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "lval", Type: "text"},
	}, []domain.Row{
		{"id": int64(1), "lval": "L1"},
		{"id": int64(2), "lval": "L2"},
		{"id": int64(3), "lval": "L3"},
	})

	// Table "right_t" has ids 2, 3, 4
	ds.addTable("right_t", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "rval", Type: "text"},
	}, []domain.Row{
		{"id": int64(2), "rval": "R2"},
		{"id": int64(3), "rval": "R3"},
		{"id": int64(4), "rval": "R4"},
	})

	builder := NewQueryBuilder(ds)

	// SELECT * FROM left_t FULL JOIN right_t ON left_t.id = right_t.id
	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "left_t",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeFull,
				Table: "right_t",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "left_t.id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "right_t.id"},
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// FULL JOIN: matched rows for id=2 and id=3, plus unmatched left (id=1) and unmatched right (id=4)
	// Total: 4 rows
	if len(result.Rows) != 4 {
		t.Errorf("FULL JOIN: expected 4 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	// Verify that id=1 from left has nil right columns
	foundLeftOnly := false
	foundRightOnly := false
	for _, row := range result.Rows {
		leftID := row["left_t.id"]
		rightID := row["right_t.id"]

		if leftID != nil && toInt64(leftID) == 1 {
			foundLeftOnly = true
			// right_t columns should be nil
			if rightID != nil {
				t.Errorf("FULL JOIN: left-only row (id=1) should have nil right_t.id, got %v", rightID)
			}
		}
		if rightID != nil && toInt64(rightID) == 4 {
			foundRightOnly = true
		}
	}
	if !foundLeftOnly {
		t.Errorf("FULL JOIN: expected unmatched left row with id=1")
	}
	if !foundRightOnly {
		t.Errorf("FULL JOIN: expected unmatched right row with id=4")
	}
}

func TestExecuteSelect_MultipleJoins(t *testing.T) {
	ds := newMockDataSource()

	ds.addTable("users", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "name", Type: "text"},
	}, []domain.Row{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	})

	ds.addTable("orders", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "user_id", Type: "int64"},
		{Name: "product_id", Type: "int64"},
	}, []domain.Row{
		{"id": int64(10), "user_id": int64(1), "product_id": int64(100)},
		{"id": int64(11), "user_id": int64(1), "product_id": int64(101)},
		{"id": int64(12), "user_id": int64(2), "product_id": int64(100)},
	})

	ds.addTable("products", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "pname", Type: "text"},
	}, []domain.Row{
		{"id": int64(100), "pname": "Widget"},
		{"id": int64(101), "pname": "Gadget"},
	})

	builder := NewQueryBuilder(ds)

	// SELECT * FROM users
	//   INNER JOIN orders ON users.id = orders.user_id
	//   INNER JOIN products ON orders.product_id = products.id
	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "users",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeInner,
				Table: "orders",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "users.id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "orders.user_id"},
				},
			},
			{
				Type:  JoinTypeInner,
				Table: "products",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "orders.product_id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "products.id"},
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Alice -> order(10, product 100=Widget), order(11, product 101=Gadget)
	// Bob   -> order(12, product 100=Widget)
	// Total: 3 rows
	if len(result.Rows) != 3 {
		t.Errorf("Multiple JOINs: expected 3 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	// Verify all three tables' data is present in merged rows
	for _, row := range result.Rows {
		hasUserName := false
		hasProductName := false
		for k := range row {
			if k == "users.name" || k == "name" {
				hasUserName = true
			}
			if k == "products.pname" || k == "pname" {
				hasProductName = true
			}
		}
		if !hasUserName {
			t.Errorf("Multiple JOINs: row missing user name: %v", row)
		}
		if !hasProductName {
			t.Errorf("Multiple JOINs: row missing product name: %v", row)
		}
	}
}

func TestExecuteSelect_InnerJoinNoMatch(t *testing.T) {
	ds := newMockDataSource()

	ds.addTable("t1", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
	}, []domain.Row{
		{"id": int64(1)},
		{"id": int64(2)},
	})

	ds.addTable("t2", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
	}, []domain.Row{
		{"id": int64(10)},
		{"id": int64(20)},
	})

	builder := NewQueryBuilder(ds)

	// INNER JOIN where no rows match (different id ranges)
	stmt := &SelectStatement{
		Columns: []SelectColumn{{IsWildcard: true}},
		From:    "t1",
		Joins: []JoinInfo{
			{
				Type:  JoinTypeInner,
				Table: "t2",
				Condition: &Expression{
					Type:     ExprTypeOperator,
					Operator: "eq",
					Left:     &Expression{Type: ExprTypeColumn, Column: "t1.id"},
					Right:    &Expression{Type: ExprTypeColumn, Column: "t2.id"},
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("INNER JOIN no match: expected 0 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}
}

func TestExecuteSelect_AggregateWithNulls(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("scores", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "value", Type: "float64", Nullable: true},
	}, []domain.Row{
		{"id": int64(1), "value": float64(10)},
		{"id": int64(2), "value": nil},
		{"id": int64(3), "value": float64(30)},
		{"id": int64(4), "value": nil},
		{"id": int64(5), "value": float64(20)},
	})

	builder := NewQueryBuilder(ds)

	// SELECT SUM(value), AVG(value), MIN(value), MAX(value) FROM scores
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "s",
				Name:  "SUM(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "avg",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "a",
				Name:  "AVG(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "min",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "mn",
				Name:  "MIN(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "max",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "mx",
				Name:  "MAX(value)",
			},
		},
		From: "scores",
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Aggregate with nulls: expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]

	// SUM: 10 + 30 + 20 = 60 (nil values skipped)
	if toFloat64(row["s"]) != 60 {
		t.Errorf("SUM with nulls: expected 60, got %v", row["s"])
	}

	// AVG: 60 / 3 = 20 (nil values skipped, count is 3 not 5)
	if toFloat64(row["a"]) != 20 {
		t.Errorf("AVG with nulls: expected 20, got %v", row["a"])
	}

	// MIN: 10
	if toFloat64(row["mn"]) != 10 {
		t.Errorf("MIN with nulls: expected 10, got %v", row["mn"])
	}

	// MAX: 30
	if toFloat64(row["mx"]) != 30 {
		t.Errorf("MAX with nulls: expected 30, got %v", row["mx"])
	}
}

func TestExecuteSelect_GroupByMultipleColumns(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("employees", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "dept", Type: "text"},
		{Name: "role", Type: "text"},
		{Name: "salary", Type: "float64"},
	}, []domain.Row{
		{"id": int64(1), "dept": "Eng", "role": "Dev", "salary": float64(100)},
		{"id": int64(2), "dept": "Eng", "role": "Dev", "salary": float64(120)},
		{"id": int64(3), "dept": "Eng", "role": "QA", "salary": float64(90)},
		{"id": int64(4), "dept": "Sales", "role": "Rep", "salary": float64(80)},
		{"id": int64(5), "dept": "Sales", "role": "Rep", "salary": float64(85)},
		{"id": int64(6), "dept": "Sales", "role": "Mgr", "salary": float64(150)},
	})

	builder := NewQueryBuilder(ds)

	// SELECT dept, role, COUNT(*) as cnt, SUM(salary) as total
	// FROM employees GROUP BY dept, role
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "dept"},
			{Name: "role"},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "salary"}},
				},
				Alias: "total",
				Name:  "SUM(salary)",
			},
		},
		From:    "employees",
		GroupBy: []string{"dept", "role"},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Groups: (Eng, Dev) cnt=2, (Eng, QA) cnt=1, (Sales, Rep) cnt=2, (Sales, Mgr) cnt=1
	if len(result.Rows) != 4 {
		t.Errorf("GROUP BY multiple columns: expected 4 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	// Build a lookup map: "dept|role" -> {cnt, total}
	type groupResult struct {
		cnt   int64
		total float64
	}
	groups := make(map[string]groupResult)
	for _, row := range result.Rows {
		key := fmt.Sprintf("%v|%v", row["dept"], row["role"])
		groups[key] = groupResult{
			cnt:   toInt64(row["cnt"]),
			total: toFloat64(row["total"]),
		}
	}

	expected := map[string]groupResult{
		"Eng|Dev":   {cnt: 2, total: 220},
		"Eng|QA":    {cnt: 1, total: 90},
		"Sales|Rep": {cnt: 2, total: 165},
		"Sales|Mgr": {cnt: 1, total: 150},
	}

	for key, exp := range expected {
		got, ok := groups[key]
		if !ok {
			t.Errorf("GROUP BY multiple columns: missing group %s", key)
			continue
		}
		if got.cnt != exp.cnt {
			t.Errorf("GROUP BY %s: expected cnt=%d, got %d", key, exp.cnt, got.cnt)
		}
		if got.total != exp.total {
			t.Errorf("GROUP BY %s: expected total=%v, got %v", key, exp.total, got.total)
		}
	}
}

func TestExecuteSelect_HavingWithAnd(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("sales", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "category", Type: "text"},
		{Name: "amount", Type: "float64"},
	}, []domain.Row{
		{"id": int64(1), "category": "A", "amount": float64(50)},
		{"id": int64(2), "category": "A", "amount": float64(60)},
		{"id": int64(3), "category": "A", "amount": float64(10)},
		{"id": int64(4), "category": "B", "amount": float64(200)},
		{"id": int64(5), "category": "C", "amount": float64(30)},
		{"id": int64(6), "category": "C", "amount": float64(40)},
	})

	builder := NewQueryBuilder(ds)

	// SELECT category, COUNT(*) as cnt, SUM(amount) as total
	// FROM sales GROUP BY category
	// HAVING COUNT(*) >= 2 AND SUM(amount) > 100
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "category"},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "total",
				Name:  "SUM(amount)",
			},
		},
		From:    "sales",
		GroupBy: []string{"category"},
		Having: &Expression{
			Type:     ExprTypeOperator,
			Operator: "and",
			Left: &Expression{
				Type:     ExprTypeOperator,
				Operator: "ge",
				Left: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Right: &Expression{
					Type:  ExprTypeValue,
					Value: int64(2),
				},
			},
			Right: &Expression{
				Type:     ExprTypeOperator,
				Operator: "gt",
				Left: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Right: &Expression{
					Type:  ExprTypeValue,
					Value: int64(100),
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Category A: cnt=3 >=2, sum=120 >100 -> PASS
	// Category B: cnt=1 <2 -> FAIL (AND short-circuits)
	// Category C: cnt=2 >=2, sum=70 <=100 -> FAIL
	// Only category A passes both conditions.
	if len(result.Rows) != 1 {
		t.Errorf("HAVING with AND: expected 1 row, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	if len(result.Rows) == 1 {
		cat := fmt.Sprintf("%v", result.Rows[0]["category"])
		if cat != "A" {
			t.Errorf("HAVING with AND: expected category 'A', got '%s'", cat)
		}
	}
}

func TestExecuteSelect_HavingWithOr(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("sales", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "category", Type: "text"},
		{Name: "amount", Type: "float64"},
	}, []domain.Row{
		{"id": int64(1), "category": "A", "amount": float64(50)},
		{"id": int64(2), "category": "A", "amount": float64(60)},
		{"id": int64(3), "category": "A", "amount": float64(10)},
		{"id": int64(4), "category": "B", "amount": float64(200)},
		{"id": int64(5), "category": "C", "amount": float64(30)},
		{"id": int64(6), "category": "C", "amount": float64(40)},
	})

	builder := NewQueryBuilder(ds)

	// SELECT category, COUNT(*) as cnt, SUM(amount) as total
	// FROM sales GROUP BY category
	// HAVING COUNT(*) >= 3 OR SUM(amount) >= 200
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{Name: "category"},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Alias: "cnt",
				Name:  "COUNT(*)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Alias: "total",
				Name:  "SUM(amount)",
			},
		},
		From:    "sales",
		GroupBy: []string{"category"},
		Having: &Expression{
			Type:     ExprTypeOperator,
			Operator: "or",
			Left: &Expression{
				Type:     ExprTypeOperator,
				Operator: "ge",
				Left: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeValue, Value: int64(1)}},
				},
				Right: &Expression{
					Type:  ExprTypeValue,
					Value: int64(3),
				},
			},
			Right: &Expression{
				Type:     ExprTypeOperator,
				Operator: "ge",
				Left: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "amount"}},
				},
				Right: &Expression{
					Type:  ExprTypeValue,
					Value: int64(200),
				},
			},
		},
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Category A: cnt=3 >=3 -> PASS (OR short-circuits, already passes)
	// Category B: cnt=1 <3, sum=200 >=200 -> PASS
	// Category C: cnt=2 <3, sum=70 <200 -> FAIL
	// Categories A and B pass.
	if len(result.Rows) != 2 {
		t.Errorf("HAVING with OR: expected 2 rows, got %d", len(result.Rows))
		for i, row := range result.Rows {
			t.Logf("  row[%d]: %v", i, row)
		}
	}

	categories := make(map[string]bool)
	for _, row := range result.Rows {
		categories[fmt.Sprintf("%v", row["category"])] = true
	}
	if !categories["A"] {
		t.Errorf("HAVING with OR: expected category A to be present")
	}
	if !categories["B"] {
		t.Errorf("HAVING with OR: expected category B to be present")
	}
	if categories["C"] {
		t.Errorf("HAVING with OR: category C should be filtered out")
	}
}

func TestExecuteSelect_CountStarOnEmptyTable(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("empty", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "value", Type: "float64"},
	}, []domain.Row{})

	builder := NewQueryBuilder(ds)

	// Test AVG on empty table -> should return 0 (different from SUM which is already tested)
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "avg",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "avg_val",
				Name:  "AVG(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "min",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "min_val",
				Name:  "MIN(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "max",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "max_val",
				Name:  "MAX(value)",
			},
		},
		From: "empty",
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	// Aggregates on empty table always return exactly 1 row
	if len(result.Rows) != 1 {
		t.Fatalf("Aggregate on empty table: expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]

	// AVG on empty table returns 0 (no rows to count)
	if toFloat64(row["avg_val"]) != 0 {
		t.Errorf("AVG on empty table: expected 0, got %v", row["avg_val"])
	}

	// MIN on empty table returns nil (computeMin returns nil for 0 rows)
	if row["min_val"] != nil {
		t.Errorf("MIN on empty table: expected nil, got %v", row["min_val"])
	}

	// MAX on empty table returns nil (computeMax returns nil for 0 rows)
	if row["max_val"] != nil {
		t.Errorf("MAX on empty table: expected nil, got %v", row["max_val"])
	}
}

func TestExecuteSelect_MinMaxAllNulls(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("nulldata", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
		{Name: "value", Type: "float64", Nullable: true},
	}, []domain.Row{
		{"id": int64(1), "value": nil},
		{"id": int64(2), "value": nil},
		{"id": int64(3), "value": nil},
	})

	builder := NewQueryBuilder(ds)

	// SELECT MIN(value), MAX(value), SUM(value), AVG(value), COUNT(value) FROM nulldata
	stmt := &SelectStatement{
		Columns: []SelectColumn{
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "min",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "mn",
				Name:  "MIN(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "max",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "mx",
				Name:  "MAX(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "sum",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "s",
				Name:  "SUM(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "avg",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "a",
				Name:  "AVG(value)",
			},
			{
				Expr: &Expression{
					Type:     ExprTypeFunction,
					Function: "count",
					Args:     []Expression{{Type: ExprTypeColumn, Column: "value"}},
				},
				Alias: "cnt",
				Name:  "COUNT(value)",
			},
		},
		From: "nulldata",
	}

	result, err := builder.executeSelect(context.Background(), stmt)
	if err != nil {
		t.Fatalf("executeSelect failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("All-null aggregates: expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]

	// MIN when all values are nil -> nil
	if row["mn"] != nil {
		t.Errorf("MIN all nulls: expected nil, got %v", row["mn"])
	}

	// MAX when all values are nil -> nil
	if row["mx"] != nil {
		t.Errorf("MAX all nulls: expected nil, got %v", row["mx"])
	}

	// SUM when all values are nil -> 0 (sum starts at 0 and no non-nil values add to it)
	if toFloat64(row["s"]) != 0 {
		t.Errorf("SUM all nulls: expected 0, got %v", row["s"])
	}

	// AVG when all values are nil -> 0 (count is 0, returns 0)
	if toFloat64(row["a"]) != 0 {
		t.Errorf("AVG all nulls: expected 0, got %v", row["a"])
	}

	// COUNT(value) when all values are nil -> 0
	if toInt64(row["cnt"]) != 0 {
		t.Errorf("COUNT(col) all nulls: expected 0, got %v", row["cnt"])
	}
}

func TestGetColumnValue_PrefixResolution(t *testing.T) {
	ds := newMockDataSource()
	ds.addTable("dummy", []domain.ColumnInfo{
		{Name: "id", Type: "int64", Primary: true},
	}, []domain.Row{})

	builder := NewQueryBuilder(ds)

	// Test 1: Direct key match
	row := domain.Row{
		"name":       "direct",
		"users.name": "prefixed",
		"orders.id":  int64(42),
	}

	// Direct lookup: key "name" exists directly
	val := builder.getColumnValue(row, "name")
	if val != "direct" {
		t.Errorf("getColumnValue direct lookup: expected 'direct', got %v", val)
	}

	// Direct lookup for prefixed key: key "orders.id" exists directly
	val = builder.getColumnValue(row, "orders.id")
	if toInt64(val) != 42 {
		t.Errorf("getColumnValue direct prefixed lookup: expected 42, got %v", val)
	}

	// Test 2: Prefix-based (suffix) lookup when direct key doesn't exist
	row2 := domain.Row{
		"users.email":  "alice@example.com",
		"orders.total": float64(99.5),
	}

	// Looking up "email" should find "users.email" via suffix match
	val = builder.getColumnValue(row2, "email")
	if val != "alice@example.com" {
		t.Errorf("getColumnValue suffix lookup 'email': expected 'alice@example.com', got %v", val)
	}

	// Looking up "total" should find "orders.total" via suffix match
	val = builder.getColumnValue(row2, "total")
	if toFloat64(val) != 99.5 {
		t.Errorf("getColumnValue suffix lookup 'total': expected 99.5, got %v", val)
	}

	// Test 3: Key that doesn't exist at all returns nil
	val = builder.getColumnValue(row2, "nonexistent")
	if val != nil {
		t.Errorf("getColumnValue missing key: expected nil, got %v", val)
	}
}
