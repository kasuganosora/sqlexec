package memory

import (
	"context"
	"fmt"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/util"
)

// TestFilterableDataSourceIntegration 测试MVCCDataSource的FilterableDataSource接口集成
func TestFilterableDataSourceIntegration(t *testing.T) {
	ctx := context.Background()

	// 创建MVCC数据源
	ds := NewMVCCDataSource(nil)
	if err := ds.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to datasource: %v", err)
	}
	defer ds.Close(ctx)

	// 创建测试表
	tableName := "test_users"
	tableInfo := &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
			{Name: "status", Type: "string"},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 插入测试数据
	numRows := 1000
	for i := 0; i < numRows; i++ {
		row := domain.Row{
			"id":     int64(i + 1),
			"name":   fmt.Sprintf("user_%d", i+1),
			"age":    int64(20 + i%50), // age range: 20-69
			"status": "active",
		}
		if i%10 == 0 {
			row["status"] = "inactive"
		}
		if _, err := ds.Insert(ctx, tableName, []domain.Row{row}, nil); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// 测试1: 验证SupportsFiltering
	t.Run("SupportsFiltering", func(t *testing.T) {
		if !ds.SupportsFiltering(tableName) {
			t.Errorf("Expected SupportsFiltering to return true for table %s", tableName)
		}

		if ds.SupportsFiltering("nonexistent_table") {
			t.Errorf("Expected SupportsFiltering to return false for nonexistent table")
		}
	})

	// 测试2: 简单过滤
	t.Run("SimpleFilter", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: ">",
			Value:    int64(40),
		}

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 0)
		if err != nil {
			t.Fatalf("Filter() failed: %v", err)
		}

		// 验证结果
		// age range: 20-69 (i%50 gives 0-49, so 20-69)
		// ages > 40: 41-69 = 29 unique values
		// each value appears numRows/50 = 1000/50 = 20 times
		// total: 29 * 20 = 580 rows
		expectedCount := 580
		if total != int64(expectedCount) {
			t.Errorf("Expected total=%d, got %d", expectedCount, total)
		}

		if len(rows) != expectedCount {
			t.Errorf("Expected %d rows, got %d", expectedCount, len(rows))
		}

		// 验证所有结果都符合过滤条件
		for _, row := range rows {
			if age, ok := row["age"].(int64); !ok || age <= 40 {
				t.Errorf("Found row with age <= 40: %v", row)
			}
		}
	})

	// 测试3: 带分页的过滤
	t.Run("FilterWithPagination", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: ">",
			Value:    int64(40),
		}

		offset := 10
		limit := 5

		rows, total, err := ds.Filter(ctx, tableName, filter, offset, limit)
		if err != nil {
			t.Fatalf("Filter() with pagination failed: %v", err)
		}

		// total应该不受limit影响
		expectedTotal := 580
		if total != int64(expectedTotal) {
			t.Errorf("Expected total=%d, got %d", expectedTotal, total)
		}

		// rows的长度应该受limit影响
		if len(rows) != limit {
			t.Errorf("Expected %d rows with limit=%d, got %d", limit, limit, len(rows))
		}

		// 验证所有结果都符合过滤条件
		for _, row := range rows {
			if age, ok := row["age"].(int64); !ok || age <= 40 {
				t.Errorf("Found row with age <= 40: %v", row)
			}
		}
	})

	// 测试4: 复合过滤 (AND)
	t.Run("CompositeFilter_AND", func(t *testing.T) {
		filter := domain.Filter{
			Logic: "AND",
			Value: []domain.Filter{
				{Field: "age", Operator: ">", Value: int64(30)},
				{Field: "age", Operator: "<", Value: int64(50)},
				{Field: "status", Operator: "=", Value: "active"},
			},
		}

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 0)
		if err != nil {
			t.Fatalf("Filter() with AND logic failed: %v", err)
		}

		// 验证结果
		if total <= 0 {
			t.Errorf("Expected some rows with composite filter, got %d", total)
		}

		// 验证所有结果都符合所有过滤条件
		for _, row := range rows {
			age, ok := row["age"].(int64)
			if !ok || age <= 30 || age >= 50 {
				t.Errorf("Found row with invalid age: %v", row)
			}

			status, ok := row["status"].(string)
			if !ok || status != "active" {
				t.Errorf("Found row with invalid status: %v", row)
			}
		}
	})

	// 测试5: 字符串过滤
	t.Run("StringFilter", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "status",
			Operator: "=",
			Value:    "active",
		}

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 0)
		if err != nil {
			t.Fatalf("Filter() with string value failed: %v", err)
		}

		expectedActive := numRows * 9 / 10 // 90% active
		if total != int64(expectedActive) {
			t.Errorf("Expected %d active users, got %d", expectedActive, total)
		}

		// 验证所有结果都是active状态
		for _, row := range rows {
			status, ok := row["status"].(string)
			if !ok || status != "active" {
				t.Errorf("Found non-active row: %v", row)
			}
		}
	})

	// 测试6: LIKE操作符
	t.Run("LikeFilter", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "name",
			Operator: "LIKE",
			Value:    "user_1%",
		}

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 0)
		if err != nil {
			t.Fatalf("Filter() with LIKE operator failed: %v", err)
		}

		// 验证结果数量 (user_10 to user_19, user_100 to user_199, etc.)
		if total <= 0 {
			t.Errorf("Expected some rows matching pattern, got %d", total)
		}

		// 验证所有结果匹配模式
		for _, row := range rows {
			name, ok := row["name"].(string)
			if !ok || len(name) < 6 || name[0:6] != "user_1" {
				t.Errorf("Found row with invalid name: %v", row)
			}
		}
	})

	// 测试7: 空过滤条件
	t.Run("EmptyFilter", func(t *testing.T) {
		filter := domain.Filter{}

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 0)
		if err != nil {
			t.Fatalf("Filter() with empty filter failed: %v", err)
		}

		// 应该返回所有行
		if total != int64(numRows) {
			t.Errorf("Expected %d rows with empty filter, got %d", numRows, total)
		}

		if len(rows) != numRows {
			t.Errorf("Expected %d rows with empty filter, got %d", numRows, len(rows))
		}
	})
}

// TestFilterableDataSource_Performance 性能测试：对比Filter方法和普通Query方法
func TestFilterableDataSource_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()

	// 创建MVCC数据源
	ds := NewMVCCDataSource(nil)
	if err := ds.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to datasource: %v", err)
	}
	defer ds.Close(ctx)

	// 创建测试表
	tableName := "perf_test_users"
	tableInfo := &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, AutoIncrement: true},
			{Name: "name", Type: "string"},
			{Name: "age", Type: "int64"},
			{Name: "status", Type: "string"},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 插入大量测试数据
	numRows := 10000
	for i := 0; i < numRows; i++ {
		row := domain.Row{
			"id":     int64(i + 1),
			"name":   fmt.Sprintf("user_%d", i+1),
			"age":    int64(20 + i%80), // age range: 20-99
			"status": "active",
		}
		if i%10 == 0 {
			row["status"] = "inactive"
		}
		if _, err := ds.Insert(ctx, tableName, []domain.Row{row}, nil); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// 准备过滤条件
	filter := domain.Filter{
		Logic: "AND",
		Value: []domain.Filter{
			{Field: "age", Operator: ">", Value: int64(60)},
			{Field: "status", Operator: "=", Value: "active"},
		},
	}

	// 方法1: 使用Filter方法（FilterableDataSource接口）
	t.Run("FilterMethod", func(t *testing.T) {
		t.Log("Testing Filter method performance...")

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 100)
		if err != nil {
			t.Fatalf("Filter() failed: %v", err)
		}

		t.Logf("Filter method returned %d rows (total=%d)", len(rows), total)

		// 验证结果正确性
		for _, row := range rows {
			age, ok := row["age"].(int64)
			if !ok || age <= 60 {
				t.Errorf("Found row with invalid age: %v", row)
			}

			status, ok := row["status"].(string)
			if !ok || status != "active" {
				t.Errorf("Found row with invalid status: %v", row)
			}
		}
	})

	// 方法2: 使用Query方法并在内存中过滤（传统方法）
	t.Run("QueryMethod", func(t *testing.T) {
		t.Log("Testing Query method performance...")

		options := &domain.QueryOptions{
			Filters: []domain.Filter{
				{Field: "age", Operator: ">", Value: int64(60)},
				{Field: "status", Operator: "=", Value: "active"},
			},
			Limit: 100,
		}

		result, err := ds.Query(ctx, tableName, options)
		if err != nil {
			t.Fatalf("Query() failed: %v", err)
		}

		t.Logf("Query method returned %d rows (total=%d)", len(result.Rows), result.Total)

		// 验证结果正确性
		for _, row := range result.Rows {
			age, ok := row["age"].(int64)
			if !ok || age <= 60 {
				t.Errorf("Found row with invalid age: %v", row)
			}

			status, ok := row["status"].(string)
			if !ok || status != "active" {
				t.Errorf("Found row with invalid status: %v", row)
			}
		}
	})
}

// TestFilterableDataSource_EdgeCases 测试边界情况
func TestFilterableDataSource_EdgeCases(t *testing.T) {
	ctx := context.Background()

	// 创建MVCC数据源
	ds := NewMVCCDataSource(nil)
	if err := ds.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to datasource: %v", err)
	}
	defer ds.Close(ctx)

	// 创建测试表
	tableName := "edge_test_table"
	tableInfo := &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "value", Type: "int64", Nullable: true},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 测试：不存在的表
	t.Run("NonexistentTable", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "id",
			Operator: ">",
			Value:    int64(0),
		}

		_, _, err := ds.Filter(ctx, "nonexistent_table", filter, 0, 0)
		if err == nil {
			t.Errorf("Expected error for nonexistent table, got nil")
		}
	})

	// 测试：超出范围的分页
	t.Run("OutOfRangePagination", func(t *testing.T) {
		// 插入少量数据
		for i := 0; i < 5; i++ {
			row := domain.Row{
				"id":    int64(i + 1),
				"value": int64(i * 10),
			}
			if _, err := ds.Insert(ctx, tableName, []domain.Row{row}, nil); err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}

		filter := domain.Filter{}
		offset := 100 // 超出范围
		limit := 10

		rows, total, err := ds.Filter(ctx, tableName, filter, offset, limit)
		if err != nil {
			t.Fatalf("Filter() failed: %v", err)
		}

		// total应该正确
		if total != 5 {
			t.Errorf("Expected total=5, got %d", total)
		}

		// rows应该为空
		if len(rows) != 0 {
			t.Errorf("Expected 0 rows with out-of-range offset, got %d", len(rows))
		}
	})

	// 测试：不匹配的过滤条件
	t.Run("NoMatchingFilter", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "value",
			Operator: ">",
			Value:    int64(1000), // 没有数据满足
		}

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 0)
		if err != nil {
			t.Fatalf("Filter() failed: %v", err)
		}

		// 应该返回空结果
		if total != 0 {
			t.Errorf("Expected total=0 for non-matching filter, got %d", total)
		}

		if len(rows) != 0 {
			t.Errorf("Expected 0 rows for non-matching filter, got %d", len(rows))
		}
	})
}

// TestFilterableDataSource_WithUtilMatchesFilter 测试util.ApplyFilters的兼容性
func TestFilterableDataSource_WithUtilMatchesFilter(t *testing.T) {
	ctx := context.Background()

	// 创建MVCC数据源
	ds := NewMVCCDataSource(nil)
	if err := ds.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to datasource: %v", err)
	}
	defer ds.Close(ctx)

	// 创建测试表
	tableName := "compat_test_table"
	tableInfo := &domain.TableInfo{
		Name: tableName,
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Primary: true, Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "age", Type: "int64", Nullable: true},
		},
	}

	if err := ds.CreateTable(ctx, tableInfo); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 插入测试数据
	testRows := []domain.Row{
		{"id": int64(1), "name": "Alice", "age": int64(30)},
		{"id": int64(2), "name": "Bob", "age": int64(25)},
		{"id": int64(3), "name": "Charlie", "age": int64(35)},
		{"id": int64(4), "name": "David", "age": int64(28)},
	}

	for _, row := range testRows {
		if _, err := ds.Insert(ctx, tableName, []domain.Row{row}, nil); err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// 测试：使用util.MatchesFilter验证Filter方法返回的结果
	t.Run("MatchesFilterCompatibility", func(t *testing.T) {
		filter := domain.Filter{
			Field:    "age",
			Operator: ">",
			Value:    int64(28),
		}

		rows, total, err := ds.Filter(ctx, tableName, filter, 0, 0)
		if err != nil {
			t.Fatalf("Filter() failed: %v", err)
		}

		expectedCount := 2 // Alice (30) and Charlie (35)
		if total != int64(expectedCount) {
			t.Errorf("Expected total=%d, got %d", expectedCount, total)
		}

		if len(rows) != expectedCount {
			t.Errorf("Expected %d rows, got %d", expectedCount, len(rows))
		}

		// 使用util.MatchesFilters验证每个结果
		for _, row := range rows {
			filters := []domain.Filter{filter}
			matches := util.MatchesFilters(row, filters)
			if !matches {
				t.Errorf("Row does not match filter: %v", row)
			}
		}
	})
}
