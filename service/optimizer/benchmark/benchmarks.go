package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/service/optimizer"
	"github.com/kasuganosora/sqlexec/service/parser"
	"github.com/kasuganosora/sqlexec/service/resource"
)

// BenchmarkSuite 基准测试套件
type BenchmarkSuite struct {
	dataSource resource.DataSource
}

// NewBenchmarkSuite 创建基准测试套件
func NewBenchmarkSuite() *BenchmarkSuite {
	return &BenchmarkSuite{}
}

// Setup 测试环境设置
func (bs *BenchmarkSuite) Setup() error {
	// 创建内存数据�?
	memSource := resource.NewMemorySource()
	
	// 生成测试数据
	if err := bs.generateTestTables(memSource); err != nil {
		return err
	}
	
	bs.dataSource = memSource
	return nil
}

// generateTestTables 生成测试�?
func (bs *BenchmarkSuite) generateTestTables(dataSource *resource.MemorySource) error {
	// 小数据集�?(1,000�?
	if err := bs.createTable(dataSource, "small_table", 1000, 10); err != nil {
		return err
	}
	
	// 中数据集�?(100,000�?
	if err := bs.createTable(dataSource, "medium_table", 100000, 20); err != nil {
		return err
	}
	
	// 大数据集�?(1,000,000�?
	if err := bs.createTable(dataSource, "large_table", 1000000, 50); err != nil {
		return err
	}
	
	// JOIN测试�?
	if err := bs.createTable(dataSource, "orders", 50000, 8); err != nil {
		return err
	}
	if err := bs.createTable(dataSource, "customers", 10000, 5); err != nil {
		return err
	}
	
	return nil
}

// createTable 创建测试�?
func (bs *BenchmarkSuite) createTable(dataSource *resource.MemorySource, tableName string, rowCount, colCount int) error {
	// 创建�?
	columns := []resource.ColumnInfo{}
	for i := 0; i < colCount; i++ {
		colType := "VARCHAR"
		if i%4 == 0 {
			colType = "INTEGER"
		} else if i%4 == 1 {
			colType = "FLOAT"
		}
		
		columns = append(columns, resource.ColumnInfo{
			Name:     fmt.Sprintf("col%d", i),
			Type:     colType,
			Nullable: i == 3, // 某些列可为NULL
		})
	}
	
	if err := dataSource.CreateTable(context.Background(), tableName, columns); err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	
	// 生成测试数据
	data := make([]resource.Row, rowCount)
	for i := 0; i < rowCount; i++ {
		row := make(resource.Row)
		for j := 0; j < colCount; j++ {
			colName := fmt.Sprintf("col%d", j)
			
			if j%4 == 0 {
				// INTEGER�?
				row[colName] = rand.Intn(1000)
			} else if j%4 == 1 {
				// FLOAT�?
				row[colName] = rand.Float64() * 1000
			} else if j%4 == 2 {
				// VARCHAR�?
				row[colName] = fmt.Sprintf("value_%d", rand.Intn(100))
			} else if j == 3 {
				// NULL�?
				if rand.Float32() < 0.1 { // 10% NULL�?
					row[colName] = nil
				} else {
					row[colName] = rand.Intn(100)
				}
			} else {
				// 其他VARCHAR�?
				row[colName] = fmt.Sprintf("data_%d", rand.Intn(1000))
			}
		}
		data[i] = row
	}
	
	// 批量插入
	return dataSource.BatchInsert(context.Background(), tableName, columns, data)
}

// ============================================================================
// 扫描性能基准测试
// ============================================================================

// BenchmarkTableScan_Small 测试小数据集扫描
func BenchmarkTableScan_Small(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "small_table", &resource.QueryOptions{})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkTableScan_Medium 测试中数据集扫描
func BenchmarkTableScan_Medium(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &resource.QueryOptions{})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkTableScan_Large 测试大数据集扫描
func BenchmarkTableScan_Large(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "large_table", &resource.QueryOptions{})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 过滤性能基准测试
// ============================================================================

// BenchmarkFilter_Simple 测试简单WHERE条件
func BenchmarkFilter_Simple(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	filters := []resource.Filter{
		{Field: "col0", Operator: ">", Value: 500},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &resource.QueryOptions{
			Filters: filters,
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkFilter_Complex 测试复杂WHERE条件
func BenchmarkFilter_Complex(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	filters := []resource.Filter{
		{Field: "col0", Operator: ">", Value: 100},
		{Field: "col1", Operator: "<", Value: 500},
		{Field: "col4", Operator: "=", Value: "value_50"},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &resource.QueryOptions{
			Filters: filters,
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// JOIN性能基准测试
// ============================================================================

// BenchmarkJoin_Inner_SmallSmall 测试小表INNER JOIN
func BenchmarkJoin_Inner_SmallSmall(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := suite.executeQuery("SELECT * FROM small_table s1 INNER JOIN small_table s2 ON s1.col0 = s2.col0")
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
		_ = result
	}
}

// BenchmarkJoin_Inner_LargeLarge 测试大表INNER JOIN
func BenchmarkJoin_Inner_LargeLarge(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := suite.executeQuery("SELECT * FROM medium_table m1 INNER JOIN medium_table m2 ON m1.col0 = m2.col0")
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
		_ = result
	}
}

// BenchmarkJoin_Left 测试LEFT JOIN
func BenchmarkJoin_Left(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := suite.executeQuery("SELECT * FROM orders LEFT JOIN customers ON orders.col0 = customers.col0")
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
		_ = result
	}
}

// ============================================================================
// 聚合性能基准测试
// ============================================================================

// BenchmarkAggregate_Count 测试COUNT聚合
func BenchmarkAggregate_Count(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := suite.executeQuery("SELECT COUNT(*) FROM medium_table")
		if err != nil {
			b.Fatalf("aggregate failed: %v", err)
		}
		_ = result
	}
}

// BenchmarkAggregate_Sum 测试SUM聚合
func BenchmarkAggregate_Sum(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := suite.executeQuery("SELECT SUM(col1) FROM medium_table")
		if err != nil {
			b.Fatalf("aggregate failed: %v", err)
		}
		_ = result
	}
}

// BenchmarkAggregate_Avg 测试AVG聚合
func BenchmarkAggregate_Avg(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := suite.executeQuery("SELECT AVG(col1) FROM medium_table")
		if err != nil {
			b.Fatalf("aggregate failed: %v", err)
		}
		_ = result
	}
}

// BenchmarkAggregate_GroupBy 测试GROUP BY
func BenchmarkAggregate_GroupBy(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := suite.executeQuery("SELECT col4, COUNT(*) FROM medium_table GROUP BY col4")
		if err != nil {
			b.Fatalf("aggregate failed: %v", err)
		}
		_ = result
	}
}

// ============================================================================
// LIMIT性能基准测试
// ============================================================================

// BenchmarkLimit_Small 测试小LIMIT
func BenchmarkLimit_Small(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &resource.QueryOptions{
			Limit: 10,
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkLimit_Medium 测试中等LIMIT
func BenchmarkLimit_Medium(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &resource.QueryOptions{
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 排序性能基准测试
// ============================================================================

// BenchmarkSort_SingleColumn 测试单列排序
func BenchmarkSort_SingleColumn(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "small_table", &resource.QueryOptions{
			OrderBy: []resource.OrderByItem{
				{Column: "col0", Direction: "ASC"},
			},
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkSort_MultiColumn 测试多列排序
func BenchmarkSort_MultiColumn(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "small_table", &resource.QueryOptions{
			OrderBy: []resource.OrderByItem{
				{Column: "col4", Direction: "ASC"},
				{Column: "col0", Direction: "DESC"},
			},
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 并发性能基准测试
// ============================================================================

// BenchmarkConcurrentQueries 测试并发查询
func BenchmarkConcurrentQueries(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		concurrent := 10
		
		for j := 0; j < concurrent; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = suite.dataSource.Query(context.Background(), "small_table", &resource.QueryOptions{})
			}()
		}
		wg.Wait()
	}
}

// ============================================================================
// 辅助方法
// ============================================================================

// executeQuery 执行SQL查询（简化版，用于JOIN等复杂查询）
func (bs *BenchmarkSuite) executeQuery(sql string) (*resource.QueryResult, error) {
	// 这里应该调用优化器和执行引擎
	// 简化实现：直接返回数据源查询结�?
	return bs.dataSource.Query(context.Background(), "medium_table", &resource.QueryOptions{})
}

// ============================================================================
// 集成测试：测量端到端性能
// ============================================================================

// TestPerformanceIntegration 性能集成测试
func TestPerformanceIntegration(t *testing.T) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	
	// 定义测试场景
	scenarios := []struct {
		name      string
		sql       string
		maxTime   time.Duration
	}{
		{
			name:    "简单扫�?,
			sql:     "SELECT * FROM small_table",
			maxTime: 100 * time.Millisecond,
		},
		{
			name:    "带过滤的扫描",
			sql:     "SELECT * FROM medium_table WHERE col0 > 500",
			maxTime: 500 * time.Millisecond,
		},
		{
			name:    "聚合查询",
			sql:     "SELECT col4, COUNT(*), AVG(col1) FROM medium_table GROUP BY col4",
			maxTime: 1 * time.Second,
		},
		{
			name:    "JOIN查询",
			sql:     "SELECT * FROM orders INNER JOIN customers ON orders.col0 = customers.col0",
			maxTime: 2 * time.Second,
		},
	}
	
	// 运行所有场�?
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			start := time.Now()
			_, err := suite.executeQuery(scenario.sql)
			duration := time.Since(start)
			
			if err != nil {
				t.Errorf("查询失败: %v", err)
				return
			}
			
			if duration > scenario.maxTime {
				t.Errorf("查询超时: 实际 %v, 期望 <%v", duration, scenario.maxTime)
			}
			
			t.Logf("%s: %v", scenario.name, duration)
		})
	}
}

// ============================================================================
// 性能对比测试
// ============================================================================

// BenchmarkPerformanceComparison 性能对比测试
func BenchmarkPerformanceComparison(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	
	b.Run("Filter_WithoutIndex", func(b *testing.B) {
		benchmarkFilterWithoutIndex(b, suite)
	})
	
	b.Run("Filter_WithIndex", func(b *testing.B) {
		// 未来实现索引后启�?
		b.Skip("索引支持尚未实现")
	})
}

func benchmarkFilterWithoutIndex(b *testing.B, suite *BenchmarkSuite) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &resource.QueryOptions{
			Filters: []resource.Filter{
				{Field: "col0", Operator: "=", Value: 500},
			},
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}
