package optimizer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/dataaccess"
	"github.com/kasuganosora/sqlexec/pkg/executor"
	"github.com/kasuganosora/sqlexec/pkg/parser"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// ============================================================================
// 基准测试套件结构
// ============================================================================

// BenchmarkSuite 基准测试套件
type BenchmarkSuite struct {
	dataSource domain.DataSource
	optimizer  *Optimizer
	tables     map[string]*domain.TableInfo
}

// NewBenchmarkSuite 创建基准测试套件
func NewBenchmarkSuite() *BenchmarkSuite {
	return &BenchmarkSuite{
		tables: make(map[string]*domain.TableInfo),
	}
}

// Setup 设置基准测试环境
func (bs *BenchmarkSuite) Setup() error {
	// 创建内存数据源
	factory := memory.NewMemoryFactory()
	memSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	if err != nil {
		return err
	}

	ms, ok := memSource.(*memory.MVCCDataSource)
	if !ok {
		return fmt.Errorf("failed to create memory source")
	}

	// 生成测试数据
	if err := bs.generateBenchmarkTables(ms); err != nil {
		return err
	}

	bs.dataSource = memSource
	bs.optimizer = NewOptimizer(memSource)
	return nil
}

// Cleanup 清理基准测试环境
func (bs *BenchmarkSuite) Cleanup() {
	if bs.dataSource != nil {
		ctx := context.Background()
		for tableName := range bs.tables {
			_ = bs.dataSource.DropTable(ctx, tableName)
		}
	}
	bs.tables = make(map[string]*domain.TableInfo)
}

// generateBenchmarkTables 生成基准测试表
func (bs *BenchmarkSuite) generateBenchmarkTables(dataSource *memory.MVCCDataSource) error {
	ctx := context.Background()

	// 单表查询测试表
	tables := []struct {
		name     string
		rowCount int
		colCount int
	}{
		{"small_table", 100, 10},
		{"medium_table", 10000, 20},
		{"large_table", 100000, 50},
	}

	for _, tbl := range tables {
		if err := bs.createTableWithData(ctx, dataSource, tbl.name, tbl.rowCount, tbl.colCount); err != nil {
			return err
		}
	}

	// JOIN测试表
	joinTables := []struct {
		name     string
		rowCount int
		colCount int
	}{
		{"join_table_1k", 1000, 8},
		{"join_table_10k", 10000, 8},
		{"join_table_500", 500, 8},
		{"fact_table", 10000, 12},
		{"dim_table_a", 1000, 6},
		{"dim_table_b", 1000, 6},
		{"dim_table_c", 500, 6},
		{"orders", 5000, 10},
		{"customers", 1000, 8},
		{"products", 500, 6},
	}

	for _, tbl := range joinTables {
		if err := bs.createTableWithData(ctx, dataSource, tbl.name, tbl.rowCount, tbl.colCount); err != nil {
			return err
		}
	}

	return nil
}

// createTableWithData 创建表并填充数据
func (bs *BenchmarkSuite) createTableWithData(ctx context.Context, dataSource *memory.MVCCDataSource, tableName string, rowCount, colCount int) error {
	// 创建表
	columns := make([]domain.ColumnInfo, colCount)
	for i := 0; i < colCount; i++ {
		colType := "VARCHAR"
		if i%4 == 0 {
			colType = "INTEGER"
		} else if i%4 == 1 {
			colType = "FLOAT"
		}

		columns[i] = domain.ColumnInfo{
			Name:     fmt.Sprintf("col%d", i),
			Type:     colType,
			Nullable: i == 3,
		}
	}

	tableInfo := &domain.TableInfo{
		Name:    tableName,
		Columns: columns,
	}

	if err := dataSource.CreateTable(ctx, tableInfo); err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}

	bs.tables[tableName] = tableInfo

	// 生成测试数据（分批插入以避免内存溢出）
	batchSize := 1000
	for i := 0; i < rowCount; i += batchSize {
		end := i + batchSize
		if end > rowCount {
			end = rowCount
		}

		data := make([]domain.Row, end-i)
		for j := i; j < end; j++ {
			row := make(domain.Row)
			for k := 0; k < colCount; k++ {
				colName := fmt.Sprintf("col%d", k)

				if k%4 == 0 {
					// INTEGER列 - 使用确定性值以便JOIN匹配
					row[colName] = (j % 1000) // 重复值，便于JOIN
				} else if k%4 == 1 {
					// FLOAT列
					row[colName] = float64(rand.Intn(10000)) / 100.0
				} else if k%4 == 2 {
					// VARCHAR列
					row[colName] = fmt.Sprintf("value_%d", rand.Intn(100))
				} else {
					// NULL列
					if rand.Float32() < 0.1 {
						row[colName] = nil
					} else {
						row[colName] = rand.Intn(100)
					}
				}
			}
			data[j-i] = row
		}

		if _, err := dataSource.Insert(ctx, tableName, data, nil); err != nil {
			return fmt.Errorf("insert batch failed: %w", err)
		}
	}

	return nil
}

// executeQuery 执行查询并返回结果
func (bs *BenchmarkSuite) executeQuery(sql string) (*domain.QueryResult, error) {
	ctx := context.Background()

	// 解析SQL
	adapter := parser.NewSQLAdapter()
	parseResult, err := adapter.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse failed: %w", err)
	}

	// 优化查询
	p, err := bs.optimizer.Optimize(ctx, parseResult.Statement)
	if err != nil {
		return nil, fmt.Errorf("optimize failed: %w", err)
	}

	// 使用executor执行计划
	das := dataaccess.NewDataService(bs.dataSource)
	exec := executor.NewExecutor(das)
	result, err := exec.Execute(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("execute failed: %w", err)
	}

	return result, nil
}

// ============================================================================
// 1. 单表查询基准测试
// ============================================================================

// BenchmarkSingleTable_Small 小数据集单表查询
func BenchmarkSingleTable_Small(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "small_table", &domain.QueryOptions{})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkSingleTable_Medium 中等数据集单表查询
func BenchmarkSingleTable_Medium(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &domain.QueryOptions{})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkSingleTable_Large 大数据集单表查询
func BenchmarkSingleTable_Large(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "large_table", &domain.QueryOptions{})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 2. JOIN性能基准测试
// ============================================================================

// BenchmarkJoin2Table_Inner 2表INNER JOIN
func BenchmarkJoin2Table_Inner(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM join_table_1k t1 INNER JOIN join_table_1k t2 ON t1.col0 = t2.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// BenchmarkJoin2Table_Left 2表LEFT JOIN
func BenchmarkJoin2Table_Left(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM join_table_10k t1 LEFT JOIN join_table_1k t2 ON t1.col0 = t2.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// BenchmarkJoin2Table_Right 2表RIGHT JOIN
func BenchmarkJoin2Table_Right(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM join_table_1k t1 RIGHT JOIN join_table_10k t2 ON t1.col0 = t2.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// BenchmarkJoin2Table_Full 2表FULL OUTER JOIN (暂不支持，跳过)
func BenchmarkJoin2Table_Full(b *testing.B) {
	b.Skip("UNION ALL not yet supported")
}

// BenchmarkJoin3Table_Chain 3表链式JOIN
func BenchmarkJoin3Table_Chain(b *testing.B) {
	b.Skip("3-table join not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM join_table_1k t1 INNER JOIN join_table_1k t2 ON t1.col0 = t2.col0 INNER JOIN join_table_1k t3 ON t2.col0 = t3.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// BenchmarkJoin3Table_Star 3表星型JOIN
func BenchmarkJoin3Table_Star(b *testing.B) {
	b.Skip("3-table star join not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM fact_table f INNER JOIN dim_table_a a ON f.col0 = a.col0 INNER JOIN dim_table_b b ON f.col1 = b.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// BenchmarkJoin4Table_Chain 4表链式JOIN
func BenchmarkJoin4Table_Chain(b *testing.B) {
	b.Skip("4-table chain join not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM join_table_500 t1 INNER JOIN join_table_500 t2 ON t1.col0 = t2.col0 INNER JOIN join_table_500 t3 ON t2.col0 = t3.col0 INNER JOIN join_table_500 t4 ON t3.col0 = t4.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// BenchmarkJoin4Table_Star 4表星型JOIN
func BenchmarkJoin4Table_Star(b *testing.B) {
	b.Skip("4-table star join not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM fact_table f INNER JOIN dim_table_a a ON f.col0 = a.col0 INNER JOIN dim_table_b b ON f.col1 = b.col0 INNER JOIN dim_table_c c ON f.col2 = c.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// BenchmarkJoin_BushyTree_4Tables 4表Bushy Tree JOIN（简化版本）
func BenchmarkJoin_BushyTree_4Tables(b *testing.B) {
	b.Skip("Bushy tree join not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 简化版本：直接链式JOIN
		sql := "SELECT * FROM join_table_500 t1 INNER JOIN join_table_500 t2 ON t1.col0 = t2.col0 INNER JOIN join_table_500 t3 ON t2.col1 = t3.col0 INNER JOIN join_table_500 t4 ON t3.col1 = t4.col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("join failed: %v", err)
		}
	}
}

// ============================================================================
// 3. 复杂查询基准测试
// ============================================================================

// BenchmarkComplexQuery_MultiCondition 多条件WHERE查询
func BenchmarkComplexQuery_MultiCondition(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM medium_table WHERE col0 > 100 AND col1 < 500 AND col2 = 'value_50'"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkComplexQuery_OR 多OR条件查询
func BenchmarkComplexQuery_OR(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM medium_table WHERE col2 = 'value_10' OR col2 = 'value_20' OR col2 = 'value_30'"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkComplexQuery_GroupBy GROUP BY聚合
func BenchmarkComplexQuery_GroupBy(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT col4, COUNT(*), AVG(col1), SUM(col0) FROM medium_table GROUP BY col4"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkComplexQuery_Subquery 嵌套子查询
func BenchmarkComplexQuery_Subquery(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 使用JOIN代替子查询以测试性能
		sql := "SELECT DISTINCT t1.* FROM medium_table t1 INNER JOIN medium_table t2 ON t1.col0 = t2.col0 WHERE t2.col1 > 500"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkComplexQuery_Complex 复合查询
func BenchmarkComplexQuery_Complex(b *testing.B) {
	b.Skip("Multi-table complex query not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 简化版本的复合查询
		sql := "SELECT t1.col4, COUNT(*) as cnt FROM medium_table t1 INNER JOIN join_table_1k t2 ON t1.col0 = t2.col0 WHERE t1.col0 > 100 GROUP BY t1.col4"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 4. 优化规则性能基准测试
// ============================================================================

// BenchmarkOptimization_PredicatePushdown 谓词下推优化效果
func BenchmarkOptimization_PredicatePushdown(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.Run("WithPredicate", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sql := "SELECT * FROM medium_table WHERE col0 > 500"
			_, err := suite.executeQuery(sql)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
		}
	})

	b.Run("WithoutPredicate", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sql := "SELECT * FROM medium_table"
			_, err := suite.executeQuery(sql)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
		}
	})
}

// BenchmarkOptimization_JoinReorder JOIN重排序优化效果
func BenchmarkOptimization_JoinReorder(b *testing.B) {
	b.Skip("Join reorder not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 多表JOIN，优化器会尝试不同的JOIN顺序
		sql := `SELECT * FROM fact_table f
                INNER JOIN dim_table_a a ON f.col0 = a.col0
                INNER JOIN dim_table_b b ON f.col1 = b.col0
                INNER JOIN dim_table_c c ON f.col2 = c.col0`
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 5. 并行执行性能基准测试
// ============================================================================

// BenchmarkParallel_Scan 并行扫描 vs 串行扫描
func BenchmarkParallel_Scan(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.Run("Serial", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := suite.dataSource.Query(context.Background(), "large_table", &domain.QueryOptions{})
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
		}
	})

	b.Run("Parallel_2", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			wg.Add(2)
			for j := 0; j < 2; j++ {
				go func() {
					defer wg.Done()
					_, _ = suite.dataSource.Query(context.Background(), "large_table", &domain.QueryOptions{})
				}()
			}
			wg.Wait()
		}
	})

	b.Run("Parallel_4", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			wg.Add(4)
			for j := 0; j < 4; j++ {
				go func() {
					defer wg.Done()
					_, _ = suite.dataSource.Query(context.Background(), "large_table", &domain.QueryOptions{})
				}()
			}
			wg.Wait()
		}
	})
}

// BenchmarkParallel_Join 并行JOIN vs 串行JOIN
func BenchmarkParallel_Join(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	sql := "SELECT * FROM join_table_10k t1 INNER JOIN join_table_1k t2 ON t1.col0 = t2.col0"

	b.Run("Serial", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := suite.executeQuery(sql)
			if err != nil {
				b.Fatalf("join failed: %v", err)
			}
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			wg.Add(2)
			for j := 0; j < 2; j++ {
				go func() {
					defer wg.Done()
					_, _ = suite.executeQuery(sql)
				}()
			}
			wg.Wait()
		}
	})
}

// ============================================================================
// 6. 聚合性能基准测试
// ============================================================================

// BenchmarkAggregate_COUNT COUNT聚合
func BenchmarkAggregate_COUNT(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT COUNT(*) FROM medium_table"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkAggregate_SUM SUM聚合
func BenchmarkAggregate_SUM(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT SUM(col1) FROM medium_table"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkAggregate_AVG AVG聚合
func BenchmarkAggregate_AVG(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT AVG(col1) FROM medium_table"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkAggregate_GroupByWithCount GROUP BY + COUNT
func BenchmarkAggregate_GroupByWithCount(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT col4, COUNT(*) FROM medium_table GROUP BY col4"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkAggregate_GroupByWithMultiple GROUP BY + 多个聚合
func BenchmarkAggregate_GroupByWithMultiple(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT col4, COUNT(*), SUM(col1), AVG(col1), MAX(col0), MIN(col0) FROM medium_table GROUP BY col4"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 7. 排序性能基准测试
// ============================================================================

// BenchmarkSort_SmallLimit 小LIMIT查询
func BenchmarkSort_SmallLimit(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := suite.dataSource.Query(context.Background(), "medium_table", &domain.QueryOptions{
			Limit: 10,
		})
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkSort_SingleColumn 单列排序
func BenchmarkSort_SingleColumn(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM medium_table ORDER BY col0"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkSort_OrderByWithLimit ORDER BY + LIMIT
func BenchmarkSort_OrderByWithLimit(b *testing.B) {
	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sql := "SELECT * FROM medium_table ORDER BY col0 LIMIT 100"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 8. 端到端场景基准测试
// ============================================================================

// BenchmarkECommerce_OrderQuery 电商订单查询
func BenchmarkECommerce_OrderQuery(b *testing.B) {
	b.Skip("E-commerce query not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 简化版本的订单查询
		sql := "SELECT c.col2 as customer_name, COUNT(o.col0) as order_count FROM orders o INNER JOIN customers c ON o.col0 = c.col0 GROUP BY c.col2"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// BenchmarkECommerce_ProductAnalysis 产品分析查询
func BenchmarkECommerce_ProductAnalysis(b *testing.B) {
	b.Skip("E-commerce query not yet supported")

	suite := NewBenchmarkSuite()
	if err := suite.Setup(); err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer suite.Cleanup()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 简化版本的产品分析查询
		sql := "SELECT p.col2 as product_name, COUNT(*) as sales_count FROM orders o INNER JOIN products p ON o.col1 = p.col0 GROUP BY p.col2"
		_, err := suite.executeQuery(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

// ============================================================================
// 基准分数记录和报告
// ============================================================================

// BenchmarkResult 基准测试结果
type BenchmarkResult struct {
	Timestamp   string                      `json:"timestamp"`
	GoVersion   string                      `json:"go_version"`
	SystemInfo  SystemInfo                  `json:"system_info"`
	Benchmarks  map[string]BenchmarkMetric  `json:"benchmarks"`
}

// SystemInfo 系统信息
type SystemInfo struct {
	CPUCores    int `json:"cpu_cores"`
	MemoryGB    int `json:"memory_gb"`
	GOMAXPROCS  int `json:"gomaxprocs"`
}

// BenchmarkMetric 基准测试指标
type BenchmarkMetric struct {
	OpsPerSec         float64 `json:"ops_per_sec"`
	NsPerOp           float64 `json:"ns_per_op"`
	AllocsPerOp       float64 `json:"allocs_per_op"`
	AllocedBytesPerOp float64 `json:"alloced_bytes_per_op"`
}

// parseBenchmarkOutput 解析基准测试输出
func parseBenchmarkOutput(output string) map[string]BenchmarkMetric {
	results := make(map[string]BenchmarkMetric)
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}

		// 解析基准测试行
		// 示例: BenchmarkSingleTable_Small-8    10000    123456 ns/op    1234 B/op    12 allocs/op
		parts := strings.Fields(line)
		if len(parts) < 6 {
			continue
		}

		name := strings.Split(parts[0], "-")[0]
		nsPerOp := parseMetric(parts[2])
		allocsPerOp := parseMetric(parts[len(parts)-2])
		allocedBytesPerOp := parseMetric(parts[len(parts)-4])

		opsPerSec := 1e9 / nsPerOp

		results[name] = BenchmarkMetric{
			OpsPerSec:        opsPerSec,
			NsPerOp:          nsPerOp,
			AllocsPerOp:      allocsPerOp,
			AllocedBytesPerOp: allocedBytesPerOp,
		}
	}

	return results
}

// parseMetric 解析指标值
func parseMetric(s string) float64 {
	var val float64
	_, err := fmt.Sscanf(s, "%f", &val)
	if err != nil {
		return 0
	}
	return val
}

// saveBenchmarkResults 保存基准测试结果
func saveBenchmarkResults(results map[string]BenchmarkMetric, filePath string) error {
	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)

	benchmarkResult := BenchmarkResult{
		Timestamp: time.Now().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		SystemInfo: SystemInfo{
			CPUCores:   runtime.NumCPU(),
			MemoryGB:   int(m.Sys / (1024 * 1024 * 1024)),
			GOMAXPROCS: runtime.GOMAXPROCS(0),
		},
		Benchmarks: results,
	}

	data, err := json.MarshalIndent(benchmarkResult, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// TestBenchmarkRunner 运行所有基准测试并保存结果
func TestBenchmarkRunner(t *testing.T) {
	// 跳过正常测试，仅用于生成基准结果
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}

	// 确保benchmark目录存在
	benchmarkDir := "benchmark"
	if err := os.MkdirAll(benchmarkDir, 0755); err != nil {
		t.Fatalf("failed to create benchmark directory: %v", err)
	}

	// 运行基准测试并捕获输出
	// 注意：这个测试主要用于演示如何运行和保存结果
	// 实际使用时应该直接运行: go test -bench=. -benchmem -run=^$ ./pkg/optimizer/
	t.Log("To run benchmarks and save results, execute:")
	t.Log("go test -bench=. -benchmem -run=^$ ./pkg/optimizer/ | tee benchmark/output.txt")
	t.Log("Then manually convert output to baseline.json or use TestParseAndSaveBenchmarkResults")
}

// TestParseAndSaveBenchmarkResults 解析并保存基准测试结果
func TestParseAndSaveBenchmarkResults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark result parsing in short mode")
	}

	// 这里应该传入实际的基准测试输出
	// 示例输出（实际运行时应该从文件或命令输出读取）
	exampleOutput := `BenchmarkSingleTable_Small-8    10000    123456 ns/op    1234 B/op    12 allocs/op
BenchmarkSingleTable_Medium-8   1000     1234567 ns/op   2345 B/op    23 allocs/op
BenchmarkJoin2Table_Inner-8     500      2000000 ns/op   3456 B/op    34 allocs/op`

	// 解析结果
	results := parseBenchmarkOutput(exampleOutput)
	if len(results) == 0 {
		t.Fatal("no benchmark results parsed")
	}

	// 保存结果
	baselinePath := "benchmark/baseline.json"
	if err := saveBenchmarkResults(results, baselinePath); err != nil {
		t.Fatalf("failed to save benchmark results: %v", err)
	}

	t.Logf("Benchmark results saved to %s", baselinePath)
}

