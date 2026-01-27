package memory

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Benchmark_Optimizer_PointQuery 点查询性能对比（有索引 vs 无索引）
func Benchmark_Optimizer_PointQuery(b *testing.B) {
	rowCounts := []int{10000, 100000, 1000000}

	for _, rowCount := range rowCounts {
		// 准备测试数据
		ds := NewMVCCDataSource(&domain.DataSourceConfig{
			Type:     domain.DataSourceTypeMemory,
			Name:     "memory",
			Writable: true,
		})
		_ = ds.Connect(context.Background())

		schema := &domain.TableInfo{
			Name:   "users",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "string", Nullable: false},
				{Name: "email", Type: "string", Nullable: true},
				{Name: "age", Type: "int64", Nullable: true},
			},
		}

		rows := make([]domain.Row, rowCount)
		for i := 0; i < rowCount; i++ {
			rows[i] = domain.Row{
				"id":    int64(i + 1),
				"name":  fmt.Sprintf("user_%d", i+1),
				"email": fmt.Sprintf("user_%d@example.com", i+1),
				"age":   int64(20 + rand.Intn(60)),
			}
		}
		_ = ds.LoadTable("users", schema, rows)

		ctx := context.Background()

		// 无索引查询
		b.Run(fmt.Sprintf("%d_NoIndex", rowCount), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Query(ctx, "users", &domain.QueryOptions{
					Filters: []domain.Filter{
						{Field: "id", Operator: "=", Value: int64(i%rowCount + 1)},
					},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// 创建索引
		_, _ = ds.indexManager.CreateIndex("users", "id", IndexTypeHash, true)

		// 有索引查询
		b.Run(fmt.Sprintf("%d_WithIndex", rowCount), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Query(ctx, "users", &domain.QueryOptions{
					Filters: []domain.Filter{
						{Field: "id", Operator: "=", Value: int64(i%rowCount + 1)},
					},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Optimizer_RangeQuery 范围查询性能对比
func Benchmark_Optimizer_RangeQuery(b *testing.B) {
	rowCounts := []int{10000, 100000, 1000000}

	for _, rowCount := range rowCounts {
		// 准备测试数据
		ds := NewMVCCDataSource(&domain.DataSourceConfig{
			Type:     domain.DataSourceTypeMemory,
			Name:     "memory",
			Writable: true,
		})
		_ = ds.Connect(context.Background())

		schema := &domain.TableInfo{
			Name:   "users",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "string", Nullable: false},
				{Name: "age", Type: "int64", Nullable: true},
			},
		}

		rows := make([]domain.Row, rowCount)
		for i := 0; i < rowCount; i++ {
			rows[i] = domain.Row{
				"id":   int64(i + 1),
				"name": fmt.Sprintf("user_%d", i+1),
				"age":  int64(20 + rand.Intn(60)),
			}
		}
		_ = ds.LoadTable("users", schema, rows)

		ctx := context.Background()

		// 无索引范围查询
		b.Run(fmt.Sprintf("%d_NoIndex", rowCount), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Query(ctx, "users", &domain.QueryOptions{
					Filters: []domain.Filter{
						{Field: "age", Operator: ">=", Value: int64(30)},
						{Field: "age", Operator: "<=", Value: int64(50)},
					},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// 创建B-Tree索引
		_, _ = ds.indexManager.CreateIndex("users", "age", IndexTypeBTree, false)

		// 有索引范围查询（注意：当前实现可能还是全表扫描，因为优化器还没有支持范围查询）
		b.Run(fmt.Sprintf("%d_WithIndex", rowCount), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Query(ctx, "users", &domain.QueryOptions{
					Filters: []domain.Filter{
						{Field: "age", Operator: ">=", Value: int64(30)},
						{Field: "age", Operator: "<=", Value: int64(50)},
					},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Optimizer_InsertPerformance 插入性能（有索引 vs 无索引）
func Benchmark_Optimizer_InsertPerformance(b *testing.B) {
	rowCounts := []int{1000, 5000, 10000}

	for _, batchSize := range rowCounts {
		// 无索引插入
		b.Run(fmt.Sprintf("%d_NoIndex", batchSize), func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := &domain.TableInfo{
				Name:   "users",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false},
					{Name: "name", Type: "string", Nullable: false},
				},
			}
			_ = ds.LoadTable("users", schema, []domain.Row{})

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				insertRows := make([]domain.Row, batchSize)
				for j := 0; j < batchSize; j++ {
					insertRows[j] = domain.Row{
						"id":   int64(i*batchSize + j + 1),
						"name": fmt.Sprintf("user_%d", i*batchSize+j+1),
					}
				}
				_, err := ds.Insert(ctx, "users", insertRows, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// 有索引插入
		b.Run(fmt.Sprintf("%d_WithIndex", batchSize), func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := &domain.TableInfo{
				Name:   "users",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false},
					{Name: "name", Type: "string", Nullable: false},
				},
			}
			_ = ds.LoadTable("users", schema, []domain.Row{})
			_, _ = ds.indexManager.CreateIndex("users", "id", IndexTypeHash, true)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				insertRows := make([]domain.Row, batchSize)
				for j := 0; j < batchSize; j++ {
					insertRows[j] = domain.Row{
						"id":   int64(i*batchSize + j + 1),
						"name": fmt.Sprintf("user_%d", i*batchSize+j+1),
					}
				}
				_, err := ds.Insert(ctx, "users", insertRows, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Optimizer_UpdatePerformance 更新性能（行级COW vs 旧版本）
func Benchmark_Optimizer_UpdatePerformance(b *testing.B) {
	rowCounts := []int{1000, 5000, 10000}

	for _, rowCount := range rowCounts {
		ds := NewMVCCDataSource(&domain.DataSourceConfig{
			Type:     domain.DataSourceTypeMemory,
			Name:     "memory",
			Writable: true,
		})
		_ = ds.Connect(context.Background())

		schema := &domain.TableInfo{
			Name:   "users",
			Schema: "test",
			Columns: []domain.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "string", Nullable: false},
				{Name: "age", Type: "int64", Nullable: true},
			},
		}

		rows := make([]domain.Row, rowCount)
		for i := 0; i < rowCount; i++ {
			rows[i] = domain.Row{
				"id":   int64(i + 1),
				"name": fmt.Sprintf("user_%d", i+1),
				"age":  int64(20 + rand.Intn(60)),
			}
		}
		_ = ds.LoadTable("users", schema, rows)

		ctx := context.Background()

		// 更新10%的行
		updateCount := rowCount / 10

		b.Run(fmt.Sprintf("%d_Update_10_Percent", rowCount), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < updateCount; j++ {
					rowID := (j * rowCount / updateCount) % rowCount
					_, err := ds.Update(ctx, "users", []domain.Filter{
						{Field: "id", Operator: "=", Value: int64(rowID + 1)},
					}, domain.Row{
						"age": int64(rand.Intn(60) + 20),
					}, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// Benchmark_Optimizer_ConcurrentQuery 并发查询性能
func Benchmark_Optimizer_ConcurrentQuery(b *testing.B) {
	rowCounts := []int{10000, 100000}

	for _, rowCount := range rowCounts {
		// 无索引并发查询
		b.Run(fmt.Sprintf("%d_NoIndex", rowCount), func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := &domain.TableInfo{
				Name:   "users",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false},
					{Name: "name", Type: "string", Nullable: false},
				},
			}

			rows := make([]domain.Row, rowCount)
			for i := 0; i < rowCount; i++ {
				rows[i] = domain.Row{
					"id":   int64(i + 1),
					"name": fmt.Sprintf("user_%d", i+1),
				}
			}
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					i++
					_, err := ds.Query(ctx, "users", &domain.QueryOptions{
						Filters: []domain.Filter{
							{Field: "id", Operator: "=", Value: int64(i%rowCount + 1)},
						},
					})
					if err != nil {
						b.Error(err)
					}
				}
			})
		})

		// 有索引并发查询
		b.Run(fmt.Sprintf("%d_WithIndex", rowCount), func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := &domain.TableInfo{
				Name:   "users",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false},
					{Name: "name", Type: "string", Nullable: false},
				},
			}

			rows := make([]domain.Row, rowCount)
			for i := 0; i < rowCount; i++ {
				rows[i] = domain.Row{
					"id":   int64(i + 1),
					"name": fmt.Sprintf("user_%d", i+1),
				}
			}
			_ = ds.LoadTable("users", schema, rows)
			_, _ = ds.indexManager.CreateIndex("users", "id", IndexTypeHash, true)

			ctx := context.Background()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					i++
					_, err := ds.Query(ctx, "users", &domain.QueryOptions{
						Filters: []domain.Filter{
							{Field: "id", Operator: "=", Value: int64(i%rowCount + 1)},
						},
					})
					if err != nil {
						b.Error(err)
					}
				}
			})
		})
	}
}
