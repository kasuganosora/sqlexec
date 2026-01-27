package memory

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Benchmark_Query_WithIndex 使用索引的查询
func Benchmark_Query_WithIndex(b *testing.B) {
	// 测试数据量
	testSizes := []struct {
		name string
		rows int
	}{
		{"10K", 10000},
		{"50K", 50000},
		{"100K", 100000},
		{"500K", 500000},
		{"1M", 1000000},
	}

	for _, tc := range testSizes {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			// 创建表和索引
			schema := &domain.TableInfo{
				Name: "users",
				Schema: "test",
				Columns: []domain.ColumnInfo{
					{Name: "id", Type: "int64", Nullable: false},
					{Name: "name", Type: "string", Nullable: true},
				},
			}

			// 生成测试数据
			rows := make([]domain.Row, tc.rows)
			for i := 0; i < tc.rows; i++ {
				rows[i] = domain.Row{
					"id":   int64(i + 1),
					"name": fmt.Sprintf("user_%d", i+1),
				}
			}

			_ = ds.LoadTable("users", schema, rows)

			// 创建索引
			_, _ = ds.indexManager.CreateIndex("users", "id", IndexTypeHash, true)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 使用索引查询
				_, err := ds.Query(ctx, "users", &domain.QueryOptions{
					Filters: []domain.Filter{
						{Field: "id", Operator: "=", Value: int64(i%tc.rows + 1)},
					},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Query_WithIndex_vs_NoIndex 对比有索引和无索引
func Benchmark_Query_WithIndex_vs_NoIndex(b *testing.B) {
	rowCount := 100000

	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "memory",
		Writable: true,
	})
	_ = ds.Connect(context.Background())

	// 创建表
	schema := &domain.TableInfo{
		Name: "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "balance", Type: "int64", Nullable: true},
		},
	}

	// 生成测试数据
	rows := make([]domain.Row, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = domain.Row{
			"id":      int64(i + 1),
			"name":    fmt.Sprintf("user_%d", i+1),
			"balance": int64(rand.Intn(10000)),
		}
	}

	_ = ds.LoadTable("users", schema, rows)

	ctx := context.Background()

	// 无索引查询
	b.Run("NoIndex", func(b *testing.B) {
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
	b.Run("WithIndex", func(b *testing.B) {
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

// Benchmark_RangeQuery 范围查询对比
func Benchmark_RangeQuery(b *testing.B) {
	rowCount := 100000

	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "memory",
		Writable: true,
	})
	_ = ds.Connect(context.Background())

	// 创建表
	schema := &domain.TableInfo{
		Name: "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
		},
	}

	// 生成测试数据
	rows := make([]domain.Row, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = domain.Row{
			"id":   int64(i + 1),
			"name": fmt.Sprintf("user_%d", i+1),
		}
	}

	_ = ds.LoadTable("users", schema, rows)

	ctx := context.Background()

	// 无索引范围查询
	b.Run("NoIndex", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := int64(i * 10)
			end := int64(start + 100)
			_, err := ds.Query(ctx, "users", &domain.QueryOptions{
				Filters: []domain.Filter{
					{Field: "id", Operator: ">=", Value: start},
					{Field: "id", Operator: "<=", Value: end},
				},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// 创建B-Tree索引
	_, _ = ds.indexManager.CreateIndex("users", "id", IndexTypeBTree, false)

	// 有索引范围查询
	b.Run("WithBTreeIndex", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := int64(i * 10)
			end := int64(start + 100)
			_, err := ds.Query(ctx, "users", &domain.QueryOptions{
				Filters: []domain.Filter{
					{Field: "id", Operator: ">=", Value: start},
					{Field: "id", Operator: "<=", Value: end},
				},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark_Index_Insert 索引插入性能
func Benchmark_Index_Insert(b *testing.B) {
	ds := NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "memory",
		Writable: true,
	})
	_ = ds.Connect(context.Background())

	schema := &domain.TableInfo{
		Name: "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
		},
	}

	_ = ds.LoadTable("users", schema, []domain.Row{})

	// 创建索引
	_, _ = ds.indexManager.CreateIndex("users", "id", IndexTypeHash, true)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := domain.Row{
			"id":   int64(i + 1),
			"name": fmt.Sprintf("user_%d", i+1),
		}
		_, err := ds.Insert(ctx, "users", []domain.Row{row}, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
