package memory

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// 生成测试数据
func generateTestData(rowCount int) []domain.Row {
	rand.Seed(time.Now().UnixNano())

	rows := make([]domain.Row, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = domain.Row{
			"id":       int64(i + 1),
			"name":     fmt.Sprintf("user_%d", i+1),
			"email":    fmt.Sprintf("user_%d@example.com", i+1),
			"age":      int64(20 + rand.Intn(60)),
			"balance":  int64(rand.Intn(100000)),
			"status":   rand.Intn(3),
			"created":  time.Now().Add(-time.Duration(rand.Intn(365*24)) * time.Hour).Unix(),
		}
	}
	return rows
}

// 创建测试表结构
func createTestSchema() *domain.TableInfo {
	return &domain.TableInfo{
		Name:   "users",
		Schema: "test",
		Columns: []domain.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: false},
			{Name: "email", Type: "string", Nullable: true},
			{Name: "age", Type: "int64", Nullable: true},
			{Name: "balance", Type: "int64", Nullable: true},
			{Name: "status", Type: "int64", Nullable: true},
			{Name: "created", Type: "int64", Nullable: true},
		},
	}
}

// 性能测试用例配置
type benchConfig struct {
	name        string
	rowCount    int
	iterations  int // 迭代次数，计算平均值
}

var benchCases = []benchConfig{
	{name: "10K", rowCount: 10000, iterations: 1},
	{name: "50K", rowCount: 50000, iterations: 1},
	{name: "100K", rowCount: 100000, iterations: 1},
	{name: "500K", rowCount: 500000, iterations: 1},
	{name: "1M", rowCount: 1000000, iterations: 1},
	{name: "5M", rowCount: 5000000, iterations: 1},
}

// Benchmark_Query_All 查询所有数据
func Benchmark_Query_All(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Query(ctx, "users", &domain.QueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Query_WithFilter 使用简单条件过滤
func Benchmark_Query_WithFilter(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

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

// Benchmark_Query_WithLimit 分页查询
func Benchmark_Query_WithLimit(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Query(ctx, "users", &domain.QueryOptions{
					Limit:  100,
					Offset: i * 100 % tc.rowCount,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Insert 批量插入
func Benchmark_Insert(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			_ = ds.LoadTable("users", schema, []domain.Row{})

			ctx := context.Background()
			insertRows := make([]domain.Row, 1000) // 每次插入1000行
			for i := range insertRows {
				insertRows[i] = domain.Row{
					"id":      int64(i),
					"name":    fmt.Sprintf("user_%d", i),
					"balance": int64(rand.Intn(1000)),
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Insert(ctx, "users", insertRows, &domain.InsertOptions{})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Update 更新操作
func Benchmark_Update(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Update(ctx, "users", []domain.Filter{
					{Field: "id", Operator: "=", Value: int64(i%tc.rowCount + 1)},
				}, domain.Row{"balance": int64(rand.Intn(100000))}, &domain.UpdateOptions{})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Delete 删除操作
func Benchmark_Delete(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Delete(ctx, "users", []domain.Filter{
					{Field: "id", Operator: "=", Value: int64(i%tc.rowCount + 1)},
				}, &domain.DeleteOptions{})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Transaction_Commit 事务提交
func Benchmark_Transaction_Commit(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txnID, err := ds.BeginTx(ctx, false)
				if err != nil {
					b.Fatal(err)
				}

				// 在事务中执行一些操作
				txnCtx := SetTransactionID(ctx, txnID)
				_, _ = ds.Update(txnCtx, "users", []domain.Filter{
					{Field: "id", Operator: "=", Value: int64(i%tc.rowCount + 1)},
				}, domain.Row{"balance": int64(rand.Intn(100000))}, &domain.UpdateOptions{})

				err = ds.CommitTx(ctx, txnID)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Transaction_Query 事务内查询
func Benchmark_Transaction_Query(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()
			txnID, err := ds.BeginTx(ctx, false)
			if err != nil {
				b.Fatal(err)
			}
			txnCtx := SetTransactionID(ctx, txnID)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ds.Query(txnCtx, "users", &domain.QueryOptions{
					Limit:  100,
					Offset: i * 100 % tc.rowCount,
				})
				if err != nil {
					b.Fatal(err)
				}
			}

			_ = ds.RollbackTx(ctx, txnID)
		})
	}
}

// Benchmark_Transaction_WithMultipleOperations 事务内多次操作
func Benchmark_Transaction_WithMultipleOperations(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txnID, err := ds.BeginTx(ctx, false)
				if err != nil {
					b.Fatal(err)
				}

				txnCtx := SetTransactionID(ctx, txnID)

				// 事务内执行10次更新
				for j := 0; j < 10; j++ {
					_, _ = ds.Update(txnCtx, "users", []domain.Filter{
						{Field: "id", Operator: "=", Value: int64((i*10+j)%tc.rowCount + 1)},
					}, domain.Row{"balance": int64(rand.Intn(100000))}, &domain.UpdateOptions{})
				}

				// 执行一次查询
				_, _ = ds.Query(txnCtx, "users", &domain.QueryOptions{Limit: 10})

				err = ds.CommitTx(ctx, txnID)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_BeginTx_RollbackTx 事务回滚
func Benchmark_BeginTx_RollbackTx(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txnID, err := ds.BeginTx(ctx, false)
				if err != nil {
					b.Fatal(err)
				}

				// 在事务中执行一些操作
				txnCtx := SetTransactionID(ctx, txnID)
				_, _ = ds.Update(txnCtx, "users", []domain.Filter{
					{Field: "id", Operator: "=", Value: int64(i%tc.rowCount + 1)},
				}, domain.Row{"balance": int64(rand.Intn(100000))}, &domain.UpdateOptions{})

				err = ds.RollbackTx(ctx, txnID)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_ReadOnlyTransaction_Commit 只读事务提交（COW优势展示）
func Benchmark_ReadOnlyTransaction_Commit(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txnID, err := ds.BeginTx(ctx, true) // 只读事务
				if err != nil {
					b.Fatal(err)
				}

				// 只读事务只查询，不修改
				txnCtx := SetTransactionID(ctx, txnID)
				_, _ = ds.Query(txnCtx, "users", &domain.QueryOptions{Limit: 10})

				err = ds.CommitTx(ctx, txnID)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_ReadOnlyTransaction_Rollback 只读事务回滚（COW优势展示）
func Benchmark_ReadOnlyTransaction_Rollback(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txnID, err := ds.BeginTx(ctx, true) // 只读事务
				if err != nil {
					b.Fatal(err)
				}

				// 只读事务只查询，不修改
				txnCtx := SetTransactionID(ctx, txnID)
				_, _ = ds.Query(txnCtx, "users", &domain.QueryOptions{Limit: 10})

				err = ds.RollbackTx(ctx, txnID)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Transaction_SingleUpdate 只更新一行的事务（COW部分优势）
func Benchmark_Transaction_SingleUpdate(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txnID, err := ds.BeginTx(ctx, false)
				if err != nil {
					b.Fatal(err)
				}

				// 只更新一行
				txnCtx := SetTransactionID(ctx, txnID)
				_, _ = ds.Update(txnCtx, "users", []domain.Filter{
					{Field: "id", Operator: "=", Value: int64(1)},
				}, domain.Row{"balance": int64(999999)}, &domain.UpdateOptions{})

				err = ds.CommitTx(ctx, txnID)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Concurrent_ReadWrite 并发读写
func Benchmark_Concurrent_ReadWrite(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if i%2 == 0 {
						// 读
						_, _ = ds.Query(ctx, "users", &domain.QueryOptions{Limit: 100})
					} else {
						// 写
						_, _ = ds.Update(ctx, "users", []domain.Filter{
							{Field: "id", Operator: "=", Value: int64(i%tc.rowCount + 1)},
						}, domain.Row{"balance": int64(rand.Intn(100000))}, &domain.UpdateOptions{})
					}
					i++
				}
			})
		})
	}
}

// Benchmark_Concurrent_Transactions 并发事务
func Benchmark_Concurrent_Transactions(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			ds := NewMVCCDataSource(&domain.DataSourceConfig{
				Type:     domain.DataSourceTypeMemory,
				Name:     "memory",
				Writable: true,
			})
			_ = ds.Connect(context.Background())

			schema := createTestSchema()
			rows := generateTestData(tc.rowCount)
			_ = ds.LoadTable("users", schema, rows)

			ctx := context.Background()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					txnID, _ := ds.BeginTx(ctx, false)
					txnCtx := SetTransactionID(ctx, txnID)

					_, _ = ds.Update(txnCtx, "users", []domain.Filter{
						{Field: "id", Operator: "=", Value: int64(i%tc.rowCount + 1)},
					}, domain.Row{"balance": int64(rand.Intn(100000))}, &domain.UpdateOptions{})

					_ = ds.CommitTx(ctx, txnID)
					i++
				}
			})
		})
	}
}
