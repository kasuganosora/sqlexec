package resource

import (
	"context"
	"fmt"

	"github.com/kasuganosora/sqlexec/pkg/resource/csv"
	"github.com/kasuganosora/sqlexec/pkg/resource/json"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
)

// Example demonstrating the capabilities interfaces

func ExampleCapabilityInterfaces() {
	fmt.Println("=== Capability Interfaces Example ===\n")

	// 创建Manager
	mgr := NewDataSourceManager()
	ctx := context.Background()

	// 创建CSV数据源（默认只读）
	csvConfig := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "csv-example",
	}
	csvDS := csv.NewCSVAdapter(csvConfig, "/tmp/test.csv")

	// 创建JSON数据源（默认只读）
	jsonConfig := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeJSON,
		Name: "json-example",
	}
	jsonDS := json.NewJSONAdapter(jsonConfig, "/tmp/test.json")

	// 创建可写的CSV数据源
	writableCSVConfig := &domain.DataSourceConfig{
		Type: domain.DataSourceTypeCSV,
		Name: "csv-writable",
		Options: map[string]interface{}{
			"writable": true,
		},
	}
	writableCSVDS := csv.NewCSVAdapter(writableCSVConfig, "/tmp/test-writable.csv")

	// 创建纯内存数据源（支持MVCC和写操作）
	memoryConfig := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "memory-example",
		Writable: true,
	}
	memoryDS := memory.NewMVCCDataSource(memoryConfig)

	// 注册到Manager
	mgr.Register("csv", csvDS)
	mgr.Register("json", jsonDS)
	mgr.Register("writable-csv", writableCSVDS)
	mgr.Register("memory", memoryDS)

	// ==================== 测试写能力 ====================
	fmt.Println("1. Testing Write Capability:")

	// 检查CSV是否可写（使用Manager的IsWritable方法）
	if writable, err := mgr.IsWritable("csv"); err == nil {
		fmt.Printf("   CSV writable: %v\n", writable)
	}

	// 检查可写CSV是否可写
	if writable, err := mgr.IsWritable("writable-csv"); err == nil {
		fmt.Printf("   Writable CSV writable: %v\n", writable)
	}

	// 检查JSON是否可写
	if writable, err := mgr.IsWritable("json"); err == nil {
		fmt.Printf("   JSON writable: %v\n", writable)
	}

	// 检查Memory是否可写
	if writable, err := mgr.IsWritable("memory"); err == nil {
		fmt.Printf("   Memory writable: %v\n", writable)
	}

	// ==================== 测试MVCC能力 ====================
	fmt.Println("\n2. Testing MVCC Capability:")

	// 检查CSV是否支持MVCC
	if supportsMVCC, err := mgr.SupportsMVCC("csv"); err == nil {
		fmt.Printf("   CSV supports MVCC: %v\n", supportsMVCC)
	}

	// 检查JSON是否支持MVCC
	if supportsMVCC, err := mgr.SupportsMVCC("json"); err == nil {
		fmt.Printf("   JSON supports MVCC: %v\n", supportsMVCC)
	}

	// 检查Memory是否支持MVCC
	if supportsMVCC, err := mgr.SupportsMVCC("memory"); err == nil {
		fmt.Printf("   Memory supports MVCC: %v\n", supportsMVCC)
	}

	// ==================== 批量查询能力 ====================
	fmt.Println("\n3. Querying Writable Sources:")

	// 获取所有可写的数据源
	if writableSources, err := mgr.GetWritableSources(ctx); err == nil {
		fmt.Printf("   Writable sources: %v\n", writableSources)
	}

	// 获取所有支持MVCC的数据源
	if mvccSources, err := mgr.GetMVCCSources(ctx); err == nil {
		fmt.Printf("   MVCC-capable sources: %v\n", mvccSources)
	}

	// ==================== 获取单个数据源的能力 ====================
	fmt.Println("\n4. Getting Individual Source Capabilities:")

	for _, name := range []string{"csv", "writable-csv", "memory"} {
		if writable, mvcc, err := mgr.GetSourceCapabilities(ctx, name); err == nil {
			fmt.Printf("   %s: writable=%v, mvcc=%v\n", name, writable, mvcc)
		}
	}

	// ==================== 使用MVCC事务 ====================
	fmt.Println("\n5. Using MVCC Transactions (Memory Source Only):")

	// 只有支持MVCC的数据源可以使用事务
	if supportsMVCC, err := mgr.SupportsMVCC("memory"); err == nil && supportsMVCC {
		// 开始只读事务
		if txnID, err := mgr.BeginTx(ctx, "memory", true); err == nil {
			fmt.Printf("   Started read-only transaction: ID=%d\n", txnID)
			
			// 提交事务
			if err := mgr.CommitTx(ctx, "memory", txnID); err == nil {
				fmt.Printf("   Committed transaction %d\n", txnID)
			}
		}

		// 开始读写事务
		if txnID, err := mgr.BeginTx(ctx, "memory", false); err == nil {
			fmt.Printf("   Started read-write transaction: ID=%d\n", txnID)
			
			// 回滚事务
			if err := mgr.RollbackTx(ctx, "memory", txnID); err == nil {
				fmt.Printf("   Rolled back transaction %d\n", txnID)
			}
		}
	}

	// ==================== 关键要点 ====================
	fmt.Println("\n6. Key Points:")
	fmt.Println("   • CSV/JSON/Parquet默认是只读的")
	fmt.Println("   • Memory数据源默认支持写和MVCC")
	fmt.Println("   • 使用IsWritable()方法检查写能力")
	fmt.Println("   • 使用SupportsMVCC()方法检查MVCC能力")
	fmt.Println("   • 使用GetWritableSources()获取所有可写源")
	fmt.Println("   • 使用GetMVCCSources()获取所有支持MVCC的源")
	fmt.Println("   • 使用BeginTx/CommitTx/RollbackTx进行事务操作（仅支持MVCC的源）")
	fmt.Println("   • 所有接口都支持优雅降级：未实现接口时返回默认值")
	fmt.Println("   • 底层是MVCCDataSource，所以所有Adapters都默认支持MVCC")
	fmt.Println("   • 只读数据源可以通过writable=true配置变为可写（如果适配器支持）")

	fmt.Println("\n=== Example Complete ===")
}
