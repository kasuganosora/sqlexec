package main

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/siddontang/go-mysql/replication"
)

func main() {
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("           Binlog Slave 客户端 - 模拟复制协议             ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("这个程序将:")
	fmt.Println("  1. 连接到 MariaDB 主服务器")
	fmt.Println("  2. 发送 COM_REGISTER_SLAVE 注册为 slave")
	fmt.Println("  3. 发送 COM_BINLOG_DUMP 请求 binlog")
	fmt.Println("  4. 接收并显示 binlog 事件")
	fmt.Println()
	fmt.Println("💡 提示：请使用 Wireshark 抓取 localhost:3306 的数据包")
	fmt.Println("   过滤器: tcp.port == 3306 and mysql")
	fmt.Println()

	// 连接参数
	host := "127.0.0.1"
	port := 3306
	username := "root"
	password := ""

	fmt.Printf("连接配置:\n")
	fmt.Printf("  Host: %s\n", host)
	fmt.Printf("  Port: %d\n", port)
	fmt.Printf("  User: %s\n", username)
	fmt.Printf("\n开始连接...\n\n")

	// 创建 binlog 同步器
	syncer := replication.NewBinlogSyncer(&replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mariadb",
		Host:     host,
		Port:     uint16(port),
		User:     username,
		Password: password,
	})

	// 获取当前 binlog 位置
	fmt.Println("📍 获取当前 binlog 位置...")
	streamer, err := syncer.StartSync(0)
	if err != nil {
		fmt.Printf("❌ 获取位置失败: %v\n", err)
		fmt.Println("\n💡 提示:")
		fmt.Println("  1. 确保 MariaDB 已启用 binlog")
		fmt.Println("  2. 确保有权限访问 binlog (REPLICATION SLAVE)")
		fmt.Println("  3. 检查 MariaDB 是否正在运行")
		fmt.Println("  4. 尝试执行: GRANT REPLICATION SLAVE ON *.* TO 'root'@'localhost';")
		return
	}
	defer streamer.Close()

	fmt.Printf("✅ 已成功连接并开始同步\n\n")

	// 接收 binlog 事件
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("开始接收 Binlog 事件 (最多接收 30 个事件)")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()

	maxEvents := 30
	eventCount := 0

	for eventCount < maxEvents {
		ev, err := streamer.GetEvent(context.Background())
		if err == io.EOF {
			fmt.Println("到达 binlog 末尾")
			break
		}
		if err != nil {
			// 超时错误忽略
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("⏱️  等待新的 binlog 事件...")
				time.Sleep(2 * time.Second)
				continue
			}
			fmt.Printf("❌ 接收事件失败: %v\n", err)
			break
		}

		eventCount++
		ev.Header.Dump(os.Stdout)

		// 显示事件类型
		switch ev.Event.(type) {
		case *replication.FormatDescriptionEvent:
			fmt.Printf("  📋 事件类型: Format Description Event\n")
		case *replication.QueryEvent:
			fmt.Printf("  📝 事件类型: Query Event\n")
			if qev, ok := ev.Event.(*replication.QueryEvent); ok {
				fmt.Printf("  SQL: %s\n", string(qev.Query))
			}
		case *replication.TableMapEvent:
			fmt.Printf("  🗂️  事件类型: Table Map Event\n")
		case *replication.XIDEvent:
			fmt.Printf("  ✅ 事件类型: XID Event (事务提交)\n")
		case *replication.RowsEvent:
			fmt.Printf("  📊 事件类型: Rows Event\n")
			if rev, ok := ev.Event.(*replication.RowsEvent); ok {
				fmt.Printf("     表: %s\n", rev.Table)
				fmt.Printf("     行数: %d\n", len(rev.Rows))
			}
		case *replication.MariadbGTIDEvent:
			fmt.Printf("  🏷️  事件类型: MariaDB GTID Event\n")
		case *replication.MariadbGTIDListEvent:
			fmt.Printf("  📋 事件类型: MariaDB GTID List Event\n")
		default:
			fmt.Printf("  ❓ 事件类型: %T\n", ev.Event)
		}

		fmt.Println()
		time.Sleep(200 * time.Millisecond)

		// 每 5 个事件暂停一下
		if eventCount%5 == 0 {
			fmt.Printf("  已接收 %d 个事件...\n\n", eventCount)
		}
	}

	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Printf("接收完成！总共收到 %d 个 binlog 事件\n", eventCount)
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 现在:")
	fmt.Println("  1. 检查 Wireshark 抓取的包，应该能看到:")
	fmt.Println("     - COM_REGISTER_SLAVE (0x14)")
	fmt.Println("     - COM_BINLOG_DUMP (0x12)")
	fmt.Println("     - 各种 binlog 事件包")
	fmt.Println("  2. 分析这些包的结构")
	fmt.Println("  3. 对比你的协议实现，找出问题所在")
}
