package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/go-mysql-org/go-mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func main() {
	fmt.Println("═══════════════════════════════════════════════════════")
	fmt.Println("      Binlog Slave 客户端 - 简单版                   ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 提示：请使用 Wireshark 抓取 localhost:3306 的数据包")
	fmt.Println("   过滤器: tcp.port == 3306 and mysql")
	fmt.Println()

	// 创建 binlog 同步器
	syncer := replication.NewBinlogSyncer(&replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mariadb",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "",
	})

	fmt.Println("✅ 同步器创建成功\n")

	// 开始同步
	fmt.Println("📍 开始同步 binlog...")
	streamer, err := syncer.StartSync(mysql.Position("", 0))
	if err != nil {
		if netErr, ok := err.(*net.OpError); ok {
			fmt.Printf("❌ 连接失败: %v\n", netErr)
			fmt.Println("\n💡 请确保:")
			fmt.Println("  1. MariaDB 正在运行")
			fmt.Println("  2. binlog 已启用")
			fmt.Println("  3. 有 REPLICATION SLAVE 权限")
			fmt.Println("\n   运行以下 SQL:")
			fmt.Println("   GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'localhost';")
			fmt.Println("   FLUSH PRIVILEGES;")
			return
		}
		log.Fatalf("❌ 同步失败: %v", err)
	}
	defer syncer.Close()

	fmt.Println("✅ 已开始同步\n")

	// 接收 binlog 事件
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("开始接收 Binlog 事件 (最多接收 50 个事件)")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()

	maxEvents := 50
	eventCount := 0

	for eventCount < maxEvents {
		ev, err := streamer.GetEvent(context.Background())
		if err == io.EOF {
			fmt.Println("到达 binlog 末尾")
			break
		}
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("⏱️  等待新的 binlog 事件...")
				time.Sleep(2 * time.Second)
				continue
			}
			log.Printf("❌ 接收事件失败: %v\n", err)
			break
		}

		eventCount++
		hdr := ev.Header

		fmt.Printf("\n【事件 %d】\n", eventCount)
		fmt.Printf("  时间戳: %d\n", hdr.Timestamp)
		fmt.Printf("  事件类型: 0x%02X (%d)\n", hdr.EventType, hdr.EventType)
		fmt.Printf("  服务器ID: %d\n", hdr.ServerID)
		fmt.Printf("  事件大小: %d\n", hdr.EventSize)
		fmt.Printf("  下一个位置: %d\n", hdr.LogPos)

		switch ev.Event.(type) {
		case *replication.FormatDescriptionEvent:
			fmt.Println("  类型: Format Description Event")
		case *replication.RotateEvent:
			fmt.Println("  类型: Rotate Event")
		case *replication.QueryEvent:
			fmt.Println("  类型: Query Event")
		case *replication.XIDEvent:
			fmt.Println("  类型: XID Event")
		case *replication.TableMapEvent:
			fmt.Println("  类型: Table Map Event")
		case *replication.RowsEvent:
			fmt.Println("  类型: Rows Event")
		case *replication.MariadbGTIDEvent:
			fmt.Println("  类型: MariaDB GTID Event")
		case *replication.MariadbGTIDListEvent:
			fmt.Println("  类型: MariaDB GTID List Event")
		default:
			fmt.Printf("  类型: %T\n", ev.Event)
		}

		if eventCount%5 == 0 {
			fmt.Printf("\n  → 已接收 %d 个事件...\n", eventCount)
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("接收完成！总共收到 %d 个 binlog 事件\n", eventCount)
	fmt.Println("═════════════════════════════════════════════════════════")
}
