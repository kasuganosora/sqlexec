package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func main() {
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("      Binlog Slave 客户端 - 使用 go-mysql 库               ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("这个程序将:")
	fmt.Println("  1. 使用 go-mysql 库连接 MariaDB")
	fmt.Println("  2. 发送 COM_REGISTER_SLAVE 注册为 slave")
	fmt.Println("  3. 发送 COM_BINLOG_DUMP 请求 binlog")
	fmt.Println("  4. 接收并解析 binlog 事件")
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

	fmt.Println("✅ 同步器创建成功\n")

	// 尝试获取当前 binlog 位置
	fmt.Println("📍 获取当前 binlog 位置...")
	streamer, err := syncer.StartSync(mysql.Position("", 0))
	if err != nil {
		if netErr, ok := err.(*net.OpError); ok {
			fmt.Printf("❌ 连接失败: %v\n", netErr)
			fmt.Println("\n💡 提示:")
			fmt.Println("  1. 确保 MariaDB 正在运行")
			fmt.Println("  2. 确保 binlog 已启用 (SHOW VARIABLES LIKE 'log_bin')")
			fmt.Println("  3. 确保有 REPLICATION SLAVE 权限:")
			fmt.Println("     GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'localhost';")
			fmt.Println("     FLUSH PRIVILEGES;")
			return
		}
		log.Fatalf("❌ 获取 binlog 位置失败: %v", err)
	}
	defer syncer.Close()

	fmt.Println("✅ 已成功连接并开始同步\n")

	// 接收 binlog 事件
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("开始接收 Binlog 事件 (最多接收 100 个事件)")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()

	maxEvents := 100
	eventCount := 0

	for eventCount < maxEvents {
		// 读取事件
		ev, err := streamer.GetEvent(context.Background())
		if err == io.EOF {
			fmt.Println("到达 binlog 末尾")
			break
		}
		if err != nil {
			// 超时错误
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("⏱️  等待新的 binlog 事件...")
				time.Sleep(2 * time.Second)
				continue
			}
			log.Printf("❌ 接收事件失败: %v\n", err)
			break
		}

		eventCount++

		// 显示事件头信息
		hdr := ev.Header
		fmt.Printf("\n【事件 %d】\n", eventCount)
		fmt.Printf("  时间戳: %d\n", hdr.Timestamp)
		fmt.Printf("  事件类型: 0x%02X (%d)\n", hdr.EventType, hdr.EventType)
		fmt.Printf("  服务器ID: %d\n", hdr.ServerID)
		fmt.Printf("  事件大小: %d\n", hdr.EventSize)
		fmt.Printf("  下一个位置: %d\n", hdr.LogPos)

		// 显示事件类型和详细信息
		switch ev.Event.(type) {
		case *replication.FormatDescriptionEvent:
			fmt.Println("  事件类型: Format Description Event (格式描述事件)")

		case *replication.RotateEvent:
			fmt.Println("  事件类型: Rotate Event (轮转事件)")

		case *replication.QueryEvent:
			fmt.Println("  事件类型: Query Event (查询事件)")
			if qe, ok := ev.Event.(*replication.QueryEvent); ok {
				fmt.Printf("    数据库: %s\n", qe.Schema)
				fmt.Printf("    查询: %s\n", string(qe.Query))
			}

		case *replication.XIDEvent:
			fmt.Println("  事件类型: XID Event (事务提交事件)")

		case *replication.TableMapEvent:
			fmt.Println("  事件类型: Table Map Event (表映射事件)")
			if tme, ok := ev.Event.(*replication.TableMapEvent); ok {
				fmt.Printf("    数据库: %s\n", tme.Schema)
				fmt.Printf("    表名: %s\n", tme.Table)
				fmt.Printf("    表ID: %d\n", tme.TableID)
			}

		case *replication.RowsEvent:
			fmt.Println("  事件类型: Rows Event (行事件)")
			if re, ok := ev.Event.(*replication.RowsEvent); ok {
				fmt.Printf("    表ID: %d\n", re.TableID)
				fmt.Printf("    行数: %d\n", len(re.Rows))
			}

		case *replication.MariadbGTIDEvent:
			fmt.Println("  事件类型: MariaDB GTID Event")

		case *replication.MariadbGTIDListEvent:
			fmt.Println("  事件类型: MariaDB GTID List Event")

		default:
			fmt.Printf("  事件类型: %T\n", ev.Event)
		}

		// 每 5 个事件暂停一下
		if eventCount%5 == 0 {
			fmt.Printf("\n  → 已接收 %d 个事件...\n", eventCount)
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("接收完成！总共收到 %d 个 binlog 事件\n", eventCount)
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 现在:")
	fmt.Println("  1. 检查 Wireshark 抓取的包")
	fmt.Println("  2. 查看完整的协议交互过程")
	fmt.Println("  3. 分析 COM_REGISTER_SLAVE 和 COM_BINLOG_DUMP 包")
	fmt.Println("  4. 对比你的 binlog 协议实现")
}

