package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"time"

	"mysql-proxy/mysql/protocol"
)

func main() {
	fmt.Println("═══════════════════════════════════════════════════")
	fmt.Println("   Binlog Slave 客户端 - 使用项目 MySQL 协议实现         ")
	fmt.Println("═════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("这个程序将:")
	fmt.Println("  1. 使用项目的 MySQL 协议实现连接 MariaDB")
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

	fmt.Printf("连接参数:\n")
	fmt.Printf("  主机: %s:%d\n", host, port)
	fmt.Printf("  用户名: %s\n", username)
	fmt.Printf("\n开始连接...\n\n")

	// 建立连接
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("❌ 连接失败: %v", err)
	}
	defer conn.Close()

	// 设置读取超时
	conn.SetReadDeadline(time.Time{})

	fmt.Println("✅ TCP 连接成功\n")

	// 读取握手包
	fmt.Println("【步骤 1: 读取握手包】")
	handshake := &protocol.HandshakeResponse{}
	err = handshake.Unmarshal(conn, 0xffffffff)
	if err != nil {
		log.Fatalf("❌ 读取握手失败: %v", err)
	}
	printPacket("收到握手", handshake.Packet)

	// 发送认证包
	fmt.Println("\n【步骤 2: 发送认证包】")
	capabilities := protocol.CLIENT_PROTOCOL_41 | protocol.CLIENT_SECURE_CONNECTION | protocol.CLIENT_PLUGIN_AUTH

	auth := &protocol.HandshakeResponse{
		ClientCapabilities:         uint16(capabilities),
		ExtendedClientCapabilities: uint16(capabilities >> 16),
		MaxPacketSize:              16777215,
		CharacterSet:               33,
		Reserved:                   make([]byte, 19),
		MariaDBCaps:                0,
		User:                       username,
		AuthResponse:               "", // 空密码
		ClientAuthPluginName:       "mysql_native_password",
	}
	auth.Packet.SequenceID = 1

	authData, err := auth.Marshal()
	if err != nil {
		log.Fatalf("❌ 序列化认证包失败: %v", err)
	}

	printAndSend("发送认证包", conn, auth.Packet.SequenceID, authData)

	// 读取认证响应
	fmt.Println("\n【步骤 3: 读取认证响应】")
	authResp := make([]byte, 4)
	_, err = conn.Read(authResp)
	if err != nil {
		log.Fatalf("❌ 读取认证响应失败: %v", err)
	}

	length := uint32(authResp[0]) | uint32(authResp[1])<<8 | uint32(authResp[2])<<16
	sequence := authResp[3]

	payload := make([]byte, length)
	_, err = conn.Read(payload)
	if err != nil {
		log.Fatalf("❌ 读取 payload 失败: %v", err)
	}

	authRespPacket := protocol.Packet{
		PayloadLength: uint32(length),
		SequenceID:    sequence,
		Payload:       payload,
	}
	printPacket("收到认证响应", authRespPacket)

	if payload[0] == 0x00 {
		fmt.Println("✅ 认证成功\n")
	} else if payload[0] == 0xFF {
		errCode := uint16(payload[1]) | uint16(payload[2])<<8
		errMsg := string(payload[4:])
		log.Fatalf("❌ 认证失败 (错误码 %d): %s\n", errCode, errMsg)
		return
	}

	// 发送 COM_REGISTER_SLAVE
	fmt.Println("【步骤 4: 发送 COM_REGISTER_SLAVE (0x14)】")
	registerSlavePacket := &protocol.ComRegisterSlavePacket{
		Command:         protocol.COM_REGISTER_SLAVE,
		ServerID:        100,
		Host:            "",
		User:            "",
		Password:        "",
		Port:            0,
		ReplicationRank: 0,
		MasterID:        0,
	}
	registerSlavePacket.Packet.SequenceID = 0

	registerSlaveData, err := registerSlavePacket.Marshal()
	if err != nil {
		log.Fatalf("❌ 序列化 COM_REGISTER_SLAVE 失败: %v", err)
	}

	printAndSend("发送 COM_REGISTER_SLAVE", conn, registerSlavePacket.Packet.SequenceID, registerSlaveData)

	// 读取 COM_REGISTER_SLAVE 响应
	fmt.Println("\n【步骤 5: 读取 COM_REGISTER_SLAVE 响应】")
	resp := make([]byte, 4)
	_, err = conn.Read(resp)
	if err != nil {
		log.Fatalf("❌ 读取响应失败: %v", err)
	}

	length = uint32(resp[0]) | uint32(resp[1])<<8 | uint32(resp[2])<<16
	sequence = resp[3]

	payload = make([]byte, length)
	_, err = conn.Read(payload)
	if err != nil {
		log.Fatalf("❌ 读取 payload 失败: %v", err)
	}

	respPacket := protocol.Packet{
		PayloadLength: uint32(length),
		SequenceID:    sequence,
		Payload:       payload,
	}
	printPacket("收到 COM_REGISTER_SLAVE 响应", respPacket)

	if payload[0] == 0x00 {
		fmt.Println("✅ COM_REGISTER_SLAVE 成功\n")
	} else if payload[0] == 0xFF {
		errCode := uint16(payload[1]) | uint16(payload[2])<<8
		errMsg := string(payload[4:])
		log.Fatalf("❌ COM_REGISTER_SLAVE 失败 (错误码 %d): %s\n", errCode, errMsg)
		return
	}

	// 查询 master status 获取 binlog 文件名和位置
	fmt.Println("【步骤 6: 查询 Master Status】")
	showMasterStatusPacket := buildQueryPacket("SHOW MASTER STATUS")
	showMasterStatusPacket.Packet.SequenceID = 0

	showMasterStatusData, err := showMasterStatusPacket.Marshal()
	if err != nil {
		log.Fatalf("❌ 序列化 SHOW MASTER STATUS 失败: %v", err)
	}

	printAndSend("发送 SHOW MASTER STATUS", conn, showMasterStatusPacket.Packet.SequenceID, showMasterStatusData)

	// 读取 SHOW MASTER STATUS 响应
	fmt.Println("读取 SHOW MASTER STATUS 响应...\n")

	// 读取并消费所有 SHOW MASTER STATUS 的响应包
	for {
		// 读取包头部
		header := make([]byte, 4)
		_, err := conn.Read(header)
		if err != nil {
			log.Fatalf("❌ 读取响应头失败: %v", err)
		}

		pktLen := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
		pktSeq := header[3]

		// 读取 payload
		payload := make([]byte, pktLen)
		_, err = conn.Read(payload)
		if err != nil {
			log.Fatalf("❌ 读取 payload 失败: %v", err)
		}

		// 检查是否是 EOF 包
		if payload[0] == 0xFE && pktLen <= 5 {
			fmt.Printf("  ✅ 收到 EOF 包 (序列号: %d)\n\n", pktSeq)
			break
		}
	}

	// 直接使用已知的值（从 SHOW MASTER STATUS 获取）
	binlogFile := "mariadb-bin.000002"
	binlogPos := uint32(4) // 从位置 4 开始，这样可以跳过伪 ROTATE 事件

	fmt.Printf("  ✅ 使用 binlog 文件: %s @ 位置: %d\n\n", binlogFile, binlogPos)

	// 发送 COM_BINLOG_DUMP
	binlogDumpPacket := &protocol.ComBinlogDumpPacket{
		Command:        protocol.COM_BINLOG_DUMP,
		BinlogPos:      binlogPos,
		Flags:          0x01, // 非阻塞模式
		ServerID:       100,
		BinlogFilename: binlogFile,
	}
	binlogDumpPacket.Packet.SequenceID = 0

	binlogDumpData, err := binlogDumpPacket.Marshal()
	if err != nil {
		log.Fatalf("❌ 序列化 COM_BINLOG_DUMP 失败: %v", err)
	}

	printAndSend("发送 COM_BINLOG_DUMP", conn, binlogDumpPacket.Packet.SequenceID, binlogDumpData)

	// 读取 binlog 事件
	fmt.Println("\n【步骤 7: 开始接收 Binlog 事件】")
	fmt.Println("═══════════════════════════════════════════════════")

	eventCount := 0
	maxEvents := 100
	buffer := make([]byte, 0) // 缓冲区，用于收集分包的数据

	for eventCount < maxEvents {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// 读取包头部
		header := make([]byte, 4)
		_, err := conn.Read(header)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("⏱️  30秒内没有新的 binlog 事件")
				break
			}
			log.Printf("❌ 读取 binlog 事件头部失败: %v\n", err)
			break
		}

		length := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
		sequence := header[3]

		payload := make([]byte, length)
		_, err = conn.Read(payload)
		if err != nil {
			log.Printf("❌ 读取 binlog 事件 payload 失败: %v\n", err)
			break
		}

		// 将数据添加到缓冲区
		buffer = append(buffer, payload...)

		eventCount++
		fmt.Printf("\n【事件 %d】\n", eventCount)
		fmt.Printf("  包长度: %d 字节\n", length)
		fmt.Printf("  序列号: %d\n", sequence)

		// 检查是否是 EOF 包
		if len(payload) > 0 && payload[0] == 0xFE {
			fmt.Println("  类型: EOF 包（服务器发送完毕）")
			if length <= 5 { // 标准 EOF 包长度
				fmt.Println("  ✅ Binlog 传输结束")
				break
			}
			continue
		}

		// 显示 payload 的前 50 字节（hex）
		if len(payload) > 0 {
			fmt.Printf("  Payload (hex, 前%d字节): %s\n", min(len(payload), 50), hex.EncodeToString(payload[:min(len(payload), 50)]))
		}

		// 如果包太短（少于 19 字节事件头），尝试缓冲
		if len(payload) < 19 {
			fmt.Printf("  ⚠️  包太短，缓冲等待...\n")
			// 检查缓冲区是否足够
			if len(buffer) >= 19 {
				fmt.Printf("  ✅ 缓冲区已收集 %d 字节，尝试解析\n", len(buffer))
				parseBinlogEvent(buffer)
				buffer = nil // 清空缓冲区
			}
			continue
		}

		// 分析 binlog 事件
		if len(payload) >= 4 {
			// 检查是否是 MariaDB ROTATE 事件的简化格式
			// 格式：[文件名长度][文件名][下一个位置]
			filenameLen := int(payload[0])
			if len(payload) >= 1+filenameLen+4 {
				// 可能是 ROTATE 事件格式
				filename := string(payload[1 : 1+filenameLen])
				nextPosOffset := 1 + filenameLen
				if len(payload) >= nextPosOffset+4 {
					// 尝试两种字节序：小端序和大端序
					nextPosLittle := uint32(payload[nextPosOffset]) | uint32(payload[nextPosOffset+1])<<8 |
						uint32(payload[nextPosOffset+2])<<16 | uint32(payload[nextPosOffset+3])<<24
					nextPosBig := uint32(payload[nextPosOffset])<<24 | uint32(payload[nextPosOffset+1])<<16 |
						uint32(payload[nextPosOffset+2])<<8 | uint32(payload[nextPosOffset+3])

					// 选择合理的值（通常小于 16MB）
					var nextPos uint32
					if nextPosLittle < 16*1024*1024 {
						nextPos = nextPosLittle
					} else if nextPosBig < 16*1024*1024 {
						nextPos = nextPosBig
					} else {
						nextPos = nextPosLittle // 默认使用小端序
					}

					fmt.Printf("  ✅ MariaDB ROTATE 事件（简化格式）\n")
					fmt.Printf("  事件类型: 0x04 (ROTATE_EVENT)\n")
					fmt.Printf("  文件名: %s\n", filename)
					fmt.Printf("  下一个位置: %d (0x%08X)\n", nextPos, nextPos)
					fmt.Printf("  位置字段(hex): %02X %02X %02X %02X\n",
						payload[nextPosOffset], payload[nextPosOffset+1],
						payload[nextPosOffset+2], payload[nextPosOffset+3])
					continue
				}
			}

			// 检查是否是标准 MySQL 格式（以 0x00 开头）
			if payload[0] == 0x00 && len(payload) >= 20 {
				eventData := payload[1:] // 跳过 OK 标记

				timestamp := uint32(eventData[0]) | uint32(eventData[1])<<8 | uint32(eventData[2])<<16 | uint32(eventData[3])<<24
				eventType := eventData[4]
				serverID := uint32(eventData[5]) | uint32(eventData[6])<<8 | uint32(eventData[7])<<16 | uint32(eventData[8])<<24
				eventSize := uint32(eventData[9]) | uint32(eventData[10])<<8 | uint32(eventData[11])<<16 | uint32(eventData[12])<<24
				nextPos := uint32(eventData[13]) | uint32(eventData[14])<<8 | uint32(eventData[15])<<16 | uint32(eventData[16])<<24
				flags := uint16(eventData[17]) | uint16(eventData[18])<<8

				fmt.Printf("  ✅ 标准 MySQL 格式 binlog 事件\n")
				fmt.Printf("  事件类型: 0x%02X\n", eventType)
				fmt.Printf("  时间戳: %d\n", timestamp)
				fmt.Printf("  服务器ID: %d\n", serverID)
				fmt.Printf("  事件大小: %d\n", eventSize)
				fmt.Printf("  下一个位置: %d\n", nextPos)
				fmt.Printf("  标志位: 0x%04X\n", flags)

				var eventName string
				switch eventType {
				case 0x00:
					eventName = "UNKNOWN_EVENT"
				case 0x01:
					eventName = "START_EVENT_V3"
				case 0x02:
					eventName = "QUERY_EVENT"
				case 0x03:
					eventName = "STOP_EVENT"
				case 0x04:
					eventName = "ROTATE_EVENT"
				case 0x0F:
					eventName = "FORMAT_DESCRIPTION_EVENT"
				case 0x10:
					eventName = "XID_EVENT"
				case 0x13:
					eventName = "TABLE_MAP_EVENT"
				case 0x19:
					eventName = "WRITE_ROWS_EVENTv1"
				case 0x1A:
					eventName = "UPDATE_ROWS_EVENTv1"
				case 0x1B:
					eventName = "DELETE_ROWS_EVENTv1"
				case 0x1D:
					eventName = "WRITE_ROWS_EVENTv2"
				case 0x1E:
					eventName = "UPDATE_ROWS_EVENTv2"
				case 0x1F:
					eventName = "DELETE_ROWS_EVENTv2"
				default:
					eventName = "其他事件"
				}
				fmt.Printf("  事件名称: %s\n", eventName)

				if len(eventData) > 18 {
					fmt.Printf("  Payload (hex, 前100字节): %s\n", hex.EncodeToString(eventData[:min(100, len(eventData))]))
				}
			} else if len(payload) >= 19 {
				// MariaDB 原始格式：直接是 binlog 事件头
				fmt.Printf("  ✅ MariaDB 原始 binlog 事件格式\n")

				timestamp := uint32(payload[0]) | uint32(payload[1])<<8 | uint32(payload[2])<<16 | uint32(payload[3])<<24
				eventType := payload[4]
				serverID := uint32(payload[5]) | uint32(payload[6])<<8 | uint32(payload[7])<<16 | uint32(payload[8])<<24
				eventSize := uint32(payload[9]) | uint32(payload[10])<<8 | uint32(payload[11])<<16 | uint32(payload[12])<<24
				nextPos := uint32(payload[13]) | uint32(payload[14])<<8 | uint32(payload[15])<<16 | uint32(payload[16])<<24
				flags := uint16(payload[17]) | uint16(payload[18])<<8

				fmt.Printf("  事件类型: 0x%02X\n", eventType)
				fmt.Printf("  时间戳: %d\n", timestamp)
				fmt.Printf("  服务器ID: %d\n", serverID)
				fmt.Printf("  事件大小: %d\n", eventSize)
				fmt.Printf("  下一个位置: %d\n", nextPos)
				fmt.Printf("  标志位: 0x%04X\n", flags)

				var eventName string
				switch eventType {
				case 0x00:
					eventName = "UNKNOWN_EVENT"
				case 0x01:
					eventName = "START_EVENT_V3"
				case 0x02:
					eventName = "QUERY_EVENT"
				case 0x03:
					eventName = "STOP_EVENT"
				case 0x04:
					eventName = "ROTATE_EVENT"
				case 0x0F:
					eventName = "FORMAT_DESCRIPTION_EVENT"
				case 0x10:
					eventName = "XID_EVENT"
				case 0x13:
					eventName = "TABLE_MAP_EVENT"
				default:
					eventName = "其他事件"
				}
				fmt.Printf("  事件名称: %s\n", eventName)

				if len(payload) > 19 {
					fmt.Printf("  Payload (hex, 前100字节): %s\n", hex.EncodeToString(payload[:min(100, len(payload))]))
				}
			} else {
				fmt.Printf("  ⚠️  Payload 长度不足，无法解析\n")
			}
		}

		if eventCount%5 == 0 {
			fmt.Printf("\n  → 已接收 %d 个事件...\n", eventCount)
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n═════════════════════════════════════════════════")
	fmt.Printf("接收完成！总共收到 %d 个 binlog 事件\n", eventCount)
	fmt.Println("═══════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 现在可以在 Wireshark 中:")
	fmt.Println("  1. 查看完整的协议交互过程")
	fmt.Println("  2. 分析每个包的字节内容")
	fmt.Println("  3. 对比你的代码实现")
	fmt.Println("  4. 找出 binlog 协议实现的问题")
}

// 构建查询包
func buildQueryPacket(query string) *protocol.ComQueryPacket {
	return &protocol.ComQueryPacket{
		Command: protocol.COM_QUERY,
		Query:   query,
	}
}

func printAndSend(description string, conn net.Conn, seqID uint8, data []byte) {
	fmt.Printf("  %s\n", description)
	fmt.Printf("    SequenceID: %d\n", seqID)
	fmt.Printf("    数据 (hex): %s\n", hex.EncodeToString(data))
	fmt.Printf("    长度: %d 字节\n", len(data))

	_, err := conn.Write(data)
	if err != nil {
		log.Printf("    ❌ 发送失败: %v\n", err)
	} else {
		fmt.Printf("    ✅ 发送成功\n")
	}
}

func printPacket(description string, pkt protocol.Packet) {
	fmt.Printf("  %s\n", description)
	fmt.Printf("    SequenceID: %d\n", pkt.SequenceID)
	fmt.Printf("    PayloadLength: %d\n", pkt.PayloadLength)
	if len(pkt.Payload) > 0 {
		fmt.Printf("    Payload (hex): %s\n", hex.EncodeToString(pkt.Payload))
		fmt.Printf("    Payload (前50字节): %x\n", pkt.Payload[:min(50, len(pkt.Payload))])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 解析 binlog 事件
func parseBinlogEvent(data []byte) {
	if len(data) < 19 {
		fmt.Printf("  ⚠️  数据太短，无法解析 binlog 事件头\n")
		return
	}

	timestamp := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	eventType := data[4]
	serverID := uint32(data[5]) | uint32(data[6])<<8 | uint32(data[7])<<16 | uint32(data[8])<<24
	eventSize := uint32(data[9]) | uint32(data[10])<<8 | uint32(data[11])<<16 | uint32(data[12])<<24
	nextPos := uint32(data[13]) | uint32(data[14])<<8 | uint32(data[15])<<16 | uint32(data[16])<<24
	flags := uint16(data[17]) | uint16(data[18])<<8

	fmt.Printf("  事件类型: 0x%02X\n", eventType)
	fmt.Printf("  时间戳: %d\n", timestamp)
	fmt.Printf("  服务器ID: %d\n", serverID)
	fmt.Printf("  事件大小: %d\n", eventSize)
	fmt.Printf("  下一个位置: %d\n", nextPos)
	fmt.Printf("  标志位: 0x%04X\n", flags)

	var eventName string
	switch eventType {
	case 0x00:
		eventName = "UNKNOWN_EVENT"
	case 0x01:
		eventName = "START_EVENT_V3"
	case 0x02:
		eventName = "QUERY_EVENT"
	case 0x03:
		eventName = "STOP_EVENT"
	case 0x04:
		eventName = "ROTATE_EVENT"
	case 0x0F:
		eventName = "FORMAT_DESCRIPTION_EVENT"
	case 0x10:
		eventName = "XID_EVENT"
	case 0x13:
		eventName = "TABLE_MAP_EVENT"
	case 0x19:
		eventName = "WRITE_ROWS_EVENTv1"
	case 0x1A:
		eventName = "UPDATE_ROWS_EVENTv1"
	case 0x1B:
		eventName = "DELETE_ROWS_EVENTv1"
	case 0x1D:
		eventName = "WRITE_ROWS_EVENTv2"
	case 0x1E:
		eventName = "UPDATE_ROWS_EVENTv2"
	case 0x1F:
		eventName = "DELETE_ROWS_EVENTv2"
	default:
		eventName = "其他事件"
	}
	fmt.Printf("  事件名称: %s\n", eventName)

	if len(data) > 19 {
		fmt.Printf("  Payload (hex, 前100字节): %s\n", hex.EncodeToString(data[:min(100, len(data))]))
	}

	// 如果是 Rotate Event，显示文件名
	if eventType == 0x04 && len(data) > 27 {
		nextPosition := uint64(data[19]) | uint64(data[20])<<8 | uint64(data[21])<<16 | uint64(data[22])<<24 |
			uint64(data[23])<<32 | uint64(data[24])<<40 | uint64(data[25])<<48 | uint64(data[26])<<56
		filename := string(data[27:])
		fmt.Printf("  ✅ Rotate Event:\n")
		fmt.Printf("    下一个位置: %d\n", nextPosition)
		fmt.Printf("    文件名: %s\n", filename)
	}
}
