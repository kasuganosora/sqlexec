package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

func main() {
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("     简单 Binlog Slave 客户端 - 使用项目自己的协议实现 ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("这个程序将:")
	fmt.Println("  1. 连接到 MariaDB 并执行握手认证")
	fmt.Println("  2. 发送 COM_REGISTER_SLAVE (0x14)")
	fmt.Println("  3. 发送 COM_BINLOG_DUMP (0x12)")
	fmt.Println("  4. 接收 binlog 事件包")
	fmt.Println()
	fmt.Println("💡 提示：请使用 Wireshark 抓取 localhost:3306 的数据包")
	fmt.Println("   过滤器: tcp.port == 3306 and mysql")
	fmt.Println()

	// 连接参数
	host := "127.0.0.1:3306"
	username := "root"
	password := ""

	fmt.Printf("正在连接到 %s ...\n", host)

	// 建立 TCP 连接
	conn, err := net.Dial("tcp", host)
	if err != nil {
		fmt.Printf("❌ 连接失败: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("✅ TCP 连接成功\n")

	// 读取握手包
	fmt.Println("📨 读取握手包...")
	handshake, err := readPacket(conn)
	if err != nil {
		fmt.Printf("❌ 读取握手包失败: %v\n", err)
		return
	}
	fmt.Printf("✅ 握手包长度: %d 字节\n", len(handshake.Payload))
	fmt.Printf("   服务器版本: %s\n", extractServerVersion(handshake.Payload))

	// 发送认证包
	fmt.Println("\n📤 发送认证包...")
	authPacket, err := buildAuthPacket(username, password, handshake.Payload)
	if err != nil {
		fmt.Printf("❌ 构建认证包失败: %v\n", err)
		return
	}

	err = writePacket(conn, authPacket)
	if err != nil {
		fmt.Printf("❌ 发送认证包失败: %v\n", err)
		return
	}

	// 读取认证响应
	fmt.Println("📨 读取认证响应...")
	resp, err := readPacket(conn)
	if err != nil {
		fmt.Printf("❌ 读取认证响应失败: %v\n", err)
		return
	}

	if resp.Payload[0] == 0x00 {
		fmt.Println("✅ 认证成功\n")
	} else if resp.Payload[0] == 0xFF {
		errCode := binary.LittleEndian.Uint16(resp.Payload[1:3])
		errMsg := string(resp.Payload[4:])
		fmt.Printf("❌ 认证失败 (错误码 %d): %s\n\n", errCode, errMsg)
		return
	} else {
		fmt.Printf("⚠️  未知响应: %02X\n\n", resp.Payload[0])
		return
	}

	// 发送 COM_REGISTER_SLAVE
	fmt.Println("📤 发送 COM_REGISTER_SLAVE (0x14)...")
	registerSlavePacket := buildRegisterSlavePacket(100)
	err = writePacket(conn, registerSlavePacket)
	if err != nil {
		fmt.Printf("❌ 发送 COM_REGISTER_SLAVE 失败: %v\n", err)
		return
	}

	// 读取响应
	resp, err = readPacket(conn)
	if err != nil {
		fmt.Printf("❌ 读取 COM_REGISTER_SLAVE 响应失败: %v\n", err)
		return
	}
	if resp.Payload[0] == 0x00 {
		fmt.Println("✅ COM_REGISTER_SLAVE 成功")
	} else if resp.Payload[0] == 0xFF {
		errCode := binary.LittleEndian.Uint16(resp.Payload[1:3])
		errMsg := string(resp.Payload[4:])
		fmt.Printf("❌ COM_REGISTER_SLAVE 失败 (错误码 %d): %s\n", errCode, errMsg)
		return
	}

	// 发送 COM_BINLOG_DUMP
	fmt.Println("\n📤 发送 COM_BINLOG_DUMP (0x12)...")
	binlogDumpPacket := buildBinlogDumpPacket(4, "mariadb-bin.000001", 100)
	err = writePacket(conn, binlogDumpPacket)
	if err != nil {
		fmt.Printf("❌ 发送 COM_BINLOG_DUMP 失败: %v\n", err)
		return
	}

	// 读取 binlog 事件
	fmt.Println("\n📨 开始接收 binlog 事件...")
	fmt.Println("═════════════════════════════════════════════════════════")

	eventCount := 0
	maxEvents := 50

	for eventCount < maxEvents {
		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))

		pkt, err := readPacket(conn)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("⏱️  10秒内没有新的 binlog 事件")
				break
			}
			fmt.Printf("❌ 读取 binlog 事件失败: %v\n", err)
			break
		}

		eventCount++
		fmt.Printf("\n【事件 %d】\n", eventCount)
		fmt.Printf("  包长度: %d 字节\n", pkt.Length)
		fmt.Printf("  序列号: %d\n", pkt.Sequence)

		// 分析事件类型
		if len(pkt.Payload) >= 19 {
			eventType := pkt.Payload[4]
			timestamp := binary.LittleEndian.Uint32(pkt.Payload[0:4])
			serverID := binary.LittleEndian.Uint32(pkt.Payload[5:9])
			eventSize := binary.LittleEndian.Uint32(pkt.Payload[9:13])
			nextPos := binary.LittleEndian.Uint32(pkt.Payload[13:17])
			flags := binary.LittleEndian.Uint16(pkt.Payload[17:19])

			fmt.Printf("  事件类型: 0x%02X\n", eventType)
			fmt.Printf("  时间戳: %d\n", timestamp)
			fmt.Printf("  服务器ID: %d\n", serverID)
			fmt.Printf("  事件大小: %d\n", eventSize)
			fmt.Printf("  下一个位置: %d\n", nextPos)
			fmt.Printf("  标志位: 0x%04X\n", flags)

			// 显示事件名称
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
		}

		if eventCount%5 == 0 {
			fmt.Printf("\n  → 已接收 %d 个事件...\n", eventCount)
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("接收完成！总共收到 %d 个 binlog 事件\n", eventCount)
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 现在可以在 Wireshark 中:")
	fmt.Println("  1. 查看完整的协议交互过程")
	fmt.Println("  2. 分析每个包的字节内容")
	fmt.Println("  3. 对比你的代码实现")
	fmt.Println("  4. 找出 binlog 协议实现的问题")
}

// Packet 结构
type Packet struct {
	Length   uint32
	Sequence uint8
	Payload  []byte
}

// 读取数据包
func readPacket(conn net.Conn) (*Packet, error) {
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	// 读取包长度 (3字节) 和序列号 (1字节)
	header := make([]byte, 4)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	length := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
	sequence := header[3]

	// 读取 payload
	payload := make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return nil, err
	}

	return &Packet{
		Length:   length,
		Sequence: sequence,
		Payload:  payload,
	}, nil
}

// 写入数据包
func writePacket(conn net.Conn, payload []byte) error {
	length := len(payload)
	header := []byte{
		byte(length),
		byte(length >> 8),
		byte(length >> 16),
	}

	_, err := conn.Write(header)
	if err != nil {
		return err
	}

	_, err = conn.Write(payload)
	return err
}

// 提取服务器版本
func extractServerVersion(payload []byte) string {
	// 握手包的第二个字段是协议版本
	// 第三个字段是服务器版本
	if len(payload) < 6 {
		return "Unknown"
	}

	reader := bufio.NewReader(bytes.NewReader(payload[1:]))
	version, _ := reader.ReadString(0x00)
	if len(version) > 0 && version[len(version)-1] == 0 {
		version = version[:len(version)-1]
	}
	return version
}

// 构建认证包
func buildAuthPacket(username, password string, handshakePayload []byte) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	reader := bufio.NewReader(bytes.NewReader(handshakePayload))

	// 跳过协议版本
	_, _ = reader.ReadByte()
	// 跳过服务器版本
	_, _ = reader.ReadString(0x00)
	// 读取连接 ID
	_ = make([]byte, 4)
	reader.Discard(4)
	// 读取 auth-plugin-data (第一部分)
	authData1, _ := reader.ReadBytes(0x00)
	if len(authData1) > 0 {
		authData1 = authData1[:len(authData1)-1]
	}
	// 跳过填充字节
	reader.Discard(1)
	// 读取服务器能力标志 (低16位)
	serverCapLow, _ := reader.ReadByte()
	serverCapLow2, _ := reader.ReadByte()
	_ = uint16(serverCapLow) | uint16(serverCapLow2)<<8
	// 跳过字符集
	reader.Discard(1)
	// 跳过服务器状态
	reader.Discard(2)
	// 跳过服务器能力标志 (高16位)
	reader.Discard(2)
	// 跳过盐长度
	reader.Discard(1)
	// 跳过保留字节 (10个)
	reader.Discard(10)
	// 读取 auth-plugin-data (第二部分)
	authData2, _ := reader.ReadBytes(0x00)
	if len(authData2) > 0 {
		authData2 = authData2[:len(authData2)-1]
	}

	// 组合完整盐值
	_ = append(authData1, authData2...)

	// 客户端能力标志
	clientCap := uint32(0x000085a6) // CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_LONG_FLAG
	if len(authData2) > 0 {
		clientCap |= 0x80000000 // CLIENT_PLUGIN_AUTH
	}

	binary.Write(buf, binary.LittleEndian, clientCap)
	binary.Write(buf, binary.LittleEndian, clientCap>>16) // 扩展标志
	binary.Write(buf, binary.LittleEndian, uint32(0x21ffffff)) // 最大包大小
	binary.Write(buf, binary.LittleEndian, uint8(33)) // 字符集 utf8mb4
	// 保留字节 (23个)
	for i := 0; i < 23; i++ {
		buf.WriteByte(0x00)
	}

	// 用户名
	buf.WriteString(username)
	buf.WriteByte(0x00)

	// 认证响应
	if len(password) == 0 {
		// 空密码
		buf.WriteByte(0x00)
	} else {
		// 简化：使用空认证响应（仅用于无密码连接）
		authResp := make([]byte, 0)
		buf.WriteByte(byte(len(authResp)))
		buf.Write(authResp)
	}

	// 数据库名 (可选)
	buf.WriteByte(0x00)

	// 认证插件名 (如果支持 CLIENT_PLUGIN_AUTH)
	if clientCap&0x80000000 != 0 {
		buf.WriteString("mysql_native_password")
		buf.WriteByte(0x00)
	}

	return buf.Bytes(), nil
}

// 构建 COM_REGISTER_SLAVE 包
func buildRegisterSlavePacket(serverID uint32) []byte {
	buf := bytes.NewBuffer(nil)

	// 命令字节
	buf.WriteByte(0x14) // COM_REGISTER_SLAVE

	// Server ID
	binary.Write(buf, binary.LittleEndian, serverID)

	// Hostname (空)
	buf.WriteByte(0x00)

	// User (空)
	buf.WriteByte(0x00)

	// Password (空)
	buf.WriteByte(0x00)

	// Port (0)
	binary.Write(buf, binary.LittleEndian, uint16(0))

	// Rank (0)
	binary.Write(buf, binary.LittleEndian, uint32(0))

	// Master ID (0)
	binary.Write(buf, binary.LittleEndian, uint32(0))

	return buf.Bytes()
}

// 构建 COM_BINLOG_DUMP 包
func buildBinlogDumpPacket(pos uint32, filename string, serverID uint32) []byte {
	buf := bytes.NewBuffer(nil)

	// 命令字节
	buf.WriteByte(0x12) // COM_BINLOG_DUMP

	// Binlog Pos
	binary.Write(buf, binary.LittleEndian, pos)

	// Flags (0)
	binary.Write(buf, binary.LittleEndian, uint16(0))

	// Server ID
	binary.Write(buf, binary.LittleEndian, serverID)

	// Binlog Filename
	buf.WriteString(filename)
	buf.WriteByte(0x00)

	return buf.Bytes()
}
