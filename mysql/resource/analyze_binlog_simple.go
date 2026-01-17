package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: analyze_binlog_simple <pcapng_file>")
		fmt.Println("")
		fmt.Println("分析 MySQL Binlog 协议抓包，用于诊断 binlog 相关问题")
		os.Exit(1)
	}

	filename := os.Args[1]
	fmt.Println("══════════════════════════════════════════════════════")
	fmt.Println("  MySQL Binlog 协议抓包分析工具")
	fmt.Println("══════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Printf("解析文件: %s\n\n", filename)

	// 读取整个文件
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("❌ 读取文件失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("搜索 Binlog 相关包...")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	var binlogPacketCount int
	var registerSlavePacketCount int
	var binlogDumpPacketCount int
	var binlogEventCount int

	// 搜索 MySQL 包
	// MySQL 包格式: [length(3 bytes)][seq(1 byte)][command...]
	for i := 0; i < len(data)-4; i++ {
		length := int(data[i]) | int(data[i+1])<<8 | int(data[i+2])<<16
		seqID := data[i+3]

		// 检查长度是否合理
		if length <= 0 || length > 16777215 {
			continue
		}

		// 确保有足够的数据
		if i+4+length > len(data) {
			continue
		}

		// 提取 MySQL 包数据
		mysqlData := data[i : i+4+length]

		// 分析不同类型的包
		command := mysqlData[4]

		switch command {
		case 0x15: // COM_REGISTER_SLAVE
			binlogPacketCount++
			registerSlavePacketCount++
			fmt.Printf("\n══════════════════════════════════════════════════════\n")
			fmt.Printf("📦 找到 COM_REGISTER_SLAVE 包 #%d\n", registerSlavePacketCount)
			fmt.Printf("══════════════════════════════════════════════════════\n")
			fmt.Printf("  包号: #%d\n", binlogPacketCount)
			fmt.Printf("  偏移: %d\n", i)
			fmt.Printf("  序列号: %d\n", seqID)
			fmt.Printf("  MySQL 包长度: %d 字节\n", length)
			fmt.Println()
			printCOMRegisterSlave(mysqlData)
			fmt.Println()
			i += 3 + length
			continue

		case 0x12: // COM_BINLOG_DUMP
			binlogPacketCount++
			binlogDumpPacketCount++
			fmt.Printf("\n══════════════════════════════════════════════════════════\n")
			fmt.Printf("📦 找到 COM_BINLOG_DUMP 包 #%d\n", binlogDumpPacketCount)
			fmt.Printf("══════════════════════════════════════════════════════════\n")
			fmt.Printf("  包号: #%d\n", binlogPacketCount)
			fmt.Printf("  偏移: %d\n", i)
			fmt.Printf("  序列号: %d\n", seqID)
			fmt.Printf("  MySQL 包长度: %d 字节\n", length)
			fmt.Println()
			printCOMBinlogDump(mysqlData)
			fmt.Println()
			i += 3 + length
			continue
		}

		// 检查 OK 包或 EOF 包（binlog 事件）
		if (command == 0x00 || command == 0xFE) && len(mysqlData) >= 20 {
			binlogPacketCount++
			binlogEventCount++
			fmt.Printf("\n══════════════════════════════════════════════════════════\n")
			fmt.Printf("📦 找到 Binlog 事件包 #%d\n", binlogEventCount)
			fmt.Printf("══════════════════════════════════════════════════════════\n")
			fmt.Printf("  包号: #%d\n", binlogPacketCount)
			fmt.Printf("  偏移: %d\n", i)
			fmt.Printf("  序列号: %d\n", seqID)
			fmt.Printf("  MySQL 包长度: %d 字节\n", length)
			fmt.Println()
			printBinlogEvent(mysqlData)
			fmt.Println()
			i += 3 + length
			continue
		}

		// 只显示前 20 个包，避免输出过多
		if binlogPacketCount >= 20 {
			fmt.Println("\n⚠️  已显示 20 个包，停止分析...")
			break
		}
	}

	fmt.Println("\n══════════════════════════════════════════════════════════")
	fmt.Printf("📊 统计信息\n")
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Printf("  总包数: %d\n", binlogPacketCount)
	fmt.Printf("  COM_REGISTER_SLAVE: %d\n", registerSlavePacketCount)
	fmt.Printf("  COM_BINLOG_DUMP: %d\n", binlogDumpPacketCount)
	fmt.Printf("  Binlog 事件: %d\n", binlogEventCount)
	fmt.Println()
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Println("💡 诊断建议")
	fmt.Println("══════════════════════════════════════════════════════════")
	fmt.Println("  1. 对比上面的 Binlog 事件解析结果与实际抓包")
	fmt.Println("  2. 检查代码是否正确跳过 OK 标记字节 (0x00)")
	fmt.Println("  3. 检查代码是否正确处理 EOF 包 (0xFE)")
	fmt.Println("  4. 对比 COM_BINLOG_DUMP 包格式是否正确")
	fmt.Println("  5. 对比 COM_REGISTER_SLAVE 包格式是否正确")
	fmt.Println("════════════════════════════════════════════════════════════")
}

// 打印 COM_REGISTER_SLAVE 包
func printCOMRegisterSlave(data []byte) {
	fmt.Println("📋 COM_REGISTER_SLAVE 包详情:")
	fmt.Println("  ┌─────────────────────────────────────────")
	fmt.Println("  │ Header (4 bytes)")
	fmt.Println("  ├─")
	length := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	fmt.Printf("  │  Packet Length: %d (0x%06X)\n", length, length)
	fmt.Printf("  │  Sequence ID: %d\n", data[3])
	fmt.Println("  ├─")
	fmt.Println("  │ Payload:")
	fmt.Printf("  │  Command: 0x%02X (COM_REGISTER_SLAVE)\n", data[4])

	if len(data) < 9 {
		fmt.Println("  └─ 包太短，无法继续解析")
		return
	}

	pos := 5

	serverID := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4
	fmt.Printf("  │  Server ID: %d\n", serverID)

	if len(data) > pos {
		// 读取 Hostname (NULL 结尾)
		hostname := readNullString(data[pos:])
		pos += len(hostname) + 1
		fmt.Printf("  │  Hostname: '%s' (len=%d)\n", hostname, len(hostname))

		// 读取 Username (NULL 结尾)
		if len(data) > pos {
			username := readNullString(data[pos:])
			pos += len(username) + 1
			fmt.Printf("  │  Username: '%s' (len=%d)\n", username, len(username))

			// 读取 Password (NULL 结尾)
			if len(data) > pos {
				password := readNullString(data[pos:])
				pos += len(password) + 1
				fmt.Printf("  │  Password: '%s' (len=%d)\n", password, len(password))

				// 读取 Port (2 bytes)
				if len(data) >= pos+2 {
					port := binary.LittleEndian.Uint16(data[pos : pos+2])
					pos += 2
					fmt.Printf("  │  Port: %d\n", port)

					// 读取 Replication Rank (4 bytes)
					if len(data) >= pos+4 {
						rank := binary.LittleEndian.Uint32(data[pos : pos+4])
						pos += 4
						fmt.Printf("  │  Replication Rank: %d\n", rank)

						// 读取 Master ID (4 bytes)
						if len(data) >= pos+4 {
							masterID := binary.LittleEndian.Uint32(data[pos : pos+4])
							fmt.Printf("  │  Master ID: %d\n", masterID)
						}
					}
				}
			}
		}
	}

	fmt.Printf("  │  Payload Length: %d\n", len(data)-4)
	fmt.Println("  └─────────────────────────────────────────")
}

// 打印 COM_BINLOG_DUMP 包
func printCOMBinlogDump(data []byte) {
	fmt.Println("📋 COM_BINLOG_DUMP 包详情:")
	fmt.Println("  ┌─────────────────────────────────────────")
	fmt.Println("  │ Header (4 bytes)")
	fmt.Println("  ├─")
	length := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	fmt.Printf("  │  Packet Length: %d (0x%06X)\n", length, length)
	fmt.Printf("  │  Sequence ID: %d\n", data[3])
	fmt.Println("  ├─")
	fmt.Println("  │ Payload:")
	fmt.Printf("  │  Command: 0x%02X (COM_BINLOG_DUMP)\n", data[4])

	if len(data) < 14 {
		fmt.Println("  └─ 包太短，无法继续解析")
		return
	}

	pos := 5

	binlogPos := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4
	fmt.Printf("  │  Binlog Position: %d\n", binlogPos)

	if len(data) >= pos+2 {
		flags := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2
		fmt.Printf("  │  Flags: 0x%04X\n", flags)
		fmt.Printf("  │  ├─ BINLOG_DUMP_NON_BLOCK: %v\n", flags&0x01 != 0)
	}

	if len(data) >= pos+4 {
		serverID := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		fmt.Printf("  │  Server ID: %d\n", serverID)
	}

	if len(data) > pos {
		binlogFilename := readNullString(data[pos:])
		fmt.Printf("  │  Binlog Filename: '%s'\n", binlogFilename)
	}

	fmt.Printf("  │  Payload Length: %d\n", len(data)-4)
	fmt.Println("  └─────────────────────────────────────────")
}

// 打印 Binlog 事件包
func printBinlogEvent(data []byte) {
	fmt.Println("📋 Binlog 事件包详情:")
	fmt.Println("  ┌─────────────────────────────────────────")
	fmt.Println("  │ Header (4 bytes)")
	fmt.Println("  ├─")
	length := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	fmt.Printf("  │  Packet Length: %d (0x%06X)\n", length, length)
	fmt.Printf("  │  Sequence ID: %d\n", data[3])
	fmt.Println("  ├─")
	fmt.Printf("  │  Status: 0x%02X", data[4])

	// 检查是否是 EOF 包
	if data[4] == 0xFE {
		fmt.Println("  │  ⚠️  这是 EOF 包（文件结束）")
		if len(data) == 5 {
			fmt.Println("  │  ⚠️  标准 EOF 包")
		}
		fmt.Println("  └─────────────────────────────────────────")
		return
	}

	// 检查是否是 OK 包
	if data[4] != 0x00 {
		fmt.Printf("  │  ⚠️  意外的状态字节: 0x%02X\n", data[4])
		fmt.Println("  └─────────────────────────────────────────")
		return
	}

	if len(data) < 20 {
		fmt.Println("  └─ 包太短，无法解析 binlog 事件")
		return
	}

	fmt.Println("  │  └─ 这是 Binlog 事件 (OK 标记后跟事件数据)")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────")
	fmt.Println("  │ Binlog Event Header (19 bytes)")
	fmt.Println("  ├─")

	eventData := data[5:] // 跳过 OK 标记

	timestamp := binary.LittleEndian.Uint32(eventData[0:4])
	eventType := eventData[4]
	serverID := binary.LittleEndian.Uint32(eventData[5:9])
	eventSize := binary.LittleEndian.Uint32(eventData[9:13])
	nextPos := binary.LittleEndian.Uint32(eventData[13:17])
	flags := binary.LittleEndian.Uint16(eventData[17:19])

	fmt.Printf("  │  Timestamp: %d (0x%08X)\n", timestamp, timestamp)
	fmt.Printf("  │  Event Type: 0x%02X (%s)\n", eventType, getEventTypeName(eventType))
	fmt.Printf("  │  Server ID: %d\n", serverID)
	fmt.Printf("  │  Event Size: %d\n", eventSize)
	fmt.Printf("  │  Next Position: %d\n", nextPos)
	fmt.Printf("  │  Flags: 0x%04X\n", flags)
	fmt.Println("  ├─")

	// 解析事件体
	if len(eventData) > 19 {
		eventBody := eventData[19:]
		fmt.Printf("  │  Event Body Length: %d\n", len(eventBody))

		// 根据事件类型解析
		switch eventType {
		case 0x04: // ROTATE_EVENT
			fmt.Println("  │  Event Type: ROTATE_EVENT")
			if len(eventBody) >= 8 {
				nextPosition := binary.LittleEndian.Uint64(eventBody[0:8])
				fmt.Printf("  │    Next Position: %d\n", nextPosition)
				if len(eventBody) > 8 {
					filename := readNullString(eventBody[8:])
					fmt.Printf("  │    Filename: '%s'\n", filename)
				}
			}

		case 0x0F: // FORMAT_DESCRIPTION_EVENT
			fmt.Println("  │  Event Type: FORMAT_DESCRIPTION_EVENT")
			if len(eventBody) >= 2 {
				formatVersion := binary.LittleEndian.Uint16(eventBody[0:2])
				fmt.Printf("  │    Format Version: %d\n", formatVersion)

				if len(eventBody) >= 57 {
					serverVersion := string(eventBody[2:52])
					// 去除 NULL 填充
					serverVersion = strings.TrimRight(serverVersion, "\x00")
					fmt.Printf("  │    Server Version: '%s'\n", serverVersion)

					createTimestamp := binary.LittleEndian.Uint32(eventBody[52:56])
					fmt.Printf("  │    Create Timestamp: %d\n", createTimestamp)

					headerLength := eventBody[56]
					fmt.Printf("  │    Header Length: %d\n", headerLength)

					// 事件类型数组长度
					if len(eventBody) >= 58 {
						arrayLen := len(eventBody) - 57 - 5 // 减去固定字段和校验和
						if arrayLen > 0 {
							fmt.Printf("  │    Event Type Array Length: %d\n", arrayLen)
							fmt.Printf("  │    Event Type Array (hex): %x\n", eventBody[57:57+arrayLen])
						}

						if len(eventBody) >= 58+arrayLen {
							checksumAlg := eventBody[57+arrayLen]
							fmt.Printf("  │    Checksum Algorithm: %d\n", checksumAlg)

							if len(eventBody) >= 58+arrayLen+4 && checksumAlg == 0x02 {
								checksum := binary.LittleEndian.Uint32(eventBody[58+arrayLen : 62+arrayLen])
								fmt.Printf("  │    CRC32 Checksum: 0x%08X\n", checksum)
							}
						}
					}
				}
			}

		default:
			fmt.Printf("  │  Event Body (hex): %x\n", eventBody)
		}
	}

	fmt.Println("  └─────────────────────────────────────────")
}

// 获取事件类型名称
func getEventTypeName(eventType uint8) string {
	names := map[uint8]string{
		0x00: "UNKNOWN_EVENT",
		0x01: "START_EVENT_V3",
		0x02: "QUERY_EVENT",
		0x03: "STOP_EVENT",
		0x04: "ROTATE_EVENT",
		0x05: "INTVAR_EVENT",
		0x06: "LOAD_EVENT",
		0x07: "SLAVE_EVENT",
		0x08: "CREATE_FILE_EVENT",
		0x09: "APPEND_BLOCK_EVENT",
		0x0A: "EXEC_LOAD_EVENT",
		0x0B: "DELETE_FILE_EVENT",
		0x0C: "NEW_LOAD_EVENT",
		0x0D: "RAND_EVENT",
		0x0E: "USER_VAR_EVENT",
		0x0F: "FORMAT_DESCRIPTION_EVENT",
		0x10: "XID_EVENT",
		0x11: "BEGIN_LOAD_QUERY_EVENT",
		0x12: "EXECUTE_LOAD_QUERY_EVENT",
		0x13: "TABLE_MAP_EVENT",
		0x14: "WRITE_ROWS_EVENTv0",
		0x15: "UPDATE_ROWS_EVENTv0",
		0x16: "DELETE_ROWS_EVENTv0",
		0x17: "INCIDENT_EVENT",
		0x18: "HEARTBEAT_LOG_EVENT",
		0x19: "IGNORABLE_EVENT",
		0x1A: "ROWS_QUERY_EVENT",
		0x1B: "WRITE_ROWS_EVENTv1",
		0x1C: "UPDATE_ROWS_EVENTv1",
		0x1D: "DELETE_ROWS_EVENTv1",
		0x1E: "BEGIN_LOAD_QUERY_EVENT",
		0x1F: "EXECUTE_LOAD_QUERY_EVENT",
	}

	if name, ok := names[eventType]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_EVENT(0x%02X)", eventType)
}

// 读取 NULL 结尾的字符串
func readNullString(data []byte) string {
	nullPos := 0
	for nullPos < len(data) && data[nullPos] != 0x00 {
		nullPos++
	}
	return string(data[:nullPos])
}

// 读取字节数组
func readBytes(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// 从字节数组读取数字
func readUint32(data []byte, offset int) uint32 {
	return binary.LittleEndian.Uint32(data[offset : offset+4])
}

func readUint16(data []byte, offset int) uint16 {
	return binary.LittleEndian.Uint16(data[offset : offset+2])
}

func readUint8(data []byte, offset int) uint8 {
	return data[offset]
}

// 字符串工具
func indexOfNull(data []byte) int {
	reader := bytes.NewReader(data)
	for i := 0; i < len(data); i++ {
		b, _ := reader.ReadByte()
		if b == 0x00 {
			return i
		}
	}
	return len(data)
}
