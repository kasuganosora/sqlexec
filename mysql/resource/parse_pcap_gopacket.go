package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: parse_pcap_gopacket <pcapng_file>")
		os.Exit(1)
	}

	filename := os.Args[1]
	fmt.Println("解析文件:", filename)

	// 打开 pcapng 文件
	handle, err := pcap.OpenOffline(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	fmt.Println("\n搜索 MySQL COM_STMT_EXECUTE 包...")
	fmt.Println("查找命令字节 0x17 (COM_STMT_EXECUTE)...")
	fmt.Println()

	found := 0
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())

	for packet := range packetSource.Packets() {
		// 遍历所有层
		for _, layer := range packet.Layers() {
			// 检查 TCP 层
			if layer.LayerType() == layers.LayerTypeTCP {
				tcp, _ := layer.(*layers.TCP)
				payload := tcp.LayerPayload()

				if len(payload) > 0 {
					// 在 TCP 载荷中搜索 MySQL 包
					mysqlPackets := findMySQLPackets(payload)
					for _, mysqlPkt := range mysqlPackets {
						found++
						fmt.Printf("╔══════════════════════════════════════════════════════════╗\n")
						fmt.Printf("║         找到 COM_STMT_EXECUTE 包 #%d                          ║\n", found)
						fmt.Printf("╚══════════════════════════════════════════════════════════╝\n\n")

						// 打印完整的包信息
						printPacketDetails(mysqlPkt, found)

						// 只显示前 3 个包
						if found >= 3 {
							break
						}
					}
				}
			}
		}

		if found >= 3 {
			break
		}
	}

	fmt.Printf("\n╔══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    总共找到 %d 个包                          ║\n", found)
	fmt.Printf("╚══════════════════════════════════════════════════════════╝\n")
}

// findMySQLPackets 在数据中查找所有 MySQL 包
func findMySQLPackets(data []byte) [][]byte {
	var packets [][]byte

	i := 0
	for i < len(data)-4 {
		// MySQL 包格式: [length(3 bytes)][seq(1 byte)][payload...]
		length := int(data[i]) | int(data[i+1])<<8 | int(data[i+2])<<16

		if length > 0 && length < 100000 && i+4+length <= len(data) {
			// 检查是否是 COM_STMT_EXECUTE (0x17)
			if data[i+3] == 0x17 {
				packetData := data[i : i+4+length]
				packets = append(packets, packetData)
			}

			i += 4 + length
		} else {
			i++
		}
	}

	return packets
}

// printPacketDetails 详细打印包信息
func printPacketDetails(data []byte, index int) {
	if len(data) < 4 {
		fmt.Println("包太短，无法解析")
		return
	}

	// 解析包头
	packetLength := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
	seqID := data[3]

	fmt.Printf("【包头信息】\n")
	fmt.Printf("  长度: %d 字节\n", packetLength)
	fmt.Printf("  Sequence ID: %d\n", seqID)
	fmt.Printf("  包头 HEX: %02x %02x %02x %02x\n\n", data[0], data[1], data[2], data[3])

	// 解析载荷
	payload := data[4:]
	if len(payload) < 10 {
		fmt.Println("载荷太短")
		return
	}

	command := payload[0]
	fmt.Printf("【载荷信息】\n")
	fmt.Printf("  Command: 0x%02x ", command)

	// 命令名称映射
	cmdNames := map[uint8]string{
		0x01: "COM_QUIT",
		0x02: "COM_INIT_DB",
		0x03: "COM_QUERY",
		0x04: "COM_FIELD_LIST",
		0x05: "COM_CREATE_DB",
		0x06: "COM_DROP_DB",
		0x07: "COM_REFRESH",
		0x08: "COM_SHUTDOWN",
		0x09: "COM_STATISTICS",
		0x0a: "COM_PROCESS_INFO",
		0x0b: "COM_CONNECT",
		0x0c: "COM_PROCESS_KILL",
		0x0d: "COM_DEBUG",
		0x0e: "COM_PING",
		0x0f: "COM_TIME",
		0x10: "COM_DELAYED_INSERT",
		0x11: "COM_CHANGE_USER",
		0x12: "COM_BINLOG_DUMP",
		0x13: "COM_TABLE_DUMP",
		0x14: "COM_CONNECT_OUT",
		0x15: "COM_REGISTER_SLAVE",
		0x16: "COM_STMT_PREPARE",
		0x17: "COM_STMT_EXECUTE",
		0x18: "COM_STMT_SEND_LONG_DATA",
		0x19: "COM_STMT_CLOSE",
		0x1a: "COM_STMT_RESET",
		0x1b: "COM_SET_OPTION",
		0x1c: "COM_STMT_FETCH",
	}

	if cmdName, ok := cmdNames[command]; ok {
		fmt.Printf("(%s)", cmdName)
	}
	fmt.Printf("\n")

	// 如果是 COM_STMT_EXECUTE，详细解析
	if command == 0x17 && len(payload) >= 11 {
		parseCOMStmtExecute(payload)
	}

	fmt.Printf("\n【完整 HEX dump】\n")
	fmt.Printf("%s\n", formatHex(data, 16))
}

// parseCOMStmtExecute 解析 COM_STMT_EXECUTE 包
func parseCOMStmtExecute(payload []byte) {
	fmt.Printf("\n【COM_STMT_EXECUTE 详细解析】\n")

	if len(payload) < 11 {
		fmt.Println("  载荷长度不足")
		return
	}

	// 读取固定字段
	statementID := binary.LittleEndian.Uint32(payload[1:5])
	flags := payload[5]
	iterationCount := binary.LittleEndian.Uint32(payload[6:10])

	fmt.Printf("  Statement ID: %d\n", statementID)
	fmt.Printf("  Flags: 0x%02x\n", flags)
	fmt.Printf("  Iteration Count: %d\n", iterationCount)

	// 解析 NULL bitmap 和 NewParamsBindFlag
	if len(payload) < 12 {
		fmt.Println("  缺少 NULL bitmap 和参数标志")
		return
	}

	// 启发式方法：确定 NULL bitmap 长度
	nullBitmapOffset := 10
	nullBitmapEnd := nullBitmapOffset

	// 查找 NULL bitmap 的结束位置（遇到 0x00 或 0x01）
	for i := nullBitmapOffset; i < len(payload); i++ {
		// 如果当前字节不是 NULL bitmap 的一部分，检查是否是 NewParamsBindFlag
		if payload[i] == 0x00 || payload[i] == 0x01 {
			// 这个字节可能是 NewParamsBindFlag
			// 但我们需要确保前面至少有 1 字节的 NULL bitmap
			if i > nullBitmapOffset {
				nullBitmapEnd = i
				break
			}
		}

		// 如果遇到非 0x00/0x01 的字节，继续
		if payload[i] != 0x00 && payload[i] != 0x01 {
			continue
		}
	}

	nullBitmap := payload[nullBitmapOffset:nullBitmapEnd]
	newParamsBindFlag := payload[nullBitmapEnd]

	fmt.Printf("\n  NULL Bitmap:\n")
	fmt.Printf("    字节数: %d\n", len(nullBitmap))
	fmt.Printf("    值 (hex): %x\n", nullBitmap)
	fmt.Printf("    值 (binary):\n")
	for _, b := range nullBitmap {
		fmt.Printf("      %08b\n", b)
	}
	fmt.Printf("  New Params Bind Flag: %d\n", newParamsBindFlag)

	// 如果有参数类型
	if newParamsBindFlag == 1 && len(payload) > nullBitmapEnd+1 {
		paramTypesOffset := nullBitmapEnd + 1
		fmt.Printf("\n  参数类型:\n")

		// 尝试读取参数类型
		paramCount := 0
		for i := paramTypesOffset; i+2 <= len(payload); i += 2 {
			paramType := payload[i]
			paramFlag := payload[i+1]

			// 检查是否是有效的 MySQL 类型
			if !isValidMySQLType(paramType) && paramCount > 0 {
				// 可能到了参数值部分
				break
			}

			typeName := getTypeName(paramType)
			fmt.Printf("    [%d] Type=0x%02x (%s), Flag=0x%02x\n",
				paramCount, paramType, typeName, paramFlag)

			paramCount++
		}

		// 参数值
		valuesOffset := paramTypesOffset + paramCount*2
		if valuesOffset < len(payload) {
			valueData := payload[valuesOffset:]
			fmt.Printf("\n  参数值:\n")
			fmt.Printf("    偏移: %d\n", valuesOffset)
			fmt.Printf("    长度: %d 字节\n", len(valueData))
			fmt.Printf("    HEX: %x\n", valueData)

			// 尝试解析参数值
			if len(valueData) > 0 {
				fmt.Printf("    解析尝试:\n")
				parseParamValues(valueData, nullBitmap, paramCount)
			}
		}
	}
}

// parseParamValues 解析参数值
func parseParamValues(data []byte, nullBitmap []byte, paramCount int) {
	offset := 0

	for i := 0; i < paramCount && offset < len(data); i++ {
		// 检查是否为 NULL（使用 MariaDB 协议：位偏移 +2）
		byteIdx := (i + 2) / 8
		bitIdx := uint((i + 2) % 8)

		isNull := false
		if byteIdx < len(nullBitmap) && (nullBitmap[byteIdx]&(1<<bitIdx)) != 0 {
			isNull = true
		}

		if isNull {
			fmt.Printf("      [%d] NULL\n", i)
			continue
		}

		// 尝试解析值
		if offset < len(data) {
			// 尝试不同的类型
			if offset+4 <= len(data) {
				// 尝试 INT
				intVal := binary.LittleEndian.Uint32(data[offset : offset+4])
				fmt.Printf("      [%d] 可能是 INT: %d\n", i, intVal)

				// 尝试字符串（长度编码）
				if data[offset] < 0xfb && offset+1+int(data[offset]) <= len(data) {
					strLen := int(data[offset])
					if strLen > 0 && strLen < 100 {
						strVal := string(data[offset+1 : offset+1+strLen])
						if isPrintable(strVal) {
							fmt.Printf("      [%d] 可能是 STRING(%d): '%s'\n", i, strLen, strVal)
						}
					}
				}

				// 根据上下文决定使用哪个
				offset += 4
			}
		}
	}
}

// isValidMySQLType 检查是否是有效的 MySQL 类型
func isValidMySQLType(t uint8) bool {
	validTypes := map[uint8]bool{
		0x01: true, // TINYINT
		0x02: true, // SMALLINT
		0x03: true, // INT
		0x04: true, // FLOAT
		0x05: true, // DOUBLE
		0x06: true, // NULL
		0x07: true, // TIMESTAMP
		0x08: true, // BIGINT
		0x09: true, // MEDIUMINT
		0x0a: true, // DATE
		0x0b: true, // TIME
		0x0c: true, // DATETIME
		0x0d: true, // YEAR
		0x0e: true, // NEWDATE
		0x0f: true, // VARCHAR
		0x10: true, // BIT
		0xfd: true, // VAR_STRING
		0xfe: true, // BLOB
		0xff: true, // GEOMETRY
	}
	return validTypes[t]
}

// getTypeName 获取类型名称
func getTypeName(t uint8) string {
	names := map[uint8]string{
		0x01: "TINYINT",
		0x02: "SMALLINT",
		0x03: "INT",
		0x04: "FLOAT",
		0x05: "DOUBLE",
		0x06: "NULL",
		0x07: "TIMESTAMP",
		0x08: "BIGINT",
		0x09: "MEDIUMINT",
		0x0a: "DATE",
		0x0b: "TIME",
		0x0c: "DATETIME",
		0x0d: "YEAR",
		0x0e: "NEWDATE",
		0x0f: "VARCHAR",
		0x10: "BIT",
		0xfd: "VAR_STRING",
		0xfe: "BLOB",
		0xff: "GEOMETRY",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return "UNKNOWN"
}

// isPrintable 检查字符串是否可打印
func isPrintable(s string) bool {
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}

// formatHex 格式化十六进制输出
func formatHex(data []byte, width int) string {
	var sb strings.Builder

	for i := 0; i < len(data); i += width {
		end := i + width
		if end > len(data) {
			end = len(data)
		}

		// 十六进制
		for j := i; j < end; j++ {
			sb.WriteString(fmt.Sprintf("%02x ", data[j]))
		}

		// 补齐
		for j := end - i; j < width; j++ {
			sb.WriteString("   ")
		}

		sb.WriteString("│ ")

		// ASCII
		for j := i; j < end; j++ {
			if data[j] >= 32 && data[j] <= 126 {
				sb.WriteByte(data[j])
			} else {
				sb.WriteByte('.')
			}
		}

		sb.WriteByte('\n')
	}

	return sb.String()
}
