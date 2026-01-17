package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dreadl0ck/gopcap"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: parse_pcap_gopcap <pcapng_file>")
		os.Exit(1)
	}

	filename := os.Args[1]
	fmt.Println("解析文件:", filename)

	// 打开 pcapng 文件
	handle, err := gopacp.OpenOffline(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	fmt.Println("\n搜索 MySQL COM_STMT_EXECUTE 包...")
	found := 0

	// 读取数据包
	packetSource := gopacp.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		// 检查是否包含数据
		data := packet.Data()

		// 搜索 MySQL 包
		// MySQL 包格式: [length(3 bytes)][seq(1 byte)][command]
		for i := 0; i < len(data)-10; i++ {
			length := int(data[i]) | int(data[i+1])<<8 | int(data[i+2])<<16

			if length > 0 && length < 10000 && i+4+length < len(data) {
				// 检查是否是 COM_STMT_EXECUTE (0x17)
				if data[i+3] == 0x17 {
					found++
					fmt.Printf("\n=== 找到 COM_STMT_EXECUTE 包 #%d ===\n", found)
					fmt.Printf("Packet #%d, Length: %d\n", found, packet.Len())
					fmt.Printf("Offset in packet: %d\n", i)

					packetData := data[i : i+4+length]
					printMySQLPacket(packetData)

					// 只显示前 3 个包
					if found >= 3 {
						break
					}
				}
			}
		}

		if found >= 3 {
			break
		}
	}

	fmt.Printf("\n总共找到 %d 个 COM_STMT_EXECUTE 包\n", found)
}

func printMySQLPacket(data []byte) {
	hexStr := fmt.Sprintf("%x", data)
	if len(hexStr) > 200 {
		hexStr = hexStr[:200] + "..."
	}

	fmt.Printf("Hex: %s\n", hexStr)
	fmt.Printf("总长度: %d 字节\n", len(data))

	if len(data) < 15 {
		fmt.Println("包太短，无法完整解析")
		return
	}

	// 解析包头
	packetLength := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
	seqID := data[3]

	fmt.Printf("\n包头:\n")
	fmt.Printf("  长度: %d (0x%02x 0x%02x 0x%02x)\n",
		packetLength, data[0], data[1], data[2])
	fmt.Printf("  SequenceID: %d\n", seqID)

	// 解析载荷
	payload := data[4:]
	command := payload[0]

	fmt.Printf("\n载荷:\n")
	fmt.Printf("  Command: 0x%02x", command)

	commands := map[uint8]string{
		0x01: "COM_QUIT",
		0x02: "COM_INIT_DB",
		0x03: "COM_QUERY",
		0x16: "COM_STMT_PREPARE",
		0x17: "COM_STMT_EXECUTE",
		0x19: "COM_STMT_CLOSE",
	}

	if cmdName, ok := commands[command]; ok {
		fmt.Printf(" (%s)", cmdName)
	}
	fmt.Println()

	if command == 0x17 && len(payload) >= 13 {
		// 解析 COM_STMT_EXECUTE
		statementID := binary.LittleEndian.Uint32(payload[1:5])
		flags := payload[5]
		iterationCount := binary.LittleEndian.Uint32(payload[6:10])

		fmt.Printf("\nCOM_STMT_EXECUTE 详情:\n")
		fmt.Printf("  StatementID: %d\n", statementID)
		fmt.Printf("  Flags: 0x%02x\n", flags)
		fmt.Printf("  IterationCount: %d\n", iterationCount)

		// NULL bitmap 和 NewParamsBindFlag
		if len(payload) >= 12 {
			nullBitmapLen := 1 // 默认至少1字节
			nullBitmap := payload[10 : 10+nullBitmapLen]
			newParamsBindFlag := payload[10+nullBitmapLen]

			fmt.Printf("  NullBitmap (%d bytes): %v\n", len(nullBitmap), nullBitmap)
			fmt.Printf("  NullBitmap (hex): %x\n", nullBitmap)
			fmt.Printf("  NewParamsBindFlag: %d\n", newParamsBindFlag)

			// 解析参数类型
			if newParamsBindFlag == 1 && len(payload) >= 14 {
				paramTypesOffset := 10 + nullBitmapLen + 1
				fmt.Printf("  ParamTypes (从偏移 %d):\n", paramTypesOffset)

				for i := paramTypesOffset; i+2 <= len(payload) && i < paramTypesOffset+20; i += 2 {
					paramType := payload[i]
					paramFlag := payload[i+1]

					// 检查是否是有效的类型字节
					if paramType == 0 && paramFlag == 0 && (i-paramTypesOffset) > 4 {
						// 可能到了参数值部分
						break
					}

					typeNames := map[uint8]string{
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
					}

					typeName := "UNKNOWN"
					if name, ok := typeNames[paramType]; ok {
						typeName = name
					}

					fmt.Printf("    [%d] Type=0x%02x (%s), Flag=0x%02x\n",
						(i-paramTypesOffset)/2, paramType, typeName, paramFlag)

					// 如果检测到非类型值，停止
					if paramType == 0xfd && i+4 < len(payload) {
						// 可能是 VAR_STRING 后面跟着参数值
						nextByte := payload[i+2]
						if nextByte < 20 || nextByte > 0x80 {
							// 可能是参数值的长度字节
							break
						}
					}
				}

				// 显示剩余数据（参数值）
				paramTypesOffset := 10 + nullBitmapLen + 1
				// 尝试确定参数类型的长度
				// 通常每个参数类型是2字节
				numParams := 0
				for i := paramTypesOffset; i+2 <= len(payload); i += 2 {
					// 如果这个字节不是常见的类型值，可能是参数值开始
					if payload[i] > 0x20 && payload[i] < 0x80 {
						// 可能是字符串长度
						break
					}
					numParams++
					if numParams >= 10 {
						break
					}
				}

				valuesOffset := paramTypesOffset + numParams*2
				if valuesOffset < len(payload) {
					valueData := payload[valuesOffset:]
					fmt.Printf("  ParamValues (从偏移 %d, 长度 %d):\n", valuesOffset, len(valueData))
					fmt.Printf("    Hex: %x\n", valueData)

					// 尝试解析参数值
					if len(valueData) > 0 {
						fmt.Printf("    尝试解析:\n")

						// INT 类型 (4字节)
						if len(valueData) >= 4 {
							intVal := binary.LittleEndian.Uint32(valueData[:4])
							fmt.Printf("      INT: %d\n", intVal)
						}

						// 字符串类型 (长度编码)
						if len(valueData) > 1 && valueData[0] < 0xfb {
							strLen := int(valueData[0])
							if len(valueData) >= 1+strLen {
								strVal := string(valueData[1 : 1+strLen])
								fmt.Printf("      STRING(%d): '%s'\n", strLen, strVal)
							}
						}
					}
				}
			}
		}
	}

	// 显示完整的 payload
	fmt.Printf("\n完整 Payload (hex):\n")
	fmt.Printf("  %s\n", formatHex(payload, 16))
}

func formatHex(data []byte, width int) string {
	var sb strings.Builder
	for i := 0; i < len(data); i += width {
		end := i + width
		if end > len(data) {
			end = len(data)
		}

		for j := i; j < end; j++ {
			sb.WriteString(fmt.Sprintf("%02x ", data[j]))
		}

		// 补齐到指定宽度
		for j := end - i; j < width; j++ {
			sb.WriteString("   ")
		}

		sb.WriteString("| ")

		// ASCII 表示
		for j := i; j < end; j++ {
			if data[j] >= 32 && data[j] <= 126 {
				sb.WriteByte(data[j])
			} else {
				sb.WriteByte('.')
			}
		}

		sb.WriteString("\n")
	}

	return sb.String()
}
