package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func main() {
	filename := `D:\code\db\mysql\resource\mysql.pcapng`
	fmt.Println("正在分析文件:", filename)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("❌ 无法打开文件: %v\n", err)
		return
	}
	defer file.Close()

	// 读取整个文件
	stat, _ := file.Stat()
	data := make([]byte, stat.Size())
	_, err = io.ReadFull(file, data)
	if err != nil {
		fmt.Printf("❌ 读取文件错误: %v\n", err)
		return
	}

	fmt.Printf("✅ 文件大小: %d 字节\n\n", stat.Size())
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("         搜索 MySQL COM_STMT_EXECUTE 包 (0x17)          ")
	fmt.Println("═══════════════════════════════════════════════════════════\n")

	found := 0
	for i := 0; i < len(data)-10; i++ {
		// MySQL 包格式: [length(3 bytes)][seq(1 byte)][command]
		length := int(data[i]) | int(data[i+1])<<8 | int(data[i+2])<<16

		if length > 0 && length < 100000 && i+4+length < len(data) {
			// 检查是否是 COM_STMT_EXECUTE (0x17)
			if data[i+3] == 0x17 {
				found++
				fmt.Printf("\n【包 #%d - 偏移: 0x%x (十进制: %d)】\n", found, i, i)

				packetData := data[i : i+4+length]
				printPacket(packetData, found)

				if found >= 5 {
					break
				}
			}
		}
	}

	fmt.Printf("\n═══════════════════════════════════════════════════════════\n")
	fmt.Printf("                    总共找到 %d 个包                          \n", found)
	fmt.Println("═══════════════════════════════════════════════════════════\n")
}

func printPacket(data []byte, index int) {
	if len(data) < 4 {
		fmt.Println("  ❌ 包太短，无法解析")
		return
	}

	// 解析包头
	packetLength := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
	seqID := data[3]

	fmt.Printf("  【包头】\n")
	fmt.Printf("    长度: %d 字节 (0x%02x 0x%02x 0x%02x)\n", packetLength, data[0], data[1], data[2])
	fmt.Printf("    Sequence ID: %d\n", seqID)

	// 解析载荷
	if len(data) < 5 {
		fmt.Println("  ❌ 载荷为空")
		return
	}

	payload := data[4:]
	command := payload[0]

	fmt.Printf("  【载荷】\n")
	fmt.Printf("    Command: 0x%02x", command)

	cmdNames := map[uint8]string{
		0x16: "COM_STMT_PREPARE",
		0x17: "COM_STMT_EXECUTE",
		0x19: "COM_STMT_CLOSE",
	}

	if cmdName, ok := cmdNames[command]; ok {
		fmt.Printf(" (%s)", cmdName)
	}
	fmt.Printf("\n")

	// 如果是 COM_STMT_EXECUTE，详细解析
	if command == 0x17 && len(payload) >= 11 {
		parseCOMStmtExecute(payload)
	}

	// 完整 HEX dump
	fmt.Printf("\n  【完整包数据 (hex)】\n")
	printHexDump(data, 16)
}

func parseCOMStmtExecute(payload []byte) {
	fmt.Printf("\n  【COM_STMT_EXECUTE 详细解析】\n")

	if len(payload) < 11 {
		fmt.Println("    ❌ 载荷长度不足")
		return
	}

	// 固定字段
	statementID := binary.LittleEndian.Uint32(payload[1:5])
	flags := payload[5]
	iterationCount := binary.LittleEndian.Uint32(payload[6:10])

	fmt.Printf("    Statement ID: %d\n", statementID)
	fmt.Printf("    Flags: 0x%02x\n", flags)
	fmt.Printf("    Iteration Count: %d\n", iterationCount)

	// 解析 NULL bitmap
	if len(payload) < 12 {
		fmt.Println("    ❌ 缺少 NULL bitmap")
		return
	}

	// 启发式：确定 NULL bitmap 长度
	// 从偏移 10 开始，读取直到遇到可能的 NewParamsBindFlag
	nullBitmapOffset := 10
	nullBitmap := make([]byte, 0)
	newParamsBindFlagOffset := nullBitmapOffset

	for i := nullBitmapOffset; i < len(payload); i++ {
		b := payload[i]

		// 如果下一个字节是 0x00 或 0x01，可能是 NewParamsBindFlag
		if (b == 0x00 || b == 0x01) && len(nullBitmap) > 0 {
			// 检查这个字节后面的字节是否是有效的类型
			if i+2 < len(payload) {
				nextType := payload[i+1]
				nextFlag := payload[i+2]

				// 如果看起来像参数类型（Type < 0x20, Flag < 0x10）
				if nextType < 0x20 && nextFlag < 0x10 {
					nullBitmap = append(nullBitmap, b)
					newParamsBindFlagOffset = i
					break
				}
			}
		}

		nullBitmap = append(nullBitmap, b)
	}

	// 如果没有找到 NewParamsBindFlag，假设最后 1 字节是 NewParamsBindFlag
	if len(nullBitmap) > 0 && newParamsBindFlagOffset == nullBitmapOffset {
		newParamsBindFlagOffset = nullBitmapOffset + len(nullBitmap) - 1
	}

	fmt.Printf("    NULL Bitmap:\n")
	fmt.Printf("      字节数: %d\n", len(nullBitmap))
	fmt.Printf("      值 (hex): %x\n", nullBitmap)
	fmt.Printf("      值 (binary):\n")
	for _, b := range nullBitmap {
		fmt.Printf("        %08b\n", b)
	}

	// NewParamsBindFlag
	if newParamsBindFlagOffset < len(payload) {
		newParamsBindFlag := payload[newParamsBindFlagOffset]
		fmt.Printf("    New Params Bind Flag: %d\n", newParamsBindFlag)

		// 解析参数类型
		if newParamsBindFlag == 1 && len(payload) > newParamsBindFlagOffset+1 {
			paramTypesOffset := newParamsBindFlagOffset + 1
			fmt.Printf("    参数类型:\n")

			paramCount := 0
			for i := paramTypesOffset; i+2 <= len(payload); i += 2 {
				paramType := payload[i]
				paramFlag := payload[i+1]

				// 验证是否是有效的类型
				if !isValidMySQLType(paramType) && paramCount > 0 {
					break
				}

				typeName := getTypeName(paramType)
				fmt.Printf("      [%d] Type=0x%02x (%s), Flag=0x%02x\n",
					paramCount, paramType, typeName, paramFlag)

				paramCount++
			}

			// 解析参数值
			valuesOffset := paramTypesOffset + paramCount*2
			if valuesOffset < len(payload) {
				valueData := payload[valuesOffset:]
				fmt.Printf("    参数值:\n")
				fmt.Printf("      偏移: %d\n", valuesOffset)
				fmt.Printf("      长度: %d 字节\n", len(valueData))
				fmt.Printf("      值 (hex): %x\n", valueData)

				// 尝试解析
				if len(valueData) > 0 {
					fmt.Printf("      尝试解析:\n")
					parseParamValues(valueData, nullBitmap, paramCount)
				}
			}
		}
	}
}

func parseParamValues(data []byte, nullBitmap []byte, paramCount int) {
	offset := 0

	for i := 0; i < paramCount && offset < len(data); i++ {
		// 检查 NULL 标志
		// 尝试 MySQL 协议（位 0 开始）
		mysqlIsNull := false
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		if byteIdx < len(nullBitmap) && (nullBitmap[byteIdx]&(1<<bitIdx)) != 0 {
			mysqlIsNull = true
		}

		// 尝试 MariaDB 协议（位 2 开始）
		mariadbIsNull := false
		byteIdx2 := (i + 2) / 8
		bitIdx2 := uint((i + 2) % 8)
		if byteIdx2 < len(nullBitmap) && (nullBitmap[byteIdx2]&(1<<bitIdx2)) != 0 {
			mariadbIsNull = true
		}

		if mysqlIsNull || mariadbIsNull {
			fmt.Printf("        [%d] NULL (MySQL:%v, MariaDB:%v)\n", i, mysqlIsNull, mariadbIsNull)
			continue
		}

		// 尝试解析值
		if offset < len(data) {
			// INT 类型 (4字节)
			if offset+4 <= len(data) {
				intVal := binary.LittleEndian.Uint32(data[offset : offset+4])
				fmt.Printf("        [%d] INT: %d\n", i, intVal)

				// 尝试字符串
				if data[offset] < 0xfb && offset+1+int(data[offset]) <= len(data) {
					strLen := int(data[offset])
					if strLen > 0 && strLen < 50 {
						strVal := string(data[offset+1 : offset+1+strLen])
						if isPrintable(strVal) {
							fmt.Printf("        [%d] STRING(%d): '%s'\n", i, strLen, strVal)
						}
					}
				}

				offset += 4
			}
		}
	}
}

func isValidMySQLType(t uint8) bool {
	validTypes := map[uint8]bool{
		0x01: true, 0x02: true, 0x03: true, 0x04: true,
		0x05: true, 0x06: true, 0x07: true, 0x08: true,
		0x09: true, 0x0a: true, 0x0b: true, 0x0c: true,
		0x0d: true, 0x0e: true, 0x0f: true, 0x10: true,
		0xfd: true, 0xfe: true, 0xff: true,
	}
	return validTypes[t]
}

func getTypeName(t uint8) string {
	names := map[uint8]string{
		0x01: "TINYINT", 0x02: "SMALLINT", 0x03: "INT",
		0x04: "FLOAT", 0x05: "DOUBLE", 0x06: "NULL",
		0x07: "TIMESTAMP", 0x08: "BIGINT", 0x09: "MEDIUMINT",
		0x0a: "DATE", 0x0b: "TIME", 0x0c: "DATETIME",
		0x0d: "YEAR", 0x0e: "NEWDATE", 0x0f: "VARCHAR",
		0x10: "BIT", 0xfd: "VAR_STRING", 0xfe: "BLOB",
		0xff: "GEOMETRY",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return "UNKNOWN"
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}

func printHexDump(data []byte, width int) {
	fmt.Printf("      ")
	for j := 0; j < width; j++ {
		fmt.Printf("%2x ", j)
	}
	fmt.Printf("  ASCII\n")
	fmt.Printf("      ")
	for j := 0; j < width; j++ {
		fmt.Printf("---")
	}
	fmt.Printf("  %s\n", bytes.Repeat([]byte("-"), width))

	for i := 0; i < len(data); i += width {
		end := i + width
		if end > len(data) {
			end = len(data)
		}

		// 偏移
		fmt.Printf("%04x  ", i)

		// 十六进制
		for j := i; j < end; j++ {
			fmt.Printf("%02x ", data[j])
		}

		// 补齐
		for j := end - i; j < width; j++ {
			fmt.Printf("   ")
		}

		// ASCII
		fmt.Printf(" │")
		for j := i; j < end; j++ {
			if data[j] >= 32 && data[j] <= 126 {
				fmt.Printf("%c", data[j])
			} else {
				fmt.Printf(".")
			}
		}
		fmt.Printf("│\n")
	}
}
