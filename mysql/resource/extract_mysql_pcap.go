package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

func main() {
	file, err := os.Open("mysql.pcapng")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer file.Close()

	// 读取整个文件到内存
	stat, _ := file.Stat()
	data := make([]byte, stat.Size())
	file.Read(data)

	fmt.Println("文件大小:", stat.Size())
	fmt.Println("查找 COM_STMT_EXECUTE 包 (0x17)...")

	found := 0
	for i := 0; i < len(data)-10; i++ {
		// 查找可能的 MySQL 包头
		// MySQL 包格式: [length(3 bytes)][seq(1 byte)][command]

		length := int(data[i]) | int(data[i+1])<<8 | int(data[i+2])<<16
		if length > 0 && length < 10000 && i+4+length < len(data) {
			// 检查是否是 COM_STMT_EXECUTE (0x17)
			if data[i+3] == 0x17 {
				found++
				fmt.Printf("\n=== 找到 COM_STMT_EXECUTE 包 #%d (offset: 0x%x) ===\n", found, i)

				packetData := data[i : i+4+length]
				hexStr := fmt.Sprintf("%x", packetData)

				// 限制显示长度
				if len(hexStr) > 200 {
					hexStr = hexStr[:200] + "..."
				}

				fmt.Printf("Hex: %s\n", hexStr)
				fmt.Printf("长度: %d 字节\n", length)

				// 解析包内容
				parseStmtExecutePacket(data[i+4:i+4+length])
			}
		}
	}

	fmt.Printf("\n总共找到 %d 个 COM_STMT_EXECUTE 包\n", found)
}

func parseStmtExecutePacket(payload []byte) {
	reader := bytes.NewReader(payload)

	command, _ := reader.ReadByte()
	statementID, _ := binary.ReadUvarint(reader)
	flags, _ := reader.ReadByte()
	iterationCount, _ := binary.ReadUvarint(reader)

	fmt.Printf("  Command: 0x%02x (COM_STMT_EXECUTE)\n", command)
	fmt.Printf("  StatementID: %d\n", statementID)
	fmt.Printf("  Flags: 0x%02x\n", flags)
	fmt.Printf("  IterationCount: %d\n", iterationCount)

	// 读取 NULL bitmap
	nullBitmap := make([]byte, 0)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			break
		}

		// 下一个字节是 NewParamsBindFlag
		if reader.Len() > 0 {
			peek, _ := reader.ReadByte()
			if peek == 0x00 || peek == 0x01 {
				nullBitmap = append(nullBitmap, b)
				break
			}
			reader.UnreadByte()
		}

		nullBitmap = append(nullBitmap, b)
	}

	fmt.Printf("  NullBitmap: %v\n", nullBitmap)

	// 读取 NewParamsBindFlag
	if reader.Len() > 0 {
		newParamsBindFlag, _ := reader.ReadByte()
		fmt.Printf("  NewParamsBindFlag: %d\n", newParamsBindFlag)

		if newParamsBindFlag == 1 && reader.Len() >= 2 {
			// 读取参数类型
			fmt.Printf("  ParamTypes:\n")
			for i := 0; reader.Len() >= 2 && i < 10; i++ {
				paramType, _ := reader.ReadByte()
				paramFlag, _ := reader.ReadByte()
				fmt.Printf("    [%d] Type=0x%02x, Flag=0x%02x\n", i, paramType, paramFlag)
			}

			// 读取参数值
			if reader.Len() > 0 {
				remaining := reader.Len()
				valueData := make([]byte, remaining)
				reader.Read(valueData)
				fmt.Printf("  ParamValues (hex): %x\n", valueData)
			}
		}
	}

	// 显示完整的 payload hex
	fmt.Printf("  完整 Payload (hex): %x\n", payload)
}
