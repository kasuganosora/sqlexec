package main

import (
	"bytes"
	"encoding/hex"
	"fmt"

	mysql_proxy "mysql-proxy/mysql/protocol"
)

func main() {
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("         COM_STMT_EXECUTE 综合测试                       ")
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println()

	tests := []struct {
		name     string
		packet   *mysql_proxy.ComStmtExecutePacket
		check    func(*mysql_proxy.ComStmtExecutePacket) error
	}{
		{
			name: "测试1: 单个 INT 参数",
			packet: &mysql_proxy.ComStmtExecutePacket{
				Packet:            mysql_proxy.Packet{},
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				NullBitmap:        []byte{0x00},
				NewParamsBindFlag: 1,
				ParamTypes: []mysql_proxy.StmtParamType{
					{Type: 0x03, Flag: 0x00}, // INT
				},
				ParamValues: []any{123},
			},
			check: func(p *mysql_proxy.ComStmtExecutePacket) error {
				if len(p.NullBitmap) != 1 {
					return fmt.Errorf("NULL bitmap 长度应该是 1，实际是 %d", len(p.NullBitmap))
				}
				if p.NullBitmap[0] != 0x00 {
					return fmt.Errorf("NULL bitmap 应该是 0x00，实际是 0x%02x", p.NullBitmap[0])
				}
				return nil
			},
		},
		{
			name: "测试2: NULL 参数 (1 个参数)",
			packet: &mysql_proxy.ComStmtExecutePacket{
				Packet:            mysql_proxy.Packet{},
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				NullBitmap:        []byte{0x04}, // MariaDB: 位 2 被设置
				NewParamsBindFlag: 1,
				ParamTypes: []mysql_proxy.StmtParamType{
					{Type: 0xfd, Flag: 0x00}, // VAR_STRING
				},
				ParamValues: []any{nil},
			},
			check: func(p *mysql_proxy.ComStmtExecutePacket) error {
				if len(p.NullBitmap) != 1 {
					return fmt.Errorf("NULL bitmap 长度应该是 1，实际是 %d", len(p.NullBitmap))
				}
				// MariaDB: 参数 1 对应位 2，值为 0x04
				if p.NullBitmap[0] != 0x04 {
					return fmt.Errorf("NULL bitmap 应该是 0x04 (MariaDB位2)，实际是 0x%02x", p.NullBitmap[0])
				}
				return nil
			},
		},
		{
			name: "测试3: 9 个参数 (测试多字节 NULL bitmap)",
			packet: &mysql_proxy.ComStmtExecutePacket{
				Packet:            mysql_proxy.Packet{},
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				NullBitmap:        []byte{0x00, 0x00},
				NewParamsBindFlag: 1,
				ParamTypes: []mysql_proxy.StmtParamType{
					{Type: 0x03, Flag: 0x00}, // INT
					{Type: 0x02, Flag: 0x00}, // SMALLINT
					{Type: 0x09, Flag: 0x00}, // MEDIUMINT
					{Type: 0x08, Flag: 0x00}, // BIGINT
					{Type: 0x04, Flag: 0x00}, // FLOAT
					{Type: 0x05, Flag: 0x00}, // DOUBLE
					{Type: 0xfd, Flag: 0x00}, // VAR_STRING
					{Type: 0x0f, Flag: 0x00}, // VARCHAR
					{Type: 0x01, Flag: 0x00}, // TINYINT
				},
				ParamValues: []any{500, 32000, 8000000, int64(9000000000000000000), 3.14159, 2.718281828459045, "test", "fixed", 100},
			},
			check: func(p *mysql_proxy.ComStmtExecutePacket) error {
				// MariaDB: (9 + 2 + 7) / 8 = 2 字节
				if len(p.NullBitmap) != 2 {
					return fmt.Errorf("NULL bitmap 长度应该是 2，实际是 %d", len(p.NullBitmap))
				}
				if p.NullBitmap[0] != 0x00 || p.NullBitmap[1] != 0x00 {
					return fmt.Errorf("NULL bitmap 应该是 0x0000，实际是 0x%02x%02x", p.NullBitmap[0], p.NullBitmap[1])
				}
				return nil
			},
		},
		{
			name: "测试4: 9 个参数，参数 7 是 NULL",
			packet: &mysql_proxy.ComStmtExecutePacket{
				Packet:            mysql_proxy.Packet{},
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				NullBitmap:        []byte{0x00, 0x02}, // 参数 7 对应位 8 (MariaDB)
				NewParamsBindFlag: 1,
				ParamTypes: []mysql_proxy.StmtParamType{
					{Type: 0x03, Flag: 0x00},
					{Type: 0x02, Flag: 0x00},
					{Type: 0x09, Flag: 0x00},
					{Type: 0x08, Flag: 0x00},
					{Type: 0x04, Flag: 0x00},
					{Type: 0x05, Flag: 0x00},
					{Type: 0xfd, Flag: 0x00},
					{Type: 0x0f, Flag: 0x00},
					{Type: 0x01, Flag: 0x00},
				},
				ParamValues: []any{500, 32000, 8000000, int64(9000000000000000000), 3.14159, 2.718281828459045, nil, "fixed", 100},
			},
			check: func(p *mysql_proxy.ComStmtExecutePacket) error {
				// MariaDB: (9 + 2 + 7) / 8 = 2 字节
				// 参数 7 对应位 (7 + 2) = 9，即第二个字节的位 1，值为 0x02
				if len(p.NullBitmap) != 2 {
					return fmt.Errorf("NULL bitmap 长度应该是 2，实际是 %d", len(p.NullBitmap))
				}
				if p.NullBitmap[0] != 0x00 || p.NullBitmap[1] != 0x02 {
					return fmt.Errorf("NULL bitmap 应该是 0x0002 (位9)，实际是 0x%02x%02x", p.NullBitmap[0], p.NullBitmap[1])
				}
				return nil
			},
		},
		{
			name: "测试5: 15 个参数 (测试 3 字节 NULL bitmap)",
			packet: &mysql_proxy.ComStmtExecutePacket{
				Packet:            mysql_proxy.Packet{},
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				NullBitmap:        []byte{0x00, 0x00, 0x00},
				NewParamsBindFlag: 1,
				ParamTypes: []mysql_proxy.StmtParamType{
					{Type: 0x03, Flag: 0x00},
					{Type: 0x02, Flag: 0x00},
					{Type: 0x09, Flag: 0x00},
					{Type: 0x08, Flag: 0x00},
					{Type: 0x04, Flag: 0x00},
					{Type: 0x05, Flag: 0x00},
					{Type: 0xfd, Flag: 0x00},
					{Type: 0x0f, Flag: 0x00},
					{Type: 0x01, Flag: 0x00},
					{Type: 0x03, Flag: 0x00},
					{Type: 0x02, Flag: 0x00},
					{Type: 0x09, Flag: 0x00},
					{Type: 0x08, Flag: 0x00},
					{Type: 0x04, Flag: 0x00},
					{Type: 0x05, Flag: 0x00},
				},
				ParamValues: []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			check: func(p *mysql_proxy.ComStmtExecutePacket) error {
				// MariaDB: (15 + 2 + 7) / 8 = 3 字节
				if len(p.NullBitmap) != 3 {
					return fmt.Errorf("NULL bitmap 长度应该是 3，实际是 %d", len(p.NullBitmap))
				}
				if p.NullBitmap[0] != 0x00 || p.NullBitmap[1] != 0x00 || p.NullBitmap[2] != 0x00 {
					return fmt.Errorf("NULL bitmap 应该是 0x000000，实际是 0x%02x%02x%02x", p.NullBitmap[0], p.NullBitmap[1], p.NullBitmap[2])
				}
				return nil
			},
		},
		{
			name: "测试6: 15 个参数，参数 7 是 NULL (测试位 9)",
			packet: &mysql_proxy.ComStmtExecutePacket{
				Packet:            mysql_proxy.Packet{},
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				NullBitmap:        []byte{0x00, 0x02, 0x00}, // 参数 7 对应位 9
				NewParamsBindFlag: 1,
				ParamTypes: []mysql_proxy.StmtParamType{
					{Type: 0x03, Flag: 0x00},
					{Type: 0x02, Flag: 0x00},
					{Type: 0x09, Flag: 0x00},
					{Type: 0x08, Flag: 0x00},
					{Type: 0x04, Flag: 0x00},
					{Type: 0x05, Flag: 0x00},
					{Type: 0xfd, Flag: 0x00},
					{Type: 0x0f, Flag: 0x00},
					{Type: 0x01, Flag: 0x00},
					{Type: 0x03, Flag: 0x00},
					{Type: 0x02, Flag: 0x00},
					{Type: 0x09, Flag: 0x00},
					{Type: 0x08, Flag: 0x00},
					{Type: 0x04, Flag: 0x00},
					{Type: 0x05, Flag: 0x00},
				},
				ParamValues: []any{1, 2, 3, 4, 5, 6, nil, 8, 9, 10, 11, 12, 13, 14, 15},
			},
			check: func(p *mysql_proxy.ComStmtExecutePacket) error {
				// MariaDB: 参数 7 对应位 (7 + 2) = 9
				// 字节 1 (第二字节), 位 1 (从 0 开始)
				// 值应该是 0x02
				if len(p.NullBitmap) != 3 {
					return fmt.Errorf("NULL bitmap 长度应该是 3，实际是 %d", len(p.NullBitmap))
				}
				if p.NullBitmap[0] != 0x00 || p.NullBitmap[1] != 0x02 || p.NullBitmap[2] != 0x00 {
					return fmt.Errorf("NULL bitmap 应该是 0x000200 (位9)，实际是 0x%02x%02x%02x", p.NullBitmap[0], p.NullBitmap[1], p.NullBitmap[2])
				}
				return nil
			},
		},
	}

	passCount := 0
	failCount := 0

	for i, test := range tests {
		fmt.Printf("【%s】\n", test.name)

		// 序列化
		data, err := test.packet.Marshal()
		if err != nil {
			fmt.Printf("  ❌ 序列化失败: %v\n\n", err)
			failCount++
			continue
		}

		// 打印序列化数据
		fmt.Printf("  ✅ 序列化成功\n")
		fmt.Printf("     数据 (hex): %s\n", hex.EncodeToString(data))
		fmt.Printf("     数据长度: %d 字节\n\n", len(data))

		// 解析
		parsedPacket := &mysql_proxy.ComStmtExecutePacket{}
		err = parsedPacket.Unmarshal(bytes.NewReader(data))
		if err != nil {
			fmt.Printf("  ❌ 解析失败: %v\n\n", err)
			failCount++
			continue
		}

		// 验证
		err = test.check(parsedPacket)
		if err != nil {
			fmt.Printf("  ❌ 验证失败: %v\n\n", err)
			failCount++
			continue
		}

		// 详细分析
		fmt.Printf("  ✅ 验证通过\n")
		fmt.Printf("     Statement ID: %d\n", parsedPacket.StatementID)
		fmt.Printf("     NULL bitmap 长度: %d 字节\n", len(parsedPacket.NullBitmap))
		fmt.Printf("     NULL bitmap 值: 0x%x\n", parsedPacket.NullBitmap)
		fmt.Printf("     参数类型数量: %d\n", len(parsedPacket.ParamTypes))
		fmt.Printf("     参数值数量: %d\n", len(parsedPacket.ParamValues))

		// 显示 NULL bitmap 的二进制
		fmt.Printf("     NULL bitmap (binary):\n")
		for j, b := range parsedPacket.NullBitmap {
			fmt.Printf("       字节 %d: %08b\n", j, b)
		}

		// 显示参数映射
		fmt.Printf("     参数 NULL 标志位 (MariaDB协议):\n")
		for j := 0; j < len(parsedPacket.ParamTypes); j++ {
			byteIdx := (j + 2) / 8
			bitIdx := uint((j + 2) % 8)
			if byteIdx < len(parsedPacket.NullBitmap) {
				isNull := (parsedPacket.NullBitmap[byteIdx] & (1 << bitIdx)) != 0
				fmt.Printf("       参数 %2d: 字节%d 位%d = %v\n", j+1, byteIdx, bitIdx, isNull)
			}
		}

		fmt.Println()
		passCount++
	}

	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Printf("测试结果: 通过 %d / %d，失败 %d\n", passCount, len(tests), failCount)
	fmt.Println("═════════════════════════════════════════════════════════")
}
