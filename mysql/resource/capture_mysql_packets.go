package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"mysql-proxy/mysql/protocol"
)

func main() {
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("      MySQL/MariaDB 协议包捕获测试工具                ")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println()

	// 连接参数
	host := "127.0.0.1"
	port := 3306
	username := "root"
	database := "test"

	fmt.Printf("连接参数:\n")
	fmt.Printf("  主机: %s:%d\n", host, port)
	fmt.Printf("  用户名: %s\n", username)
	fmt.Printf("  数据库: %s\n", database)
	fmt.Println()

	// 连接到 MariaDB
	fmt.Println("正在连接到 MariaDB...")
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("❌ 连接失败: %v", err)
	}
	defer conn.Close()
	fmt.Println("✅ 连接成功")
	fmt.Println()
	fmt.Println("💡 提示：请使用 Wireshark 抓取 localhost:3306 的数据包")
	fmt.Println("   过滤器: tcp.port == 3306 and mysql")
	fmt.Println()

	// 读取握手包
	fmt.Println("【步骤 1: 握手】")
	handshake := &protocol.HandshakeResponse{}
	err = handshake.Unmarshal(conn)
	if err != nil {
		log.Fatalf("❌ 读取握手失败: %v", err)
	}
	printPacket("收到握手", handshake.Packet)

	// 发送认证包
	fmt.Println("【步骤 2: 认证】")
	auth := &protocol.HandshakeResponse{
		CapabilityFlags:         protocol.CLIENT_PROTOCOL_41 | protocol.CLIENT_SECURE_CONNECTION | protocol.CLIENT_PLUGIN_AUTH,
		MaxPacketSize:           16777215,
		CharacterSet:            33,
		Username:                username,
		AuthResponse:            []byte{0},
		AuthPluginName:         "mysql_native_password",
		ExtendedClientCapabilities: protocol.CLIENT_MYSQL | protocol.CLIENT_PLUGIN_AUTH,
	}
	auth.Packet.SequenceID = 1

	authData, err := auth.Marshal()
	if err != nil {
		log.Fatalf("❌ 序列化认证包失败: %v", err)
	}

	printAndSend("发送认证包", conn, auth.Packet.SequenceID, authData)

	// 读取认证响应
	okPkt := &protocol.OkPacket{}
	okPkt.Unmarshal(conn)
	printPacket("收到认证响应", okPkt.Packet)
	fmt.Println()

	// 测试场景
	testScenarios := []struct {
		name  string
		query string
		params []any
		paramTypes []protocol.StmtParamType
	}{
		{
			name:  "场景1: 单个 INT 参数",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_int = ?",
			params: []any{int32(500)},
			paramTypes: []protocol.StmtParamType{{Type: 0x03, Flag: 0}},
		},
		{
			name:  "场景2: 单个 VARCHAR 参数",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_varchar = ?",
			params: []any{"variable length"},
			paramTypes: []protocol.StmtParamType{{Type: 0xfd, Flag: 0}},
		},
		{
			name:  "场景3: 多个参数 (INT + VARCHAR)",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_int = ? AND type_varchar = ?",
			params: []any{int32(500), "variable length"},
			paramTypes: []protocol.StmtParamType{
				{Type: 0x03, Flag: 0}, // INT
				{Type: 0xfd, Flag: 0}, // VAR_STRING
			},
		},
		{
			name:  "场景4: 带 NULL 参数",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_bool = ?",
			params: []any{nil},
			paramTypes: []protocol.StmtParamType{{Type: 0x01, Flag: 0}},
		},
		{
			name:  "场景5: TINYINT 参数",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_tinyint = ?",
			params: []any{int8(100)},
			paramTypes: []protocol.StmtParamType{{Type: 0x01, Flag: 0}},
		},
		{
			name:  "场景6: BIGINT 参数",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_bigint = ?",
			params: []any{int64(9000000000000000000)},
			paramTypes: []protocol.StmtParamType{{Type: 0x08, Flag: 0}},
		},
		{
			name:  "场景7: FLOAT 参数",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_float = ?",
			params: []any{float32(3.14159)},
			paramTypes: []protocol.StmtParamType{{Type: 0x04, Flag: 0}},
		},
		{
			name:  "场景8: DOUBLE 参数",
			query:  "SELECT * FROM mysql_data_types_demo WHERE type_double = ?",
			params: []any{float64(2.718281828459045)},
			paramTypes: []protocol.StmtParamType{{Type: 0x05, Flag: 0}},
		},
	}

	// 初始化数据库（选择数据库）
	fmt.Println("【步骤 3: 选择数据库】")
	initDbPkt := &protocol.ComInitDbPacket{}
	initDbPkt.Command = protocol.COM_INIT_DB
	initDbPkt.DatabaseName = database
	initDbPkt.Packet.SequenceID = 0

	initDbData, _ := initDbPkt.Marshal()
	printAndSend("发送 INIT_DB", conn, initDbPkt.Packet.SequenceID, initDbData)

	initDbOk := &protocol.OkPacket{}
	initDbOk.Unmarshal(conn)
	printPacket("收到 INIT_DB 响应", initDbOk.Packet)
	fmt.Println()

	// 运行测试场景
	for i, scenario := range testScenarios {
		fmt.Printf("【测试场景 %d: %s】\n", i+1, scenario.name)
		fmt.Printf("  查询: %s\n", scenario.query)
		fmt.Printf("  参数数量: %d\n", len(scenario.params))

		// PREPARE
		fmt.Println("\n  → 执行 COM_STMT_PREPARE")
		preparePkt := &protocol.ComStmtPreparePacket{
			Packet: protocol.Packet{SequenceID: 0},
			Command: protocol.COM_STMT_PREPARE,
			Query:   scenario.query,
		}
		prepareData, _ := preparePkt.Marshal()
		printAndSend("  发送 PREPARE", conn, preparePkt.Packet.SequenceID, prepareData)

		// 读取 PREPARE 响应
		prepareResp := &protocol.StmtPrepareResponsePacket{}
		prepareResp.Unmarshal(conn)
		printPacket("  收到 PREPARE 响应", prepareResp.Packet)
		fmt.Printf("    StatementID: %d\n", prepareResp.StatementID)
		fmt.Printf("    ParamCount: %d\n", prepareResp.ParamCount)
		fmt.Printf("    ColumnCount: %d\n", prepareResp.ColumnCount)

		// 读取参数定义（如果有）
		for j := 0; j < int(prepareResp.ParamCount); j++ {
			paramPkt := &protocol.FieldMetaPacket{}
			paramPkt.Unmarshal(conn, 0)
		}

		// 读取列定义
		for j := 0; j < int(prepareResp.ColumnCount); j++ {
			colPkt := &protocol.FieldMetaPacket{}
			colPkt.Unmarshal(conn, protocol.CLIENT_PROTOCOL_41)
		}

		// 读取 EOF
		eofPkt := &protocol.EOFPacket{}
		eofPkt.Unmarshal(conn)

		// EXECUTE
		fmt.Println("\n  → 执行 COM_STMT_EXECUTE")
		executePkt := &protocol.ComStmtExecutePacket{
			Packet: protocol.Packet{SequenceID: 0},
			Command:           protocol.COM_STMT_EXECUTE,
			StatementID:       prepareResp.StatementID,
			Flags:             0,
			IterationCount:    1,
			NullBitmap:        calculateNullBitmap(scenario.params),
			NewParamsBindFlag: 1,
			ParamTypes:        scenario.paramTypes,
			ParamValues:       scenario.params,
		}

		executeData, _ := executePkt.Marshal()
		printAndSend("  发送 EXECUTE", conn, executePkt.Packet.SequenceID, executeData)

		// 读取结果集
		readResultSet(conn)

		fmt.Println()
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("              所有测试场景执行完成                      ")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("💡 现在请检查 Wireshark 抓取的数据包")
	fmt.Println("   应该能看到各种类型的 COM_STMT_PREPARE 和 COM_STMT_EXECUTE 包")
}

func calculateNullBitmap(params []any) []byte {
	// MySQL 协议：NULL bitmap
	nullBitmapLen := (len(params) + 7) / 8
	nullBitmap := make([]byte, nullBitmapLen)

	for i, param := range params {
		if param == nil {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			nullBitmap[byteIdx] |= (1 << bitIdx)
		}
	}

	return nullBitmap
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

func readResultSet(conn net.Conn) {
	// 读取 ColumnCount
	colCountPkt := &protocol.ColumnCountPacket{}
	colCountPkt.Unmarshal(conn)

	// 读取列定义
	for i := 0; i < int(colCountPkt.ColumnCount); i++ {
		colPkt := &protocol.FieldMetaPacket{}
		colPkt.Unmarshal(conn, protocol.CLIENT_PROTOCOL_41)
	}

	// 读取 EOF
	eofPkt := &protocol.EOFPacket{}
	eofPkt.Unmarshal(conn)

	// 读取行数据（最多读取10行）
	for i := 0; i < 10; i++ {
		rowPkt := &protocol.Packet{}
		err := rowPkt.Unmarshal(conn)
		if err != nil {
			break
		}

		// 如果是 EOF 或 OK，停止
		if len(rowPkt.Payload) > 0 {
			cmd := rowPkt.Payload[0]
			if cmd == 0xfe || cmd == 0x00 || cmd == 0x0a {
				break
			}
		}
	}

	// 读取 EOF
	eofPkt2 := &protocol.EOFPacket{}
	eofPkt2.Unmarshal(conn)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
