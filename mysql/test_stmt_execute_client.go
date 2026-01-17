package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"mysql-proxy/mysql/protocol"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:13307")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	fmt.Println("连接到服务器 127.0.0.1:13307")

	// 读取握手
	handshake := &protocol.HandshakeResponse{}
	handshake.Unmarshal(conn)
	fmt.Printf("服务器版本: %s\n", handshake.ServerVersion)
	fmt.Printf("Capability flags: 0x%08x\n", handshake.CapabilityFlags)

	// 发送认证
	auth := &protocol.HandshakeResponse{
		CapabilityFlags:         protocol.CLIENT_PROTOCOL_41 | protocol.CLIENT_SECURE_CONNECTION,
		MaxPacketSize:           16777215,
		CharacterSet:            33,
		Username:                "root",
		AuthResponse:            []byte{0},
		AuthPluginName:         "mysql_native_password",
		ExtendedClientCapabilities: protocol.CLIENT_MYSQL | protocol.CLIENT_PLUGIN_AUTH,
	}
	authData, _ := auth.Marshal()
	conn.Write(authData)
	fmt.Println("发送认证")

	// 读取 OK
	okPkt := &protocol.OkPacket{}
	okPkt.Unmarshal(conn)
	fmt.Println("认证成功")

	// 测试1: PREPARE
	fmt.Println("\n=== 测试1: PREPARE ===")
	prepareReq := &protocol.ComStmtPreparePacket{
		Packet: protocol.Packet{
			SequenceID: 0,
		},
		Command: protocol.COM_STMT_PREPARE,
		Query:   "SELECT * FROM t WHERE id = ?",
	}
	prepareData, _ := prepareReq.Marshal()
	fmt.Printf("PREPARE 请求数据 (hex): %s\n", hex.EncodeToString(prepareData))
	fmt.Printf("PREPARE 请求数据长度: %d\n", len(prepareData))
	conn.Write(prepareData)

	// 读取 PREPARE 响应
	prepareResp := &protocol.StmtPrepareResponsePacket{}
	prepareResp.Unmarshal(conn)
	fmt.Printf("StatementID: %d\n", prepareResp.StatementID)
	fmt.Printf("ParamCount: %d\n", prepareResp.ParamCount)
	fmt.Printf("ColumnCount: %d\n", prepareResp.ColumnCount)

	// 测试2: EXECUTE - 单个参数
	fmt.Println("\n=== 测试2: EXECUTE - 单个整型参数 ===")
	executeReq := &protocol.ComStmtExecutePacket{
		Packet: protocol.Packet{
			SequenceID: 1,
		},
		Command:           protocol.COM_STMT_EXECUTE,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x00}, // 无 NULL
		NewParamsBindFlag: 1,
		ParamTypes: []protocol.StmtParamType{
			{Type: 0x03, Flag: 0}, // INT
		},
		ParamValues: []any{int32(123)},
	}

	executeData, _ := executeReq.Marshal()
	fmt.Printf("EXECUTE 请求数据 (hex): %s\n", hex.EncodeToString(executeData))
	fmt.Printf("EXECUTE 请求数据长度: %d\n", len(executeData))

	// 打印详细包结构
	fmt.Printf("  Header: %x\n", executeData[:4])
	fmt.Printf("  Payload: %x\n", executeData[4:])
	fmt.Printf("  Command: 0x%02x\n", executeData[4])
	fmt.Printf("  StatementID: %x\n", executeData[5:9])
	fmt.Printf("  Flags: 0x%02x\n", executeData[9])
	fmt.Printf("  IterationCount: %x\n", executeData[10:14])
	fmt.Printf("  NullBitmap: %x\n", executeData[14:15])
	fmt.Printf("  NewParamsBindFlag: 0x%02x\n", executeData[15])
	fmt.Printf("  ParamTypes: %x\n", executeData[16:18])
	fmt.Printf("  ParamValue: %x\n", executeData[18:])

	conn.Write(executeData)

	// 读取结果集
	// ColumnCount
	colCountPkt := &protocol.ColumnCountPacket{}
	colCountPkt.Unmarshal(conn)
	fmt.Printf("收到 ColumnCount: %d\n", colCountPkt.ColumnCount)

	// Column
	colPkt := &protocol.FieldMetaPacket{}
	colPkt.Unmarshal(conn, protocol.CLIENT_PROTOCOL_41)
	fmt.Printf("收到 Column: %s.%s.%s (type=0x%02x)\n",
		colPkt.Schema, colPkt.Table, colPkt.Name, colPkt.Type)

	// EOF
	eofPkt := &protocol.EOFPacket{}
	eofPkt.Unmarshal(conn)
	fmt.Printf("收到 EOF\n")

	// Row
	rowPkt := &protocol.Packet{}
	rowPkt.Unmarshal(conn)
	fmt.Printf("收到 Row 数据: %x\n", rowPkt.Payload)

	// EOF
	eofPkt2 := &protocol.EOFPacket{}
	eofPkt2.Unmarshal(conn)
	fmt.Printf("收到 EOF\n")

	// 测试3: EXECUTE - 多个参数
	fmt.Println("\n=== 测试3: EXECUTE - 多个参数 ===")
	executeReq2 := &protocol.ComStmtExecutePacket{
		Packet: protocol.Packet{
			SequenceID: 1,
		},
		Command:           protocol.COM_STMT_EXECUTE,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x00},
		NewParamsBindFlag: 1,
		ParamTypes: []protocol.StmtParamType{
			{Type: 0x03, Flag: 0}, // INT
			{Type: 0xfd, Flag: 0}, // VAR_STRING
		},
		ParamValues: []any{
			int32(456),
			"test",
		},
	}

	executeData2, _ := executeReq2.Marshal()
	fmt.Printf("EXECUTE 请求数据 (hex): %s\n", hex.EncodeToString(executeData2))
	fmt.Printf("EXECUTE 请求数据长度: %d\n", len(executeData2))
	conn.Write(executeData2)

	// 读取结果集（简单处理）
	_ = readPacket(conn) // ColumnCount
	_ = readPacket(conn) // Column
	_ = readPacket(conn) // EOF
	_ = readPacket(conn) // Row
	_ = readPacket(conn) // EOF
	fmt.Println("收到结果集")

	// 测试4: EXECUTE - 带 NULL 参数
	fmt.Println("\n=== 测试4: EXECUTE - 带 NULL 参数 ===")
	executeReq3 := &protocol.ComStmtExecutePacket{
		Packet: protocol.Packet{
			SequenceID: 1,
		},
		Command:           protocol.COM_STMT_EXECUTE,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x01}, // 第1个参数为 NULL (位2)
		NewParamsBindFlag: 1,
		ParamTypes: []protocol.StmtParamType{
			{Type: 0xfd, Flag: 0}, // VAR_STRING
		},
		ParamValues: []any{nil},
	}

	executeData3, _ := executeReq3.Marshal()
	fmt.Printf("EXECUTE 请求数据 (hex): %s\n", hex.EncodeToString(executeData3))
	fmt.Printf("EXECUTE 请求数据长度: %d\n", len(executeData3))
	conn.Write(executeData3)

	// 读取结果集（简单处理）
	_ = readPacket(conn) // ColumnCount
	_ = readPacket(conn) // Column
	_ = readPacket(conn) // EOF
	_ = readPacket(conn) // Row
	_ = readPacket(conn) // EOF
	fmt.Println("收到结果集")

	fmt.Println("\n=== 测试完成 ===")
}

func readPacket(conn net.Conn) *protocol.Packet {
	pkt := &protocol.Packet{}
	pkt.Unmarshal(conn)
	return pkt
}
