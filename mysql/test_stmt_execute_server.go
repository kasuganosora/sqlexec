package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"mysql-proxy/mysql/protocol"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":13307")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	fmt.Println("测试服务器监听在 :13307")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("\n=== 新连接 ===")

	// 发送握手包
	handshake := &protocol.HandshakeResponse{}
	handshakeData := handshake.GetDefaultHandshake()
	conn.Write(handshakeData)
	fmt.Println("发送握手包")

	// 读取客户端认证
	authPacket := &protocol.HandshakeResponse{}
	authPacket.Unmarshal(conn)
	fmt.Printf("收到认证包: capabilities=0x%08x, username=%s\n",
		authPacket.ClientCapabilities, authPacket.Username)

	// 发送 OK 包
	okPacket := &protocol.OkPacket{}
	okData, _ := okPacket.Marshal()
	conn.Write(okData)
	fmt.Println("发送 OK 包")

	// 读取命令
	for {
		pkt := &protocol.Packet{}
		err := pkt.Unmarshal(conn)
		if err != nil {
			fmt.Println("读取包错误:", err)
			break
		}

		if len(pkt.Payload) == 0 {
			continue
		}

		command := pkt.Payload[0]
		fmt.Printf("\n收到命令: 0x%02x\n", command)

		switch command {
		case protocol.COM_STMT_PREPARE:
			fmt.Println("收到 COM_STMT_PREPARE")
			prepareReq := &protocol.ComStmtPreparePacket{}
			prepareReq.Unmarshal(bytes.NewReader(pkt.Payload))
			fmt.Printf("Query: %s\n", prepareReq.Query)

			// 返回 Prepare 响应
			resp := &protocol.StmtPrepareResponsePacket{
				Packet: protocol.Packet{
					SequenceID: pkt.SequenceID + 1,
				},
				StatementID:  1,
				ColumnCount:   1,
				ParamCount:    1,
				Reserved:      0,
				WarningCount:  0,
			}
			respData, _ := resp.Marshal()
			conn.Write(respData)
			fmt.Println("发送 PREPARE 响应")

		case protocol.COM_STMT_EXECUTE:
			fmt.Println("收到 COM_STMT_EXECUTE")
			fmt.Printf("原始包数据 (hex): %s\n", hex.EncodeToString(pkt.Payload))

			// 直接解析 Packet Payload
			executePkt := &protocol.ComStmtExecutePacket{}
			err := executePkt.Unmarshal(bytes.NewReader(pkt.Payload))
			if err != nil {
				fmt.Printf("解析 COM_STMT_EXECUTE 错误: %v\n", err)
				// 发送错误包
				errPkt := &protocol.ErrPacket{}
				errPkt.SequenceID = pkt.SequenceID + 1
				errPkt.ErrorCode = 1064
				errPkt.SQLState = "42000"
				errPkt.ErrorMessage = err.Error()
				errData, _ := errPkt.Marshal()
				conn.Write(errData)
				continue
			}

			fmt.Printf("StatementID: %d\n", executePkt.StatementID)
			fmt.Printf("Flags: 0x%02x\n", executePkt.Flags)
			fmt.Printf("IterationCount: %d\n", executePkt.IterationCount)
			fmt.Printf("NullBitmap: %v\n", executePkt.NullBitmap)
			fmt.Printf("NewParamsBindFlag: %d\n", executePkt.NewParamsBindFlag)
			fmt.Printf("ParamTypes 数量: %d\n", len(executePkt.ParamTypes))
			for i, pt := range executePkt.ParamTypes {
				fmt.Printf("  ParamTypes[%d]: Type=0x%02x, Flag=0x%02x\n", i, pt.Type, pt.Flag)
			}
			fmt.Printf("ParamValues 数量: %d\n", len(executePkt.ParamValues))
			for i, pv := range executePkt.ParamValues {
				fmt.Printf("  ParamValues[%d]: %v (%T)\n", i, pv, pv)
			}

			// 返回结果集
			// ColumnCount
			colCountPkt := &protocol.ColumnCountPacket{
				Packet: protocol.Packet{
					SequenceID: pkt.SequenceID + 1,
				},
				ColumnCount: 1,
			}
			colCountData, _ := colCountPkt.Marshal()
			conn.Write(colCountData)

			// Column 定义
			col := &protocol.FieldMetaPacket{
				Packet: protocol.Packet{
					SequenceID: pkt.SequenceID + 2,
				},
				Catalog:                   "def",
				Schema:                    "test",
				Table:                     "t",
				OrgTable:                  "t",
				Name:                      "id",
				OrgName:                   "id",
				LengthOfFixedLengthFields: 12,
				CharacterSet:              33,
				ColumnLength:              11,
				Type:                      0x03, // INT
				Flags:                     0x81,
				Decimals:                  0,
				Reserved:                  "\x00\x00",
			}
			colData, _ := col.Marshal()
			conn.Write(colData)

			// EOF
			eofPkt := &protocol.EOFPacket{}
			eofPkt.SequenceID = pkt.SequenceID + 3
			eofData, _ := eofPkt.Marshal()
			conn.Write(eofData)

			// Result row
			resultPkt := &protocol.Packet{
				Payload:      []byte{0x01, 0x00, 0x00, 0x01}, // 值为 1
				SequenceID:   pkt.SequenceID + 4,
			}
			resultData, _ := resultPkt.Marshal()
			conn.Write(resultData)

			// EOF
			eofPkt2 := &protocol.EOFPacket{}
			eofPkt2.SequenceID = pkt.SequenceID + 5
			eofData2, _ := eofPkt2.Marshal()
			conn.Write(eofData2)
			fmt.Println("发送结果集")

		default:
			fmt.Printf("未处理的命令: 0x%02x\n", command)
		}
	}
}
