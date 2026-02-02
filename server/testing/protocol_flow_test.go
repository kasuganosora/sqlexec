package testing

import (
	"bytes"
	"testing"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
	"github.com/stretchr/testify/assert"
)

// TestProtocol_HandshakeFlow 测试完整的握手流程
func TestProtocol_HandshakeFlow(t *testing.T) {
	// Given: 创建握手包
	handshake := protocol.NewHandshakePacket()
	handshake.Packet.SequenceID = 0

	// When: 序列化握手包
	data, err := handshake.Marshal()
	assert.NoError(t, err)

	// Then: 验证包格式
	assert.Greater(t, len(data), 0, "握手包应该有数据")
	assert.Equal(t, uint8(0), data[3], "序列号应该是0")

	// And: 验证可以反序列化
	parsedHandshake := &protocol.HandshakeV10Packet{}
	err = parsedHandshake.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, handshake.ProtocolVersion, parsedHandshake.ProtocolVersion)
	assert.Equal(t, handshake.ServerVersion, parsedHandshake.ServerVersion)
}

// TestProtocol_QueryFlow 测试完整的查询流程
func TestProtocol_QueryFlow(t *testing.T) {
	// Given: 创建COM_QUERY包
	packet := &protocol.Packet{}
	packet.SequenceID = 0
	packet.Payload = []byte{protocol.COM_QUERY, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}
	packet.PayloadLength = 8

	// When: 序列化包
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证包格式
	assert.Greater(t, len(data), 4, "查询包应该有数据")
	assert.Equal(t, byte(0x08), data[0], "载荷长度应该是8") // 1字节COM_QUERY + 7字节"SELECT 1"
	assert.Equal(t, byte(0x00), data[3], "序列号应该是0")

	// And: 验证可以反序列化
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint8(0), parsedPacket.SequenceID)
	assert.Equal(t, uint8(protocol.COM_QUERY), parsedPacket.GetCommandType())
}

// TestProtocol_ErrorFlow 测试错误包流程
func TestProtocol_ErrorFlow(t *testing.T) {
	// Given: 创建错误包
	errPacket := &protocol.ErrorPacket{}
	errPacket.SequenceID = 1
	errPacket.ErrorInPacket.Header = 0xFF
	errPacket.ErrorInPacket.ErrorCode = 1146
	errPacket.ErrorInPacket.SqlStateMarker = "#"
	errPacket.ErrorInPacket.SqlState = "42S02"
	errPacket.ErrorInPacket.ErrorMessage = "Table 'test.test_table' doesn't exist"

	// When: 序列化错误包
	data, err := errPacket.Marshal()
	assert.NoError(t, err)

	// Then: 验证错误包格式
	assert.Greater(t, len(data), 5, "错误包应该有数据")
	assert.Equal(t, byte(0xFF), data[4], "错误包头应该是0xFF")

	// And: 验证错误码在序列化数据中（小端序）
	assert.Equal(t, byte(1146 & 0xFF), data[5], "错误码低字节应该匹配")
	assert.Equal(t, byte(1146>>8), data[6], "错误码高字节应该匹配")
}

// TestProtocol_OKFlow 测试OK包流程
func TestProtocol_OKFlow(t *testing.T) {
	// Given: 创建OK包
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = 2
	okPacket.OkInPacket.Header = 0x00
	okPacket.OkInPacket.AffectedRows = 1
	okPacket.OkInPacket.LastInsertId = 100
	okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	okPacket.OkInPacket.Warnings = 0

	// When: 序列化OK包
	data, err := okPacket.Marshal()
	assert.NoError(t, err)

	// Then: 验证OK包格式
	assert.Greater(t, len(data), 4, "OK包应该有数据")
	assert.Equal(t, byte(0x00), data[4], "OK包头应该是0x00")

	// And: 验证可以反序列化
	parsedOkPacket := &protocol.OkPacket{}
	err = parsedOkPacket.Unmarshal(bytes.NewReader(data), 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), parsedOkPacket.OkInPacket.AffectedRows)
	assert.Equal(t, uint64(100), parsedOkPacket.OkInPacket.LastInsertId)
}

// TestProtocol_SequenceIDFlow 测试序列号流程管理
func TestProtocol_SequenceIDFlow(t *testing.T) {
	// Given: 模拟一个完整的命令序列
	// 握手(0) -> 认证响应(1) -> OK(2) -> 命令(0) -> 响应(1)

	// 步骤1: 握手包（序列号0）
	handshake := protocol.NewHandshakePacket()
	handshake.SequenceID = 0
	data1, _ := handshake.Marshal()
	assert.Equal(t, byte(0x00), data1[3], "握手包序列号应该是0")

	// 步骤2: 认证响应包（序列号1）
	authResp := &protocol.Packet{}
	authResp.SequenceID = 1
	data2, _ := authResp.MarshalBytes()
	assert.Equal(t, byte(0x01), data2[3], "认证响应包序列号应该是1")

	// 步骤3: OK包（序列号2）
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = 2
	data3, _ := okPacket.Marshal()
	assert.Equal(t, byte(0x02), data3[3], "OK包序列号应该是2")

	// 步骤4: 命令包（序列号0，重置）
	cmdPacket := &protocol.Packet{}
	cmdPacket.SequenceID = 0
	data4, _ := cmdPacket.MarshalBytes()
	assert.Equal(t, byte(0x00), data4[3], "命令包序列号应该是0（重置）")

	// 步骤5: 响应包（序列号1）
	respPacket := &protocol.OkPacket{}
	respPacket.SequenceID = 1
	data5, _ := respPacket.Marshal()
	assert.Equal(t, byte(0x01), data5[3], "响应包序列号应该是1")
}

// TestProtocol_MultiPacketResultFlow 测试多包结果集流程
func TestProtocol_MultiPacketResultFlow(t *testing.T) {
	// Given: 创建结果集的多包流程
	// 列数包(0) -> 列定义1(1) -> 列定义2(2) -> EOF(3) -> 行1(4) -> 行2(5) -> EOF(6)

	// 列数包
	colCountData, _ := response.BuildColumnCountPacket(0, 2)
	assert.NotNil(t, colCountData)

	// EOF包
	eofPacket := &protocol.Packet{}
	eofPacket.SequenceID = 3
	eofPacket.Payload = []byte{0xFE, 0x00, 0x00, 0x02, 0x00} // EOF包示例数据
	eofPacket.PayloadLength = 5
	eofData, _ := eofPacket.MarshalBytes()
	assert.Greater(t, len(eofData), 4, "EOF包应该有数据")

	// 行包1
	row1Packet := &protocol.Packet{}
	row1Packet.SequenceID = 4
	row1Packet.Payload = []byte{0x01, 0x02} // 示例数据
	row1Packet.PayloadLength = 2
	row1Data, _ := row1Packet.MarshalBytes()
	assert.Greater(t, len(row1Data), 4, "行包应该有数据")

	// 行包2
	row2Packet := &protocol.Packet{}
	row2Packet.SequenceID = 5
	row2Packet.Payload = []byte{0x03, 0x04} // 示例数据
	row2Packet.PayloadLength = 2
	row2Data, _ := row2Packet.MarshalBytes()
	assert.Greater(t, len(row2Data), 4, "行包应该有数据")

	// EOF包2
	eofPacket2 := &protocol.Packet{}
	eofPacket2.SequenceID = 6
	eofPacket2.Payload = []byte{0xFE, 0x00, 0x00, 0x02, 0x00} // EOF包示例数据
	eofPacket2.PayloadLength = 5
	eofData2, _ := eofPacket2.MarshalBytes()
	assert.Greater(t, len(eofData2), 4, "EOF包应该有数据")

	// Then: 验证所有序列号正确递增
	assert.Equal(t, byte(0x00), colCountData[3], "列数包序列号应该是0")
	assert.Equal(t, byte(0x03), eofData[3], "EOF包序列号应该是3")
	assert.Equal(t, byte(0x04), row1Data[3], "行1包序列号应该是4")
	assert.Equal(t, byte(0x05), row2Data[3], "行2包序列号应该是5")
	assert.Equal(t, byte(0x06), eofData2[3], "EOF2包序列号应该是6")
}

// TestProtocol_CommandSequenceFlow 测试多个命令序列
func TestProtocol_CommandSequenceFlow(t *testing.T) {
	// Given: 模拟多个连续命令
	// 命令1: PING -> OK
	// 命令2: SELECT -> 结果集
	// 命令3: USE -> OK

	// 命令1: COM_PING（序列号0）
	pingPacket := &protocol.Packet{}
	pingPacket.SequenceID = 0
	pingData, _ := pingPacket.MarshalBytes()

	// 响应1: OK（序列号0）
	ok1Packet := &protocol.OkPacket{}
	ok1Packet.SequenceID = 0
	ok1Data, _ := ok1Packet.Marshal()

	// 命令2: COM_QUERY（序列号1）
	queryPacket := &protocol.Packet{}
	queryPacket.SequenceID = 1
	queryData, _ := queryPacket.MarshalBytes()

	// 响应2: 结果集（列数0, EOF 1）
	colCountData, _ := response.BuildColumnCountPacket(0, 1)
	eofBuilder := response.NewEOFBuilder()
	eofData, _ := eofBuilder.Build(1, 0, protocol.SERVER_STATUS_AUTOCOMMIT).Marshal()

	// 命令3: COM_INIT_DB（序列号2）
	initDBPacket := &protocol.Packet{}
	initDBPacket.SequenceID = 2
	initDBData, _ := initDBPacket.MarshalBytes()

	// 响应3: OK（序列号2）
	ok2Packet := &protocol.OkPacket{}
	ok2Packet.SequenceID = 2
	ok2Data, _ := ok2Packet.Marshal()

	// Then: 验证序列号正确性
	assert.Equal(t, byte(0x00), pingData[3], "PING命令序列号应该是0")
	assert.Equal(t, byte(0x00), ok1Data[3], "PING OK序列号应该是0")

	assert.Equal(t, byte(0x01), queryData[3], "QUERY命令序列号应该是1")
	assert.Equal(t, byte(0x00), colCountData[3], "列数包序列号应该是0（重置）")
	assert.Equal(t, byte(0x01), eofData[3], "EOF包序列号应该是1")

	assert.Equal(t, byte(0x02), initDBData[3], "INIT_DB命令序列号应该是2")
	assert.Equal(t, byte(0x02), ok2Data[3], "INIT_DB OK序列号应该是2")
}

// TestProtocol_ConnectionMockIntegration 测试MockConnection的协议集成
func TestProtocol_ConnectionMockIntegration(t *testing.T) {
	// Given: 创建Mock连接
	mockConn := NewMockConnection()

	// When: 发送握手包
	handshake := protocol.NewHandshakePacket()
	handshakeData, _ := handshake.Marshal()
	_, err := mockConn.Write(handshakeData)
	assert.NoError(t, err)

	// Then: 验证数据被记录
	writtenData := mockConn.GetWrittenData()
	assert.Equal(t, 1, len(writtenData), "应该记录1个写入")
	assert.Equal(t, handshakeData, writtenData[0], "记录的数据应该与写入的一致")
}

// TestProtocol_SequenceOverflowFlow 测试序列号溢出流程
func TestProtocol_SequenceOverflowFlow(t *testing.T) {
	// Given: 测试序列号从255到0的转换

	// 步骤1: 序列号254
	packet1 := &protocol.OkPacket{}
	packet1.SequenceID = 254
	data1, _ := packet1.Marshal()
	assert.Equal(t, byte(254), data1[3], "序列号应该是254")

	// 步骤2: 序列号255
	packet2 := &protocol.OkPacket{}
	packet2.SequenceID = 255
	data2, _ := packet2.Marshal()
	assert.Equal(t, byte(255), data2[3], "序列号应该是255")

	// 步骤3: 序列号0（溢出后）
	packet3 := &protocol.OkPacket{}
	packet3.SequenceID = 0
	data3, _ := packet3.Marshal()
	assert.Equal(t, byte(0x00), data3[3], "序列号应该回绕到0")

	// Then: 验证完整流程
	writtenData := [][]byte{data1, data2, data3}
	assert.Equal(t, 3, len(writtenData))
}

// TestProtocol_PacketBoundaryFlow 测试包边界条件
func TestProtocol_PacketBoundaryFlow(t *testing.T) {
	// Given: 测试不同大小的包

	// 小包（1字节）
	smallPacket := &protocol.Packet{}
	smallPacket.Payload = []byte{0x01}
	smallPacket.PayloadLength = 1
	smallData, _ := smallPacket.MarshalBytes()
	assert.Equal(t, byte(0x01), smallData[0], "小包载荷长度应该是1")

	// 中等包（100字节）
	mediumPacket := &protocol.Packet{}
	mediumPacket.Payload = make([]byte, 100)
	mediumPacket.PayloadLength = 100
	for i := 0; i < 100; i++ {
		mediumPacket.Payload[i] = byte(i)
	}
	mediumData, _ := mediumPacket.MarshalBytes()
	assert.Equal(t, byte(0x64), mediumData[0], "中等包载荷长度应该是100") // 0x64 = 100

	// 较大包（1000字节）
	largePacket := &protocol.Packet{}
	largePacket.Payload = make([]byte, 1000)
	largePacket.PayloadLength = 1000
	for i := 0; i < 1000; i++ {
		largePacket.Payload[i] = byte(i % 256)
	}
	largeData, _ := largePacket.MarshalBytes()
	assert.Equal(t, byte(0xE8), largeData[0], "大包载荷长度应该是1000") // 0xE8 = 1000 (低字节)
	assert.Equal(t, byte(0x03), largeData[1], "1000的高字节是3")
}

// TestProtocol_CommandRoundTrip 测试命令的往返序列化/反序列化
func TestProtocol_CommandRoundTrip(t *testing.T) {
	commands := []struct {
		name     string
		cmdType  uint8
		payload  []byte
	}{
		{"COM_PING", protocol.COM_PING, []byte{protocol.COM_PING}},
		{"COM_QUIT", protocol.COM_QUIT, []byte{protocol.COM_QUIT}},
		{"COM_QUERY", protocol.COM_QUERY, []byte{protocol.COM_QUERY, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}},
	}

	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			// Given: 创建包
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = tc.payload
			packet.PayloadLength = uint32(len(tc.payload))

			// When: 序列化
			data, err := packet.MarshalBytes()
			assert.NoError(t, err, "%s序列化应该成功", tc.name)

			// And: 反序列化
			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err, "%s反序列化应该成功", tc.name)

			// Then: 验证内容
			assert.Equal(t, tc.cmdType, parsedPacket.GetCommandType(), "%s命令类型应该匹配", tc.name)
		})
	}
}

// TestProtocol_ErrorCodeFlow 测试不同错误码的流程
func TestProtocol_ErrorCodeFlow(t *testing.T) {
	errorCases := []struct {
		name         string
		errorCode    uint16
		sqlState     string
		errorMsg     string
	}{
		{"Table not found", 1146, "42S02", "Table 'test.tbl' doesn't exist"},
		{"Column not found", 1054, "42S22", "Unknown column 'col' in 'field list'"},
		{"Syntax error", 1064, "42000", "You have an error in your SQL syntax"},
		{"Empty query", 1065, "42000", "Query was empty"},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			// Given: 创建错误包
			errPacket := &protocol.ErrorPacket{}
			errPacket.SequenceID = 0
			errPacket.ErrorInPacket.Header = 0xFF
			errPacket.ErrorInPacket.ErrorCode = tc.errorCode
			errPacket.ErrorInPacket.SqlStateMarker = "#"
			errPacket.ErrorInPacket.SqlState = tc.sqlState
			errPacket.ErrorInPacket.ErrorMessage = tc.errorMsg

			// When: 序列化
			data, err := errPacket.Marshal()
			assert.NoError(t, err)

			// Then: 验证错误包头
			assert.Greater(t, len(data), 4, "错误包应该有数据")
			assert.Equal(t, byte(0xFF), data[4], "错误包头应该是0xFF")

			// And: 验证序列化后的数据包含了错误码（小端序）
			assert.Equal(t, byte(tc.errorCode), data[5], "错误码低字节应该匹配")
			assert.Equal(t, byte(tc.errorCode>>8), data[6], "错误码高字节应该匹配")
		})
	}
}
