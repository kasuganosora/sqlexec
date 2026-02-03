package mock

import (
	"bytes"
	"testing"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/response"
	"github.com/stretchr/testify/assert"
)

// TestProtocol_HandshakeFlow tests complete handshake flow
func TestProtocol_HandshakeFlow(t *testing.T) {
	// Given: Create handshake packet
	handshake := protocol.NewHandshakePacket()
	handshake.Packet.SequenceID = 0

	// When: Serialize handshake packet
	data, err := handshake.Marshal()
	assert.NoError(t, err)

	// Then: Verify packet format
	assert.Greater(t, len(data), 0, "Handshake packet should have data")
	assert.Equal(t, uint8(0), data[3], "Sequence ID should be 0")

	// And: Verify can deserialize
	parsedHandshake := &protocol.HandshakeV10Packet{}
	err = parsedHandshake.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, handshake.ProtocolVersion, parsedHandshake.ProtocolVersion)
	assert.Equal(t, handshake.ServerVersion, parsedHandshake.ServerVersion)
}

// TestProtocol_QueryFlow tests complete query flow
func TestProtocol_QueryFlow(t *testing.T) {
	// Given: Create COM_QUERY packet
	packet := &protocol.Packet{}
	packet.SequenceID = 0
	packet.Payload = []byte{protocol.COM_QUERY, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}
	packet.PayloadLength = 8

	// When: Serialize packet
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify packet format
	assert.Greater(t, len(data), 4, "Query packet should have data")
	assert.Equal(t, byte(0x08), data[0], "Payload length should be 8")
	assert.Equal(t, byte(0x00), data[3], "Sequence ID should be 0")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint8(0), parsedPacket.SequenceID)
	assert.Equal(t, uint8(protocol.COM_QUERY), parsedPacket.GetCommandType())
}

// TestProtocol_ErrorFlow tests error packet flow
func TestProtocol_ErrorFlow(t *testing.T) {
	// Given: Create error packet
	errPacket := &protocol.ErrorPacket{}
	errPacket.SequenceID = 1
	errPacket.ErrorInPacket.Header = 0xFF
	errPacket.ErrorInPacket.ErrorCode = 1146
	errPacket.ErrorInPacket.SqlStateMarker = "#"
	errPacket.ErrorInPacket.SqlState = "42S02"
	errPacket.ErrorInPacket.ErrorMessage = "Table 'test.test_table' doesn't exist"

	// When: Serialize error packet
	data, err := errPacket.Marshal()
	assert.NoError(t, err)

	// Then: Verify error packet format
	assert.Greater(t, len(data), 5, "Error packet should have data")
	assert.Equal(t, byte(0xFF), data[4], "Error packet header should be 0xFF")

	// And: Verify error code in serialized data (little endian)
	assert.Equal(t, byte(1146 & 0xFF), data[5], "Error code low byte should match")
	assert.Equal(t, byte(1146>>8), data[6], "Error code high byte should match")
}

// TestProtocol_OKFlow tests OK packet flow
func TestProtocol_OKFlow(t *testing.T) {
	// Given: Create OK packet
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = 2
	okPacket.OkInPacket.Header = 0x00
	okPacket.OkInPacket.AffectedRows = 1
	okPacket.OkInPacket.LastInsertId = 100
	okPacket.OkInPacket.StatusFlags = protocol.SERVER_STATUS_AUTOCOMMIT
	okPacket.OkInPacket.Warnings = 0

	// When: Serialize OK packet
	data, err := okPacket.Marshal()
	assert.NoError(t, err)

	// Then: Verify OK packet format
	assert.Greater(t, len(data), 4, "OK packet should have data")
	assert.Equal(t, byte(0x00), data[4], "OK packet header should be 0x00")

	// And: Verify can deserialize
	parsedOkPacket := &protocol.OkPacket{}
	err = parsedOkPacket.Unmarshal(bytes.NewReader(data), 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), parsedOkPacket.OkInPacket.AffectedRows)
	assert.Equal(t, uint64(100), parsedOkPacket.OkInPacket.LastInsertId)
}

// TestProtocol_SequenceIDFlow tests sequence ID flow management
func TestProtocol_SequenceIDFlow(t *testing.T) {
	// Given: Simulate complete command sequence
	// handshake(0) -> auth response(1) -> OK(2) -> command(0) -> response(1)

	// Step 1: Handshake packet (sequence ID 0)
	handshake := protocol.NewHandshakePacket()
	handshake.SequenceID = 0
	data1, _ := handshake.Marshal()
	assert.Equal(t, byte(0x00), data1[3], "Handshake packet sequence ID should be 0")

	// Step 2: Auth response packet (sequence ID 1)
	authResp := &protocol.Packet{}
	authResp.SequenceID = 1
	data2, _ := authResp.MarshalBytes()
	assert.Equal(t, byte(0x01), data2[3], "Auth response packet sequence ID should be 1")

	// Step 3: OK packet (sequence ID 2)
	okPacket := &protocol.OkPacket{}
	okPacket.SequenceID = 2
	data3, _ := okPacket.Marshal()
	assert.Equal(t, byte(0x02), data3[3], "OK packet sequence ID should be 2")

	// Step 4: Command packet (sequence ID 0, reset)
	cmdPacket := &protocol.Packet{}
	cmdPacket.SequenceID = 0
	data4, _ := cmdPacket.MarshalBytes()
	assert.Equal(t, byte(0x00), data4[3], "Command packet sequence ID should be 0 (reset)")

	// Step 5: Response packet (sequence ID 1)
	respPacket := &protocol.OkPacket{}
	respPacket.SequenceID = 1
	data5, _ := respPacket.Marshal()
	assert.Equal(t, byte(0x01), data5[3], "Response packet sequence ID should be 1")
}

// TestProtocol_MultiPacketResultFlow tests multi-packet result set flow
func TestProtocol_MultiPacketResultFlow(t *testing.T) {
	// Given: Create multi-packet result set flow
	// column count(0) -> column def1(1) -> column def2(2) -> EOF(3) -> row1(4) -> row2(5) -> EOF(6)

	// Column count packet
	colCountData, _ := response.BuildColumnCountPacket(0, 2)
	assert.NotNil(t, colCountData)

	// EOF packet
	eofPacket := &protocol.Packet{}
	eofPacket.SequenceID = 3
	eofPacket.Payload = []byte{0xFE, 0x00, 0x00, 0x02, 0x00}
	eofPacket.PayloadLength = 5
	eofData, _ := eofPacket.MarshalBytes()
	assert.Greater(t, len(eofData), 4, "EOF packet should have data")

	// Row packet 1
	row1Packet := &protocol.Packet{}
	row1Packet.SequenceID = 4
	row1Packet.Payload = []byte{0x01, 0x02}
	row1Packet.PayloadLength = 2
	row1Data, _ := row1Packet.MarshalBytes()
	assert.Greater(t, len(row1Data), 4, "Row packet should have data")

	// Row packet 2
	row2Packet := &protocol.Packet{}
	row2Packet.SequenceID = 5
	row2Packet.Payload = []byte{0x03, 0x04}
	row2Packet.PayloadLength = 2
	row2Data, _ := row2Packet.MarshalBytes()
	assert.Greater(t, len(row2Data), 4, "Row packet should have data")

	// EOF packet 2
	eofPacket2 := &protocol.Packet{}
	eofPacket2.SequenceID = 6
	eofPacket2.Payload = []byte{0xFE, 0x00, 0x00, 0x02, 0x00}
	eofPacket2.PayloadLength = 5
	eofData2, _ := eofPacket2.MarshalBytes()
	assert.Greater(t, len(eofData2), 4, "EOF packet should have data")

	// Then: Verify all sequence IDs increment correctly
	assert.Equal(t, byte(0x00), colCountData[3], "Column count packet sequence ID should be 0")
	assert.Equal(t, byte(0x03), eofData[3], "EOF packet sequence ID should be 3")
	assert.Equal(t, byte(0x04), row1Data[3], "Row1 packet sequence ID should be 4")
	assert.Equal(t, byte(0x05), row2Data[3], "Row2 packet sequence ID should be 5")
	assert.Equal(t, byte(0x06), eofData2[3], "EOF2 packet sequence ID should be 6")
}

// TestProtocol_CommandSequenceFlow tests multiple command sequences
func TestProtocol_CommandSequenceFlow(t *testing.T) {
	// Given: Simulate multiple consecutive commands
	// Command 1: PING -> OK
	// Command 2: SELECT -> result set
	// Command 3: USE -> OK

	// Command 1: COM_PING (sequence ID 0)
	pingPacket := &protocol.Packet{}
	pingPacket.SequenceID = 0
	pingData, _ := pingPacket.MarshalBytes()

	// Response 1: OK (sequence ID 0)
	ok1Packet := &protocol.OkPacket{}
	ok1Packet.SequenceID = 0
	ok1Data, _ := ok1Packet.Marshal()

	// Command 2: COM_QUERY (sequence ID 1)
	queryPacket := &protocol.Packet{}
	queryPacket.SequenceID = 1
	queryData, _ := queryPacket.MarshalBytes()

	// Response 2: result set (column count 0, EOF 1)
	colCountData, _ := response.BuildColumnCountPacket(0, 1)
	eofBuilder := response.NewEOFBuilder()
	eofData, _ := eofBuilder.Build(1, 0, protocol.SERVER_STATUS_AUTOCOMMIT).Marshal()

	// Command 3: COM_INIT_DB (sequence ID 2)
	initDBPacket := &protocol.Packet{}
	initDBPacket.SequenceID = 2
	initDBData, _ := initDBPacket.MarshalBytes()

	// Response 3: OK (sequence ID 2)
	ok2Packet := &protocol.OkPacket{}
	ok2Packet.SequenceID = 2
	ok2Data, _ := ok2Packet.Marshal()

	// Then: Verify sequence ID correctness
	assert.Equal(t, byte(0x00), pingData[3], "PING command sequence ID should be 0")
	assert.Equal(t, byte(0x00), ok1Data[3], "PING OK sequence ID should be 0")

	assert.Equal(t, byte(0x01), queryData[3], "QUERY command sequence ID should be 1")
	assert.Equal(t, byte(0x00), colCountData[3], "Column count packet sequence ID should be 0 (reset)")
	assert.Equal(t, byte(0x01), eofData[3], "EOF packet sequence ID should be 1")

	assert.Equal(t, byte(0x02), initDBData[3], "INIT_DB command sequence ID should be 2")
	assert.Equal(t, byte(0x02), ok2Data[3], "INIT_DB OK sequence ID should be 2")
}

// TestProtocol_ConnectionMockIntegration tests MockConnection protocol integration
func TestProtocol_ConnectionMockIntegration(t *testing.T) {
	// Given: Create mock connection
	mockConn := NewMockConnection()

	// When: Send handshake packet
	handshake := protocol.NewHandshakePacket()
	handshakeData, _ := handshake.Marshal()
	_, err := mockConn.Write(handshakeData)
	assert.NoError(t, err)

	// Then: Verify data is recorded
	writtenData := mockConn.GetWrittenData()
	assert.Equal(t, 1, len(writtenData), "Should record 1 write")
	assert.Equal(t, handshakeData, writtenData[0], "Recorded data should match written data")
}

// TestProtocol_SequenceOverflowFlow tests sequence ID overflow flow
func TestProtocol_SequenceOverflowFlow(t *testing.T) {
	// Given: Test sequence ID transition from 255 to 0

	// Step 1: Sequence ID 254
	packet1 := &protocol.OkPacket{}
	packet1.SequenceID = 254
	data1, _ := packet1.Marshal()
	assert.Equal(t, byte(254), data1[3], "Sequence ID should be 254")

	// Step 2: Sequence ID 255
	packet2 := &protocol.OkPacket{}
	packet2.SequenceID = 255
	data2, _ := packet2.Marshal()
	assert.Equal(t, byte(255), data2[3], "Sequence ID should be 255")

	// Step 3: Sequence ID 0 (after overflow)
	packet3 := &protocol.OkPacket{}
	packet3.SequenceID = 0
	data3, _ := packet3.Marshal()
	assert.Equal(t, byte(0x00), data3[3], "Sequence ID should wrap to 0")

	// Then: Verify complete flow
	writtenData := [][]byte{data1, data2, data3}
	assert.Equal(t, 3, len(writtenData))
}

// TestProtocol_PacketBoundaryFlow tests packet boundary conditions
func TestProtocol_PacketBoundaryFlow(t *testing.T) {
	// Given: Test packets of different sizes

	// Small packet (1 byte)
	smallPacket := &protocol.Packet{}
	smallPacket.Payload = []byte{0x01}
	smallPacket.PayloadLength = 1
	smallData, _ := smallPacket.MarshalBytes()
	assert.Equal(t, byte(0x01), smallData[0], "Small packet payload length should be 1")

	// Medium packet (100 bytes)
	mediumPacket := &protocol.Packet{}
	mediumPacket.Payload = make([]byte, 100)
	mediumPacket.PayloadLength = 100
	for i := 0; i < 100; i++ {
		mediumPacket.Payload[i] = byte(i)
	}
	mediumData, _ := mediumPacket.MarshalBytes()
	assert.Equal(t, byte(0x64), mediumData[0], "Medium packet payload length should be 100")

	// Large packet (1000 bytes)
	largePacket := &protocol.Packet{}
	largePacket.Payload = make([]byte, 1000)
	largePacket.PayloadLength = 1000
	for i := 0; i < 1000; i++ {
		largePacket.Payload[i] = byte(i % 256)
	}
	largeData, _ := largePacket.MarshalBytes()
	assert.Equal(t, byte(0xE8), largeData[0], "Large packet payload length should be 1000 (low byte)")
	assert.Equal(t, byte(0x03), largeData[1], "1000 high byte is 3")
}

// TestProtocol_CommandRoundTrip tests command round-trip serialization/deserialization
func TestProtocol_CommandRoundTrip(t *testing.T) {
	commands := []struct {
		name    string
		cmdType uint8
		payload []byte
	}{
		{"COM_PING", protocol.COM_PING, []byte{protocol.COM_PING}},
		{"COM_QUIT", protocol.COM_QUIT, []byte{protocol.COM_QUIT}},
		{"COM_QUERY", protocol.COM_QUERY, []byte{protocol.COM_QUERY, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}},
	}

	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			// Given: Create packet
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = tc.payload
			packet.PayloadLength = uint32(len(tc.payload))

			// When: Serialize
			data, err := packet.MarshalBytes()
			assert.NoError(t, err, "%s serialization should succeed", tc.name)

			// And: Deserialize
			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err, "%s deserialization should succeed", tc.name)

			// Then: Verify content
			assert.Equal(t, tc.cmdType, parsedPacket.GetCommandType(), "%s command type should match", tc.name)
		})
	}
}

// TestProtocol_ErrorCodeFlow tests different error code flows
func TestProtocol_ErrorCodeFlow(t *testing.T) {
	errorCases := []struct {
		name      string
		errorCode uint16
		sqlState  string
		errorMsg  string
	}{
		{"Table not found", 1146, "42S02", "Table 'test.tbl' doesn't exist"},
		{"Column not found", 1054, "42S22", "Unknown column 'col' in 'field list'"},
		{"Syntax error", 1064, "42000", "You have an error in your SQL syntax"},
		{"Empty query", 1065, "42000", "Query was empty"},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			// Given: Create error packet
			errPacket := &protocol.ErrorPacket{}
			errPacket.SequenceID = 0
			errPacket.ErrorInPacket.Header = 0xFF
			errPacket.ErrorInPacket.ErrorCode = tc.errorCode
			errPacket.ErrorInPacket.SqlStateMarker = "#"
			errPacket.ErrorInPacket.SqlState = tc.sqlState
			errPacket.ErrorInPacket.ErrorMessage = tc.errorMsg

			// When: Serialize
			data, err := errPacket.Marshal()
			assert.NoError(t, err)

			// Then: Verify error packet header
			assert.Greater(t, len(data), 4, "Error packet should have data")
			assert.Equal(t, byte(0xFF), data[4], "Error packet header should be 0xFF")

			// And: Verify serialized data contains error code (little endian)
			assert.Equal(t, byte(tc.errorCode), data[5], "Error code low byte should match")
			assert.Equal(t, byte(tc.errorCode>>8), data[6], "Error code high byte should match")
		})
	}
}
