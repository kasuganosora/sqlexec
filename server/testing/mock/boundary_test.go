package mock

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
)

// TestBoundary_LargePacket tests large packet handling
func TestBoundary_LargePacket(t *testing.T) {
	// Given: Create a large packet (1MB)
	largeSize := 1024 * 1024 // 1MB
	largePacket := &protocol.Packet{}
	largePacket.SequenceID = 0
	largePacket.Payload = make([]byte, largeSize)
	largePacket.PayloadLength = uint32(largeSize)

	// Fill data
	for i := 0; i < largeSize; i++ {
		largePacket.Payload[i] = byte(i % 256)
	}

	// When: Serialize packet
	data, err := largePacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify packet format
	assert.Equal(t, byte(0x00), data[0], "Payload length low byte should be 0x00")
	assert.Equal(t, byte(0x00), data[1], "Payload length mid byte should be 0x00")
	assert.Equal(t, byte(0x10), data[2], "Payload length high byte should be 0x10 (1MB)")
	assert.Equal(t, byte(0x00), data[3], "Sequence ID should be 0")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(largeSize), parsedPacket.PayloadLength)
}

// TestBoundary_MediumPacket tests medium size packet
func TestBoundary_MediumPacket(t *testing.T) {
	// Given: Create 100KB packet
	mediumSize := 100 * 1024 // 100KB
	mediumPacket := &protocol.Packet{}
	mediumPacket.SequenceID = 1
	mediumPacket.Payload = make([]byte, mediumSize)
	mediumPacket.PayloadLength = uint32(mediumSize)

	// When: Serialize packet
	data, err := mediumPacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify packet format
	assert.Equal(t, byte(0x00), data[0], "Payload length low byte should be 0x00")
	assert.Equal(t, byte(0x90), data[1], "Payload length mid byte should be 0x90 (100KB)")
	assert.Equal(t, byte(0x01), data[2], "Payload length high byte should be 0x01")
	assert.Equal(t, byte(0x01), data[3], "Sequence ID should be 1")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(mediumSize), parsedPacket.PayloadLength)
}

// TestBoundary_EmptyPacket tests empty packet
func TestBoundary_EmptyPacket(t *testing.T) {
	// Given: Create empty packet
	emptyPacket := &protocol.Packet{}
	emptyPacket.SequenceID = 0
	emptyPacket.Payload = []byte{}
	emptyPacket.PayloadLength = 0

	// When: Serialize packet
	data, err := emptyPacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify packet format
	assert.Equal(t, byte(0x00), data[0], "Empty packet payload length should be 0")
	assert.Equal(t, byte(0x00), data[1], "Empty packet payload length should be 0")
	assert.Equal(t, byte(0x00), data[2], "Empty packet payload length should be 0")
	assert.Equal(t, byte(0x00), data[3], "Sequence ID should be 0")
	assert.Equal(t, 4, len(data), "Empty packet should be 4 bytes")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), parsedPacket.PayloadLength)
	assert.Equal(t, uint8(0), parsedPacket.SequenceID)
}

// TestBoundary_SingleBytePacket tests single byte packet
func TestBoundary_SingleBytePacket(t *testing.T) {
	// Given: Create single byte packet
	singlePacket := &protocol.Packet{}
	singlePacket.SequenceID = 5
	singlePacket.Payload = []byte{0x01}
	singlePacket.PayloadLength = 1

	// When: Serialize packet
	data, err := singlePacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify packet format
	assert.Equal(t, byte(0x01), data[0], "Payload length should be 1")
	assert.Equal(t, byte(0x00), data[1], "Payload length high byte should be 0")
	assert.Equal(t, byte(0x00), data[2], "Payload length high byte should be 0")
	assert.Equal(t, byte(0x05), data[3], "Sequence ID should be 5")
	assert.Equal(t, 5, len(data), "Single byte packet should be 5 bytes")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), parsedPacket.PayloadLength)
	assert.Equal(t, uint8(5), parsedPacket.SequenceID)
}

// TestBoundary_SpecialCharacters tests special character handling
func TestBoundary_SpecialCharacters(t *testing.T) {
	specialStrings := []struct {
		name  string
		value string
	}{
		{"ASCII", "Hello World"},
		{"Unicode", "Chinese"},
		{"Emoji", "🎉🚀"},
		{"Mixed", "Hello 🌍"},
		{"Quotes", "\"quoted\" and 'single'"},
		{"Backslash", "path\\to\\file"},
		{"Newlines", "line1\nline2\rline3"},
		{"Tabs", "col1\tcol2\tcol3"},
		{"Null", "text\x00middle"},
		{"Control", "\x01\x02\x03\x04"},
	}

	for _, tc := range specialStrings {
		t.Run(tc.name, func(t *testing.T) {
			// Given: Create packet with special characters
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = []byte(tc.value)
			packet.PayloadLength = uint32(len(tc.value))

			// When: Serialize packet
			data, err := packet.MarshalBytes()
			assert.NoError(t, err, "%s serialization should succeed", tc.name)

			// And: Deserialize
			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err, "%s deserialization should succeed", tc.name)

			// Then: Verify content matches
			assert.Equal(t, tc.value, string(parsedPacket.Payload), "%s content should match", tc.name)
		})
	}
}

// TestBoundary_QueryStringSpecialChars tests special characters in query strings
func TestBoundary_QueryStringSpecialChars(t *testing.T) {
	queryCases := []struct {
		name  string
		query string
	}{
		{"Simple", "SELECT 1"},
		{"WithQuotes", "SELECT 'it''s a test'"},
		{"WithBackticks", "SELECT `table`.`column`"},
		{"WithWhitespace", "  SELECT   1  "},
		{"WithComments", "SELECT /* comment */ 1"},
		{"WithNewlines", "SELECT\n1"},
		{"WithTabs", "SELECT\t1"},
		{"WithUnicode", "SELECT 'Chinese'"},
		{"WithEmoji", "SELECT '😀'"},
		{"LongQuery", strings.Repeat("SELECT ", 1000) + "1"},
	}

	for _, tc := range queryCases {
		t.Run(tc.name, func(t *testing.T) {
			// Given: Create query packet
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = append([]byte{protocol.COM_QUERY}, []byte(tc.query)...)
			packet.PayloadLength = uint32(len(packet.Payload))

			// When: Serialize packet
			data, err := packet.MarshalBytes()
			assert.NoError(t, err)

			// And: Deserialize
			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err)

			// Then: Verify query string is preserved (including spaces)
			query := string(parsedPacket.Payload[1:]) // Skip command byte
			assert.Equal(t, tc.query, query, "Query string should be preserved, including spaces")
		})
	}
}

// TestBoundary_UTF8Encoding tests UTF-8 encoding
func TestBoundary_UTF8Encoding(t *testing.T) {
	utf8Strings := []string{
		"Latin: Cañón",
		"Cyrillic: Привет",
		"Chinese: 你好",
		"Japanese: こんにちは",
		"Arabic: مرحبا",
		"Hebrew: שלום",
		"Greek: Γειά σου",
		"Korean: 안녕하세요",
		"Emoji: 😀😎🎉",
	}

	for _, s := range utf8Strings {
		t.Run(s[:min(10, len(s))], func(t *testing.T) {
			// Given: Verify UTF-8 validity
			assert.True(t, utf8.ValidString(s), "String should be valid UTF-8")

			// When: Create packet
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = []byte(s)
			packet.PayloadLength = uint32(len(s))

			// And: Serialize/deserialize
			data, err := packet.MarshalBytes()
			assert.NoError(t, err)

			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err)

			// Then: Verify UTF-8 correctness
			assert.True(t, utf8.Valid(parsedPacket.Payload), "Payload should be valid UTF-8")
			assert.Equal(t, s, string(parsedPacket.Payload), "UTF-8 string should match")
		})
	}
}

// TestBoundary_DatabaseNameSpecialChars tests special characters in database names
func TestBoundary_DatabaseNameSpecialChars(t *testing.T) {
	dbNames := []struct {
		name string
		db   string
	}{
		{"Simple", "test_db"},
		{"WithNumbers", "db123"},
		{"WithUnderscores", "my_test_database"},
		{"MaxLength", strings.Repeat("a", 64)}, // MySQL database name max length 64
	}

	for _, tc := range dbNames {
		t.Run(tc.name, func(t *testing.T) {
			// Given: Create COM_INIT_DB packet
			packet := &protocol.ComInitDBPacket{}
			packet.Payload = append([]byte{protocol.COM_INIT_DB}, []byte(tc.db)...)
			packet.PayloadLength = uint32(len(packet.Payload))

			// When: Get database name
			dbName := string(packet.Payload[1:])

			// Then: Verify database name
			assert.Equal(t, tc.db, dbName, "Database name should match")
		})
	}
}

// TestBoundary_ErrorMessageSpecialChars tests special characters in error messages
func TestBoundary_ErrorMessageSpecialChars(t *testing.T) {
	errorCases := []struct {
		name     string
		errorMsg string
	}{
		{"ASCII", "Table 'test.table' doesn't exist"},
		{"Unicode", "Table does not exist"},
		{"Quotes", "Column \"user's name\" not found"},
		{"Path", "File 'path/to/file.sql' not found"},
		{"Long", strings.Repeat("error ", 1000)},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			// Given: Create error packet
			errPacket := &protocol.ErrorPacket{}
			errPacket.SequenceID = 0
			errPacket.ErrorInPacket.Header = 0xFF
			errPacket.ErrorInPacket.ErrorCode = 1146
			errPacket.ErrorInPacket.SqlStateMarker = "#"
			errPacket.ErrorInPacket.SqlState = "42S02"
			errPacket.ErrorInPacket.ErrorMessage = tc.errorMsg

			// When: Serialize
			data, err := errPacket.Marshal()
			assert.NoError(t, err)

			// Then: Verify error packet header
			assert.Greater(t, len(data), 4, "Error packet should have data")
			assert.Equal(t, byte(0xFF), data[4], "Error packet header should be 0xFF")

			// And: Verify data length
			assert.Greater(t, len(data), 10, "Error packet should have enough data")
		})
	}
}

// TestBoundary_ConnectionClosed tests connection closed boundary condition
func TestBoundary_ConnectionClosed(t *testing.T) {
	// Given: Create mock connection
	mockConn := NewMockConnection()
	mockConn.Close()

	// Then: Verify connection is closed
	assert.True(t, mockConn.IsClosed(), "Connection should be closed")
}

// TestBoundary_ConnectionError tests connection error handling
func TestBoundary_ConnectionError(t *testing.T) {
	// Given: Create mock connection and set write error
	mockConn := NewMockConnection()
	mockConn.SetWriteError(assert.AnError)

	// When: Try to write data
	_, err := mockConn.Write([]byte{0x01})

	// Then: Verify returns error
	assert.Error(t, err, "Should return write error")
}

// TestBoundary_MultiplePackets tests multiple consecutive packets
func TestBoundary_MultiplePackets(t *testing.T) {
	// Given: Create mock connection
	mockConn := NewMockConnection()

	// When: Send multiple packets
	numPackets := 100
	for i := 0; i < numPackets; i++ {
		packet := &protocol.Packet{}
		packet.SequenceID = uint8(i)
		packet.Payload = []byte{byte(i)}
		packet.PayloadLength = 1

		_, err := packet.MarshalBytes()
		assert.NoError(t, err)

		mockConn.Write(packet.Payload)
	}

	// Then: Verify all packets are recorded
	writtenData := mockConn.GetWrittenData()
	assert.Equal(t, numPackets, len(writtenData), "Should record all packets")
}

// TestBoundary_SequenceID255 tests sequence ID 255 boundary
func TestBoundary_SequenceID255(t *testing.T) {
	// Given: Create packet with sequence ID 255
	packet := &protocol.Packet{}
	packet.SequenceID = 255
	packet.Payload = []byte{0x01}
	packet.PayloadLength = 1

	// When: Serialize packet
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify sequence ID 255
	assert.Equal(t, byte(255), data[3], "Sequence ID should be 255")
	assert.Equal(t, byte(0xFF), data[3], "Sequence ID should be 0xFF")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint8(255), parsedPacket.SequenceID)
}

// TestBoundary_MaxPayloadLength tests maximum payload length
func TestBoundary_MaxPayloadLength(t *testing.T) {
	// Given: Create larger payload length packet (1MB)
	maxPayload := 1024 * 1024 // 1,048,576
	packet := &protocol.Packet{}
	packet.SequenceID = 0
	packet.Payload = make([]byte, maxPayload)
	packet.PayloadLength = uint32(maxPayload)

	// When: Serialize packet
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify payload length
	assert.Equal(t, byte(0x00), data[0], "Low byte should be 0x00")
	assert.Equal(t, byte(0x00), data[1], "Mid byte should be 0x00")
	assert.Equal(t, byte(0x10), data[2], "High byte should be 0x10 (1MB)")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(maxPayload), parsedPacket.PayloadLength)
}

// TestBoundary_ZeroSequenceID tests sequence ID 0
func TestBoundary_ZeroSequenceID(t *testing.T) {
	// Given: Create packet with sequence ID 0
	packet := &protocol.Packet{}
	packet.SequenceID = 0
	packet.Payload = []byte{0x01}
	packet.PayloadLength = 1

	// When: Serialize packet
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: Verify sequence ID 0
	assert.Equal(t, byte(0x00), data[3], "Sequence ID should be 0")

	// And: Verify can deserialize
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint8(0), parsedPacket.SequenceID)
}

// Helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
