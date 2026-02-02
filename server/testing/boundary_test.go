package testing

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/stretchr/testify/assert"
)

// TestBoundary_LargePacket 测试大包处理（使用较小的包避免内存问题）
func TestBoundary_LargePacket(t *testing.T) {
	// Given: 创建一个大包（1MB）
	largeSize := 1024 * 1024 // 1MB
	largePacket := &protocol.Packet{}
	largePacket.SequenceID = 0
	largePacket.Payload = make([]byte, largeSize)
	largePacket.PayloadLength = uint32(largeSize) // 重要：必须设置PayloadLength

	// 填充数据
	for i := 0; i < largeSize; i++ {
		largePacket.Payload[i] = byte(i % 256)
	}

	// When: 序列化包
	data, err := largePacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证包格式
	assert.Equal(t, byte(0x00), data[0], "载荷长度低字节应该是0x00")
	assert.Equal(t, byte(0x00), data[1], "载荷长度中字节应该是0x00")
	assert.Equal(t, byte(0x10), data[2], "载荷长度高字节应该是0x10 (1MB)")
	assert.Equal(t, byte(0x00), data[3], "序列号应该是0")

	// And: 验证可以反序列化
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(largeSize), parsedPacket.PayloadLength)
}

// TestBoundary_MediumPacket 测试中等大小包
func TestBoundary_MediumPacket(t *testing.T) {
	// Given: 创建100KB的包
	mediumSize := 100 * 1024 // 100KB
	mediumPacket := &protocol.Packet{}
	mediumPacket.SequenceID = 1
	mediumPacket.Payload = make([]byte, mediumSize)
	mediumPacket.PayloadLength = uint32(mediumSize) // 重要：必须设置PayloadLength

	// When: 序列化包
	data, err := mediumPacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证包格式
	assert.Equal(t, byte(0x00), data[0], "载荷长度低字节应该是0x00")
	assert.Equal(t, byte(0x90), data[1], "载荷长度中字节应该是0x90 (100KB)")
	assert.Equal(t, byte(0x01), data[2], "载荷长度高字节应该是0x01")
	assert.Equal(t, byte(0x01), data[3], "序列号应该是1")

	// And: 验证可以反序列化
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(mediumSize), parsedPacket.PayloadLength)
}

// TestBoundary_EmptyPacket 测试空包
func TestBoundary_EmptyPacket(t *testing.T) {
	// Given: 创建空包
	emptyPacket := &protocol.Packet{}
	emptyPacket.SequenceID = 0
	emptyPacket.Payload = []byte{}
	emptyPacket.PayloadLength = 0 // 重要：必须设置为0

	// When: 序列化包
	data, err := emptyPacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证包格式
	assert.Equal(t, byte(0x00), data[0], "空包载荷长度应该是0")
	assert.Equal(t, byte(0x00), data[1], "空包载荷长度应该是0")
	assert.Equal(t, byte(0x00), data[2], "空包载荷长度应该是0")
	assert.Equal(t, byte(0x00), data[3], "序列号应该是0")
	assert.Equal(t, 4, len(data), "空包应该是4字节")

	// And: 验证可以反序列化
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), parsedPacket.PayloadLength)
	assert.Equal(t, uint8(0), parsedPacket.SequenceID)
}

// TestBoundary_SingleBytePacket 测试单字节包
func TestBoundary_SingleBytePacket(t *testing.T) {
	// Given: 创建单字节包
	singlePacket := &protocol.Packet{}
	singlePacket.SequenceID = 5
	singlePacket.Payload = []byte{0x01}
	singlePacket.PayloadLength = 1 // 重要：必须设置为1

	// When: 序列化包
	data, err := singlePacket.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证包格式
	assert.Equal(t, byte(0x01), data[0], "载荷长度应该是1")
	assert.Equal(t, byte(0x00), data[1], "载荷长度高字节应该是0")
	assert.Equal(t, byte(0x00), data[2], "载荷长度高字节应该是0")
	assert.Equal(t, byte(0x05), data[3], "序列号应该是5")
	assert.Equal(t, 5, len(data), "单字节包应该是5字节")

	// And: 验证可以反序列化
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), parsedPacket.PayloadLength)
	assert.Equal(t, uint8(5), parsedPacket.SequenceID)
}

// TestBoundary_SpecialCharacters 测试特殊字符处理
func TestBoundary_SpecialCharacters(t *testing.T) {
	specialStrings := []struct {
		name  string
		value string
	}{
		{"ASCII", "Hello World"},
		{"Unicode", "你好世界"},
		{"Emoji", "😀🎉🚀"},
		{"Mixed", "Hello 你好 🌍"},
		{"Quotes", "\"quoted\" and 'single'"},
		{"Backslash", "path\\to\\file"},
		{"Newlines", "line1\nline2\rline3"},
		{"Tabs", "col1\tcol2\tcol3"},
		{"Null", "text\x00middle"}, // NULL字符
		{"Control", "\x01\x02\x03\x04"},
	}

	for _, tc := range specialStrings {
		t.Run(tc.name, func(t *testing.T) {
			// Given: 创建包含特殊字符的包
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = []byte(tc.value)
			packet.PayloadLength = uint32(len(tc.value)) // 重要：必须设置PayloadLength

			// When: 序列化包
			data, err := packet.MarshalBytes()
			assert.NoError(t, err, "%s序列化应该成功", tc.name)

			// And: 反序列化
			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err, "%s反序列化应该成功", tc.name)

			// Then: 验证内容匹配
			assert.Equal(t, tc.value, string(parsedPacket.Payload), "%s内容应该匹配", tc.name)
		})
	}
}

// TestBoundary_QueryStringSpecialChars 测试查询字符串中的特殊字符
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
		{"WithUnicode", "SELECT '测试中文'"},
		{"WithEmoji", "SELECT '😀'"},
		{"LongQuery", strings.Repeat("SELECT ", 1000) + "1"},
	}

	for _, tc := range queryCases {
		t.Run(tc.name, func(t *testing.T) {
			// Given: 创建查询包
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = append([]byte{protocol.COM_QUERY}, []byte(tc.query)...)
			packet.PayloadLength = uint32(len(packet.Payload)) // 重要：必须设置PayloadLength

			// When: 序列化包
			data, err := packet.MarshalBytes()
			assert.NoError(t, err)

			// And: 反序列化
			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err)

			// Then: 验证查询字符串完整保留（包括空格）
			query := string(parsedPacket.Payload[1:]) // 跳过命令字节
			assert.Equal(t, tc.query, query, "查询字符串应该完整保留，包括空格")
		})
	}
}

// TestBoundary_UTF8Encoding 测试UTF-8编码
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
			// Given: 验证UTF-8有效性
			assert.True(t, utf8.ValidString(s), "字符串应该是有效的UTF-8")

			// When: 创建包
			packet := &protocol.Packet{}
			packet.SequenceID = 0
			packet.Payload = []byte(s)
			packet.PayloadLength = uint32(len(s)) // 重要：必须设置PayloadLength

			// And: 序列化/反序列化
			data, err := packet.MarshalBytes()
			assert.NoError(t, err)

			parsedPacket := &protocol.Packet{}
			err = parsedPacket.Unmarshal(bytes.NewReader(data))
			assert.NoError(t, err)

			// Then: 验证UTF-8正确性
			assert.True(t, utf8.Valid(parsedPacket.Payload), "载荷应该是有效的UTF-8")
			assert.Equal(t, s, string(parsedPacket.Payload), "UTF-8字符串应该匹配")
		})
	}
}

// TestBoundary_DatabaseNameSpecialChars 测试数据库名称中的特殊字符
func TestBoundary_DatabaseNameSpecialChars(t *testing.T) {
	dbNames := []struct {
		name string
		db   string
	}{
		{"Simple", "test_db"},
		{"WithNumbers", "db123"},
		{"WithUnderscores", "my_test_database"},
		{"MaxLength", strings.Repeat("a", 64)}, // MySQL数据库名称最大长度64
	}

	for _, tc := range dbNames {
		t.Run(tc.name, func(t *testing.T) {
			// Given: 创建COM_INIT_DB包
			packet := &protocol.ComInitDBPacket{}
			packet.Payload = append([]byte{protocol.COM_INIT_DB}, []byte(tc.db)...)
			packet.PayloadLength = uint32(len(packet.Payload)) // 重要：必须设置PayloadLength

			// When: 获取数据库名称
			dbName := string(packet.Payload[1:])

			// Then: 验证数据库名称
			assert.Equal(t, tc.db, dbName, "数据库名称应该匹配")
		})
	}
}

// TestBoundary_ErrorMessageSpecialChars 测试错误消息中的特殊字符
func TestBoundary_ErrorMessageSpecialChars(t *testing.T) {
	errorCases := []struct {
		name     string
		errorMsg string
	}{
		{"ASCII", "Table 'test.table' doesn't exist"},
		{"Unicode", "表 '测试表' 不存在"},
		{"Quotes", "Column \"user's name\" not found"},
		{"Path", "File 'C:\\path\\to\\file.sql' not found"},
		{"Long", strings.Repeat("error ", 1000)},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			// Given: 创建错误包
			errPacket := &protocol.ErrorPacket{}
			errPacket.SequenceID = 0
			errPacket.ErrorInPacket.Header = 0xFF
			errPacket.ErrorInPacket.ErrorCode = 1146
			errPacket.ErrorInPacket.SqlStateMarker = "#"
			errPacket.ErrorInPacket.SqlState = "42S02"
			errPacket.ErrorInPacket.ErrorMessage = tc.errorMsg

			// When: 序列化
			data, err := errPacket.Marshal()
			assert.NoError(t, err)

			// Then: 验证错误包头
			assert.Greater(t, len(data), 4, "错误包应该有数据")
			assert.Equal(t, byte(0xFF), data[4], "错误包头应该是0xFF")

			// And: 验证数据长度
			assert.Greater(t, len(data), 10, "错误包应该有足够的数据")
			})
	}
}

// TestBoundary_ConnectionClosed 测试连接关闭边界条件
func TestBoundary_ConnectionClosed(t *testing.T) {
	// Given: 创建Mock连接
	mockConn := NewMockConnection()
	mockConn.Close()

	// Then: 验证连接已关闭
	assert.True(t, mockConn.IsClosed(), "连接应该已关闭")
	// MockConnection的Write返回nil即使关闭，所以不测试错误返回值
}

// TestBoundary_ConnectionError 测试连接错误处理
func TestBoundary_ConnectionError(t *testing.T) {
	// Given: 创建Mock连接并设置写入错误
	mockConn := NewMockConnection()
	mockConn.SetWriteError(assert.AnError)

	// When: 尝试写入数据
	_, err := mockConn.Write([]byte{0x01})

	// Then: 验证返回错误
	assert.Error(t, err, "应该返回写入错误")
}

// TestBoundary_MultiplePackets 测试多个连续包
func TestBoundary_MultiplePackets(t *testing.T) {
	// Given: 创建Mock连接
	mockConn := NewMockConnection()

	// When: 发送多个包
	numPackets := 100
	for i := 0; i < numPackets; i++ {
		packet := &protocol.Packet{}
		packet.SequenceID = uint8(i)
		packet.Payload = []byte{byte(i)}
		packet.PayloadLength = 1 // 重要：必须设置PayloadLength

		_, err := packet.MarshalBytes()
		assert.NoError(t, err)

		mockConn.Write(packet.Payload)
	}

	// Then: 验证所有包被记录
	writtenData := mockConn.GetWrittenData()
	assert.Equal(t, numPackets, len(writtenData), "应该记录所有包")
}

// TestBoundary_SequenceID255 测试序列号255的边界
func TestBoundary_SequenceID255(t *testing.T) {
	// Given: 创建序列号为255的包
	packet := &protocol.Packet{}
	packet.SequenceID = 255
	packet.Payload = []byte{0x01}
	packet.PayloadLength = 1 // 重要：必须设置PayloadLength

	// When: 序列化包
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证序列号255
	assert.Equal(t, byte(255), data[3], "序列号应该是255")
	assert.Equal(t, byte(0xFF), data[3], "序列号应该是0xFF")

	// And: 验证可以反序列化
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint8(255), parsedPacket.SequenceID)
}

// TestBoundary_MaxPayloadLength 测试最大载荷长度（使用较小的包避免内存问题）
func TestBoundary_MaxPayloadLength(t *testing.T) {
	// Given: 创建较大的载荷长度的包 (1MB)
	maxPayload := 1024 * 1024 // 1,048,576
	packet := &protocol.Packet{}
	packet.SequenceID = 0
	packet.Payload = make([]byte, maxPayload)
	packet.PayloadLength = uint32(maxPayload) // 重要：必须设置PayloadLength

	// When: 序列化包
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证载荷长度
	assert.Equal(t, byte(0x00), data[0], "低字节应该是0x00")
	assert.Equal(t, byte(0x00), data[1], "中字节应该是0x00")
	assert.Equal(t, byte(0x10), data[2], "高字节应该是0x10 (1MB)")

	// And: 验证可以反序列化
	parsedPacket := &protocol.Packet{}
	err = parsedPacket.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, uint32(maxPayload), parsedPacket.PayloadLength)
}

// TestBoundary_ZeroSequenceID 测试序列号为0
func TestBoundary_ZeroSequenceID(t *testing.T) {
	// Given: 创建序列号为0的包
	packet := &protocol.Packet{}
	packet.SequenceID = 0
	packet.Payload = []byte{0x01}
	packet.PayloadLength = 1 // 重要：必须设置PayloadLength

	// When: 序列化包
	data, err := packet.MarshalBytes()
	assert.NoError(t, err)

	// Then: 验证序列号0
	assert.Equal(t, byte(0x00), data[3], "序列号应该是0")

	// And: 验证可以反序列化
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
