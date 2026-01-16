package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 587	74.720975	::1	::1	MySQL	157	Server Greeting  proto=10 version=10.3.12-MariaDB
// WQAAAAo1LjUuNS0xMC4zLjEyLU1hcmlhREIACAAAAEpzKWxmPkFoAP73CAIAv4EVAAAAAAAABwAAAGpaZW10fDQrekk6KQBteXNxbF9uYXRpdmVfcGFzc3dvcmQA
// []byte{0x59, 0x0, 0x0, 0x0, 0xa, 0x35, 0x2e, 0x35, 0x2e, 0x35, 0x2d, 0x31, 0x30, 0x2e, 0x33, 0x2e, 0x31, 0x32, 0x2d, 0x4d, 0x61, 0x72, 0x69, 0x61, 0x44, 0x42, 0x0, 0x8, 0x0, 0x0, 0x0, 0x4a, 0x73, 0x29, 0x6c, 0x66, 0x3e, 0x41, 0x68, 0x0, 0xfe, 0xf7, 0x8, 0x2, 0x0, 0xbf, 0x81, 0x15, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x6a, 0x5a, 0x65, 0x6d, 0x74, 0x7c, 0x34, 0x2b, 0x7a, 0x49, 0x3a, 0x29, 0x0, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x0}
func TestServerGreeting(t *testing.T) {
	packetBase64 := "WQAAAAo1LjUuNS0xMC4zLjEyLU1hcmlhREIACAAAAEpzKWxmPkFoAP73CAIAv4EVAAAAAAAABwAAAGpaZW10fDQrekk6KQBteXNxbF9uYXRpdmVfcGFzc3dvcmQA"
	packet, err := base64.StdEncoding.DecodeString(packetBase64)
	if err != nil {
		t.Fatal(err)
	}

	// 打印十六进制数据用于调试
	t.Logf("Raw packet hex: %s", hex.EncodeToString(packet))
	t.Logf("Packet length: %d", len(packet))

	handshake := &HandshakeV10Packet{}
	err = handshake.Unmarshal(bytes.NewReader(packet))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("handshake: %+v", handshake)
	assert.Equal(t, uint32(89), handshake.Packet.PayloadLength)
	assert.Equal(t, uint8(0), handshake.Packet.SequenceID)
	assert.Equal(t, uint8(10), handshake.ProtocolVersion)
	assert.Equal(t, "5.5.5-10.3.12-MariaDB", handshake.ServerVersion)
	assert.Equal(t, uint32(8), handshake.ThreadID)
	assert.Equal(t, []byte{0x4a, 0x73, 0x29, 0x6c, 0x66, 0x3e, 0x41, 0x68}, handshake.AuthPluginDataPart)
	assert.Equal(t, uint16(0xf7fe), handshake.CapabilityFlags1)
	assert.Equal(t, uint8(8), handshake.CharacterSet)
	assert.Equal(t, uint16(2), handshake.StatusFlags)
	assert.Equal(t, uint16(0x81bf), handshake.CapabilityFlags2)
	assert.Equal(t, uint8(21), handshake.AuthPluginDataLen)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, handshake.Reserved)
	assert.Equal(t, uint32(7), handshake.MariaDBCaps)
	assert.Equal(t, []byte{0x6a, 0x5a, 0x65, 0x6d, 0x74, 0x7c, 0x34, 0x2b, 0x7a, 0x49, 0x3a, 0x29, 0x0}, handshake.AuthPluginDataPart2)
	assert.Equal(t, "mysql_native_password", handshake.AuthPluginName)

}

func TestUnmarshalHandshakeResponse(t *testing.T) {
	packet := []byte{0xdc, 0x0, 0x0, 0x1, 0x84, 0xa6, 0x9f, 0x20, 0x0, 0x0, 0x0, 0x1, 0x1c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x72, 0x6f, 0x6f, 0x74, 0x0, 0x14, 0x97, 0x3b, 0x9a, 0xc9, 0xc, 0x1e, 0x33, 0xe2, 0xa7, 0x5f, 0x82, 0x10, 0xbf, 0x56, 0x10, 0xcd, 0x5c, 0xce, 0xdb, 0xba, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x0, 0x8b, 0x3, 0x5f, 0x6f, 0x73, 0x7, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x73, 0xc, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0xa, 0x6c, 0x69, 0x62, 0x6d, 0x61, 0x72, 0x69, 0x61, 0x64, 0x62, 0x4, 0x5f, 0x70, 0x69, 0x64, 0x4, 0x32, 0x38, 0x35, 0x36, 0xc, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x68, 0x6f, 0x73, 0x74, 0x9, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x9, 0x5f, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x5, 0x41, 0x4d, 0x44, 0x36, 0x34, 0xf, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5, 0x33, 0x2e, 0x30, 0x2e, 0x38, 0xc, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x61, 0x6d, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x7, 0x5f, 0x74, 0x68, 0x72, 0x65, 0x61, 0x64, 0x4, 0x32, 0x38, 0x34, 0x38}
	// 包描述如下
	// MySQL Protocol
	// Packet Length: 220
	// Packet Number: 1
	// Login Request
	//     Client Capabilities: 0xa684
	//     Extended Client Capabilities: 0x209f
	//     MAX Packet: 16777216
	//     Collation: gbk COLLATE gbk_chinese_ci (28)
	//     Unused: 00000000000000000000000000000000000000
	//     MariaDB Extended Client Capabilities: 0x00000007
	//     Username: root
	//     Password: 973b9ac90c1e33e2a75f8210bf5610cd5ccedbba
	//     Client Auth Plugin: mysql_native_password
	//     Connection Attributes
	//         Connection Attributes length: 139
	//         Connection Attribute - _os: Windows
	//         Connection Attribute - _client_name: libmariadb
	//         Connection Attribute - _pid: 2856
	//         Connection Attribute - _server_host: 127.0.0.1
	//         Connection Attribute - _platform: AMD64
	//         Connection Attribute - _client_version: 3.0.8
	//         Connection Attribute - program_name: mysql
	//         Connection Attribute - _thread: 2848

	// 计算 capabilities 标志位
	capabilities := uint32(0xa684) | uint32(0x209f)<<16

	handshakeResponse := &HandshakeResponse{}
	err := handshakeResponse.Unmarshal(bytes.NewReader(packet), capabilities)
	if err != nil {
		t.Fatal(err)
	}

	// 验证基本包信息
	assert.Equal(t, uint32(220), handshakeResponse.Packet.PayloadLength)
	assert.Equal(t, uint8(1), handshakeResponse.Packet.SequenceID)

	// 验证客户端能力标志
	assert.Equal(t, uint16(0xa684), handshakeResponse.ClientCapabilities)
	assert.Equal(t, uint16(0x209f), handshakeResponse.ExtendedClientCapabilities)

	// 验证最大包大小
	assert.Equal(t, uint32(16777216), handshakeResponse.MaxPacketSize)

	// 验证字符集 (gbk COLLATE gbk_chinese_ci = 28)
	assert.Equal(t, uint8(28), handshakeResponse.CharacterSet)

	// 验证保留字段 (19字节的0)
	expectedReserved := make([]byte, 19)
	assert.Equal(t, expectedReserved, handshakeResponse.Reserved)

	// 验证 MariaDB 扩展能力
	assert.Equal(t, uint32(7), handshakeResponse.MariaDBCaps)

	// 验证用户名
	assert.Equal(t, "root", handshakeResponse.User)

	// 验证认证响应 (密码哈希)
	expectedPassword := "973b9ac90c1e33e2a75f8210bf5610cd5ccedbba"
	assert.Equal(t, expectedPassword, handshakeResponse.AuthResponse)

	// 验证客户端认证插件名称
	assert.Equal(t, "mysql_native_password", handshakeResponse.ClientAuthPluginName)

	// 验证连接属性长度
	assert.Equal(t, uint64(139), handshakeResponse.ConnectionAttributesLength)

	// 验证连接属性
	assert.Equal(t, 8, len(handshakeResponse.ConnectionAttributes))

	// 验证具体的连接属性
	expectedAttributes := map[string]string{
		"_os":             "Windows",
		"_client_name":    "libmariadb",
		"_pid":            "2856",
		"_server_host":    "127.0.0.1",
		"_platform":       "AMD64",
		"_client_version": "3.0.8",
		"program_name":    "mysql",
		"_thread":         "2848",
	}

	for _, attr := range handshakeResponse.ConnectionAttributes {
		expectedValue, exists := expectedAttributes[attr.Name]
		assert.True(t, exists, "Unexpected attribute: %s", attr.Name)
		assert.Equal(t, expectedValue, attr.Value, "Attribute %s value mismatch", attr.Name)
	}

	t.Logf("HandshakeResponse: %+v", handshakeResponse)
}

func TestHandshakeResponseMarshal(t *testing.T) {
	// 创建一个 HandshakeResponse 实例
	handshakeResponse := &HandshakeResponse{
		Packet: Packet{
			PayloadLength: 220,
			SequenceID:    1,
		},
		ClientCapabilities:         0xa684,
		ExtendedClientCapabilities: 0x209f,
		MaxPacketSize:              16777216,
		CharacterSet:               28,
		Reserved:                   make([]byte, 19),
		MariaDBCaps:                7,
		User:                       "root",
		AuthResponse:               "973b9ac90c1e33e2a75f8210bf5610cd5ccedbba",
		Database:                   "",
		ClientAuthPluginName:       "mysql_native_password",
		ConnectionAttributesLength: 139,
		ConnectionAttributes: []ConnectionAttributeItem{
			{Name: "_os", Value: "Windows"},
			{Name: "_client_name", Value: "libmariadb"},
			{Name: "_pid", Value: "2856"},
			{Name: "_server_host", Value: "127.0.0.1"},
			{Name: "_platform", Value: "AMD64"},
			{Name: "_client_version", Value: "3.0.8"},
			{Name: "program_name", Value: "mysql"},
			{Name: "_thread", Value: "2848"},
		},
		ZstdCompressionLevel: 0,
	}

	// 序列化
	data, err := handshakeResponse.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化回来，应该和原始结构体一致
	handshakeResponse2 := &HandshakeResponse{}
	capabilities := uint32(0xa684) | uint32(0x209f)<<16
	err = handshakeResponse2.Unmarshal(bytes.NewReader(data), capabilities)
	if err != nil {
		t.Fatal(err)
	}

	// 验证所有字段
	assert.Equal(t, handshakeResponse.Packet.PayloadLength, handshakeResponse2.Packet.PayloadLength)
	assert.Equal(t, handshakeResponse.Packet.SequenceID, handshakeResponse2.Packet.SequenceID)
	assert.Equal(t, handshakeResponse.ClientCapabilities, handshakeResponse2.ClientCapabilities)
	assert.Equal(t, handshakeResponse.ExtendedClientCapabilities, handshakeResponse2.ExtendedClientCapabilities)
	assert.Equal(t, handshakeResponse.MaxPacketSize, handshakeResponse2.MaxPacketSize)
	assert.Equal(t, handshakeResponse.CharacterSet, handshakeResponse2.CharacterSet)
	assert.Equal(t, handshakeResponse.Reserved, handshakeResponse2.Reserved)
	assert.Equal(t, handshakeResponse.MariaDBCaps, handshakeResponse2.MariaDBCaps)
	assert.Equal(t, handshakeResponse.User, handshakeResponse2.User)
	assert.Equal(t, handshakeResponse.AuthResponse, handshakeResponse2.AuthResponse)
	assert.Equal(t, handshakeResponse.Database, handshakeResponse2.Database)
	assert.Equal(t, handshakeResponse.ClientAuthPluginName, handshakeResponse2.ClientAuthPluginName)
	assert.Equal(t, handshakeResponse.ConnectionAttributesLength, handshakeResponse2.ConnectionAttributesLength)
	assert.Equal(t, len(handshakeResponse.ConnectionAttributes), len(handshakeResponse2.ConnectionAttributes))
	assert.Equal(t, handshakeResponse.ZstdCompressionLevel, handshakeResponse2.ZstdCompressionLevel)

	// 验证连接属性
	for i, attr := range handshakeResponse.ConnectionAttributes {
		assert.Equal(t, attr.Name, handshakeResponse2.ConnectionAttributes[i].Name)
		assert.Equal(t, attr.Value, handshakeResponse2.ConnectionAttributes[i].Value)
	}

	t.Logf("Original: %+v", handshakeResponse)
	t.Logf("Unmarshaled: %+v", handshakeResponse2)
}

// OK 包
// []byte{0x7, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0}
// 描述如下
// MySQL Protocol - response OK
//     Packet Length: 7
//     Packet Number: 2
//     Response Code: OK Packet (0x00)
//     Affected Rows: 0
//     Server Status: 0x0002
//     Warnings: 0

// ERR 包
//  []byte{0x48, 0x0, 0x0, 0x2, 0xff, 0x15, 0x4, 0x23, 0x32, 0x38, 0x30, 0x30, 0x30, 0x41, 0x63, 0x63, 0x65, 0x73, 0x73, 0x20, 0x64, 0x65, 0x6e, 0x69, 0x65, 0x64, 0x20, 0x66, 0x6f, 0x72, 0x20, 0x75, 0x73, 0x65, 0x72, 0x20, 0x27, 0x72, 0x6f, 0x6f, 0x74, 0x27, 0x40, 0x27, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, 0x27, 0x20, 0x28, 0x75, 0x73, 0x69, 0x6e, 0x67, 0x20, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x3a, 0x20, 0x59, 0x45, 0x53, 0x29}
// 描述如下
// MySQL Protocol - response ERROR
// Packet Length: 72
// Packet Number: 2
// Response Code: ERR Packet (0xff)
// Error Code: 1045
// SQL state: 28000
// Error message: Access denied for user 'root'@'localhost' (using password: YES)

// 查询包
// []byte{0x21, 0x0, 0x0, 0x0, 0x3, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31}
// 描述如下
// MySQL Protocol
// Packet Length: 33
// Packet Number: 0
// Request Command Query
// 	Command: Query (3)
// 	Statement: select @@version_comment limit 1

// 查询包返回 组合
func TestQueryResponsePackets(t *testing.T) {
	packetData := []byte{0x1, 0x0, 0x0, 0x1, 0x1, 0x27, 0x0, 0x0, 0x2, 0x3, 0x64, 0x65, 0x66, 0x0, 0x0, 0x0, 0x11, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x0, 0xc, 0x1c, 0x0, 0x3e, 0x0, 0x0, 0x0, 0xfd, 0x0, 0x0, 0x27, 0x0, 0x0, 0x5, 0x0, 0x0, 0x3, 0xfe, 0x0, 0x0, 0x2, 0x0, 0x20, 0x0, 0x0, 0x4, 0x1f, 0x6d, 0x61, 0x72, 0x69, 0x61, 0x64, 0x62, 0x2e, 0x6f, 0x72, 0x67, 0x20, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x20, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x5, 0x0, 0x0, 0x5, 0xfe, 0x0, 0x0, 0x2, 0x0}

	offset := 0

	// 1. 解析列数包
	{
		header := packetData[offset : offset+4]
		length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		packet := packetData[offset : offset+4+length]
		columnCountPacket := &ColumnCountPacket{}
		err := columnCountPacket.UnmarshalDefault(bytes.NewReader(packet))
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), columnCountPacket.PayloadLength)
		assert.Equal(t, uint8(1), columnCountPacket.SequenceID)
		assert.Equal(t, uint64(1), columnCountPacket.ColumnCount)
		offset += 4 + length
	}

	// 2. 解析字段元数据包
	{
		header := packetData[offset : offset+4]
		length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		packet := packetData[offset : offset+4+length]
		fieldMetaPacket := &FieldMetaPacket{}
		err := fieldMetaPacket.UnmarshalDefault(bytes.NewReader(packet))
		assert.NoError(t, err)
		assert.Equal(t, uint32(39), fieldMetaPacket.PayloadLength)
		assert.Equal(t, uint8(2), fieldMetaPacket.SequenceID)
		assert.Equal(t, "def", fieldMetaPacket.Catalog)
		assert.Equal(t, "@@version_comment", fieldMetaPacket.Name)
		assert.Equal(t, uint16(28), fieldMetaPacket.CharacterSet)
		assert.Equal(t, uint32(62), fieldMetaPacket.ColumnLength)
		assert.Equal(t, uint8(253), fieldMetaPacket.Type)
		assert.Equal(t, uint16(0), fieldMetaPacket.Flags)
		assert.Equal(t, uint8(39), fieldMetaPacket.Decimals)
		offset += 4 + length
	}

	// 3. 解析中间EOF包
	{
		header := packetData[offset : offset+4]
		length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		packet := packetData[offset : offset+4+length]
		eofPacket1 := &EofPacket{}
		err := eofPacket1.Unmarshal(bytes.NewReader(packet), CLIENT_PROTOCOL_41)
		assert.NoError(t, err)
		assert.Equal(t, uint32(5), eofPacket1.PayloadLength)
		assert.Equal(t, uint8(3), eofPacket1.SequenceID)
		assert.Equal(t, uint8(0xfe), eofPacket1.Header)
		assert.Equal(t, uint16(0), eofPacket1.Warnings)
		assert.Equal(t, uint16(2), eofPacket1.StatusFlags)
		offset += 4 + length
	}

	// 4. 解析数据行包
	{
		header := packetData[offset : offset+4]
		length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		packet := packetData[offset : offset+4+length]
		rowDataPacket := &RowDataPacket{}
		err := rowDataPacket.Unmarshal(bytes.NewReader(packet))
		assert.NoError(t, err)
		assert.Equal(t, uint32(32), rowDataPacket.PayloadLength)
		assert.Equal(t, uint8(4), rowDataPacket.SequenceID)
		assert.Equal(t, 1, len(rowDataPacket.RowData))
		assert.Equal(t, "mariadb.org binary distribution", rowDataPacket.RowData[0])
		offset += 4 + length
	}

	// 5. 解析最终EOF包
	{
		header := packetData[offset : offset+4]
		length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		packet := packetData[offset : offset+4+length]
		eofPacket2 := &EofPacket{}
		err := eofPacket2.Unmarshal(bytes.NewReader(packet), CLIENT_PROTOCOL_41)
		assert.NoError(t, err)
		assert.Equal(t, uint32(5), eofPacket2.PayloadLength)
		assert.Equal(t, uint8(5), eofPacket2.SequenceID)
		assert.Equal(t, uint8(0xfe), eofPacket2.Header)
		assert.Equal(t, uint16(0), eofPacket2.Warnings)
		assert.Equal(t, uint16(2), eofPacket2.StatusFlags)
		offset += 4 + length
	}
}

func TestHandshakeV10PacketMarshal(t *testing.T) {
	h := &HandshakeV10Packet{
		Packet: Packet{
			PayloadLength: 89,
			SequenceID:    0,
		},
		ProtocolVersion:     10,
		ServerVersion:       "5.5.5-10.3.12-MariaDB",
		ThreadID:            8,
		AuthPluginDataPart:  []byte{0x4a, 0x73, 0x29, 0x6c, 0x66, 0x3e, 0x41, 0x68},
		Filter:              0,
		CapabilityFlags1:    0xf7fe,
		CharacterSet:        8,
		StatusFlags:         2,
		CapabilityFlags2:    0x81bf,
		AuthPluginDataLen:   21,
		Reserved:            []byte{0, 0, 0, 0, 0, 0},
		MariaDBCaps:         7,
		AuthPluginDataPart2: []byte{0x6a, 0x5a, 0x65, 0x6d, 0x74, 0x7c, 0x34, 0x2b, 0x7a, 0x49, 0x3a, 0x29, 0x0},
		AuthPluginName:      "mysql_native_password",
	}
	data, err := h.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化回来，应该和原始结构体一致
	h2 := &HandshakeV10Packet{}
	err = h2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, h.ProtocolVersion, h2.ProtocolVersion)
	assert.Equal(t, h.ServerVersion, h2.ServerVersion)
	assert.Equal(t, h.ThreadID, h2.ThreadID)
	assert.Equal(t, h.AuthPluginDataPart, h2.AuthPluginDataPart)
	assert.Equal(t, h.Filter, h2.Filter)
	assert.Equal(t, h.CapabilityFlags1, h2.CapabilityFlags1)
	assert.Equal(t, h.CharacterSet, h2.CharacterSet)
	assert.Equal(t, h.StatusFlags, h2.StatusFlags)
	assert.Equal(t, h.CapabilityFlags2, h2.CapabilityFlags2)
	assert.Equal(t, h.AuthPluginDataLen, h2.AuthPluginDataLen)
	assert.Equal(t, h.Reserved, h2.Reserved)
	assert.Equal(t, h.MariaDBCaps, h2.MariaDBCaps)
	assert.Equal(t, h.AuthPluginDataPart2, h2.AuthPluginDataPart2)
	assert.Equal(t, h.AuthPluginName, h2.AuthPluginName)
}

func TestConnectionAttributeItemUnmarshal(t *testing.T) {
	packet1 := []byte{0x3, 0x5f, 0x6f, 0x73, 0x7, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x73}
	// key: _os value: Windows

	packet2 := []byte{0xc, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x68, 0x6f, 0x73, 0x74, 0x9, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31}
	// key: _server_host value: 127.0.0.1

	// 测试第一个包
	item1 := &ConnectionAttributeItem{}
	err := item1.Unmarshal(bytes.NewReader(packet1))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "_os", item1.Name)
	assert.Equal(t, "Windows", item1.Value)

	// 测试第二个包
	item2 := &ConnectionAttributeItem{}
	err = item2.Unmarshal(bytes.NewReader(packet2))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "_server_host", item2.Name)
	assert.Equal(t, "127.0.0.1", item2.Value)
}

func TestConnectionAttributeItemMarshal(t *testing.T) {
	// 测试用例1: _os = Windows
	item1 := &ConnectionAttributeItem{
		Name:  "_os",
		Value: "Windows",
	}

	data1, err := item1.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	item1Back := &ConnectionAttributeItem{}
	err = item1Back.Unmarshal(bytes.NewReader(data1))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, item1.Name, item1Back.Name)
	assert.Equal(t, item1.Value, item1Back.Value)

	// 测试用例2: _server_host = 127.0.0.1
	item2 := &ConnectionAttributeItem{
		Name:  "_server_host",
		Value: "127.0.0.1",
	}

	data2, err := item2.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	item2Back := &ConnectionAttributeItem{}
	err = item2Back.Unmarshal(bytes.NewReader(data2))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, item2.Name, item2Back.Name)
	assert.Equal(t, item2.Value, item2Back.Value)

	// 验证序列化结果与原始测试数据一致
	expected1 := []byte{0x3, 0x5f, 0x6f, 0x73, 0x7, 0x57, 0x69, 0x6e, 0x64, 0x6f, 0x77, 0x73}
	assert.Equal(t, expected1, data1)

	expected2 := []byte{0xc, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x68, 0x6f, 0x73, 0x74, 0x9, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31}
	assert.Equal(t, expected2, data2)
}

func TestComQueryPacket(t *testing.T) {
	// 测试 COM_QUERY 包
	queryPacket := &ComQueryPacket{
		Packet: Packet{
			PayloadLength: 33,
			SequenceID:    0,
		},
		Command: 0x03,
		Query:   "select @@version_comment limit 1",
	}

	data, err := queryPacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	queryPacket2 := &ComQueryPacket{}
	err = queryPacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, queryPacket.Command, queryPacket2.Command)
	assert.Equal(t, queryPacket.Query, queryPacket2.Query)
}

func TestComStmtPreparePacket(t *testing.T) {
	// 测试 COM_STMT_PREPARE 包
	stmtPreparePacket := &ComStmtPreparePacket{
		Packet: Packet{
			PayloadLength: 25,
			SequenceID:    1,
		},
		Command: 0x16,
		Query:   "SELECT * FROM users WHERE id = ?",
	}

	data, err := stmtPreparePacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	stmtPreparePacket2 := &ComStmtPreparePacket{}
	err = stmtPreparePacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, stmtPreparePacket.Command, stmtPreparePacket2.Command)
	assert.Equal(t, stmtPreparePacket.Query, stmtPreparePacket2.Query)
}

func TestComStmtExecutePacket(t *testing.T) {
	// 测试 COM_STMT_EXECUTE 包
	stmtExecutePacket := &ComStmtExecutePacket{
		Packet: Packet{
			PayloadLength: 15,
			SequenceID:    2,
		},
		Command:           0x17,
		StatementID:       1,
		Flags:             0,
		IterationCount:    1,
		NullBitmap:        []byte{0x00},
		NewParamsBindFlag: 1,
		ParamTypes: []StmtParamType{
			{Type: 3, Flag: 0}, // INT
		},
		ParamValues: []any{"123"},
	}

	data, err := stmtExecutePacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	stmtExecutePacket2 := &ComStmtExecutePacket{}
	err = stmtExecutePacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, stmtExecutePacket.Command, stmtExecutePacket2.Command)
	assert.Equal(t, stmtExecutePacket.StatementID, stmtExecutePacket2.StatementID)
	assert.Equal(t, stmtExecutePacket.Flags, stmtExecutePacket2.Flags)
	assert.Equal(t, stmtExecutePacket.IterationCount, stmtExecutePacket2.IterationCount)
}

func TestComStmtClosePacket(t *testing.T) {
	// 测试 COM_STMT_CLOSE 包
	stmtClosePacket := &ComStmtClosePacket{
		Packet: Packet{
			PayloadLength: 5,
			SequenceID:    3,
		},
		Command:     0x19,
		StatementID: 1,
	}

	data, err := stmtClosePacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	stmtClosePacket2 := &ComStmtClosePacket{}
	err = stmtClosePacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, stmtClosePacket.Command, stmtClosePacket2.Command)
	assert.Equal(t, stmtClosePacket.StatementID, stmtClosePacket2.StatementID)
}

func TestComPingPacket(t *testing.T) {
	// 测试 COM_PING 包
	pingPacket := &ComPingPacket{
		Packet: Packet{
			PayloadLength: 1,
			SequenceID:    4,
		},
		Command: 0x0e,
	}

	data, err := pingPacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	pingPacket2 := &ComPingPacket{}
	err = pingPacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, pingPacket.Command, pingPacket2.Command)
}

func TestComQuitPacket(t *testing.T) {
	// 测试 COM_QUIT 包
	quitPacket := &ComQuitPacket{
		Packet: Packet{
			PayloadLength: 1,
			SequenceID:    5,
		},
		Command: 0x01,
	}

	data, err := quitPacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	quitPacket2 := &ComQuitPacket{}
	err = quitPacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, quitPacket.Command, quitPacket2.Command)
}

func TestComChangeUserPacket(t *testing.T) {
	// 测试 COM_CHANGE_USER 包
	changeUserPacket := &ComChangeUserPacket{
		Packet: Packet{
			PayloadLength: 20,
			SequenceID:    6,
		},
		Command:      0x11,
		User:         "newuser",
		AuthResponse: "password_hash",
		Database:     "testdb",
		CharacterSet: 33, // utf8_general_ci
	}

	data, err := changeUserPacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	changeUserPacket2 := &ComChangeUserPacket{}
	err = changeUserPacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, changeUserPacket.Command, changeUserPacket2.Command)
	assert.Equal(t, changeUserPacket.User, changeUserPacket2.User)
	assert.Equal(t, changeUserPacket.AuthResponse, changeUserPacket2.AuthResponse)
	assert.Equal(t, changeUserPacket.Database, changeUserPacket2.Database)
	assert.Equal(t, changeUserPacket.CharacterSet, changeUserPacket2.CharacterSet)
}

func TestComSetOptionPacket(t *testing.T) {
	// 测试 COM_SET_OPTION 包
	setOptionPacket := &ComSetOptionPacket{
		ComPacket: ComPacket{
			Packet: Packet{
				PayloadLength: 3,
				SequenceID:    7,
			},
			Command: 0x1b,
		},
		OptionOperation: 0x0001, // MYSQL_OPTION_MULTI_STATEMENTS_ON
	}

	data, err := setOptionPacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	setOptionPacket2 := &ComSetOptionPacket{}
	err = setOptionPacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, setOptionPacket.Command, setOptionPacket2.Command)
	assert.Equal(t, setOptionPacket.OptionOperation, setOptionPacket2.OptionOperation)
}

func TestComStmtSendLongDataPacket(t *testing.T) {
	// 测试 COM_STMT_SEND_LONG_DATA 包
	longDataPacket := &ComStmtSendLongDataPacket{
		Packet: Packet{
			PayloadLength: 15,
			SequenceID:    8,
		},
		Command:     0x18,
		StatementID: 1,
		ParamID:     0,
		Data:        []byte("This is long data for testing"),
	}

	data, err := longDataPacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	longDataPacket2 := &ComStmtSendLongDataPacket{}
	err = longDataPacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, longDataPacket.Command, longDataPacket2.Command)
	assert.Equal(t, longDataPacket.StatementID, longDataPacket2.StatementID)
	assert.Equal(t, longDataPacket.ParamID, longDataPacket2.ParamID)
	assert.Equal(t, longDataPacket.Data, longDataPacket2.Data)
}

func TestComStmtResetPacket(t *testing.T) {
	// 测试 COM_STMT_RESET 包
	resetPacket := &ComStmtResetPacket{
		Packet: Packet{
			PayloadLength: 5,
			SequenceID:    9,
		},
		Command:     0x1a,
		StatementID: 1,
	}

	data, err := resetPacket.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	// 反序列化验证
	resetPacket2 := &ComStmtResetPacket{}
	err = resetPacket2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, resetPacket.Command, resetPacket2.Command)
	assert.Equal(t, resetPacket.StatementID, resetPacket2.StatementID)
}

func TestComFetchPacket(t *testing.T) {
	packet := &ComFetchPacket{
		Packet: Packet{
			PayloadLength: 9,
			SequenceID:    1,
		},
		Command:     COM_STMT_FETCH,
		StatementID: 1,
		RowCount:    10,
	}

	data, err := packet.Marshal()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	// 反序列化
	packet2 := &ComFetchPacket{}
	err = packet2.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)
	assert.Equal(t, packet.Command, packet2.Command)
	assert.Equal(t, packet.StatementID, packet2.StatementID)
	assert.Equal(t, packet.RowCount, packet2.RowCount)
}

func TestEofPacketFunctions(t *testing.T) {
	// 测试创建EOF包
	eofPacket := CreateEofPacket(1)
	assert.Equal(t, uint8(1), eofPacket.SequenceID)
	assert.Equal(t, uint8(EOF_MARKER), eofPacket.EofInPacket.Header)
	assert.Equal(t, uint16(0), eofPacket.EofInPacket.Warnings)
	assert.Equal(t, uint16(0), eofPacket.EofInPacket.StatusFlags)

	// 测试状态标志设置
	eofPacket.EofInPacket.SetAutoCommit(true)
	assert.True(t, eofPacket.EofInPacket.IsAutoCommit())
	assert.Equal(t, uint16(2), eofPacket.EofInPacket.StatusFlags)

	eofPacket.EofInPacket.SetInTransaction(true)
	assert.True(t, eofPacket.EofInPacket.IsInTransaction())
	assert.Equal(t, uint16(3), eofPacket.EofInPacket.StatusFlags) // 2 + 1

	eofPacket.EofInPacket.SetMoreResults(true)
	assert.True(t, eofPacket.EofInPacket.HasMoreResults())
	assert.Equal(t, uint16(11), eofPacket.EofInPacket.StatusFlags) // 3 + 8

	// 测试状态标志描述
	descriptions := eofPacket.EofInPacket.GetStatusFlagsDescription()
	assert.Contains(t, descriptions, "AUTOCOMMIT")
	assert.Contains(t, descriptions, "IN_TRANSACTION")
	assert.Contains(t, descriptions, "MORE_RESULTS")

	// 测试便捷构造函数
	intermediateEof := CreateIntermediateEofPacket(2)
	assert.Equal(t, uint8(2), intermediateEof.SequenceID)
	assert.True(t, intermediateEof.EofInPacket.IsAutoCommit())
	assert.False(t, intermediateEof.EofInPacket.IsInTransaction())

	finalEof := CreateFinalEofPacket(3)
	assert.Equal(t, uint8(3), finalEof.SequenceID)
	assert.True(t, finalEof.EofInPacket.IsAutoCommit())
	assert.False(t, finalEof.EofInPacket.IsInTransaction())

	// 测试序列化和反序列化
	data, err := eofPacket.Marshal()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	eofPacket2 := &EofPacket{}
	err = eofPacket2.Unmarshal(bytes.NewReader(data), CLIENT_PROTOCOL_41)
	assert.NoError(t, err)
	assert.Equal(t, eofPacket.EofInPacket.Header, eofPacket2.EofInPacket.Header)
	assert.Equal(t, eofPacket.EofInPacket.Warnings, eofPacket2.EofInPacket.Warnings)
	assert.Equal(t, eofPacket.EofInPacket.StatusFlags, eofPacket2.EofInPacket.StatusFlags)
}

func TestComQueryPacketUnmarshal(t *testing.T) {
	// 测试数据：select @@version_comment limit 1
	// 根据实际包内容：21 00 00 00 03 03 73 65 6c 65 63 74 20 40 40 76 65 72 73 69 6f 6e 5f 63 6f 6d 6d 65 6e 74 20 6c 69 6d 69 74 20 31
	testData := []byte{
		0x21, 0x00, 0x00, 0x00, // 包长度 33
		0x03, // 序列ID
		0x03, // 命令类型 COM_QUERY
		// 查询字符串: "select @@version_comment limit 1"
		0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
	}

	packet := &ComQueryPacket{}
	err := packet.Unmarshal(bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// 验证包头部
	if packet.PayloadLength != 33 {
		t.Errorf("Expected PayloadLength 33, got %d", packet.PayloadLength)
	}
	if packet.SequenceID != 3 {
		t.Errorf("Expected SequenceID 3, got %d", packet.SequenceID)
	}

	// 验证命令类型
	if packet.Command != 0x03 {
		t.Errorf("Expected Command 0x03, got 0x%02x", packet.Command)
	}

	// 验证查询字符串
	expectedQuery := "select @@version_comment limit 1"
	if packet.Query != expectedQuery {
		t.Errorf("Expected Query '%s', got '%s'", expectedQuery, packet.Query)
	}

	t.Logf("Successfully parsed ComQueryPacket:")
	t.Logf("  PayloadLength: %d", packet.PayloadLength)
	t.Logf("  SequenceID: %d", packet.SequenceID)
	t.Logf("  Command: 0x%02x", packet.Command)
	t.Logf("  Query: '%s'", packet.Query)
}

func TestComQueryPacketDebug(t *testing.T) {
	// 根据您提供的实际包内容
	// 0000   21 00 00 00 03 73 65 6c 65 63 74 20 40 40 76 65   !....select @@ve
	// 0010   72 73 69 6f 6e 5f 63 6f 6d 6d 65 6e 74 20 6c 69   rsion_comment li
	// 0020   6d 69 74 20 31                                    mit 1
	
	// 解析：21 00 00 00 = 33 (包长度), 03 = 序列ID, 03 = 命令类型
	testData := []byte{
		0x21, 0x00, 0x00, 0x00, // 包长度 33
		0x03,                     // 序列ID
		0x03,                     // 命令类型 COM_QUERY
		// 查询字符串: "select @@version_comment limit 1"
		0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
	}

	t.Logf("Test data length: %d", len(testData))
	t.Logf("Test data hex: %x", testData)
	
	// 详细检查前几个字节
	t.Logf("First 8 bytes: %x", testData[:8])
	t.Logf("Byte 0-3 (length): %x", testData[0:4])
	t.Logf("Byte 4 (sequence): %x", testData[4])
	t.Logf("Byte 5 (command): %x", testData[5])

	// 先测试包头部解析
	packet := &Packet{}
	err := packet.Unmarshal(bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Packet.Unmarshal failed: %v", err)
	}

	t.Logf("Packet header - PayloadLength: %d, SequenceID: %d", packet.PayloadLength, packet.SequenceID)

	// 然后测试完整的ComQueryPacket
	queryPacket := &ComQueryPacket{}
	err = queryPacket.Unmarshal(bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("ComQueryPacket.Unmarshal failed: %v", err)
	}

	t.Logf("ComQueryPacket - PayloadLength: %d, SequenceID: %d, Command: 0x%02x, Query: '%s'", 
		queryPacket.PayloadLength, queryPacket.SequenceID, queryPacket.Command, queryPacket.Query)

	// 验证结果
	if packet.PayloadLength != 33 {
		t.Errorf("Expected PayloadLength 33, got %d", packet.PayloadLength)
	}
	if packet.SequenceID != 3 {
		t.Errorf("Expected SequenceID 3, got %d", packet.SequenceID)
	}
	if queryPacket.Command != 0x03 {
		t.Errorf("Expected Command 0x03, got 0x%02x", queryPacket.Command)
	}
	expectedQuery := "select @@version_comment limit 1"
	if queryPacket.Query != expectedQuery {
		t.Errorf("Expected Query '%s', got '%s'", expectedQuery, queryPacket.Query)
	}
}
