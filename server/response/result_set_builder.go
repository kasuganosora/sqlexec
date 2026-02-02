package response

// ResultSetBuilder 结果集构建器
type ResultSetBuilder struct{}

// NewResultSetBuilder 创建结果集构建器
func NewResultSetBuilder() *ResultSetBuilder {
	return &ResultSetBuilder{}
}

// BuildColumnCountPacket 构建列数包的字节
func BuildColumnCountPacket(sequenceID uint8, count uint64) ([]byte, error) {
	// 使用 Length Encoded Integer 编码列数
	encoded := encodeLengthEncodedInteger(count)

	// 构建完整的包（包含包头）
	packet := make([]byte, 4+len(encoded))
	packet[0] = byte(len(encoded))
	packet[1] = byte(len(encoded) >> 8)
	packet[2] = byte(len(encoded) >> 16)
	packet[3] = sequenceID
	copy(packet[4:], encoded)

	return packet, nil
}

// encodeLengthEncodedInteger 编码长度编码整数
func encodeLengthEncodedInteger(v uint64) []byte {
	if v < 251 {
		return []byte{byte(v)}
	}

	if v < 65536 {
		// 0xfc + 2 bytes
		return []byte{
			0xfc,
			byte(v),
			byte(v >> 8),
		}
	}

	if v < 16777216 {
		// 0xfd + 3 bytes
		return []byte{
			0xfd,
			byte(v),
			byte(v >> 8),
			byte(v >> 16),
		}
	}

	// 0xfe + 8 bytes
	return []byte{
		0xfe,
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
		byte(v >> 32),
		byte(v >> 40),
		byte(v >> 48),
	}
}
