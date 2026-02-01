package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================
// BinlogEventHeader 测试
// ============================================

func TestBinlogEventHeader(t *testing.T) {
	testData := []byte{
		0x71, 0x17, 0x28, 0x5a, // 时间戳: 1512576881
		0x0f,                         // 事件类型: FORMAT_DESCRIPTION_EVENT
		0x8c, 0x27, 0x00, 0x00, // 服务器ID: 10124
		0xfc, 0x00, 0x00, 0x00, // 事件长度: 252
		0x01, 0x09, 0x00, 0x00, // 下一个位置: 2305
		0x00, 0x00,                 // 标志: 0
	}

	header := &BinlogEventHeader{}
	err := header.Unmarshal(bytes.NewReader(testData))
	assert.NoError(t, err)

	assert.Equal(t, uint32(1512576881), header.Timestamp)
	assert.Equal(t, uint8(0x0f), header.EventType)
	assert.Equal(t, uint32(10124), header.ServerID)
	assert.Equal(t, uint32(252), header.EventLength)
	assert.Equal(t, uint32(2305), header.NextPos)
	assert.Equal(t, uint16(0), header.Flags)

	marshaled, err := header.Marshal()
	assert.NoError(t, err)
	assert.Equal(t, testData, marshaled)

	t.Logf("BinlogEventHeader: Type=%s, ServerID=%d, Length=%d",
		GetBinlogEventTypeName(header.EventType), header.ServerID, header.EventLength)
}

// ============================================
// COM_REGISTER_SLAVE 测试
// ============================================

func TestComRegisterSlave(t *testing.T) {
	payload := []byte{
		0x15,               // 命令: COM_REGISTER_SLAVE
		0x75, 0x27, 0x00, 0x00, // 服务器ID: 10101
		's', 'l', 'a', 'v', 'e', '_', 'n', '_', '1',
		0x00, // 主机名（以 NULL 结尾）
		0x00, // 用户名（为空，以 NULL 结尾）
		0x00, // 密码（为空，以 NULL 结尾）
		0xc9, 0x5a, // 端口: 23241
		0x00, 0x00, 0x00, 0x00, // 复制等级: 0
		0x00, 0x00, 0x00, 0x00, // 主服务器ID: 0
	}

	packetData := make([]byte, 4)
	packetData[0] = byte(len(payload))
	packetData[1] = byte(len(payload) >> 8)
	packetData[2] = byte(len(payload) >> 16)
	packetData[3] = 0x00

	testData := append(packetData, payload...)

	packet := &ComRegisterSlavePacket{}
	err := packet.Unmarshal(bytes.NewReader(testData))
	assert.NoError(t, err)

	assert.Equal(t, uint8(0x15), packet.Payload[0])
	assert.Equal(t, uint32(10101), packet.ServerID)
	assert.Equal(t, "slave_n_1", packet.Host)
	assert.Equal(t, "", packet.User)
	assert.Equal(t, "", packet.Password)
	assert.Equal(t, uint16(23241), packet.Port)

	t.Logf("COM_REGISTER_SLAVE: ServerID=%d, Host=%s, Port=%d",
		packet.ServerID, packet.Host, packet.Port)
}

// ============================================
// COM_BINLOG_DUMP 测试
// ============================================

func TestComBinlogDump(t *testing.T) {
	payload := []byte{
		0x12,               // 命令: COM_BINLOG_DUMP
		0x34, 0x06, 0x00, 0x00, // 日志位置: 1588
		0x02, 0x00,               // 标志
		0x75, 0x27, 0x00, 0x00, // 服务器ID: 10101
		'm', 'y', 's', 'q', 'l', '-', 'b', 'i', 'n', '.', '0', '0', '0', '0', '1', '9',
		0x00, // NULL 终止符
	}

	packetData := make([]byte, 4)
	packetData[0] = byte(len(payload))
	packetData[1] = byte(len(payload) >> 8)
	packetData[2] = byte(len(payload) >> 16)
	packetData[3] = 0x00

	testData := append(packetData, payload...)

	packet := &ComBinlogDumpPacket{}
	err := packet.Unmarshal(bytes.NewReader(testData))
	assert.NoError(t, err)

	assert.Equal(t, uint8(0x12), packet.Payload[0])
	assert.Equal(t, uint32(1588), packet.BinlogPos)
	assert.Equal(t, uint16(0x02), packet.Flags)
	assert.Equal(t, "mysql-bin.000019", packet.BinlogFilename)

	t.Logf("COM_BINLOG_DUMP: Pos=%d, Filename=%s, ServerID=%d",
		packet.BinlogPos, packet.BinlogFilename, packet.ServerID)
}

// ============================================
// FORMAT_DESCRIPTION_EVENT 测试
// ============================================

func TestFormatDescriptionEventSimple(t *testing.T) {
	bodyData := []byte{
		0x04, 0x00, // 格式版本: 4 (2字节)
		// 服务器版本（50字节）：'10.2.10-MariaDB-log' (19字符) + NULL (1字节) + 填充 (30字节)
		'1', '0', '.', '2', '.', '1', '0', '-', 'M', 'a', 'r', 'i', 'a', 'D', 'B', '-', 'l', 'o', 'g', 0x00, // 20字节
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 填充 10字节
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 填充 10字节
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 填充 10字节，总共50字节
		0x5a, 0x15, 0xaf, 0x4d, // 创建时间戳: 1512576881 (4字节)
		0x13, // 事件头长度: 19 (1字节)
		// 事件类型后长度数组（14字节，对应前14种事件类型）
		0x13, 0x0f, 0x13, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		0x02, // 校验和算法: CRC32 (值为2) (1字节)
		0xe2, 0x13, 0xce, 0xd6, // CRC32 校验和（4字节）
	}

	// EventLength = 19(header) + len(bodyData) = 19 + 76 = 95
	header := &BinlogEventHeader{
		Timestamp:   1512576881,
		EventType:   BINLOG_FORMAT_DESCRIPTION_EVENT,
		ServerID:    10124,
		EventLength: 95, // 19(header) + 2(版本) + 50(服务器版本) + 4(时间戳) + 1(HeaderLength) + 14(数组) + 1(CRC算法) + 4(CRC32值)
		NextPos:     2305,
		Flags:       0,
	}

	event := &FormatDescriptionEvent{Header: *header}

	// 解析完整事件体
	err := event.Unmarshal(bytes.NewReader(bodyData))
	assert.NoError(t, err)

	assert.Equal(t, uint16(4), event.BinlogFormatVersion)
	assert.Contains(t, event.ServerVersion, "10.2.10")
	assert.Equal(t, uint8(19), event.HeaderLength)
	assert.Greater(t, len(event.EventTypePostHeader), 0)
	assert.Equal(t, uint8(BINLOG_CHECKSUM_ALG_CRC32), event.ChecksumAlgorithm)

	t.Logf("FORMAT_DESCRIPTION_EVENT: Version=%s, HeaderLength=%d, ArrayLen=%d",
		event.ServerVersion, event.HeaderLength, len(event.EventTypePostHeader))
}

// ============================================
// GTID_EVENT 测试
// ============================================

func TestGtidEventSimple(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   1512576683,
		EventType:   BINLOG_GTID_EVENT,
		ServerID:    10124,
		EventLength: 42,
		NextPos:     535,
		Flags:       0,
	}

	bodyData := []byte{
		0x9b, 0x26, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // GTID 序列号: 9883 (8字节)
		0x00, 0x00, 0x00, 0x00, // 域ID: 0
		0x29, // 标志: FL_STANDALONE | FL_ALLOW_PARALLEL | FL_DDL
		// 6 字节的 0 填充
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// CRC32 校验和
		0x8e, 0x66, 0x9a, 0x30,
	}

	event := &GtidEvent{Header: *header}

	bodyWithoutCRC := bodyData[:len(bodyData)-4]
	err := event.Unmarshal(bytes.NewReader(bodyWithoutCRC))
	assert.NoError(t, err)

	assert.Equal(t, uint64(9883), event.GtidSeqNo)
	assert.Equal(t, uint32(0), event.DomainID)
	assert.Equal(t, uint8(0x29), event.Flags)
	assert.Equal(t, "0-10124-9883", event.String())

	t.Logf("GTID_EVENT: %s, Flags=0x%02x", event.String(), event.Flags)
}

// ============================================
// ROTATE_EVENT 测试
// ============================================

func TestRotateEventSimple(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   1234567890,
		EventType:   BINLOG_ROTATE_EVENT,
		ServerID:    10124,
		EventLength: 25,
		NextPos:     4,
		Flags:       0,
	}

	bodyData := []byte{
		0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 下一个位置: 4
		'm', 'y', 's', 'q', 'l', '-', 'b', 'i', 'n', '.', '0', '0', '0', '1', '9',
		// CRC32 校验和
		0xb2, 0xbc, 0xdb, 0xbf,
	}

	event := &RotateEvent{Header: *header}

	bodyWithoutCRC := bodyData[:len(bodyData)-4]
	err := event.Unmarshal(bytes.NewReader(bodyWithoutCRC))
	assert.NoError(t, err)

	assert.Equal(t, uint64(4), event.NextPosition)
	assert.Equal(t, "mysql-bin.00019", event.BinlogFile)

	t.Logf("ROTATE_EVENT: NextPos=%d, NextFile=%s", event.NextPosition, event.BinlogFile)
}

// ============================================
// QUERY_EVENT 测试
// ============================================

func TestQueryEventSimple(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   1512576881,
		EventType:   BINLOG_QUERY_EVENT,
		ServerID:    10124,
		EventLength: 50,
		NextPos:     2305,
		Flags:       0,
	}

	bodyData := []byte{
		0x66, 0x01, 0x00, 0x00, // 线程ID: 358
		0x00, 0x00, 0x00, 0x00, // 执行时间: 0
		0x00,                     // 数据库名长度: 0
		0x00, 0x00,               // 错误代码: 0
		0x00, 0x00,               // 状态变量块长度: 0
		0x00,                     // 数据库名: 空
		// SQL 语句: "SELECT 1"
		'S', 'E', 'L', 'E', 'C', 'T', ' ', '1',
		0x00, // NULL 终止符
	}

	event := &QueryEvent{Header: *header}

	err := event.Unmarshal(bytes.NewReader(bodyData))
	assert.NoError(t, err)

	assert.Equal(t, uint32(358), event.ThreadID)
	assert.Equal(t, uint32(0), event.ExecutionTime)
	assert.Equal(t, "", event.DatabaseName)
	assert.Equal(t, "SELECT 1", event.Query)

	t.Logf("QUERY_EVENT: Database=%s, Query=%s", event.DatabaseName, event.Query)
}

// ============================================
// TABLE_MAP_EVENT 测试
// ============================================

func TestTableMapEventSimple(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   1512576683,
		EventType:   BINLOG_TABLE_MAP_EVENT,
		ServerID:    10124,
		EventLength: 35,
		NextPos:     535,
		Flags:       0,
	}

	bodyData := []byte{
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // 表ID: 1
		0x00, 0x00,                           // 保留: 0
		0x04,                     // 数据库名长度: 4
		't', 'e', 's', 't',     // 数据库名
		0x00,                     // NULL 终止符
		0x02,                     // 表名长度: 2
		't', '1',               // 表名
		0x00,                     // NULL 终止符
		0x02,                     // 列数: 2
		0x01, 0x02,               // 列类型
		0x02, 0x00,               // 元数据长度: 2
		0x08, 0x02,               // 元数据
		0x01,                     // NULL 位图
		// CRC32 校验和
		0x00, 0x00, 0x00, 0x00,
	}

	event := &TableMapEvent{Header: *header}

	bodyWithoutCRC := bodyData[:len(bodyData)-4]
	err := event.Unmarshal(bytes.NewReader(bodyWithoutCRC))
	assert.NoError(t, err)

	assert.Equal(t, uint64(1), event.TableID)
	assert.Equal(t, "test", event.DatabaseName)
	assert.Equal(t, "t1", event.TableName)
	assert.Equal(t, 2, event.ColumnCount)

	t.Logf("TABLE_MAP_EVENT: DB=%s, Table=%s, Columns=%d", event.DatabaseName, event.TableName, event.ColumnCount)
}

// ============================================
// XID_EVENT 测试
// ============================================

func TestXidEventSimple(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   1512576683,
		EventType:   BINLOG_XID_EVENT,
		ServerID:    10124,
		EventLength: 17,
		NextPos:     535,
		Flags:       0,
	}

	bodyData := []byte{
		0x78, 0x56, 0x34, 0x12, 0x78, 0x56, 0x34, 0x12, // XID: 0x1234567812345678
		// CRC32 校验和
		0x00, 0x00, 0x00, 0x00,
	}

	event := &XidEvent{Header: *header}

	bodyWithoutCRC := bodyData[:len(bodyData)-4]
	err := event.Unmarshal(bytes.NewReader(bodyWithoutCRC))
	assert.NoError(t, err)

	assert.Equal(t, uint64(0x1234567812345678), event.XID)

	t.Logf("XID_EVENT: XID=%d", event.XID)
}

// ============================================
// HEARTBEAT_LOG_EVENT 测试
// ============================================

func TestHeartbeatLogEventSimple(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   1512576683,
		EventType:   BINLOG_HEARTBEAT_LOG_EVENT,
		ServerID:    10124,
		EventLength: 20,
		NextPos:     0,
		Flags:       0,
	}

	bodyData := []byte{
		// 心跳时间戳字符串
		'2', '0', '2', '6', '-', '0', '1', '-', '1', '7',
		0x00, // NULL 终止符
	}

	event := &HeartbeatLogEvent{Header: *header}

	err := event.Unmarshal(bytes.NewReader(bodyData))
	assert.NoError(t, err)

	assert.Equal(t, "2026-01-17", event.Timestamp)

	t.Logf("HEARTBEAT_LOG_EVENT: Timestamp=%s", event.Timestamp)
}

// ============================================
// ReplicationNetworkStream 测试
// ============================================

func TestReplicationNetworkStream(t *testing.T) {
	buf := new(bytes.Buffer)

	eventBuf := new(bytes.Buffer)
	binary.Write(eventBuf, binary.LittleEndian, uint32(1512576683)) // 时间戳
	eventBuf.WriteByte(BINLOG_GTID_EVENT)                           // 事件类型
	binary.Write(eventBuf, binary.LittleEndian, uint32(10124))      // 服务器ID
	binary.Write(eventBuf, binary.LittleEndian, uint32(42))        // 事件长度
	binary.Write(eventBuf, binary.LittleEndian, uint32(535))       // 下一个位置
	binary.Write(eventBuf, binary.LittleEndian, uint16(0))         // 标志
	binary.Write(eventBuf, binary.LittleEndian, uint64(9883))     // GTID 序列号
	binary.Write(eventBuf, binary.LittleEndian, uint32(0))        // 域ID
	eventBuf.WriteByte(0x29)                                     // 标志
	eventBuf.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // 填充

	eventData := eventBuf.Bytes()

	packetBuf := new(bytes.Buffer)
	packetBuf.WriteByte(byte(len(eventData)))
	packetBuf.WriteByte(byte(len(eventData) >> 8))
	packetBuf.WriteByte(byte(len(eventData) >> 16))
	packetBuf.WriteByte(0x00) // sequence ID
	packetBuf.WriteByte(0x00) // status: OK
	packetBuf.Write(eventData)

	buf.Write(packetBuf.Bytes())

	stream := NewReplicationNetworkStream(buf)
	header, eventData, status, err := stream.ReadEvent()
	assert.NoError(t, err)
	assert.Equal(t, uint8(0x00), status)
	assert.Equal(t, uint8(BINLOG_GTID_EVENT), header.EventType)
	assert.NotNil(t, eventData)

	t.Logf("ReplicationNetworkStream: EventType=%s, Status=%d, DataLen=%d",
		GetBinlogEventTypeName(header.EventType), status, len(eventData))
}

// ============================================
// Marshal 测试
// ============================================

func TestBinlogEventHeaderMarshal(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   1512576881,
		EventType:   BINLOG_FORMAT_DESCRIPTION_EVENT,
		ServerID:    10124,
		EventLength: 252,
		NextPos:     2305,
		Flags:       0,
	}

	data, err := header.Marshal()
	assert.NoError(t, err)
	assert.Equal(t, 19, len(data))

	parsed := &BinlogEventHeader{}
	err = parsed.Unmarshal(bytes.NewReader(data))
	assert.NoError(t, err)

	assert.Equal(t, header.Timestamp, parsed.Timestamp)
	assert.Equal(t, header.EventType, parsed.EventType)
	assert.Equal(t, header.ServerID, parsed.ServerID)
	assert.Equal(t, header.EventLength, parsed.EventLength)
	assert.Equal(t, header.NextPos, parsed.NextPos)
	assert.Equal(t, header.Flags, parsed.Flags)

	t.Logf("Marshal Test: Type=%s", GetBinlogEventTypeName(parsed.EventType))
}
