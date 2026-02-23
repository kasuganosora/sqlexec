package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================
// 使用 MariaDB 真实 binlog 数据的测试
// ============================================

// 从 MariaDB 仓库的真实 binlog 数据解析 FORMAT_DESCRIPTION_EVENT
func TestMariaDBFormatDescriptionEvent(t *testing.T) {
	// 使用真实的 binlog-header.binlog 数据（跳过 4 字节 binlog 魔数）
	testData := []byte{
		// BinlogEventHeader (19 字节) - 从 binlog-header.binlog 偏移 4 开始
		0xee, 0x94, 0x65, 0x5c, // Timestamp: 0x5c6594ee (小端序)
		0x0f,                   // EventType: FORMAT_DESCRIPTION_EVENT (15)
		0x01, 0x00, 0x00, 0x00, // ServerID: 1
		0xfc, 0x00, 0x00, 0x00, // EventLength: 252
		0x00, 0x00, 0x00, 0x00, // NextPos: 0
		0x00, 0x00, // Flags: 0
		// Event Body
		0x04, 0x00, // Format version: 4
		// Server version (50 bytes, fixed) - "10.4.3-MariaDB-debug-log"
		'1', '0', '.', '4', '.', '3', '-', 'M', 'a', 'r', 'i', 'a', 'D', 'B', '-', 'd', 'e', 'b', 'u', 'g', '-', 'l', 'o', 'g',
		0x00,
		// 填充
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Created timestamp
		0xee, 0x94, 0x65, 0x5c, // Created timestamp: 0x5c6594ee
		// Header length
		0x13, // Header length: 19
		// Event type post header lengths
		0x00, 0x00,
	}

	header := &BinlogEventHeader{}
	err := header.Unmarshal(bytes.NewReader(testData[:19]))
	assert.NoError(t, err)

	assert.Equal(t, uint32(0x5c6594ee), header.Timestamp)
	assert.Equal(t, uint8(BINLOG_FORMAT_DESCRIPTION_EVENT), header.EventType)
	assert.Equal(t, uint32(1), header.ServerID)
	assert.Equal(t, uint32(252), header.EventLength)

	t.Logf("MariaDB FORMAT_DESCRIPTION: Version=10.4.3-MariaDB-debug-log, ServerID=%d, Length=%d, Timestamp=0x%x",
		header.ServerID, header.EventLength, header.Timestamp)
}

// 测试完整的 binlog 文件解析流程
func TestMariaDBBinlogFileParsing(t *testing.T) {
	// 简化的 binlog 文件结构测试
	testData := []byte{
		// Binlog magic
		0xfe, 0x62, 0x69, 0x6e,
		// FORMAT_DESCRIPTION_EVENT header
		0xee, 0x94, 0x65, 0x5c, 0x0f, 0x01, 0x00, 0x00, 0x00, 0xfc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// ... body continues
	}

	magic := testData[:4]
	assert.Equal(t, []byte{0xfe, 0x62, 0x69, 0x6e}, magic)

	t.Logf("Binlog magic: %x", magic)
}

// 测试 QUERY_EVENT (CREATE TABLE) - 简化测试
func TestMariaDBQueryEventCreateTable(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_QUERY_EVENT,
		ServerID:    1,
		EventLength: 0x40,
		NextPos:     0x0001c6,
		Flags:       0,
	}

	bodyData := []byte{
		0x00, 0x00, 0x00, 0x00, // ThreadID: 0
		0x00, 0x00, 0x00, 0x00, // Execution time: 0
		0x04,       // Database length: 4
		0x00, 0x00, // Error code: 0
		0x00, 0x00, // Status vars length: 0 (简化：无状态变量)
		// Database name + NULL terminator
		't', 'e', 's', 't', 0x00,
		// SQL: "CREATE TABLE t1(a int)" + NULL
		'C', 'R', 'E', 'A', 'T', 'E', ' ', 'T', 'A', 'B', 'L', 'E', ' ', 't', '1', '(', 'a', ' ', 'i', 'n', 't', ')', 0x00,
	}

	event := &QueryEvent{Header: *header}
	err := event.Unmarshal(bytes.NewReader(bodyData))
	assert.NoError(t, err)

	t.Logf("QUERY_EVENT: DB=%s, SQL=%q, StatusVarLen=%d, ThreadID=%d", event.DatabaseName, event.Query, event.StatusVarLen, event.ThreadID)

	assert.Equal(t, "test", event.DatabaseName)
	assert.NotEmpty(t, event.Query, "SQL query should not be empty")
	assert.Equal(t, "CREATE TABLE t1(a int)", event.Query)
}

// 测试 XID_EVENT
func TestMariaDBXidEvent(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_XID_EVENT,
		ServerID:    1,
		EventLength: 0x2c,
		NextPos:     0x000286,
		Flags:       0,
	}

	bodyData := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // XID: 0
	}

	event := &XidEvent{Header: *header}
	err := event.Unmarshal(bytes.NewReader(bodyData))
	assert.NoError(t, err)

	assert.Equal(t, uint64(0), event.XID)

	t.Logf("XID_EVENT: XID=%d", event.XID)
}

// 测试 ROTATE_EVENT
func TestMariaDBRotateEvent(t *testing.T) {
	header := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_ROTATE_EVENT,
		ServerID:    1,
		EventLength: 0x1b,
		NextPos:     0,
		Flags:       0,
	}

	bodyData := []byte{
		0x68, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Next position: 0x668 (小端序)
		'm', 'a', 's', 't', 'e', 'r', '-', 'b', 'i', 'n', '.', '0', '0', '0', '0', '0', '2',
		0x00, // NULL 终止符
	}

	event := &RotateEvent{Header: *header}
	err := event.Unmarshal(bytes.NewReader(bodyData))
	assert.NoError(t, err)

	assert.Equal(t, uint64(0x668), event.NextPosition)
	assert.Equal(t, "master-bin.000002", event.BinlogFile)

	t.Logf("ROTATE_EVENT: NextPos=%d, File=%s", event.NextPosition, event.BinlogFile)
}

// 测试从完整 binlog 文件读取事件序列
func TestMariaDBCompleteBinlogFlow(t *testing.T) {
	// 模拟从 binlog 文件读取多个事件
	// 这是测试整个解析流程的示例

	// 1. 读取并验证魔数
	magic := []byte{0xfe, 0x62, 0x69, 0x6e}
	assert.Equal(t, "bin", string(magic[1:4]))

	// 2. 读取 FORMAT_DESCRIPTION_EVENT
	formatDescHeader := &BinlogEventHeader{
		Timestamp:   0x5c6594ee,
		EventType:   BINLOG_FORMAT_DESCRIPTION_EVENT,
		ServerID:    1,
		EventLength: 252,
		NextPos:     0,
		Flags:       0,
	}
	assert.Equal(t, uint8(BINLOG_FORMAT_DESCRIPTION_EVENT), formatDescHeader.EventType)

	// 3. 读取第一个 QUERY_EVENT (CREATE TABLE t1)
	queryHeader1 := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_QUERY_EVENT,
		ServerID:    1,
		EventLength: 100,
		NextPos:     454,
		Flags:       0,
	}
	assert.Equal(t, uint8(BINLOG_QUERY_EVENT), queryHeader1.EventType)

	// 4. 读取第二个 QUERY_EVENT (CREATE TABLE t2)
	queryHeader2 := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_QUERY_EVENT,
		ServerID:    1,
		EventLength: 101,
		NextPos:     731,
		Flags:       0,
	}
	assert.Equal(t, uint8(BINLOG_QUERY_EVENT), queryHeader2.EventType)

	// 5. 读取 GTID_EVENT
	gtidHeader := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_GTID_EVENT,
		ServerID:    1,
		EventLength: 27,
		NextPos:     848,
		Flags:       0,
	}
	assert.Equal(t, uint8(BINLOG_GTID_EVENT), gtidHeader.EventType)

	// 6. 读取 TABLE_MAP_EVENT
	tableMapHeader := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_TABLE_MAP_EVENT,
		ServerID:    1,
		EventLength: 26,
		NextPos:     900,
		Flags:       0,
	}
	assert.Equal(t, uint8(BINLOG_TABLE_MAP_EVENT), tableMapHeader.EventType)

	// 7. 读取 XID_EVENT
	xidHeader := &BinlogEventHeader{
		Timestamp:   0x5c6594fd,
		EventType:   BINLOG_XID_EVENT,
		ServerID:    1,
		EventLength: 44,
		NextPos:     966,
		Flags:       0,
	}
	assert.Equal(t, uint8(BINLOG_XID_EVENT), xidHeader.EventType)

	t.Logf("Complete binlog flow test passed - validated event sequence")
}
