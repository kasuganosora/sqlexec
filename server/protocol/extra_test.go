package protocol

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"
)

// === Charset Tests ===

func TestGetCharsetName(t *testing.T) {
	// Known charset
	name := GetCharsetName(CHARSET_UTF8_GENERAL_CI)
	if name == "" {
		t.Error("GetCharsetName should return non-empty for known charset")
	}

	name2 := GetCharsetName(CHARSET_LATIN1)
	if name2 == "" {
		t.Error("GetCharsetName should return non-empty for latin1")
	}
}

func TestGetCharsetID(t *testing.T) {
	id := GetCharsetID("utf8")
	if id == 0 {
		t.Error("GetCharsetID should return non-zero for utf8")
	}

	id2 := GetCharsetID("latin1")
	if id2 == 0 {
		t.Error("GetCharsetID should return non-zero for latin1")
	}
}

// === GetCommandName Tests ===

func TestGetCommandName(t *testing.T) {
	tests := []struct {
		cmd  uint8
		want string
	}{
		{COM_QUIT, "COM_QUIT"},
		{COM_QUERY, "COM_QUERY"},
		{COM_PING, "COM_PING"},
		{COM_INIT_DB, "COM_INIT_DB"},
		{COM_FIELD_LIST, "COM_FIELD_LIST"},
	}

	for _, tt := range tests {
		name := GetCommandName(tt.cmd)
		if name != tt.want {
			t.Errorf("GetCommandName(0x%02x) = %q, want %q", tt.cmd, name, tt.want)
		}
	}
}

func TestGetCommandName_Unknown(t *testing.T) {
	name := GetCommandName(0xFE)
	if name == "" {
		t.Error("GetCommandName should return non-empty for unknown commands")
	}
}

// === GetBinlogEventTypeName Tests ===

func TestGetBinlogEventTypeName_Unknown(t *testing.T) {
	name := GetBinlogEventTypeName(0xFF)
	if name == "" {
		t.Error("should return non-empty for unknown event type")
	}
}

// === Helper Tests ===

func TestNewHandshakePacket(t *testing.T) {
	pkt := NewHandshakePacket()
	if pkt == nil {
		t.Fatal("NewHandshakePacket returned nil")
	}
	if pkt.ProtocolVersion != 10 {
		t.Errorf("ProtocolVersion = %d, want 10", pkt.ProtocolVersion)
	}
	if pkt.SequenceID != 0 {
		t.Errorf("SequenceID = %d, want 0", pkt.SequenceID)
	}
}

func TestPacket_MarshalBytes(t *testing.T) {
	pkt := &Packet{
		PayloadLength: 3,
		SequenceID:    1,
		Payload:       []byte{0x01, 0x02, 0x03},
	}

	data, err := pkt.MarshalBytes()
	if err != nil {
		t.Fatalf("MarshalBytes error: %v", err)
	}
	if len(data) != 7 { // 4 header + 3 payload
		t.Errorf("len = %d, want 7", len(data))
	}
	if data[3] != 1 {
		t.Errorf("sequence ID = %d, want 1", data[3])
	}
}

func TestPacket_MarshalBytes_NilPayload(t *testing.T) {
	pkt := &Packet{
		PayloadLength: 0,
		SequenceID:    0,
	}

	data, err := pkt.MarshalBytes()
	if err != nil {
		t.Fatalf("MarshalBytes error: %v", err)
	}
	if len(data) != 4 {
		t.Errorf("len = %d, want 4", len(data))
	}
}

func TestPacket_RawBytes(t *testing.T) {
	pkt := &Packet{
		PayloadLength: 3,
		SequenceID:    1,
		Payload:       []byte{0x01, 0x02, 0x03},
	}
	raw := pkt.RawBytes()
	if len(raw) != 7 { // 4 header + 3 payload
		t.Errorf("RawBytes len = %d, want 7", len(raw))
	}
	if raw[3] != 1 {
		t.Errorf("sequence ID = %d, want 1", raw[3])
	}
}

func TestPacket_GetCommandType(t *testing.T) {
	pkt := &Packet{
		Payload: []byte{COM_QUERY, 'S', 'E', 'L'},
	}
	if pkt.GetCommandType() != COM_QUERY {
		t.Errorf("GetCommandType = 0x%02x, want COM_QUERY", pkt.GetCommandType())
	}
}

func TestPacket_GetCommandType_EmptyPayload(t *testing.T) {
	pkt := &Packet{}
	if pkt.GetCommandType() != 0 {
		t.Errorf("GetCommandType with empty payload = 0x%02x, want 0", pkt.GetCommandType())
	}
}

// === Packet Send/ReadPacket with mock conn ===

type mockConn struct {
	buf    bytes.Buffer
	closed bool
}

func (m *mockConn) Read(b []byte) (n int, err error)   { return m.buf.Read(b) }
func (m *mockConn) Write(b []byte) (n int, err error)  { return m.buf.Write(b) }
func (m *mockConn) Close() error                       { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestPacket_Send(t *testing.T) {
	conn := &mockConn{}
	pkt := &Packet{
		PayloadLength: 1,
		SequenceID:    0,
		Payload:       []byte{COM_PING},
	}

	err := pkt.Send(conn)
	if err != nil {
		t.Fatalf("Send error: %v", err)
	}
	if conn.buf.Len() == 0 {
		t.Error("expected data written to connection")
	}
}

func TestReadPacket(t *testing.T) {
	// Build a valid packet manually: length(3) + seqID(1) + payload
	var buf bytes.Buffer
	buf.Write([]byte{0x01, 0x00, 0x00, 0x00, COM_PING}) // 1-byte payload, seqID=0

	conn := &mockConn{buf: buf}
	pkt, err := ReadPacket(conn)
	if err != nil {
		t.Fatalf("ReadPacket error: %v", err)
	}
	if pkt.PayloadLength != 1 {
		t.Errorf("PayloadLength = %d, want 1", pkt.PayloadLength)
	}
	if pkt.SequenceID != 0 {
		t.Errorf("SequenceID = %d, want 0", pkt.SequenceID)
	}
}

func TestSendOK(t *testing.T) {
	conn := &mockConn{}
	err := SendOK(conn, 2)
	if err != nil {
		t.Fatalf("SendOK error: %v", err)
	}
	if conn.buf.Len() == 0 {
		t.Error("expected data written")
	}
}

func TestSendError(t *testing.T) {
	conn := &mockConn{}
	err := SendError(conn, fmt.Errorf("test error"))
	if err != nil {
		t.Fatalf("SendError error: %v", err)
	}
	if conn.buf.Len() == 0 {
		t.Error("expected data written")
	}
}

// === OkInPacket Status Flags ===

func TestOkInPacket_StatusFlags(t *testing.T) {
	pkt := &OkInPacket{StatusFlags: SERVER_STATUS_AUTOCOMMIT}
	if !pkt.IsAutoCommit() {
		t.Error("should be autocommit")
	}
	if pkt.IsInTransaction() {
		t.Error("should not be in transaction")
	}
	if pkt.IsInTransactionReadOnly() {
		t.Error("should not be in read-only transaction")
	}
	if pkt.HasMoreResults() {
		t.Error("should not have more results")
	}
}

func TestOkInPacket_SetFlags(t *testing.T) {
	pkt := &OkInPacket{}

	pkt.SetAutoCommit(true)
	if !pkt.IsAutoCommit() {
		t.Error("should be autocommit after set")
	}
	pkt.SetAutoCommit(false)
	if pkt.IsAutoCommit() {
		t.Error("should not be autocommit after unset")
	}

	pkt.SetInTransaction(true)
	if !pkt.IsInTransaction() {
		t.Error("should be in transaction after set")
	}
	pkt.SetInTransaction(false)

	pkt.SetInTransactionReadOnly(true)
	if !pkt.IsInTransactionReadOnly() {
		t.Error("should be in read-only transaction after set")
	}
	pkt.SetInTransactionReadOnly(false)

	pkt.SetMoreResults(true)
	if !pkt.HasMoreResults() {
		t.Error("should have more results after set")
	}
	pkt.SetMoreResults(false)

	pkt.SetSessionStateChanged(true)
	pkt.SetSessionStateChanged(false)
}

func TestOkInPacket_GetStatusFlagsDescription(t *testing.T) {
	pkt := &OkInPacket{StatusFlags: SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS}
	desc := pkt.GetStatusFlagsDescription()
	if len(desc) == 0 {
		t.Error("description should not be empty")
	}
}

// === EofInPacket Status Flags ===

func TestEofInPacket_StatusFlags(t *testing.T) {
	pkt := &EofInPacket{StatusFlags: SERVER_STATUS_AUTOCOMMIT}
	if !pkt.IsAutoCommit() {
		t.Error("should be autocommit")
	}
}

func TestEofInPacket_SetFlags(t *testing.T) {
	pkt := &EofInPacket{}

	pkt.SetAutoCommit(true)
	if !pkt.IsAutoCommit() {
		t.Error("should be autocommit")
	}
	pkt.SetAutoCommit(false)

	pkt.SetInTransactionReadOnly(true)
	pkt.SetInTransactionReadOnly(false)

	pkt.SetMoreResults(true)
	pkt.SetMoreResults(false)

	pkt.SetSessionStateChanged(true)
	pkt.SetSessionStateChanged(false)
}

func TestEofInPacket_GetStatusFlagsDescription(t *testing.T) {
	pkt := &EofInPacket{StatusFlags: SERVER_STATUS_AUTOCOMMIT}
	desc := pkt.GetStatusFlagsDescription()
	if len(desc) == 0 {
		t.Error("description should not be empty")
	}
}

func TestIsEofPacket_Extra(t *testing.T) {
	// Valid EOF packet: length(3) + seqID(1) + 0xFE header = 5 bytes total, payload length < 9
	pkt := []byte{0x05, 0x00, 0x00, 0x00, 0xFE, 0x00, 0x00, 0x02, 0x00}
	if !IsEofPacket(pkt) {
		t.Error("should be EOF packet")
	}

	// Not EOF - wrong header
	pkt2 := []byte{0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00}
	if IsEofPacket(pkt2) {
		t.Error("should not be EOF packet")
	}

	// Too short
	if IsEofPacket([]byte{0x01, 0x02}) {
		t.Error("too short should not be EOF")
	}
}

// === Command Packet Marshal/Unmarshal ===

func TestComPingPacket_MarshalUnmarshal(t *testing.T) {
	pkt := &ComPingPacket{}
	pkt.Payload = []byte{COM_PING}
	pkt.PayloadLength = 1

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty marshal data")
	}

	pkt2 := &ComPingPacket{}
	err = pkt2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

func TestComQuitPacket_MarshalUnmarshal(t *testing.T) {
	pkt := &ComQuitPacket{}
	pkt.Payload = []byte{COM_QUIT}
	pkt.PayloadLength = 1

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	pkt2 := &ComQuitPacket{}
	err = pkt2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

func TestComRefreshPacket_MarshalUnmarshal(t *testing.T) {
	pkt := &ComRefreshPacket{}
	pkt.Payload = []byte{COM_REFRESH, 0x01}
	pkt.PayloadLength = 2

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	pkt2 := &ComRefreshPacket{}
	err = pkt2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

func TestComShutdownPacket_MarshalUnmarshal(t *testing.T) {
	pkt := &ComShutdownPacket{}
	pkt.Payload = []byte{COM_SHUTDOWN, 0x00}
	pkt.PayloadLength = 2

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	pkt2 := &ComShutdownPacket{}
	err = pkt2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

func TestComStatisticsPacket_MarshalUnmarshal(t *testing.T) {
	pkt := &ComStatisticsPacket{}
	pkt.Payload = []byte{COM_STATISTICS}
	pkt.PayloadLength = 1

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	pkt2 := &ComStatisticsPacket{}
	err = pkt2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

func TestComProcessKillPacket_MarshalUnmarshal(t *testing.T) {
	pkt := &ComProcessKillPacket{}
	pkt.Payload = []byte{COM_PROCESS_KILL, 0x01, 0x00, 0x00, 0x00}
	pkt.PayloadLength = 5
	pkt.ProcessID = 1

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	pkt2 := &ComProcessKillPacket{}
	err = pkt2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

func TestComDebugPacket_MarshalUnmarshal(t *testing.T) {
	pkt := &ComDebugPacket{}
	pkt.Payload = []byte{COM_DEBUG}
	pkt.PayloadLength = 1

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	pkt2 := &ComDebugPacket{}
	err = pkt2.Unmarshal(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
}

// === ErrorPacket Marshal ===

func TestErrorPacket_Marshal(t *testing.T) {
	pkt := &ErrorPacket{}
	pkt.SequenceID = 1
	pkt.Header = 0xFF
	pkt.ErrorCode = 1064
	pkt.SqlStateMarker = "#"
	pkt.SqlState = "42000"
	pkt.ErrorMessage = "syntax error"

	data, err := pkt.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty marshal data")
	}
	// Check header byte at position 4
	if data[4] != 0xFF {
		t.Errorf("header = 0x%02x, want 0xFF", data[4])
	}
}

// === Type Helpers - WriteLenencNumber ===

func TestWriteLenencNumber(t *testing.T) {
	tests := []struct {
		name string
		val  uint64
	}{
		{"zero", 0},
		{"small", 100},
		{"boundary_250", 250},
		{"boundary_251", 251},
		{"two_byte", 1000},
		{"boundary_65535", 65535},
		{"three_byte", 65536},
		{"large", 16777216},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteLenencNumber(buf, tt.val)
			if err != nil {
				t.Fatalf("WriteLenencNumber error: %v", err)
			}
			if buf.Len() == 0 {
				t.Fatal("expected data written")
			}
		})
	}
}

// === Type Helpers - ReadLenencNumber ===

func TestReadLenencNumber(t *testing.T) {
	// 1-byte value
	val, err := ReadLenencNumber[uint64](bytes.NewReader([]byte{100}))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if val != 100 {
		t.Errorf("val=%d, want 100", val)
	}

	// 2-byte marker (0xfc)
	val, err = ReadLenencNumber[uint64](bytes.NewReader([]byte{0xfc, 0xe8, 0x03}))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if val != 1000 {
		t.Errorf("val=%d, want 1000", val)
	}

	// 3-byte marker (0xfd)
	val, err = ReadLenencNumber[uint64](bytes.NewReader([]byte{0xfd, 0x00, 0x00, 0x01}))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if val != 65536 {
		t.Errorf("val=%d, want 65536", val)
	}

	// 8-byte marker (0xfe)
	val, err = ReadLenencNumber[uint64](bytes.NewReader([]byte{0xfe, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if val != 16777216 {
		t.Errorf("val=%d, want 16777216", val)
	}
}

// === Type Helpers - WriteStringByLenenc ===

func TestWriteStringByLenenc(t *testing.T) {
	buf := &bytes.Buffer{}
	err := WriteStringByLenenc(buf, "hello")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("expected data written")
	}
}

func TestWriteStringByLenenc_Empty(t *testing.T) {
	buf := &bytes.Buffer{}
	err := WriteStringByLenenc(buf, "")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestWriteStringByLenenc_Long(t *testing.T) {
	buf := &bytes.Buffer{}
	longStr := make([]byte, 300)
	for i := range longStr {
		longStr[i] = 'a'
	}
	err := WriteStringByLenenc(buf, string(longStr))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

// === Type Helpers - WriteNumber ===

func TestWriteNumber(t *testing.T) {
	buf := &bytes.Buffer{}
	if err := WriteNumber(buf, uint64(0xFF), 1); err != nil {
		t.Fatalf("WriteNumber 1-byte error: %v", err)
	}
	if buf.Len() != 1 {
		t.Errorf("len = %d, want 1", buf.Len())
	}

	buf.Reset()
	if err := WriteNumber(buf, uint64(0xFFFF), 2); err != nil {
		t.Fatalf("WriteNumber 2-byte error: %v", err)
	}
	if buf.Len() != 2 {
		t.Errorf("len = %d, want 2", buf.Len())
	}

	buf.Reset()
	if err := WriteNumber(buf, uint64(0xFFFFFFFF), 4); err != nil {
		t.Fatalf("WriteNumber 4-byte error: %v", err)
	}
	if buf.Len() != 4 {
		t.Errorf("len = %d, want 4", buf.Len())
	}
}

// === CreateOkPacketWithStatus ===

func TestCreateOkPacketWithStatus(t *testing.T) {
	pkt := CreateOkPacketWithStatus(1, 0, true, false)
	if pkt == nil {
		t.Fatal("CreateOkPacketWithStatus returned nil")
	}
	if pkt.OkInPacket.AffectedRows != 1 {
		t.Errorf("AffectedRows = %d, want 1", pkt.OkInPacket.AffectedRows)
	}
	if !pkt.OkInPacket.IsAutoCommit() {
		t.Error("should be autocommit")
	}
}

// === NewReader ===

func TestNewReader(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03}
	r := NewReader(data)
	if r == nil {
		t.Fatal("NewReader returned nil")
	}
}
