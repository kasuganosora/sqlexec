package query

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/testing/mock"
)

func newTestCtx() (*handler.HandlerContext, *mock.MockConnection, *mock.MockLogger) {
	driver := session.NewMemoryDriver()
	mgr := session.NewSessionMgr(context.Background(), driver)
	sess, _ := mgr.CreateSession(context.Background(), "127.0.0.1", "12345")

	conn := mock.NewMockConnection()
	logger := mock.NewMockLogger()
	ctx := &handler.HandlerContext{
		Session:    sess,
		Connection: conn,
		Logger:     logger,
	}
	return ctx, conn, logger
}

// === formatValue Tests ===

func TestFormatValue_Nil(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(nil)
	if result != "___SQL_EXEC_NULL___" {
		t.Errorf("formatValue(nil) = %q, want NULL marker", result)
	}
}

func TestFormatValue_String(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue("hello")
	if result != "hello" {
		t.Errorf("formatValue('hello') = %q, want 'hello'", result)
	}
}

func TestFormatValue_Int(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(42)
	if result != "42" {
		t.Errorf("formatValue(42) = %q, want '42'", result)
	}
}

func TestFormatValue_Int64(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(int64(123456789))
	if result != "123456789" {
		t.Errorf("formatValue(int64) = %q, want '123456789'", result)
	}
}

func TestFormatValue_Uint(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(uint(100))
	if result != "100" {
		t.Errorf("formatValue(uint) = %q, want '100'", result)
	}
}

func TestFormatValue_Float64_Whole(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(float64(10.0))
	if result != "10" {
		t.Errorf("formatValue(10.0) = %q, want '10'", result)
	}
}

func TestFormatValue_Float64_Decimal(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(float64(3.14))
	if result != "3.14" {
		t.Errorf("formatValue(3.14) = %q, want '3.14'", result)
	}
}

func TestFormatValue_Float64_Inf(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(math.Inf(1))
	if result != "+Inf" {
		t.Errorf("formatValue(+Inf) = %q, want '+Inf'", result)
	}
}

func TestFormatValue_Float64_NaN(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(math.NaN())
	if result != "NaN" {
		t.Errorf("formatValue(NaN) = %q, want 'NaN'", result)
	}
}

func TestFormatValue_Float32_Whole(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(float32(5.0))
	if result != "5" {
		t.Errorf("formatValue(float32(5.0)) = %q, want '5'", result)
	}
}

func TestFormatValue_Float32_Decimal(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(float32(2.5))
	if result != "2.5" {
		t.Errorf("formatValue(float32(2.5)) = %q, want '2.5'", result)
	}
}

func TestFormatValue_Bool_True(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(true)
	if result != "1" {
		t.Errorf("formatValue(true) = %q, want '1'", result)
	}
}

func TestFormatValue_Bool_False(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue(false)
	if result != "0" {
		t.Errorf("formatValue(false) = %q, want '0'", result)
	}
}

func TestFormatValue_Default(t *testing.T) {
	h := NewQueryHandler()
	result := h.formatValue([]int{1, 2, 3})
	if result != "[1 2 3]" {
		t.Errorf("formatValue([]int) = %q, want '[1 2 3]'", result)
	}
}

// === mapMySQLType Tests ===

func TestMapMySQLType(t *testing.T) {
	h := NewQueryHandler()

	tests := []struct {
		input    string
		expected byte
	}{
		{"int", protocol.MYSQL_TYPE_LONG},
		{"integer", protocol.MYSQL_TYPE_LONG},
		{"tinyint", protocol.MYSQL_TYPE_TINY},
		{"smallint", protocol.MYSQL_TYPE_SHORT},
		{"bigint", protocol.MYSQL_TYPE_LONGLONG},
		{"float", protocol.MYSQL_TYPE_FLOAT},
		{"double", protocol.MYSQL_TYPE_DOUBLE},
		{"decimal", protocol.MYSQL_TYPE_DECIMAL},
		{"numeric", protocol.MYSQL_TYPE_DECIMAL},
		{"date", protocol.MYSQL_TYPE_DATE},
		{"datetime", protocol.MYSQL_TYPE_DATETIME},
		{"timestamp", protocol.MYSQL_TYPE_TIMESTAMP},
		{"time", protocol.MYSQL_TYPE_TIME},
		{"text", protocol.MYSQL_TYPE_VAR_STRING},
		{"string", protocol.MYSQL_TYPE_VAR_STRING},
		{"boolean", protocol.MYSQL_TYPE_TINY},
		{"bool", protocol.MYSQL_TYPE_TINY},
		{"unknown_type", protocol.MYSQL_TYPE_VAR_STRING},
		{"", protocol.MYSQL_TYPE_VAR_STRING},
	}

	for _, tt := range tests {
		result := h.mapMySQLType(tt.input)
		if result != tt.expected {
			t.Errorf("mapMySQLType(%q) = 0x%02x, want 0x%02x", tt.input, result, tt.expected)
		}
	}
}

// === Handle error path tests ===

func TestQueryHandler_Handle_InvalidPacket(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewQueryHandler()

	err := h.Handle(ctx, "not a query packet")
	if err != nil {
		t.Fatalf("Handle should return nil (sends error packet): %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet to be written")
	}
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

func TestQueryHandler_Handle_NilAPISession(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewQueryHandler()

	cmd := &protocol.ComQueryPacket{}
	cmd.Payload = []byte{protocol.COM_QUERY, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle should return nil (sends error packet): %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet to be written")
	}
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

func TestQueryHandler_Handle_InvalidAPISessionType(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewQueryHandler()

	// Set API session to wrong type
	ctx.Session.APISession = "not-api-session"

	cmd := &protocol.ComQueryPacket{}
	cmd.Payload = []byte{protocol.COM_QUERY, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1'}

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle should return nil (sends error packet): %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet to be written")
	}
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

// === InitDBHandler Tests ===

func TestInitDBHandler_Handle_InvalidPacket(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewInitDBHandler(nil)

	err := h.Handle(ctx, "not an init db packet")
	if err != nil {
		t.Fatalf("Handle should return nil (sends error packet): %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet to be written")
	}
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

func TestInitDBHandler_Handle_WithDBName(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewInitDBHandler(nil)

	cmd := &protocol.ComInitDBPacket{}
	cmd.Payload = []byte{protocol.COM_INIT_DB, 't', 'e', 's', 't', '_', 'd', 'b'}

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	// Should send OK
	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet to be written")
	}
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
}

func TestInitDBHandler_Constructor(t *testing.T) {
	h := NewInitDBHandler(nil)
	if h == nil {
		t.Fatal("NewInitDBHandler returned nil")
	}
	if h.okBuilder == nil {
		t.Fatal("okBuilder should be created when nil passed")
	}
}

func TestQueryHandler_Handle_EmptyPayload(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewQueryHandler()

	// Empty payload (only command byte) — query will be empty string
	cmd := &protocol.ComQueryPacket{}
	cmd.Payload = []byte{protocol.COM_QUERY}

	// APISession is nil so it will error
	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle should return nil (sends error packet): %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet")
	}
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

func TestInitDBHandler_Handle_WithAPISession_WrongType(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewInitDBHandler(nil)

	// Set API session to wrong type
	ctx.Session.APISession = "wrong-type"

	cmd := &protocol.ComInitDBPacket{}
	cmd.Payload = []byte{protocol.COM_INIT_DB, 't', 'e', 's', 't'}

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	// Should still send OK
	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet")
	}
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
}

func TestInitDBHandler_Handle_NilAPISession(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewInitDBHandler(nil)

	// APISession is nil by default
	cmd := &protocol.ComInitDBPacket{}
	cmd.Payload = []byte{protocol.COM_INIT_DB, 'd', 'b'}

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	// Should still send OK
	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet")
	}
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
}

func TestNewQueryHandler(t *testing.T) {
	h := NewQueryHandler()
	if h == nil {
		t.Fatal("NewQueryHandler returned nil")
	}
	if h.resultSetBuilder == nil {
		t.Fatal("resultSetBuilder should be initialized")
	}
}

// === buildFieldPacket Tests ===

func TestBuildFieldPacket(t *testing.T) {
	h := NewQueryHandler()
	col := domain.ColumnInfo{Name: "id", Type: "int"}
	pkt := h.buildFieldPacket(1, col)

	if pkt.SequenceID != 1 {
		t.Errorf("SequenceID = %d, want 1", pkt.SequenceID)
	}
	if pkt.Catalog != "def" {
		t.Errorf("Catalog = %q, want 'def'", pkt.Catalog)
	}
	if pkt.Name != "id" {
		t.Errorf("Name = %q, want 'id'", pkt.Name)
	}
	if pkt.OrgName != "id" {
		t.Errorf("OrgName = %q, want 'id'", pkt.OrgName)
	}
	if pkt.Type != protocol.MYSQL_TYPE_LONG {
		t.Errorf("Type = 0x%02x, want MYSQL_TYPE_LONG", pkt.Type)
	}
	if pkt.CharacterSet != 0xff {
		t.Errorf("CharacterSet = 0x%02x, want 0xff", pkt.CharacterSet)
	}
}

// === buildRowPacket Tests ===

func TestBuildRowPacket(t *testing.T) {
	h := NewQueryHandler()
	columns := []domain.ColumnInfo{
		{Name: "id", Type: "int"},
		{Name: "name", Type: "text"},
	}
	row := domain.Row{
		"id":   42,
		"name": "test",
	}

	data := h.buildRowPacket(1, columns, row)
	if data == nil {
		t.Fatal("buildRowPacket returned nil")
	}
	if len(data) == 0 {
		t.Fatal("buildRowPacket returned empty data")
	}
}

func TestBuildRowPacket_NullValue(t *testing.T) {
	h := NewQueryHandler()
	columns := []domain.ColumnInfo{
		{Name: "id", Type: "int"},
		{Name: "missing", Type: "text"},
	}
	row := domain.Row{
		"id": 1,
		// "missing" is absent → NULL marker
	}

	data := h.buildRowPacket(1, columns, row)
	if data == nil {
		t.Fatal("buildRowPacket returned nil")
	}
}

// === sendQueryResult Tests ===

func TestSendQueryResult(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	ctx.Session.ResetSequenceID()
	h := NewQueryHandler()

	columns := []domain.ColumnInfo{
		{Name: "id", Type: "int"},
		{Name: "name", Type: "text"},
	}
	rows := []domain.Row{
		{"id": 1, "name": "alice"},
		{"id": 2, "name": "bob"},
	}

	err := h.sendQueryResult(ctx, columns, rows)
	if err != nil {
		t.Fatalf("sendQueryResult error: %v", err)
	}

	written := conn.GetWrittenData()
	// Should write: column count + 2 column defs + EOF + 2 rows + EOF = 7 packets
	if len(written) != 7 {
		t.Errorf("expected 7 writes, got %d", len(written))
	}
}

func TestSendQueryResult_EmptyRows(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	ctx.Session.ResetSequenceID()
	h := NewQueryHandler()

	columns := []domain.ColumnInfo{
		{Name: "id", Type: "int"},
	}
	var rows []domain.Row

	err := h.sendQueryResult(ctx, columns, rows)
	if err != nil {
		t.Fatalf("sendQueryResult error: %v", err)
	}

	written := conn.GetWrittenData()
	// Should write: column count + 1 column def + EOF + EOF = 4 packets
	if len(written) != 4 {
		t.Errorf("expected 4 writes, got %d", len(written))
	}
}

func TestSendQueryResult_WriteError(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	ctx.Session.ResetSequenceID()
	h := NewQueryHandler()

	conn.SetWriteError(fmt.Errorf("write failed"))

	columns := []domain.ColumnInfo{{Name: "id", Type: "int"}}
	rows := []domain.Row{{"id": 1}}

	err := h.sendQueryResult(ctx, columns, rows)
	if err == nil {
		t.Fatal("expected write error")
	}
}

// === InitDBHandler with valid DB name ===

func TestInitDBHandler_Handle_WithDBName_SetsSession(t *testing.T) {
	ctx, _, logger := newTestCtx()
	h := NewInitDBHandler(nil)

	cmd := &protocol.ComInitDBPacket{}
	cmd.Payload = []byte{protocol.COM_INIT_DB, 'm', 'y', 'd', 'b'}

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	// Verify logger recorded the DB name
	if !logger.ContainsLog("mydb") {
		t.Error("logger should contain the database name 'mydb'")
	}
}

// === FieldListHandler Tests ===

func TestFieldListHandler_CommandAndName(t *testing.T) {
	h := NewFieldListHandler(nil)
	if h.Command() != protocol.COM_FIELD_LIST {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_FIELD_LIST)
	}
	if h.Name() != "COM_FIELD_LIST" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_FIELD_LIST")
	}
}

func TestFieldListHandler_Constructor(t *testing.T) {
	h := NewFieldListHandler(nil)
	if h == nil {
		t.Fatal("NewFieldListHandler returned nil")
	}
	if h.eofBuilder == nil {
		t.Fatal("eofBuilder should be created when nil passed")
	}
}

func TestFieldListHandler_Handle_Valid(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewFieldListHandler(nil)

	cmd := &protocol.ComFieldListPacket{}
	cmd.Payload = []byte{protocol.COM_FIELD_LIST, 't', 'e', 's', 't'}

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected EOF packet to be written")
	}
	// EOF header at byte 4
	if written[0][4] != 0xFE {
		t.Errorf("expected EOF header 0xFE, got 0x%02x", written[0][4])
	}
}

func TestFieldListHandler_Handle_InvalidPacket(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewFieldListHandler(nil)

	err := h.Handle(ctx, "not a field list packet")
	if err != nil {
		t.Fatalf("Handle should return nil (sends error packet): %v", err)
	}

	// Should send a MySQL error packet (0xFF header) instead of returning raw error
	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet to be written")
	}
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

func TestFieldListHandler_Handle_WriteError(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewFieldListHandler(nil)
	conn.SetWriteError(fmt.Errorf("write failed"))

	cmd := &protocol.ComFieldListPacket{}
	cmd.Payload = []byte{protocol.COM_FIELD_LIST}

	err := h.Handle(ctx, cmd)
	if err == nil {
		t.Fatal("expected write error")
	}
}

// === sendQueryResult error paths ===

func TestSendQueryResult_MultipleColumnsAndRows(t *testing.T) {
	ctx, _, _ := newTestCtx()
	ctx.Session.ResetSequenceID()
	h := NewQueryHandler()

	conn2 := mock.NewMockConnection()
	ctx2 := &handler.HandlerContext{
		Session:    ctx.Session,
		Connection: conn2,
		Logger:     mock.NewMockLogger(),
	}
	ctx2.Session.ResetSequenceID()

	columns := []domain.ColumnInfo{{Name: "id", Type: "int"}, {Name: "name", Type: "text"}}
	rows := []domain.Row{{"id": 1, "name": "test"}}

	err := h.sendQueryResult(ctx2, columns, rows)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildRowPacket_AllNulls(t *testing.T) {
	h := NewQueryHandler()
	columns := []domain.ColumnInfo{
		{Name: "a", Type: "int"},
		{Name: "b", Type: "text"},
		{Name: "c", Type: "float"},
	}
	// Empty row - all columns missing
	row := domain.Row{}

	data := h.buildRowPacket(1, columns, row)
	if data == nil {
		t.Fatal("buildRowPacket returned nil for all-null row")
	}
}

func TestInitDBHandler_Handle_EmptyDBName_FallbackToSession(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewInitDBHandler(nil)

	// Pre-set current_database in session
	ctx.Session.Set("current_database", "fallback_db")

	cmd := &protocol.ComInitDBPacket{}
	cmd.Payload = []byte{protocol.COM_INIT_DB} // Empty DB name

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet")
	}
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
}

func TestSendQueryResult_SingleColumn(t *testing.T) {
	ctx, _, _ := newTestCtx()
	h := NewQueryHandler()

	conn2 := mock.NewMockConnection()
	ctx2 := &handler.HandlerContext{
		Session:    ctx.Session,
		Connection: conn2,
		Logger:     mock.NewMockLogger(),
	}
	ctx2.Session.ResetSequenceID()

	columns := []domain.ColumnInfo{{Name: "val", Type: "text"}}
	rows := []domain.Row{
		{"val": "hello"},
		{"val": nil},
		{"val": true},
	}

	err := h.sendQueryResult(ctx2, columns, rows)
	if err != nil {
		t.Fatalf("sendQueryResult error: %v", err)
	}

	// column count + 1 col def + EOF + 3 rows + EOF = 7 writes
	written := conn2.GetWrittenData()
	if len(written) != 7 {
		t.Errorf("expected 7 writes, got %d", len(written))
	}
}

