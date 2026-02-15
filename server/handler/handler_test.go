package handler

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/testing/mock"
)

func newTestContext() (*HandlerContext, *mock.MockConnection, *mock.MockLogger) {
	sess := &session.Session{
		ID: "test-session",
	}

	conn := mock.NewMockConnection()
	logger := mock.NewMockLogger()

	ctx := &HandlerContext{
		Session:    sess,
		Connection: conn,
		Logger:     logger,
	}
	return ctx, conn, logger
}

// === HandlerContext Tests ===

func TestNewHandlerContext(t *testing.T) {
	sess := &session.Session{
		ID: "test-session",
	}

	conn := mock.NewMockConnection()
	logger := mock.NewMockLogger()

	ctx := NewHandlerContext(sess, conn, 0x03, logger)
	if ctx == nil {
		t.Fatal("NewHandlerContext returned nil")
	}
	if ctx.Session != sess {
		t.Error("Session not set")
	}
	if ctx.Command != 0x03 {
		t.Errorf("Command = 0x%02x, want 0x03", ctx.Command)
	}
}

func TestHandlerContext_SendOK(t *testing.T) {
	ctx, conn, _ := newTestContext()
	ctx.Session.ResetSequenceID()

	err := ctx.SendOK()
	if err != nil {
		t.Fatalf("SendOK error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) != 1 {
		t.Fatalf("expected 1 write, got %d", len(written))
	}
	// OK packet header byte is at position 4 (after 4-byte packet header)
	if written[0][4] != 0x00 {
		t.Errorf("OK packet header = 0x%02x, want 0x00", written[0][4])
	}
}

func TestHandlerContext_SendOKWithSequenceID(t *testing.T) {
	ctx, conn, _ := newTestContext()

	err := ctx.SendOKWithSequenceID(5)
	if err != nil {
		t.Fatalf("SendOKWithSequenceID error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) != 1 {
		t.Fatalf("expected 1 write, got %d", len(written))
	}
	// Sequence ID is at position 3 in MySQL packet
	if written[0][3] != 5 {
		t.Errorf("sequence ID = %d, want 5", written[0][3])
	}
}

func TestHandlerContext_SendOKWithRows(t *testing.T) {
	ctx, conn, _ := newTestContext()
	ctx.Session.ResetSequenceID()

	err := ctx.SendOKWithRows(10, 42)
	if err != nil {
		t.Fatalf("SendOKWithRows error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) != 1 {
		t.Fatalf("expected 1 write, got %d", len(written))
	}
	// Verify it's an OK packet
	if written[0][4] != 0x00 {
		t.Errorf("OK packet header = 0x%02x, want 0x00", written[0][4])
	}
}

func TestHandlerContext_SendError(t *testing.T) {
	ctx, conn, logger := newTestContext()
	ctx.Session.ResetSequenceID()

	testErr := errors.New("test error message")
	err := ctx.SendError(testErr)
	if err != nil {
		t.Fatalf("SendError error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) != 1 {
		t.Fatalf("expected 1 write, got %d", len(written))
	}
	// Error packet header byte at position 4
	if written[0][4] != 0xFF {
		t.Errorf("Error packet header = 0x%02x, want 0xFF", written[0][4])
	}

	// Logger should have recorded the error
	if !logger.ContainsLog("test error message") {
		t.Error("logger should contain error message")
	}
}

func TestHandlerContext_SendError_WriteError(t *testing.T) {
	ctx, conn, _ := newTestContext()
	ctx.Session.ResetSequenceID()
	conn.SetWriteError(errors.New("write failed"))

	err := ctx.SendError(errors.New("some error"))
	if err == nil {
		t.Fatal("expected write error")
	}
}

func TestHandlerContext_ResetSequenceID(t *testing.T) {
	ctx, _, _ := newTestContext()

	// Advance sequence ID
	ctx.GetNextSequenceID()
	ctx.GetNextSequenceID()

	ctx.ResetSequenceID()

	seqID := ctx.GetNextSequenceID()
	if seqID != 1 { // After reset, first call returns 1 (increments then returns)
		t.Errorf("after reset, GetNextSequenceID = %d, want 1", seqID)
	}
}

func TestHandlerContext_GetNextSequenceID(t *testing.T) {
	ctx, _, _ := newTestContext()
	ctx.Session.ResetSequenceID()

	ids := make([]uint8, 5)
	for i := range ids {
		ids[i] = ctx.GetNextSequenceID()
	}

	for i, id := range ids {
		expected := uint8(i + 1) // Session.GetNextSequenceID increments first
		if id != expected {
			t.Errorf("ids[%d] = %d, want %d", i, id, expected)
		}
	}
}

func TestHandlerContext_GetNextSequenceID_NilSession(t *testing.T) {
	ctx := &HandlerContext{Session: nil}
	seqID := ctx.GetNextSequenceID()
	if seqID != 0 {
		t.Errorf("with nil session, GetNextSequenceID = %d, want 0", seqID)
	}
}

func TestHandlerContext_ResetSequenceID_NilSession(t *testing.T) {
	ctx := &HandlerContext{Session: nil}
	// Should not panic
	ctx.ResetSequenceID()
}

func TestHandlerContext_Log(t *testing.T) {
	ctx, _, logger := newTestContext()
	ctx.Log("hello %s", "world")

	if !logger.ContainsLog("hello world") {
		t.Error("logger should contain 'hello world'")
	}
}

func TestHandlerContext_Log_NilLogger(t *testing.T) {
	ctx := &HandlerContext{Logger: nil}
	// Should not panic
	ctx.Log("test %d", 42)
}

func TestHandlerContext_SetDB(t *testing.T) {
	ctx, _, _ := newTestContext()
	if ctx.DB != nil {
		t.Error("DB should be nil initially")
	}
	// SetDB is a simple setter - just verify it doesn't panic
	ctx.SetDB(nil)
}

// === HandlerError Tests ===

func TestNewHandlerError(t *testing.T) {
	err := NewHandlerError("something failed")
	if err == nil {
		t.Fatal("NewHandlerError returned nil")
	}
	if err.Error() != "something failed" {
		t.Errorf("Error() = %q, want %q", err.Error(), "something failed")
	}
}

func TestHandlerError_ImplementsError(t *testing.T) {
	var err error = NewHandlerError("test")
	if err == nil {
		t.Fatal("HandlerError should implement error interface")
	}
}

// === HandlerRegistry Tests ===

// mockHandler is a simple test handler
type mockHandler struct {
	cmd  uint8
	name string
	fn   func(ctx *HandlerContext, packet interface{}) error
}

func (h *mockHandler) Handle(ctx *HandlerContext, packet interface{}) error {
	if h.fn != nil {
		return h.fn(ctx, packet)
	}
	return nil
}
func (h *mockHandler) Command() uint8 { return h.cmd }
func (h *mockHandler) Name() string   { return h.name }

func TestNewHandlerRegistry(t *testing.T) {
	logger := mock.NewMockLogger()
	reg := NewHandlerRegistry(logger)
	if reg == nil {
		t.Fatal("NewHandlerRegistry returned nil")
	}
	if reg.Count() != 0 {
		t.Errorf("Count = %d, want 0", reg.Count())
	}
}

func TestHandlerRegistry_Register(t *testing.T) {
	logger := mock.NewMockLogger()
	reg := NewHandlerRegistry(logger)

	h := &mockHandler{cmd: 0x01, name: "test"}
	err := reg.Register(h)
	if err != nil {
		t.Fatalf("Register error: %v", err)
	}
	if reg.Count() != 1 {
		t.Errorf("Count = %d, want 1", reg.Count())
	}
}

func TestHandlerRegistry_Register_Nil(t *testing.T) {
	reg := NewHandlerRegistry(nil)
	err := reg.Register(nil)
	if err == nil {
		t.Fatal("Register(nil) should return error")
	}
}

func TestHandlerRegistry_Register_Duplicate(t *testing.T) {
	reg := NewHandlerRegistry(nil)
	h1 := &mockHandler{cmd: 0x01, name: "first"}
	h2 := &mockHandler{cmd: 0x01, name: "second"}

	reg.Register(h1)
	err := reg.Register(h2)
	if err == nil {
		t.Fatal("Register duplicate should return error")
	}
}

func TestHandlerRegistry_Get(t *testing.T) {
	reg := NewHandlerRegistry(nil)
	h := &mockHandler{cmd: 0x03, name: "query"}
	reg.Register(h)

	got, ok := reg.Get(0x03)
	if !ok {
		t.Fatal("Get should find registered handler")
	}
	if got.Name() != "query" {
		t.Errorf("Name = %q, want %q", got.Name(), "query")
	}
}

func TestHandlerRegistry_Get_NotFound(t *testing.T) {
	reg := NewHandlerRegistry(nil)
	_, ok := reg.Get(0xFF)
	if ok {
		t.Fatal("Get should not find unregistered handler")
	}
}

func TestHandlerRegistry_Handle(t *testing.T) {
	reg := NewHandlerRegistry(nil)
	called := false
	h := &mockHandler{cmd: 0x01, name: "test", fn: func(ctx *HandlerContext, packet interface{}) error {
		called = true
		return nil
	}}
	reg.Register(h)

	ctx, _, _ := newTestContext()
	err := reg.Handle(ctx, 0x01, nil)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestHandlerRegistry_Handle_NotRegistered(t *testing.T) {
	logger := mock.NewMockLogger()
	reg := NewHandlerRegistry(logger)

	ctx, _, _ := newTestContext()
	err := reg.Handle(ctx, 0xFF, nil)
	if err == nil {
		t.Fatal("Handle unregistered should return error")
	}
}

func TestHandlerRegistry_List(t *testing.T) {
	reg := NewHandlerRegistry(nil)
	reg.Register(&mockHandler{cmd: 0x01, name: "h1"})
	reg.Register(&mockHandler{cmd: 0x02, name: "h2"})
	reg.Register(&mockHandler{cmd: 0x03, name: "h3"})

	list := reg.List()
	if len(list) != 3 {
		t.Errorf("List length = %d, want 3", len(list))
	}
}

// === PacketParserRegistry Tests ===

type mockParser struct {
	cmd  uint8
	name string
}

func (p *mockParser) Command() uint8 { return p.cmd }
func (p *mockParser) Name() string   { return p.name }
func (p *mockParser) Parse(pkt *protocol.Packet) (interface{}, error) {
	return "parsed", nil
}

func TestNewPacketParserRegistry(t *testing.T) {
	reg := NewPacketParserRegistry(nil)
	if reg == nil {
		t.Fatal("NewPacketParserRegistry returned nil")
	}
	if reg.Count() != 0 {
		t.Errorf("Count = %d, want 0", reg.Count())
	}
}

func TestPacketParserRegistry_Register(t *testing.T) {
	logger := mock.NewMockLogger()
	reg := NewPacketParserRegistry(logger)

	p := &mockParser{cmd: 0x03, name: "query"}
	err := reg.Register(p)
	if err != nil {
		t.Fatalf("Register error: %v", err)
	}
	if reg.Count() != 1 {
		t.Errorf("Count = %d, want 1", reg.Count())
	}
}

func TestPacketParserRegistry_Register_Nil(t *testing.T) {
	reg := NewPacketParserRegistry(nil)
	err := reg.Register(nil)
	if err == nil {
		t.Fatal("Register(nil) should return error")
	}
}

func TestPacketParserRegistry_Register_Duplicate(t *testing.T) {
	reg := NewPacketParserRegistry(nil)
	p1 := &mockParser{cmd: 0x03, name: "first"}
	p2 := &mockParser{cmd: 0x03, name: "second"}

	reg.Register(p1)
	err := reg.Register(p2)
	if err == nil {
		t.Fatal("Register duplicate should return error")
	}
}

func TestPacketParserRegistry_Get(t *testing.T) {
	reg := NewPacketParserRegistry(nil)
	p := &mockParser{cmd: 0x03, name: "query"}
	reg.Register(p)

	got, ok := reg.Get(0x03)
	if !ok {
		t.Fatal("Get should find registered parser")
	}
	if got.Name() != "query" {
		t.Errorf("Name = %q, want %q", got.Name(), "query")
	}
}

func TestPacketParserRegistry_Get_NotFound(t *testing.T) {
	reg := NewPacketParserRegistry(nil)
	_, ok := reg.Get(0xFF)
	if ok {
		t.Fatal("Get should not find unregistered parser")
	}
}

func TestPacketParserRegistry_Parse(t *testing.T) {
	reg := NewPacketParserRegistry(nil)
	p := &mockParser{cmd: 0x03, name: "query"}
	reg.Register(p)

	result, err := reg.Parse(0x03, &protocol.Packet{})
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if result != "parsed" {
		t.Errorf("Parse result = %v, want %q", result, "parsed")
	}
}

func TestPacketParserRegistry_Parse_NotRegistered(t *testing.T) {
	logger := mock.NewMockLogger()
	reg := NewPacketParserRegistry(logger)

	_, err := reg.Parse(0xFF, &protocol.Packet{})
	if err == nil {
		t.Fatal("Parse unregistered should return error")
	}
}

func TestPacketParserRegistry_List(t *testing.T) {
	reg := NewPacketParserRegistry(nil)
	reg.Register(&mockParser{cmd: 0x01, name: "p1"})
	reg.Register(&mockParser{cmd: 0x02, name: "p2"})

	list := reg.List()
	if len(list) != 2 {
		t.Errorf("List length = %d, want 2", len(list))
	}
}

// === Concurrency Tests ===

func TestHandlerRegistry_ConcurrentAccess(t *testing.T) {
	reg := NewHandlerRegistry(nil)
	var wg sync.WaitGroup

	// Register handlers concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reg.Register(&mockHandler{cmd: uint8(i), name: fmt.Sprintf("h%d", i)})
		}(i)
	}
	wg.Wait()

	if reg.Count() != 10 {
		t.Errorf("Count = %d, want 10", reg.Count())
	}

	// Read concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reg.Get(uint8(i))
			reg.List()
			reg.Count()
		}(i)
	}
	wg.Wait()
}
