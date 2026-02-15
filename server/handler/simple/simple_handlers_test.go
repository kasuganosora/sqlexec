package simple

import (
	"strings"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/testing/mock"
)

func newTestCtx() (*handler.HandlerContext, *mock.MockConnection, *mock.MockLogger) {
	sess := &session.Session{
		ID: "test",
	}
	conn := mock.NewMockConnection()
	logger := mock.NewMockLogger()
	ctx := &handler.HandlerContext{
		Session:    sess,
		Connection: conn,
		Logger:     logger,
	}
	return ctx, conn, logger
}

// === PingHandler ===

func TestPingHandler_Handle(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewPingHandler(nil)

	err := h.Handle(ctx, nil)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet to be written")
	}
	// OK packet header at byte 4
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
}

func TestPingHandler_CommandAndName(t *testing.T) {
	h := NewPingHandler(nil)
	if h.Command() != protocol.COM_PING {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_PING)
	}
	if h.Name() != "COM_PING" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_PING")
	}
}

// === QuitHandler ===

func TestQuitHandler_Handle(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewQuitHandler()

	err := h.Handle(ctx, nil)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	// Quit should NOT write any data
	written := conn.GetWrittenData()
	if len(written) != 0 {
		t.Errorf("expected no data written, got %d writes", len(written))
	}
}

func TestQuitHandler_CommandAndName(t *testing.T) {
	h := NewQuitHandler()
	if h.Command() != protocol.COM_QUIT {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_QUIT)
	}
	if h.Name() != "COM_QUIT" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_QUIT")
	}
}

// === RefreshHandler ===

func TestRefreshHandler_Handle(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewRefreshHandler(nil)

	err := h.Handle(ctx, nil)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet to be written")
	}
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
}

func TestRefreshHandler_CommandAndName(t *testing.T) {
	h := NewRefreshHandler(nil)
	if h.Command() != protocol.COM_REFRESH {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_REFRESH)
	}
	if h.Name() != "COM_REFRESH" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_REFRESH")
	}
}

// === SetOptionHandler ===

func TestSetOptionHandler_Handle_Valid(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewSetOptionHandler(nil)

	pkt := &protocol.ComSetOptionPacket{}
	pkt.OptionOperation = 0

	err := h.Handle(ctx, pkt)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet to be written")
	}
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
}

func TestSetOptionHandler_Handle_InvalidPacket(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewSetOptionHandler(nil)

	// Pass wrong packet type
	err := h.Handle(ctx, "invalid")
	if err != nil {
		t.Fatalf("Handle should not return error (sends error packet instead): %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet to be written")
	}
	// Error packet header at byte 4
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

func TestSetOptionHandler_CommandAndName(t *testing.T) {
	h := NewSetOptionHandler(nil)
	if h.Command() != protocol.COM_SET_OPTION {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_SET_OPTION)
	}
	if h.Name() != "COM_SET_OPTION" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_SET_OPTION")
	}
}

// === DebugHandler ===

func TestDebugHandler_Handle(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewDebugHandler()

	err := h.Handle(ctx, nil)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	// Debug should not write anything
	written := conn.GetWrittenData()
	if len(written) != 0 {
		t.Errorf("expected no data written, got %d writes", len(written))
	}
}

func TestDebugHandler_CommandAndName(t *testing.T) {
	h := NewDebugHandler()
	if h.Command() != protocol.COM_DEBUG {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_DEBUG)
	}
	if h.Name() != "COM_DEBUG" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_DEBUG")
	}
}

// === ShutdownHandler ===

func TestShutdownHandler_Handle(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewShutdownHandler()

	err := h.Handle(ctx, nil)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) != 0 {
		t.Errorf("expected no data written, got %d writes", len(written))
	}
}

func TestShutdownHandler_CommandAndName(t *testing.T) {
	h := NewShutdownHandler()
	if h.Command() != protocol.COM_SHUTDOWN {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_SHUTDOWN)
	}
	if h.Name() != "COM_SHUTDOWN" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_SHUTDOWN")
	}
}

// === StatisticsHandler ===

func TestStatisticsHandler_Handle(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewStatisticsHandler()

	err := h.Handle(ctx, nil)
	if err != nil {
		t.Fatalf("Handle error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected statistics packet to be written")
	}

	// The payload starts at byte 4 (after MySQL packet header)
	payload := string(written[0][4:])
	if !strings.Contains(payload, "Uptime:") {
		t.Errorf("statistics should contain 'Uptime:', got %q", payload)
	}
}

func TestStatisticsHandler_CommandAndName(t *testing.T) {
	h := NewStatisticsHandler()
	if h.Command() != protocol.COM_STATISTICS {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_STATISTICS)
	}
	if h.Name() != "COM_STATISTICS" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_STATISTICS")
	}
}

// === Constructor Tests ===

func TestNewPingHandler_NilBuilder(t *testing.T) {
	h := NewPingHandler(nil)
	if h == nil {
		t.Fatal("NewPingHandler returned nil")
	}
	if h.okBuilder == nil {
		t.Fatal("okBuilder should be created when nil passed")
	}
}

func TestNewRefreshHandler_NilBuilder(t *testing.T) {
	h := NewRefreshHandler(nil)
	if h == nil {
		t.Fatal("NewRefreshHandler returned nil")
	}
	if h.okBuilder == nil {
		t.Fatal("okBuilder should be created when nil passed")
	}
}

func TestNewSetOptionHandler_NilBuilder(t *testing.T) {
	h := NewSetOptionHandler(nil)
	if h == nil {
		t.Fatal("NewSetOptionHandler returned nil")
	}
	if h.okBuilder == nil {
		t.Fatal("okBuilder should be created when nil passed")
	}
}
