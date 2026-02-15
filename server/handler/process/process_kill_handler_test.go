package process

import (
	"fmt"
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

func TestProcessKillHandler_CommandAndName(t *testing.T) {
	h := NewProcessKillHandler(nil)
	if h.Command() != protocol.COM_PROCESS_KILL {
		t.Errorf("Command = 0x%02x, want 0x%02x", h.Command(), protocol.COM_PROCESS_KILL)
	}
	if h.Name() != "COM_PROCESS_KILL" {
		t.Errorf("Name = %q, want %q", h.Name(), "COM_PROCESS_KILL")
	}
}

func TestProcessKillHandler_Constructor(t *testing.T) {
	h := NewProcessKillHandler(nil)
	if h == nil {
		t.Fatal("NewProcessKillHandler returned nil")
	}
	if h.okBuilder == nil {
		t.Fatal("okBuilder should be created when nil passed")
	}
}

func TestProcessKillHandler_Handle_InvalidPacket(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewProcessKillHandler(nil)

	err := h.Handle(ctx, "not a process kill packet")
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

func TestProcessKillHandler_Handle_NonexistentThread(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewProcessKillHandler(nil)

	cmd := &protocol.ComProcessKillPacket{}
	cmd.ProcessID = 99999 // Non-existent thread

	err := h.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Handle should return nil (sends error packet): %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected error packet to be written")
	}
	// Should send error about unknown thread
	if written[0][4] != 0xFF {
		t.Errorf("expected error header 0xFF, got 0x%02x", written[0][4])
	}
}

func TestProcessKillHandler_sendOK(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewProcessKillHandler(nil)

	err := h.sendOK(ctx, 3)
	if err != nil {
		t.Fatalf("sendOK error: %v", err)
	}

	written := conn.GetWrittenData()
	if len(written) == 0 {
		t.Fatal("expected OK packet to be written")
	}
	if written[0][4] != 0x00 {
		t.Errorf("expected OK header 0x00, got 0x%02x", written[0][4])
	}
	if written[0][3] != 3 {
		t.Errorf("sequence ID = %d, want 3", written[0][3])
	}
}

func TestProcessKillHandler_sendOK_WriteError(t *testing.T) {
	ctx, conn, _ := newTestCtx()
	h := NewProcessKillHandler(nil)
	conn.SetWriteError(fmt.Errorf("write failed"))

	err := h.sendOK(ctx, 1)
	if err == nil {
		t.Fatal("expected write error")
	}
}
