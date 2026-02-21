package process

import (
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/session"
	"github.com/kasuganosora/sqlexec/server/handler"
	"github.com/kasuganosora/sqlexec/server/protocol"
	"github.com/kasuganosora/sqlexec/server/testing/mock"
	"github.com/stretchr/testify/assert"
)

func newBugfixTestContext() (*handler.HandlerContext, *mock.MockConnection, *mock.MockLogger) {
	sess := &session.Session{
		ID: "test-session",
	}
	conn := mock.NewMockConnection()
	logger := mock.NewMockLogger()

	ctx := &handler.HandlerContext{
		Session:      sess,
		Connection:   conn,
		Logger:       logger,
		DebugEnabled: true,
	}
	return ctx, conn, logger
}

// ==========================================================================
// Bug 14 (P2): ProcessKillHandler consumes seqID before packet validation
// GetNextSequenceID() is called at line 33, before the packet type check.
// If the kill fails (e.g., nonexistent thread), SendError() internally calls
// GetNextSequenceID() again, resulting in seqID=2 instead of the correct
// seqID=1 in the error response packet.
// ==========================================================================

func TestBug14_ProcessKillHandler_ErrorPacket_CorrectSeqID(t *testing.T) {
	ctx, conn, _ := newBugfixTestContext()
	ctx.Session.ResetSequenceID()

	h := NewProcessKillHandler(nil)

	// Use a nonexistent thread ID — KillQueryByThreadID will fail
	pkt := &protocol.ComProcessKillPacket{ProcessID: 999999}
	_ = h.Handle(ctx, pkt)

	// The error packet should have seqID=1 (first response after reset)
	written := conn.GetWrittenData()
	if assert.GreaterOrEqual(t, len(written), 1, "should have written error packet") {
		seqID := written[0][3] // MySQL packet: [3-byte length][1-byte seqID][payload]
		assert.Equal(t, byte(1), seqID,
			"error packet seqID should be 1, not 2 (early consumption bug)")
	}
}

func TestBug14_ProcessKillHandler_InvalidPacket_CorrectSeqID(t *testing.T) {
	ctx, conn, _ := newBugfixTestContext()
	ctx.Session.ResetSequenceID()

	h := NewProcessKillHandler(nil)

	// Pass wrong packet type to trigger early error path
	wrongPacket := &protocol.ComQueryPacket{}
	_ = h.Handle(ctx, wrongPacket)

	// The error packet should have seqID=1
	written := conn.GetWrittenData()
	if assert.GreaterOrEqual(t, len(written), 1, "should have written error packet") {
		seqID := written[0][3]
		assert.Equal(t, byte(1), seqID,
			"error packet seqID should be 1, not 2 (early consumption bug)")
	}
}
