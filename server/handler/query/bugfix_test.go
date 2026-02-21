package query

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
// Bug 13 (P1): FieldListHandler returns raw error instead of MySQL error packet
// When the packet type check fails, FieldListHandler returns fmt.Errorf(...)
// directly instead of calling ctx.SendError(). This means the client receives
// no MySQL error packet and the connection may hang.
// All other handlers (QueryHandler, InitDBHandler, ProcessKillHandler) use
// ctx.SendError() for packet type errors.
// ==========================================================================

func TestBug13_FieldListHandler_InvalidPacket_SendsErrorPacket(t *testing.T) {
	ctx, conn, _ := newBugfixTestContext()
	ctx.Session.ResetSequenceID()

	h := NewFieldListHandler(nil)

	// Pass wrong packet type (should trigger error)
	wrongPacket := &protocol.ComQueryPacket{}
	err := h.Handle(ctx, wrongPacket)

	// After fix: SendError returns nil on success (error packet was sent to client)
	assert.NoError(t, err, "handler should send error packet via ctx.SendError, not return raw error")

	// Verify an error packet was actually written to the connection
	written := conn.GetWrittenData()
	assert.GreaterOrEqual(t, len(written), 1, "should have written error packet to connection")
	if len(written) > 0 {
		// Error packet header byte is 0xFF at position 4 (after 4-byte packet header)
		assert.Equal(t, byte(0xFF), written[0][4], "should be a MySQL error packet (0xFF header)")
	}
}

// ==========================================================================
// Bug 14 (P2): InitDBHandler and FieldListHandler consume seqID too early
// Both handlers call ctx.GetNextSequenceID() before packet validation.
// If validation fails, ctx.SendError() internally calls GetNextSequenceID()
// again, resulting in seqID=2 instead of the correct seqID=1.
// ==========================================================================

func TestBug14_InitDBHandler_ErrorPacket_CorrectSeqID(t *testing.T) {
	ctx, conn, _ := newBugfixTestContext()
	ctx.Session.ResetSequenceID()

	h := NewInitDBHandler(nil)

	// Pass wrong packet type to trigger error path
	wrongPacket := &protocol.ComQueryPacket{}
	_ = h.Handle(ctx, wrongPacket)

	// The error packet should have seqID=1 (first response after reset)
	written := conn.GetWrittenData()
	if assert.GreaterOrEqual(t, len(written), 1, "should have written error packet") {
		seqID := written[0][3] // MySQL packet: [3-byte length][1-byte seqID][payload]
		assert.Equal(t, byte(1), seqID,
			"error packet seqID should be 1, not 2 (early consumption bug)")
	}
}

func TestBug14_FieldListHandler_ErrorPacket_CorrectSeqID(t *testing.T) {
	ctx, conn, _ := newBugfixTestContext()
	ctx.Session.ResetSequenceID()

	h := NewFieldListHandler(nil)

	// Pass wrong packet type to trigger error path
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
