package handler

import (
	"testing"

	"github.com/kasuganosora/sqlexec/server/testing/mock"
)

// ==========================================================================
// Bug 15 (P2): Debug log messages always printed, no configurable switch
// [DEBUG] log messages in handler.go are always emitted when Logger is non-nil.
// There should be a DebugEnabled flag to control debug output (default on,
// configurable off via server config).
// ==========================================================================

func TestBug15_DebugLog_Enabled(t *testing.T) {
	ctx, _, logger := newTestContext()
	ctx.DebugEnabled = true

	ctx.DebugLog("test debug message %d", 42)

	if !logger.ContainsLog("[DEBUG] test debug message 42") {
		t.Error("DebugLog should emit log when DebugEnabled is true")
	}
}

func TestBug15_DebugLog_Disabled(t *testing.T) {
	ctx, _, logger := newTestContext()
	ctx.DebugEnabled = false

	ctx.DebugLog("should not appear %d", 42)

	if logger.ContainsLog("should not appear") {
		t.Error("DebugLog should NOT emit log when DebugEnabled is false")
	}
}

func TestBug15_DebugLog_DefaultEnabled(t *testing.T) {
	sess := &mockSessionForDebug{}
	conn := mock.NewMockConnection()
	logger := mock.NewMockLogger()

	ctx := &HandlerContext{
		Session:      nil,
		Connection:   conn,
		Logger:       logger,
		DebugEnabled: true, // default should be true
	}
	_ = sess // just to silence import

	ctx.DebugLog("debug is on by default")

	if !logger.ContainsLog("[DEBUG] debug is on by default") {
		t.Error("DebugLog should work when DebugEnabled is true")
	}
}

func TestBug15_SendOK_UsesDebugLog(t *testing.T) {
	ctx, _, logger := newTestContext()
	ctx.Session.ResetSequenceID()
	ctx.DebugEnabled = false

	// When debug is disabled, SendOK should NOT emit [DEBUG] messages
	err := ctx.SendOK()
	if err != nil {
		t.Fatalf("SendOK error: %v", err)
	}

	for _, log := range logger.GetLogs() {
		if len(log) >= 7 && log[:7] == "[DEBUG]" {
			t.Errorf("SendOK should not emit [DEBUG] messages when debug is disabled, got: %s", log)
		}
	}
}

type mockSessionForDebug struct{}
