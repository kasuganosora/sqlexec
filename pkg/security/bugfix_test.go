package security

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==========================================================================
// Bug 6 (P0): Permission constants use sequential iota, not bit flags
// GrantPermission/HasPermission/RevokePermission use bitwise operations
// (|, &, &^) but the constants are sequential (0,1,2,3...) instead of
// powers of 2 (0,1,2,4,8...). This causes permission confusion:
//   - PermissionRead|PermissionWrite = 1|2 = 3 = PermissionDelete
//   - HasPermission(Delete) returns true when only Read+Write granted
// ==========================================================================

func TestBug6_Permission_GrantAndCheck(t *testing.T) {
	am := NewAuthorizationManager()
	require.NoError(t, am.CreateUser("alice", "hash", []Role{RoleGuest}))

	// Grant Read and Write on "users" table
	require.NoError(t, am.GrantPermission("alice", PermissionRead, "users"))
	require.NoError(t, am.GrantPermission("alice", PermissionWrite, "users"))

	// Should have Read and Write
	assert.True(t, am.HasPermission("alice", PermissionRead, "users"),
		"alice should have Read permission")
	assert.True(t, am.HasPermission("alice", PermissionWrite, "users"),
		"alice should have Write permission")

	// Should NOT have Delete — this is the critical check
	assert.False(t, am.HasPermission("alice", PermissionDelete, "users"),
		"alice should NOT have Delete permission (only Read+Write were granted)")
}

func TestBug6_Permission_GrantMultiple_NoConfusion(t *testing.T) {
	am := NewAuthorizationManager()
	require.NoError(t, am.CreateUser("bob", "hash", []Role{RoleGuest}))

	// Grant Create and Alter
	require.NoError(t, am.GrantPermission("bob", PermissionCreate, "orders"))
	require.NoError(t, am.GrantPermission("bob", PermissionAlter, "orders"))

	assert.True(t, am.HasPermission("bob", PermissionCreate, "orders"))
	assert.True(t, am.HasPermission("bob", PermissionAlter, "orders"))

	// Should NOT have other permissions
	assert.False(t, am.HasPermission("bob", PermissionDrop, "orders"),
		"bob should NOT have Drop permission")
	assert.False(t, am.HasPermission("bob", PermissionGrant, "orders"),
		"bob should NOT have Grant permission")
}

func TestBug6_Permission_Revoke(t *testing.T) {
	am := NewAuthorizationManager()
	require.NoError(t, am.CreateUser("carol", "hash", []Role{RoleGuest}))

	// Grant Read, Write, Delete
	require.NoError(t, am.GrantPermission("carol", PermissionRead, "products"))
	require.NoError(t, am.GrantPermission("carol", PermissionWrite, "products"))
	require.NoError(t, am.GrantPermission("carol", PermissionDelete, "products"))

	// Revoke Write
	require.NoError(t, am.RevokePermission("carol", PermissionWrite, "products"))

	// Should still have Read and Delete
	assert.True(t, am.HasPermission("carol", PermissionRead, "products"),
		"carol should still have Read after revoking Write")
	assert.True(t, am.HasPermission("carol", PermissionDelete, "products"),
		"carol should still have Delete after revoking Write")
	// Should NOT have Write
	assert.False(t, am.HasPermission("carol", PermissionWrite, "products"),
		"carol should NOT have Write after it was revoked")
}

// ==========================================================================
// Bug 7 (P1): LogError panics when err is nil
// LogError calls err.Error() without checking for nil, causing a panic.
// ==========================================================================

func TestBug7_LogError_NilErr(t *testing.T) {
	al := NewAuditLogger(100)

	// Should not panic
	assert.NotPanics(t, func() {
		al.LogError("trace-1", "admin", "testdb", "something went wrong", nil)
	}, "LogError should not panic when err is nil")

	// Verify the event was logged
	events := al.GetEventsByType(EventTypeError)
	require.NotEmpty(t, events)
	assert.Equal(t, "something went wrong", events[0].Message)
}

func TestBug7_LogError_WithErr(t *testing.T) {
	al := NewAuditLogger(100)

	al.LogError("trace-2", "admin", "testdb", "query failed",
		assert.AnError)

	events := al.GetEventsByType(EventTypeError)
	require.NotEmpty(t, events)
	assert.Equal(t, assert.AnError.Error(), events[0].Metadata["error"])
}

// ==========================================================================
// Bug 8 (P1): appendSQLArg ignores recursive call error
// Line 251: buf, _ = appendSQLArg(buf, rv.Interface())
// If the dereferenced pointer has an unsupported type, the error is
// silently discarded and the buffer may contain incomplete data.
// ==========================================================================

func TestBug8_AppendSQLArg_RecursiveError(t *testing.T) {
	type unsupported struct{ X int }
	val := &unsupported{X: 42}

	// EscapeSQL should propagate the error from the recursive call
	_, err := EscapeSQL("SELECT %?", val)
	assert.Error(t, err,
		"EscapeSQL should return error for pointer to unsupported type")
}

func TestBug8_AppendSQLArg_NilPointer(t *testing.T) {
	var p *int = nil

	result, err := EscapeSQL("SELECT %?", p)
	require.NoError(t, err)
	assert.Equal(t, "SELECT NULL", result,
		"nil pointer should produce NULL")
}
