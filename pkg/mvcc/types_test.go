package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXIDOperations(t *testing.T) {
	// Test XID generation
	xid1 := NextXID(XIDBootstrap)
	xid2 := NextXID(xid1)
	
	assert.Equal(t, XIDBootstrap+1, xid2)
	
	// Test XID comparison
	assert.True(t, xid2 > xid1)
	assert.False(t, xid1 > xid2)
}

func TestXIDComparison(t *testing.T) {
	xid1 := XID(100)
	xid2 := XID(200)
	xid3 := XID(100)
	
	assert.True(t, xid2 > xid1)
	assert.True(t, xid1 >= xid3)
	assert.False(t, xid1 > xid3)
}

func TestTransactionStatusValues(t *testing.T) {
	assert.Equal(t, 0, int(TxnStatusInProgress))
	assert.Equal(t, 1, int(TxnStatusCommitted))
	assert.Equal(t, 2, int(TxnStatusAborted))
}
