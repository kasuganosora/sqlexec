package virtual

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVirtualTableInterface(t *testing.T) {
	// VirtualTable interface is tested through datasource tests
	assert.NotNil(t, NewVirtualDataSource(nil))
}
