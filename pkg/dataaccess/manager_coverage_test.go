package dataaccess

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_HealthCheck_MultipleSources(t *testing.T) {
	ds1 := &TestDataSource{}
	manager := NewManager(ds1)

	ds2 := &TestDataSource{}
	err := manager.RegisterDataSource("source2", ds2)
	require.NoError(t, err)

	ctx := context.Background()
	health := manager.HealthCheck(ctx)

	assert.NotNil(t, health)
	assert.Contains(t, health, "default")
	assert.Contains(t, health, "source2")
}

func TestManager_GetDataSource_Default(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	retrievedDs, err := manager.GetDataSource("default")
	require.NoError(t, err)
	assert.NotNil(t, retrievedDs)
}

func TestManager_GetStats_WithConnections(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	_ = manager.AcquireConnection("conn1")
	_ = manager.AcquireConnection("conn2")

	stats := manager.GetStats()
	assert.Equal(t, 2, stats["connections"])
}

func TestManager_GetStats_WithMultipleSources(t *testing.T) {
	ds1 := &TestDataSource{}
	manager := NewManager(ds1)

	ds2 := &TestDataSource{}
	_ = manager.RegisterDataSource("source2", ds2)

	stats := manager.GetStats()
	assert.Equal(t, 2, stats["data_sources"])
}

func TestManager_HealthCheck_AllSources(t *testing.T) {
	ds1 := &TestDataSource{}
	manager := NewManager(ds1)

	ds2 := &TestDataSource{}
	_ = manager.RegisterDataSource("source2", ds2)

	ctx := context.Background()
	health := manager.HealthCheck(ctx)

	assert.Equal(t, 2, len(health))
	assert.True(t, health["default"])
	assert.True(t, health["source2"])
}

func TestManager_GetDataSource_AfterRegisterMultiple(t *testing.T) {
	ds1 := &TestDataSource{}
	manager := NewManager(ds1)

	ds2 := &TestDataSource{}
	ds3 := &TestDataSource{}

	_ = manager.RegisterDataSource("source1", ds2)
	_ = manager.RegisterDataSource("source2", ds3)

	retrievedDs1, _ := manager.GetDataSource("source1")
	retrievedDs2, _ := manager.GetDataSource("source2")

	assert.NotNil(t, retrievedDs1)
	assert.NotNil(t, retrievedDs2)
}
