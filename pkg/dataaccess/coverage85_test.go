package dataaccess

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_RegisterDataSource_Success(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	newDs := &TestDataSource{}
	err := manager.RegisterDataSource("test_source", newDs)
	require.NoError(t, err)

	retrievedDs, err := manager.GetDataSource("test_source")
	require.NoError(t, err)
	assert.Equal(t, newDs, retrievedDs)
}

func TestRouter_AddRoute_ThenGet(t *testing.T) {
	router := NewRouter()

	router.AddRoute("table1", "source1")

	routes := router.GetRoutes()
	assert.Equal(t, 1, len(routes))
	assert.Equal(t, "source1", routes["table1"])
}

func TestRouter_RemoveRoute_AndVerify(t *testing.T) {
	router := NewRouter()

	router.AddRoute("table1", "source1")
	router.AddRoute("table2", "source2")

	router.RemoveRoute("table1")

	routes := router.GetRoutes()
	assert.Equal(t, 1, len(routes))
	assert.NotContains(t, routes, "table1")
	assert.Contains(t, routes, "table2")
}

func TestManager_AcquireAndRelease(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	err := manager.AcquireConnection("conn1")
	require.NoError(t, err)

	err = manager.AcquireConnection("conn1")
	assert.Error(t, err)

	manager.ReleaseConnection("conn1")

	err = manager.AcquireConnection("conn1")
	assert.NoError(t, err)
}

func TestManager_GetStats_AfterRegister(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	newDs := &TestDataSource{}
	_ = manager.RegisterDataSource("source1", newDs)

	stats := manager.GetStats()
	assert.Equal(t, 2, stats["data_sources"])
}

func TestRouter_AddMultipleRoutes(t *testing.T) {
	router := NewRouter()

	router.AddRoute("table1", "source1")
	router.AddRoute("table2", "source2")
	router.AddRoute("table3", "source3")

	routes := router.GetRoutes()
	assert.Equal(t, 3, len(routes))
}

func TestRouter_SetDefaultMultipleTimes(t *testing.T) {
	router := NewRouter()

	for i := 1; i <= 5; i++ {
		name := "source" + string(rune('0'+i))
		router.SetDefaultDataSource(name)
		assert.Equal(t, name, router.defaultDataSourceName)
	}
}

func TestManager_GetDataSource_AfterRegister(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	newDs := &TestDataSource{}
	_ = manager.RegisterDataSource("source1", newDs)

	retrievedDs, err := manager.GetDataSource("source1")
	require.NoError(t, err)
	assert.NotNil(t, retrievedDs)
	assert.Equal(t, newDs, retrievedDs)
}

func TestManager_HealthCheck_Default(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	ctx := context.Background()
	health := manager.HealthCheck(ctx)

	assert.NotNil(t, health)
	assert.Contains(t, health, "default")
	assert.True(t, len(health) > 0)
}

func TestRouter_GetRoutes_ReturnsCopy(t *testing.T) {
	router := NewRouter()

	router.AddRoute("table1", "source1")

	routes1 := router.GetRoutes()

	router.AddRoute("table2", "source2")

	routes2 := router.GetRoutes()

	// 验证 routes1 不包含 table2
	_, contains := routes1["table2"]
	assert.False(t, contains)

	// 验证 routes2 包含 table1 和 table2
	_, contains = routes2["table1"]
	assert.True(t, contains)
	_, contains = routes2["table2"]
	assert.True(t, contains)
}

func TestManager_ConcurrentAccess(t *testing.T) {
	ds := &TestDataSource{}
	manager := NewManager(ds)

	// 并发访问数据源
	done := make(chan bool)
	go func() {
		_, _ = manager.GetDataSource("default")
		done <- true
	}()

	go func() {
		stats := manager.GetStats()
		assert.NotNil(t, stats)
		done <- true
	}()

	// 等待两个 goroutine 完成
	<-done
	<-done
}
