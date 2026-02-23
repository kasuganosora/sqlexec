package dataaccess

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_Route_WithDefault(t *testing.T) {
	router := NewRouter()
	router.SetManager(&Manager{})

	_, err := router.Route("unknown_table")
	assert.Error(t, err)
}

func TestRouter_GetRoutes_Copy(t *testing.T) {
	router := NewRouter()

	router.AddRoute("table1", "source1")

	routes1 := router.GetRoutes()
	router.AddRoute("table2", "source2")

	routes2 := router.GetRoutes()

	// routes1 不应该包含 table2
	_, exists := routes1["table2"]
	assert.False(t, exists)

	// routes2 应该包含 table1 和 table2
	_, exists = routes2["table1"]
	assert.True(t, exists)
	_, exists = routes2["table2"]
	assert.True(t, exists)
}

func TestRouter_RemoveRoute_NonExistent(t *testing.T) {
	router := NewRouter()

	// 移除不存在的路由不应该 panic
	router.RemoveRoute("nonexistent")

	routes := router.GetRoutes()
	assert.Equal(t, 0, len(routes))
}

func TestRouter_Route_WithRouteAndManager(t *testing.T) {
	router := NewRouter()

	ds := &TestDataSource{}
	manager := NewManager(ds)
	router.SetManager(manager)

	router.AddRoute("table1", "default")

	retrievedDs, err := router.Route("table1")
	require.NoError(t, err)
	assert.NotNil(t, retrievedDs)
}

func TestRouter_Route_MultipleTables(t *testing.T) {
	router := NewRouter()

	ds := &TestDataSource{}
	manager := NewManager(ds)
	router.SetManager(manager)

	router.AddRoute("table1", "default")
	router.AddRoute("table2", "default")
	router.AddRoute("table3", "default")

	for i := 1; i <= 3; i++ {
		tableName := "table" + string(rune('0'+i))
		_, err := router.Route(tableName)
		require.NoError(t, err)
	}
}

func TestRouter_AddRoute_Overwrite(t *testing.T) {
	router := NewRouter()

	router.AddRoute("table1", "source1")
	assert.Equal(t, "source1", router.routes["table1"])

	// 覆盖已存在的路由
	router.AddRoute("table1", "source2")
	assert.Equal(t, "source2", router.routes["table1"])
}
