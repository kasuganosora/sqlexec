package dataaccess

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRouter(t *testing.T) {
	router := NewRouter()
	
	assert.NotNil(t, router)
	assert.NotNil(t, router.routes)
	assert.Equal(t, "default", router.defaultDataSourceName)
}

func TestRouter_AddRoute(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	routes := router.GetRoutes()
	
	assert.Equal(t, "source1", routes["table1"])
}

func TestRouter_RemoveRoute(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	router.RemoveRoute("table1")
	routes := router.GetRoutes()
	
	_, exists := routes["table1"]
	assert.False(t, exists)
}

func TestRouter_SetDefaultDataSource(t *testing.T) {
	router := NewRouter()
	
	router.SetDefaultDataSource("new_default")
	assert.Equal(t, "new_default", router.defaultDataSourceName)
}

func TestRouter_GetRoutes(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	router.AddRoute("table2", "source2")
	
	routes := router.GetRoutes()
	assert.Equal(t, "source1", routes["table1"])
	assert.Equal(t, "source2", routes["table2"])
	assert.Equal(t, 2, len(routes))
}

func TestRouter_Route_WithRoute(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	router.SetManager(&Manager{})
	
	_, err := router.Route("table1")
	assert.Error(t, err) // Manager will not have the data source
}

func TestRouter_Route_NoRoute(t *testing.T) {
	router := NewRouter()
	
	router.SetManager(&Manager{})
	
	_, err := router.Route("unknown_table")
	assert.Error(t, err) // Manager will not have the data source
}

func TestRouter_Route_NoManager(t *testing.T) {
	router := NewRouter()
	
	_, err := router.Route("table1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manager not initialized")
}
