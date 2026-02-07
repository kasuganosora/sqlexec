package dataaccess

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouter_AddRemove_Add(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	router.AddRoute("table2", "source2")
	router.AddRoute("table3", "source3")
	
	routes := router.GetRoutes()
	assert.Equal(t, 3, len(routes))
}

func TestRouter_AddRemove_Remove(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	router.RemoveRoute("table1")
	
	routes := router.GetRoutes()
	assert.Equal(t, 0, len(routes))
}

func TestRouter_AddRemove_Get(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	router.AddRoute("table2", "source2")
	
	routes := router.GetRoutes()
	assert.Equal(t, 2, len(routes))
	assert.Equal(t, "source1", routes["table1"])
	assert.Equal(t, "source2", routes["table2"])
}

func TestRouter_AddRemove_GetAgain(t *testing.T) {
	router := NewRouter()
	
	router.AddRoute("table1", "source1")
	router.RemoveRoute("table1")
	
	routes := router.GetRoutes()
	assert.Equal(t, 0, len(routes))
	
	router.AddRoute("table2", "source2")
	
	routes = router.GetRoutes()
	assert.Equal(t, 1, len(routes))
}
