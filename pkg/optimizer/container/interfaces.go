package container

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Container is a dependency injection container that manages services.
// It provides methods to register and retrieve services by name.
type Container interface {
	// Register registers a service with the given name.
	// name: unique name for the service
	// service: the service instance
	// If a service with the same name already exists, it will be overwritten.
	Register(name string, service interface{})

	// Get retrieves a service by name.
	// name: name of the service
	// Returns the service and true if found, or nil and false if not found.
	Get(name string) (interface{}, bool)

	// MustGet retrieves a service by name.
	// name: name of the service
	// Returns the service, or panics if not found.
	// This is a convenience method when you're certain the service exists.
	MustGet(name string) interface{}

	// Has checks if a service with the given name exists.
	// name: name of the service
	// Returns true if the service exists, false otherwise.
	Has(name string) bool

	// BuildOptimizer builds a basic optimizer using services from the container.
	// Returns an Optimizer instance.
	BuildOptimizer() interface{}

	// BuildEnhancedOptimizer builds an enhanced optimizer with advanced features.
	// parallelism: degree of parallelism for optimization
	// Returns an Optimizer instance with enhanced capabilities.
	BuildEnhancedOptimizer(parallelism int) interface{}

	// BuildExecutor builds a plan executor using services from the container.
	// Returns a PlanExecutor instance.
	BuildExecutor() interface{}

	// BuildShowProcessor builds a SHOW statement processor.
	// Returns a ShowProcessor instance.
	BuildShowProcessor() interface{}

	// BuildVariableManager builds a variable manager.
	// Returns a VariableManager instance.
	BuildVariableManager() interface{}

	// BuildExpressionEvaluator builds an expression evaluator.
	// Returns an ExpressionEvaluator instance.
	BuildExpressionEvaluator() interface{}

	// GetDataSource returns the data source used by the container.
	GetDataSource() domain.DataSource
}
