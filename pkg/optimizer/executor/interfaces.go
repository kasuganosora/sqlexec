package executor

import (
	"context"

	"github.com/kasuganosora/sqlexec/pkg/optimizer/plan"
)

// ResultSet is the interface for query execution results.
// It provides methods to iterate over rows and access column metadata.
type ResultSet interface {
	// Columns returns the column names of the result set.
	Columns() []string

	// Next returns the next row as a map of column names to values.
	// Returns an error if there are no more rows.
	Next() (map[string]interface{}, error)

	// HasNext returns true if there are more rows to read.
	HasNext() bool

	// Total returns the total number of rows in the result set.
	Total() int64

	// Close closes the result set and releases any resources.
	Close() error
}

// PlanExecutor is the interface for executing physical execution plans.
// It takes a plan and produces query results.
type PlanExecutor interface {
	// Execute executes the given physical plan and returns the result set.
	// ctx: execution context
	// plan: physical execution plan
	// Returns the query results or an error if execution fails.
	Execute(ctx context.Context, plan *plan.Plan) (ResultSet, error)

	// Explain returns a string representation of the execution plan.
	// This is used for EXPLAIN statements.
	Explain(plan *plan.Plan) string
}

// ShowProcessor is the interface for processing SHOW statements.
// It handles various SHOW commands like SHOW TABLES, SHOW DATABASES, etc.
type ShowProcessor interface {
	// ProcessShowTables processes SHOW TABLES statement.
	// Returns the list of tables in the current database.
	ProcessShowTables(ctx context.Context) (ResultSet, error)

	// ProcessShowDatabases processes SHOW DATABASES statement.
	// Returns the list of all databases.
	ProcessShowDatabases(ctx context.Context) (ResultSet, error)

	// ProcessShowColumns processes SHOW COLUMNS statement.
	// tableName: name of the table to show columns for
	// Returns the column information for the table.
	ProcessShowColumns(ctx context.Context, tableName string) (ResultSet, error)

	// ProcessShowIndex processes SHOW INDEX statement.
	// tableName: name of the table to show indexes for
	// Returns the index information for the table.
	ProcessShowIndex(ctx context.Context, tableName string) (ResultSet, error)

	// ProcessShowProcessList processes SHOW PROCESSLIST statement.
	// Returns the list of currently executing processes.
	ProcessShowProcessList(ctx context.Context) (ResultSet, error)

	// ProcessShowVariables processes SHOW VARIABLES statement.
	// Returns the list of system variables and their values.
	ProcessShowVariables(ctx context.Context) (ResultSet, error)

	// ProcessShowStatus processes SHOW STATUS statement.
	// Returns the list of status variables and their values.
	ProcessShowStatus(ctx context.Context) (ResultSet, error)

	// ProcessShowCreateTable processes SHOW CREATE TABLE statement.
	// tableName: name of the table
	// Returns the CREATE TABLE statement for the table.
	ProcessShowCreateTable(ctx context.Context, tableName string) (ResultSet, error)
}

// VariableManager is the interface for managing system variables.
// It provides methods to get, set, and list system variables.
type VariableManager interface {
	// GetVariable retrieves the value of a system variable.
	// name: name of the variable
	// Returns the value and true if found, or nil and false if not found.
	GetVariable(name string) (interface{}, bool)

	// SetVariable sets the value of a system variable.
	// name: name of the variable
	// value: new value for the variable
	// Returns an error if the variable cannot be set.
	SetVariable(name string, value interface{}) error

	// ListVariables returns all system variables and their values.
	// Returns a map of variable names to their values.
	ListVariables() map[string]interface{}

	// GetVariableNames returns the names of all system variables.
	// Returns a slice of variable names.
	GetVariableNames() []string
}

// ExpressionEvaluator is the interface for evaluating SQL expressions.
// It handles expression parsing, type checking, and evaluation.
type ExpressionEvaluator interface {
	// Evaluate evaluates an expression with the given context.
	// expr: the expression to evaluate
	// ctx: evaluation context (row data, variables, etc.)
	// Returns the result value or an error if evaluation fails.
	Evaluate(expr interface{}, ctx ExpressionContext) (interface{}, error)

	// EvaluateBoolean evaluates an expression and returns a boolean result.
	// This is commonly used for WHERE clause conditions.
	// expr: the expression to evaluate
	// ctx: evaluation context
	// Returns the boolean result or an error if evaluation fails.
	EvaluateBoolean(expr interface{}, ctx ExpressionContext) (bool, error)

	// Validate validates an expression for correctness.
	// expr: the expression to validate
	// Returns an error if the expression is invalid.
	Validate(expr interface{}) error
}

// ExpressionContext provides context for expression evaluation.
type ExpressionContext interface {
	// GetRowValue gets the value of a column in the current row.
	// colName: name of the column
	// Returns the column value and true if found, or nil and false if not found.
	GetRowValue(colName string) (interface{}, bool)

	// GetVariable gets the value of a variable.
	// varName: name of the variable
	// Returns the variable value and true if found, or nil and false if not found.
	GetVariable(varName string) (interface{}, bool)

	// GetFunction gets a function by name.
	// funcName: name of the function
	// Returns the function and true if found, or nil and false if not found.
	GetFunction(funcName string) (interface{}, bool)

	// GetCurrentTime gets the current timestamp for function evaluation.
	// Returns the current time.
	GetCurrentTime() interface{}
}
