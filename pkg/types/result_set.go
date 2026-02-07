package types

// ResultSet represents a query result set.
// It contains column metadata and row data.
type ResultSet interface {
	// GetColumns returns the column definitions.
	GetColumns() []ColumnInfo

	// GetRows returns the row data.
	GetRows() [][]interface{}

	// GetRowCount returns the number of rows.
	GetRowCount() int

	// GetColumnCount returns the number of columns.
	GetColumnCount() int
}

// SimpleResultSet is a basic implementation of ResultSet.
type SimpleResultSet struct {
	Columns []ColumnInfo
	Rows    [][]interface{}
}

// GetColumns returns the column definitions.
func (rs *SimpleResultSet) GetColumns() []ColumnInfo {
	return rs.Columns
}

// GetRows returns the row data.
func (rs *SimpleResultSet) GetRows() [][]interface{} {
	return rs.Rows
}

// GetRowCount returns the number of rows.
func (rs *SimpleResultSet) GetRowCount() int {
	return len(rs.Rows)
}

// GetColumnCount returns the number of columns.
func (rs *SimpleResultSet) GetColumnCount() int {
	return len(rs.Columns)
}

// NewSimpleResultSet creates a new SimpleResultSet.
func NewSimpleResultSet(columns []ColumnInfo, rows [][]interface{}) *SimpleResultSet {
	return &SimpleResultSet{
		Columns: columns,
		Rows:    rows,
	}
}
