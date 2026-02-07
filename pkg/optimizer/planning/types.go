package planning

// AggregationType represents the type of aggregation function
type AggregationType int

const (
	Count AggregationType = iota
	Sum
	Avg
	Max
	Min
)

// String returns the string representation of AggregationType
func (t AggregationType) String() string {
	switch t {
	case Count:
		return "COUNT"
	case Sum:
		return "SUM"
	case Avg:
		return "AVG"
	case Max:
		return "MAX"
	case Min:
		return "MIN"
	default:
		return "UNKNOWN"
	}
}

// AggregationItem represents an aggregation function in a query
type AggregationItem struct {
	Type     AggregationType
	Expr     interface{} // Using interface{} to avoid parser dependency issues
	Alias    string
	Distinct bool
}

// LimitInfo represents limit information
type LimitInfo struct {
	Limit  int64
	Offset int64
}
