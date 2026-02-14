package httpapi

import "github.com/kasuganosora/sqlexec/pkg/resource/domain"

// QueryRequest represents an HTTP API query request
type QueryRequest struct {
	SQL      string `json:"sql"`
	Database string `json:"database,omitempty"`
}

// QueryResponse represents a successful query response
type QueryResponse struct {
	Columns []domain.ColumnInfo `json:"columns,omitempty"`
	Rows    []domain.Row        `json:"rows"`
	Total   int64               `json:"total"`
}

// ExecResponse represents a successful execute (DML/DDL) response
type ExecResponse struct {
	AffectedRows int64 `json:"affected_rows"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// HealthResponse represents a health check response
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}
