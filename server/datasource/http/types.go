package http

import "github.com/kasuganosora/sqlexec/pkg/resource/domain"

// ── HTTP API 请求结构体 ──

// QueryRequest 查询请求
type QueryRequest struct {
	Filters       []domain.Filter `json:"filters,omitempty"`
	OrderBy       string          `json:"order_by,omitempty"`
	Order         string          `json:"order,omitempty"` // ASC, DESC
	Limit         int             `json:"limit,omitempty"`
	Offset        int             `json:"offset,omitempty"`
	SelectColumns []string        `json:"select_columns,omitempty"`
}

// InsertRequest 插入请求
type InsertRequest struct {
	Rows    []domain.Row          `json:"rows"`
	Options *domain.InsertOptions `json:"options,omitempty"`
}

// UpdateRequest 更新请求
type UpdateRequest struct {
	Filters []domain.Filter      `json:"filters,omitempty"`
	Updates domain.Row           `json:"updates"`
	Options *domain.UpdateOptions `json:"options,omitempty"`
}

// DeleteRequest 删除请求
type DeleteRequest struct {
	Filters []domain.Filter      `json:"filters,omitempty"`
	Options *domain.DeleteOptions `json:"options,omitempty"`
}

// ── HTTP API 响应结构体 ──

// QueryResponse 查询响应
type QueryResponse struct {
	Columns []domain.ColumnInfo `json:"columns"`
	Rows    []domain.Row        `json:"rows"`
	Total   int64               `json:"total"`
}

// TablesResponse 表列表响应
type TablesResponse struct {
	Tables []string `json:"tables"`
}

// SchemaResponse 表结构响应
type SchemaResponse struct {
	Name    string              `json:"name"`
	Columns []domain.ColumnInfo `json:"columns"`
}

// MutationResponse 写操作响应（insert/update/delete）
type MutationResponse struct {
	Affected int64 `json:"affected"`
}

// HealthResponse 健康检查响应
type HealthResponse struct {
	Status string `json:"status"`
}

// ErrorResponse 错误响应
type ErrorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}
