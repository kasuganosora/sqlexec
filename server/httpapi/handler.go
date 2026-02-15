package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/security"
)

// QueryHandler handles SQL query execution via HTTP
type QueryHandler struct {
	db          *api.DB
	configDir   string
	auditLogger *security.AuditLogger
}

// NewQueryHandler creates a new QueryHandler
func NewQueryHandler(db *api.DB, configDir string, auditLogger *security.AuditLogger) *QueryHandler {
	return &QueryHandler{
		db:          db,
		configDir:   configDir,
		auditLogger: auditLogger,
	}
}

// ServeHTTP handles POST /api/v1/query
func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error: "method not allowed",
			Code:  http.StatusMethodNotAllowed,
		})
		return
	}

	client := GetClientFromContext(r.Context())
	if client == nil {
		writeJSON(w, http.StatusUnauthorized, ErrorResponse{
			Error: "unauthorized",
			Code:  http.StatusUnauthorized,
		})
		return
	}

	// Parse request body from context (already read by auth middleware)
	bodyStr := GetBodyFromContext(r.Context())
	var req QueryRequest
	if err := json.Unmarshal([]byte(bodyStr), &req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "invalid request body: " + err.Error(),
			Code:  http.StatusBadRequest,
		})
		return
	}

	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error: "sql field is required",
			Code:  http.StatusBadRequest,
		})
		return
	}

	start := time.Now()
	clientIP := getClientIP(r)

	// Create ephemeral session
	session := h.db.Session()
	defer session.Close()
	session.SetConfigDir(h.configDir)
	session.SetUser(client.Name)
	if req.Database != "" {
		session.SetCurrentDB(req.Database)
	}

	// Determine if this is a read or write query
	sqlUpper := strings.TrimSpace(strings.ToUpper(req.SQL))
	isRead := strings.HasPrefix(sqlUpper, "SELECT") ||
		strings.HasPrefix(sqlUpper, "SHOW") ||
		strings.HasPrefix(sqlUpper, "DESCRIBE") ||
		strings.HasPrefix(sqlUpper, "DESC ") ||
		strings.HasPrefix(sqlUpper, "EXPLAIN")

	duration := time.Since(start).Milliseconds()

	if isRead {
		query, err := session.Query(req.SQL)
		duration = time.Since(start).Milliseconds()
		if err != nil {
			h.logRequest(client.Name, clientIP, r.Method, r.URL.Path, req.SQL, req.Database, duration, false)
			writeJSON(w, http.StatusBadRequest, ErrorResponse{
				Error: err.Error(),
				Code:  http.StatusBadRequest,
			})
			return
		}
		defer query.Close()

		rows := make([]domain.Row, 0, 64)
		for query.Next() {
			rows = append(rows, query.Row())
		}

		h.logRequest(client.Name, clientIP, r.Method, r.URL.Path, req.SQL, req.Database, duration, true)

		writeJSON(w, http.StatusOK, QueryResponse{
			Columns: query.Columns(),
			Rows:    rows,
			Total:   int64(len(rows)),
		})
	} else {
		result, err := session.Execute(req.SQL)
		duration = time.Since(start).Milliseconds()
		if err != nil {
			h.logRequest(client.Name, clientIP, r.Method, r.URL.Path, req.SQL, req.Database, duration, false)
			writeJSON(w, http.StatusBadRequest, ErrorResponse{
				Error: err.Error(),
				Code:  http.StatusBadRequest,
			})
			return
		}

		h.logRequest(client.Name, clientIP, r.Method, r.URL.Path, req.SQL, req.Database, duration, true)

		writeJSON(w, http.StatusOK, ExecResponse{
			AffectedRows: result.RowsAffected,
		})
	}
}

func (h *QueryHandler) logRequest(clientName, ip, method, path, sql, database string, duration int64, success bool) {
	if h.auditLogger != nil {
		h.auditLogger.LogAPIRequest(clientName, ip, method, path, sql, database, duration, success)
	}
}

// getClientIP extracts the client IP from the request
func getClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.SplitN(xff, ",", 2)
		return strings.TrimSpace(parts[0])
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	// RemoteAddr is "IP:port"
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}
