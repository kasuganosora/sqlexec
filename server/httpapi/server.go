package httpapi

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/security"
)

// Server is the HTTP REST API server
type Server struct {
	db          *api.DB
	configDir   string
	cfg         *config.HTTPAPIConfig
	auditLogger *security.AuditLogger
	httpServer  *http.Server
}

// NewServer creates a new HTTP API server
func NewServer(db *api.DB, configDir string, cfg *config.HTTPAPIConfig, auditLogger *security.AuditLogger) *Server {
	return &Server{
		db:          db,
		configDir:   configDir,
		cfg:         cfg,
		auditLogger: auditLogger,
	}
}

// Start starts the HTTP API server (blocking)
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)

	clientStore := NewClientStore(s.configDir)
	queryHandler := NewQueryHandler(s.db, s.configDir, s.auditLogger)

	mux := http.NewServeMux()

	// Health check (no auth required)
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, HealthResponse{
			Status:  "ok",
			Version: "1.0.0",
		})
	})

	// Query endpoint (auth required)
	authedQuery := AuthMiddleware(clientStore)(queryHandler)
	mux.Handle("/api/v1/query", authedQuery)

	// Apply global middleware: Recovery → CORS → Logging
	handler := RecoveryMiddleware(CORSMiddleware(LoggingMiddleware(mux)))

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("[HTTP API] 启动 HTTP API 服务器: %s", addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP API server
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}
