package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/config_schema"
)

type contextKey string

const (
	ctxKeyClient contextKey = "api_client"
	ctxKeyBody   contextKey = "request_body"
)

// GetClientFromContext returns the authenticated API client from the request context
func GetClientFromContext(ctx context.Context) *config_schema.APIClient {
	client, _ := ctx.Value(ctxKeyClient).(*config_schema.APIClient)
	return client
}

// GetBodyFromContext returns the cached request body from the context
func GetBodyFromContext(ctx context.Context) string {
	body, _ := ctx.Value(ctxKeyBody).(string)
	return body
}

// RecoveryMiddleware recovers from panics and returns a 500 error
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[HTTP API] panic recovered: %v", err)
				writeJSON(w, http.StatusInternalServerError, ErrorResponse{
					Error: "internal server error",
					Code:  http.StatusInternalServerError,
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// CORSMiddleware adds CORS headers
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, "+headerAPIKey+", "+headerTimestamp+", "+headerNonce+", "+headerSignature+", Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// LoggingMiddleware logs HTTP requests
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		wrapped := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		client := GetClientFromContext(r.Context())
		clientName := "-"
		if client != nil {
			clientName = client.Name
		}

		log.Printf("[HTTP API] %s %s %s %d %s", clientName, r.Method, r.URL.Path, wrapped.statusCode, duration)
	})
}

// AuthMiddleware validates API key and HMAC signature
func AuthMiddleware(store *ClientStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := r.Header.Get(headerAPIKey)
			if apiKey == "" {
				writeJSON(w, http.StatusUnauthorized, ErrorResponse{
					Error: "missing X-API-Key header",
					Code:  http.StatusUnauthorized,
				})
				return
			}

			client, err := store.GetClient(apiKey)
			if err != nil {
				writeJSON(w, http.StatusUnauthorized, ErrorResponse{
					Error: err.Error(),
					Code:  http.StatusUnauthorized,
				})
				return
			}

			// Read body for signature verification
			body, err := io.ReadAll(r.Body)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, ErrorResponse{
					Error: "failed to read request body",
					Code:  http.StatusBadRequest,
				})
				return
			}

			timestamp := r.Header.Get(headerTimestamp)
			nonce := r.Header.Get(headerNonce)
			signature := r.Header.Get(headerSignature)

			if timestamp == "" || nonce == "" || signature == "" {
				writeJSON(w, http.StatusUnauthorized, ErrorResponse{
					Error: "missing signature headers (X-Timestamp, X-Nonce, X-Signature)",
					Code:  http.StatusUnauthorized,
				})
				return
			}

			if err := ValidateSignature(client.APISecret, r.Method, r.URL.Path, timestamp, nonce, string(body), signature); err != nil {
				writeJSON(w, http.StatusUnauthorized, ErrorResponse{
					Error: "signature verification failed: " + err.Error(),
					Code:  http.StatusUnauthorized,
				})
				return
			}

			// Store client and body in context
			ctx := context.WithValue(r.Context(), ctxKeyClient, client)
			ctx = context.WithValue(ctx, ctxKeyBody, string(body))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// statusWriter wraps http.ResponseWriter to capture status code
type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
