package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/api"
	"github.com/kasuganosora/sqlexec/pkg/config"
	"github.com/kasuganosora/sqlexec/pkg/config_schema"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/kasuganosora/sqlexec/pkg/security"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testEnv holds shared test infrastructure
type testEnv struct {
	db          *api.DB
	configDir   string
	auditLogger *security.AuditLogger
	client      config_schema.APIClient
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	configDir := t.TempDir()

	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: false,
		DebugMode:    false,
	})
	require.NoError(t, err)

	// Register a memory datasource
	ds := memory.NewMVCCDataSource(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "default",
		Writable: true,
	})
	require.NoError(t, ds.Connect(nil))
	require.NoError(t, db.RegisterDataSource("default", ds))

	// Create a test API client
	client := config_schema.APIClient{
		Name:      "test_client",
		APIKey:    "test-api-key-12345",
		APISecret: "test-secret-abcdef0123456789abcdef0123456789",
		Enabled:   true,
		CreatedAt: time.Now().Format(time.RFC3339),
		UpdatedAt: time.Now().Format(time.RFC3339),
	}

	clients := []config_schema.APIClient{client}
	data, err := json.MarshalIndent(clients, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "api_clients.json"), data, 0600))

	return &testEnv{
		db:          db,
		configDir:   configDir,
		auditLogger: security.NewAuditLogger(100),
		client:      client,
	}
}

func signRequest(method, path, body, apiSecret string) (timestamp, nonce, signature string) {
	timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	nonce = "test-nonce-123"
	message := method + path + timestamp + nonce + body
	mac := hmac.New(sha256.New, []byte(apiSecret))
	mac.Write([]byte(message))
	signature = hex.EncodeToString(mac.Sum(nil))
	return
}

func TestHealthEndpoint(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, HealthResponse{Status: "ok", Version: "1.0.0"})
	})
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))

	handler := RecoveryMiddleware(CORSMiddleware(LoggingMiddleware(mux)))
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/v1/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var healthResp HealthResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&healthResp))
	assert.Equal(t, "ok", healthResp.Status)
}

func TestQueryEndpoint_NoAuth(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SELECT 1"}`
	resp, err := http.Post(server.URL+"/api/v1/query", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestQueryEndpoint_InvalidSignature(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SELECT 1"}`
	req, err := http.NewRequest("POST", server.URL+"/api/v1/query", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	req.Header.Set("X-Nonce", "test-nonce")
	req.Header.Set("X-Signature", "invalid-signature")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestQueryEndpoint_ValidQuery(t *testing.T) {
	env := setupTestEnv(t)

	// Create a table and insert data first
	session := env.db.Session()
	_, err := session.Execute("CREATE TABLE test_users (id INT, name VARCHAR(100))")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO test_users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO test_users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)
	session.Close()

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SELECT * FROM test_users"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var queryResp QueryResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&queryResp))
	assert.Equal(t, int64(2), queryResp.Total)
	assert.Len(t, queryResp.Rows, 2)
}

func TestQueryEndpoint_ExecuteDML(t *testing.T) {
	env := setupTestEnv(t)

	// Create a table first
	session := env.db.Session()
	_, err := session.Execute("CREATE TABLE dml_test (id INT, value VARCHAR(100))")
	require.NoError(t, err)
	session.Close()

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	// INSERT
	body := `{"sql":"INSERT INTO dml_test (id, value) VALUES (1, 'hello')"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var execResp ExecResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&execResp))
	assert.Equal(t, int64(1), execResp.AffectedRows)
}

func TestQueryEndpoint_EmptySQL(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":""}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCORSMiddleware(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := CORSMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Test preflight
	req, err := http.NewRequest("OPTIONS", server.URL+"/test", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestValidateSignature(t *testing.T) {
	secret := "my-secret"
	method := "POST"
	path := "/api/v1/query"
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "random-nonce"
	body := `{"sql":"SELECT 1"}`

	message := method + path + timestamp + nonce + body
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	signature := hex.EncodeToString(mac.Sum(nil))

	err := ValidateSignature(secret, method, path, timestamp, nonce, body, signature)
	assert.NoError(t, err)

	// Test with wrong signature
	err = ValidateSignature(secret, method, path, timestamp, nonce, body, "wrong")
	assert.Error(t, err)

	// Test with expired timestamp
	oldTimestamp := strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)
	message2 := method + path + oldTimestamp + nonce + body
	mac2 := hmac.New(sha256.New, []byte(secret))
	mac2.Write([]byte(message2))
	sig2 := hex.EncodeToString(mac2.Sum(nil))
	err = ValidateSignature(secret, method, path, oldTimestamp, nonce, body, sig2)
	assert.Error(t, err)
}

func TestClientStore(t *testing.T) {
	configDir := t.TempDir()

	clients := []config_schema.APIClient{
		{
			Name:      "active",
			APIKey:    "key-active",
			APISecret: "secret-active",
			Enabled:   true,
		},
		{
			Name:      "disabled",
			APIKey:    "key-disabled",
			APISecret: "secret-disabled",
			Enabled:   false,
		},
	}

	data, err := json.MarshalIndent(clients, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "api_clients.json"), data, 0600))

	store := NewClientStore(configDir)

	t.Run("get active client", func(t *testing.T) {
		c, err := store.GetClient("key-active")
		require.NoError(t, err)
		assert.Equal(t, "active", c.Name)
	})

	t.Run("get disabled client", func(t *testing.T) {
		_, err := store.GetClient("key-disabled")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "disabled")
	})

	t.Run("get nonexistent client", func(t *testing.T) {
		_, err := store.GetClient("key-nonexistent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid api key")
	})
}

func TestAuditLogging(t *testing.T) {
	env := setupTestEnv(t)

	// Create a table
	session := env.db.Session()
	_, err := session.Execute("CREATE TABLE audit_test (id INT)")
	require.NoError(t, err)
	session.Close()

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SELECT * FROM audit_test"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	// Check audit log
	events := env.auditLogger.GetEventsByType(security.EventTypeAPIRequest)
	assert.NotEmpty(t, events, "should have audit events")
	if len(events) > 0 {
		event := events[len(events)-1]
		assert.Equal(t, "test_client", event.User)
		assert.Equal(t, "SELECT * FROM audit_test", event.Query)
		assert.True(t, event.Success)
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	handler := RecoveryMiddleware(panicHandler)
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Equal(t, "internal server error", errResp.Error)
}

func TestQueryEndpoint_MethodNotAllowed(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	// GET to query endpoint should return 405
	body := `{"sql":"SELECT 1"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("GET", path, body, env.client.APISecret)

	req, err := http.NewRequest("GET", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Equal(t, "method not allowed", errResp.Error)
}

func TestQueryEndpoint_InvalidJSON(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `not-valid-json`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Contains(t, errResp.Error, "invalid request body")
}

func TestQueryEndpoint_MissingSignatureHeaders(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SELECT 1"}`
	req, err := http.NewRequest("POST", server.URL+"/api/v1/query", strings.NewReader(body))
	require.NoError(t, err)
	// API key present but missing timestamp/nonce/signature
	req.Header.Set("X-API-Key", env.client.APIKey)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Contains(t, errResp.Error, "missing signature headers")
}

func TestQueryEndpoint_QueryError(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Query a nonexistent table
	body := `{"sql":"SELECT * FROM nonexistent_table_xyz"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestQueryEndpoint_DMLError(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	// INSERT into nonexistent table
	body := `{"sql":"INSERT INTO nonexistent_table_xyz (id) VALUES (1)"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestQueryEndpoint_WithDatabase(t *testing.T) {
	env := setupTestEnv(t)

	// Create a table
	session := env.db.Session()
	_, err := session.Execute("CREATE TABLE db_test (id INT)")
	require.NoError(t, err)
	session.Close()

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SELECT * FROM db_test","database":"default"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGetClientIP(t *testing.T) {
	t.Run("X-Forwarded-For single IP", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Forwarded-For", "1.2.3.4")
		assert.Equal(t, "1.2.3.4", getClientIP(req))
	})

	t.Run("X-Forwarded-For multiple IPs", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
		assert.Equal(t, "1.2.3.4", getClientIP(req))
	})

	t.Run("X-Real-IP", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Real-IP", "10.0.0.1")
		assert.Equal(t, "10.0.0.1", getClientIP(req))
	})

	t.Run("RemoteAddr with port", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.1:12345"
		assert.Equal(t, "192.168.1.1", getClientIP(req))
	})

	t.Run("RemoteAddr without port", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.1"
		assert.Equal(t, "192.168.1.1", getClientIP(req))
	})

	t.Run("X-Forwarded-For takes precedence over X-Real-IP", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Forwarded-For", "1.2.3.4")
		req.Header.Set("X-Real-IP", "5.6.7.8")
		assert.Equal(t, "1.2.3.4", getClientIP(req))
	})
}

func TestGetClientByName(t *testing.T) {
	configDir := t.TempDir()

	clients := []config_schema.APIClient{
		{
			Name:      "myclient",
			APIKey:    "key-myclient",
			APISecret: "secret-myclient",
			Enabled:   true,
		},
	}
	data, err := json.MarshalIndent(clients, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "api_clients.json"), data, 0600))

	store := NewClientStore(configDir)

	c, err := store.GetClientByName("key-myclient")
	require.NoError(t, err)
	assert.Equal(t, "myclient", c.Name)

	_, err = store.GetClientByName("nonexistent")
	assert.Error(t, err)
}

func TestClientStore_LoadError(t *testing.T) {
	// Use a directory without api_clients.json
	store := NewClientStore(t.TempDir())
	_, err := store.GetClient("any-key")
	assert.Error(t, err)
}

func TestValidateSignature_InvalidTimestamp(t *testing.T) {
	err := ValidateSignature("secret", "POST", "/path", "not-a-number", "nonce", "body", "sig")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid timestamp")
}

func TestNewServer(t *testing.T) {
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: false,
		DebugMode:    false,
	})
	require.NoError(t, err)

	cfg := &config.HTTPAPIConfig{
		Host: "127.0.0.1",
		Port: 0,
	}

	auditLogger := security.NewAuditLogger(100)
	s := NewServer(db, t.TempDir(), cfg, auditLogger)
	assert.NotNil(t, s)
}

func TestServer_ShutdownNil(t *testing.T) {
	db, err := api.NewDB(&api.DBConfig{
		CacheEnabled: false,
		DebugMode:    false,
	})
	require.NoError(t, err)

	cfg := &config.HTTPAPIConfig{
		Host: "127.0.0.1",
		Port: 0,
	}

	s := NewServer(db, t.TempDir(), cfg, nil)
	// Shutdown before Start — httpServer is nil
	err = s.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestLoggingMiddleware(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("ok"))
	})

	handler := LoggingMiddleware(inner)
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestLoggingMiddleware_WithClient(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with a middleware that injects a client into context
	withClient := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client := &config_schema.APIClient{Name: "test-client"}
		ctx := context.WithValue(r.Context(), ctxKeyClient, client)
		inner.ServeHTTP(w, r.WithContext(ctx))
	})

	handler := LoggingMiddleware(withClient)
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/test")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGetClientFromContext_NoClient(t *testing.T) {
	ctx := context.Background()
	client := GetClientFromContext(ctx)
	assert.Nil(t, client)
}

func TestGetBodyFromContext_NoBody(t *testing.T) {
	ctx := context.Background()
	body := GetBodyFromContext(ctx)
	assert.Equal(t, "", body)
}

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusOK, map[string]string{"key": "value"})

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var result map[string]string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&result))
	assert.Equal(t, "value", result["key"])
}

func TestStatusWriter(t *testing.T) {
	w := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}

	sw.WriteHeader(http.StatusNotFound)
	assert.Equal(t, http.StatusNotFound, sw.statusCode)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestQueryEndpoint_ShowDatabases(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SHOW DATABASES"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var queryResp QueryResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&queryResp))
	fmt.Printf("SHOW DATABASES result: %+v\n", queryResp)
}

// ==========================================================================
// Tests for bugfixes: error sanitization, duration timing, result truncation
// ==========================================================================

func TestQueryEndpoint_ErrorMessageSanitized(t *testing.T) {
	env := setupTestEnv(t)

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	t.Run("SELECT error does not expose internal details", func(t *testing.T) {
		body := `{"sql":"SELECT * FROM nonexistent_table_for_sanitize_test"}`
		path := "/api/v1/query"
		ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

		req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("X-API-Key", env.client.APIKey)
		req.Header.Set("X-Timestamp", ts)
		req.Header.Set("X-Nonce", nonce)
		req.Header.Set("X-Signature", sig)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		// Error message should be generic, not containing internal error details
		assert.Equal(t, "query failed", errResp.Error)
	})

	t.Run("INSERT error does not expose internal details", func(t *testing.T) {
		body := `{"sql":"INSERT INTO nonexistent_table_for_sanitize_test (id) VALUES (1)"}`
		path := "/api/v1/query"
		ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

		req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("X-API-Key", env.client.APIKey)
		req.Header.Set("X-Timestamp", ts)
		req.Header.Set("X-Nonce", nonce)
		req.Header.Set("X-Signature", sig)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var errResp ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		assert.Equal(t, "execute failed", errResp.Error)
	})
}

func TestQueryResponse_TruncatedField(t *testing.T) {
	// Test that QueryResponse has Truncated field and it's false for small results
	env := setupTestEnv(t)

	session := env.db.Session()
	_, err := session.Execute("CREATE TABLE truncate_test (id INT)")
	require.NoError(t, err)
	_, err = session.Execute("INSERT INTO truncate_test (id) VALUES (1)")
	require.NoError(t, err)
	session.Close()

	queryHandler := NewQueryHandler(env.db, env.configDir, env.auditLogger)
	clientStore := NewClientStore(env.configDir)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", AuthMiddleware(clientStore)(queryHandler))
	handler := RecoveryMiddleware(mux)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"sql":"SELECT * FROM truncate_test"}`
	path := "/api/v1/query"
	ts, nonce, sig := signRequest("POST", path, body, env.client.APISecret)

	req, err := http.NewRequest("POST", server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-API-Key", env.client.APIKey)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", sig)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var queryResp QueryResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&queryResp))
	assert.Equal(t, int64(1), queryResp.Total)
	assert.False(t, queryResp.Truncated, "small result set should not be truncated")
}

func TestMaxResultRows_Constant(t *testing.T) {
	// Verify the constant is set to a reasonable value
	assert.Equal(t, 10000, maxResultRows)
}

func TestWriteJSON_ErrorHandling(t *testing.T) {
	// writeJSON should not panic even with unusual input
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusOK, ErrorResponse{Error: "test", Code: 200})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}
