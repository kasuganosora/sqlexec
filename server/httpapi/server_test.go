package httpapi

import (
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
			Name:    "active",
			APIKey:  "key-active",
			APISecret: "secret-active",
			Enabled: true,
		},
		{
			Name:    "disabled",
			APIKey:  "key-disabled",
			APISecret: "secret-disabled",
			Enabled: false,
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
