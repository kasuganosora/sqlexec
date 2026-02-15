package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/kasuganosora/sqlexec/pkg/config_schema"
)

const (
	headerAPIKey    = "X-API-Key"
	headerTimestamp = "X-Timestamp"
	headerNonce     = "X-Nonce"
	headerSignature = "X-Signature"

	// timestampTolerance is the maximum allowed time difference for request timestamps
	timestampTolerance = 5 * time.Minute

	// clientCacheTTL is how long the client cache is valid before reloading from disk
	clientCacheTTL = 30 * time.Second
)

// ClientStore provides access to API client credentials with in-memory caching
type ClientStore struct {
	configDir string
	mu        sync.RWMutex
	cache     map[string]*config_schema.APIClient // keyed by APIKey
	loadedAt  time.Time
}

// NewClientStore creates a new ClientStore
func NewClientStore(configDir string) *ClientStore {
	return &ClientStore{configDir: configDir}
}

// GetClient returns an API client by API key, using a cached map with TTL
func (s *ClientStore) GetClient(apiKey string) (*config_schema.APIClient, error) {
	// Fast path: read lock, check cache
	s.mu.RLock()
	if s.cache != nil && time.Since(s.loadedAt) < clientCacheTTL {
		client, ok := s.cache[apiKey]
		s.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("invalid api key")
		}
		if !client.Enabled {
			return nil, fmt.Errorf("api client '%s' is disabled", client.Name)
		}
		return client, nil
	}
	s.mu.RUnlock()

	// Slow path: write lock, reload from disk
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if s.cache != nil && time.Since(s.loadedAt) < clientCacheTTL {
		client, ok := s.cache[apiKey]
		if !ok {
			return nil, fmt.Errorf("invalid api key")
		}
		if !client.Enabled {
			return nil, fmt.Errorf("api client '%s' is disabled", client.Name)
		}
		return client, nil
	}

	clients, err := config_schema.LoadAPIClients(s.configDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load api clients: %w", err)
	}

	// Rebuild cache map
	s.cache = make(map[string]*config_schema.APIClient, len(clients))
	for i := range clients {
		s.cache[clients[i].APIKey] = &clients[i]
	}
	s.loadedAt = time.Now()

	client, ok := s.cache[apiKey]
	if !ok {
		return nil, fmt.Errorf("invalid api key")
	}
	if !client.Enabled {
		return nil, fmt.Errorf("api client '%s' is disabled", client.Name)
	}
	return client, nil
}

// GetClientByName returns an API client by name (for MCP Bearer token auth)
func (s *ClientStore) GetClientByName(apiKey string) (*config_schema.APIClient, error) {
	return s.GetClient(apiKey)
}

// ValidateSignature validates the HMAC-SHA256 signature
func ValidateSignature(secret, method, path, timestamp, nonce, body, signature string) error {
	// Validate timestamp
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp")
	}

	diff := time.Since(time.Unix(ts, 0))
	if math.Abs(diff.Seconds()) > timestampTolerance.Seconds() {
		return fmt.Errorf("timestamp expired")
	}

	// Compute expected signature
	message := method + path + timestamp + nonce + body
	expected := computeHMAC(secret, message)

	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("invalid signature")
	}

	return nil
}

// computeHMAC computes HMAC-SHA256
func computeHMAC(secret, message string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}
