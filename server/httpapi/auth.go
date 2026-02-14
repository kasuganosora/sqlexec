package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
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
)

// ClientStore provides access to API client credentials
type ClientStore struct {
	configDir string
}

// NewClientStore creates a new ClientStore
func NewClientStore(configDir string) *ClientStore {
	return &ClientStore{configDir: configDir}
}

// GetClient returns an API client by API key
func (s *ClientStore) GetClient(apiKey string) (*config_schema.APIClient, error) {
	clients, err := config_schema.LoadAPIClients(s.configDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load api clients: %w", err)
	}

	for _, c := range clients {
		if c.APIKey == apiKey {
			if !c.Enabled {
				return nil, fmt.Errorf("api client '%s' is disabled", c.Name)
			}
			return &c, nil
		}
	}

	return nil, fmt.Errorf("invalid api key")
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
