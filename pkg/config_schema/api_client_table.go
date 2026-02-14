package config_schema

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

const apiClientsFileName = "api_clients.json"

// apiClientMu protects concurrent access to the api_clients.json file
var apiClientMu sync.Mutex

// APIClient represents an API client credential record
type APIClient struct {
	Name        string `json:"name"`
	APIKey      string `json:"api_key"`
	APISecret   string `json:"api_secret"`
	Enabled     bool   `json:"enabled"`
	Permissions string `json:"permissions"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// APIClientTable is a writable virtual table for managing API client credentials
type APIClientTable struct {
	configDir string
}

// NewAPIClientTable creates a new APIClientTable
func NewAPIClientTable(configDir string) *APIClientTable {
	return &APIClientTable{configDir: configDir}
}

// GetName returns the table name
func (t *APIClientTable) GetName() string {
	return "api_client"
}

// GetSchema returns the table schema
func (t *APIClientTable) GetSchema() []domain.ColumnInfo {
	return []domain.ColumnInfo{
		{Name: "name", Type: "varchar(64)", Nullable: false, Primary: true},
		{Name: "api_key", Type: "varchar(64)", Nullable: true},
		{Name: "api_secret", Type: "varchar(128)", Nullable: true},
		{Name: "enabled", Type: "boolean", Nullable: true},
		{Name: "permissions", Type: "text", Nullable: true},
		{Name: "created_at", Type: "varchar(32)", Nullable: true},
		{Name: "updated_at", Type: "varchar(32)", Nullable: true},
	}
}

// Query executes a query against the api_client table
func (t *APIClientTable) Query(ctx context.Context, filters []domain.Filter, options *domain.QueryOptions) (*domain.QueryResult, error) {
	clients, err := loadAPIClients(t.configDir)
	if err != nil {
		return nil, err
	}

	rows := make([]domain.Row, 0, len(clients))
	for _, c := range clients {
		rows = append(rows, t.clientToRow(c, true))
	}

	if len(filters) > 0 {
		rows = applyFilters(rows, filters)
	}

	total := int64(len(rows))

	if options != nil && options.Limit > 0 {
		start := options.Offset
		if start < 0 {
			start = 0
		}
		end := start + int(options.Limit)
		if end > len(rows) {
			end = len(rows)
		}
		if start >= len(rows) {
			rows = []domain.Row{}
		} else {
			rows = rows[start:end]
		}
	}

	return &domain.QueryResult{
		Columns: t.GetSchema(),
		Rows:    rows,
		Total:   total,
	}, nil
}

// Insert inserts new API client records
func (t *APIClientTable) Insert(ctx context.Context, rows []domain.Row) (int64, error) {
	apiClientMu.Lock()
	defer apiClientMu.Unlock()

	clients, err := loadAPIClients(t.configDir)
	if err != nil {
		return 0, err
	}

	existing := make(map[string]bool)
	for _, c := range clients {
		existing[c.Name] = true
	}

	now := time.Now().Format(time.RFC3339)
	var inserted int64

	for _, row := range rows {
		name := fmt.Sprintf("%v", row["name"])
		if name == "" || name == "<nil>" {
			return inserted, fmt.Errorf("api_client name is required")
		}
		if existing[name] {
			return inserted, fmt.Errorf("api_client '%s' already exists", name)
		}

		client := APIClient{
			Name:      name,
			APIKey:    uuid.New().String(),
			APISecret: generateSecret(32),
			Enabled:   true,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if v, ok := row["enabled"]; ok {
			client.Enabled = toBool(v)
		}
		if v, ok := row["permissions"]; ok {
			client.Permissions = fmt.Sprintf("%v", v)
			if client.Permissions == "<nil>" {
				client.Permissions = ""
			}
		}

		clients = append(clients, client)
		existing[name] = true
		inserted++
	}

	if err := saveAPIClientsUnlocked(t.configDir, clients); err != nil {
		return inserted, err
	}

	return inserted, nil
}

// Update updates API client records matching the filters
func (t *APIClientTable) Update(ctx context.Context, filters []domain.Filter, updates domain.Row) (int64, error) {
	apiClientMu.Lock()
	defer apiClientMu.Unlock()

	clients, err := loadAPIClients(t.configDir)
	if err != nil {
		return 0, err
	}

	now := time.Now().Format(time.RFC3339)
	var updated int64

	for i := range clients {
		row := domain.Row{"name": clients[i].Name}
		if !matchesFilters(row, filters) {
			continue
		}

		if v, ok := updates["enabled"]; ok {
			clients[i].Enabled = toBool(v)
		}
		if v, ok := updates["permissions"]; ok {
			clients[i].Permissions = fmt.Sprintf("%v", v)
			if clients[i].Permissions == "<nil>" {
				clients[i].Permissions = ""
			}
		}
		clients[i].UpdatedAt = now
		updated++
	}

	if updated > 0 {
		if err := saveAPIClientsUnlocked(t.configDir, clients); err != nil {
			return updated, err
		}
	}

	return updated, nil
}

// Delete deletes API client records matching the filters
func (t *APIClientTable) Delete(ctx context.Context, filters []domain.Filter) (int64, error) {
	apiClientMu.Lock()
	defer apiClientMu.Unlock()

	clients, err := loadAPIClients(t.configDir)
	if err != nil {
		return 0, err
	}

	var deleted int64
	remaining := make([]APIClient, 0, len(clients))

	for _, c := range clients {
		row := domain.Row{"name": c.Name}
		if matchesFilters(row, filters) {
			deleted++
		} else {
			remaining = append(remaining, c)
		}
	}

	if deleted > 0 {
		if err := saveAPIClientsUnlocked(t.configDir, remaining); err != nil {
			return deleted, err
		}
	}

	return deleted, nil
}

// clientToRow converts an APIClient to a Row (with optional secret masking)
func (t *APIClientTable) clientToRow(c APIClient, mask bool) domain.Row {
	secret := c.APISecret
	if mask && secret != "" {
		secret = "****"
	}

	return domain.Row{
		"name":        c.Name,
		"api_key":     c.APIKey,
		"api_secret":  secret,
		"enabled":     c.Enabled,
		"permissions": c.Permissions,
		"created_at":  c.CreatedAt,
		"updated_at":  c.UpdatedAt,
	}
}

// loadAPIClients loads API client configs from the JSON file (no lock)
func loadAPIClients(configDir string) ([]APIClient, error) {
	filePath := filepath.Join(configDir, apiClientsFileName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []APIClient{}, nil
		}
		return nil, fmt.Errorf("failed to read %s: %w", filePath, err)
	}

	if len(data) == 0 {
		return []APIClient{}, nil
	}

	var clients []APIClient
	if err := json.Unmarshal(data, &clients); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", filePath, err)
	}

	return clients, nil
}

// LoadAPIClients is the exported version for use by auth layer
func LoadAPIClients(configDir string) ([]APIClient, error) {
	apiClientMu.Lock()
	defer apiClientMu.Unlock()
	return loadAPIClients(configDir)
}

// saveAPIClientsUnlocked saves without acquiring the lock (caller must hold it)
func saveAPIClientsUnlocked(configDir string, clients []APIClient) error {
	filePath := filepath.Join(configDir, apiClientsFileName)

	data, err := json.MarshalIndent(clients, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal api_client configs: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write %s: %w", filePath, err)
	}

	return nil
}

// generateSecret generates a cryptographically secure random hex string
func generateSecret(bytes int) string {
	b := make([]byte, bytes)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use uuid-based secret
		return uuid.New().String() + uuid.New().String()
	}
	return hex.EncodeToString(b)
}
