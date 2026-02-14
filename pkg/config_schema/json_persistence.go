package config_schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

const datasourcesFileName = "datasources.json"

// fileMu protects concurrent access to the datasources.json file
var fileMu sync.Mutex

// loadDatasources loads datasource configs from the JSON file (no lock)
func loadDatasources(configDir string) ([]domain.DataSourceConfig, error) {
	filePath := filepath.Join(configDir, datasourcesFileName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []domain.DataSourceConfig{}, nil
		}
		return nil, fmt.Errorf("failed to read %s: %w", filePath, err)
	}

	if len(data) == 0 {
		return []domain.DataSourceConfig{}, nil
	}

	var configs []domain.DataSourceConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", filePath, err)
	}

	return configs, nil
}

// LoadDatasources is the exported version for use by server startup
func LoadDatasources(configDir string) ([]domain.DataSourceConfig, error) {
	fileMu.Lock()
	defer fileMu.Unlock()
	return loadDatasources(configDir)
}
