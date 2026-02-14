package plugin

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kasuganosora/sqlexec/pkg/config_schema"
	"github.com/kasuganosora/sqlexec/pkg/resource/application"
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// PluginManager manages the lifecycle of datasource plugins
type PluginManager struct {
	registry  *application.Registry
	dsManager *application.DataSourceManager
	configDir string
	loader    PluginLoader
	mu        sync.Mutex
	plugins   []PluginInfo
}

// NewPluginManager creates a new plugin manager
func NewPluginManager(registry *application.Registry, dsManager *application.DataSourceManager, configDir string) *PluginManager {
	return &PluginManager{
		registry:  registry,
		dsManager: dsManager,
		configDir: configDir,
		loader:    newPlatformLoader(),
		plugins:   make([]PluginInfo, 0),
	}
}

// ScanAndLoad scans the plugin directory and loads all compatible plugins
func (pm *PluginManager) ScanAndLoad(pluginDir string) error {
	if pm.loader == nil {
		log.Printf("[PLUGIN] Plugin loading not supported on this platform")
		return nil
	}

	// Check if plugin directory exists
	info, err := os.Stat(pluginDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[PLUGIN] Plugin directory '%s' does not exist, skipping", pluginDir)
			return nil
		}
		return fmt.Errorf("failed to stat plugin directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("plugin path '%s' is not a directory", pluginDir)
	}

	ext := pm.loader.SupportedExtension()
	log.Printf("[PLUGIN] Scanning '%s' for %s files...", pluginDir, ext)

	entries, err := os.ReadDir(pluginDir)
	if err != nil {
		return fmt.Errorf("failed to read plugin directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ext) {
			continue
		}

		pluginPath := filepath.Join(pluginDir, name)
		if err := pm.LoadPlugin(pluginPath); err != nil {
			log.Printf("[PLUGIN] Failed to load plugin '%s': %v", name, err)
			continue
		}
	}

	pm.mu.Lock()
	count := len(pm.plugins)
	pm.mu.Unlock()
	log.Printf("[PLUGIN] Loaded %d plugin(s)", count)

	// After loading all plugins, create datasource instances from config
	pm.createDatasourcesFromConfig()

	return nil
}

// LoadPlugin loads a single plugin file
func (pm *PluginManager) LoadPlugin(path string) error {
	factory, info, err := pm.loader.Load(path)
	if err != nil {
		return err
	}

	// Register the factory in the registry
	if err := pm.registry.Register(factory); err != nil {
		// If already registered, skip
		if strings.Contains(err.Error(), "already registered") {
			log.Printf("[PLUGIN] Factory type '%s' already registered, skipping", info.Type)
			return nil
		}
		return fmt.Errorf("failed to register factory: %w", err)
	}

	pm.mu.Lock()
	pm.plugins = append(pm.plugins, info)
	pm.mu.Unlock()
	log.Printf("[PLUGIN] Loaded plugin: type=%s, version=%s, file=%s",
		info.Type, info.Version, filepath.Base(path))

	return nil
}

// createDatasourcesFromConfig reads config.datasource and creates instances for plugin types
func (pm *PluginManager) createDatasourcesFromConfig() {
	if pm.configDir == "" {
		return
	}

	configs, err := config_schema.LoadDatasources(pm.configDir)
	if err != nil {
		log.Printf("[PLUGIN] Failed to load datasource configs: %v", err)
		return
	}

	// Build a set of plugin types
	pm.mu.Lock()
	pluginTypes := make(map[domain.DataSourceType]bool)
	for _, p := range pm.plugins {
		pluginTypes[p.Type] = true
	}
	pm.mu.Unlock()

	for _, cfg := range configs {
		// Only create datasources that match a loaded plugin type
		if !pluginTypes[cfg.Type] {
			continue
		}

		// Check if already registered
		if _, err := pm.dsManager.Get(cfg.Name); err == nil {
			log.Printf("[PLUGIN] Datasource '%s' already registered, skipping", cfg.Name)
			continue
		}

		cfgCopy := cfg
		ds, err := pm.dsManager.CreateFromConfig(&cfgCopy)
		if err != nil {
			log.Printf("[PLUGIN] Failed to create datasource '%s' (type=%s): %v",
				cfg.Name, cfg.Type, err)
			continue
		}

		if err := ds.Connect(context.Background()); err != nil {
			log.Printf("[PLUGIN] Failed to connect datasource '%s': %v", cfg.Name, err)
			continue
		}

		if err := pm.dsManager.Register(cfg.Name, ds); err != nil {
			// Close the connection to avoid resource leak
			ds.Close(context.Background())
			log.Printf("[PLUGIN] Failed to register datasource '%s': %v", cfg.Name, err)
			continue
		}

		log.Printf("[PLUGIN] Created datasource '%s' from config (type=%s)", cfg.Name, cfg.Type)
	}
}

// GetLoadedPlugins returns the list of loaded plugins
func (pm *PluginManager) GetLoadedPlugins() []PluginInfo {
	pm.mu.Lock()
	result := make([]PluginInfo, len(pm.plugins))
	copy(result, pm.plugins)
	pm.mu.Unlock()
	return result
}
