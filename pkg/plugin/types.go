package plugin

import (
	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// PluginInfo contains metadata about a loaded plugin
type PluginInfo struct {
	Type        domain.DataSourceType `json:"type"`
	Version     string                `json:"version"`
	Description string                `json:"description"`
	FilePath    string                `json:"file_path"`
}

// PluginLoader is the interface for loading plugins from shared library files
// Platform-specific implementations handle .so (Go plugin) or .dll (Windows DLL)
type PluginLoader interface {
	// Load loads a plugin from the given file path
	// Returns a DataSourceFactory, plugin metadata, and any error
	Load(path string) (domain.DataSourceFactory, PluginInfo, error)

	// SupportedExtension returns the file extension this loader handles (e.g., ".so", ".dll")
	SupportedExtension() string
}

// PluginRequest is the JSON-RPC request format for DLL plugins
type PluginRequest struct {
	Method string                 `json:"method"`
	ID     string                 `json:"id,omitempty"`
	Params map[string]interface{} `json:"params,omitempty"`
}

// PluginResponse is the JSON-RPC response format from DLL plugins
type PluginResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}
