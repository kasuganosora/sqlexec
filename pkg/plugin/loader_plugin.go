//go:build linux || darwin || freebsd

package plugin

import (
	"fmt"
	"plugin"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// GoPluginLoader loads Go plugins (.so files) on Linux/macOS
type GoPluginLoader struct{}

func newPlatformLoader() PluginLoader {
	return &GoPluginLoader{}
}

// SupportedExtension returns ".so"
func (l *GoPluginLoader) SupportedExtension() string {
	return ".so"
}

// Load loads a Go plugin from the given .so file
// The plugin must export a "NewFactory" variable of type func() domain.DataSourceFactory
func (l *GoPluginLoader) Load(path string) (domain.DataSourceFactory, PluginInfo, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, PluginInfo{}, fmt.Errorf("failed to open plugin '%s': %w", path, err)
	}

	// Look up the NewFactory symbol
	sym, err := p.Lookup("NewFactory")
	if err != nil {
		return nil, PluginInfo{}, fmt.Errorf("plugin '%s' missing 'NewFactory' symbol: %w", path, err)
	}

	// Try as a function variable: var NewFactory func() domain.DataSourceFactory
	factoryFn, ok := sym.(*func() domain.DataSourceFactory)
	if !ok {
		return nil, PluginInfo{}, fmt.Errorf("plugin '%s': 'NewFactory' has wrong type, expected *func() domain.DataSourceFactory", path)
	}

	factory := (*factoryFn)()
	if factory == nil {
		return nil, PluginInfo{}, fmt.Errorf("plugin '%s': NewFactory() returned nil", path)
	}

	// Try to get optional plugin info
	info := PluginInfo{
		Type:     factory.GetType(),
		FilePath: path,
		Version:  "1.0.0",
	}

	// Check for optional PluginVersion variable
	if vSym, err := p.Lookup("PluginVersion"); err == nil {
		if version, ok := vSym.(*string); ok {
			info.Version = *version
		}
	}

	// Check for optional PluginDescription variable
	if dSym, err := p.Lookup("PluginDescription"); err == nil {
		if desc, ok := dSym.(*string); ok {
			info.Description = *desc
		}
	}

	return factory, info, nil
}
