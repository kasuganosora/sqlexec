//go:build !linux && !darwin && !freebsd && !windows

package plugin

import (
	"fmt"
	"runtime"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// unsupportedLoader is used on platforms where plugin loading is not available
type unsupportedLoader struct{}

func newPlatformLoader() PluginLoader {
	return &unsupportedLoader{}
}

func (l *unsupportedLoader) SupportedExtension() string {
	return ""
}

func (l *unsupportedLoader) Load(path string) (domain.DataSourceFactory, PluginInfo, error) {
	return nil, PluginInfo{}, fmt.Errorf("plugin loading is not supported on %s/%s", runtime.GOOS, runtime.GOARCH)
}
