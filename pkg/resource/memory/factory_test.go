package memory

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewMemoryFactory(t *testing.T) {
	factory := NewMemoryFactory()

	assert.NotNil(t, factory)
	assert.Equal(t, domain.DataSourceTypeMemory, factory.GetType())
}

func TestMemoryFactory_Create_DefaultConfig(t *testing.T) {
	factory := NewMemoryFactory()

	datasource, err := factory.Create(nil)

	assert.NoError(t, err)
	assert.NotNil(t, datasource)

	mvccDS, ok := datasource.(*MVCCDataSource)
	assert.True(t, ok)
	assert.NotNil(t, mvccDS)
	assert.Equal(t, "memory", mvccDS.config.Name)
	assert.True(t, mvccDS.config.Writable)
}

func TestMemoryFactory_Create_WithConfig(t *testing.T) {
	factory := NewMemoryFactory()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "test_memory",
		Writable: false,
	}

	datasource, err := factory.Create(config)

	assert.NoError(t, err)
	assert.NotNil(t, datasource)

	mvccDS, ok := datasource.(*MVCCDataSource)
	assert.True(t, ok)
	assert.Equal(t, "test_memory", mvccDS.config.Name)
	assert.False(t, mvccDS.config.Writable)
}

func TestMemoryFactory_Create_WithWritableTrue(t *testing.T) {
	factory := NewMemoryFactory()

	config := &domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Name:     "writable_db",
		Writable: true,
	}

	datasource, err := factory.Create(config)

	assert.NoError(t, err)
	assert.NotNil(t, datasource)

	mvccDS, ok := datasource.(*MVCCDataSource)
	assert.True(t, ok)
	assert.True(t, mvccDS.config.Writable)
}

func TestMemoryFactory_Create_MultipleInstances(t *testing.T) {
	factory := NewMemoryFactory()

	// 创建多个独立的数据源实例
	ds1, err := factory.Create(&domain.DataSourceConfig{Name: "db1", Writable: true})
	assert.NoError(t, err)
	assert.NotNil(t, ds1)

	ds2, err := factory.Create(&domain.DataSourceConfig{Name: "db2", Writable: false})
	assert.NoError(t, err)
	assert.NotNil(t, ds2)

	// 确保它们是不同的实例
	assert.NotEqual(t, ds1, ds2)

	mvccDS1, ok1 := ds1.(*MVCCDataSource)
	mvccDS2, ok2 := ds2.(*MVCCDataSource)
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, "db1", mvccDS1.config.Name)
	assert.Equal(t, "db2", mvccDS2.config.Name)
}

func TestMemoryFactory_Create_ConnectAndClose(t *testing.T) {
	factory := NewMemoryFactory()

	datasource, err := factory.Create(nil)
	assert.NoError(t, err)

	ctx := context.Background()

	// 测试连接
	err = datasource.Connect(ctx)
	assert.NoError(t, err)

	// 测试关闭
	err = datasource.Close(ctx)
	assert.NoError(t, err)
}

func TestMemoryFactory_GetType(t *testing.T) {
	factory := NewMemoryFactory()

	assert.Equal(t, domain.DataSourceTypeMemory, factory.GetType())
}
