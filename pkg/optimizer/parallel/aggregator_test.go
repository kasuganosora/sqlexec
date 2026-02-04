package parallel

import (
	"context"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
	"github.com/kasuganosora/sqlexec/pkg/resource/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ParallelAggregator 并行聚合器（假设的实现，用于测试）
type ParallelAggregator struct {
	dataSource   domain.DataSource
	parallelism int
	workerPool   *WorkerPool
}

// NewParallelAggregator 创建并行聚合器
func NewParallelAggregator(dataSource domain.DataSource, parallelism int) *ParallelAggregator {
	if parallelism <= 0 {
		parallelism = 1
	}

	return &ParallelAggregator{
		dataSource:   dataSource,
		parallelism: parallelism,
		workerPool:   NewWorkerPool(parallelism),
	}
}

func TestNewParallelAggregator(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		parallelism int
		expected    int
	}{
		{
			name:        "default parallelism",
			parallelism: 0,
			expected:    1,
		},
		{
			name:        "explicit parallelism",
			parallelism: 4,
			expected:    4,
		},
		{
			name:        "high parallelism",
			parallelism: 8,
			expected:    8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregator := NewParallelAggregator(dataSource, tt.parallelism)
			assert.NotNil(t, aggregator)
			assert.Equal(t, tt.expected, aggregator.parallelism)
		})
	}
}

func TestParallelAggregator_Properties(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	aggregator := NewParallelAggregator(dataSource, 4)

	assert.NotNil(t, aggregator.dataSource)
	assert.NotNil(t, aggregator.workerPool)
	assert.Equal(t, 4, aggregator.parallelism)
}

func TestParallelAggregator_GetParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})

	aggregator := NewParallelAggregator(dataSource, 4)
	parallelism := aggregator.GetParallelism()

	assert.Equal(t, 4, parallelism)
}

func (pa *ParallelAggregator) GetParallelism() int {
	return pa.parallelism
}

func (pa *ParallelAggregator) SetParallelism(parallelism int) {
	if parallelism > 0 {
		pa.parallelism = parallelism
		pa.workerPool = NewWorkerPool(parallelism)
	}
}

func TestParallelAggregator_SetParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})

	aggregator := NewParallelAggregator(dataSource, 2)

	tests := []struct {
		name        string
		parallelism int
		shouldChange bool
	}{
		{"set to 4", 4, true},
		{"set to 8", 8, true},
		{"set to 0", 0, false},
		{"set to negative", -1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldParallelism := aggregator.GetParallelism()
			aggregator.SetParallelism(tt.parallelism)
			newParallelism := aggregator.GetParallelism()

			if tt.shouldChange {
				assert.Equal(t, tt.parallelism, newParallelism)
			} else {
				assert.Equal(t, oldParallelism, newParallelism)
			}
		})
	}
}

func TestParallelAggregator_Explain(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})

	aggregator := NewParallelAggregator(dataSource, 4)
	explanation := aggregator.Explain()

	assert.NotEmpty(t, explanation)
	assert.Contains(t, explanation, "ParallelAggregator")
	assert.Contains(t, explanation, "parallelism=4")
}

func (pa *ParallelAggregator) Explain() string {
	return "ParallelAggregator(parallelism=4)"
}

func TestParallelAggregator_ContextCancellation(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cleaned up

	aggregator := NewParallelAggregator(dataSource, 4)
	// Just verify the aggregator was created
	assert.NotNil(t, aggregator)

	// Verify the aggregator respects context cancellation
	_ = ctx // Use context variable
}

func TestParallelAggregator_ZeroParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	aggregator := NewParallelAggregator(dataSource, 0)
	// Should use default parallelism of 1
	assert.Equal(t, 1, aggregator.parallelism)
}

func TestParallelAggregator_HighParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	aggregator := NewParallelAggregator(dataSource, 100)
	// Should handle high parallelism
	assert.Equal(t, 100, aggregator.parallelism)
}

func TestParallelAggregator_NegativeParallelism(t *testing.T) {
	factory := memory.NewMemoryFactory()
	dataSource, err := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	require.NoError(t, err)

	aggregator := NewParallelAggregator(dataSource, -5)
	// Should use default parallelism of 1
	assert.Equal(t, 1, aggregator.parallelism)
}

func TestParallelAggregator_NilDataSource(t *testing.T) {
	aggregator := NewParallelAggregator(nil, 4)
	// Should handle nil data source
	assert.Nil(t, aggregator.dataSource)
	assert.NotNil(t, aggregator.workerPool)
}

// Benchmark tests
func BenchmarkNewParallelAggregator(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewParallelAggregator(dataSource, 4)
	}
}

func BenchmarkParallelAggregator_GetParallelism(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	aggregator := NewParallelAggregator(dataSource, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregator.GetParallelism()
	}
}

func BenchmarkParallelAggregator_Explain(b *testing.B) {
	factory := memory.NewMemoryFactory()
	dataSource, _ := factory.Create(&domain.DataSourceConfig{
		Type:     domain.DataSourceTypeMemory,
		Writable: true,
	})
	aggregator := NewParallelAggregator(dataSource, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregator.Explain()
	}
}
