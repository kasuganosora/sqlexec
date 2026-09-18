package memory

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func diskBBQParams(nlist, nprobe int) map[string]interface{} {
	return map[string]interface{}{
		"nlist":         nlist,
		"nprobe":        nprobe,
		"ncoarse":       maxInt(2, nlist/4),
		"nprobe_coarse": maxInt(2, nprobe/2),
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func mustBuildDiskBBQ(t *testing.T, records []VectorRecord, dim int, params map[string]interface{}) *DiskBBQIndex {
	t.Helper()
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  dim,
		Params:     params,
	})
	require.NoError(t, err)
	require.NoError(t, idx.Build(context.Background(), &testVectorDataLoader{records: records}))
	return idx
}

func TestDiskBBQ_StressConcurrentSearch(t *testing.T) {
	const dim = 32
	const n = 2400
	rng := rand.New(rand.NewSource(11))
	records := makeVectorRecords(rng, n, dim)
	idx := mustBuildDiskBBQ(t, records, dim, diskBBQParams(48, 24))

	const workers = 24
	const perWorker = 240
	var failed atomic.Int32
	var wg sync.WaitGroup
	wg.Add(workers)
	start := time.Now()
	for w := 0; w < workers; w++ {
		go func(seed int64) {
			defer wg.Done()
			local := rand.New(rand.NewSource(seed))
			for i := 0; i < perWorker; i++ {
				q := records[local.Intn(n)].Vector
				result, err := idx.Search(context.Background(), q, 10, nil)
				if err != nil || len(result.IDs) == 0 || len(result.IDs) != len(result.Distances) {
					failed.Add(1)
					return
				}
			}
		}(int64(w + 1))
	}
	wg.Wait()
	elapsed := time.Since(start)
	total := workers * perWorker
	t.Logf("concurrent search n=%d workers=%d queries=%d elapsed=%s qps=%.0f",
		n, workers, total, elapsed, float64(total)/elapsed.Seconds())
	assert.Zero(t, failed.Load())
	assert.Equal(t, int64(n), idx.Stats().Count)
}

func TestDiskBBQ_StressConcurrentMutateAndSearch(t *testing.T) {
	const dim = 16
	rng := rand.New(rand.NewSource(17))
	records := makeVectorRecords(rng, 1200, dim)
	idx := mustBuildDiskBBQ(t, records, dim, diskBBQParams(24, 24))

	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	const readers = 18
	wg.Add(readers + 2)
	start := time.Now()
	for r := 0; r < readers; r++ {
		go func(seed int64) {
			defer wg.Done()
			local := rand.New(rand.NewSource(seed))
			for i := 0; i < 180; i++ {
				q := make([]float32, dim)
				for d := 0; d < dim; d++ {
					q[d] = local.Float32()
				}
				result, err := idx.Search(context.Background(), q, 5, nil)
				if err != nil {
					errCh <- err
					return
				}
				if len(result.IDs) != len(result.Distances) {
					errCh <- fmt.Errorf("mismatched result lengths")
					return
				}
			}
		}(int64(100 + r))
	}
	go func() {
		defer wg.Done()
		for i := 0; i < 240; i++ {
			vec := make([]float32, dim)
			copy(vec, records[i%len(records)].Vector)
			vec[0] += 0.01
			if err := idx.Insert(int64(10000+i), vec); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 120; i++ {
			if err := idx.Delete(int64(i)); err != nil {
				errCh <- err
				return
			}
		}
	}()
	wg.Wait()
	t.Logf("concurrent mutate+search n=%d readers=%d elapsed=%s", 1200, readers, time.Since(start))
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	assert.Greater(t, idx.Stats().Count, int64(0))
}

func TestDiskBBQ_StressIncrementalChurn(t *testing.T) {
	ctx := context.Background()
	const dim = 32
	const base = 900
	rng := rand.New(rand.NewSource(23))
	records := makeVectorRecords(rng, base, dim)
	idx := mustBuildDiskBBQ(t, records, dim, diskBBQParams(36, 24))

	start := time.Now()
	const churn = 450
	for i := 0; i < churn; i++ {
		id := int64(1000 + i)
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = rng.Float32()
		}
		require.NoError(t, idx.Insert(id, vec))
		got, err := idx.Search(ctx, vec, 8, nil)
		require.NoError(t, err)
		require.NotEmpty(t, got.IDs)
		require.NoError(t, idx.Delete(id))
		after, err := idx.Search(ctx, vec, 8, nil)
		require.NoError(t, err)
		assert.False(t, containsID(after.IDs, id))
	}
	t.Logf("incremental churn base=%d cycles=%d elapsed=%s", base, churn, time.Since(start))
	assert.Equal(t, int64(base), idx.Stats().Count)
}

func TestDiskBBQ_StressScale(t *testing.T) {
	scales := []struct {
		n      int
		dim    int
		nlist  int
		nprobe int
		k      int
	}{
		{1500, 32, 48, 24, 10},
		{4500, 32, 72, 24, 10},
		{9000, 64, 96, 36, 10},
	}
	if testing.Short() {
		scales = scales[:2]
	}
	for _, tc := range scales {
		t.Run(fmt.Sprintf("n=%d_dim=%d", tc.n, tc.dim), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(tc.n)))
			records := makeVectorRecords(rng, tc.n, tc.dim)
			buildStart := time.Now()
			idx := mustBuildDiskBBQ(t, records, tc.dim, diskBBQParams(tc.nlist, tc.nprobe))
			buildElapsed := time.Since(buildStart)
			assert.Equal(t, int64(tc.n), idx.Stats().Count)

			hits := 0
			queries := 120
			searchStart := time.Now()
			for i := 0; i < queries; i++ {
				q := records[i*tc.n/queries].Vector
				result, err := idx.Search(context.Background(), q, tc.k, nil)
				require.NoError(t, err)
				require.LessOrEqual(t, len(result.IDs), tc.k)
				require.NotEmpty(t, result.IDs)
				if containsID(result.IDs, records[i*tc.n/queries].ID) {
					hits++
				}
			}
			searchElapsed := time.Since(searchStart)
			rate := float64(hits) / float64(queries)
			t.Logf("self-recall %d/%d = %.2f build=%s search=%s (%.2fms/q) mem=%d",
				hits, queries, rate, buildElapsed, searchElapsed,
				float64(searchElapsed.Microseconds())/1000.0/float64(queries),
				idx.Stats().MemorySize)
			require.GreaterOrEqual(t, rate, 0.5, "self-recall too low at scale %d", tc.n)
		})
	}
}

func TestDiskBBQ_StressFilterNoLeak(t *testing.T) {
	const dim = 32
	rng := rand.New(rand.NewSource(29))
	records := makeVectorRecords(rng, 1800, dim)
	idx := mustBuildDiskBBQ(t, records, dim, diskBBQParams(48, 24))
	allowed := []int64{3, 17, 42, 99, 120, 250, 400, 800, 1200, 1600}
	filter := &VectorFilter{IDs: allowed}
	start := time.Now()
	for i := 0; i < 240; i++ {
		result, err := idx.Search(context.Background(), records[i].Vector, 10, filter)
		require.NoError(t, err)
		for _, id := range result.IDs {
			assert.True(t, containsID(allowed, id), "filter leaked id %d", id)
		}
	}
	t.Logf("filter stress n=%d queries=240 elapsed=%s", 1800, time.Since(start))
}
