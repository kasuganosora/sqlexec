package memory

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func clusteredVectorRecords(rng *rand.Rand, clusters, perCluster, dim int) []VectorRecord {
	records := make([]VectorRecord, 0, clusters*perCluster)
	var id int64
	for c := 0; c < clusters; c++ {
		center := make([]float32, dim)
		center[c%dim] = 1
		for d := 0; d < dim; d++ {
			center[d] += rng.Float32() * 0.04
		}
		for i := 0; i < perCluster; i++ {
			vec := make([]float32, dim)
			for d := 0; d < dim; d++ {
				vec[d] = center[d] + (rng.Float32()-0.5)*0.06
			}
			records = append(records, VectorRecord{ID: id, Vector: vec})
			id++
		}
	}
	return records
}

func benchDiskBBQ(records []VectorRecord, dim int) *DiskBBQIndex {
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  dim,
		Params:     diskBBQParams(16, 8),
	})
	if err != nil {
		panic(err)
	}
	if err := idx.Build(context.Background(), &testVectorDataLoader{records: records}); err != nil {
		panic(err)
	}
	return idx
}

func BenchmarkDiskBBQ_Build(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	const dim = 64
	records := makeVectorRecords(rng, 1000, dim)
	loader := &testVectorDataLoader{records: records}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
			MetricType: VectorMetricL2,
			Dimension:  dim,
			Params:     diskBBQParams(16, 8),
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := idx.Build(context.Background(), loader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskBBQ_Search(b *testing.B) {
	rng := rand.New(rand.NewSource(2))
	const dim = 64
	records := makeVectorRecords(rng, 1500, dim)
	idx := benchDiskBBQ(records, dim)
	queries := make([][]float32, 64)
	for i := range queries {
		queries[i] = records[i*20].Vector
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := idx.Search(ctx, queries[i%len(queries)], 10, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskBBQ_Insert(b *testing.B) {
	const dim = 32
	idx, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  dim,
		Params:     diskBBQParams(8, 8),
	})
	if err != nil {
		b.Fatal(err)
	}
	vec := make([]float32, dim)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vec[0] = float32(i)
		if err := idx.Insert(int64(i), vec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiskBBQ_SearchVsFlat(b *testing.B) {
	rng := rand.New(rand.NewSource(4))
	const dim = 64
	records := makeVectorRecords(rng, 1200, dim)
	loader := &testVectorDataLoader{records: records}
	query := records[10].Vector

	flat, err := NewFlatIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim})
	if err != nil {
		b.Fatal(err)
	}
	if err := flat.Build(context.Background(), loader); err != nil {
		b.Fatal(err)
	}
	bbq := benchDiskBBQ(records, dim)
	ctx := context.Background()

	b.Run("flat", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := flat.Search(ctx, query, 10, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("diskbbq", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := bbq.Search(ctx, query, 10, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestDiskBBQ_SearchLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skip performance test in short mode")
	}
	ctx := context.Background()
	const dim = 64
	const n = 2000
	rng := rand.New(rand.NewSource(41))
	records := makeVectorRecords(rng, n, dim)
	idx := mustBuildDiskBBQ(t, records, dim, diskBBQParams(24, 8))

	const queries = 200
	latencies := make([]float64, queries)
	for i := 0; i < queries; i++ {
		start := time.Now()
		_, err := idx.Search(ctx, records[i*n/queries].Vector, 10, nil)
		require.NoError(t, err)
		latencies[i] = float64(time.Since(start).Microseconds()) / 1000.0
	}
	sort.Float64s(latencies)
	var sum float64
	for _, l := range latencies {
		sum += l
	}
	avg := sum / float64(queries)
	p95 := latencies[int(float64(queries)*0.95)]
	p99 := latencies[int(float64(queries)*0.99)]
	t.Logf("DiskBBQ latency n=%d dim=%d: avg=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
		n, dim, avg, p95, p99, latencies[queries-1])
	require.LessOrEqual(t, p99, 20.0, "P99 should be <= 20ms, got %.3fms", p99)
	require.LessOrEqual(t, avg, 10.0, "avg should be <= 10ms, got %.3fms", avg)
}

func TestDiskBBQ_RecallVsFlat(t *testing.T) {
	ctx := context.Background()
	const dim = 32
	rng := rand.New(rand.NewSource(43))
	records := clusteredVectorRecords(rng, 20, 40, dim)
	loader := &testVectorDataLoader{records: records}

	flat, err := NewFlatIndex("embedding", &VectorIndexConfig{MetricType: VectorMetricL2, Dimension: dim})
	require.NoError(t, err)
	require.NoError(t, flat.Build(ctx, loader))

	bbq, err := NewDiskBBQIndex("embedding", &VectorIndexConfig{
		MetricType: VectorMetricL2,
		Dimension:  dim,
		Params:     diskBBQParams(20, 12),
	})
	require.NoError(t, err)
	require.NoError(t, bbq.Build(ctx, loader))

	const nq = 40
	const k = 10
	trueIDs := make([][]int64, nq)
	gotIDs := make([][]int64, nq)
	for i := 0; i < nq; i++ {
		q := records[i*len(records)/nq].Vector
		truth, err := flat.Search(ctx, q, k, nil)
		require.NoError(t, err)
		approx, err := bbq.Search(ctx, q, k, nil)
		require.NoError(t, err)
		trueIDs[i] = truth.IDs
		gotIDs[i] = approx.IDs
	}
	recall := GetRecallValue(trueIDs, gotIDs)
	t.Logf("DiskBBQ recall@%d vs Flat: %.3f", k, recall)
	require.GreaterOrEqual(t, recall, 0.35, "clustered recall@10 should be >= 35%%, got %.3f", recall)
}
