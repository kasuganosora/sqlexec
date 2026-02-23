package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// --- Benchmark data generators ---

func generateString(length int, seed byte) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = 'a' + (seed+byte(i))%26
	}
	return string(b)
}

func generateRows(count, strLen int) []string {
	rows := make([]string, count)
	for i := range rows {
		rows[i] = generateString(strLen, byte(i%256))
	}
	return rows
}

// --- MatchesLike micro-benchmarks ---

func BenchmarkMatchesLike_Prefix(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
	}
	for _, l := range lengths {
		value := generateString(l.len, 0)
		prefix := value[:3] + "%"
		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				MatchesLike(value, prefix)
			}
		})
	}
}

func BenchmarkMatchesLike_Suffix(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
	}
	for _, l := range lengths {
		value := generateString(l.len, 0)
		suffix := "%" + value[l.len-3:]
		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				MatchesLike(value, suffix)
			}
		})
	}
}

func BenchmarkMatchesLike_Contains(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
	}
	for _, l := range lengths {
		value := generateString(l.len, 0)
		mid := value[l.len/2-1 : l.len/2+2]
		pattern := "%" + mid + "%"
		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				MatchesLike(value, pattern)
			}
		})
	}
}

func BenchmarkMatchesLike_Exact(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
	}
	for _, l := range lengths {
		value := generateString(l.len, 0)
		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				MatchesLike(value, value)
			}
		})
	}
}

func BenchmarkMatchesLike_Complex(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
	}
	for _, l := range lengths {
		value := generateString(l.len, 0)
		// Pattern: %<char>%<char>%<char>%
		c1 := string(value[1])
		c2 := string(value[l.len/2])
		c3 := string(value[l.len-2])
		pattern := "%" + c1 + "%" + c2 + "%" + c3 + "%"
		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				MatchesLike(value, pattern)
			}
		})
	}
}

func BenchmarkMatchesLike_Underscore(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
	}
	for _, l := range lengths {
		value := generateString(l.len, 0)
		// Pattern: first char + _ + rest of prefix + %
		pattern := string(value[0]) + "_" + string(value[2]) + "%"
		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				MatchesLike(value, pattern)
			}
		})
	}
}

func BenchmarkMatchesLike_SinglePercent(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
	}
	for _, l := range lengths {
		value := generateString(l.len, 0)
		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				MatchesLike(value, "%")
			}
		})
	}
}

// BenchmarkCompareLike_FullPath benchmarks the full CompareValues("LIKE") path
func BenchmarkCompareLike_FullPath(b *testing.B) {
	cases := []struct {
		name    string
		value   interface{}
		pattern interface{}
	}{
		{"StringPrefix", "hello world", "hello%"},
		{"StringContains", "hello world", "%llo%"},
		{"StringSuffix", "hello world", "%world"},
		{"StringExact", "hello", "hello"},
		{"IntToString", 12345, "%234%"},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				CompareValues(c.value, c.pattern, "LIKE")
			}
		})
	}
}

// BenchmarkStringsContains benchmarks stdlib strings.Contains at various lengths
func BenchmarkStringsContains(b *testing.B) {
	lengths := []struct {
		name string
		len  int
	}{
		{"Short10", 10},
		{"Medium100", 100},
		{"Long1000", 1000},
		{"VeryLong10000", 10000},
	}
	for _, l := range lengths {
		s := generateString(l.len, 0)
		substr := s[l.len/2-2 : l.len/2+2]

		b.Run(l.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				strings.Contains(s, substr)
			}
		})
	}
}

// BenchmarkMatchesLike_Batch simulates filtering rows with LIKE
func BenchmarkMatchesLike_Batch(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"1K", 1000},
		{"10K", 10000},
	}
	patterns := []struct {
		name    string
		pattern string
	}{
		{"Prefix", "abc%"},
		{"Suffix", "%xyz"},
		{"Contains", "%mno%"},
	}
	for _, s := range sizes {
		rows := generateRows(s.n, 50)
		for _, p := range patterns {
			b.Run(s.name+"_"+p.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, row := range rows {
						MatchesLike(row, p.pattern)
					}
				}
			})
		}
	}
}

// --- Baseline collection ---

type LikeBenchmarkResult struct {
	NsPerOp     float64 `json:"ns_per_op"`
	AllocsPerOp int64   `json:"allocs_per_op"`
	BytesPerOp  int64   `json:"bytes_per_op"`
	Iterations  int     `json:"iterations"`
}

type LikeBaseline struct {
	Timestamp  string                         `json:"timestamp"`
	GoVersion  string                         `json:"go_version"`
	SystemInfo map[string]interface{}         `json:"system_info"`
	Benchmarks map[string]LikeBenchmarkResult `json:"benchmarks"`
}

func TestSaveLikeBaseline(t *testing.T) {
	if os.Getenv("LIKE_SAVE_BASELINE") == "" {
		t.Skip("Set LIKE_SAVE_BASELINE=1 to generate baseline")
	}

	benchmarks := map[string]func(*testing.B){
		"Prefix_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := v[:3] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Prefix_Medium100": func(b *testing.B) {
			v := generateString(100, 0)
			p := v[:3] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Prefix_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := v[:3] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Suffix_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := "%" + v[7:]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Suffix_Medium100": func(b *testing.B) {
			v := generateString(100, 0)
			p := "%" + v[97:]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Suffix_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := "%" + v[997:]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Contains_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := "%" + v[3:6] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Contains_Medium100": func(b *testing.B) {
			v := generateString(100, 0)
			p := "%" + v[48:52] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Contains_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := "%" + v[498:502] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Exact_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, v)
			}
		},
		"Exact_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, v)
			}
		},
		"Complex_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := "%" + string(v[1]) + "%" + string(v[5]) + "%" + string(v[8]) + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Complex_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := "%" + string(v[1]) + "%" + string(v[500]) + "%" + string(v[998]) + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"SinglePercent": func(b *testing.B) {
			v := generateString(100, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, "%")
			}
		},
		"CompareLike_String": func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				CompareValues("hello world", "hello%", "LIKE")
			}
		},
		"CompareLike_Int": func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				CompareValues(12345, "%234%", "LIKE")
			}
		},
		"StdlibContains_1000": func(b *testing.B) {
			s := generateString(1000, 0)
			sub := s[498:502]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				strings.Contains(s, sub)
			}
		},
		"Batch1K_Prefix": func(b *testing.B) {
			rows := generateRows(1000, 50)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, r := range rows {
					MatchesLike(r, "abc%")
				}
			}
		},
		"Batch1K_Contains": func(b *testing.B) {
			rows := generateRows(1000, 50)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, r := range rows {
					MatchesLike(r, "%mno%")
				}
			}
		},
	}

	baseline := LikeBaseline{
		Timestamp: time.Now().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		SystemInfo: map[string]interface{}{
			"cpu_cores":  runtime.NumCPU(),
			"gomaxprocs": runtime.GOMAXPROCS(0),
		},
		Benchmarks: make(map[string]LikeBenchmarkResult),
	}

	for name, fn := range benchmarks {
		result := testing.Benchmark(fn)
		baseline.Benchmarks[name] = LikeBenchmarkResult{
			NsPerOp:     float64(result.T.Nanoseconds()) / float64(result.N),
			AllocsPerOp: int64(result.AllocsPerOp()),
			BytesPerOp:  int64(result.AllocedBytesPerOp()),
			Iterations:  result.N,
		}
		fmt.Printf("  %-30s %10.1f ns/op  %5d allocs  %8d B/op\n",
			name,
			float64(result.T.Nanoseconds())/float64(result.N),
			result.AllocsPerOp(),
			result.AllocedBytesPerOp(),
		)
	}

	data, err := json.MarshalIndent(baseline, "", "  ")
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	if err := os.WriteFile("like_baseline.json", data, 0644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	t.Logf("Baseline saved to like_baseline.json with %d benchmarks", len(baseline.Benchmarks))
}

func TestCompareLikeBaseline(t *testing.T) {
	if os.Getenv("LIKE_COMPARE") == "" {
		t.Skip("Set LIKE_COMPARE=1 to compare against baseline")
	}

	// Load baseline
	data, err := os.ReadFile("like_baseline.json")
	if err != nil {
		t.Fatalf("read baseline: %v (run with LIKE_SAVE_BASELINE=1 first)", err)
	}
	var baseline LikeBaseline
	if err := json.Unmarshal(data, &baseline); err != nil {
		t.Fatalf("parse baseline: %v", err)
	}

	// Re-run benchmarks with same definitions (minus removed CustomContains)
	benchmarks := map[string]func(*testing.B){
		"Prefix_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := v[:3] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Prefix_Medium100": func(b *testing.B) {
			v := generateString(100, 0)
			p := v[:3] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Prefix_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := v[:3] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Suffix_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := "%" + v[7:]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Suffix_Medium100": func(b *testing.B) {
			v := generateString(100, 0)
			p := "%" + v[97:]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Suffix_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := "%" + v[997:]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Contains_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := "%" + v[3:6] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Contains_Medium100": func(b *testing.B) {
			v := generateString(100, 0)
			p := "%" + v[48:52] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Contains_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := "%" + v[498:502] + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Exact_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, v)
			}
		},
		"Exact_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, v)
			}
		},
		"Complex_Short10": func(b *testing.B) {
			v := generateString(10, 0)
			p := "%" + string(v[1]) + "%" + string(v[5]) + "%" + string(v[8]) + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"Complex_Long1000": func(b *testing.B) {
			v := generateString(1000, 0)
			p := "%" + string(v[1]) + "%" + string(v[500]) + "%" + string(v[998]) + "%"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, p)
			}
		},
		"SinglePercent": func(b *testing.B) {
			v := generateString(100, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MatchesLike(v, "%")
			}
		},
		"CompareLike_String": func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				CompareValues("hello world", "hello%", "LIKE")
			}
		},
		"CompareLike_Int": func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				CompareValues(12345, "%234%", "LIKE")
			}
		},
		"Batch1K_Prefix": func(b *testing.B) {
			rows := generateRows(1000, 50)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, r := range rows {
					MatchesLike(r, "abc%")
				}
			}
		},
		"Batch1K_Contains": func(b *testing.B) {
			rows := generateRows(1000, 50)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, r := range rows {
					MatchesLike(r, "%mno%")
				}
			}
		},
	}

	fmt.Println("\n=== LIKE Optimization Comparison ===")
	fmt.Printf("%-30s %12s %12s %10s\n", "Benchmark", "Before", "After", "Change")
	fmt.Println(strings.Repeat("-", 70))

	for name, fn := range benchmarks {
		oldResult, exists := baseline.Benchmarks[name]
		if !exists {
			continue
		}
		result := testing.Benchmark(fn)
		newNs := float64(result.T.Nanoseconds()) / float64(result.N)

		change := (newNs - oldResult.NsPerOp) / oldResult.NsPerOp * 100
		sign := "+"
		if change < 0 {
			sign = ""
		}

		fmt.Printf("%-30s %10.1f ns %10.1f ns %s%.1f%%\n",
			name, oldResult.NsPerOp, newNs, sign, change)
	}
	fmt.Println()
}
