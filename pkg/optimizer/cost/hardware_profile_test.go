package cost

import (
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectHardwareProfile(t *testing.T) {
	profile := DetectHardwareProfile()

	assert.NotNil(t, profile, "profile should not be nil")
	assert.NotEmpty(t, profile.OS, "OS should not be empty")
	assert.NotEmpty(t, profile.Architecture, "architecture should not be empty")
	assert.NotEmpty(t, profile.RuntimeVersion, "runtime version should not be empty")
	assert.Greater(t, profile.CPUCores, 0, "CPU cores should be positive")
	assert.Greater(t, profile.CPUFrequency, 0.0, "CPU frequency should be positive")
	assert.Greater(t, profile.TotalMemory, int64(0), "total memory should be positive")
	assert.Greater(t, profile.AvailableMemory, int64(0), "available memory should be positive")
	assert.NotEmpty(t, profile.DiskType, "disk type should not be empty")
	assert.Greater(t, profile.DiskIO, 0.0, "disk IO should be positive")
	assert.GreaterOrEqual(t, profile.DiskSeekTime, 0.0, "disk seek time should be non-negative")
	assert.Greater(t, profile.NetworkBandwidth, 0.0, "network bandwidth should be positive")
	assert.GreaterOrEqual(t, profile.NetworkLatency, 0.0, "network latency should be non-negative")
	assert.NotEmpty(t, profile.MeasuredAt.String(), "measured time should be set")
}

func TestDetectHardwareProfile_SystemValues(t *testing.T) {
	profile := DetectHardwareProfile()

	// Check if detected values match runtime
	assert.Equal(t, runtime.GOOS, profile.OS, "OS should match runtime")
	assert.Equal(t, runtime.GOARCH, profile.Architecture, "architecture should match runtime")
	assert.Equal(t, runtime.Version(), profile.RuntimeVersion, "runtime version should match")
	assert.Equal(t, runtime.NumCPU(), profile.CPUCores, "CPU cores should match runtime")
}

func TestDetectHardwareProfile_DiskType(t *testing.T) {
	profile := DetectHardwareProfile()

	// Disk type should be one of the known types
	validTypes := map[string]bool{
		"SSD":  true,
		"HDD":  true,
		"NVMe": true,
	}

	assert.True(t, validTypes[profile.DiskType], "disk type should be valid")
}

func TestDetectCloudEnvironment(t *testing.T) {
	// Test without cloud environment variables
	os.Unsetenv("AWS_REGION")
	os.Unsetenv("GOOGLE_CLOUD_PROJECT")
	os.Unsetenv("AZURE_RESOURCE_GROUP")

	isCloud := detectCloudEnvironment()
	assert.False(t, isCloud, "should not detect cloud environment without env vars")

	// Test with AWS environment variable
	os.Setenv("AWS_REGION", "us-east-1")
	defer os.Unsetenv("AWS_REGION")

	isCloud = detectCloudEnvironment()
	assert.True(t, isCloud, "should detect cloud environment with AWS env var")
}

func TestEstimateCPUFrequency(t *testing.T) {
	freq := estimateCPUFrequency()

	assert.Greater(t, freq, 0.0, "CPU frequency should be positive")
	assert.LessOrEqual(t, freq, 10.0, "CPU frequency should be reasonable (<= 10GHz)")
}

func TestNormalizeCPUSpeed(t *testing.T) {
	tests := []struct {
		name          string
		cores         int
		frequency     float64
		expectNonZero bool
	}{
		{
			name:          "baseline",
			cores:         4,
			frequency:     2.4,
			expectNonZero: true,
		},
		{
			name:          "high performance",
			cores:         8,
			frequency:     3.0,
			expectNonZero: true,
		},
		{
			name:          "low performance",
			cores:         2,
			frequency:     1.6,
			expectNonZero: true,
		},
		{
			name:          "single core",
			cores:         1,
			frequency:     2.0,
			expectNonZero: true,
		},
		{
			name:          "many cores",
			cores:         32,
			frequency:     2.4,
			expectNonZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			speed := normalizeCPUSpeed(tt.cores, tt.frequency)
			if tt.expectNonZero {
				assert.Greater(t, speed, 0.0, "speed should be positive")
			}
		})
	}

	// Check that baseline returns 1.0
	baselineSpeed := normalizeCPUSpeed(4, 2.4)
	assert.InDelta(t, 1.0, baselineSpeed, 0.01, "baseline should be 1.0")
}

func TestGetSystemMemory(t *testing.T) {
	mem := getSystemMemory()

	assert.Greater(t, mem, int64(0), "system memory should be positive")
	assert.LessOrEqual(t, mem, int64(1024*1024), "system memory should be reasonable (<= 1TB)")
}

func TestGetAvailableMemory(t *testing.T) {
	mem := getAvailableMemory()

	assert.Greater(t, mem, int64(0), "available memory should be positive")
	assert.LessOrEqual(t, mem, int64(1024*1024), "available memory should be reasonable (<= 1TB)")
}

func TestDetectDiskType(t *testing.T) {
	diskType := detectDiskType()

	assert.NotEmpty(t, diskType, "disk type should not be empty")
	validTypes := map[string]bool{
		"SSD":  true,
		"HDD":  true,
		"NVMe": true,
	}
	assert.True(t, validTypes[diskType], "disk type should be valid")
}

func TestEstimateDiskIO(t *testing.T) {
	tests := []struct {
		name      string
		diskType  string
		wantRange [2]float64 // [min, max]
	}{
		{
			name:      "NVMe",
			diskType:  "NVMe",
			wantRange: [2]float64{3000.0, 4000.0},
		},
		{
			name:      "SSD",
			diskType:  "SSD",
			wantRange: [2]float64{400.0, 600.0},
		},
		{
			name:      "HDD",
			diskType:  "HDD",
			wantRange: [2]float64{80.0, 120.0},
		},
		{
			name:      "unknown",
			diskType:  "UNKNOWN",
			wantRange: [2]float64{400.0, 600.0}, // defaults to SSD
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ioSpeed := estimateDiskIO(tt.diskType)
			assert.GreaterOrEqual(t, ioSpeed, tt.wantRange[0], "IO speed should be >= min")
			assert.LessOrEqual(t, ioSpeed, tt.wantRange[1], "IO speed should be <= max")
		})
	}
}

func TestEstimateDiskSeekTime(t *testing.T) {
	tests := []struct {
		name      string
		diskType  string
		wantRange [2]float64 // [min, max]
	}{
		{
			name:      "NVMe",
			diskType:  "NVMe",
			wantRange: [2]float64{0.005, 0.02},
		},
		{
			name:      "SSD",
			diskType:  "SSD",
			wantRange: [2]float64{0.05, 0.2},
		},
		{
			name:      "HDD",
			diskType:  "HDD",
			wantRange: [2]float64{4.0, 6.0},
		},
		{
			name:      "unknown",
			diskType:  "UNKNOWN",
			wantRange: [2]float64{0.05, 0.2}, // defaults to SSD
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seekTime := estimateDiskSeekTime(tt.diskType)
			assert.GreaterOrEqual(t, seekTime, tt.wantRange[0], "seek time should be >= min")
			assert.LessOrEqual(t, seekTime, tt.wantRange[1], "seek time should be <= max")
		})
	}
}

func TestCalculateCostFactors(t *testing.T) {
	profile := &HardwareProfile{
		CPUCores:         4,
		CPUFrequency:     2.4,
		CPUSpeed:         1.0,
		TotalMemory:      8192,
		AvailableMemory:  4096,
		MemorySpeed:      1.0,
		DiskType:         "SSD",
		DiskIO:           500.0,
		NetworkBandwidth: 1000.0,
		NetworkLatency:   1.0,
		IsCloudEnv:       false,
	}

	factors := profile.CalculateCostFactors()

	assert.NotNil(t, factors)
	assert.Greater(t, factors.IOFactor, 0.0, "IO factor should be positive")
	assert.Greater(t, factors.CPUFactor, 0.0, "CPU factor should be positive")
	assert.Greater(t, factors.MemoryFactor, 0.0, "memory factor should be positive")
	assert.Greater(t, factors.NetworkFactor, 0.0, "network factor should be positive")
}

func TestCalculateCostFactors_CloudEnvironment(t *testing.T) {
	localProfile := &HardwareProfile{
		CPUCores:         4,
		CPUFrequency:     2.4,
		DiskType:         "SSD",
		DiskIO:           500.0,
		NetworkBandwidth: 1000.0,
		IsCloudEnv:       false,
	}

	cloudProfile := &HardwareProfile{
		CPUCores:         4,
		CPUFrequency:     2.4,
		DiskType:         "SSD",
		DiskIO:           500.0,
		NetworkBandwidth: 1000.0,
		IsCloudEnv:       true,
	}

	localFactors := localProfile.CalculateCostFactors()
	cloudFactors := cloudProfile.CalculateCostFactors()

	// Cloud environment should have higher network factor
	assert.Greater(t, cloudFactors.NetworkFactor, localFactors.NetworkFactor,
		"cloud network factor should be higher")
}

func TestEstimateCacheHitRate(t *testing.T) {
	profile := &HardwareProfile{
		CacheHitRate: 0.8,
	}

	rate := profile.EstimateCacheHitRate()
	assert.Equal(t, 0.8, rate, "cache hit rate should match")
}

func TestHardwareProfile_String(t *testing.T) {
	profile := &HardwareProfile{
		CPUCores:        4,
		CPUFrequency:    2.4,
		AvailableMemory: 4096,
		DiskType:        "SSD",
		DiskIO:          500.0,
	}

	str := profile.String()
	assert.NotEmpty(t, str, "string representation should not be empty")
	assert.Contains(t, str, "CPU", "should contain CPU info")
	assert.Contains(t, str, "Memory", "should contain Memory info")
	assert.Contains(t, str, "Disk", "should contain Disk info")
}

func TestHardwareProfile_Explain(t *testing.T) {
	profile := DetectHardwareProfile()
	explanation := profile.Explain()

	assert.NotEmpty(t, explanation, "explanation should not be empty")
	assert.Contains(t, explanation, "=== Hardware Profile ===", "should contain header")
	assert.Contains(t, explanation, "CPU:", "should contain CPU section")
	assert.Contains(t, explanation, "Memory:", "should contain Memory section")
	assert.Contains(t, explanation, "Disk:", "should contain Disk section")
	assert.Contains(t, explanation, "Network:", "should contain Network section")
	assert.Contains(t, explanation, "Environment:", "should contain Environment section")
	assert.Contains(t, explanation, "Cost Factors:", "should contain Cost Factors section")
}

func TestUpdateCacheHitRate(t *testing.T) {
	profile := &HardwareProfile{
		CacheHitRate: 0.8,
	}

	// Update cache hit rate
	profile.UpdateCacheHitRate(0.9)
	assert.Equal(t, 0.9, profile.CacheHitRate, "cache hit rate should be updated")

	// Test edge cases
	tests := []struct {
		name string
		rate float64
	}{
		{"zero rate", 0.0},
		{"full rate", 1.0},
		{"high rate", 0.95},
		{"low rate", 0.05},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile.UpdateCacheHitRate(tt.rate)
			assert.Equal(t, tt.rate, profile.CacheHitRate)
		})
	}
}

func TestHardwareProfile_CompleteProfile(t *testing.T) {
	profile := DetectHardwareProfile()

	// Test that all fields are properly initialized
	assert.NotNil(t, profile, "profile should not be nil")
	assert.Greater(t, profile.CPUSpeed, 0.0, "CPU speed should be positive")
	assert.GreaterOrEqual(t, profile.MemorySpeed, 0.0, "memory speed should be non-negative")
	assert.False(t, profile.MeasuredAt.IsZero(), "measurement should be set")
}

// Benchmark tests
func BenchmarkDetectHardwareProfile(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DetectHardwareProfile()
	}
}

func BenchmarkCalculateCostFactors(b *testing.B) {
	profile := DetectHardwareProfile()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		profile.CalculateCostFactors()
	}
}

func BenchmarkEstimateDiskIO(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		estimateDiskIO("SSD")
	}
}
