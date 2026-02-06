package cost

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
)

// HardwareProfile 硬件配置文件
// 基于实际硬件动态调整成本因子
type HardwareProfile struct {
	// CPU相关
	CPUCores        int     // CPU核心数
	CPUFrequency   float64 // CPU频率（GHz）
	CPUSpeed       float64 // CPU速度（相对值，基准1.0）
	
	// 内存相关
	TotalMemory    int64   // 总内存（MB）
	AvailableMemory int64   // 可用内存（MB）
	MemorySpeed    float64 // 内存速度（相对值，基准1.0）
	
	// 磁盘相关
	DiskType       string   // 磁盘类型: "SSD", "HDD", "NVMe"
	DiskIO        float64  // 磁盘IO速度（MB/s）
	DiskSeekTime  float64  // 磁盘寻道时间（ms）
	
	// 网络相关
	NetworkBandwidth float64 // 网络带宽（Mbps）
	NetworkLatency  float64 // 网络延迟（ms）
	
	// 系统相关
	OS            string    // 操作系统
	Architecture   string    // 架构: "amd64", "arm64"
	RuntimeVersion string    // Go运行时版本
	
	// 测量信息
	MeasuredAt     time.Time // 测量时间
	CacheHitRate  float64   // 平均缓存命中率
	IsCloudEnv    bool      // 是否云环境
}

// DetectHardwareProfile 自动检测硬件配置
func DetectHardwareProfile() *HardwareProfile {
	profile := &HardwareProfile{
		MeasuredAt:    time.Now(),
		OS:            runtime.GOOS,
		Architecture:   runtime.GOARCH,
		RuntimeVersion: runtime.Version(),
		IsCloudEnv:    detectCloudEnvironment(),
	}
	
	// 检测CPU信息
	profile.CPUCores = runtime.NumCPU()
	profile.CPUFrequency = estimateCPUFrequency()
	profile.CPUSpeed = normalizeCPUSpeed(profile.CPUCores, profile.CPUFrequency)
	
	// 检测内存信息
	profile.TotalMemory = getSystemMemory()
	profile.AvailableMemory = getAvailableMemory()
	profile.MemorySpeed = 1.0 // 默认，可以通过sysfs或WMIC获取
	
	// 检测磁盘信息
	profile.DiskType = detectDiskType()
	profile.DiskIO = estimateDiskIO(profile.DiskType)
	profile.DiskSeekTime = estimateDiskSeekTime(profile.DiskType)
	
	// 默认网络配置
	profile.NetworkBandwidth = 1000.0 // 1 Gbps
	profile.NetworkLatency = 1.0     // 1ms
	
	// 默认缓存命中率
	profile.CacheHitRate = 0.8 // 80%命中率
	
	return profile
}

// detectCloudEnvironment 检测是否在云环境中运行
func detectCloudEnvironment() bool {
	// 简化检测：检查环境变量或特定文件路径
	// AWS, GCP, Azure等都有特定标识
	envVars := []string{"AWS_REGION", "GOOGLE_CLOUD_PROJECT", "AZURE_RESOURCE_GROUP"}
	for _, env := range envVars {
		if os.Getenv(env) != "" {
			return true
		}
	}
	return false
}

// estimateCPUFrequency 估算CPU频率（简化）
func estimateCPUFrequency() float64 {
	// Linux: 可以从 /proc/cpuinfo 读取
	// Windows: 可以通过 WMI 获取
	// 简化：使用典型值
	return 2.4 // 典型的2.4GHz
}

// normalizeCPUSpeed 标准化CPU速度
func normalizeCPUSpeed(cores int, frequency float64) float64 {
	// 基准: 4核 @ 2.4GHz = 1.0
	benchmarkCores := 4
	benchmarkFreq := 2.4
	
	capacity := float64(cores) * frequency
	benchmarkCapacity := float64(benchmarkCores) * benchmarkFreq
	
	return capacity / benchmarkCapacity
}

// getSystemMemory 获取系统总内存（MB）
func getSystemMemory() int64 {
	// Linux: 从 /proc/meminfo
	// Windows: 通过 GlobalMemoryStatusEx
	// macOS: 通过 sysctl hw.memsize
	// 简化：使用runtime限制
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// 返回系统内存（这里简化处理）
	// 实际应该调用系统API
	return 8192 // 默认8GB
}

// getAvailableMemory 获取可用内存（MB）
func getAvailableMemory() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// 简化：返回堆内存的估算值
	// 实际应该从系统获取
	return 4096 // 默认4GB可用
}

// detectDiskType 检测磁盘类型
func detectDiskType() string {
	// 简化：默认SSD
	// 实际应该检测挂载点或磁盘信息
	return "SSD"
}

// estimateDiskIO 估算磁盘IO速度（MB/s）
func estimateDiskIO(diskType string) float64 {
	switch diskType {
	case "NVMe":
		return 3500.0 // ~3.5 GB/s
	case "SSD":
		return 500.0  // ~500 MB/s
	case "HDD":
		return 100.0  // ~100 MB/s
	default:
		return 500.0  // 默认SSD
	}
}

// estimateDiskSeekTime 估算磁盘寻道时间（ms）
func estimateDiskSeekTime(diskType string) float64 {
	switch diskType {
	case "NVMe":
		return 0.01 // 几乎没有寻道
	case "SSD":
		return 0.1  // ~0.1ms
	case "HDD":
		return 5.0   // ~5ms
	default:
		return 0.1
	}
}

// AdaptiveCostFactor 自适应成本因子
// 基于硬件配置动态调整
type AdaptiveCostFactor struct {
	IOFactor      float64 // IO成本因子
	CPUFactor     float64 // CPU成本因子
	MemoryFactor  float64 // 内存成本因子
	NetworkFactor float64 // 网络成本因子
}

// CalculateCostFactors 计算自适应成本因子
func (hp *HardwareProfile) CalculateCostFactors() *AdaptiveCostFactor {
	factor := &AdaptiveCostFactor{}
	
	// IO因子：基于磁盘速度
	// 基准: SSD @ 500MB/s = 0.1
	baseDiskIO := 500.0
	factor.IOFactor = 0.1 * (baseDiskIO / hp.DiskIO)
	
	// CPU因子：基于核心数和频率
	// 基准: 4核 @ 2.4GHz = 0.01
	factor.CPUFactor = 0.01 / hp.CPUSpeed
	
	// 内存因子：基于内存速度
	// 基准: 1.0 = 0.001
	factor.MemoryFactor = 0.001 * hp.MemorySpeed
	
	// 网络因子：基于带宽
	// 基准: 1Gbps = 0.01
	baseBandwidth := 1000.0
	factor.NetworkFactor = 0.01 * (baseBandwidth / hp.NetworkBandwidth)
	
	// 云环境调整
	if hp.IsCloudEnv {
		// 云环境网络延迟通常更高
		factor.NetworkFactor *= 1.5
	}
	
	return factor
}

// EstimateCacheHitRate 估算缓存命中率（基于历史）
func (hp *HardwareProfile) EstimateCacheHitRate() float64 {
	// 简化：使用配置值
	// 实际应该基于查询历史统计
	return hp.CacheHitRate
}

// String 返回硬件配置的字符串表示
func (hp *HardwareProfile) String() string {
	return fmt.Sprintf(
		"CPU: %d cores @ %.2fGHz, Memory: %dMB, Disk: %s @ %.2fMB/s",
		hp.CPUCores, hp.CPUFrequency, hp.AvailableMemory, hp.DiskType, hp.DiskIO,
	)
}

// Explain 返回详细的硬件配置说明
func (hp *HardwareProfile) Explain() string {
	var explanation strings.Builder
	explanation.WriteString(fmt.Sprintf("=== Hardware Profile ===\n"))
	explanation.WriteString(fmt.Sprintf("CPU:         %d cores @ %.2fGHz (Speed: %.2fx)\n", 
		hp.CPUCores, hp.CPUFrequency, hp.CPUSpeed))
	explanation.WriteString(fmt.Sprintf("Memory:      %d / %d MB (Speed: %.2fx)\n",
		hp.AvailableMemory, hp.TotalMemory, hp.MemorySpeed))
	explanation.WriteString(fmt.Sprintf("Disk:        %s @ %.2f MB/s (Seek: %.2fms)\n",
		hp.DiskType, hp.DiskIO, hp.DiskSeekTime))
	explanation.WriteString(fmt.Sprintf("Network:     %.2f Mbps @ %.2fms latency (Factor: %.4fx)\n",
		hp.NetworkBandwidth, hp.NetworkLatency, 0.01*1000.0/hp.NetworkBandwidth))
	explanation.WriteString(fmt.Sprintf("Environment:  %s, OS: %s, Arch: %s\n",
		map[bool]string{true: "Cloud", false: "Local"}[hp.IsCloudEnv], hp.OS, hp.Architecture))
	explanation.WriteString(fmt.Sprintf("Cost Factors: IO=%.4f, CPU=%.4f, Mem=%.4f\n",
		hp.CalculateCostFactors().IOFactor,
		hp.CalculateCostFactors().CPUFactor,
		hp.CalculateCostFactors().MemoryFactor))
	
	return explanation.String()
}

// UpdateCacheHitRate 更新缓存命中率
func (hp *HardwareProfile) UpdateCacheHitRate(rate float64) {
	hp.CacheHitRate = rate
}
