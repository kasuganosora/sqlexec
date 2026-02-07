package genetic

// GeneticAlgorithmConfig 遗传算法配置
type GeneticAlgorithmConfig struct {
	PopulationSize    int
	MaxGenerations    int
	MutationRate      float64
	CrossoverRate     float64
	MaxIndexes        int
	MaxTotalSize      int64
	MaxColumns        int
	AdaptiveEnabled   bool   // 是否启用自适应参数
	SelectionStrategy string // 选择策略: "roulette" or "tournament"
	TournamentSize    int    // 锦标赛选择的大小
}

// DefaultGeneticAlgorithmConfig 返回默认配置
func DefaultGeneticAlgorithmConfig() *GeneticAlgorithmConfig {
	return &GeneticAlgorithmConfig{
		PopulationSize:    50,
		MaxGenerations:    100,
		MutationRate:      0.1,
		CrossoverRate:     0.8,
		MaxIndexes:        5,
		MaxTotalSize:      100 * 1024 * 1024, // 100MB
		MaxColumns:        3,
		AdaptiveEnabled:   true,
		SelectionStrategy: "roulette",
		TournamentSize:    5,
	}
}
