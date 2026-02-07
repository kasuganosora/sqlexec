package optimizer

import "github.com/kasuganosora/sqlexec/pkg/optimizer/genetic"

// GeneticAlgorithm 遗传算法，用于搜索最优索引组合
// Deprecated: 已迁移到 pkg/optimizer/genetic 包，此类型仅为兼容性保留
type GeneticAlgorithm = genetic.GeneticAlgorithm

// GeneticAlgorithmConfig 遗传算法配置
// Deprecated: 已迁移到 pkg/optimizer/genetic 包，此类型仅为兼容性保留
type GeneticAlgorithmConfig = genetic.GeneticAlgorithmConfig

// DefaultGeneticAlgorithmConfig 返回默认配置
// Deprecated: 已迁移到 pkg/optimizer/genetic 包，此函数仅为兼容性保留
func DefaultGeneticAlgorithmConfig() *genetic.GeneticAlgorithmConfig {
	return genetic.DefaultGeneticAlgorithmConfig()
}

// NewGeneticAlgorithm 创建遗传算法实例
// Deprecated: 已迁移到 pkg/optimizer/genetic 包，此函数仅为兼容性保留
func NewGeneticAlgorithm(config *genetic.GeneticAlgorithmConfig) *genetic.GeneticAlgorithm {
	return genetic.NewGeneticAlgorithm(config)
}

// ConvertGeneticCandidates 转换遗传算法候选索引类型
func ConvertGeneticCandidates(candidates []*IndexCandidate) []*genetic.IndexCandidate {
	result := make([]*genetic.IndexCandidate, len(candidates))
	for i, c := range candidates {
		result[i] = &genetic.IndexCandidate{
			TableName: c.TableName,
			Columns:   c.Columns,
			Priority:  c.Priority,
		}
	}
	return result
}

// ConvertGeneticResults 转换遗传算法结果类型
func ConvertGeneticResults(results []*genetic.IndexCandidate) []*IndexCandidate {
	result := make([]*IndexCandidate, len(results))
	for i, r := range results {
		result[i] = &IndexCandidate{
			TableName: r.TableName,
			Columns:   r.Columns,
			Priority:  r.Priority,
		}
	}
	return result
}
