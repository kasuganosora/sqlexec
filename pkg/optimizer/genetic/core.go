package genetic

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// GeneticAlgorithm 遗传算法，用于搜索最优索引组合
type GeneticAlgorithm struct {
	config *GeneticAlgorithmConfig

	// 候选索引
	candidates []*IndexCandidate
	// 索引收益映射
	benefits map[string]float64

	// 算子
	selector  SelectionOperator
	crossover CrossoverOperator
	mutator   MutationOperator

	// 随机数生成器
	rng *rand.Rand
	mu  sync.Mutex

	// 收敛状态跟踪
	convergenceHistory []float64 // 历史收敛指标
	bestFitnessHistory []float64 // 历史最优适应度
	avgFitnessHistory  []float64 // 历史平均适应度
	windowSize         int       // 滑动窗口大小
}

// NewGeneticAlgorithm 创建遗传算法实例
func NewGeneticAlgorithm(config *GeneticAlgorithmConfig) *GeneticAlgorithm {
	if config == nil {
		config = DefaultGeneticAlgorithmConfig()
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &GeneticAlgorithm{
		config:             config,
		rng:                rng,
		selector:           NewDefaultSelectionOperator(config.PopulationSize, config.SelectionStrategy, config.TournamentSize, rng),
		crossover:          NewDefaultCrossoverOperator(config.CrossoverRate, rng),
		mutator:            NewDefaultMutationOperator(config.MutationRate, rng),
		windowSize:         5, // 默认滑动窗口大小为 5
		convergenceHistory: make([]float64, 0),
		bestFitnessHistory: make([]float64, 0),
		avgFitnessHistory:  make([]float64, 0),
	}
}

// InitializePopulation 初始化种群
func (ga *GeneticAlgorithm) InitializePopulation(candidates []*IndexCandidate, benefits map[string]float64) *Population {
	ga.mu.Lock()
	defer ga.mu.Unlock()

	ga.candidates = candidates
	ga.benefits = benefits

	pop := &Population{
		Individuals: make([]*Individual, 0, ga.config.PopulationSize),
	}

	// 生成初始个体
	for i := 0; i < ga.config.PopulationSize; i++ {
		individual := ga.createIndividual()
		pop.Individuals = append(pop.Individuals, individual)
	}

	// 计算适应度
	for _, ind := range pop.Individuals {
		ind.Fitness = ga.calculateFitness(ind)
	}

	return pop
}

// createIndividual 创建一个随机个体
func (ga *GeneticAlgorithm) createIndividual() *Individual {
	numCandidates := len(ga.candidates)
	individual := &Individual{
		Genes: make([]bool, numCandidates),
	}

	// 随机选择约 30% 的索引
	for i := 0; i < numCandidates; i++ {
		if ga.rng.Float64() < 0.3 {
			individual.Genes[i] = true
		}
	}

	return individual
}

// generateCandidateKey 生成候选索引的key
func generateCandidateKey(candidate *IndexCandidate) string {
	// 简单实现，可以根据需要调整
	return fmt.Sprintf("%s(%s)", candidate.TableName, joinStrings(candidate.Columns, ","))
}

// joinStrings 连接字符串数组
func joinStrings(arr []string, sep string) string {
	if len(arr) == 0 {
		return ""
	}
	result := arr[0]
	for i := 1; i < len(arr); i++ {
		result += sep + arr[i]
	}
	return result
}

// calculateFitness 计算个体的适应度
// 适应度 = 总收益 - 惩罚项
func (ga *GeneticAlgorithm) calculateFitness(individual *Individual) float64 {
	totalBenefit := 0.0

	// 计算总收益
	for i, gene := range individual.Genes {
		if gene && i < len(ga.candidates) {
			candidate := ga.candidates[i]
			key := generateCandidateKey(candidate)

			// 查找对应的收益
			benefit := 0.0
			if val, ok := ga.benefits[key]; ok {
				benefit = val
			} else {
				// 如果没有明确的收益映射，基于优先级估算
				benefit = float64(candidate.Priority) * 0.01
			}

			totalBenefit += benefit
		}
	}

	// 惩罚项
	penalty := 0.0

	// 检查约束
	if !ga.checkConstraints(individual) {
		penalty += 1000.0 // 大惩罚
	}

	return totalBenefit - penalty
}

// checkConstraints 检查约束条件
func (ga *GeneticAlgorithm) checkConstraints(individual *Individual) bool {
	selectedCount := 0

	// 检查索引数量
	for _, gene := range individual.Genes {
		if gene {
			selectedCount++
		}
	}

	if selectedCount > ga.config.MaxIndexes {
		return false
	}

	// 检查列数
	for i, gene := range individual.Genes {
		if gene && i < len(ga.candidates) {
			if len(ga.candidates[i].Columns) > ga.config.MaxColumns {
				return false
			}
		}
	}

	return true
}

// IsConverged 检查种群是否收敛
func (ga *GeneticAlgorithm) IsConverged(pop *Population, threshold float64) bool {
	if len(pop.Individuals) < 2 {
		return true
	}

	// 计算平均适应度
	totalFitness := 0.0
	for _, ind := range pop.Individuals {
		totalFitness += ind.Fitness
	}
	avgFitness := totalFitness / float64(len(pop.Individuals))

	// 计算最大适应度
	maxFitness := pop.Individuals[0].Fitness
	for _, ind := range pop.Individuals {
		if ind.Fitness > maxFitness {
			maxFitness = ind.Fitness
		}
	}

	// 如果最大和平均的差异小于或等于阈值，认为收敛
	if maxFitness > 0 {
		diff := (maxFitness - avgFitness) / maxFitness
		return diff <= threshold
	}

	return true
}

// CalculateConvergenceMetrics 计算收敛指标（使用滑动窗口）
func (ga *GeneticAlgorithm) CalculateConvergenceMetrics(pop *Population) (float64, float64, float64) {
	if len(pop.Individuals) == 0 {
		return 0, 0, 0
	}

	// 计算当前代的平均适应度和最大适应度
	totalFitness := 0.0
	maxFitness := 0.0
	for _, ind := range pop.Individuals {
		totalFitness += ind.Fitness
		if ind.Fitness > maxFitness {
			maxFitness = ind.Fitness
		}
	}
	avgFitness := totalFitness / float64(len(pop.Individuals))

	// 更新历史记录
	ga.bestFitnessHistory = append(ga.bestFitnessHistory, maxFitness)
	ga.avgFitnessHistory = append(ga.avgFitnessHistory, avgFitness)

	// 计算滑动窗口内的平均适应度变化率
	windowSize := ga.windowSize
	if len(ga.avgFitnessHistory) < windowSize {
		windowSize = len(ga.avgFitnessHistory)
	}

	var avgChangeRate float64
	if windowSize >= 2 {
		changes := 0.0
		for i := windowSize - 1; i > 0; i-- {
			curr := ga.avgFitnessHistory[len(ga.avgFitnessHistory)-i]
			prev := ga.avgFitnessHistory[len(ga.avgFitnessHistory)-i-1]
			if prev > 0 {
				changes += (curr - prev) / prev
			}
		}
		avgChangeRate = changes / float64(windowSize-1)
	}

	// 计算当前收敛指标：最大适应度与平均适应度的差异
	var convergenceMetric float64
	if maxFitness > 0 {
		convergenceMetric = (maxFitness - avgFitness) / maxFitness
	}

	ga.convergenceHistory = append(ga.convergenceHistory, convergenceMetric)

	return convergenceMetric, avgChangeRate, maxFitness
}

// AdaptParameters 根据收敛状态自适应调整参数
func (ga *GeneticAlgorithm) AdaptParameters(generation int, convergenceMetric, changeRate float64) {
	if !ga.config.AdaptiveEnabled {
		return
	}

	// 如果适应度变化率很小，说明种群可能陷入局部最优
	if ga.windowSize < 5 {
		return
	}

	// 根据收敛指标和变化率调整参数
	// 策略1: 当收敛指标很低（种群已经收敛）且变化率也很小时，增加变异率以引入新的多样性
	if convergenceMetric < 0.01 && changeRate < 0.001 {
		ga.config.MutationRate = 0.2  // 提高变异率
		ga.config.CrossoverRate = 0.7 // 降低交叉率，增加探索
	} else if convergenceMetric > 0.1 {
		// 策略2: 当种群多样性很大时，提高交叉率，降低变异率以加速收敛
		ga.config.MutationRate = 0.05
		ga.config.CrossoverRate = 0.9
	} else {
		// 策略3: 中等状态，使用默认参数
		ga.config.MutationRate = 0.1
		ga.config.CrossoverRate = 0.8
	}
}

// GetBestIndividual 获取最优个体
func (ga *GeneticAlgorithm) GetBestIndividual(pop *Population) *Individual {
	if len(pop.Individuals) == 0 {
		return nil
	}

	best := pop.Individuals[0]
	for _, ind := range pop.Individuals {
		if ind.Fitness > best.Fitness {
			best = ind
		}
	}

	return best
}

// ExtractSolution 从最优个体提取索引集合
func (ga *GeneticAlgorithm) ExtractSolution(individual *Individual) []*IndexCandidate {
	if individual == nil {
		return nil
	}

	var selected []*IndexCandidate
	for i, gene := range individual.Genes {
		if gene && i < len(ga.candidates) {
			selected = append(selected, ga.candidates[i])
		}
	}

	return selected
}

// GetConvergenceHistory 返回收敛历史记录
func (ga *GeneticAlgorithm) GetConvergenceHistory() []float64 {
	return ga.convergenceHistory
}

// GetBestFitnessHistory 返回最优适应度历史记录
func (ga *GeneticAlgorithm) GetBestFitnessHistory() []float64 {
	return ga.bestFitnessHistory
}

// GetAvgFitnessHistory 返回平均适应度历史记录
func (ga *GeneticAlgorithm) GetAvgFitnessHistory() []float64 {
	return ga.avgFitnessHistory
}

// Run 运行遗传算法
func (ga *GeneticAlgorithm) Run(ctx context.Context, candidates []*IndexCandidate, benefits map[string]float64) []*IndexCandidate {
	// 初始化种群
	pop := ga.InitializePopulation(candidates, benefits)

	// 进化循环
	eliteCount := 2              // 保留前2个精英
	convergenceThreshold := 0.01 // 1% 差异认为收敛

	for generation := 0; generation < ga.config.MaxGenerations; generation++ {
		// 检查上下文取消
		select {
		case <-ctx.Done():
			debugf("[GeneticAlgorithm] Context cancelled at generation %d\n", generation)
			best := ga.GetBestIndividual(pop)
			return ga.ExtractSolution(best)
		default:
		}

		// 计算收敛指标和变化率
		convergenceMetric, changeRate, _ := ga.CalculateConvergenceMetrics(pop)

		// 自适应参数调整
		if generation >= 5 {
			ga.AdaptParameters(generation, convergenceMetric, changeRate)
		}

		// 选择
		pop = ga.selector.Select(pop, eliteCount)

		// 交叉和变异
		newIndividuals := make([]*Individual, 0, ga.config.PopulationSize)

		// 保留精英
		for i := 0; i < eliteCount && i < len(pop.Individuals); i++ {
			newIndividuals = append(newIndividuals, pop.Individuals[i])
		}

		// 生成后代
		for i := eliteCount; i < ga.config.PopulationSize; i += 2 {
			// 随机选择父母
			parent1 := pop.Individuals[ga.rng.Intn(len(pop.Individuals))]
			parent2 := pop.Individuals[ga.rng.Intn(len(pop.Individuals))]

			// 交叉
			child1, child2 := ga.crossover.Crossover(parent1, parent2)

			// 变异
			child1 = ga.mutator.Mutate(child1)
			child2 = ga.mutator.Mutate(child2)

			// 计算适应度
			child1.Fitness = ga.calculateFitness(child1)
			child2.Fitness = ga.calculateFitness(child2)

			newIndividuals = append(newIndividuals, child1, child2)
		}

		// 更新种群
		if len(newIndividuals) < ga.config.PopulationSize {
			pop.Individuals = newIndividuals
		} else {
			pop.Individuals = newIndividuals[:ga.config.PopulationSize]
		}

		// 检查收敛（使用改进的收敛判断）
		if generation > 10 && ga.IsConverged(pop, convergenceThreshold) {
			debugf("[GeneticAlgorithm] Converged at generation %d (metric: %.4f, change rate: %.6f)\n",
				generation, convergenceMetric, changeRate)
			break
		}

		if generation%10 == 0 {
			best := ga.GetBestIndividual(pop)
			debugf("[GeneticAlgorithm] Generation %d, Best fitness: %.4f, Mutation: %.3f, Crossover: %.3f\n",
				generation, best.Fitness, ga.config.MutationRate, ga.config.CrossoverRate)
		}
	}

	// 返回最优解
	best := ga.GetBestIndividual(pop)
	return ga.ExtractSolution(best)
}
