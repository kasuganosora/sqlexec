package genetic

// Individual 个体（候选解）
type Individual struct {
	Genes   []bool  // 每个基因对应一个候选索引，true=选中
	Fitness float64 // 适应度
}

// Clone 克隆个体
func (ind *Individual) Clone() *Individual {
	cloned := &Individual{
		Genes:   make([]bool, len(ind.Genes)),
		Fitness: ind.Fitness,
	}
	copy(cloned.Genes, ind.Genes)
	return cloned
}

// Population 种群
type Population struct {
	Individuals []*Individual
}

// Size 返回种群大小
func (p *Population) Size() int {
	return len(p.Individuals)
}

// GetBest 获取最优个体
func (p *Population) GetBest() *Individual {
	if len(p.Individuals) == 0 {
		return nil
	}
	best := p.Individuals[0]
	for _, ind := range p.Individuals {
		if ind.Fitness > best.Fitness {
			best = ind
		}
	}
	return best
}

// GetAverageFitness 获取平均适应度
func (p *Population) GetAverageFitness() float64 {
	if len(p.Individuals) == 0 {
		return 0
	}
	total := 0.0
	for _, ind := range p.Individuals {
		total += ind.Fitness
	}
	return total / float64(len(p.Individuals))
}

// IndexCandidate 索引候选
type IndexCandidate struct {
	TableName string
	Columns   []string
	Priority  int
}

// GetConfig 获取配置（测试用途）
func (ga *GeneticAlgorithm) GetConfig() *GeneticAlgorithmConfig {
	return ga.config
}

// GetCandidates 获取候选索引（测试用途）
func (ga *GeneticAlgorithm) GetCandidates() []*IndexCandidate {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	return ga.candidates
}

// SetCandidatesForTest 设置候选索引（测试用途）
func (ga *GeneticAlgorithm) SetCandidatesForTest(candidates []*IndexCandidate, benefits map[string]float64) {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	ga.candidates = candidates
	ga.benefits = benefits
}
