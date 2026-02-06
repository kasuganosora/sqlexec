package optimizer

// Hints support methods for LogicalAggregate

// Algorithm 返回聚合算法
func (p *LogicalAggregate) Algorithm() AggregationAlgorithm {
	if p.algorithm == 0 {
		return HashAggAlgorithm // 默认使用 hash agg
	}
	return p.algorithm
}

// SetAlgorithm 设置聚合算法
func (p *LogicalAggregate) SetAlgorithm(algo AggregationAlgorithm) {
	p.algorithm = algo
}

// SetHintApplied 标记已应用的 hint
func (p *LogicalAggregate) SetHintApplied(hint string) {
	if p.appliedHints == nil {
		p.appliedHints = []string{}
	}
	p.appliedHints = append(p.appliedHints, hint)
}

// GetAppliedHints 获取已应用的 hints
func (p *LogicalAggregate) GetAppliedHints() []string {
	return p.appliedHints
}
