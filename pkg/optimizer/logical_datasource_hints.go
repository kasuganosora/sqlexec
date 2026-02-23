package optimizer

// Hints support methods for LogicalDataSource

// ForceUseIndex 强制使用指定索引（FORCE_INDEX）
func (p *LogicalDataSource) ForceUseIndex(indexName string) {
	p.forceUseIndex = indexName
	p.addAppliedHint("FORCE_INDEX")
}

// PreferIndex 优先使用指定索引（USE_INDEX）
func (p *LogicalDataSource) PreferIndex(indexName string) {
	p.preferIndex = indexName
	p.addAppliedHint("USE_INDEX")
}

// IgnoreIndex 忽略指定索引（IGNORE_INDEX）
func (p *LogicalDataSource) IgnoreIndex(indexName string) {
	if p.ignoreIndexes == nil {
		p.ignoreIndexes = []string{}
	}
	p.ignoreIndexes = append(p.ignoreIndexes, indexName)
	p.addAppliedHint("IGNORE_INDEX")
}

// SetOrderIndex 设置排序索引（ORDER_INDEX）
func (p *LogicalDataSource) SetOrderIndex(indexName string) {
	p.orderIndex = indexName
	p.addAppliedHint("ORDER_INDEX")
}

// IgnoreOrderIndex 忽略排序索引（NO_ORDER_INDEX）
func (p *LogicalDataSource) IgnoreOrderIndex(indexName string) {
	p.ignoreOrderIndex = indexName
	p.addAppliedHint("NO_ORDER_INDEX")
}

// SetHintApplied 标记已应用的 hint
func (p *LogicalDataSource) SetHintApplied(hint string) {
	p.addAppliedHint(hint)
}

// addAppliedHint 添加已应用的 hint
func (p *LogicalDataSource) addAppliedHint(hint string) {
	if p.appliedHints == nil {
		p.appliedHints = []string{}
	}
	p.appliedHints = append(p.appliedHints, hint)
}

// GetAppliedHints 获取已应用的 hints
func (p *LogicalDataSource) GetAppliedHints() []string {
	return p.appliedHints
}
