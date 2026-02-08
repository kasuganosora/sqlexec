# Learning to Rank (LTR) 自动调优设计方案

## 1. 问题分析

### 当前痛点

当前的全文搜索系统使用 BM25 算法，参数 (k1, b) 需要人工调优：

```go
BM25Params: BM25Params{
    K1: 1.2,  // 词频饱和参数 (1.2-2.0) - 需要调优
    B:  0.75, // 长度归一化参数 (0-1) - 需要调优
}
```

**人工调优的问题**：
- 依赖领域专家经验
- 无法适应数据分布变化
- 无法优化用户满意度指标 (点击率、转化率等)
- 调优成本高，需要反复 A/B 测试

### Learning to Rank 的优势

✅ **自动学习**：从用户行为数据中学习最优排序
✅ **个性化**：支持不同场景/用户的个性化排序
✅ **可优化**：直接优化业务指标 (CTR, CVR, 用户满意度)
✅ **可扩展**：支持多特征融合 (文档质量、时效性、用户偏好等)

---

## 2. 设计目标

### 核心目标

1. **自动优化 BM25 参数**：k1, b
2. **融合多维度特征**：文档质量、时效性、点击率等
3. **支持在线学习**：实时适应用户行为变化
4. **可解释性**：提供特征重要性分析
5. **低延迟**：不影响查询性能

### 非功能性目标

- **训练效率**：支持百万级文档训练
- **查询延迟**：增加 < 5ms
- **内存占用**：模型 < 100MB
- **可扩展性**：支持新增特征和排序算法

---

## 3. 架构设计

### 3.1 整体架构

```
用户查询
    ↓
特征提取层 (Feature Extractor)
    ↓
┌─────────────────────────────────────┐
│  排序模型层 (Ranking Model)         │
│  - LambdaMART (Gradient Boosting)   │
│  - Linear Model (简单线性)          │
│  - Neural Network (深度模型)        │
└─────────────────────────────────────┘
    ↓
排序结果
    ↓
用户反馈 (点击/转化) ←─── 数据收集 ───→ 训练流水线
```

### 3.2 模块划分

```
pkg/fulltext/ltr/
├── model/                    # 排序模型
│   ├── interface.go         # 模型接口
│   ├── lambda_mart.go       # LambdaMART 实现
│   ├── linear.go            # 线性模型
│   └── neural_net.go        # 神经网络模型
├── feature/                 # 特征工程
│   ├── extractor.go         # 特征提取器
│   ├── transformer.go       # 特征转换
│   └── store.go            # 特征存储
├── data/                    # 数据处理
│   ├── sample.go           # 采样
│   ├── label.go            # 标签生成
│   └── dataset.go          # 数据集
├── training/                # 训练
│   ├── trainer.go          # 训练器
│   ├── optimizer.go        # 优化器
│   └── evaluator.go        # 评估器
└── service/                 # 服务层
    ├── ranker.go           # 在线排序服务
    └── updater.go          # 模型更新
```

---

## 4. 特征工程

### 4.1 特征分类

#### 文本相关性特征 (BM25 相关)

| 特征名 | 描述 | 类型 |
|--------|------|------|
| `bm25_score` | BM25 基础分数 | float |
| `tf_score` | 词频分数 | float |
| `idf_score` | 逆文档频率分数 | float |
| `doc_length` | 文档长度 | int |
| `avg_doc_length` | 平均文档长度 | float |
| `term_overlap` | 查询-文档词重叠率 | float |

#### 文档质量特征

| 特征名 | 描述 | 类型 |
|--------|------|------|
| `doc_age_days` | 文档年龄 (天) | int |
| `doc_quality_score` | 文档质量评分 (0-1) | float |
| `click_through_rate` | 历史点击率 | float |
| `conversion_rate` | 历史转化率 | float |
| `popularity_score` | 流行度分数 | float |

#### 用户行为特征

| 特征名 | 描述 | 类型 |
|--------|------|------|
| `user_click_history` | 用户点击历史 | float |
| `user_dwell_time` | 用户停留时间 | float |
| `query_category_match` | 查询-类别匹配度 | float |
| `session_depth` | 当前会话深度 | int |

#### 查询特征

| 特征名 | 描述 | 类型 |
|--------|------|------|
| `query_length` | 查询长度 | int |
| `query_term_count` | 查询词数量 | int |
| `query_has_numeric` | 是否包含数字 | bool |
| `query_is_question` | 是否是问句 | bool |

### 4.2 特征提取流程

```go
type FeatureExtractor struct {
    // BM25 相关
    bm25Scorer *bm25.Scorer
    
    // 统计相关
    stats *bm25.CollectionStats
    
    // 用户行为
    userBehaviorStore *UserBehaviorStore
}

func (fe *FeatureExtractor) Extract(
    query string,
    doc *index.Document,
    userID string,
) FeatureVector {
    vector := make(FeatureVector)
    
    // 1. 文本特征
    vector["bm25_score"] = fe.calculateBM25(query, doc)
    vector["tf_score"] = fe.calculateTF(query, doc)
    vector["idf_score"] = fe.calculateIDF(query, doc)
    
    // 2. 文档质量特征
    vector["doc_age_days"] = fe.calculateDocAge(doc)
    vector["click_through_rate"] = fe.getCTR(doc.ID)
    vector["doc_quality_score"] = fe.getDocQuality(doc)
    
    // 3. 用户行为特征
    if userID != "" {
        vector["user_click_history"] = fe.getUserClickHistory(userID, doc.ID)
        vector["user_category_preference"] = fe.getUserCategoryPref(userID, doc)
    }
    
    // 4. 查询特征
    vector["query_length"] = len(query)
    vector["query_term_count"] = len(strings.Fields(query))
    
    return vector
}
```

### 4.3 特征转换

```go
type FeatureTransformer struct {
    // 归一化器
    normalizers map[string]*Normalizer
    
    // 分桶 (Bucketing)
    buckets map[string][]float64
}

// 转换示例
func (ft *FeatureTransformer) Transform(vector FeatureVector) FeatureVector {
    transformed := make(FeatureVector)
    
    for name, value := range vector {
        // 数值归一化
        if normalizer, exists := ft.normalizers[name]; exists {
            transformed[name] = normalizer.Normalize(value)
        }
        
        // 分桶 (如: 日期分桶)
        if buckets, exists := ft.buckets[name]; exists {
            bucket := findBucket(value.(float64), buckets)
            transformed[name+"_bucket"] = float64(bucket)
        }
    }
    
    return transformed
}
```

---

## 5. 排序算法

### 5.1 LambdaMART (推荐)

**原理**：基于 LambdaRank 的 Gradient Boosting Decision Trees

**优势**：
- 业界标准 (Yahoo, Bing 使用)
- 直接优化 NDCG 指标
- 支持多特征
- 可解释性强

**实现**：

```go
type LambdaMART struct {
    // 模型参数
    numTrees      int
    learningRate  float64
    maxDepth      int
    minSamplesSplit int
    
    // 树集合
    trees []*RegressionTree
    
    // 特征重要性
    featureImportance map[string]float64
}

// 预测分数
func (l *LambdaMART) Predict(features FeatureVector) float64 {
    score := 0.0
    for _, tree := range l.trees {
        score += l.learningRate * tree.Predict(features)
    }
    return score
}

// 训练
func (l *LambdaMART) Train(dataset *Dataset) error {
    for iter := 0; iter < l.numTrees; iter++ {
        // 1. 计算梯度 (Lambda)
        lambdas := l.computeLambdas(dataset)
        
        // 2. 拟合回归树
        tree := NewRegressionTree(l.maxDepth, l.minSamplesSplit)
        tree.Fit(dataset, lambdas)
        
        // 3. 更新模型
        l.trees = append(l.trees, tree)
        
        // 4. 更新样本分数
        l.updateScores(dataset, tree)
    }
    return nil
}

// 计算 Lambda (梯度)
func (l *LambdaMART) computeLambdas(dataset *Dataset) []float64 {
    lambdas := make([]float64, len(dataset.Samples))
    
    // 按 query_id 分组
    groups := dataset.GroupByQuery()
    
    for _, group := range groups {
        // 计算每对的 lambda
        for i := 0; i < len(group)-1; i++ {
            for j := i + 1; j < len(group); j++ {
                s1 := group[i]
                s2 := group[j]
                
                // 计算 NDCG 变化
                deltaNDCG := l.computeDeltaNDCG(s1, s2)
                
                // 计算 lambda
                lambda := l.computePairLambda(s1, s2, deltaNDCG)
                
                lambdas[s1.ID] += lambda
                lambdas[s2.ID] -= lambda
            }
        }
    }
    
    return lambdas
}
```

### 5.2 线性模型 (备选)

**适用场景**：特征少，需要低延迟

```go
type LinearModel struct {
    weights map[string]float64  // 特征权重
    bias    float64              // 偏置
}

func (l *LinearModel) Predict(features FeatureVector) float64 {
    score := l.bias
    for name, weight := range l.weights {
        if value, exists := features[name]; exists {
            score += weight * value.(float64)
        }
    }
    return score
}
```

### 5.3 神经网络模型 (高级)

**适用场景**：大规模数据，复杂模式

```go
type NeuralNet struct {
    layers []*DenseLayer
    dropout float64
}

type DenseLayer struct {
    weights *mat.Dense
    bias    *mat.VecDense
    activation ActivationFunc
}
```

---

## 6. 数据收集与标签生成

### 6.1 用户行为收集

```go
type UserBehavior struct {
    QueryID     string    // 查询ID
    UserID      string    // 用户ID
    DocID       int64     // 文档ID
    Action      string    // click, dwell, convert
    Timestamp   time.Time // 时间戳
    Position    int       // 点击位置
    DwellTime   int64     // 停留时间 (ms)
}

type BehaviorCollector struct {
    storage *BehaviorStorage
}

// 收集点击行为
func (bc *BehaviorCollector) CollectClick(behavior UserBehavior) error {
    // 1. 存储行为
    err := bc.storage.Save(behavior)
    
    // 2. 实时更新 CTR
    bc.updateCTR(behavior.QueryID, behavior.DocID)
    
    // 3. 触发在线学习 (可选)
    if bc.shouldTriggerOnlineLearning() {
        bc.triggerOnlineLearning()
    }
    
    return err
}
```

### 6.2 标签生成

**问题**：用户行为是隐式反馈，需要转换为排序标签

**方法**：

```go
type LabelGenerator struct {
    // 点击阈值 (ms)
    dwellTimeThreshold int64
    
    // 权重配置
    clickWeight    float64
    dwellWeight    float64
    convertWeight  float64
}

// 生成排序标签 (1-5)
func (lg *LabelGenerator) GenerateLabel(behaviors []UserBehavior) int {
    score := 0.0
    
    for _, b := range behaviors {
        switch b.Action {
        case "click":
            score += lg.clickWeight
            
            // 长停留时间 = 高质量
            if b.DwellTime > lg.dwellTimeThreshold {
                score += lg.dwellWeight
            }
            
        case "convert":  // 转化 (购买/注册)
            score += lg.convertWeight
        }
    }
    
    // 映射到 1-5 标签
    return lg.mapToLabel(score)
}

// 生成训练样本
func (lg *LabelGenerator) GenerateSamples(
    query string,
    docIDs []int64,
    behaviors []UserBehavior,
) []TrainingSample {
    
    samples := make([]TrainingSample, len(docIDs))
    
    for i, docID := range docIDs {
        // 获取文档行为
        docBehaviors := lg.filterByDocID(behaviors, docID)
        
        // 生成标签
        label := lg.GenerateLabel(docBehaviors)
        
        samples[i] = TrainingSample{
            Query:    query,
            DocID:    docID,
            Label:    label,  // 1-5
            QueryID:  generateQueryID(query),
        }
    }
    
    return samples
}
```

**标签定义**：
- 5: 完美匹配 (用户转化)
- 4: 高度相关 (长停留时间 + 点击)
- 3: 相关 (点击)
- 2: 弱相关 (浏览未点击)
- 1: 不相关 (快速跳过)

---

## 7. 训练流水线

### 7.1 批处理训练 (离线)

```go
type TrainingPipeline struct {
    extractor   *FeatureExtractor
    transformer *FeatureTransformer
    trainer     *ModelTrainer
    evaluator   *ModelEvaluator
    
    // 数据存储
    sampleStore *SampleStore
    modelStore  *ModelStore
}

// 执行完整训练流程
func (tp *TrainingPipeline) Run(ctx context.Context) error {
    // 1. 数据收集 (每天凌晨)
    samples, err := tp.sampleStore.GetRecentSamples(7 * 24 * time.Hour) // 最近7天
    if err != nil {
        return err
    }
    
    // 2. 特征提取
    fmt.Printf("Extracting features for %d samples...\n", len(samples))
    dataset, err := tp.extractFeatures(samples)
    if err != nil {
        return err
    }
    
    // 3. 特征转换 (归一化、分桶)
    fmt.Println("Transforming features...")
    dataset = tp.transformer.TransformDataset(dataset)
    
    // 4. 划分训练/验证集
    trainSet, valSet := dataset.Split(0.8) // 80% 训练
    
    // 5. 训练模型
    fmt.Println("Training LambdaMART model...")
    model := ltrmodel.NewLambdaMART(&ltrmodel.Config{
        NumTrees:       100,
        LearningRate:   0.1,
        MaxDepth:       6,
        MinSamplesSplit: 10,
    })
    
    if err := model.Train(trainSet); err != nil {
        return err
    }
    
    // 6. 评估模型
    fmt.Println("Evaluating model...")
    metrics := tp.evaluator.Evaluate(model, valSet)
    fmt.Printf("Validation Metrics: NDCG@10=%.4f, MAP=%.4f\n",
        metrics.NDCG10, metrics.MAP)
    
    // 7. 判断模型是否可上线
    if metrics.NDCG10 > 0.75 && metrics.MAP > 0.6 {
        // 8. 保存模型
        if err := tp.modelStore.Save(model, metrics); err != nil {
            return err
        }
        
        // 9. 触发模型热更新
        tp.triggerModelUpdate()
        
        fmt.Println("Model deployed successfully!")
    } else {
        fmt.Printf("Model quality insufficient: NDCG@10=%.4f (< 0.75)\n",
            metrics.NDCG10)
    }
    
    return nil
}
```

### 7.2 在线学习 (增量)

```go
type OnlineLearner struct {
    model       ltrmodel.Model
    buffer      *SampleBuffer  // 缓存最近样本
    updateFreq  int            // 每 N 个样本更新一次
    
    // 锁
    mu sync.RWMutex
}

// 接收在线样本
func (ol *OnlineLearner) AddSample(sample TrainingSample) {
    ol.mu.Lock()
    defer ol.mu.Unlock()
    
    // 添加到缓存
    ol.buffer.Add(sample)
    
    // 达到阈值时更新
    if ol.buffer.Size() >= ol.updateFreq {
        ol.updateModel()
        ol.buffer.Clear()
    }
}

// 增量更新模型
func (ol *OnlineLearner) updateModel() {
    samples := ol.buffer.GetAll()
    dataset := ol.convertToDataset(samples)
    
    // 增量训练 (1-2棵树)
    ol.model.IncrementalTrain(dataset, numTrees=2)
}
```

---

## 8. 评估指标

### 8.1 排序评估指标

```go
type RankingMetrics struct {
    NDCG10   float64  // Normalized Discounted Cumulative Gain @10
    NDCG50   float64  // NDCG @50
    MAP      float64  // Mean Average Precision
    MRR      float64  // Mean Reciprocal Rank
    Precision10 float64  // Precision @10
    Recall10    float64  // Recall @10
}

type MetricsCalculator struct{}

// 计算 NDCG
func (mc *MetricsCalculator) NDCG(
    predicted []SearchResult,
    actual []TrainingSample,
    k int,
) float64 {
    
    // 构建相关性映射
    relevance := make(map[int64]int)
    for _, sample := range actual {
        relevance[sample.DocID] = sample.Label
    }
    
    // 计算 DCG
    dcg := 0.0
    for i, result := range predicted[:min(k, len(predicted))] {
        rel := relevance[result.DocID]
        dcg += (math.Pow(2, float64(rel)) - 1) / math.Log2(float64(i+2))
    }
    
    // 计算 IDCG (理想 DCG)
    idcg := mc.computeIDCG(actual, k)
    
    if idcg == 0 {
        return 0
    }
    
    return dcg / idcg
}
```

### 8.2 A/B 测试框架

```go
type ABTestFramework struct {
    // 流量分配
    controlTraffic   float64  // 对照组 (原模型)
    treatmentTraffic float64  // 实验组 (新模型)
    
    // 指标追踪
    metrics map[string]*ABMetrics
}

type ABMetrics struct {
    CTR        float64  // 点击率
    CVR        float64  // 转化率
    AvgPosition float64  // 平均位置
    UserSatisfaction float64  // 用户满意度
}

// 运行 A/B 测试
func (ab *ABTestFramework) RunTest(
    controlModel,
    treatmentModel ltrmodel.Model,
    duration time.Duration,
) (*TestResult, error) {
    
    // 1. 分流
    ab.splitTraffic(controlModel, treatmentModel)
    
    // 2. 收集数据
    time.Sleep(duration)
    
    // 3. 计算指标
    controlMetrics := ab.collectMetrics("control")
    treatmentMetrics := ab.collectMetrics("treatment")
    
    // 4. 统计显著性检验
    significant, pValue := ab.statisticalTest(
        controlMetrics,
        treatmentMetrics,
    )
    
    return &TestResult{
        Significant: significant,
        PValue:      pValue,
        Improvement: (treatmentMetrics.CTR - controlMetrics.CTR) / controlMetrics.CTR,
    }, nil
}
```

---

## 9. 部署与更新

### 9.1 模型服务

```go
type RankingService struct {
    model        ltrmodel.Model
    extractor    *FeatureExtractor
    
    // 模型版本
    version      string
    
    // 更新锁
    updateMu     sync.RWMutex
}

// 在线排序
func (rs *RankingService) Rank(
    ctx context.Context,
    query string,
    docIDs []int64,
    userID string,
) ([]RankedResult, error) {
    
    rs.updateMu.RLock()
    defer rs.updateMu.RUnlock()
    
    results := make([]RankedResult, len(docIDs))
    
    for i, docID := range docIDs {
        // 1. 获取文档
        doc := rs.getDocument(docID)
        if doc == nil {
            continue
        }
        
        // 2. 提取特征
        features := rs.extractor.Extract(query, doc, userID)
        
        // 3. 模型预测
        score := rs.model.Predict(features)
        
        results[i] = RankedResult{
            DocID: docID,
            Score: score,
            Doc:   doc,
        }
    }
    
    // 4. 排序
    sort.Slice(results, func(i, j int) bool {
        return results[i].Score > results[j].Score
    })
    
    return results, nil
}
```

### 9.2 模型热更新

```go
// 热更新模型
func (rs *RankingService) UpdateModel(newModel ltrmodel.Model, version string) error {
    rs.updateMu.Lock()
    defer rs.updateMu.Unlock()
    
    // 验证模型
    if err := rs.validateModel(newModel); err != nil {
        return fmt.Errorf("model validation failed: %w", err)
    }
    
    // 切换模型
    oldModel := rs.model
    rs.model = newModel
    rs.version = version
    
    // 优雅关闭旧模型
    go oldModel.Close()
    
    log.Printf("Model updated to version: %s", version)
    return nil
}
```

---

## 10. 实施计划

### 阶段 1：基础框架 (1-2 周)

- [ ] 特征提取器实现
- [ ] 训练样本结构定义
- [ ] 数据集类实现
- [ ] 评估指标实现 (NDCG, MAP, MRR)

**优先级**: 🔴 高

---

### 阶段 2：LambdaMART 模型 (2-3 周)

- [ ] LambdaMART 核心算法
- [ ] 回归树实现
- [ ] 梯度计算
- [ ] 模型保存/加载
- [ ] 单元测试

**优先级**: 🔴 高

---

### 阶段 3：数据收集 (1-2 周)

- [ ] 用户行为收集器
- [ ] 行为存储 (ClickHouse/Postgres)
- [ ] 标签生成器
- [ ] 样本采样策略

**优先级**: 🟡 中

---

### 阶段 4：训练流水线 (2 周)

- [ ] 离线训练流水线
- [ ] 在线学习框架
- [ ] 模型评估器
- [ ] A/B 测试框架

**优先级**: 🟡 中

---

### 阶段 5：部署集成 (1-2 周)

- [ ] 排序服务接口
- [ ] 模型热更新
- [ ] 监控指标
- [ ] 性能优化

**优先级**: 🟡 中

---

### 阶段 6：优化与调参 (1 周)

- [ ] 超参数调优
- [ ] 特征选择
- [ ] 性能调优
- [ ] 文档完善

**优先级**: 🟢 低

---

## 11. 预期效果

### 11.1 业务指标提升

| 指标 | 基准 (BM25) | 目标 (LTR) | 提升 |
|------|-------------|-----------|------|
| NDCG@10 | 0.65 | 0.80 | +23% |
| CTR | 8.5% | 11.2% | +32% |
| 用户满意度 | 3.8/5 | 4.3/5 | +13% |

### 11.2 技术收益

- **自动化**: 无需人工调优参数
- **可解释性**: 特征重要性分析
- **可扩展性**: 支持新增特征和模型
- **实时性**: 在线学习适应变化

---

## 12. 风险评估

### 12.1 技术风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| 训练数据不足 | 中 | 高 | 冷启动使用 BM25，逐步收集数据 |
| 过拟合 | 中 | 中 | 正则化、交叉验证、早停 |
| 模型漂移 | 低 | 中 | 在线学习 + 定期重训练 |
| 性能下降 | 低 | 高 | 模型压缩、特征选择、缓存 |

### 12.2 业务风险

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| 效果不显著 | 中 | 高 | A/B 测试验证，多模型对比 |
| 用户反感 | 低 | 高 | 监控用户反馈，快速回滚 |
| 数据隐私 | 低 | 高 | 匿名化处理，合规审查 |

---

## 13. 资源需求

### 13.1 计算资源

- **训练服务器**: 16核 CPU, 64GB RAM, 1TB SSD
- **GPU (可选)**: NVIDIA A100 (加速神经网络训练)
- **存储**: 10TB (用户行为数据, 模型快照)

### 13.2 人力资源

- **算法工程师**: 1人 (模型开发)
- **后端工程师**: 1人 (服务集成)
- **数据工程师**: 0.5人 (数据管道)

### 13.3 时间投入

- **开发周期**: 6-8 周
- **数据收集**: 2-4 周 (并行)
- **效果验证**: 2-3 周 (A/B 测试)

---

## 14. 总结

### 14.1 核心价值

✅ **自动优化**: 从用户行为中学习最优排序
✅ **业务驱动**: 直接优化 CTR、转化率等指标
✅ **技术先进**: LambdaMART + 在线学习
✅ **可扩展**: 支持多特征、多模型

### 14.2 实施建议

**优先级排序**: LambdaMART > 数据收集 > 在线学习 > A/B 测试

**成功关键**: 
1. 高质量训练数据
2. 有效的特征工程
3. 持续的模型监控
4. 快速的迭代能力

### 14.3 后续扩展

- **深度学习**: BERT-based 排序模型
- **个性化**: 用户 embedding + 矩阵分解
- **多目标**: 平衡 CTR + 转化率 + 用户体验
- **强化学习**: 在线探索-利用 (Explore-Exploit)

---

**状态**: 设计方案完成 ✅
**下一步**: 按阶段实施 (建议从 LambdaMART 开始)
