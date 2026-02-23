package statistics

import (
	"fmt"
	"math"
	"sort"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// Histogram 直方图类型
type HistogramType int

const (
	EquiWidthHistogram HistogramType = iota // 等宽直方图
	EquiDepthHistogram                      // 等深直方图
	FrequencyHistogram                      // 频率直方图
)

// HistogramBucket 直方图桶
type HistogramBucket struct {
	LowerBound interface{}
	UpperBound interface{}
	Count      int64
	Distinct   int64
	NDV        int64 // 唯一值数
}

// Histogram 直方图
type Histogram struct {
	Type        HistogramType
	Buckets     []*HistogramBucket
	MinValue    interface{}
	MaxValue    interface{}
	BucketCount int
	NDV         int64 // 总唯一值数
	NullCount   int64
}

// BuildEquiWidthHistogram 构建等宽直方图
func BuildEquiWidthHistogram(values []interface{}, bucketCount int) *Histogram {
	if len(values) == 0 {
		return &Histogram{
			Type:        EquiWidthHistogram,
			Buckets:     []*HistogramBucket{},
			BucketCount: bucketCount,
		}
	}

	// 过滤非空值
	nonNullValues := make([]interface{}, 0, len(values))
	for _, val := range values {
		if val != nil {
			nonNullValues = append(nonNullValues, val)
		}
	}

	if len(nonNullValues) == 0 {
		return &Histogram{
			Type:        EquiWidthHistogram,
			Buckets:     []*HistogramBucket{},
			BucketCount: bucketCount,
			NullCount:   int64(len(values)),
		}
	}

	// 排序
	sortedValues := make([]interface{}, len(nonNullValues))
	copy(sortedValues, nonNullValues)
	sort.Slice(sortedValues, func(i, j int) bool {
		return compareHistogramValues(sortedValues[i], sortedValues[j]) < 0
	})

	// 计算Min/Max
	hist := &Histogram{
		Type:        EquiWidthHistogram,
		MinValue:    sortedValues[0],
		MaxValue:    sortedValues[len(sortedValues)-1],
		BucketCount: bucketCount,
		NullCount:   int64(len(values) - len(nonNullValues)),
	}

	// 计算总唯一值数
	uniqueSet := make(map[interface{}]bool)
	for _, val := range sortedValues {
		uniqueSet[val] = true
	}
	hist.NDV = int64(len(uniqueSet))

	// 构建桶
	if bucketCount <= 0 {
		bucketCount = 10
	}
	if bucketCount > len(nonNullValues) {
		bucketCount = len(nonNullValues)
	}

	// 计算每个桶的值数
	valuesPerBucket := float64(len(nonNullValues)) / float64(bucketCount)
	hist.Buckets = make([]*HistogramBucket, 0, bucketCount)

	for i := 0; i < bucketCount; i++ {
		start := int(float64(i) * valuesPerBucket)
		end := int(float64(i+1) * valuesPerBucket)
		if end > len(nonNullValues) {
			end = len(nonNullValues)
		}

		bucketValues := sortedValues[start:end]
		if len(bucketValues) == 0 {
			continue
		}

		bucket := &HistogramBucket{
			LowerBound: bucketValues[0],
			UpperBound: bucketValues[len(bucketValues)-1],
			Count:      int64(len(bucketValues)),
		}

		// 计算桶内唯一值数
		bucketUnique := make(map[interface{}]bool)
		for _, val := range bucketValues {
			bucketUnique[val] = true
		}
		bucket.Distinct = int64(len(bucketUnique))
		bucket.NDV = bucket.Distinct

		hist.Buckets = append(hist.Buckets, bucket)
	}

	return hist
}

// BuildFrequencyHistogram 构建频率直方图
func BuildFrequencyHistogram(values []interface{}, bucketCount int) *Histogram {
	if len(values) == 0 {
		return &Histogram{
			Type:        FrequencyHistogram,
			Buckets:     []*HistogramBucket{},
			BucketCount: bucketCount,
		}
	}

	// 计算值频率
	freq := make(map[interface{}]int64)
	nonNullCount := int64(0)
	for _, val := range values {
		if val != nil {
			freq[val]++
			nonNullCount++
		}
	}

	hist := &Histogram{
		Type:        FrequencyHistogram,
		BucketCount: bucketCount,
		NullCount:   int64(len(values)) - nonNullCount,
		NDV:         int64(len(freq)),
	}

	if len(freq) == 0 {
		return hist
	}

	// 按频率排序
	type valueFreq struct {
		value interface{}
		freq  int64
	}

	sortedFreq := make([]valueFreq, 0, len(freq))
	for val, f := range freq {
		sortedFreq = append(sortedFreq, valueFreq{value: val, freq: f})
	}

	sort.Slice(sortedFreq, func(i, j int) bool {
		return sortedFreq[i].freq > sortedFreq[j].freq
	})

	// 确定Min/Max
	hist.MinValue = sortedFreq[0].value
	hist.MaxValue = sortedFreq[len(sortedFreq)-1].value

	// 将高频率值分组到桶
	if bucketCount > len(sortedFreq) {
		bucketCount = len(sortedFreq)
	}

	hist.Buckets = make([]*HistogramBucket, 0, bucketCount)
	totalFreq := int64(0)

	for i := 0; i < bucketCount; i++ {
		start := i * len(sortedFreq) / bucketCount
		end := (i + 1) * len(sortedFreq) / bucketCount
		if end > len(sortedFreq) {
			end = len(sortedFreq)
		}

		bucket := &HistogramBucket{}
		for j := start; j < end; j++ {
			bucket.Count += sortedFreq[j].freq
			totalFreq += sortedFreq[j].freq
		}

		bucket.LowerBound = sortedFreq[start].value
		bucket.UpperBound = sortedFreq[end-1].value
		bucket.NDV = int64(end - start)

		hist.Buckets = append(hist.Buckets, bucket)
	}

	return hist
}

// EstimateSelectivity 估算过滤器的选择性（基于直方图）
func (h *Histogram) EstimateSelectivity(filter domain.Filter) float64 {
	if h == nil || len(h.Buckets) == 0 {
		return 0.1 // 默认选择率
	}

	switch filter.Operator {
	case "=", "!=":
		return h.EstimateEqualitySelectivity(filter.Value)
	case ">", ">=", "<", "<=":
		return h.estimateRangeSelectivity(filter.Operator, filter.Value)
	case "IN":
		if valList, ok := filter.Value.([]interface{}); ok {
			return h.estimateInSelectivity(valList)
		}
		return 0.2
	case "BETWEEN":
		if vals, ok := filter.Value.([]interface{}); ok && len(vals) == 2 {
			sel1 := h.estimateRangeSelectivity(">=", vals[0])
			sel2 := h.estimateRangeSelectivity("<=", vals[1])
			return sel1 * sel2
		}
		return 0.3
	case "LIKE":
		return 0.25 // LIKE默认选择率
	default:
		return 0.5 // 默认50%
	}
}

// estimateEqualitySelectivity 估算等值查询的选择率
func (h *Histogram) EstimateEqualitySelectivity(value interface{}) float64 {
	// 查找值所在的桶
	for _, bucket := range h.Buckets {
		if h.isValueInRange(value, bucket.LowerBound, bucket.UpperBound) {
			// 选择率 ≈ 桶内NDV / 总NDV
			if bucket.NDV > 0 && h.NDV > 0 {
				sel := float64(bucket.NDV) / float64(h.NDV)
				// 考虑频率
				return math.Min(sel, float64(bucket.Count)/float64(h.totalCount()))
			}
		}
	}

	// 值不在任何桶中，选择率为0
	return 0.0
}

// estimateRangeSelectivity 估算范围查询的选择率
func (h *Histogram) estimateRangeSelectivity(operator string, value interface{}) float64 {
	valueNum, ok := toFloat64(value)
	if !ok {
		return 0.3
	}

	minNum, minOk := toFloat64(h.MinValue)
	maxNum, maxOk := toFloat64(h.MaxValue)

	if !minOk || !maxOk {
		return 0.3
	}

	// 计算范围内的桶数
	inRangeBuckets := 0
	totalRange := maxNum - minNum

	for _, bucket := range h.Buckets {
		lowerNum, lowerOk := toFloat64(bucket.LowerBound)
		upperNum, upperOk := toFloat64(bucket.UpperBound)

		if !lowerOk || !upperOk {
			continue
		}

		bucketInRange := false

		switch operator {
		case ">":
			bucketInRange = upperNum > valueNum
		case ">=":
			bucketInRange = upperNum >= valueNum
		case "<":
			bucketInRange = lowerNum < valueNum
		case "<=":
			bucketInRange = lowerNum <= valueNum
		}

		if bucketInRange {
			inRangeBuckets++
		}
	}

	// 选择率 = 范围内桶数 / 总桶数
	if len(h.Buckets) > 0 {
		return float64(inRangeBuckets) / float64(len(h.Buckets))
	}

	// 如果是等宽直方图且能确定数值范围
	if h.Type == EquiWidthHistogram && totalRange > 0 {
		var fraction float64
		switch operator {
		case ">":
			fraction = (maxNum - valueNum) / totalRange
		case ">=":
			fraction = (maxNum - valueNum + 0.0001) / totalRange
		case "<":
			fraction = (valueNum - minNum) / totalRange
		case "<=":
			fraction = (valueNum - minNum + 0.0001) / totalRange
		}
		return math.Max(0.0, math.Min(1.0, fraction))
	}

	return 0.3
}

// estimateInSelectivity 估算IN查询的选择率
func (h *Histogram) estimateInSelectivity(values []interface{}) float64 {
	if len(h.Buckets) == 0 {
		return 0.2
	}

	// 计算IN列表中不同值的数量
	distinctInValues := make(map[interface{}]bool)
	for _, val := range values {
		if val != nil {
			distinctInValues[val] = true
		}
	}

	// 估算覆盖的桶数
	coveredBuckets := 0
	for _, val := range values {
		for _, bucket := range h.Buckets {
			if h.isValueInRange(val, bucket.LowerBound, bucket.UpperBound) {
				coveredBuckets++
				break
			}
		}
	}

	// 选择率 = 覆盖桶数 * (IN值数/总NDV) + 未覆盖桶的默认选择率
	bucketCoverage := float64(coveredBuckets) / float64(len(h.Buckets))
	valueCoverage := float64(len(distinctInValues)) / float64(h.NDV)

	return bucketCoverage * valueCoverage
}

// isValueInRange 检查值是否在桶范围内
func (h *Histogram) isValueInRange(value, lower, upper interface{}) bool {
	if value == nil {
		return false
	}

	cmpLower := compareHistogramValues(value, lower)
	cmpUpper := compareHistogramValues(value, upper)

	// 等宽直方图：值可能在桶边界
	if h.Type == EquiWidthHistogram {
		return cmpLower >= 0 && cmpUpper <= 0
	}

	// 频率直方图：值应该在桶的Lower和Upper之间
	return cmpLower >= 0 && cmpUpper <= 0
}

// totalCount 计算总行数（不包括NULL）
func (h *Histogram) totalCount() int64 {
	var total int64
	for _, bucket := range h.Buckets {
		total += bucket.Count
	}
	return total
}

// Explain 返回直方图的描述
func (h *Histogram) Explain() string {
	if h == nil {
		return "Empty Histogram"
	}

	typeStr := ""
	switch h.Type {
	case EquiWidthHistogram:
		typeStr = "Equi-Width"
	case FrequencyHistogram:
		typeStr = "Frequency"
	default:
		typeStr = "Unknown"
	}

	bucketCount := h.BucketCount
	if bucketCount == 0 && len(h.Buckets) > 0 {
		bucketCount = len(h.Buckets)
	}

	return fmt.Sprintf("Histogram(type=%s, buckets=%d, ndv=%d, min=%v, max=%v)",
		typeStr, bucketCount, h.NDV, h.MinValue, h.MaxValue)
}

// compareHistogramValues 比较两个值
func compareHistogramValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// 数值比较
	if aNum, aOk := toFloat64(a); aOk {
		if bNum, bOk := toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// 字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}
