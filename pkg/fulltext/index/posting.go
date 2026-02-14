package index

import (
	"sort"
)

// Posting 倒排列表项
type Posting struct {
	DocID     int64
	Frequency int     // 词频
	Positions []int   // 位置信息（用于短语查询）
	BM25Score float64 // 预计算的BM25分数
}

// PostingsList 倒排列表
type PostingsList struct {
	TermID      int64
	Postings    []Posting
	DocCount    int64
	MaxScore    float64   // 最大分数（用于MAXSCORE优化）
	SkipList    *SkipList // 跳表（用于快速跳转）
}

// NewPostingsList 创建倒排列表
func NewPostingsList(termID int64) *PostingsList {
	return &PostingsList{
		TermID:   termID,
		Postings: make([]Posting, 0),
		SkipList: NewSkipList(),
	}
}

// AddPosting 添加倒排项
func (pl *PostingsList) AddPosting(posting Posting) {
	pl.Postings = append(pl.Postings, posting)
	pl.DocCount++
	
	// 更新最大分数
	if posting.BM25Score > pl.MaxScore {
		pl.MaxScore = posting.BM25Score
	}
	
	// 更新跳表（每N个文档添加一个跳点）
	skipInterval := 64
	if len(pl.Postings)%skipInterval == 0 {
		pl.SkipList.AddSkipPoint(posting.DocID, len(pl.Postings)-1, posting.BM25Score)
	}
}

// FindPosting 查找指定文档的倒排项
func (pl *PostingsList) FindPosting(docID int64) *Posting {
	// 使用二分查找（假设Postings按DocID排序）
	idx := sort.Search(len(pl.Postings), func(i int) bool {
		return pl.Postings[i].DocID >= docID
	})
	
	if idx < len(pl.Postings) && pl.Postings[idx].DocID == docID {
		return &pl.Postings[idx]
	}
	return nil
}

// FindPostingWithSkip 使用跳表查找
func (pl *PostingsList) FindPostingWithSkip(docID int64) *Posting {
	// 使用跳表快速定位
	skipIdx := pl.SkipList.FindNearestSkipPoint(docID)
	startIdx := 0
	if skipIdx >= 0 {
		startIdx = skipIdx
	}
	
	// 线性搜索
	for i := startIdx; i < len(pl.Postings); i++ {
		if pl.Postings[i].DocID == docID {
			return &pl.Postings[i]
		}
		if pl.Postings[i].DocID > docID {
			return nil
		}
	}
	
	return nil
}

// RemovePosting 移除指定文档的倒排项
func (pl *PostingsList) RemovePosting(docID int64) bool {
	for i, p := range pl.Postings {
		if p.DocID == docID {
			pl.Postings = append(pl.Postings[:i], pl.Postings[i+1:]...)
			pl.DocCount--
			return true
		}
	}
	return false
}

// SortByDocID 按DocID排序
func (pl *PostingsList) SortByDocID() {
	sort.Slice(pl.Postings, func(i, j int) bool {
		return pl.Postings[i].DocID < pl.Postings[j].DocID
	})
}

// GetIterator 获取迭代器
func (pl *PostingsList) GetIterator() *PostingsIterator {
	return &PostingsIterator{
		postings: pl.Postings,
		position: -1,
	}
}

// PostingsIterator 倒排列表迭代器
type PostingsIterator struct {
	postings []Posting
	position int
}

// Next 移动到下一个
func (it *PostingsIterator) Next() bool {
	it.position++
	return it.position < len(it.postings)
}

// Current 获取当前项
func (it *PostingsIterator) Current() *Posting {
	if it.position < 0 || it.position >= len(it.postings) {
		return nil
	}
	return &it.postings[it.position]
}

// DocID 获取当前DocID
func (it *PostingsIterator) DocID() int64 {
	if it.position < 0 || it.position >= len(it.postings) {
		return -1
	}
	return it.postings[it.position].DocID
}

// BM25Score 获取当前BM25分数
func (it *PostingsIterator) BM25Score() float64 {
	if it.position < 0 || it.position >= len(it.postings) {
		return 0
	}
	return it.postings[it.position].BM25Score
}

// AdvanceTo 前进到指定DocID
func (it *PostingsIterator) AdvanceTo(docID int64) bool {
	for it.Next() {
		if it.DocID() >= docID {
			return true
		}
	}
	return false
}

// SkipList 跳表
type SkipList struct {
	Points []SkipPoint
}

// SkipPoint 跳点
type SkipPoint struct {
	DocID    int64
	Index    int
	MaxScore float64
}

// NewSkipList 创建跳表
func NewSkipList() *SkipList {
	return &SkipList{
		Points: make([]SkipPoint, 0),
	}
}

// AddSkipPoint 添加跳点
func (sl *SkipList) AddSkipPoint(docID int64, index int, maxScore float64) {
	sl.Points = append(sl.Points, SkipPoint{
		DocID:    docID,
		Index:    index,
		MaxScore: maxScore,
	})
}

// FindNearestSkipPoint 查找最近的跳点
func (sl *SkipList) FindNearestSkipPoint(docID int64) int {
	if len(sl.Points) == 0 {
		return -1
	}
	
	// 二分查找
	idx := sort.Search(len(sl.Points), func(i int) bool {
		return sl.Points[i].DocID >= docID
	})
	
	if idx > 0 {
		return sl.Points[idx-1].Index
	}
	return -1
}

// GetMaxScoreUpTo 获取到指定DocID为止的最大分数
func (sl *SkipList) GetMaxScoreUpTo(docID int64) float64 {
	maxScore := 0.0
	for _, point := range sl.Points {
		if point.DocID <= docID && point.MaxScore > maxScore {
			maxScore = point.MaxScore
		}
	}
	return maxScore
}
