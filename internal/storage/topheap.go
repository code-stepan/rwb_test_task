package storage

import (
	"container/heap"
	"sync"
)

type heapItem struct {
	query string
	count uint64
	index int // позиция в слайсе кучи
}

type topHeap struct {
	items []*heapItem
	index map[string]*heapItem // быстрый lookup по запросу
}

func newTopHeap() *topHeap {
	return &topHeap{
		items: make([]*heapItem, 0),
		index: make(map[string]*heapItem),
	}
}

func (h *topHeap) Len() int           { return len(h.items) }
func (h *topHeap) Less(i, j int) bool { return h.items[i].count < h.items[j].count }
func (h *topHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *topHeap) Push(x interface{}) {
	item := x.(*heapItem)
	item.index = len(h.items)
	h.items = append(h.items, item)
	h.index[item.query] = item
}

func (h *topHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	h.items = old[:n-1]
	delete(h.index, item.query)
	return item
}

type TopHeap struct {
	inner   *topHeap
	maxSize int
	mu      sync.RWMutex
}

func NewTopHeap(maxSize int) *TopHeap {
	h := &TopHeap{
		inner:   newTopHeap(),
		maxSize: maxSize,
	}
	heap.Init(h.inner)
	return h
}

func (th *TopHeap) Update(query string, count uint64) {
	th.mu.Lock()
	defer th.mu.Unlock()

	if item, ok := th.inner.index[query]; ok {
		item.count = count
		heap.Fix(th.inner, item.index)
		return
	}

	if th.inner.Len() < th.maxSize {
		heap.Push(th.inner, &heapItem{query: query, count: count})
		return
	}

	if th.inner.Len() > 0 && th.inner.items[0].count < count {
		min := th.inner.items[0]
		delete(th.inner.index, min.query)
		min.query = query
		min.count = count
		th.inner.index[query] = min
		heap.Fix(th.inner, 0)
	}
}

func (th *TopHeap) GetAll() []QueryCount {
	th.mu.RLock()
	defer th.mu.RUnlock()

	res := make([]QueryCount, 0, th.inner.Len())
	for _, item := range th.inner.items {
		res = append(res, QueryCount{Query: item.query, Count: item.count})
	}
	return res
}
