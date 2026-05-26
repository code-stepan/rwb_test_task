package storage

import (
	"container/heap"
	"sort"
	"sync"
)

type QueryCount struct {
	Query string
	Count uint64
}

type GlobalCounter struct {
	counts map[string]uint64
	mu     sync.RWMutex
}

func NewGlobalCounter() *GlobalCounter {
	return &GlobalCounter{counts: make(map[string]uint64)}
}

func (gc *GlobalCounter) Inc(query string) uint64 {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.counts[query]++
	return gc.counts[query]
}

func (gc *GlobalCounter) ApplyEvictions(evicted map[string]uint32) {
	if len(evicted) == 0 {
		return
	}
	gc.mu.Lock()
	defer gc.mu.Unlock()
	for q, c := range evicted {
		if gc.counts[q] <= uint64(c) {
			delete(gc.counts, q)
		} else {
			gc.counts[q] -= uint64(c)
		}
	}
}

type topCand struct {
	query string
	count uint64
	index int
}

type topCandHeap []topCand

func (h topCandHeap) Len() int           { return len(h) }
func (h topCandHeap) Less(i, j int) bool { return h[i].count < h[j].count }
func (h topCandHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *topCandHeap) Push(x interface{}) {
	it := x.(topCand)
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *topCandHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

func (gc *GlobalCounter) SnapshotTopCandidates(limit int) []QueryCount {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	if len(gc.counts) == 0 {
		return nil
	}
	if limit <= 0 {
		return gc.snapshotAll()
	}

	h := &topCandHeap{}
	heap.Init(h)

	for q, c := range gc.counts {
		if h.Len() < limit {
			heap.Push(h, topCand{query: q, count: c})
		} else if c > (*h)[0].count {
			(*h)[0] = topCand{query: q, count: c}
			heap.Fix(h, 0)
		}
	}

	res := make([]QueryCount, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		it := heap.Pop(h).(topCand)
		res[i] = QueryCount{Query: it.query, Count: it.count}
	}
	return res
}

func (gc *GlobalCounter) snapshotAll() []QueryCount {
	res := make([]QueryCount, 0, len(gc.counts))
	for q, c := range gc.counts {
		res = append(res, QueryCount{Query: q, Count: c})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Count > res[j].Count })
	return res
}

func (gc *GlobalCounter) Get(query string) uint64 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.counts[query]
}

func (gc *GlobalCounter) Size() int {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return len(gc.counts)
}
