package storage

import (
	"testing"
)

func BenchmarkTopHeapUpdate(b *testing.B) {
	h := NewTopHeap(300)
	for i := 0; i < b.N; i++ {
		h.Update("query_bench", uint64(i))
	}
}

func BenchmarkGlobalCounterInc(b *testing.B) {
	c := NewGlobalCounter()
	for i := 0; i < b.N; i++ {
		c.Inc("query_bench")
	}
}

func BenchmarkGlobalCounterSnapshot(b *testing.B) {
	c := NewGlobalCounter()
	for i := 0; i < 10000; i++ {
		c.Inc(string(rune('a'+(i%26))))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.SnapshotTopCandidates(100)
	}
}

func BenchmarkTrendCacheStore(b *testing.B) {
	cache := NewTrendCache()
	data := &TrendsData{
		WindowSeconds: 300,
		TotalQueries:  5000,
	}
	for i := 0; i < 100; i++ {
		data.Trends = append(data.Trends, TrendItem{
			Query: "query_bench",
			Count: uint64(1000 - i),
			Rank:  i + 1,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Store(data)
	}
}

func BenchmarkTrendCacheLoadJSON(b *testing.B) {
	cache := NewTrendCache()
	data := &TrendsData{WindowSeconds: 300}
	for i := 0; i < 100; i++ {
		data.Trends = append(data.Trends, TrendItem{
			Query: "query_bench",
			Count: uint64(1000 - i),
			Rank:  i + 1,
		})
	}
	cache.Store(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.LoadJSON(10)
	}
}
