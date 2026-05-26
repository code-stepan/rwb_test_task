package processor

import (
	"testing"
	"time"

	"github.com/wildberries/trending-search-service/internal/config"
	"github.com/wildberries/trending-search-service/internal/models"
	"github.com/wildberries/trending-search-service/internal/storage"
)

func BenchmarkPipelineProcess(b *testing.B) {
	cfg := &config.Config{
		RateLimitIP:    1000,
		RateLimitUser:  1000,
		RateLimitBurst: 100,
		WindowSlots:    30,
		SlotDuration:   10 * time.Second,
		QuarantineDur:  60 * time.Second,
	}
	window := storage.NewWindow(cfg.WindowSlots, cfg.SlotDuration)
	counter := storage.NewGlobalCounter()
	topHeap := storage.NewTopHeap(300)
	p := NewPipeline(cfg, window, counter, topHeap, nil, nil)

	evt := &models.SearchEvent{
		EventID:         "bench",
		Timestamp:       time.Now(),
		QueryNormalized: "bench query",
		UserID:          "bench_user",
		IPHash:          "bench_ip",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Process(evt)
	}
}
