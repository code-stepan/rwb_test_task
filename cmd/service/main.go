package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/wildberries/trending-search-service/internal/api"
	"github.com/wildberries/trending-search-service/internal/config"
	"github.com/wildberries/trending-search-service/internal/consumer"
	"github.com/wildberries/trending-search-service/internal/processor"
	"github.com/wildberries/trending-search-service/internal/stoplist"
	"github.com/wildberries/trending-search-service/internal/storage"
)

func main() {
	cfg := config.Load()

	window := storage.NewWindow(cfg.WindowSlots, cfg.SlotDuration)
	counter := storage.NewGlobalCounter()
	topHeap := storage.NewTopHeap(cfg.TopK)
	cache := storage.NewTrendCache()
	stopList := stoplist.NewManager()

	recalcNotify := make(chan struct{}, 1)

	pipeline := processor.NewPipeline(cfg, window, counter, topHeap,
		func(status string) { api.EventsTotal.WithLabelValues(status).Inc() },
		func(q string) bool { return stopList.IsBlocked(q) },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(cfg.SlotDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pipeline.Rotate()
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pipeline.RotateDedup()
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pipeline.Cleanup()
			case <-ctx.Done():
				return
			}
		}
	}()

	server := api.New(cfg, cache, stopList, topHeap, counter, recalcNotify)
	recalcDone := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(cfg.RecalcInterval)
		defer ticker.Stop()
		first := true
		for {
			select {
			case <-ticker.C:
				recalcAndCache(cfg, counter, topHeap, stopList, cache)
				if first {
					first = false
					server.SetReady()
					recalcDone <- struct{}{}
				}
			case <-recalcNotify:
				recalcAndCache(cfg, counter, topHeap, stopList, cache)
			case <-ctx.Done():
				return
			}
		}
	}()

	cons, err := consumer.New(cfg, pipeline.Process)
	if err != nil {
		fmt.Fprintf(os.Stderr, "consumer init error: %v\n", err)
		os.Exit(1)
	}
	go cons.Run(ctx)
	<-cons.Ready()

	<-recalcDone

	go func() {
		if err := server.Listen(); err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			cancel()
		}
	}()

	fmt.Printf("Service started on port %s (consumer=%s, topic=%s)\n",
		cfg.HTTPPort, cfg.KafkaGroupID, cfg.KafkaTopic)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("Shutting down...")
	cancel()
	cons.Close()
	time.Sleep(300 * time.Millisecond)
}

func recalcAndCache(cfg *config.Config, counter *storage.GlobalCounter, topHeap *storage.TopHeap, sl *stoplist.Manager, cache *storage.TrendCache) {
	start := time.Now()

	heapCandidates := topHeap.GetAll()
	verified := make([]storage.QueryCount, 0, len(heapCandidates))
	for _, c := range heapCandidates {
		actual := counter.Get(c.Query)
		if actual == 0 {
			continue
		}
		verified = append(verified, storage.QueryCount{Query: c.Query, Count: actual})
	}

	need := cfg.TopN * 2
	if len(verified) < need {
		top := counter.SnapshotTopCandidates(need)
		seen := make(map[string]struct{}, len(verified))
		for _, c := range verified {
			seen[c.Query] = struct{}{}
		}
		for _, a := range top {
			if _, ok := seen[a.Query]; !ok {
				verified = append(verified, a)
			}
		}
	}

	sort.Slice(verified, func(i, j int) bool {
		return verified[i].Count > verified[j].Count
	})

	trends := make([]storage.TrendItem, 0, cfg.TopN)
	rank := 1
	for _, c := range verified {
		if sl.IsBlocked(c.Query) {
			continue
		}
		trends = append(trends, storage.TrendItem{
			Query: c.Query,
			Count: c.Count,
			Rank:  rank,
		})
		rank++
		if len(trends) >= cfg.TopN {
			break
		}
	}

	windowSec := cfg.WindowSlots * int(cfg.SlotDuration.Seconds())
	cache.Store(&storage.TrendsData{
		UpdatedAt:     time.Now().UTC(),
		WindowSeconds: windowSec,
		TotalQueries:  counter.Size(),
		Trends:        trends,
	})

	api.RecalcDuration.Observe(time.Since(start).Seconds())
	api.MemoryUniqueQueries.Set(float64(counter.Size()))
	api.TopCacheTimestamp.Set(float64(time.Now().Unix()))
}
