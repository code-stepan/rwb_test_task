package processor

import (
	"sync"
	"time"

	"github.com/wildberries/trending-search-service/internal/config"
	"github.com/wildberries/trending-search-service/internal/models"
	"github.com/wildberries/trending-search-service/internal/storage"
)

type EventCallback func(status string)

type StoplistChecker func(query string) bool

type Pipeline struct {
	window        *storage.Window
	counter       *storage.GlobalCounter
	topHeap       *storage.TopHeap
	ipLimiter     *RateLimiter
	userLimiter   *RateLimiter
	dedup         *DedupWindow
	burst         *BurstTracker
	quarantine    map[string]int64
	quarantineMu  sync.RWMutex
	quarantineDur time.Duration
	stoplistCheck StoplistChecker
	eventCallback EventCallback
}

func NewPipeline(cfg *config.Config, window *storage.Window, counter *storage.GlobalCounter, topHeap *storage.TopHeap, eventCallback EventCallback, stoplistCheck StoplistChecker) *Pipeline {
	return &Pipeline{
		window:        window,
		counter:       counter,
		topHeap:       topHeap,
		ipLimiter:     NewRateLimiter(cfg.RateLimitIP, cfg.RateLimitBurst),
		userLimiter:   NewRateLimiter(cfg.RateLimitUser, cfg.RateLimitBurst),
		dedup:         NewDedupWindow(),
		burst:         NewBurstTracker(),
		quarantine:    make(map[string]int64),
		quarantineDur: cfg.QuarantineDur,
		stoplistCheck: stoplistCheck,
		eventCallback: eventCallback,
	}
}

func (p *Pipeline) Process(evt *models.SearchEvent) bool {
	q := NormalizeQuery(evt.QueryNormalized)
	if q == "" {
		p.emitEvent("dropped_normalize")
		return false
	}

	if p.stoplistCheck != nil && p.stoplistCheck(q) {
		p.emitEvent("dropped_stoplisted")
		return false
	}

	now := time.Now().UnixNano()

	if !p.ipLimiter.Allow(evt.IPHash) {
		p.emitEvent("dropped_ip_limit")
		return false
	}

	if !p.userLimiter.Allow(evt.UserID) {
		p.emitEvent("dropped_user_limit")
		return false
	}

	if p.dedup.IsDuplicate(evt.UserID, q) {
		p.emitEvent("dropped_dedup")
		return false
	}
	p.dedup.Add(evt.UserID, q)

	p.quarantineMu.RLock()
	if until, ok := p.quarantine[q]; ok && now < until {
		p.quarantineMu.RUnlock()
		p.emitEvent("dropped_quarantine")
		return false
	}
	p.quarantineMu.RUnlock()

	if p.burst.Record(q) {
		p.quarantineMu.Lock()
		p.quarantine[q] = now + int64(p.quarantineDur)
		p.quarantineMu.Unlock()
		p.emitEvent("dropped_burst")
		return false
	}

	p.window.Add(q, now)
	count := p.counter.Inc(q)
	p.topHeap.Update(q, count)

	p.emitEvent("processed")
	return true
}

func (p *Pipeline) emitEvent(status string) {
	if p.eventCallback != nil {
		p.eventCallback(status)
	}
}

func (p *Pipeline) Rotate() {
	now := time.Now().UnixNano()
	evicted := p.window.Rotate(now)
	p.counter.ApplyEvictions(evicted)
	p.burst.Rotate()
}

func (p *Pipeline) RotateDedup() {
	p.dedup.Rotate()
}

func (p *Pipeline) Cleanup() {
	p.ipLimiter.Cleanup(5 * time.Minute)
	p.userLimiter.Cleanup(5 * time.Minute)
}
