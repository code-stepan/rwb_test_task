package processor

import (
	"math"
	"sync"
	"time"
)

type TokenBucket struct {
	tokens   float64
	lastSeen int64
	mu       sync.Mutex
}

func (tb *TokenBucket) Allow(now int64, rate float64, burst float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if tb.lastSeen == 0 {
		tb.lastSeen = now
		tb.tokens = burst
	}

	elapsed := float64(now-tb.lastSeen) / 1e9
	tb.tokens = math.Min(burst, tb.tokens+elapsed*rate)
	tb.lastSeen = now

	if tb.tokens >= 1.0 {
		tb.tokens--
		return true
	}
	return false
}

type RateLimiter struct {
	buckets map[string]*TokenBucket
	rate    float64
	burst   float64
	mu      sync.RWMutex
}

func NewRateLimiter(ratePerSecond float64, burst float64) *RateLimiter {
	return &RateLimiter{
		buckets: make(map[string]*TokenBucket),
		rate:    ratePerSecond,
		burst:   burst,
	}
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.RLock()
	bucket, ok := rl.buckets[key]
	rl.mu.RUnlock()

	if !ok {
		rl.mu.Lock()
		bucket, ok = rl.buckets[key]
		if !ok {
			bucket = &TokenBucket{}
			rl.buckets[key] = bucket
		}
		rl.mu.Unlock()
	}

	return bucket.Allow(time.Now().UnixNano(), rl.rate, rl.burst)
}

func (rl *RateLimiter) Cleanup(olderThan time.Duration) {
	cutoff := time.Now().Add(-olderThan).UnixNano()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for k, b := range rl.buckets {
		b.mu.Lock()
		if b.lastSeen < cutoff {
			delete(rl.buckets, k)
		}
		b.mu.Unlock()
	}
}

type userQueryKey struct {
	user  string
	query string
}

type DedupWindow struct {
	current  map[userQueryKey]struct{}
	previous map[userQueryKey]struct{}
	mu       sync.RWMutex
}

func NewDedupWindow() *DedupWindow {
	return &DedupWindow{
		current:  make(map[userQueryKey]struct{}),
		previous: make(map[userQueryKey]struct{}),
	}
}

func (dw *DedupWindow) Rotate() {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	dw.previous = dw.current
	dw.current = make(map[userQueryKey]struct{})
}

func (dw *DedupWindow) IsDuplicate(user, query string) bool {
	k := userQueryKey{user: user, query: query}
	dw.mu.RLock()
	_, inPrev := dw.previous[k]
	_, inCurr := dw.current[k]
	dw.mu.RUnlock()
	return inPrev || inCurr
}

func (dw *DedupWindow) Add(user, query string) {
	dw.mu.Lock()
	defer dw.mu.Unlock()
	dw.current[userQueryKey{user: user, query: query}] = struct{}{}
}

const burstAbsoluteThreshold uint32 = 20

const burstRelativeThreshold uint32 = 5

type BurstSlot struct {
	counts [3]uint32
	idx    int
}

type BurstTracker struct {
	trackers map[string]*BurstSlot
	mu       sync.RWMutex
}

func NewBurstTracker() *BurstTracker {
	return &BurstTracker{trackers: make(map[string]*BurstSlot)}
}

func (bt *BurstTracker) Record(query string) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	t, ok := bt.trackers[query]
	if !ok {
		t = &BurstSlot{}
		bt.trackers[query] = t
	}

	t.counts[t.idx]++
	prevTotal := t.counts[(t.idx+1)%3] + t.counts[(t.idx+2)%3]

	if prevTotal > 0 && t.counts[t.idx] > prevTotal*burstRelativeThreshold {
		return true
	}
	if t.counts[t.idx] > burstAbsoluteThreshold {
		return true
	}
	return false
}

func (bt *BurstTracker) Rotate() {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	for _, t := range bt.trackers {
		t.idx = (t.idx + 1) % 3
		t.counts[t.idx] = 0
	}
}
