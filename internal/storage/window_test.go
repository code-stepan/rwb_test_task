package storage

import (
	"testing"
	"time"
)

func TestWindowAddAndRotate(t *testing.T) {
	w := NewWindow(3, time.Second) // 3 slots × 1s = 3s window
	now := time.Now().UnixNano()

	w.Add("iphone", now)
	w.Add("iphone", now)
	w.Add("samsung", now)

	// Should have both queries in current slot
	evicted := w.Rotate(now)
	if len(evicted) != 0 {
		t.Fatalf("expected no eviction, got %v", evicted)
	}

	// Move time forward past window
	future := now + int64(4*time.Second)
	evicted = w.Rotate(future)

	if evicted["iphone"] != 2 {
		t.Fatalf("expected iphone=2 evicted, got %d", evicted["iphone"])
	}
	if evicted["samsung"] != 1 {
		t.Fatalf("expected samsung=1 evicted, got %d", evicted["samsung"])
	}
}

func TestWindowStaleSlotAutoClear(t *testing.T) {
	w := NewWindow(2, time.Minute)
	now := time.Now().UnixNano()

	w.Add("query", now)
	// Same slot index but next cycle (2 minutes later)
	later := now + int64(2*time.Minute)
	w.Add("query2", later)

	// The old slot should have been wiped, so eviction of old cycle returns nothing
	evicted := w.Rotate(later + int64(time.Minute))
	if len(evicted) != 0 {
		t.Fatalf("expected empty eviction after auto-clear, got %v", evicted)
	}
}
