package processor

import (
	"testing"
	"time"
)

func TestTokenBucketAllow(t *testing.T) {
	tb := &TokenBucket{}
	now := time.Now().UnixNano()

	// burst=3, rate=1/s
	if !tb.Allow(now, 1.0, 3.0) {
		t.Fatal("expected first request allowed")
	}
	if !tb.Allow(now, 1.0, 3.0) {
		t.Fatal("expected second request allowed")
	}
	if !tb.Allow(now, 1.0, 3.0) {
		t.Fatal("expected third request allowed")
	}
	if tb.Allow(now, 1.0, 3.0) {
		t.Fatal("expected fourth request blocked")
	}
}

func TestDedupWindow(t *testing.T) {
	dw := NewDedupWindow()
	if dw.IsDuplicate("u1", "q1") {
		t.Fatal("new entry should not be duplicate")
	}
	dw.Add("u1", "q1")
	if !dw.IsDuplicate("u1", "q1") {
		t.Fatal("same user+query should be duplicate")
	}
	if dw.IsDuplicate("u2", "q1") {
		t.Fatal("different user should not be duplicate")
	}

	dw.Rotate()
	if !dw.IsDuplicate("u1", "q1") {
		t.Fatal("should still be duplicate in previous slot")
	}

	dw.Rotate()
	if dw.IsDuplicate("u1", "q1") {
		t.Fatal("should not be duplicate after two rotations")
	}
}

func TestBurstTracker(t *testing.T) {
	bt := NewBurstTracker()
	q := "spike"

	if bt.Record(q) {
		t.Fatal("first record should not be burst")
	}
	if bt.Record(q) {
		t.Fatal("second record should not be burst")
	}

	bt.Rotate()
	bt.Rotate()

	for i := 0; i < 50; i++ {
		bt.Record(q)
	}

	if !bt.Record(q) {
		t.Fatal("expected burst detection after spike")
	}
}

func TestBurstTrackerAbsoluteThreshold(t *testing.T) {
	bt := NewBurstTracker()
	q := "flash"

	for i := 0; i < 20; i++ {
		if bt.Record(q) {
			t.Fatal("no burst expected at <= 20 events in fresh slot")
		}
	}

	if !bt.Record(q) {
		t.Fatal("expected burst detection at 21st event by absolute threshold")
	}
}
