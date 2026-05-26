package storage

import "testing"

func TestGlobalCounterIncAndEvict(t *testing.T) {
	gc := NewGlobalCounter()

	gc.Inc("a")
	gc.Inc("a")
	gc.Inc("b")

	if gc.Size() != 2 {
		t.Fatalf("expected size 2, got %d", gc.Size())
	}

	gc.ApplyEvictions(map[string]uint32{"a": 2})
	if gc.Size() != 1 {
		t.Fatalf("expected size 1 after eviction, got %d", gc.Size())
	}

	gc.ApplyEvictions(map[string]uint32{"b": 1})
	if gc.Size() != 0 {
		t.Fatalf("expected size 0, got %d", gc.Size())
	}
}

func TestGlobalCounterPartialEvict(t *testing.T) {
	gc := NewGlobalCounter()
	gc.Inc("x")
	gc.Inc("x")
	gc.Inc("x")

	gc.ApplyEvictions(map[string]uint32{"x": 2})

	all := gc.SnapshotTopCandidates(0)
	if len(all) != 1 || all[0].Count != 1 {
		t.Fatalf("expected x=1, got %v", all)
	}
}
