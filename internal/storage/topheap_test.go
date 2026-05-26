package storage

import "testing"

func TestTopHeapMaintainsTopK(t *testing.T) {
	h := NewTopHeap(3)

	h.Update("a", 10)
	h.Update("b", 20)
	h.Update("c", 30)
	h.Update("d", 5)  // lower than min, should not enter
	h.Update("e", 40) // should evict min (a=10)

	items := h.GetAll()
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}

	found := make(map[string]uint64)
	for _, it := range items {
		found[it.Query] = it.Count
	}

	if _, ok := found["a"]; ok {
		t.Fatal("expected 'a' to be evicted")
	}
	if found["e"] != 40 {
		t.Fatalf("expected e=40, got %d", found["e"])
	}
}

func TestTopHeapUpdateExisting(t *testing.T) {
	h := NewTopHeap(2)
	h.Update("x", 5)
	h.Update("y", 10)
	h.Update("x", 15) // update x to exceed y

	items := h.GetAll()
	found := make(map[string]uint64)
	for _, it := range items {
		found[it.Query] = it.Count
	}
	if found["x"] != 15 {
		t.Fatalf("expected x=15 after update, got %d", found["x"])
	}
}
