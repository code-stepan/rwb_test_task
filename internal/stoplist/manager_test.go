package stoplist

import "testing"

func TestStopListExact(t *testing.T) {
	m := NewManager()
	m.AddExact("xxx")

	if !m.IsBlocked("xxx") {
		t.Fatal("exact match should block")
	}
	if m.IsBlocked("xxxy") {
		t.Fatal("partial should not block exact")
	}
}

func TestStopListPattern(t *testing.T) {
	m := NewManager()
	m.AddPattern("порно")

	if !m.IsBlocked("фильмы порно онлайн") {
		t.Fatal("substring should block")
	}
	if m.IsBlocked("нормальный фильм") {
		t.Fatal("non-matching should pass")
	}
}

func TestStopListRemove(t *testing.T) {
	m := NewManager()
	m.AddExact("bad")
	if !m.IsBlocked("bad") {
		t.Fatal("should block before removal")
	}
	m.RemoveExact("bad")
	if m.IsBlocked("bad") {
		t.Fatal("should not block after removal")
	}
}
