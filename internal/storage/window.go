package storage

import (
	"sync"
	"time"
)

type TimeSlot struct {
	counts    map[string]uint32 // запрос -> количество
	startTime int64             // unix nano начала слота
}

type Window struct {
	slots     []TimeSlot
	slotCount int
	slotDur   int64 // длительность слота в наносекундах
	mu        sync.RWMutex
}

func NewWindow(slotCount int, slotDuration time.Duration) *Window {
	slots := make([]TimeSlot, slotCount)
	for i := range slots {
		slots[i] = TimeSlot{counts: make(map[string]uint32)}
	}
	return &Window{
		slots:     slots,
		slotCount: slotCount,
		slotDur:   int64(slotDuration),
	}
}

func (w *Window) Add(query string, timestamp int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	slotIdx := int((timestamp / w.slotDur) % int64(w.slotCount))
	expectedStart := (timestamp / w.slotDur) * w.slotDur

	if w.slots[slotIdx].startTime != expectedStart {
		// Слот устарел — очищаем
		w.slots[slotIdx] = TimeSlot{
			counts:    make(map[string]uint32),
			startTime: expectedStart,
		}
	}

	w.slots[slotIdx].counts[query]++
}

func (w *Window) Rotate(now int64) map[string]uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()

	cutoff := now - int64(w.slotCount)*w.slotDur
	evicted := make(map[string]uint32)

	for i := range w.slots {
		if w.slots[i].startTime != 0 && w.slots[i].startTime < cutoff {
			for q, c := range w.slots[i].counts {
				evicted[q] += c
			}
			w.slots[i] = TimeSlot{counts: make(map[string]uint32)}
		}
	}
	return evicted
}
