package stoplist

import (
	"strings"
	"sync"
)

type Manager struct {
	exact    map[string]struct{} // точные совпадения
	patterns []string            // паттерны для поиска подстроки
	mu       sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		exact: make(map[string]struct{}),
	}
}

func (m *Manager) AddExact(word string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.exact[strings.ToLower(strings.TrimSpace(word))] = struct{}{}
}

func (m *Manager) AddPattern(pattern string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, strings.ToLower(pattern))
}

func (m *Manager) RemoveExact(word string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.exact, strings.ToLower(strings.TrimSpace(word)))
}

func (m *Manager) RemovePattern(pattern string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pat := strings.ToLower(pattern)
	for i, p := range m.patterns {
		if p == pat {
			m.patterns = append(m.patterns[:i], m.patterns[i+1:]...)
			return
		}
	}
}

func (m *Manager) IsBlocked(query string) bool {
	q := strings.ToLower(query)
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.exact[q]; ok {
		return true
	}
	for _, p := range m.patterns {
		if strings.Contains(q, p) {
			return true
		}
	}
	return false
}

func (m *Manager) List() ([]string, []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exact := make([]string, 0, len(m.exact))
	for k := range m.exact {
		exact = append(exact, k)
	}
	patterns := make([]string, len(m.patterns))
	copy(patterns, m.patterns)
	return exact, patterns
}
