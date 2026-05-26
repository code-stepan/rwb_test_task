package storage

import (
	"encoding/json"
	"sync/atomic"
	"time"
)

type TrendItem struct {
	Query string `json:"query"`
	Count uint64 `json:"count"`
	Rank  int    `json:"rank"`
}

type TrendsData struct {
	UpdatedAt     time.Time   `json:"updated_at"`
	WindowSeconds int         `json:"window_seconds"`
	TotalQueries  int         `json:"total_queries"`
	Trends        []TrendItem `json:"trends"`
}

type TrendCache struct {
	data    atomic.Pointer[TrendsData]
	jsonBin [101]*atomic.Pointer[[]byte]
}

func NewTrendCache() *TrendCache {
	tc := &TrendCache{}
	for i := range tc.jsonBin {
		tc.jsonBin[i] = &atomic.Pointer[[]byte]{}
	}
	return tc
}

func (tc *TrendCache) Store(data *TrendsData) {
	tc.data.Store(data)

	n := len(data.Trends)
	for limit := 1; limit <= n && limit < len(tc.jsonBin); limit++ {
		cp := *data
		cp.Trends = cp.Trends[:limit]
		b, err := json.Marshal(&cp)
		if err != nil {
			continue
		}
		tc.jsonBin[limit].Store(&b)
	}
}

func (tc *TrendCache) Load() *TrendsData {
	return tc.data.Load()
}

func (tc *TrendCache) LoadJSON(limit int) []byte {
	if limit < 0 || limit >= len(tc.jsonBin) {
		return nil
	}
	p := tc.jsonBin[limit].Load()
	if p == nil {
		return nil
	}
	return *p
}
