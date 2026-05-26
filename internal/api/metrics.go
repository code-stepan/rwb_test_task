package api

import "github.com/prometheus/client_golang/prometheus"

var (
	EventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trending_search_events_total",
			Help: "Total number of processed events.",
		},
		[]string{"status"},
	)

	ApiRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "trending_search_api_requests_total",
			Help: "Total number of API requests.",
		},
		[]string{"endpoint", "status"},
	)

	RecalcDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "trending_search_recalc_duration_seconds",
			Help:    "Duration of top recalculation.",
			Buckets: prometheus.DefBuckets,
		},
	)

	MemoryUniqueQueries = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "trending_search_memory_unique_queries",
			Help: "Number of unique queries in the global counter.",
		},
	)

	TopCacheTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "trending_search_top_cache_timestamp",
			Help: "Unix timestamp of the last top recalculation.",
		},
	)
)

func init() {
	prometheus.MustRegister(EventsTotal)
	prometheus.MustRegister(ApiRequestsTotal)
	prometheus.MustRegister(RecalcDuration)
	prometheus.MustRegister(MemoryUniqueQueries)
	prometheus.MustRegister(TopCacheTimestamp)
}

