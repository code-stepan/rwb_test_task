package api

import (
	"encoding/json"
	"strconv"

	"github.com/valyala/fasthttp"

	"github.com/wildberries/trending-search-service/internal/storage"
)

func (s *Server) handleTrends(ctx *fasthttp.RequestCtx) {
	limitStr := string(ctx.QueryArgs().Peek("limit"))
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 || limit > s.cfg.TopN {
		limit = s.cfg.TopN
	}

	if jsonBytes := s.cache.LoadJSON(limit); jsonBytes != nil {
		ctx.SetContentType("application/json")
		ctx.Write(jsonBytes)
		ApiRequestsTotal.WithLabelValues("trends", "200").Inc()
		return
	}

	data := s.cache.Load()
	if data == nil {
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		ctx.SetContentType("application/json")
		ctx.WriteString(`{"error":"not ready"}`)
		ApiRequestsTotal.WithLabelValues("trends", "503").Inc()
		return
	}

	n := limit
	if n > len(data.Trends) {
		n = len(data.Trends)
	}
	out, _ := json.Marshal(struct {
		UpdatedAt     string              `json:"updated_at"`
		WindowSeconds int                 `json:"window_seconds"`
		TotalQueries  int                 `json:"total_queries"`
		Trends        []storage.TrendItem `json:"trends"`
	}{
		UpdatedAt:     data.UpdatedAt.Format("2006-01-02T15:04:05Z"),
		WindowSeconds: data.WindowSeconds,
		TotalQueries:  data.TotalQueries,
		Trends:        data.Trends[:n],
	})
	ctx.SetContentType("application/json")
	ctx.Write(out)
	ApiRequestsTotal.WithLabelValues("trends", "200").Inc()
}
