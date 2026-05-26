package api

import (
	"encoding/json"

	"github.com/valyala/fasthttp"
)

type stoplistRequest struct {
	Word      string `json:"word"`
	MatchType string `json:"match_type"`
}

func (s *Server) triggerRecalc() {
	if s.recalcNotify != nil {
		select {
		case s.recalcNotify <- struct{}{}:
		default:
		}
	}
}

func (s *Server) handleAddStoplist(ctx *fasthttp.RequestCtx) {
	var req stoplistRequest
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString(`{"error":"invalid json"}`)
		ApiRequestsTotal.WithLabelValues("stoplist_add", "400").Inc()
		return
	}

	if req.MatchType == "contains" {
		s.stoplist.AddPattern(req.Word)
	} else {
		s.stoplist.AddExact(req.Word)
	}
	s.triggerRecalc()

	ctx.SetStatusCode(fasthttp.StatusCreated)
	ctx.SetContentType("application/json")
	ctx.WriteString(`{"status":"added"}`)
	ApiRequestsTotal.WithLabelValues("stoplist_add", "201").Inc()
}

func (s *Server) handleRemoveStoplist(ctx *fasthttp.RequestCtx) {
	word := ctx.UserValue("word").(string)
	s.stoplist.RemoveExact(word)
	s.triggerRecalc()
	ctx.SetContentType("application/json")
	ctx.WriteString(`{"status":"removed"}`)
	ApiRequestsTotal.WithLabelValues("stoplist_remove", "200").Inc()
}

func (s *Server) handleRemovePattern(ctx *fasthttp.RequestCtx) {
	word := ctx.UserValue("word").(string)
	s.stoplist.RemovePattern(word)
	s.triggerRecalc()
	ctx.SetContentType("application/json")
	ctx.WriteString(`{"status":"removed"}`)
	ApiRequestsTotal.WithLabelValues("stoplist_remove_pattern", "200").Inc()
}

func (s *Server) handleListStoplist(ctx *fasthttp.RequestCtx) {
	exact, patterns := s.stoplist.List()
	out, _ := json.Marshal(map[string]any{
		"exact":    exact,
		"patterns": patterns,
	})
	ctx.SetContentType("application/json")
	ctx.Write(out)
	ApiRequestsTotal.WithLabelValues("stoplist_list", "200").Inc()
}
