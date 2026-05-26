package api

import (
	"github.com/valyala/fasthttp"
)

func (s *Server) handleHealth(ctx *fasthttp.RequestCtx) {
	readiness := s.IsReady()
	if !readiness {
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
	}
	ctx.SetContentType("application/json")
	if readiness {
		ctx.WriteString(`{"status":"ok","ready":true}`)
	} else {
		ctx.WriteString(`{"status":"not_ready","ready":false}`)
	}
}
