package api

import (
	"sync/atomic"

	"github.com/fasthttp/router"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"

	"github.com/wildberries/trending-search-service/internal/config"
	"github.com/wildberries/trending-search-service/internal/stoplist"
	"github.com/wildberries/trending-search-service/internal/storage"
)

type Server struct {
	cfg      *config.Config
	cache    *storage.TrendCache
	stoplist *stoplist.Manager
	topHeap  *storage.TopHeap
	counter  *storage.GlobalCounter
	router   *router.Router

	ready        atomic.Bool
	recalcNotify chan<- struct{}
}

func New(cfg *config.Config, cache *storage.TrendCache, sl *stoplist.Manager, topHeap *storage.TopHeap, counter *storage.GlobalCounter, recalcNotify chan<- struct{}) *Server {
	s := &Server{
		cfg:          cfg,
		cache:        cache,
		stoplist:     sl,
		topHeap:      topHeap,
		counter:      counter,
		recalcNotify: recalcNotify,
	}
	s.setupRoutes()
	return s
}

func (s *Server) SetReady() {
	s.ready.Store(true)
}

func (s *Server) IsReady() bool {
	return s.ready.Load()
}

func (s *Server) setupRoutes() {
	r := router.New()
	r.GET("/api/v1/trends", s.handleTrends)
	r.POST("/api/v1/stoplist", s.handleAddStoplist)
	r.DELETE("/api/v1/stoplist/{word}", s.handleRemoveStoplist)
	r.DELETE("/api/v1/stoplist/pattern/{word}", s.handleRemovePattern)
	r.GET("/api/v1/stoplist", s.handleListStoplist)
	r.GET("/health", s.handleHealth)
	r.GET("/metrics", fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler()))
	s.router = r
}

func (s *Server) Listen() error {
	return fasthttp.ListenAndServe(":"+s.cfg.HTTPPort, s.router.Handler)
}
