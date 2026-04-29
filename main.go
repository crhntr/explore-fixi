package main

import (
	"bytes"
	"cmp"
	"context"
	"embed"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/crhntr/channels/broadcast"
	"github.com/typelate/sse"
)

//go:embed *.gohtml
var templateFiles embed.FS

//go:generate go tool muxt generate --use-receiver-type=Server
var templates = template.Must(template.ParseFS(templateFiles, "*"))

type Server struct {
	count            int64
	countUpdates     chan int64
	countCoordinator *broadcast.Coordinator[int64]
	logger           *log.Logger
}

func (s *Server) Count() int64 {
	n := atomic.AddInt64(&s.count, 1)
	s.countUpdates <- n
	return n
}

func (s *Server) Index() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *Server) Updates(res http.ResponseWriter, req *http.Request) {
	c, closeSub := s.countCoordinator.SubscribeAll(1)
	defer closeSub()
	src, ok := sse.New(res, req, http.StatusOK)
	if !ok {
		s.logger.Println("new event source failed")
		http.Error(res, "failed to create sse connection", http.StatusBadRequest)
		return
	}
	buf := bytes.NewBuffer(nil)
	for update := range c {
		buf.Reset()
		if err := templates.ExecuteTemplate(buf, "hello", update); err != nil {
			s.logger.Println("execute template error", err)
			continue
		}
		if err := src.Message(buf.Bytes()); err != nil {
			s.logger.Println("sse send error", err)
			continue
		}
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	TemplateRoutes(mux, s)
	mux.HandleFunc("GET /updates", s.Updates)
	return mux
}

func addr() string { return cmp.Or(os.Getenv("HTTP_ADDR"), ":"+cmp.Or(os.Getenv("PORT"), "8080")) }

func main() {
	c := make(chan int64, 1)
	defer close(c)
	s := &Server{
		countUpdates:     c,
		countCoordinator: broadcast.New(c),
		logger:           log.Default(),
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	srv := &http.Server{Addr: addr(), Handler: s.routes(), ErrorLog: s.logger}
	s.logger.Println("starting http server", srv.Addr)
	go func() {
		<-ctx.Done()
		s.logger.Println("shutting down http server", srv.Addr)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			s.logger.Println("server shutdown error", err)
		}
	}()
	if err := srv.ListenAndServe(); err != nil {
		s.logger.Fatalln(err)
	}
}
