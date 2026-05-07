package main

import (
	"bytes"
	"cmp"
	"context"
	"embed"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	logger           *slog.Logger
	stat             statistics
}

func (s *Server) Increment() int64 {
	n := atomic.AddInt64(&s.count, 1)
	s.logger.Debug("increment count", slog.Int64("count", n))
	s.countUpdates <- n
	return n
}

func (s *Server) Index() int64 {
	s.logger.Debug("index handler called")
	return atomic.LoadInt64(&s.count)
}

func (s *Server) Updates(res http.ResponseWriter, req *http.Request) {
	updateListenerID := s.stat.incrementUpdateListenerCount()
	defer s.stat.decrementUpdateListenerCount()

	defer func() {
		_ = req.Body.Close()
	}()

	ctx := req.Context()
	rLogger := s.logger.With(slog.Int64("listener_id", updateListenerID))
	rLogger.Debug("updates", slog.Int64("count", atomic.LoadInt64(&s.count)))

	c, closeSub := s.countCoordinator.SubscribeLatest(1)
	defer closeSub()
	src, ok := sse.New(res, req, http.StatusOK)
	if !ok {
		rLogger.DebugContext(ctx, "new event source failed")
		http.Error(res, "failed to create sse connection", http.StatusBadRequest)
		return
	}

	buf := bytes.NewBuffer(nil)
	if initial, ok := s.countCoordinator.Latest(); ok {
		s.sendCount(ctx, rLogger, src, buf, 0, initial)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for eventID := int64(0); ; eventID++ {
		select {
		case <-ticker.C:
			rLogger.DebugContext(ctx, "send tick")
			if err := src.Message([]byte(time.Now().String()), sse.WithEvent(`{"target": "#tick", "swap": "innerHTML"}`)); err != nil {
				rLogger.DebugContext(ctx, "sse send error", err)
				return
			}
		case <-ctx.Done():
			rLogger.DebugContext(ctx, "close")
			return
		case update, open := <-c:
			if !open {
				rLogger.DebugContext(ctx, "stop sending count updates")
				_ = src.Message([]byte("stream closed"), sse.WithEvent("close"))
				return
			}
			s.sendCount(ctx, rLogger, src, buf, eventID, update)
		}
	}
}

func (s *Server) sendCount(ctx context.Context, rLogger *slog.Logger, src *sse.Response, buf *bytes.Buffer, eventID, update int64) {
	defer buf.Reset()
	rLogger.DebugContext(ctx, "send count update", slog.Int64("count", update), slog.Int64("event", eventID))
	if err := templates.ExecuteTemplate(buf, "count-text", update); err != nil {
		rLogger.DebugContext(ctx, "execute template error", err)
		return
	}
	if err := src.Message(buf.Bytes(), sse.WithRetry(time.Second), sse.WithID(strconv.FormatInt(eventID, 16))); err != nil {
		rLogger.DebugContext(ctx, "sse send error", err)
		return
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	TemplateRoutes(mux, s)
	mux.HandleFunc("GET /updates", s.Updates)
	return mux
}

func (TemplateRoutePaths) Updates() string { return "/updates" }

func addr() string { return cmp.Or(os.Getenv("HTTP_ADDR"), ":"+cmp.Or(os.Getenv("PORT"), "8080")) }

func main() {
	var (
		logLevel = new(slog.LevelVar)
	)
	logLevel.Set(slog.LevelInfo)
	flag.TextVar(logLevel, "log-level", logLevel, "structured log level")
	flag.Parse()
	c := make(chan int64, 1)
	defer close(c)
	s := &Server{
		countUpdates:     c,
		countCoordinator: broadcast.New(c),
		logger:           slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})),
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	srv := &http.Server{Addr: addr(), Handler: s.routes(), ErrorLog: slog.NewLogLogger(s.logger.Handler(), slog.LevelDebug)}
	s.logger.Info("starting http server", slog.String("addr", srv.Addr))
	go func() {
		<-ctx.Done()
		s.logger.Info("shutting down http server", slog.String("addr", srv.Addr))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			s.logger.Info("server shutdown error", err)
		}
	}()
	if err := srv.ListenAndServe(); err != nil {
		s.logger.Error(err.Error())
	}
}
