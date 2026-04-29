package main

import (
	"bytes"
	"cmp"
	"embed"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"text/template"

	"github.com/crhntr/channels/broadcast"
	"github.com/typelate/sse"
)

//go:embed *.gohtml
var templateFiles embed.FS

//go:generate go tool muxt generate --use-receiver-type=Server
var templates = template.Must(template.ParseFS(templateFiles, "*"))

type Server struct {
	count   int64
	updates chan<- int64
}

func (s *Server) Count() int64 {
	n := atomic.AddInt64(&s.count, 1)
	s.updates <- n
	return n
}

func main() {
	mux := http.NewServeMux()
	s := new(Server)
	updates := make(chan int64)
	s.updates = updates
	bc := broadcast.New(updates)
	TemplateRoutes(mux, s)

	mux.HandleFunc("GET /feed", func(res http.ResponseWriter, req *http.Request) {
		c, closeSub := bc.SubscribeAll(5)
		defer closeSub()
		src, ok := sse.New(res, req, http.StatusOK)
		if !ok {
			log.Println("new event source failed")
			http.Error(res, "failed to create sse connection", http.StatusBadRequest)
			return
		}
		buf := bytes.NewBuffer(nil)
		for update := range c {
			buf.Reset()
			if err := templates.ExecuteTemplate(buf, "history-entree", update); err != nil {
				log.Println("execute history-entree error", err)
				continue
			}
			if err := src.Message(buf.Bytes()); err != nil {
				log.Println("failed to send message", err)
				continue
			}
		}
	})

	addr := cmp.Or(os.Getenv("HTTP_ADDR"), ":"+cmp.Or(os.Getenv("PORT"), "8080"))
	log.Println("Starting http server", addr)
	if err := http.ListenAndServe(addr, http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		log.Println("Received request", req.Method, req.URL.Path)
		mux.ServeHTTP(res, req)
	})); err != nil {
		log.Fatalln(err)
	}
}
