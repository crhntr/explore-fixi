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

func (s *Server) Index() int64 {
	return atomic.LoadInt64(&s.count)
}

func main() {
	mux := http.NewServeMux()
	s := new(Server)
	updates := make(chan int64, 5)
	s.updates = updates
	bc := broadcast.New(updates)
	TemplateRoutes(mux, s)

	mux.HandleFunc("GET /updates", func(res http.ResponseWriter, req *http.Request) {
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
			if err := templates.ExecuteTemplate(buf, "hello", update); err != nil {
				log.Println("execute template error", err)
				continue
			}
			if err := src.Message(buf.Bytes()); err != nil {
				log.Println("sse send error", err)
				continue
			}
		}
	})

	addr := cmp.Or(os.Getenv("HTTP_ADDR"), ":"+cmp.Or(os.Getenv("PORT"), "8080"))
	log.Println("Starting http server", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalln(err)
	}
}
