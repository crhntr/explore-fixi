package main

import (
	"cmp"
	"embed"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"text/template"
)

//go:embed *.gohtml
var templateFiles embed.FS

//go:generate go tool muxt generate --use-receiver-type=Server
var templates = template.Must(template.ParseFS(templateFiles, "*"))

type Server struct {
	count int64
}

func (s *Server) Count() int64 {
	return atomic.AddInt64(&s.count, 1)
}

func main() {
	mux := http.NewServeMux()
	s := new(Server)
	TemplateRoutes(mux, s)
	addr := cmp.Or(os.Getenv("HTTP_ADDR"), ":"+cmp.Or(os.Getenv("PORT"), "8080"))
	log.Println("Starting http server", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalln(err)
	}
}
