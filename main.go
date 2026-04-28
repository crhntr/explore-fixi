package main

import (
	"bytes"
	"cmp"
	"io"
	"log"
	"net/http"
	"os"
	"text/template"
)

func main() {
	mux := http.NewServeMux()
	mux.Handle("GET /{$}", http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		renderTemplate(res, "index.gohtml", struct{}{}, "index.gohtml")
	}))
	mux.Handle("GET /example", http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		renderTemplate(res, "example", struct{}{}, "index.gohtml")
	}))
	addr := cmp.Or(os.Getenv("HTTP_ADDR"), ":"+cmp.Or(os.Getenv("PORT"), "8080"))
	log.Println("Starting http server", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalln(err)
	}
}

func renderTemplate(res http.ResponseWriter, name string, data any, templateFiles ...string) {
	templates, err := template.ParseFiles(templateFiles...)
	if err != nil {
		http.Error(res, "internal server error", http.StatusInternalServerError)
		return
	}
	var buf bytes.Buffer
	if err := templates.ExecuteTemplate(&buf, name, data); err != nil {
		http.Error(res, "internal server error", http.StatusInternalServerError)
		return
	}
	res.WriteHeader(http.StatusOK)
	_, _ = io.Copy(res, &buf)
}
