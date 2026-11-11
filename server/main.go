package main

import (
	"embed"
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"time"
)

//go:embed ui
var uiFiles embed.FS

func main() {
	addr := flag.String("addr", ":4100", "listen address")
	flag.Parse()

	store := NewStore()
	ui, _ := fs.Sub(uiFiles, "ui")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", func(w http.ResponseWriter, r *http.Request) {
		var p IngestPayload
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		store.Ingest(p)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /api/query", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().Unix()
		from := queryInt(r, "from", now-15*60)
		to := queryInt(r, "to", now)
		q, err := Parse(r.URL.Query().Get("q"))
		if err == nil {
			var res *Result
			if res, err = Execute(store, q, from, to); err == nil {
				writeJSON(w, http.StatusOK, res)
				return
			}
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	})
	mux.HandleFunc("GET /api/functions", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, FunctionNames())
	})
	// Values of one label, for dashboard variables. Other query parameters filter by equality,
	// e.g. /api/labels/stage/values?pipeline=orders.
	mux.HandleFunc("GET /api/labels/{name}/values", func(w http.ResponseWriter, r *http.Request) {
		var ms []Matcher
		for k, vs := range r.URL.Query() {
			ms = append(ms, Matcher{Label: k, Op: "=", Value: vs[0]})
		}
		writeJSON(w, http.StatusOK, store.LabelValues(r.PathValue("name"), ms))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /", http.FileServerFS(ui))

	log.Printf("Weir server listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

func queryInt(r *http.Request, name string, def int64) int64 {
	if v, err := strconv.ParseInt(r.URL.Query().Get(name), 10, 64); err == nil {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
