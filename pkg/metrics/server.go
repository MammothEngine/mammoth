package metrics

import "net/http"

// Handler returns an http.HandlerFunc that serves Prometheus metrics.
func Handler(reg *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		WritePrometheus(w, reg.All())
	}
}
