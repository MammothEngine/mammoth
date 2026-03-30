package main

import (
	"encoding/json"
	"net/http"
	"time"
)

type healthServer struct {
	startTime time.Time
	version   string
}

func (s *healthServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status := map[string]any{
		"status": "ok",
		"uptime": time.Since(s.startTime).Round(time.Second).String(),
		"version": s.version,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
