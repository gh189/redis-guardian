package guardian

import (
	"encoding/json"
	"net/http"
)

func (g *Guardian) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"name":    "redis-guardian",
			"version": Version,
			"endpoints": []string{
				"/api/v1/node", "/api/v1/cluster", "/api/v1/health", "/api/v1/events", "/api/v1/backups", "/healthz", "/readyz",
			},
		})
	})
	mux.HandleFunc("GET /api/v1/node", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, g.Node(r.Context()))
	})
	mux.HandleFunc("GET /api/v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, g.Cluster())
	})
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, g.Health())
	})
	mux.HandleFunc("GET /api/v1/events", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"events": g.Events()})
	})
	mux.HandleFunc("GET /api/v1/backups", func(w http.ResponseWriter, r *http.Request) {
		backups, err := g.Backups(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"backups": backups})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		h := g.Health()
		code := http.StatusOK
		if !h.Ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, h)
	})

	mux.HandleFunc("GET /internal/state", g.internal(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, peerState{Name: g.cfg.Self, Version: Version, Redis: g.redis.Inspect(r.Context()),
			Restore: g.restoreStatus()})
	}))
	lease := g.internal(func(w http.ResponseWriter, r *http.Request) {
		var req lockRequest
		if readJSON(w, r, &req) {
			writeJSON(w, http.StatusOK, g.handleLease(r.URL.Path, req))
		}
	})
	mux.HandleFunc("POST /internal/lock", lease)
	mux.HandleFunc("POST /internal/unlock", lease)
	mux.HandleFunc("POST /internal/failover", g.internal(func(w http.ResponseWriter, r *http.Request) {
		var req failoverRequest
		if readJSON(w, r, &req) {
			writeJSON(w, http.StatusOK, g.handleFailover(r.Context(), req))
		}
	}))
	mux.HandleFunc("POST /internal/backup/capture", g.internal(func(w http.ResponseWriter, r *http.Request) {
		var req captureRequest
		if !readJSON(w, r, &req) {
			return
		}
		if g.store == nil {
			http.Error(w, "backups are not configured on this node", http.StatusConflict)
			return
		}
		go g.capture(req)
		writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
	}))
	mux.HandleFunc("POST /internal/bootstrap", g.internal(func(w http.ResponseWriter, r *http.Request) {
		var req bootstrapRequest
		if !readJSON(w, r, &req) {
			return
		}
		g.opMu.Lock()
		err := g.applyBootstrap(r.Context(), req)
		g.opMu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		g.kickRestore()
		g.event("bootstrap", "became master on request of the bootstrap initiator")
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}))
	return mux
}

func (g *Guardian) internal(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if g.cfg.Token != "" && r.Header.Get(tokenHeader) != g.cfg.Token {
			http.Error(w, "invalid guardian token", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(v); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
