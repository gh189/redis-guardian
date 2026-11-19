package guardian_test

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	guardian "github.com/gh189/redis-guardian"
)

func Example() {
	cfg := guardian.DefaultConfig()
	cfg.Nodes = []string{
		"redis-0:6379", "redis-1:6379", "redis-2:6379",
		"redis-3:6379", "redis-4:6379", "redis-5:6379",
	}
	g, err := guardian.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := g.ListenAndRun(ctx); err != nil {
		log.Fatal(err)
	}
}

func ExampleGuardian_Handler() {
	cfg, err := guardian.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}
	g, err := guardian.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", g.Handler())
	mux.HandleFunc("GET /my/status", func(w http.ResponseWriter, r *http.Request) {
		if h := g.Health(); !h.Ready {
			http.Error(w, "redis cluster is not ready", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})
	go http.ListenAndServe(":80", mux)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	g.Run(ctx)
}
