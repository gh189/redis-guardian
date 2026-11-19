package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	guardian "github.com/gh189/redis-guardian"
)

func main() {
	var level slog.Level
	level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL")))
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	cfg, err := guardian.LoadConfig()
	if err != nil {
		fatal("invalid configuration", err)
	}
	g, err := guardian.New(cfg)
	if err != nil {
		fatal("invalid configuration", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := g.ListenAndRun(ctx); err != nil {
		fatal("redis-guardian failed", err)
	}
}

func fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}
