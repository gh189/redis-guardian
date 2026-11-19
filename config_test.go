package guardian

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	t.Setenv("REDIS_NODES", "redis-2.redis:6379, redis-0.redis:6379,redis-1.redis:6379,redis-5.redis:6379,redis-4.redis:6379,redis-3.redis:6379")
	t.Setenv("REDIS_SELF", "redis-3.redis:6379")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Nodes[0] != "redis-0.redis:6379" || len(cfg.Nodes) != 6 {
		t.Errorf("nodes must be sorted: %v", cfg.Nodes)
	}
	if cfg.masters() != 3 || cfg.redisPort() != 6379 || !cfg.Managed {
		t.Errorf("unexpected defaults: masters=%d port=%d managed=%v", cfg.masters(), cfg.redisPort(), cfg.Managed)
	}
	if got := cfg.peerURL("redis-1.redis:6379"); got != "http://redis-1.redis:80" {
		t.Errorf("peerURL = %q", got)
	}
}

func TestLoadConfigRejectsBadInput(t *testing.T) {
	t.Setenv("REDIS_NODES", "")
	if _, err := LoadConfig(); err == nil {
		t.Error("missing REDIS_NODES must fail")
	}
	t.Setenv("REDIS_NODES", "a:6379,b")
	if _, err := LoadConfig(); err == nil {
		t.Error("entry without port must fail")
	}
	t.Setenv("REDIS_NODES", "a:6379,b:6379")
	t.Setenv("REDIS_SELF", "c:6379")
	if _, err := LoadConfig(); err == nil {
		t.Error("REDIS_SELF outside REDIS_NODES must fail")
	}
	t.Setenv("REDIS_NODES", "a:6379,a:6379,b:6379")
	t.Setenv("REDIS_SELF", "a:6379")
	if _, err := LoadConfig(); err == nil {
		t.Error("a node listed twice would vote twice and must be rejected")
	}
}

func TestNewCompletesConfig(t *testing.T) {
	nodes := []string{"b:6379", "a:6379"}
	g, err := New(Config{Nodes: nodes, Self: "a:6379"})
	if err != nil {
		t.Fatal(err)
	}
	if nodes[0] != "b:6379" {
		t.Error("New must not reorder the caller's slice")
	}
	if g.cfg.Nodes[0] != "a:6379" || g.cfg.HTTPPort != 80 || g.cfg.Interval != 3*time.Second || g.cfg.Grace != 15*time.Second {
		t.Errorf("config not completed: %+v", g.cfg)
	}
	if g.sup != nil {
		t.Error("a zero Config does not manage Redis; DefaultConfig does")
	}
	g, err = New(Config{Nodes: nodes, Self: "a:6379", DataDir: "redis-data"})
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(g.cfg.DataDir) || filepath.Base(g.cfg.DataDir) != "redis-data" {
		t.Errorf("a relative data dir must become absolute, got %q", g.cfg.DataDir)
	}
	if _, err := New(Config{}); err == nil {
		t.Error("a config without nodes must be rejected")
	}
}

func TestQuorumBreaksTies(t *testing.T) {
	cfg := &Config{Nodes: []string{"a:1", "b:1", "c:1", "d:1", "e:1", "f:1"}}
	votes := func(names ...string) (total int) {
		for _, n := range names {
			total += cfg.weight(n)
		}
		return
	}
	if cfg.quorum() != 4 {
		t.Fatalf("quorum = %d, want 4", cfg.quorum())
	}
	if votes("a:1", "b:1", "c:1") < cfg.quorum() {
		t.Error("the half holding the first node must have a quorum")
	}
	if votes("d:1", "e:1", "f:1") >= cfg.quorum() {
		t.Error("the other half must not have a quorum")
	}

	odd := &Config{Nodes: []string{"a:1", "b:1", "c:1"}}
	if odd.quorum() != 2 || odd.weight("a:1") != 1 {
		t.Errorf("odd node count needs a plain majority, got quorum %d", odd.quorum())
	}
}
