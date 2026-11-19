package guardian

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

const Version = "0.1.0"

const (
	lockTTL     = 30 * time.Second
	opTimeout   = 20 * time.Second
	peerTimeout = 3 * time.Second
	maxEvents   = 100
)

type peerState struct {
	Name    string         `json:"name"`
	Version string         `json:"version"`
	Redis   cluster.State  `json:"redis"`
	Restore *RestoreStatus `json:"restore,omitempty"`
}

type observation struct {
	At    time.Time
	Self  cluster.State
	Peers map[string]*peerState
}

func (v observation) live() map[string]string {
	ids := make(map[string]string)
	for name, p := range v.Peers {
		if p != nil && p.Redis.Ready {
			ids[p.Redis.ID] = name
		}
	}
	return ids
}

func (v observation) seed(self string) *peerState {
	var best *peerState
	for name, p := range v.Peers {
		if name == self || p == nil || !p.Redis.Assigned() {
			continue
		}
		if best == nil || p.Redis.Epoch() > best.Redis.Epoch() ||
			(p.Redis.Epoch() == best.Redis.Epoch() && name < best.Name) {
			best = p
		}
	}
	return best
}

type Event struct {
	Time   time.Time `json:"time"`
	Action string    `json:"action"`
	Detail string    `json:"detail"`
}

type Guardian struct {
	cfg     *Config
	redis   *cluster.Client
	sup     *cluster.Supervisor
	http    *http.Client
	lease   lease
	started time.Time
	store   *store

	life context.Context
	stop context.CancelFunc
	work sync.WaitGroup

	opMu sync.Mutex

	mu      sync.RWMutex
	view    observation
	names   map[string]string
	lastID  string
	issues  []string
	events  []Event
	backup  BackupStatus
	restore RestoreStatus

	saveMu      sync.Mutex
	restoreBusy atomic.Bool
	restoreKick chan struct{}

	since   map[string]time.Time
	touched map[string]bool
}

func New(config Config) (*Guardian, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	cfg := &config
	g := &Guardian{
		cfg:     cfg,
		redis:   cluster.NewClient("127.0.0.1:" + strconv.Itoa(cfg.redisPort())),
		http:    &http.Client{Timeout: peerTimeout},
		started: time.Now(),
		names:   make(map[string]string),
		events:  []Event{},
		since:   make(map[string]time.Time),
		touched: make(map[string]bool),

		restoreKick: make(chan struct{}, 1),
	}
	g.life, g.stop = context.WithCancel(context.Background())
	if cfg.BackupDSN != "" {
		st, err := openStore(cfg.BackupDSN, cfg.BackupCluster)
		if err != nil {
			return nil, err
		}
		g.store = st
	}
	if cfg.Managed {
		g.sup = &cluster.Supervisor{
			Bin:     "redis-server",
			Args:    redisArgs(cfg),
			DataDir: cfg.DataDir,
			Gate:    g.gate,
		}
	}
	return g, nil
}

func (g *Guardian) ListenAndRun(ctx context.Context) error {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(g.cfg.HTTPPort))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: g.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		if err := server.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			cancel()
		}
	}()
	g.Run(ctx)
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	server.Shutdown(shutdownCtx)
	select {
	case err := <-serveErr:
		return err
	default:
		return nil
	}
}

func (g *Guardian) Run(ctx context.Context) {
	slog.Info("redis-guardian running", "version", Version, "self", g.cfg.Self, "nodes", g.cfg.Nodes,
		"masters", g.cfg.masters(), "replicas_per_master", g.cfg.Replicas, "managed", g.cfg.Managed,
		"backups", g.store != nil)
	redisCtx, stopRedis := context.WithCancel(context.Background())
	var redisDone sync.WaitGroup
	if g.sup != nil {
		redisDone.Add(1)
		go func() {
			defer redisDone.Done()
			g.sup.Run(redisCtx)
		}()
	}

	if g.store != nil {
		g.work.Add(2)
		go func() {
			defer g.work.Done()
			g.backupLoop(ctx)
		}()
		go func() {
			defer g.work.Done()
			g.restoreLoop(ctx)
		}()
	}

	g.reconcile(ctx)

	slog.Info("redis-guardian shutting down")
	g.stop()
	g.work.Wait()
	if g.sup != nil {
		handoffCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		g.handoff(handoffCtx)
		cancel()
	}
	stopRedis()
	redisDone.Wait()
	if g.store != nil {
		g.store.close()
	}
}

func redisArgs(cfg *Config) []string {
	args := []string{
		"--port", strconv.Itoa(cfg.redisPort()),
		"--protected-mode", "no",
		"--dir", cfg.DataDir,
		"--save", "",
		"--appendonly", "no",
		"--repl-diskless-load", "on-empty-db",
		"--rdb-del-sync-files", "yes",
		"--cluster-enabled", "yes",
		"--cluster-config-file", "nodes.conf",
		"--cluster-node-timeout", "5000",
		"--cluster-require-full-coverage", "no",
	}
	if host, _, _ := splitHostPort(cfg.Self); !isIP(host) {
		args = append(args, "--cluster-announce-hostname", host)
	}
	return append(args, cfg.RedisArgs...)
}

func (g *Guardian) reconcile(ctx context.Context) {
	for {
		g.tick(ctx)
		wait := g.cfg.Interval + time.Duration(rand.Int64N(int64(g.cfg.Interval)/4+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (g *Guardian) observe(ctx context.Context) observation {
	v := observation{At: time.Now(), Peers: make(map[string]*peerState)}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, name := range g.cfg.Nodes {
		if name == g.cfg.Self {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ps peerState
			err := g.call(ctx, name, http.MethodGet, "/internal/state", nil, &ps)
			mu.Lock()
			defer mu.Unlock()
			if v.Peers[name] = nil; err == nil {
				ps.Name = name
				v.Peers[name] = &ps
			}
		}()
	}
	v.Self = g.redis.Inspect(ctx)
	wg.Wait()
	v.Peers[g.cfg.Self] = &peerState{Name: g.cfg.Self, Version: Version, Redis: v.Self, Restore: g.restoreStatus()}
	return v
}

func (g *Guardian) remember(v observation) {
	issues := checkTopology(g.cfg, v, g.deadFn(v))
	g.mu.Lock()
	changed := !slices.Equal(issues, g.issues)
	g.view, g.issues = v, issues
	for name, p := range v.Peers {
		if p != nil && p.Redis.Ready {
			g.names[p.Redis.ID] = name
		}
	}
	if v.Self.Ready {
		g.lastID = v.Self.ID
	}
	mentioned := map[string]bool{}
	for _, p := range v.Peers {
		if p != nil {
			for _, n := range p.Redis.Nodes {
				mentioned[n.ID] = true
			}
		}
	}
	for id := range g.names {
		if !mentioned[id] {
			delete(g.names, id)
		}
	}
	g.mu.Unlock()
	if changed {
		if len(issues) == 0 {
			slog.Info("topology check: healthy")
		} else {
			slog.Warn("topology check: issues found", "issues", issues)
		}
	}
}

func (g *Guardian) snapshot() (observation, []string) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.view, g.issues
}

func (g *Guardian) nodeName(n cluster.Node) string {
	if name := g.cfg.nameByAddr(n); name != "" {
		return name
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.names[n.ID]
}

func (g *Guardian) deadFn(v observation) func(cluster.Node) bool {
	live := v.live()
	return func(n cluster.Node) bool {
		if _, alive := live[n.ID]; alive {
			return false
		}
		return n.Failed() || n.Has("noaddr") || n.LinkState != "connected" || g.replaced(n, v)
	}
}

func (g *Guardian) replaced(n cluster.Node, v observation) bool {
	p := v.Peers[g.nodeName(n)]
	return p != nil && p.Redis.Ready && p.Redis.ID != n.ID
}

func (g *Guardian) event(action, detail string) {
	slog.Info(action, "detail", detail)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events = append(g.events, Event{time.Now(), action, detail})
	if len(g.events) > maxEvents {
		g.events = g.events[len(g.events)-maxEvents:]
	}
}

func (g *Guardian) stable(key string, cond bool, d time.Duration) bool {
	if !cond {
		delete(g.since, key)
		return false
	}
	g.touched[key] = true
	first, ok := g.since[key]
	if !ok {
		first = time.Now()
		g.since[key] = first
	}
	return time.Since(first) >= d
}

func (g *Guardian) endTick() {
	for key := range g.since {
		if !g.touched[key] {
			delete(g.since, key)
		}
	}
	clear(g.touched)
}
