package guardian

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type lease struct {
	mu      sync.Mutex
	holder  string
	token   string
	expires time.Time
}

func (l *lease) grant(holder, token string, ttl time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.token != "" && l.token != token && time.Now().Before(l.expires) {
		return false
	}
	l.holder, l.token, l.expires = holder, token, time.Now().Add(ttl)
	return true
}

func (l *lease) release(token string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.token == token {
		l.holder, l.token = "", ""
	}
}

type lockRequest struct {
	Holder string `json:"holder"`
	Token  string `json:"token"`
}

type lockResponse struct {
	Granted bool `json:"granted"`
}

func (g *Guardian) withLock(ctx context.Context, what string, fn func(ctx context.Context) error) {
	buf := make([]byte, 8)
	rand.Read(buf)
	req := lockRequest{Holder: g.cfg.Self, Token: hex.EncodeToString(buf)}

	votes := g.requestLeases(ctx, "/internal/lock", req)
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), peerTimeout)
		defer cancel()
		g.requestLeases(rctx, "/internal/unlock", req)
	}()
	if votes < g.cfg.quorum() {
		slog.Info("topology lock not acquired, will retry", "for", what, "votes", votes, "quorum", g.cfg.quorum())
		return
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	g.opMu.Lock()
	err := fn(ctx)
	g.opMu.Unlock()
	if err != nil {
		g.event(what+"-failed", err.Error())
	}
}

func (g *Guardian) requestLeases(ctx context.Context, path string, req lockRequest) int {
	var wg sync.WaitGroup
	var mu sync.Mutex
	votes := 0
	for _, name := range g.cfg.Nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp lockResponse
			if name == g.cfg.Self {
				resp = g.handleLease(path, req)
			} else if err := g.call(ctx, name, http.MethodPost, path, req, &resp); err != nil {
				return
			}
			if resp.Granted {
				mu.Lock()
				votes += g.cfg.weight(name)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return votes
}

func (g *Guardian) handleLease(path string, req lockRequest) lockResponse {
	if path == "/internal/unlock" {
		g.lease.release(req.Token)
		return lockResponse{}
	}
	return lockResponse{Granted: g.lease.grant(req.Holder, req.Token, lockTTL)}
}
