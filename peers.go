package guardian

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
)

const tokenHeader = "X-Guardian-Token"

func (g *Guardian) call(ctx context.Context, name, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, g.cfg.peerURL(name)+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(tokenHeader, g.cfg.Token)
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %s: %s", name, path, resp.Status, bytes.TrimSpace(msg))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type failoverRequest struct {
	Name string `json:"name"`
	ID   string `json:"id,omitempty"`
	Mode string `json:"mode"`
}

type failoverResponse struct {
	OK      bool   `json:"ok"`
	Started bool   `json:"started,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

func (g *Guardian) askFailover(ctx context.Context, req failoverRequest) (pending, unreachable []string, started bool) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, name := range g.cfg.Nodes {
		if name == g.cfg.Self {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp failoverResponse
			err := g.call(ctx, name, http.MethodPost, "/internal/failover", req, &resp)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				unreachable = append(unreachable, name)
			} else if !resp.OK {
				pending = append(pending, name)
				started = started || resp.Started
			}
		}()
	}
	wg.Wait()
	return pending, unreachable, started
}

func resolve(ctx context.Context, name string) (string, int, error) {
	host, port, err := splitHostPort(name)
	if err != nil || isIP(host) {
		return host, port, err
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", 0, err
	}
	for _, a := range addrs {
		if a.IP.To4() != nil {
			return a.IP.String(), port, nil
		}
	}
	return addrs[0].IP.String(), port, nil
}

func isIP(host string) bool { return net.ParseIP(host) != nil }
