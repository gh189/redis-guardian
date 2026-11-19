package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

type Client struct {
	rdb *redis.Client
}

func NewClient(addr string) *Client {
	return newClient("tcp", addr, 3*time.Second)
}

func NewRestoreClient(network, addr string) *Client {
	return newClient(network, addr, 2*time.Minute)
}

func newClient(network, addr string, timeout time.Duration) *Client {
	return &Client{rdb: redis.NewClient(&redis.Options{
		Network:               network,
		Addr:                  addr,
		Protocol:              2,
		DisableIdentity:       true,
		MaxRetries:            -1,
		DialerRetries:         1,
		DialTimeout:           time.Second,
		ReadTimeout:           timeout,
		WriteTimeout:          timeout,
		PoolSize:              4,
		ContextTimeoutEnabled: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})}
}

func (c *Client) Close() error { return c.rdb.Close() }

type State struct {
	Ready        bool              `json:"ready"`
	Loading      bool              `json:"loading,omitempty"`
	Error        string            `json:"error,omitempty"`
	ID           string            `json:"node_id,omitempty"`
	Role         string            `json:"role,omitempty"`
	MasterID     string            `json:"master_id,omitempty"`
	Slots        []SlotRange       `json:"slots,omitempty"`
	Keys         int64             `json:"keys"`
	MasterLinkUp bool              `json:"master_link_up"`
	NotSynced    bool              `json:"not_synced,omitempty"`
	ReplOffset   int64             `json:"repl_offset"`
	ClusterInfo  map[string]string `json:"cluster_info,omitempty"`
	Nodes        []Node            `json:"nodes,omitempty"`
	Info         map[string]string `json:"-"`
}

func (s *State) Node(id string) *Node {
	for i := range s.Nodes {
		if s.Nodes[i].ID == id {
			return &s.Nodes[i]
		}
	}
	return nil
}

func (s *State) Assigned() bool {
	return s.Ready && (len(s.Slots) > 0 || s.Role == "replica")
}

func (s *State) Unassigned() bool { return s.Ready && !s.Assigned() }

func (s *State) Epoch() int64 {
	e, _ := strconv.ParseInt(s.ClusterInfo["cluster_current_epoch"], 10, 64)
	return e
}

func (c *Client) Inspect(ctx context.Context) State {
	var s State
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		s.Error = err.Error()
		s.Loading = strings.HasPrefix(s.Error, "LOADING")
		return s
	}
	info, err := c.rdb.Info(ctx).Result()
	if err != nil {
		s.Error = err.Error()
		return s
	}
	s.Info = parseInfo(info)
	if s.Info["cluster_enabled"] != "1" {
		s.Error = "redis is not running in cluster mode"
		return s
	}
	clusterInfo, err := c.rdb.ClusterInfo(ctx).Result()
	if err != nil {
		s.Error = err.Error()
		return s
	}
	s.ClusterInfo = parseInfo(clusterInfo)
	raw, err := c.rdb.ClusterNodes(ctx).Result()
	if err != nil {
		s.Error = err.Error()
		return s
	}
	s.Nodes = ParseNodes(raw)
	for _, n := range s.Nodes {
		if n.Has("myself") {
			s.ID, s.MasterID, s.Slots = n.ID, n.MasterID, n.Slots
			s.Role = "replica"
			if n.IsMaster() {
				s.Role = "master"
			}
		}
	}
	s.Keys, _ = c.rdb.DBSize(ctx).Result()
	offsetKey := "master_repl_offset"
	if s.Role == "replica" {
		offsetKey = "slave_repl_offset"
		s.MasterLinkUp = s.Info["master_link_status"] == "up"
		s.NotSynced = !s.MasterLinkUp && s.Info["master_link_down_since_seconds"] == "-1"
	}
	s.ReplOffset, _ = strconv.ParseInt(s.Info[offsetKey], 10, 64)
	s.Ready = s.ID != ""
	return s
}

func parseInfo(raw string) map[string]string {
	m := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if k, v, ok := strings.Cut(line, ":"); ok && !strings.HasPrefix(line, "#") {
			m[k] = v
		}
	}
	return m
}

func (c *Client) Meet(ctx context.Context, ip string, port int) error {
	return c.rdb.ClusterMeet(ctx, ip, strconv.Itoa(port)).Err()
}

func (c *Client) Replicate(ctx context.Context, masterID string) error {
	return c.rdb.ClusterReplicate(ctx, masterID).Err()
}

func (c *Client) Forget(ctx context.Context, id string) error {
	return c.rdb.ClusterForget(ctx, id).Err()
}

func (c *Client) Failover(ctx context.Context, mode string) error {
	args := []any{"cluster", "failover"}
	if mode != "" {
		args = append(args, mode)
	}
	return c.rdb.Do(ctx, args...).Err()
}

func (c *Client) AddSlots(ctx context.Context, ranges []SlotRange) error {
	return c.slotsCmd(ctx, "addslotsrange", ranges)
}

func (c *Client) DelSlots(ctx context.Context, ranges []SlotRange) error {
	return c.slotsCmd(ctx, "delslotsrange", ranges)
}

func (c *Client) slotsCmd(ctx context.Context, sub string, ranges []SlotRange) error {
	if len(ranges) == 0 {
		return nil
	}
	args := []any{"cluster", sub}
	for _, r := range ranges {
		args = append(args, r.Start, r.End)
	}
	return c.rdb.Do(ctx, args...).Err()
}

func (c *Client) SetConfigEpoch(ctx context.Context, epoch int) error {
	return c.rdb.Do(ctx, "cluster", "set-config-epoch", epoch).Err()
}

func (c *Client) BumpEpoch(ctx context.Context) error {
	return c.rdb.Do(ctx, "cluster", "bumpepoch").Err()
}

func (c *Client) FlushAll(ctx context.Context) error {
	return c.rdb.FlushAll(ctx).Err()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) Persistence(ctx context.Context) (map[string]string, error) {
	raw, err := c.rdb.Info(ctx, "persistence").Result()
	if err != nil {
		return nil, err
	}
	return parseInfo(raw), nil
}

func (c *Client) BGSave(ctx context.Context) error {
	return c.rdb.BgSave(ctx).Err()
}

func (c *Client) ConfigGet(ctx context.Context, name string) (string, error) {
	m, err := c.rdb.ConfigGet(ctx, name).Result()
	if err != nil {
		return "", err
	}
	v, ok := m[name]
	if !ok {
		return "", fmt.Errorf("unknown config parameter %q", name)
	}
	return v, nil
}

func (c *Client) DBSize(ctx context.Context) (int64, error) {
	return c.rdb.DBSize(ctx).Result()
}

func (c *Client) Scan(ctx context.Context, cursor uint64, count int64) ([]string, uint64, error) {
	return c.rdb.Scan(ctx, cursor, "", count).Result()
}

type DumpedKey struct {
	Key      string
	Payload  string
	ExpireAt int64
}

func expireAt(v int64) (at int64, exists bool) {
	switch v {
	case -2:
		return 0, false
	case -1:
		return 0, true
	default:
		return v, true
	}
}

func (c *Client) DumpKeys(ctx context.Context, keys []string) ([]DumpedKey, error) {
	pipe := c.rdb.Pipeline()
	dumps := make([]*redis.Cmd, len(keys))
	expiries := make([]*redis.Cmd, len(keys))
	for i, k := range keys {
		dumps[i] = pipe.Do(ctx, "DUMP", k)
		expiries[i] = pipe.Do(ctx, "PEXPIRETIME", k)
	}
	pipe.Exec(ctx)
	out := make([]DumpedKey, 0, len(keys))
	for i, k := range keys {
		payload, err := dumps[i].Text()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("DUMP %q: %w", k, err)
		}
		v, err := expiries[i].Int64()
		if err != nil {
			return nil, fmt.Errorf("PEXPIRETIME %q: %w", k, err)
		}
		at, exists := expireAt(v)
		if !exists {
			continue
		}
		out = append(out, DumpedKey{Key: k, Payload: payload, ExpireAt: at})
	}
	return out, nil
}

func (c *Client) RestoreKeys(ctx context.Context, keys []DumpedKey) (restored, existing int, err error) {
	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.Cmd, len(keys))
	for i, k := range keys {
		cmds[i] = pipe.Do(ctx, "RESTORE", k.Key, k.ExpireAt, k.Payload, "ABSTTL")
	}
	pipe.Exec(ctx)
	for i, cmd := range cmds {
		switch err := cmd.Err(); {
		case err == nil:
			restored++
		case strings.HasPrefix(err.Error(), "BUSYKEY"):
			existing++
		default:
			return restored, existing, fmt.Errorf("RESTORE %q: %w", keys[i].Key, err)
		}
	}
	return restored, existing, nil
}
