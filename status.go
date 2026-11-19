package guardian

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"

	"github.com/gh189/redis-guardian/cluster"
)

type HealthStatus struct {
	Status       string         `json:"status"`
	Ready        bool           `json:"ready"`
	RedisReady   bool           `json:"redis_ready"`
	Role         string         `json:"role,omitempty"`
	ClusterState string         `json:"cluster_state,omitempty"`
	Issues       []string       `json:"issues"`
	ObservedAt   string         `json:"observed_at"`
	Backup       *BackupStatus  `json:"backup,omitempty"`
	Restore      *RestoreStatus `json:"restore,omitempty"`
}

func (g *Guardian) Health() HealthStatus {
	v, issues := g.snapshot()
	h := HealthStatus{
		Status:       "ok",
		RedisReady:   v.Self.Ready,
		Role:         v.Self.Role,
		ClusterState: v.Self.ClusterInfo["cluster_state"],
		Issues:       append([]string{}, issues...),
		ObservedAt:   v.At.Format(time.RFC3339),
	}
	serving := v.Self.Assigned() && h.ClusterState == "ok"
	h.Ready = serving && (v.Self.Role != "replica" || v.Self.MasterLinkUp)
	if g.store != nil {
		h.Backup, h.Restore = g.backupStatus(), g.restoreStatus()
		switch {
		case h.Restore.Running:
			h.Ready = false
			h.Issues = append(h.Issues, fmt.Sprintf("restoring slots %v from backup %d", h.Restore.Slots, h.Restore.Seq))
		case h.Restore.Queued > 0:
			h.Ready = false
			h.Issues = append(h.Issues, fmt.Sprintf("%d restore jobs are waiting to run here", h.Restore.Queued))
		}
	}
	switch {
	case !serving:
		h.Status = "down"
	case len(h.Issues) > 0:
		h.Status = "degraded"
	}
	return h
}

type NodeStatus struct {
	Guardian struct {
		Version       string `json:"version"`
		Node          string `json:"node"`
		ManagesRedis  bool   `json:"manages_redis"`
		StartedAt     string `json:"started_at"`
		UptimeSeconds int64  `json:"uptime_seconds"`
	} `json:"guardian"`
	Host struct {
		Hostname          string  `json:"hostname"`
		OS                string  `json:"os"`
		Arch              string  `json:"arch"`
		CPUCores          int     `json:"cpu_cores"`
		CPUPercent        float64 `json:"cpu_percent"`
		Load1             float64 `json:"load1"`
		Load5             float64 `json:"load5"`
		Load15            float64 `json:"load15"`
		MemoryTotalBytes  uint64  `json:"memory_total_bytes"`
		MemoryUsedBytes   uint64  `json:"memory_used_bytes"`
		MemoryUsedPercent float64 `json:"memory_used_percent"`
	} `json:"host"`
	Redis struct {
		Ready              bool    `json:"ready"`
		Error              string  `json:"error,omitempty"`
		Version            string  `json:"version,omitempty"`
		UptimeSeconds      int64   `json:"uptime_seconds"`
		Keys               int64   `json:"keys"`
		UsedMemoryBytes    int64   `json:"used_memory_bytes"`
		UsedMemoryRSSBytes int64   `json:"used_memory_rss_bytes"`
		MaxMemoryBytes     int64   `json:"maxmemory_bytes"`
		UsedCPUSysSeconds  float64 `json:"used_cpu_sys_seconds"`
		UsedCPUUserSeconds float64 `json:"used_cpu_user_seconds"`
		ConnectedClients   int64   `json:"connected_clients"`
		OpsPerSec          int64   `json:"ops_per_sec"`
	} `json:"redis"`
	Cluster struct {
		NodeID       string              `json:"node_id,omitempty"`
		Role         string              `json:"role,omitempty"`
		Slots        []cluster.SlotRange `json:"slots"`
		SlotCount    int                 `json:"slot_count"`
		Master       *NodeInfo           `json:"master,omitempty"`
		Replicas     []NodeInfo          `json:"replicas"`
		MasterLinkUp *bool               `json:"master_link_up,omitempty"`
		ReplOffset   int64               `json:"repl_offset"`
		State        string              `json:"cluster_state,omitempty"`
		KnownNodes   int                 `json:"known_nodes"`
		CurrentEpoch int64               `json:"current_epoch"`
	} `json:"cluster"`
	Issues []string `json:"issues"`
}

func (g *Guardian) Node(ctx context.Context) NodeStatus {
	v, issues := g.snapshot()
	st := g.redis.Inspect(ctx)
	v.Self = st
	var s NodeStatus

	s.Guardian.Version = Version
	s.Guardian.Node = g.cfg.Self
	s.Guardian.ManagesRedis = g.cfg.Managed
	s.Guardian.StartedAt = g.started.Format(time.RFC3339)
	s.Guardian.UptimeSeconds = int64(time.Since(g.started).Seconds())

	s.Host.Hostname, _ = os.Hostname()
	s.Host.OS, s.Host.Arch, s.Host.CPUCores = runtime.GOOS, runtime.GOARCH, runtime.NumCPU()
	if p, err := cpu.PercentWithContext(ctx, 0, false); err == nil && len(p) > 0 {
		s.Host.CPUPercent = p[0]
	}
	if l, err := load.AvgWithContext(ctx); err == nil {
		s.Host.Load1, s.Host.Load5, s.Host.Load15 = l.Load1, l.Load5, l.Load15
	}
	if m, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		s.Host.MemoryTotalBytes, s.Host.MemoryUsedBytes, s.Host.MemoryUsedPercent = m.Total, m.Used, m.UsedPercent
	}

	num := func(key string) int64 { n, _ := strconv.ParseInt(st.Info[key], 10, 64); return n }
	dec := func(key string) float64 { f, _ := strconv.ParseFloat(st.Info[key], 64); return f }
	s.Redis.Ready, s.Redis.Error = st.Ready, st.Error
	s.Redis.Version = st.Info["redis_version"]
	s.Redis.UptimeSeconds = num("uptime_in_seconds")
	s.Redis.Keys = st.Keys
	s.Redis.UsedMemoryBytes = num("used_memory")
	s.Redis.UsedMemoryRSSBytes = num("used_memory_rss")
	s.Redis.MaxMemoryBytes = num("maxmemory")
	s.Redis.UsedCPUSysSeconds = dec("used_cpu_sys")
	s.Redis.UsedCPUUserSeconds = dec("used_cpu_user")
	s.Redis.ConnectedClients = num("connected_clients")
	s.Redis.OpsPerSec = num("instantaneous_ops_per_sec")

	c := &s.Cluster
	c.NodeID, c.Role, c.ReplOffset = st.ID, st.Role, st.ReplOffset
	c.Slots, c.SlotCount = append([]cluster.SlotRange{}, st.Slots...), cluster.CountSlots(st.Slots)
	c.State, c.KnownNodes, c.CurrentEpoch = st.ClusterInfo["cluster_state"], len(st.Nodes), st.Epoch()
	c.Replicas = []NodeInfo{}
	for _, n := range st.Nodes {
		switch {
		case st.Role == "replica" && n.ID == st.MasterID:
			info := g.describe(n, v)
			c.Master, c.MasterLinkUp = &info, &st.MasterLinkUp
		case n.MasterID == st.ID && st.ID != "":
			c.Replicas = append(c.Replicas, g.describe(n, v))
		}
	}
	s.Issues = append([]string{}, issues...)
	return s
}

type NodeInfo struct {
	cluster.Node
	Name    string `json:"name,omitempty"`
	Role    string `json:"role"`
	Healthy bool   `json:"healthy"`
	Keys    *int64 `json:"keys,omitempty"`
}

type ShardInfo struct {
	Slots     []cluster.SlotRange `json:"slots"`
	SlotCount int                 `json:"slot_count"`
	Keys      int64               `json:"keys"`
	Status    string              `json:"status"`
	Master    NodeInfo            `json:"master"`
	Replicas  []NodeInfo          `json:"replicas"`
}

type MemberInfo struct {
	Name              string         `json:"name"`
	GuardianReachable bool           `json:"guardian_reachable"`
	RedisReady        bool           `json:"redis_ready"`
	NodeID            string         `json:"node_id,omitempty"`
	Role              string         `json:"role,omitempty"`
	Keys              int64          `json:"keys"`
	Restore           *RestoreStatus `json:"restore,omitempty"`
}

type ClusterStatus struct {
	State           string              `json:"state"`
	Healthy         bool                `json:"healthy"`
	Issues          []string            `json:"issues"`
	TotalKeys       int64               `json:"total_keys"`
	ExpectedNodes   int                 `json:"expected_nodes"`
	ExpectedMasters int                 `json:"expected_masters"`
	UnassignedSlots []cluster.SlotRange `json:"unassigned_slots"`
	Shards          []ShardInfo         `json:"shards"`
	Members         []MemberInfo        `json:"members"`
	ClusterNodes    []NodeInfo          `json:"cluster_nodes"`
	ClusterInfo     map[string]string   `json:"cluster_info"`
	Source          string              `json:"source"`
	ObservedAt      string              `json:"observed_at"`
}

func (g *Guardian) Cluster() ClusterStatus {
	v, issues := g.snapshot()
	src, source := v.Self, g.cfg.Self
	if !src.Assigned() {
		if seed := v.seed(g.cfg.Self); seed != nil {
			src, source = seed.Redis, seed.Name
		}
	}
	s := ClusterStatus{
		State:           src.ClusterInfo["cluster_state"],
		Healthy:         len(issues) == 0 && v.Self.Ready,
		Issues:          append([]string{}, issues...),
		ExpectedNodes:   len(g.cfg.Nodes),
		ExpectedMasters: g.cfg.masters(),
		Shards:          []ShardInfo{},
		ClusterNodes:    []NodeInfo{},
		ClusterInfo:     src.ClusterInfo,
		Source:          source,
		ObservedAt:      v.At.Format(time.RFC3339),
	}
	owners := cluster.SlotOwners(src.Nodes)
	s.UnassignedSlots = append([]cluster.SlotRange{}, cluster.Ranges(func(slot int) bool { return owners[slot] < 0 })...)

	for _, n := range src.Nodes {
		info := g.describe(n, v)
		s.ClusterNodes = append(s.ClusterNodes, info)
		if !n.IsMaster() || len(n.Slots) == 0 {
			continue
		}
		shard := ShardInfo{Slots: n.Slots, SlotCount: cluster.CountSlots(n.Slots), Master: info, Replicas: []NodeInfo{}, Status: "degraded"}
		for _, r := range src.Nodes {
			if r.MasterID == n.ID {
				replica := g.describe(r, v)
				shard.Replicas = append(shard.Replicas, replica)
				if replica.Healthy && info.Healthy {
					shard.Status = "ok"
				}
			}
		}
		if !info.Healthy {
			shard.Status = "down"
		}
		if info.Keys != nil {
			shard.Keys = *info.Keys
			s.TotalKeys += shard.Keys
		}
		s.Shards = append(s.Shards, shard)
	}
	for _, name := range g.cfg.Nodes {
		m := MemberInfo{Name: name}
		if p := v.Peers[name]; p != nil {
			m.GuardianReachable, m.RedisReady = true, p.Redis.Ready
			m.NodeID, m.Role, m.Keys = p.Redis.ID, p.Redis.Role, p.Redis.Keys
			if p.Restore.active() {
				m.Restore = p.Restore
			}
		}
		s.Members = append(s.Members, m)
	}
	return s
}

func (g *Guardian) describe(n cluster.Node, v observation) NodeInfo {
	live := v.live()
	info := NodeInfo{Node: n, Name: live[n.ID], Role: "replica", Healthy: !g.deadFn(v)(n) && !unsynced(g.cfg, v, live, n)}
	if n.IsMaster() {
		info.Role = "master"
	}
	if p := v.Peers[info.Name]; p != nil {
		info.Keys = &p.Redis.Keys
	} else {
		info.Name = g.nodeName(n)
	}
	return info
}

func (g *Guardian) Events() []Event {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return slices.Clone(g.events)
}
