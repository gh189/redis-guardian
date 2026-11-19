package guardian

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

func (g *Guardian) tick(ctx context.Context) {
	defer g.endTick()
	v := g.observe(ctx)
	g.remember(v)

	self := &v.Self
	if !self.Ready {
		hung := g.sup != nil && g.sup.Running() && !self.Loading
		if g.stable("hung", hung, 2*g.cfg.Grace) {
			g.event("restart-redis", "local redis is unresponsive: "+self.Error)
			g.sup.Kill()
		}
		return
	}
	switch {
	case self.Unassigned() && v.seed(g.cfg.Self) != nil:
		g.join(ctx, v)
	case self.Unassigned():
		g.bootstrap(ctx, v)
	default:
		if self.Role == "replica" {
			g.checkMaster(ctx, v)
		} else {
			g.adoptLeftovers(ctx, v)
		}
		g.remeet(ctx, v)
		g.forget(ctx, v)
	}
}

type bootstrapRequest struct {
	Slots []cluster.SlotRange `json:"slots"`
	Epoch int                 `json:"epoch"`
}

func (g *Guardian) bootstrapNodes(v observation) ([]string, bool) {
	var nodes []string
	for _, name := range g.cfg.Nodes {
		if p := v.Peers[name]; p != nil && p.Redis.Unassigned() && p.Redis.Keys == 0 {
			nodes = append(nodes, name)
		}
	}
	return nodes, len(nodes) == len(g.cfg.Nodes)
}

func (g *Guardian) bootstrap(ctx context.Context, v observation) {
	nodes, ok := g.bootstrapNodes(v)
	if !ok {
		slog.Info("no cluster yet, waiting for every node to be up and blank", "ready", len(nodes), "want", len(g.cfg.Nodes))
		return
	}
	if nodes[0] != g.cfg.Self {
		return
	}
	g.withLock(ctx, "bootstrap", func(ctx context.Context) error {
		v := g.observe(ctx)
		nodes, ok := g.bootstrapNodes(v)
		if !ok || nodes[0] != g.cfg.Self || v.seed(g.cfg.Self) != nil {
			return nil
		}
		masters := nodes[:g.cfg.masters()]
		slots := cluster.SplitSlots(len(masters))
		g.prepareBootstrapRestore(ctx, slots, v.Self.Info["redis_version"])
		for i := len(masters) - 1; i >= 0; i-- {
			req := bootstrapRequest{Slots: slots[i : i+1], Epoch: i + 1}
			var err error
			if masters[i] == g.cfg.Self {
				err = g.applyBootstrap(ctx, req)
			} else {
				err = g.call(ctx, masters[i], http.MethodPost, "/internal/bootstrap", req, nil)
			}
			if err != nil {
				return fmt.Errorf("assigning slots to %s: %w", masters[i], err)
			}
		}
		for _, name := range nodes[1:] {
			ip, port, err := resolve(ctx, name)
			if err == nil {
				err = g.redis.Meet(ctx, ip, port)
			}
			if err != nil {
				slog.Warn("bootstrap: meet failed, the node will join later", "node", name, "err", err)
			}
		}
		g.event("bootstrap", fmt.Sprintf("created cluster, masters: %s", strings.Join(masters, ", ")))
		return nil
	})
}

func (g *Guardian) applyBootstrap(ctx context.Context, req bootstrapRequest) error {
	st := g.redis.Inspect(ctx)
	if !st.Unassigned() || st.Keys > 0 {
		return fmt.Errorf("refusing bootstrap: local redis is not a blank node")
	}
	if err := g.redis.AddSlots(ctx, req.Slots); err != nil {
		return err
	}
	if err := g.redis.SetConfigEpoch(ctx, req.Epoch); err != nil {
		slog.Debug("set-config-epoch refused", "err", err)
	}
	return nil
}

func (g *Guardian) join(ctx context.Context, v observation) {
	plan := planJoin(v.seed(g.cfg.Self).Redis.Nodes, v.live(), g.deadFn(v))
	if len(plan.Adopt) > 0 {
		if !g.stable("adopt:"+fmt.Sprint(plan.Adopt), true, g.cfg.Grace) {
			slog.Info("ownerless slots found, waiting before adopting them", "slots", plan.Adopt)
			return
		}
	} else if plan.Master == "" {
		slog.Info("cluster found, waiting for a living master to join")
		return
	}
	g.withLock(ctx, "join", func(ctx context.Context) error {
		v := g.observe(ctx)
		seed := v.seed(g.cfg.Self)
		if !v.Self.Unassigned() || seed == nil {
			return nil
		}
		fresh := planJoin(seed.Redis.Nodes, v.live(), g.deadFn(v))
		if len(fresh.Adopt) > 0 && !slices.Equal(fresh.Adopt, plan.Adopt) {
			return nil
		}
		if v.Self.Keys > 0 {
			if err := g.redis.FlushAll(ctx); err != nil {
				return err
			}
		}
		if v.Self.Node(seed.Redis.ID) == nil {
			ip, port, err := resolve(ctx, seed.Name)
			if err != nil {
				return err
			}
			if err := g.redis.Meet(ctx, ip, port); err != nil {
				return err
			}
		}
		if len(fresh.Adopt) > 0 {
			st, err := g.waitKnown(ctx, seed.Redis.ID)
			if err != nil {
				return err
			}
			return g.claimSlots(ctx, st, fresh.Adopt)
		}
		if fresh.Master == "" {
			return nil
		}
		if _, err := g.waitKnown(ctx, fresh.Master); err != nil {
			return err
		}
		if err := g.redis.Replicate(ctx, fresh.Master); err != nil {
			return err
		}
		g.event("replicate", fmt.Sprintf("joined as replica of %s (%s)", short(fresh.Master), v.live()[fresh.Master]))
		return nil
	})
}

func (g *Guardian) claimSlots(ctx context.Context, st cluster.State, slots []cluster.SlotRange) error {
	g.registerShardRestore(ctx, slots, st.Info["redis_version"])
	owners := cluster.SlotOwners(st.Nodes)
	wanted := cluster.SlotOwners([]cluster.Node{{Slots: slots}})
	stale := cluster.Ranges(func(s int) bool { return wanted[s] == 0 && owners[s] >= 0 })
	if err := g.redis.DelSlots(ctx, stale); err != nil {
		return err
	}
	if err := g.redis.AddSlots(ctx, slots); err != nil {
		return err
	}
	if err := g.redis.BumpEpoch(ctx); err != nil {
		return err
	}
	g.event("adopt-slots", fmt.Sprintf("became master of ownerless slots %v", slots))
	g.kickRestore()
	return nil
}

func (g *Guardian) beforeTakeover(ctx context.Context, st cluster.State) {
	if !st.NotSynced {
		return
	}
	if m := st.Node(st.MasterID); m != nil && len(m.Slots) > 0 {
		g.event("takeover-unsynced", fmt.Sprintf("the local replica never synced with %s; restoring slots %v from backup", short(m.ID), m.Slots))
		g.registerShardRestore(ctx, m.Slots, st.Info["redis_version"])
	}
}

func (g *Guardian) afterTakeover(st cluster.State) {
	if st.NotSynced {
		g.kickRestore()
	}
}

func (g *Guardian) adoptLeftovers(ctx context.Context, v observation) {
	chosen := func(v observation) []cluster.SlotRange {
		for _, name := range g.cfg.Nodes {
			if p := v.Peers[name]; p == nil || !p.Redis.Assigned() {
				return nil
			}
		}
		live, self := v.live(), &v.Self
		for _, n := range self.Nodes {
			_, alive := live[n.ID]
			size, mine := cluster.CountSlots(n.Slots), cluster.CountSlots(self.Slots)
			if alive && n.IsMaster() && size > 0 && (size < mine || (size == mine && n.ID < self.ID)) {
				return nil
			}
		}
		return planJoin(self.Nodes, live, g.deadFn(v)).Adopt
	}
	slots := chosen(v)
	if !g.stable("adopt-leftovers:"+fmt.Sprint(slots), len(slots) > 0, 4*g.cfg.Grace) {
		return
	}
	g.withLock(ctx, "adopt-leftovers", func(ctx context.Context) error {
		v := g.observe(ctx)
		if fresh := chosen(v); !slices.Equal(fresh, slots) || v.Self.Role != "master" {
			return nil
		}
		return g.claimSlots(ctx, v.Self, slots)
	})
}

func (g *Guardian) waitKnown(ctx context.Context, id string) (cluster.State, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := g.redis.Inspect(ctx)
		if st.Node(id) != nil {
			return st, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return st, fmt.Errorf("node %s is still unknown to the local redis", short(id))
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (g *Guardian) checkMaster(ctx context.Context, v observation) {
	self := &v.Self
	m := self.Node(self.MasterID)
	if m == nil {
		return
	}
	if g.stable("takeover:"+m.ID, g.masterReplaced(v, *m), g.cfg.Grace) && bestReplica(v, g.cfg.Self) {
		g.withLock(ctx, "takeover", func(ctx context.Context) error {
			v := g.observe(ctx)
			if !g.masterReplaced(v, *m) || v.Self.Role != "replica" || v.Self.MasterID != m.ID {
				return nil
			}
			g.beforeTakeover(ctx, v.Self)
			if err := g.redis.Failover(ctx, "TAKEOVER"); err != nil {
				return err
			}
			g.event("takeover", fmt.Sprintf("master %s (%s) was replaced by a new Redis, promoted the local replica", short(m.ID), m.Addr))
			g.afterTakeover(v.Self)
			return nil
		})
		return
	}

	move := func(v observation) string {
		self, dead := &v.Self, g.deadFn(v)
		m := self.Node(self.MasterID)
		plan := planJoin(self.Nodes, v.live(), dead)
		if self.Role != "replica" || m == nil || dead(*m) || plan.Master == "" || plan.Master == m.ID {
			return ""
		}
		replicas := replicaCounts(self.Nodes, dead)
		misplaced := !m.IsMaster() || len(m.Slots) == 0
		surplus := replicas[m.ID] > g.cfg.Replicas && replicas[plan.Master] < g.cfg.Replicas
		if misplaced || surplus {
			return plan.Master
		}
		return ""
	}
	if g.stable("move:"+m.ID, move(v) != "", g.cfg.Grace) {
		g.withLock(ctx, "rebalance", func(ctx context.Context) error {
			target := move(g.observe(ctx))
			if target == "" {
				return nil
			}
			if err := g.redis.Replicate(ctx, target); err != nil {
				return err
			}
			g.event("replicate", fmt.Sprintf("moved from master %s to %s, which needs a replica", short(m.ID), short(target)))
			return nil
		})
	}
}

func (g *Guardian) masterReplaced(v observation, m cluster.Node) bool {
	_, alive := v.live()[m.ID]
	return !alive && g.replaced(m, v)
}

func bestReplica(v observation, self string) bool {
	me := v.Self
	for name, p := range v.Peers {
		if name == self || p == nil || !p.Redis.Ready || p.Redis.Role != "replica" || p.Redis.MasterID != me.MasterID {
			continue
		}
		if p.Redis.ReplOffset > me.ReplOffset || (p.Redis.ReplOffset == me.ReplOffset && p.Redis.ID < me.ID) {
			return false
		}
	}
	return true
}

func (g *Guardian) remeet(ctx context.Context, v observation) {
	for _, name := range g.cfg.Nodes {
		p := v.Peers[name]
		if name == g.cfg.Self || p == nil || !p.Redis.Assigned() {
			continue
		}
		unknown := v.Self.Node(p.Redis.ID) == nil
		if !g.stable("meet:"+p.Redis.ID, unknown, g.cfg.Grace) {
			continue
		}
		ip, port, err := resolve(ctx, name)
		if err == nil {
			err = g.redis.Meet(ctx, ip, port)
		}
		if err != nil {
			slog.Warn("meet failed", "node", name, "err", err)
			continue
		}
		g.event("meet", fmt.Sprintf("re-introduced %s (%s) to the local redis", name, short(p.Redis.ID)))
	}
}

func (g *Guardian) forget(ctx context.Context, v observation) {
	for _, n := range v.Self.Nodes {
		gone, wait := g.forgettable(n, v)
		if !g.stable("forget:"+n.ID, gone, wait) {
			continue
		}
		if err := g.redis.Forget(ctx, n.ID); err != nil {
			slog.Warn("forget failed", "node", short(n.ID), "err", err)
			continue
		}
		g.event("forget", fmt.Sprintf("removed dead node %s (%s)", short(n.ID), n.Addr))
	}
}

func (g *Guardian) forgettable(n cluster.Node, v observation) (bool, time.Duration) {
	_, alive := v.live()[n.ID]
	gone := !alive && (n.Failed() || n.Has("noaddr")) && len(n.Slots) == 0 &&
		!n.Has("myself") && n.ID != v.Self.MasterID
	if g.nodeName(n) == "" {
		return gone, 4 * g.cfg.Grace
	}
	return gone && g.replaced(n, v), g.cfg.Grace
}

func (g *Guardian) gate(ctx context.Context) {
	g.mu.RLock()
	req := failoverRequest{Name: g.cfg.Self, ID: g.lastID, Mode: "takeover"}
	g.mu.RUnlock()
	start := time.Now()
	for ctx.Err() == nil {
		pending, unreachable, _ := g.askFailover(ctx, req)
		waited := time.Since(start)
		if len(pending) == 0 && (len(unreachable) == 0 || waited >= g.cfg.Grace) {
			return
		}
		if waited >= 2*g.cfg.Grace {
			slog.Warn("start gate timed out, starting redis anyway", "pending", pending)
			return
		}
		slog.Info("start gate: waiting before starting redis", "replicas_taking_over", pending, "unreachable", unreachable)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func (g *Guardian) handoff(ctx context.Context) {
	st := g.redis.Inspect(ctx)
	if !st.Ready || st.Role != "master" || len(st.Slots) == 0 {
		return
	}
	mine := map[string]bool{}
	for _, n := range st.Nodes {
		if name := g.nodeName(n); n.MasterID == st.ID && !n.Failed() && name != "" {
			mine[name] = true
		}
	}
	isMine := func(name string) bool { return mine[name] }
	req := failoverRequest{Name: g.cfg.Self, ID: st.ID, Mode: "graceful"}
	for ctx.Err() == nil {
		pending, unreachable, started := g.askFailover(ctx, req)
		if !slices.ContainsFunc(pending, isMine) && !slices.ContainsFunc(unreachable, isMine) {
			return
		}
		wait := time.Second
		if started {
			wait = 5 * time.Second
		}
		for until := time.Now().Add(wait); time.Now().Before(until) && ctx.Err() == nil; {
			time.Sleep(200 * time.Millisecond)
			if st = g.redis.Inspect(ctx); st.Ready && st.Role == "replica" {
				g.event("handoff", "handed the master role over to a replica before shutdown")
				return
			}
		}
	}
	slog.Warn("handoff did not complete, shutting down anyway")
}

func (g *Guardian) handleFailover(ctx context.Context, req failoverRequest) failoverResponse {
	st := g.redis.Inspect(ctx)
	if !st.Ready {
		running := g.sup != nil && g.sup.Running()
		return failoverResponse{OK: !running, Detail: "local redis is not ready"}
	}
	if st.Role != "replica" {
		return failoverResponse{OK: true}
	}
	m := st.Node(st.MasterID)
	if m == nil || (m.ID != req.ID && g.nodeName(*m) != req.Name) {
		return failoverResponse{OK: true}
	}
	view, _ := g.snapshot()
	view.Self = st
	if !bestReplica(view, g.cfg.Self) {
		return failoverResponse{Detail: "a sibling replica is promoted"}
	}
	if st.NotSynced && req.Mode != "takeover" {
		return failoverResponse{Detail: "the local replica is still in its initial sync"}
	}
	g.opMu.Lock()
	defer g.opMu.Unlock()
	mode := ""
	if req.Mode == "takeover" {
		mode = "TAKEOVER"
	}
	g.beforeTakeover(ctx, st)
	if err := g.redis.Failover(ctx, mode); err != nil {
		return failoverResponse{Detail: err.Error()}
	}
	g.event("failover", fmt.Sprintf("%s failover requested by master %s", req.Mode, req.Name))
	g.afterTakeover(st)
	return failoverResponse{Started: true, Detail: "failover started"}
}
