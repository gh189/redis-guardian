package guardian

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

func master(id string, start, end int, flags ...string) cluster.Node {
	return cluster.Node{ID: id, Flags: append([]string{"master"}, flags...), LinkState: "connected",
		Slots: []cluster.SlotRange{{Start: start, End: end}}}
}

func replica(id, masterID string, flags ...string) cluster.Node {
	return cluster.Node{ID: id, Flags: append([]string{"slave"}, flags...), LinkState: "connected", MasterID: masterID}
}

func dead(n cluster.Node) cluster.Node {
	n.Flags = append(n.Flags, "fail")
	n.LinkState = "disconnected"
	return n
}

func plan(nodes []cluster.Node, live map[string]string) joinPlan {
	return planJoin(nodes, live, deadByFlags(live))
}

func deadByFlags(live map[string]string) func(cluster.Node) bool {
	return func(n cluster.Node) bool {
		_, alive := live[n.ID]
		return !alive && (n.Failed() || n.LinkState != "connected")
	}
}

func liveIDs(ids ...string) map[string]string {
	m := map[string]string{}
	for _, id := range ids {
		m[id] = "node-" + id
	}
	return m
}

func TestPlanJoinReplicatesMasterWithFewestReplicas(t *testing.T) {
	nodes := []cluster.Node{
		master("m1", 0, 5460), master("m2", 5461, 10922), master("m3", 10923, 16383),
		replica("r1", "m1"), dead(replica("r2", "m2")), replica("r3", "m3"),
	}
	plan := plan(nodes, liveIDs("m1", "m2", "m3", "r1", "r3"))
	if plan.Master != "m2" || plan.Adopt != nil {
		t.Errorf("plan = %+v, want replica of m2", plan)
	}
}

func TestPlanJoinNeverReplacesDeadMasterThatHasLivingReplica(t *testing.T) {
	nodes := []cluster.Node{
		master("m1", 0, 5460), dead(master("m2", 5461, 10922)), master("m3", 10923, 16383),
		replica("r1", "m1"), replica("r2", "m2"), replica("r3", "m3"),
	}
	live := liveIDs("m1", "m3", "r1", "r2", "r3")
	if got := plan(nodes, live); got.Master != "" || got.Adopt != nil {
		t.Fatalf("plan = %+v, want to wait for the promotion", got)
	}

	nodes[1].Slots = nil
	nodes[4] = master("r2", 5461, 10922)
	if got := plan(nodes, live); got.Master != "r2" {
		t.Errorf("plan = %+v, want replica of the promoted r2", got)
	}
}

func TestPlanJoinTrustsGuardianOverStaleView(t *testing.T) {
	nodes := []cluster.Node{
		master("m1", 0, 5460), master("m2", 5461, 10922), master("m3", 10923, 16383),
		replica("r1", "m1"), replica("r2", "m2"), replica("r3", "m3"),
	}
	live := liveIDs("m1", "m3", "r1", "r2", "r3")
	replaced := func(n cluster.Node) bool { return n.ID == "m2" }
	if got := planJoin(nodes, live, replaced); got.Master != "" || got.Adopt != nil {
		t.Errorf("plan = %+v, want to wait for the promotion", got)
	}
}

func TestPlanJoinAdoptsSlotsOfLostShard(t *testing.T) {
	nodes := []cluster.Node{
		master("m1", 0, 5460), dead(master("m2", 5461, 10922)), dead(master("m3", 10923, 16383)),
		replica("r1", "m1"), dead(replica("r2", "m2")), dead(replica("r3", "m3")),
	}
	plan := plan(nodes, liveIDs("m1", "r1"))
	want := []cluster.SlotRange{{Start: 5461, End: 10922}}
	if !reflect.DeepEqual(plan.Adopt, want) {
		t.Errorf("plan.Adopt = %v, want %v", plan.Adopt, want)
	}
}

func TestPlanJoinAdoptsUnassignedSlots(t *testing.T) {
	nodes := []cluster.Node{master("m1", 0, 5460), master("m2", 5461, 10922)}
	plan := plan(nodes, liveIDs("m1", "m2"))
	want := []cluster.SlotRange{{Start: 10923, End: 16383}}
	if !reflect.DeepEqual(plan.Adopt, want) {
		t.Errorf("plan.Adopt = %v, want %v", plan.Adopt, want)
	}
}

func TestPlanJoinWaitsWithoutLivingMaster(t *testing.T) {
	nodes := []cluster.Node{master("m1", 0, 16383)}
	if plan := plan(nodes, liveIDs()); plan.Master != "" || plan.Adopt != nil {
		t.Errorf("plan = %+v, want nothing to do", plan)
	}
}

func TestLease(t *testing.T) {
	var l lease
	if !l.grant("a", "t1", time.Minute) {
		t.Fatal("free lease must be granted")
	}
	if l.grant("b", "t2", time.Minute) {
		t.Fatal("held lease must not be granted to another holder")
	}
	if !l.grant("a", "t1", time.Minute) {
		t.Fatal("holder must be able to renew")
	}
	l.release("t2")
	if l.grant("b", "t2", time.Minute) {
		t.Fatal("release with a foreign token must not free the lease")
	}
	l.release("t1")
	if !l.grant("b", "t2", time.Millisecond) {
		t.Fatal("released lease must be granted")
	}
	time.Sleep(5 * time.Millisecond)
	if !l.grant("c", "t3", time.Minute) {
		t.Fatal("expired lease must be granted")
	}
}

func TestCheckTopology(t *testing.T) {
	cfg := &Config{Nodes: []string{"a:6379", "b:6379", "c:6379", "d:6379", "e:6379", "f:6379"}, Replicas: 1}
	nodes := []cluster.Node{
		master("m1", 0, 5460), master("m2", 5461, 10922), master("m3", 10923, 16383),
		replica("r1", "m1"), replica("r2", "m2"), replica("r3", "m3"),
	}
	ids := []string{"m1", "m2", "m3", "r1", "r2", "r3"}
	view := func(nodes []cluster.Node) observation {
		v := observation{Peers: map[string]*peerState{}}
		for i, name := range cfg.Nodes {
			v.Peers[name] = &peerState{Name: name, Redis: cluster.State{Ready: true, ID: ids[i]}}
		}
		v.Self = cluster.State{Ready: true, ID: "m1", Role: "master", Slots: nodes[0].Slots, Nodes: nodes,
			ClusterInfo: map[string]string{"cluster_state": "ok"}}
		return v
	}

	if issues := checkTopology(cfg, view(nodes), deadByFlags(view(nodes).live())); len(issues) != 0 {
		t.Errorf("healthy cluster reported issues: %v", issues)
	}

	broken := append([]cluster.Node{}, nodes...)
	broken[4] = dead(broken[4])
	v := view(broken)
	v.Peers["e:6379"] = nil
	issues := strings.Join(checkTopology(cfg, v, deadByFlags(v.live())), "\n")
	for _, want := range []string{"guardian unreachable: e:6379", "node r2", "master m2", "0/1 healthy replicas"} {
		if !strings.Contains(issues, want) {
			t.Errorf("issues do not mention %q:\n%s", want, issues)
		}
	}

	v = view(nodes)
	v.Peers["f:6379"].Redis.Role, v.Peers["f:6379"].Redis.MasterLinkUp = "replica", false
	issues = strings.Join(checkTopology(cfg, v, deadByFlags(v.live())), "\n")
	if !strings.Contains(issues, "master m3") || !strings.Contains(issues, "0/1 healthy replicas") || strings.Contains(issues, "failing") {
		t.Errorf("a syncing replica must not count as healthy, nor as failing:\n%s", issues)
	}
}

func TestTakeoverNeedsProofTheMasterIsGone(t *testing.T) {
	cfg := &Config{Nodes: []string{"a:6379", "b:6379"}}
	g := &Guardian{cfg: cfg, names: map[string]string{}}
	m := master("m1", 0, 16383)
	m.Addr = "a:6379"
	view := func(peerA *peerState) observation {
		return observation{Peers: map[string]*peerState{
			"a:6379": peerA,
			"b:6379": {Name: "b:6379", Redis: cluster.State{Ready: true, ID: "r1", Role: "replica", MasterID: "m1"}},
		}}
	}

	if g.masterReplaced(view(nil), dead(m)) {
		t.Error("master and guardian unreachable (crash or network split): no forced takeover")
	}
	if g.masterReplaced(view(&peerState{Name: "a:6379", Redis: cluster.State{Ready: true, ID: "m1"}}), m) {
		t.Error("a living master is not replaced")
	}
	if !g.masterReplaced(view(&peerState{Name: "a:6379", Redis: cluster.State{Ready: true, ID: "new"}}), dead(m)) {
		t.Error("the master's address runs a new Redis: the old master is gone, take over")
	}
}

func TestForgetNeedsProofTheIDIsGone(t *testing.T) {
	cfg := &Config{Nodes: []string{"a:6379", "b:6379"}, Grace: 15 * time.Second}
	g := &Guardian{cfg: cfg, names: map[string]string{}}
	n := dead(replica("r1", "m1"))
	n.Addr = "b:6379"
	view := func(peerB *peerState) observation {
		return observation{
			Self: cluster.State{Ready: true, ID: "a1", Role: "master"},
			Peers: map[string]*peerState{
				"a:6379": {Name: "a:6379", Redis: cluster.State{Ready: true, ID: "a1"}},
				"b:6379": peerB,
			},
		}
	}

	if gone, _ := g.forgettable(n, view(nil)); gone {
		t.Error("node and guardian unreachable (crash or network split): keep the ID")
	}
	if gone, _ := g.forgettable(n, view(&peerState{Name: "b:6379", Redis: cluster.State{Ready: true, ID: "r1"}})); gone {
		t.Error("a living node is not forgotten")
	}
	if gone, wait := g.forgettable(n, view(&peerState{Name: "b:6379", Redis: cluster.State{Ready: true, ID: "new"}})); !gone || wait != cfg.Grace {
		t.Errorf("the address runs a new Redis: forget after the grace period, got %v %v", gone, wait)
	}
	n.Addr = "removed:6379"
	if gone, wait := g.forgettable(n, view(nil)); !gone || wait != 4*cfg.Grace {
		t.Errorf("an ID of an unconfigured node is forgotten after a longer wait, got %v %v", gone, wait)
	}
}

func TestLoadingReplicaIsNotHealthy(t *testing.T) {
	cfg := &Config{Nodes: []string{"a:6379", "b:6379", "c:6379", "d:6379", "e:6379", "f:6379"}, Replicas: 1}
	nodes := []cluster.Node{
		master("m1", 0, 5460), master("m2", 5461, 10922), master("m3", 10923, 16383),
		replica("r1", "m1"), replica("r2", "m2"), replica("r3", "m3"),
	}
	for i := range nodes {
		nodes[i].Addr = cfg.Nodes[i]
	}
	v := observation{Peers: map[string]*peerState{}}
	for i, name := range cfg.Nodes {
		v.Peers[name] = &peerState{Name: name, Redis: cluster.State{Ready: true, ID: nodes[i].ID}}
	}
	v.Peers["f:6379"].Redis = cluster.State{Loading: true, Error: "LOADING Redis is loading the dataset in memory"}
	v.Self = cluster.State{Ready: true, ID: "m1", Role: "master", Slots: nodes[0].Slots, Nodes: nodes,
		ClusterInfo: map[string]string{"cluster_state": "ok"}}

	issues := strings.Join(checkTopology(cfg, v, deadByFlags(v.live())), "\n")
	if !strings.Contains(issues, "master m3") || !strings.Contains(issues, "0/1 healthy replicas") {
		t.Errorf("a loading replica must not count as healthy:\n%s", issues)
	}
}
