package guardian

import (
	"reflect"
	"testing"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

func healthyView() observation {
	nodes := []cluster.Node{
		master("m1", 0, 5460), master("m2", 5461, 10922), master("m3", 10923, 16383),
		replica("r1", "m1"), replica("r2", "m2"), replica("r3", "m3"),
	}
	v := observation{Peers: map[string]*peerState{}}
	for _, n := range nodes {
		st := cluster.State{Ready: true, ID: n.ID, Role: "master", Slots: n.Slots, Nodes: nodes,
			ClusterInfo: map[string]string{"cluster_state": "ok"}}
		if n.MasterID != "" {
			st.Role, st.MasterID, st.MasterLinkUp, st.ReplOffset = "replica", n.MasterID, true, 100
		}
		name := "node-" + n.ID
		v.Peers[name] = &peerState{Name: name, Redis: st}
	}
	v.Self = v.Peers["node-m1"].Redis
	return v
}

func TestPlanBackupSnapshotsReplicas(t *testing.T) {
	v := healthyView()
	plan, ok := planBackup(v)
	if !ok || len(plan) != 3 {
		t.Fatalf("plan = %+v, ok = %v", plan, ok)
	}
	var slots []cluster.SlotRange
	for i, p := range plan {
		if p.Part != i || p.Node != "node-r"+p.MasterID[1:] || p.NodeID != "r"+p.MasterID[1:] {
			t.Errorf("part %d = %+v, want the replica of its master", i, p)
		}
		slots = append(slots, p.Slots...)
	}
	if !cluster.Covers(slots, []cluster.SlotRange{{Start: 0, End: cluster.TotalSlots - 1}}) {
		t.Errorf("the plan must cover every slot: %v", slots)
	}
}

func TestPlanBackupFallsBackToMaster(t *testing.T) {
	v := healthyView()
	v.Peers["node-r2"].Redis.MasterLinkUp = false
	delete(v.Peers, "node-r3")
	plan, ok := planBackup(v)
	if !ok || plan[1].NodeID != "m2" || plan[2].NodeID != "m3" {
		t.Fatalf("shards without a usable replica must snapshot their master: %+v", plan)
	}
}

func TestPlanBackupRefusesBrokenCluster(t *testing.T) {
	v := healthyView()
	for i, n := range v.Self.Nodes {
		if n.ID == "m3" {
			v.Self.Nodes[i] = dead(n)
		}
	}
	if _, ok := planBackup(v); ok {
		t.Error("no backup while a slot owner is failing")
	}

	v = healthyView()
	delete(v.Peers, "node-m2")
	delete(v.Peers, "node-r2")
	if _, ok := planBackup(v); ok {
		t.Error("no backup when a shard has no reachable node")
	}

	v = healthyView()
	v.Self.ClusterInfo = map[string]string{"cluster_state": "fail"}
	if _, ok := planBackup(v); ok {
		t.Error("no backup while cluster_state is not ok")
	}
}

func TestUploadDelay(t *testing.T) {
	interval := 15 * time.Minute
	var got []time.Duration
	for i := range 3 {
		got = append(got, uploadDelay(i, 3, interval))
	}
	want := []time.Duration{0, 150 * time.Second, 300 * time.Second}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("delays = %v, want %v (all within half an interval)", got, want)
	}
}

func TestRetention(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	gen := func(seq int64, status string) generationInfo {
		return generationInfo{Seq: seq, Status: status, StartedAt: t0.Add(time.Duration(seq) * time.Minute)}
	}
	gens := []generationInfo{
		gen(1, "complete"), gen(2, "complete"), gen(3, "failed"), gen(4, "complete"),
		gen(5, "failed"), gen(6, "complete"), gen(7, "failed"), gen(8, "running"),
	}
	dropData, dropRows := retention(gens, 2, 1)
	if want := []int64{7, 5, 3, 2}; !reflect.DeepEqual(dropData, want) {
		t.Errorf("dropData = %v, want %v", dropData, want)
	}
	if want := []int64{3, 2}; !reflect.DeepEqual(dropRows, want) {
		t.Errorf("dropRows = %v, want %v", dropRows, want)
	}

	gens = []generationInfo{
		{Seq: 900, Status: "complete", StartedAt: t0},
		{Seq: 100, Status: "complete", StartedAt: t0.Add(time.Hour)},
	}
	if dropData, _ := retention(gens, 1, -1); !reflect.DeepEqual(dropData, []int64{900}) {
		t.Errorf("the older generation must go, got %v", dropData)
	}
}

func TestVersionNewer(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"8.10.3", "8.8.3", true}, {"8.8.3", "8.10.3", false}, {"8.8.3", "8.8.3", false},
		{"8.8.4", "8.8.3", true}, {"9.0", "8.8.3", true}, {"8.8", "8.8.0", false},
	}
	for _, c := range cases {
		if got := versionNewer(c.a, c.b); got != c.want {
			t.Errorf("versionNewer(%q, %q) = %v", c.a, c.b, got)
		}
	}
}

func TestBootstrapNeedsEveryNodeBlank(t *testing.T) {
	nodes := []string{"a:1", "b:1", "c:1", "d:1", "e:1", "f:1"}
	g := &Guardian{cfg: &Config{Nodes: nodes, Replicas: 1}}
	blank := func() observation {
		v := observation{Peers: map[string]*peerState{}}
		for _, n := range nodes {
			v.Peers[n] = &peerState{Name: n, Redis: cluster.State{Ready: true, ID: n, Role: "master"}}
		}
		return v
	}
	if _, ok := g.bootstrapNodes(blank()); !ok {
		t.Error("six blank nodes make a cluster")
	}
	v := blank()
	v.Peers["f:1"] = nil
	if _, ok := g.bootstrapNodes(v); ok {
		t.Error("never bootstrap with a node missing")
	}
	v = blank()
	v.Peers["c:1"].Redis.Keys = 3
	if _, ok := g.bootstrapNodes(v); ok {
		t.Error("never bootstrap with a node holding keys")
	}
}

func TestHealthDuringRestore(t *testing.T) {
	st, err := openStore("postgres://localhost/none", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	g := &Guardian{cfg: &Config{}, store: st}
	g.view = healthyView()
	if h := g.Health(); !h.Ready || h.Status != "ok" {
		t.Fatalf("healthy node: %+v", h)
	}
	g.restore = RestoreStatus{Running: true, Seq: 7, Slots: slots(0, 5460)}
	h := g.Health()
	if h.Ready || h.Status != "degraded" || len(h.Issues) != 1 {
		t.Errorf("restoring node: ready=%v status=%q issues=%v", h.Ready, h.Status, h.Issues)
	}
	g.restore = RestoreStatus{Queued: 1}
	if h := g.Health(); h.Ready {
		t.Error("a node with a restore waiting is not ready")
	}
}

func TestReplicaReadyOnlyWhenSynced(t *testing.T) {
	g := &Guardian{cfg: &Config{}}
	g.view = healthyView()
	g.view.Self = g.view.Peers["node-r1"].Redis
	if h := g.Health(); !h.Ready {
		t.Fatalf("a synced replica is ready: %+v", h)
	}
	g.view.Self.MasterLinkUp, g.view.Self.NotSynced = false, true
	if h := g.Health(); h.Ready {
		t.Error("a replica still in its initial sync is not ready")
	}
}
