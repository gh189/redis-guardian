package guardian

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gh189/redis-guardian/cluster"
)

func replicaCounts(nodes []cluster.Node, dead func(cluster.Node) bool) map[string]int {
	counts := map[string]int{}
	for _, n := range nodes {
		if n.MasterID != "" && !dead(n) {
			counts[n.MasterID]++
		}
	}
	return counts
}

type joinPlan struct {
	Adopt  []cluster.SlotRange
	Master string
}

func planJoin(nodes []cluster.Node, live map[string]string, dead func(cluster.Node) bool) joinPlan {
	owners := cluster.SlotOwners(nodes)
	replicas := replicaCounts(nodes, dead)
	for s := 0; s < cluster.TotalSlots; s++ {
		group := owners[s]
		if group >= 0 && !dead(nodes[group]) {
			continue
		}
		if group >= 0 && replicas[nodes[group].ID] > 0 {
			return joinPlan{}
		}
		return joinPlan{Adopt: cluster.Ranges(func(s int) bool { return owners[s] == group })}
	}
	var best *cluster.Node
	for i := range nodes {
		n := &nodes[i]
		if _, alive := live[n.ID]; !alive || !n.IsMaster() || len(n.Slots) == 0 {
			continue
		}
		if best == nil || replicas[n.ID] < replicas[best.ID] ||
			(replicas[n.ID] == replicas[best.ID] && n.Slots[0].Start < best.Slots[0].Start) {
			best = n
		}
	}
	if best == nil {
		return joinPlan{}
	}
	return joinPlan{Master: best.ID}
}

func checkTopology(cfg *Config, v observation, dead func(cluster.Node) bool) []string {
	self := v.Self
	if !self.Ready {
		return []string{"local redis is not ready: " + self.Error}
	}
	var issues []string
	var unreachable []string
	for _, name := range cfg.Nodes {
		if v.Peers[name] == nil {
			unreachable = append(unreachable, name)
		}
	}
	if len(unreachable) > 0 {
		issues = append(issues, "guardian unreachable: "+strings.Join(unreachable, ", "))
	}
	if self.Unassigned() {
		return append(issues, "local node has no role in the cluster yet")
	}
	if state := self.ClusterInfo["cluster_state"]; state != "ok" {
		issues = append(issues, "cluster_state is "+state)
	}
	if self.Role == "replica" && !self.MasterLinkUp {
		issues = append(issues, "replication link to master is down")
	}
	owners := cluster.SlotOwners(self.Nodes)
	unassigned := cluster.CountSlots(cluster.Ranges(func(s int) bool { return owners[s] < 0 }))
	if unassigned > 0 {
		issues = append(issues, fmt.Sprintf("%d slots are not assigned", unassigned))
	}
	live := v.live()
	healthyReplicas := replicaCounts(self.Nodes, func(n cluster.Node) bool { return dead(n) || unsynced(cfg, v, live, n) })
	masters := 0
	for _, n := range self.Nodes {
		switch {
		case dead(n):
			issues = append(issues, fmt.Sprintf("node %s (%s) is failing", short(n.ID), n.Addr))
		case n.IsMaster() && len(n.Slots) > 0:
			masters++
			if healthyReplicas[n.ID] < cfg.Replicas {
				issues = append(issues, fmt.Sprintf("master %s (%s) has %d/%d healthy replicas",
					short(n.ID), n.Addr, healthyReplicas[n.ID], cfg.Replicas))
			}
		}
	}
	if masters != cfg.masters() {
		issues = append(issues, fmt.Sprintf("%d healthy masters, expected %d", masters, cfg.masters()))
	}
	if len(self.Nodes) != len(cfg.Nodes) {
		issues = append(issues, fmt.Sprintf("cluster knows %d nodes, expected %d", len(self.Nodes), len(cfg.Nodes)))
	}
	sort.Strings(issues)
	return issues
}

func unsynced(cfg *Config, v observation, live map[string]string, n cluster.Node) bool {
	name, ok := live[n.ID]
	if !ok {
		name = cfg.nameByAddr(n)
	}
	switch p := v.Peers[name]; {
	case p == nil:
		return false
	case !p.Redis.Ready:
		return true
	default:
		return p.Redis.ID == n.ID && p.Redis.Role == "replica" && !p.Redis.MasterLinkUp
	}
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
