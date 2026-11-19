package cluster

import (
	"sort"
	"strconv"
	"strings"
)

const TotalSlots = 16384

type SlotRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type Node struct {
	ID          string      `json:"id"`
	Addr        string      `json:"addr"`
	Hostname    string      `json:"hostname,omitempty"`
	Flags       []string    `json:"flags"`
	MasterID    string      `json:"master_id,omitempty"`
	ConfigEpoch int64       `json:"config_epoch"`
	LinkState   string      `json:"link_state"`
	Slots       []SlotRange `json:"slots,omitempty"`
}

func (n Node) Has(flag string) bool {
	for _, f := range n.Flags {
		if f == flag {
			return true
		}
	}
	return false
}

func (n Node) IsMaster() bool { return n.Has("master") }

func (n Node) Failed() bool { return n.Has("fail") || n.Has("fail?") }

func ParseNodes(raw string) []Node {
	var nodes []Node
	for _, line := range strings.Split(raw, "\n") {
		f := strings.Fields(line)
		if len(f) < 8 {
			continue
		}
		n := Node{ID: f[0], Flags: strings.Split(f[2], ","), LinkState: f[7]}
		addr, hostname, _ := strings.Cut(f[1], ",")
		n.Addr, _, _ = strings.Cut(addr, "@")
		n.Hostname = hostname
		if f[3] != "-" {
			n.MasterID = f[3]
		}
		n.ConfigEpoch, _ = strconv.ParseInt(f[6], 10, 64)
		for _, s := range f[8:] {
			if strings.HasPrefix(s, "[") {
				continue
			}
			lo, hi, isRange := strings.Cut(s, "-")
			start, err := strconv.Atoi(lo)
			if err != nil {
				continue
			}
			end := start
			if isRange {
				if end, err = strconv.Atoi(hi); err != nil {
					continue
				}
			}
			n.Slots = append(n.Slots, SlotRange{start, end})
		}
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func CountSlots(ranges []SlotRange) int {
	total := 0
	for _, r := range ranges {
		total += r.End - r.Start + 1
	}
	return total
}

func SplitSlots(n int) []SlotRange {
	ranges := make([]SlotRange, 0, n)
	for i := 0; i < n; i++ {
		ranges = append(ranges, SlotRange{i * TotalSlots / n, (i+1)*TotalSlots/n - 1})
	}
	return ranges
}

func SlotOwners(nodes []Node) []int {
	owners := make([]int, TotalSlots)
	for i := range owners {
		owners[i] = -1
	}
	for i, n := range nodes {
		for _, r := range n.Slots {
			for s := r.Start; s <= r.End && s < TotalSlots; s++ {
				owners[s] = i
			}
		}
	}
	return owners
}

func Ranges(pick func(slot int) bool) []SlotRange {
	var ranges []SlotRange
	start := -1
	for s := 0; s <= TotalSlots; s++ {
		in := s < TotalSlots && pick(s)
		switch {
		case in && start < 0:
			start = s
		case !in && start >= 0:
			ranges = append(ranges, SlotRange{start, s - 1})
			start = -1
		}
	}
	return ranges
}
