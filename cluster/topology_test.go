package cluster

import (
	"reflect"
	"testing"
)

const sampleNodes = `07c37dfeb235213a872192d90877d0cd55635b91 10.0.0.2:6379@16379,redis-1.redis slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 10.0.0.1:6379@16379,redis-0.redis myself,master - 0 0 1 connected 0-5460 [5461->-67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1]
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 10.0.0.3:6379@16379 master,fail - 1426238316232 1426238316232 2 disconnected 5461-10922 16000
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f :0@0 master,noaddr - 0 0 3 disconnected
`

func TestParseNodes(t *testing.T) {
	nodes := ParseNodes(sampleNodes)
	if len(nodes) != 4 {
		t.Fatalf("got %d nodes, want 4", len(nodes))
	}
	byID := map[string]Node{}
	for _, n := range nodes {
		byID[n.ID] = n
	}

	self := byID["e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca"]
	if !self.Has("myself") || !self.IsMaster() || self.Addr != "10.0.0.1:6379" || self.Hostname != "redis-0.redis" {
		t.Errorf("unexpected myself node: %+v", self)
	}
	if !reflect.DeepEqual(self.Slots, []SlotRange{{0, 5460}}) {
		t.Errorf("migration marker must be ignored, got slots %v", self.Slots)
	}

	replica := byID["07c37dfeb235213a872192d90877d0cd55635b91"]
	if replica.IsMaster() || replica.MasterID != self.ID || replica.ConfigEpoch != 4 {
		t.Errorf("unexpected replica: %+v", replica)
	}

	failed := byID["67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1"]
	if !failed.Failed() || failed.LinkState != "disconnected" || CountSlots(failed.Slots) != 5463 {
		t.Errorf("unexpected failed master: %+v", failed)
	}
}

func TestSplitSlots(t *testing.T) {
	got := SplitSlots(3)
	want := []SlotRange{{0, 5460}, {5461, 10921}, {10922, 16383}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SplitSlots(3) = %v, want %v", got, want)
	}
	if CountSlots(got) != TotalSlots {
		t.Errorf("split does not cover all slots")
	}
}

func TestSlotOwnersAndRanges(t *testing.T) {
	nodes := []Node{{Slots: []SlotRange{{0, 99}}}, {Slots: []SlotRange{{200, 16383}}}}
	owners := SlotOwners(nodes)
	if owners[0] != 0 || owners[100] != -1 || owners[200] != 1 {
		t.Fatalf("unexpected owners: %d %d %d", owners[0], owners[100], owners[200])
	}
	unassigned := Ranges(func(s int) bool { return owners[s] < 0 })
	if !reflect.DeepEqual(unassigned, []SlotRange{{100, 199}}) {
		t.Errorf("unassigned = %v", unassigned)
	}
}
