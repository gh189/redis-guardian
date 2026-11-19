package cluster

import (
	"strings"
	"testing"
)

func TestKeySlot(t *testing.T) {
	if got := crc16("123456789"); got != 0x31C3 {
		t.Fatalf("crc16 = %#x, want 0x31C3 (Redis Cluster spec test vector)", got)
	}
	cases := map[string]int{
		"{user:42}:balance": 15880,
		"{user:42}:profile": 15880,
		"user:42:balance":   2560,
		"foo{}bar":          14292,
		"{a}{b}":            15495,
		"{a}{c}":            15495,
		"{unclosed":         KeySlot("{unclosed"),
	}
	for key, want := range cases {
		if got := KeySlot(key); got != want {
			t.Errorf("KeySlot(%q) = %d, want %d", key, got, want)
		}
	}
	if KeySlot("{a}{b}") != KeySlot("a") {
		t.Error("a hash tag must hash like its body")
	}
	bin := "\x00\xff{\x01}\x02"
	if KeySlot(bin) != KeySlot("\x01") {
		t.Error("binary keys must be hashed byte by byte")
	}
	if s := KeySlot(strings.Repeat("x", 1000)); s < 0 || s >= TotalSlots {
		t.Errorf("slot out of range: %d", s)
	}
}

func TestCoversAndOverlaps(t *testing.T) {
	node := []SlotRange{{0, 5460}, {10923, 16383}}
	if !Covers(node, []SlotRange{{100, 200}, {16000, 16383}}) {
		t.Error("ranges inside the node's slots must be covered")
	}
	if Covers(node, []SlotRange{{5000, 6000}}) {
		t.Error("a range leaving the node's slots is not covered")
	}
	if !Overlaps(node, []SlotRange{{5460, 5461}}) || Overlaps(node, []SlotRange{{5461, 10922}}) {
		t.Error("Overlaps must detect a single shared slot and nothing else")
	}
}

func TestExpireAt(t *testing.T) {
	if _, exists := expireAt(-2); exists {
		t.Error("-2 means the key is gone")
	}
	if at, exists := expireAt(-1); !exists || at != 0 {
		t.Error("-1 means no expiry, restored with 0")
	}
	if at, exists := expireAt(1794932141798); !exists || at != 1794932141798 {
		t.Error("an absolute expiry is passed through")
	}
}
