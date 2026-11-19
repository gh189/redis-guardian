package cluster

import "strings"

func KeySlot(key string) int {
	if s := strings.IndexByte(key, '{'); s >= 0 {
		if e := strings.IndexByte(key[s+1:], '}'); e > 0 {
			key = key[s+1 : s+1+e]
		}
	}
	return int(crc16(key)) % TotalSlots
}

func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func SlotSet(ranges []SlotRange) []bool {
	set := make([]bool, TotalSlots)
	for _, r := range ranges {
		for s := max(r.Start, 0); s <= r.End && s < TotalSlots; s++ {
			set[s] = true
		}
	}
	return set
}

func Covers(outer, inner []SlotRange) bool {
	set := SlotSet(outer)
	for _, r := range inner {
		for s := r.Start; s <= r.End; s++ {
			if s < 0 || s >= TotalSlots || !set[s] {
				return false
			}
		}
	}
	return true
}

func Overlaps(a, b []SlotRange) bool {
	set := SlotSet(a)
	for _, r := range b {
		for s := max(r.Start, 0); s <= r.End && s < TotalSlots; s++ {
			if set[s] {
				return true
			}
		}
	}
	return false
}
