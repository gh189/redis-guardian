package guardian

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

const (
	chunkSize          = 8 << 20
	backupStatusPeriod = 10 * time.Second
)

type backupPart struct {
	Part     int                 `json:"part"`
	Slots    []cluster.SlotRange `json:"slots"`
	MasterID string              `json:"master_id"`
	Node     string              `json:"node"`
	NodeID   string              `json:"node_id"`
}

type BackupStatus struct {
	Initiates      bool   `json:"initiates"`
	LastComplete   int64  `json:"last_complete_seq,omitempty"`
	LastCompleteAt string `json:"last_complete_at,omitempty"`
	AgeSeconds     int64  `json:"age_seconds,omitempty"`
	LastError      string `json:"last_error,omitempty"`
	LastErrorAt    string `json:"last_error_at,omitempty"`

	lastCompleteAt time.Time
}

type captureRequest struct {
	Seq      int64         `json:"seq"`
	Part     backupPart    `json:"part"`
	Parts    int           `json:"parts"`
	Interval time.Duration `json:"interval"`
	Deadline time.Time     `json:"deadline"`
}

func planBackup(v observation) ([]backupPart, bool) {
	self := v.Self
	if !self.Assigned() || self.ClusterInfo["cluster_state"] != "ok" {
		return nil, false
	}
	for _, o := range cluster.SlotOwners(self.Nodes) {
		if o < 0 || self.Nodes[o].Failed() || self.Nodes[o].Has("noaddr") {
			return nil, false
		}
	}
	var masters []cluster.Node
	for _, n := range self.Nodes {
		if n.IsMaster() && len(n.Slots) > 0 {
			masters = append(masters, n)
		}
	}
	sort.Slice(masters, func(i, j int) bool { return masters[i].Slots[0].Start < masters[j].Slots[0].Start })

	live := v.live()
	plan := make([]backupPart, 0, len(masters))
	for i, m := range masters {
		part := backupPart{Part: i, Slots: m.Slots, MasterID: m.ID}
		var best *peerState
		for _, r := range self.Nodes {
			name, alive := live[r.ID]
			if r.MasterID != m.ID || !alive {
				continue
			}
			p := v.Peers[name]
			if p.Redis.Role != "replica" || p.Redis.MasterID != m.ID || !p.Redis.MasterLinkUp {
				continue
			}
			if best == nil || p.Redis.ReplOffset > best.Redis.ReplOffset ||
				(p.Redis.ReplOffset == best.Redis.ReplOffset && p.Name < best.Name) {
				best = p
			}
		}
		switch name, alive := live[m.ID]; {
		case best != nil:
			part.Node, part.NodeID = best.Name, best.Redis.ID
		case alive:
			part.Node, part.NodeID = name, m.ID
		default:
			return nil, false
		}
		plan = append(plan, part)
	}
	return plan, len(plan) > 0
}

func uploadDelay(part, parts int, interval time.Duration) time.Duration {
	if parts <= 0 {
		return 0
	}
	return interval * time.Duration(part) / time.Duration(2*parts)
}

func (g *Guardian) backupLoop(ctx context.Context) {
	var refreshed time.Time
	for {
		if g.cfg.BackupEnabled {
			g.backupTick(ctx)
		}
		if time.Since(refreshed) >= backupStatusPeriod {
			g.refreshBackupStatus(ctx)
			refreshed = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(g.cfg.Interval):
		}
	}
}

func (g *Guardian) backupTick(ctx context.Context) {
	v, _ := g.snapshot()
	plan, ok := planBackup(v)
	if !ok {
		return
	}
	seq, deadline, started, err := g.store.startGeneration(ctx, g.cfg.BackupInterval, g.cfg.Self, v.Self.Info["redis_version"], plan)
	if err != nil {
		g.backupError(fmt.Errorf("cannot start a backup: %w", err))
		return
	}
	if !started {
		return
	}
	g.event("backup-start", fmt.Sprintf("generation %d: %d parts", seq, len(plan)))
	for _, part := range plan {
		req := captureRequest{Seq: seq, Part: part, Parts: len(plan), Interval: g.cfg.BackupInterval, Deadline: deadline}
		if part.Node == g.cfg.Self {
			go g.capture(req)
			continue
		}
		go func() {
			if err := g.call(ctx, part.Node, http.MethodPost, "/internal/backup/capture", req, nil); err != nil {
				slog.Warn("backup: capture request failed", "seq", seq, "node", part.Node, "err", err)
			}
		}()
	}
	if err := g.store.cleanup(ctx, g.cfg.BackupRetain); err != nil {
		g.backupError(fmt.Errorf("cleaning up old backups: %w", err))
	}
}

func (g *Guardian) capture(req captureRequest) {
	ctx, cancel := context.WithDeadline(g.life, req.Deadline)
	defer cancel()
	what := fmt.Sprintf("generation %d part %d", req.Seq, req.Part.Part)
	fail := func(err error) {
		g.backupError(fmt.Errorf("%s: %w", what, err))
		g.event("backup-failed", fmt.Sprintf("%s: %v", what, err))
	}

	rec, file, err := g.snapshotRDB(ctx, req)
	if file != "" {
		defer os.Remove(file)
	}
	if err != nil {
		fail(err)
		return
	}
	g.event("backup-captured", fmt.Sprintf("%s (%d keys)", what, rec.Keys))

	select {
	case <-ctx.Done():
		fail(errors.New("deadline passed before the upload"))
		return
	case <-time.After(uploadDelay(req.Part.Part, req.Parts, req.Interval)):
	}
	complete, gone, err := g.uploadPart(ctx, rec, file)
	switch {
	case err != nil:
		fail(fmt.Errorf("upload: %w", err))
	case gone:
		fail(errors.New("the generation timed out before the upload finished"))
	default:
		g.event("backup-uploaded", what)
		if complete {
			g.event("backup-complete", fmt.Sprintf("generation %d", req.Seq))
		}
	}
}

func (g *Guardian) snapshotRDB(ctx context.Context, req captureRequest) (partRecord, string, error) {
	p := req.Part
	st := g.redis.Inspect(ctx)
	switch {
	case !st.Ready:
		return partRecord{}, "", errors.New("local redis is not ready")
	case st.ID != p.NodeID:
		return partRecord{}, "", fmt.Errorf("local redis is %s, the plan expects %s", short(st.ID), short(p.NodeID))
	case st.ID == p.MasterID && st.Role != "master",
		st.ID != p.MasterID && (st.Role != "replica" || st.MasterID != p.MasterID):
		return partRecord{}, "", errors.New("local redis no longer has the role the plan expects")
	}

	g.saveMu.Lock()
	defer g.saveMu.Unlock()
	info, err := g.redis.Persistence(ctx)
	if err != nil {
		return partRecord{}, "", err
	}
	if info["rdb_bgsave_in_progress"] != "0" {
		return partRecord{}, "", errors.New("another BGSAVE is in progress")
	}
	before, _ := strconv.ParseInt(info["rdb_saves"], 10, 64)
	captured := time.Now()
	if err := g.redis.BGSave(ctx); err != nil {
		return partRecord{}, "", fmt.Errorf("BGSAVE: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return partRecord{}, "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		if info, err = g.redis.Persistence(ctx); err != nil {
			return partRecord{}, "", err
		}
		saves, _ := strconv.ParseInt(info["rdb_saves"], 10, 64)
		if info["rdb_bgsave_in_progress"] == "0" && saves > before {
			break
		}
	}
	if info["rdb_last_bgsave_status"] != "ok" {
		return partRecord{}, "", fmt.Errorf("BGSAVE failed: %s", info["rdb_last_bgsave_status"])
	}
	dir, err := g.redis.ConfigGet(ctx, "dir")
	if err != nil {
		return partRecord{}, "", err
	}
	dbfile, err := g.redis.ConfigGet(ctx, "dbfilename")
	if err != nil {
		return partRecord{}, "", err
	}
	backupDir := filepath.Join(g.cfg.DataDir, "backup")
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		return partRecord{}, "", err
	}
	file := filepath.Join(backupDir, fmt.Sprintf("%d-%d.rdb", req.Seq, p.Part))
	if err := os.Rename(filepath.Join(dir, dbfile), file); err != nil {
		return partRecord{}, "", err
	}
	return partRecord{
		Seq: req.Seq, Part: p.Part, Slots: p.Slots, Node: g.cfg.Self, NodeID: st.ID, Role: st.Role,
		CapturedAt: captured, ReplOffset: st.ReplOffset, Keys: st.Keys,
	}, file, nil
}

func (g *Guardian) uploadPart(ctx context.Context, rec partRecord, file string) (complete, gone bool, err error) {
	f, err := os.Open(file)
	if err != nil {
		return false, false, err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, chunkSize)
	for {
		n, rerr := io.ReadFull(f, buf)
		if n > 0 {
			h.Write(buf[:n])
			if err := g.store.putChunk(ctx, rec.Seq, rec.Part, rec.Chunks, buf[:n]); err != nil {
				return false, false, err
			}
			rec.Chunks++
			rec.Bytes += int64(n)
		}
		if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) {
			break
		}
		if rerr != nil {
			return false, false, rerr
		}
	}
	rec.SHA256 = hex.EncodeToString(h.Sum(nil))
	return g.store.commitPart(ctx, rec)
}

func (g *Guardian) Backups(ctx context.Context) ([]BackupInfo, error) {
	if g.store == nil {
		return nil, errors.New("backups are not configured (BACKUP_PG_DSN)")
	}
	return g.store.listGenerations(ctx, 2*g.cfg.BackupRetain)
}

func (g *Guardian) refreshBackupStatus(ctx context.Context) {
	seq, _, at, ok, err := g.store.latestComplete(ctx)
	if err != nil {
		g.backupError(fmt.Errorf("reading backups: %w", err))
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if ok {
		g.backup.LastComplete, g.backup.lastCompleteAt = seq, at
	}
}

func (g *Guardian) backupError(err error) {
	slog.Warn("backup", "err", err)
	g.mu.Lock()
	g.backup.LastError, g.backup.LastErrorAt = err.Error(), time.Now().Format(time.RFC3339)
	g.mu.Unlock()
}

func (g *Guardian) backupStatus() *BackupStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	b := g.backup
	b.Initiates = g.cfg.BackupEnabled
	if !b.lastCompleteAt.IsZero() {
		b.LastCompleteAt = b.lastCompleteAt.Format(time.RFC3339)
		b.AgeSeconds = int64(time.Since(b.lastCompleteAt).Seconds())
	}
	return &b
}
