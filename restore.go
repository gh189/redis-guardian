package guardian

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

const (
	maxAttempts       = 3
	restorePoll       = 10 * time.Second
	stallLimit        = 10 * time.Minute
	progressEvery     = time.Minute
	servingWait       = 2 * time.Minute
	scanCount         = 500
	dumpBatch         = 50
	restoreBatchKeys  = 500
	restoreBatchBytes = 64 << 20
)

var errNotOurs = errors.New("the job is no longer this node's to run")

type RestoreStatus struct {
	Running    bool                `json:"running"`
	Queued     int                 `json:"queued"`
	JobID      int64               `json:"job_id,omitempty"`
	Seq        int64               `json:"seq,omitempty"`
	Slots      []cluster.SlotRange `json:"slots,omitempty"`
	Phase      string              `json:"phase,omitempty"`
	StartedAt  string              `json:"started_at,omitempty"`
	Restored   int64               `json:"restored"`
	Skipped    int64               `json:"skipped"`
	Scanned    int64               `json:"scanned"`
	Total      int64               `json:"total"`
	LastResult string              `json:"last_result,omitempty"`
	LastError  string              `json:"last_error,omitempty"`

	queuedIDs []int64
}

func (s *RestoreStatus) active() bool { return s != nil && (s.Running || s.Queued > 0) }

func (g *Guardian) registerShardRestore(ctx context.Context, slots []cluster.SlotRange, localVersion string) {
	if g.store == nil {
		return
	}
	jobs, err := g.store.unfinishedJobs(ctx)
	if err != nil {
		g.event("restore-skipped", "cannot reach PostgreSQL: "+err.Error())
		return
	}
	var covered []cluster.SlotRange
	for _, j := range jobs {
		covered = append(covered, j.Slots...)
	}
	have, want := cluster.SlotSet(covered), cluster.SlotSet(slots)
	rest := cluster.Ranges(func(s int) bool { return want[s] && !have[s] })
	if len(rest) == 0 {
		return
	}
	seq, ok := g.pickBackup(ctx, localVersion)
	if !ok {
		return
	}
	id, err := g.store.addJob(ctx, seq, rest)
	if err != nil {
		g.event("restore-skipped", "cannot record the restore: "+err.Error())
		return
	}
	slog.Info("restore registered", "job", id, "seq", seq, "slots", rest)
}

func (g *Guardian) prepareBootstrapRestore(ctx context.Context, slots []cluster.SlotRange, localVersion string) {
	if g.store == nil {
		return
	}
	var seq int64
	var jobs [][]cluster.SlotRange
	if g.cfg.RestoreOnBootstrap {
		var ok bool
		if seq, ok = g.pickBackup(ctx, localVersion); ok {
			for _, r := range slots {
				jobs = append(jobs, []cluster.SlotRange{r})
			}
		}
	}
	cancelled, err := g.store.replaceJobs(ctx, seq, jobs)
	if err != nil {
		g.event("restore-skipped", "cannot record the restore: "+err.Error())
		return
	}
	if cancelled > 0 {
		g.event("restore-cancelled", fmt.Sprintf("%d unfinished jobs of the previous cluster", cancelled))
	}
	if len(jobs) > 0 {
		slog.Info("restore registered for the new cluster", "seq", seq, "jobs", len(jobs))
	}
}

func (g *Guardian) pickBackup(ctx context.Context, localVersion string) (int64, bool) {
	seq, version, _, ok, err := g.store.latestComplete(ctx)
	switch {
	case err != nil:
		g.event("restore-skipped", "cannot reach PostgreSQL: "+err.Error())
	case !ok:
		g.event("restore-skipped", "no complete backup")
	case versionNewer(version, localVersion):
		g.event("restore-skipped", fmt.Sprintf("backup %d comes from redis %s, newer than the local %s", seq, version, localVersion))
	default:
		return seq, true
	}
	return 0, false
}

func versionNewer(a, b string) bool {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := range max(len(as), len(bs)) {
		var x, y int
		if i < len(as) {
			x, _ = strconv.Atoi(as[i])
		}
		if i < len(bs) {
			y, _ = strconv.Atoi(bs[i])
		}
		if x != y {
			return x > y
		}
	}
	return false
}

func (g *Guardian) restoreLoop(ctx context.Context) {
	for {
		g.restoreTick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(restorePoll):
		case <-g.restoreKick:
		}
	}
}

func (g *Guardian) kickRestore() {
	select {
	case g.restoreKick <- struct{}{}:
	default:
	}
}

func (g *Guardian) restoreTick(ctx context.Context) {
	self := g.redis.Inspect(ctx)
	if !self.Ready || self.Role != "master" || len(self.Slots) == 0 {
		g.setQueued(nil)
		return
	}
	stalled, err := g.store.failStalled(ctx, stallLimit)
	if err != nil {
		slog.Warn("restore: cannot reach PostgreSQL", "err", err)
		return
	}
	for _, j := range stalled {
		g.event("restore-failed", fmt.Sprintf("job %d: %s (attempt %d/%d, now %s)", j.ID, j.Error, j.Attempts, maxAttempts, j.Status))
	}
	jobs, err := g.store.unfinishedJobs(ctx)
	if err != nil {
		slog.Warn("restore: cannot reach PostgreSQL", "err", err)
		return
	}
	var mine []restoreJob
	for _, j := range jobs {
		if cluster.Covers(self.Slots, j.Slots) {
			mine = append(mine, j)
		}
	}
	g.setQueued(mine)
	if len(mine) == 0 || !g.restoreBusy.CompareAndSwap(false, true) {
		return
	}
	g.work.Add(1)
	go func() {
		defer g.work.Done()
		defer g.restoreBusy.Store(false)
		g.runRestore(mine[0])
	}()
}

func (g *Guardian) setQueued(jobs []restoreJob) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.restore.queuedIDs = g.restore.queuedIDs[:0]
	for _, j := range jobs {
		if !(g.restore.Running && j.ID == g.restore.JobID) {
			g.restore.queuedIDs = append(g.restore.queuedIDs, j.ID)
		}
	}
	g.restore.Queued = len(g.restore.queuedIDs)
}

func (g *Guardian) updateRestore(fn func(s *RestoreStatus)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	fn(&g.restore)
}

func (g *Guardian) restoreStatus() *RestoreStatus {
	if g.store == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	s := g.restore
	s.Slots = slices.Clone(s.Slots)
	s.queuedIDs = nil
	return &s
}

func (g *Guardian) runRestore(job restoreJob) {
	ctx := g.life
	g.updateRestore(func(s *RestoreStatus) {
		*s = RestoreStatus{Running: true, JobID: job.ID, Seq: job.Seq, Slots: job.Slots, Phase: "waiting",
			StartedAt: time.Now().Format(time.RFC3339), LastResult: s.LastResult, LastError: s.LastError,
			queuedIDs: slices.DeleteFunc(s.queuedIDs, func(id int64) bool { return id == job.ID })}
		s.Queued = len(s.queuedIDs)
	})
	nodeID, err := g.executeRestore(ctx, job)
	result := "done"
	switch {
	case err == nil:
		g.event("restore-done", fmt.Sprintf("job %d: slots %v from backup %d", job.ID, job.Slots, job.Seq))
	case errors.Is(err, errNotOurs), ctx.Err() != nil:
		result = "stopped"
		slog.Info("restore stopped", "job", job.ID, "reason", err)
	default:
		status, attempts, ferr := g.store.failJob(context.Background(), job.ID, nodeID, err.Error())
		result = "failed"
		detail := fmt.Sprintf("job %d: %v", job.ID, err)
		if ferr == nil && status != "" {
			detail += fmt.Sprintf(" (attempt %d/%d, now %s)", attempts, maxAttempts, status)
		}
		g.event("restore-failed", detail)
	}
	g.updateRestore(func(s *RestoreStatus) {
		s.Running, s.Phase, s.LastResult = false, "", result
		if err != nil && result == "failed" {
			s.LastError = err.Error()
		}
	})
}

func (g *Guardian) executeRestore(ctx context.Context, job restoreJob) (string, error) {
	st, err := g.waitServing(ctx, job.Slots)
	if err != nil {
		return st.ID, err
	}
	nodeID := st.ID
	if ok, err := g.store.claimJob(ctx, job.ID, nodeID); err != nil || !ok {
		return nodeID, cmpErr(err, errNotOurs)
	}
	g.event("restore-start", fmt.Sprintf("job %d: slots %v from backup %d", job.ID, job.Slots, job.Seq))

	parts, err := g.store.parts(ctx, job.Seq)
	if err != nil {
		return nodeID, err
	}
	var available []cluster.SlotRange
	for _, p := range parts {
		available = append(available, p.Slots...)
	}
	if !cluster.Covers(available, job.Slots) {
		return nodeID, fmt.Errorf("backup %d does not cover the slots of the job", job.Seq)
	}

	local := cluster.NewRestoreClient("tcp", "127.0.0.1:"+strconv.Itoa(g.cfg.redisPort()))
	defer local.Close()
	r := &restoreRun{g: g, job: job, nodeID: nodeID, local: local, want: cluster.SlotSet(job.Slots), saved: time.Now()}
	for _, p := range parts {
		if cluster.Overlaps(p.Slots, job.Slots) {
			if err := r.part(ctx, p); err != nil {
				return nodeID, err
			}
		}
	}
	if ok, err := g.store.finishJob(ctx, job.ID, nodeID, r.restored, r.skipped); err != nil || !ok {
		return nodeID, cmpErr(err, errNotOurs)
	}
	return nodeID, nil
}

func cmpErr(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

func (g *Guardian) waitServing(ctx context.Context, slots []cluster.SlotRange) (cluster.State, error) {
	deadline := time.Now().Add(servingWait)
	for {
		st := g.redis.Inspect(ctx)
		if !st.Ready || st.Role != "master" || !cluster.Covers(st.Slots, slots) {
			return st, errNotOurs
		}
		if st.ClusterInfo["cluster_state"] == "ok" {
			return st, nil
		}
		if time.Now().After(deadline) {
			return st, errors.New("cluster_state did not become ok")
		}
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

type restoreRun struct {
	g      *Guardian
	job    restoreJob
	nodeID string
	local  *cluster.Client
	want   []bool

	restored, skipped int64
	saved             time.Time
}

func (r *restoreRun) tick(ctx context.Context) error {
	if time.Since(r.saved) < progressEvery {
		return nil
	}
	r.saved = time.Now()
	ok, err := r.g.store.jobProgress(ctx, r.job.ID, r.nodeID, r.restored, r.skipped)
	if err != nil || !ok {
		return cmpErr(err, errNotOurs)
	}
	r.g.mu.RLock()
	queued := slices.Clone(r.g.restore.queuedIDs)
	r.g.mu.RUnlock()
	if len(queued) > 0 {
		return r.g.store.touchJobs(ctx, queued)
	}
	return nil
}

func (r *restoreRun) phase(name string) {
	r.g.updateRestore(func(s *RestoreStatus) { s.Phase = name })
}

func (r *restoreRun) part(ctx context.Context, p partRecord) error {
	g := r.g
	dir := filepath.Join(g.cfg.DataDir, "restore")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := cluster.GiveToRedis(dir); err != nil {
		return err
	}
	file := filepath.Join(dir, fmt.Sprintf("%d-%d.rdb", p.Seq, p.Part))
	defer os.Remove(file)

	r.phase("downloading")
	if err := r.download(ctx, p, file); err != nil {
		return err
	}
	if err := cluster.GiveToRedis(file); err != nil {
		return err
	}

	r.phase("loading")
	sock := filepath.Join(dir, "redis.sock")
	args := append(slices.Clone(g.cfg.RedisArgs),
		"--cluster-enabled", "no", "--port", "0", "--unixsocket", sock, "--unixsocketperm", "700",
		"--save", "", "--appendonly", "no", "--maxmemory", "0", "--dir", dir, "--dbfilename", filepath.Base(file))
	in, err := cluster.StartInstance("redis-server", args)
	if err != nil {
		return fmt.Errorf("starting a temporary redis-server: %w", err)
	}
	defer in.Stop()
	tmp := cluster.NewRestoreClient("unix", sock)
	defer tmp.Close()
	for tmp.Ping(ctx) != nil {
		if in.Exited() {
			return errors.New("the temporary redis-server exited while loading the backup (see its log)")
		}
		if err := r.tick(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	total, err := tmp.DBSize(ctx)
	if err != nil {
		return err
	}
	g.updateRestore(func(s *RestoreStatus) { s.Total += total })

	r.phase("restoring")
	return r.copyKeys(ctx, tmp)
}

func (r *restoreRun) download(ctx context.Context, p partRecord, file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if err := r.g.store.readPart(ctx, p, io.MultiWriter(f, h), func() error { return r.tick(ctx) }); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if fi, err := os.Stat(file); err != nil || fi.Size() != p.Bytes {
		return fmt.Errorf("part %d of backup %d: wrong size", p.Part, p.Seq)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); sum != p.SHA256 {
		return fmt.Errorf("part %d of backup %d: checksum mismatch", p.Part, p.Seq)
	}
	return nil
}

func (r *restoreRun) copyKeys(ctx context.Context, tmp *cluster.Client) error {
	var batch []cluster.DumpedKey
	var batchBytes int
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		restored, existing, err := r.local.RestoreKeys(ctx, batch)
		r.restored += int64(restored)
		r.skipped += int64(existing)
		batch, batchBytes = batch[:0], 0
		return err
	}
	var cursor uint64
	for {
		keys, next, err := tmp.Scan(ctx, cursor, scanCount)
		if err != nil {
			return err
		}
		mine := slices.DeleteFunc(keys, func(k string) bool { return !r.want[cluster.KeySlot(k)] })
		for len(mine) > 0 {
			n := min(dumpBatch, len(mine))
			dumped, err := tmp.DumpKeys(ctx, mine[:n])
			if err != nil {
				return err
			}
			r.skipped += int64(n - len(dumped))
			mine = mine[n:]
			for _, d := range dumped {
				if len(batch) > 0 && (len(batch) >= restoreBatchKeys || batchBytes+len(d.Payload) > restoreBatchBytes) {
					if err := flush(); err != nil {
						return err
					}
				}
				batch = append(batch, d)
				batchBytes += len(d.Payload)
			}
		}
		r.g.updateRestore(func(s *RestoreStatus) {
			s.Scanned += int64(len(keys))
			s.Restored, s.Skipped = r.restored, r.skipped
		})
		if err := r.tick(ctx); err != nil {
			return err
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if err := flush(); err != nil {
		return err
	}
	r.g.updateRestore(func(s *RestoreStatus) { s.Restored, s.Skipped = r.restored, r.skipped })
	return nil
}
