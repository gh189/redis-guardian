package guardian

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gh189/redis-guardian/cluster"
)

func testStore(t *testing.T) *store {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	s, err := openStore(dsn, fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	return s
}

func slots(start, end int) []cluster.SlotRange { return []cluster.SlotRange{{Start: start, End: end}} }

func TestStoreGenerations(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	plan := []backupPart{{Part: 0, Slots: slots(0, 8191)}, {Part: 1, Slots: slots(8192, 16383)}}
	if err := s.cleanup(ctx, 3); err != nil {
		t.Fatalf("cleanup of a fresh namespace (no failed restore to protect) must succeed: %v", err)
	}

	seq, _, ok, err := s.startGeneration(ctx, time.Hour, "a", "8.8.3", plan)
	if err != nil || !ok {
		t.Fatalf("first guardian must start the generation: ok=%v err=%v", ok, err)
	}
	if _, _, ok, _ := s.startGeneration(ctx, time.Hour, "b", "8.8.3", plan); ok {
		t.Error("a second guardian must not start the same generation")
	}

	for part := range 2 {
		if err := s.putChunk(ctx, seq, part, 0, []byte("rdb")); err != nil {
			t.Fatal(err)
		}
		complete, gone, err := s.commitPart(ctx, partRecord{Seq: seq, Part: part, Slots: plan[part].Slots, Chunks: 1, Bytes: 3})
		if err != nil || gone || complete != (part == 1) {
			t.Fatalf("part %d: complete=%v gone=%v err=%v", part, complete, gone, err)
		}
	}
	got, _, _, ok, err := s.latestComplete(ctx)
	if err != nil || !ok || got != seq {
		t.Fatalf("latestComplete = %d %v %v", got, ok, err)
	}

	if _, err := s.pool.Exec(ctx, `UPDATE backup_generation SET status = 'failed' WHERE cluster = $1`, s.cluster); err != nil {
		t.Fatal(err)
	}
	s.putChunk(ctx, seq, 2, 0, []byte("late"))
	if _, gone, _ := s.commitPart(ctx, partRecord{Seq: seq, Part: 2}); !gone {
		t.Error("a part of a failed generation must be refused")
	}
	var chunks int
	s.pool.QueryRow(ctx, `SELECT count(*) FROM backup_chunk WHERE cluster = $1 AND part = 2`, s.cluster).Scan(&chunks)
	if chunks != 0 {
		t.Error("the refused part's chunks must be removed")
	}
}

func TestStoreNoBackupDuringRestore(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	if _, err := s.addJob(ctx, 1, slots(0, 100)); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := s.startGeneration(ctx, time.Hour, "a", "8.8.3", nil); err != nil || ok {
		t.Fatalf("no generation may start while a restore is unfinished: ok=%v err=%v", ok, err)
	}
}

func TestStoreJobLifecycle(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	id, err := s.addJob(ctx, 7, slots(0, 5460))
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.claimJob(ctx, id, "node-a"); !ok {
		t.Fatal("claim")
	}
	if ok, _ := s.claimJob(ctx, id, "node-b"); !ok {
		t.Fatal("takeover")
	}
	if ok, _ := s.jobProgress(ctx, id, "node-a", 1, 0); ok {
		t.Error("the previous executor must not update the job any more")
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		s.claimJob(ctx, id, "node-b")
		status, attempts, err := s.failJob(ctx, id, "node-b", "boom")
		want := "pending"
		if attempt == maxAttempts {
			want = "failed"
		}
		if err != nil || attempts != attempt || status != want {
			t.Fatalf("attempt %d: status=%q attempts=%d err=%v", attempt, status, attempts, err)
		}
	}
	if ok, _ := s.claimJob(ctx, id, "node-b"); ok {
		t.Error("a failed job must not run again")
	}
}

func TestStoreStalledJobs(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	running, _ := s.addJob(ctx, 1, slots(0, 10))
	queued, _ := s.addJob(ctx, 1, slots(11, 20))
	s.claimJob(ctx, running, "node-a")
	if _, err := s.pool.Exec(ctx, `UPDATE restore_job SET updated_at = now() - interval '11 minutes' WHERE cluster = $1`, s.cluster); err != nil {
		t.Fatal(err)
	}
	if err := s.touchJobs(ctx, []int64{queued}); err != nil {
		t.Fatal(err)
	}
	stalled, err := s.failStalled(ctx, stallLimit)
	if err != nil || len(stalled) != 1 || stalled[0].ID != running || stalled[0].Status != "pending" || stalled[0].Attempts != 1 {
		t.Fatalf("stalled = %+v, err = %v", stalled, err)
	}
	if again, _ := s.failStalled(ctx, stallLimit); len(again) != 0 {
		t.Error("a stall is counted once")
	}
}

func TestStoreReplaceJobs(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	old, _ := s.addJob(ctx, 1, slots(0, 16383))
	cancelled, err := s.replaceJobs(ctx, 9, [][]cluster.SlotRange{slots(0, 5460), slots(5461, 10922), slots(10923, 16383)})
	if err != nil || cancelled != 1 {
		t.Fatalf("cancelled = %d, err = %v", cancelled, err)
	}
	jobs, _ := s.unfinishedJobs(ctx)
	if len(jobs) != 3 {
		t.Fatalf("jobs = %+v", jobs)
	}
	for _, j := range jobs {
		if j.ID == old || j.Seq != 9 {
			t.Errorf("unexpected job %+v", j)
		}
	}
}
