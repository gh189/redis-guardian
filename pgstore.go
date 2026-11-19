package guardian

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gh189/redis-guardian/cluster"
)

const (
	pgTimeout   = 5 * time.Second
	pgDataLimit = 60 * time.Second
)

type store struct {
	pool    *pgxpool.Pool
	cluster string

	mu    sync.Mutex
	ready bool
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS backup_generation (
		cluster       text        NOT NULL,
		seq           bigint      NOT NULL,
		initiator     text        NOT NULL,
		started_at    timestamptz NOT NULL,
		deadline_at   timestamptz NOT NULL,
		status        text        NOT NULL,
		completed_at  timestamptz,
		redis_version text        NOT NULL,
		slot_map      jsonb       NOT NULL,
		error         text        NOT NULL DEFAULT '',
		PRIMARY KEY (cluster, seq))`,
	`CREATE TABLE IF NOT EXISTS backup_part (
		cluster     text        NOT NULL,
		seq         bigint      NOT NULL,
		part        int         NOT NULL,
		slots       jsonb       NOT NULL,
		node        text        NOT NULL,
		node_id     text        NOT NULL,
		role        text        NOT NULL,
		captured_at timestamptz NOT NULL,
		repl_offset bigint      NOT NULL,
		key_count   bigint      NOT NULL,
		bytes       bigint      NOT NULL,
		chunks      int         NOT NULL,
		sha256      text        NOT NULL,
		PRIMARY KEY (cluster, seq, part))`,
	`CREATE TABLE IF NOT EXISTS backup_chunk (
		cluster text   NOT NULL,
		seq     bigint NOT NULL,
		part    int    NOT NULL,
		idx     int    NOT NULL,
		data    bytea  NOT NULL,
		PRIMARY KEY (cluster, seq, part, idx))`,
	`CREATE TABLE IF NOT EXISTS restore_job (
		cluster     text        NOT NULL,
		id          bigserial,
		seq         bigint      NOT NULL,
		slots       jsonb       NOT NULL,
		node_id     text        NOT NULL DEFAULT '',
		status      text        NOT NULL,
		attempts    int         NOT NULL DEFAULT 0,
		restored    bigint      NOT NULL DEFAULT 0,
		skipped     bigint      NOT NULL DEFAULT 0,
		error       text        NOT NULL DEFAULT '',
		created_at  timestamptz NOT NULL DEFAULT now(),
		started_at  timestamptz,
		updated_at  timestamptz NOT NULL DEFAULT now(),
		finished_at timestamptz,
		PRIMARY KEY (cluster, id))`,
}

func openStore(dsn, cluster string) (*store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("BACKUP_PG_DSN: %w", err)
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("BACKUP_PG_DSN: %w", err)
	}
	return &store{pool: pool, cluster: cluster}, nil
}

func (s *store) close() { s.pool.Close() }

func (s *store) ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ready {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, pgTimeout)
	defer cancel()
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('redis-guardian schema'))`); err != nil {
			return err
		}
		for _, stmt := range schema {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}
	s.ready = true
	return nil
}

func (s *store) query(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, pgTimeout)
	defer cancel()
	return fn(ctx)
}

func (s *store) startGeneration(ctx context.Context, interval time.Duration, initiator, version string, plan []backupPart) (seq int64, deadline time.Time, ok bool, err error) {
	secs := max(int(interval/time.Second), 1)
	err = s.query(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
			INSERT INTO backup_generation (cluster, seq, initiator, started_at, deadline_at, status, redis_version, slot_map)
			SELECT $1, floor(extract(epoch FROM now()) / $2::int), $3, now(), now() + $2::int * interval '1 second', 'running', $4, $5
			WHERE NOT EXISTS (SELECT 1 FROM restore_job WHERE cluster = $1 AND status IN ('pending', 'running'))
			ON CONFLICT DO NOTHING
			RETURNING seq, deadline_at`, s.cluster, secs, initiator, version, plan).Scan(&seq, &deadline)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, false, nil
	}
	return seq, deadline, err == nil, err
}

type partRecord struct {
	Seq        int64               `json:"-"`
	Part       int                 `json:"part"`
	Slots      []cluster.SlotRange `json:"slots"`
	Node       string              `json:"node"`
	NodeID     string              `json:"node_id"`
	Role       string              `json:"role"`
	CapturedAt time.Time           `json:"captured_at"`
	ReplOffset int64               `json:"repl_offset"`
	Keys       int64               `json:"keys"`
	Bytes      int64               `json:"bytes"`
	Chunks     int                 `json:"chunks"`
	SHA256     string              `json:"sha256"`
}

func (s *store) putChunk(ctx context.Context, seq int64, part, idx int, data []byte) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, pgDataLimit)
	defer cancel()
	_, err := s.pool.Exec(ctx, `INSERT INTO backup_chunk (cluster, seq, part, idx, data) VALUES ($1, $2, $3, $4, $5)`,
		s.cluster, seq, part, idx, data)
	return err
}

func (s *store) commitPart(ctx context.Context, p partRecord) (complete, gone bool, err error) {
	err = s.query(ctx, func(ctx context.Context) error {
		return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var status string
			var planned int
			err := tx.QueryRow(ctx, `SELECT status, jsonb_array_length(slot_map) FROM backup_generation
				WHERE cluster = $1 AND seq = $2 FOR UPDATE`, s.cluster, p.Seq).Scan(&status, &planned)
			if errors.Is(err, pgx.ErrNoRows) || err == nil && status != "running" {
				gone = true
				_, err = tx.Exec(ctx, `DELETE FROM backup_chunk WHERE cluster = $1 AND seq = $2 AND part = $3`, s.cluster, p.Seq, p.Part)
				return err
			}
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO backup_part (cluster, seq, part, slots, node, node_id, role, captured_at,
				repl_offset, key_count, bytes, chunks, sha256) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT DO NOTHING`, s.cluster, p.Seq, p.Part, p.Slots, p.Node, p.NodeID, p.Role, p.CapturedAt,
				p.ReplOffset, p.Keys, p.Bytes, p.Chunks, p.SHA256); err != nil {
				return err
			}
			var parts int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM backup_part WHERE cluster = $1 AND seq = $2`,
				s.cluster, p.Seq).Scan(&parts); err != nil {
				return err
			}
			if parts < planned {
				return nil
			}
			complete = true
			_, err = tx.Exec(ctx, `UPDATE backup_generation SET status = 'complete', completed_at = now()
				WHERE cluster = $1 AND seq = $2`, s.cluster, p.Seq)
			return err
		})
	})
	return complete, gone, err
}

type generationInfo struct {
	Seq       int64
	Status    string
	StartedAt time.Time
}

func (s *store) cleanup(ctx context.Context, retain int) error {
	return s.query(ctx, func(ctx context.Context) error {
		if _, err := s.pool.Exec(ctx, `UPDATE backup_generation SET status = 'failed', error = 'not complete before its deadline'
			WHERE cluster = $1 AND status = 'running' AND deadline_at < now()`, s.cluster); err != nil {
			return err
		}
		rows, _ := s.pool.Query(ctx, `SELECT seq, status, started_at FROM backup_generation WHERE cluster = $1`, s.cluster)
		gens, err := pgx.CollectRows(rows, pgx.RowToStructByPos[generationInfo])
		if err != nil {
			return err
		}
		protected := int64(-1)
		err = s.pool.QueryRow(ctx, `SELECT seq FROM restore_job WHERE cluster = $1 AND status = 'failed'
			ORDER BY updated_at DESC, id DESC LIMIT 1`, s.cluster).Scan(&protected)
		if errors.Is(err, pgx.ErrNoRows) {
			err = nil
		}
		if err != nil {
			return err
		}
		dropData, dropRows := retention(gens, retain, protected)
		if len(dropData) > 0 {
			for _, table := range []string{"backup_chunk", "backup_part"} {
				if _, err := s.pool.Exec(ctx, `DELETE FROM `+table+` WHERE cluster = $1 AND seq = ANY($2)`, s.cluster, dropData); err != nil {
					return err
				}
			}
		}
		if len(dropRows) > 0 {
			_, err = s.pool.Exec(ctx, `DELETE FROM backup_generation WHERE cluster = $1 AND seq = ANY($2)`, s.cluster, dropRows)
		}
		return err
	})
}

func retention(gens []generationInfo, retain int, protected int64) (dropData, dropRows []int64) {
	sort.Slice(gens, func(i, j int) bool {
		if !gens[i].StartedAt.Equal(gens[j].StartedAt) {
			return gens[i].StartedAt.After(gens[j].StartedAt)
		}
		return gens[i].Seq > gens[j].Seq
	})
	complete, failed := 0, 0
	for _, g := range gens {
		switch g.Status {
		case "complete":
			if complete < retain {
				complete++
				continue
			}
			if g.Seq == protected {
				continue
			}
			dropData = append(dropData, g.Seq)
			dropRows = append(dropRows, g.Seq)
		case "failed":
			dropData = append(dropData, g.Seq)
			if failed++; failed > retain {
				dropRows = append(dropRows, g.Seq)
			}
		}
	}
	return dropData, dropRows
}

func (s *store) latestComplete(ctx context.Context) (seq int64, version string, at time.Time, ok bool, err error) {
	err = s.query(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `SELECT seq, redis_version, completed_at FROM backup_generation
			WHERE cluster = $1 AND status = 'complete' ORDER BY started_at DESC, seq DESC LIMIT 1`, s.cluster).Scan(&seq, &version, &at)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", time.Time{}, false, nil
	}
	return seq, version, at, err == nil, err
}

func (s *store) parts(ctx context.Context, seq int64) ([]partRecord, error) {
	var parts []partRecord
	err := s.query(ctx, func(ctx context.Context) error {
		rows, _ := s.pool.Query(ctx, `SELECT part, slots, node, node_id, role, captured_at, repl_offset, key_count,
			bytes, chunks, sha256 FROM backup_part WHERE cluster = $1 AND seq = $2 ORDER BY part`, s.cluster, seq)
		var err error
		parts, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (partRecord, error) {
			p := partRecord{Seq: seq}
			err := row.Scan(&p.Part, &p.Slots, &p.Node, &p.NodeID, &p.Role, &p.CapturedAt, &p.ReplOffset, &p.Keys,
				&p.Bytes, &p.Chunks, &p.SHA256)
			return p, err
		})
		return err
	})
	return parts, err
}

func (s *store) readPart(ctx context.Context, p partRecord, w io.Writer, progress func() error) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	for idx := range p.Chunks {
		var data []byte
		qctx, cancel := context.WithTimeout(ctx, pgDataLimit)
		err := s.pool.QueryRow(qctx, `SELECT data FROM backup_chunk WHERE cluster = $1 AND seq = $2 AND part = $3 AND idx = $4`,
			s.cluster, p.Seq, p.Part, idx).Scan(&data)
		cancel()
		if err != nil {
			return fmt.Errorf("reading chunk %d of part %d: %w", idx, p.Part, err)
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if err := progress(); err != nil {
			return err
		}
	}
	return nil
}

type BackupInfo struct {
	Seq          int64        `json:"seq"`
	Status       string       `json:"status"`
	Initiator    string       `json:"initiator"`
	StartedAt    time.Time    `json:"started_at"`
	DeadlineAt   time.Time    `json:"deadline_at"`
	CompletedAt  *time.Time   `json:"completed_at,omitempty"`
	RedisVersion string       `json:"redis_version"`
	Error        string       `json:"error,omitempty"`
	Parts        []partRecord `json:"parts"`
}

func (s *store) listGenerations(ctx context.Context, limit int) ([]BackupInfo, error) {
	var gens []BackupInfo
	err := s.query(ctx, func(ctx context.Context) error {
		rows, _ := s.pool.Query(ctx, `SELECT seq, status, initiator, started_at, deadline_at, completed_at, redis_version, error
			FROM backup_generation WHERE cluster = $1 ORDER BY started_at DESC, seq DESC LIMIT $2`, s.cluster, limit)
		var err error
		gens, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (BackupInfo, error) {
			b := BackupInfo{Parts: []partRecord{}}
			err := row.Scan(&b.Seq, &b.Status, &b.Initiator, &b.StartedAt, &b.DeadlineAt, &b.CompletedAt, &b.RedisVersion, &b.Error)
			return b, err
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	for i := range gens {
		if gens[i].Parts, err = s.parts(ctx, gens[i].Seq); err != nil {
			return nil, err
		}
		if gens[i].Parts == nil {
			gens[i].Parts = []partRecord{}
		}
	}
	return gens, nil
}

type restoreJob struct {
	ID        int64               `json:"id"`
	Seq       int64               `json:"seq"`
	Slots     []cluster.SlotRange `json:"slots"`
	NodeID    string              `json:"node_id"`
	Status    string              `json:"status"`
	Attempts  int                 `json:"attempts"`
	Restored  int64               `json:"restored"`
	Skipped   int64               `json:"skipped"`
	Error     string              `json:"error,omitempty"`
	UpdatedAt time.Time           `json:"updated_at"`
}

const jobColumns = `id, seq, slots, node_id, status, attempts, restored, skipped, error, updated_at`

func scanJob(row pgx.CollectableRow) (restoreJob, error) {
	var j restoreJob
	err := row.Scan(&j.ID, &j.Seq, &j.Slots, &j.NodeID, &j.Status, &j.Attempts, &j.Restored, &j.Skipped, &j.Error, &j.UpdatedAt)
	return j, err
}

func (s *store) unfinishedJobs(ctx context.Context) ([]restoreJob, error) {
	var jobs []restoreJob
	err := s.query(ctx, func(ctx context.Context) error {
		rows, _ := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM restore_job
			WHERE cluster = $1 AND status IN ('pending', 'running') ORDER BY id`, s.cluster)
		var err error
		jobs, err = pgx.CollectRows(rows, scanJob)
		return err
	})
	return jobs, err
}

func (s *store) addJob(ctx context.Context, seq int64, slots []cluster.SlotRange) (int64, error) {
	var id int64
	err := s.query(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `INSERT INTO restore_job (cluster, seq, slots, status) VALUES ($1, $2, $3, 'pending')
			RETURNING id`, s.cluster, seq, slots).Scan(&id)
	})
	return id, err
}

func (s *store) replaceJobs(ctx context.Context, seq int64, jobs [][]cluster.SlotRange) (cancelled int64, err error) {
	err = s.query(ctx, func(ctx context.Context) error {
		return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx, `UPDATE restore_job SET status = 'cancelled', updated_at = now(), finished_at = now()
				WHERE cluster = $1 AND status IN ('pending', 'running')`, s.cluster)
			if err != nil {
				return err
			}
			cancelled = tag.RowsAffected()
			for _, slots := range jobs {
				if _, err := tx.Exec(ctx, `INSERT INTO restore_job (cluster, seq, slots, status) VALUES ($1, $2, $3, 'pending')`,
					s.cluster, seq, slots); err != nil {
					return err
				}
			}
			return nil
		})
	})
	return cancelled, err
}

func (s *store) exec(ctx context.Context, sql string, args ...any) (bool, error) {
	var changed bool
	err := s.query(ctx, func(ctx context.Context) error {
		tag, err := s.pool.Exec(ctx, sql, append([]any{s.cluster}, args...)...)
		changed = tag.RowsAffected() > 0
		return err
	})
	return changed, err
}

func (s *store) claimJob(ctx context.Context, id int64, nodeID string) (bool, error) {
	return s.exec(ctx, `UPDATE restore_job SET status = 'running', node_id = $3, started_at = coalesce(started_at, now()),
		updated_at = now(), restored = 0, skipped = 0 WHERE cluster = $1 AND id = $2 AND status IN ('pending', 'running')`, id, nodeID)
}

func (s *store) jobProgress(ctx context.Context, id int64, nodeID string, restored, skipped int64) (bool, error) {
	return s.exec(ctx, `UPDATE restore_job SET restored = $4, skipped = $5, updated_at = now()
		WHERE cluster = $1 AND id = $2 AND status = 'running' AND node_id = $3`, id, nodeID, restored, skipped)
}

func (s *store) touchJobs(ctx context.Context, ids []int64) error {
	_, err := s.exec(ctx, `UPDATE restore_job SET updated_at = now() WHERE cluster = $1 AND id = ANY($2) AND status = 'pending'`, ids)
	return err
}

func (s *store) finishJob(ctx context.Context, id int64, nodeID string, restored, skipped int64) (bool, error) {
	return s.exec(ctx, `UPDATE restore_job SET status = 'done', restored = $4, skipped = $5, error = '', updated_at = now(),
		finished_at = now() WHERE cluster = $1 AND id = $2 AND status = 'running' AND node_id = $3`, id, nodeID, restored, skipped)
}

var failSet = fmt.Sprintf(`attempts = attempts + 1, node_id = '', updated_at = now(),
	status = CASE WHEN attempts + 1 >= %[1]d THEN 'failed' ELSE 'pending' END,
	finished_at = CASE WHEN attempts + 1 >= %[1]d THEN now() END`, maxAttempts)

func (s *store) failJob(ctx context.Context, id int64, nodeID, msg string) (status string, attempts int, err error) {
	err = s.query(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `UPDATE restore_job SET error = $4, `+failSet+`
			WHERE cluster = $1 AND id = $2 AND status = 'running' AND node_id = $3 RETURNING status, attempts`,
			s.cluster, id, nodeID, msg).Scan(&status, &attempts)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, nil
	}
	return status, attempts, err
}

func (s *store) failStalled(ctx context.Context, stall time.Duration) ([]restoreJob, error) {
	var jobs []restoreJob
	err := s.query(ctx, func(ctx context.Context) error {
		rows, _ := s.pool.Query(ctx, `UPDATE restore_job SET error = $3, `+failSet+`
			WHERE cluster = $1 AND status IN ('pending', 'running') AND updated_at < now() - $2::int * interval '1 second'
			RETURNING `+jobColumns, s.cluster, int(stall/time.Second), fmt.Sprintf("no progress for %s", stall))
		var err error
		jobs, err = pgx.CollectRows(rows, scanJob)
		return err
	})
	return jobs, err
}
