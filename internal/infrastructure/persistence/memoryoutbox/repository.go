// Package memoryoutbox stores projection work for at-least-once delivery.
package memoryoutbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"agi-assistant/internal/domain/memory/consistency"
	"github.com/lib/pq"
)

type Repository struct{ db *sql.DB }

func New(db *sql.DB) *Repository { return &Repository{db: db} }

// Claim locks ready rows without waiting on other workers. While processing,
// available_at is the persisted lease deadline; expired claims can be retried
// even if the next worker uses a different lease duration. A reclaimed event
// counts the interrupted attempt. Projectors must tolerate duplicate delivery.
func (r *Repository) Claim(ctx context.Context, target consistency.Target, worker string, limit int, lease time.Duration) ([]consistency.OutboxEvent, error) {
	if r.db == nil {
		return nil, fmt.Errorf("outbox unavailable")
	}
	if !validTarget(target) || strings.TrimSpace(worker) == "" || limit <= 0 || lease <= 0 {
		return nil, fmt.Errorf("outbox: target, worker, positive limit and lease are required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,event_id,aggregate_id,user_id,aggregate_version,event_type,target,payload,
  attempts+CASE WHEN status = 'processing' THEN 1 ELSE 0 END,created_at
  FROM memory_outbox WHERE target=$1 AND (status='pending' OR status='processing') AND available_at<=NOW()
  ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED`, string(target), limit)
	if err != nil {
		return nil, err
	}
	var events []consistency.OutboxEvent
	var ids []int64
	for rows.Next() {
		var e consistency.OutboxEvent
		var typ, targetText string
		var payload []byte
		if err = rows.Scan(&e.ID, &e.EventID, &e.AggregateID, &e.UserID, &e.AggregateVersion, &typ, &targetText, &payload, &e.Attempts, &e.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		e.Type = consistency.EventType(typ)
		e.Target = consistency.Target(targetText)
		e.Payload = json.RawMessage(payload)
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(ids) > 0 {
		res, e := tx.ExecContext(ctx, `UPDATE memory_outbox SET status = 'processing',
   attempts=attempts+CASE WHEN status='processing' THEN 1 ELSE 0 END,
   locked_by=$1,locked_at=NOW(),available_at=NOW()+($3 * INTERVAL '1 second') WHERE id=ANY($2)`, worker, pq.Array(ids), lease.Seconds())
		if e != nil {
			return nil, e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return nil, e
		}
		if n != int64(len(ids)) {
			return nil, fmt.Errorf("outbox: claim updated %d of %d rows", n, len(ids))
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return events, nil
}

// Status changes require a processing row. These compatibility methods do not
// carry a claim token: callers must prevent a stale worker from acknowledging
// a newer claim. They do not provide exactly-once processing or lease renewal.
func (r *Repository) MarkProcessed(ctx context.Context, id string) error {
	return r.exec(ctx, `UPDATE memory_outbox SET status='processed',processed_at=NOW(),locked_at=NULL,locked_by=NULL WHERE event_id=$1 AND status='processing'`, id)
}
func (r *Repository) MarkRetry(ctx context.Context, id, msg string, at time.Time) error {
	return r.exec(ctx, `UPDATE memory_outbox SET status='pending',attempts=attempts+1,available_at=$2,last_error=$3,locked_at=NULL,locked_by=NULL WHERE event_id=$1 AND status='processing'`, id, at, msg)
}
func (r *Repository) MarkDead(ctx context.Context, id, msg string) error {
	return r.exec(ctx, `UPDATE memory_outbox SET status='dead',attempts=attempts+1,last_error=$2,locked_at=NULL,locked_by=NULL WHERE event_id=$1 AND status='processing'`, id, msg)
}
func (r *Repository) exec(ctx context.Context, q string, args ...any) error {
	if r.db == nil {
		return fmt.Errorf("outbox unavailable")
	}
	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("outbox: status update affected %d rows", n)
	}
	return nil
}

func (r *Repository) EnqueueRepair(ctx context.Context, e consistency.OutboxEvent, key string) (bool, error) {
	if r.db == nil {
		return false, fmt.Errorf("outbox unavailable")
	}
	if strings.TrimSpace(key) == "" || !validTarget(e.Target) || !json.Valid(e.Payload) {
		return false, fmt.Errorf("outbox: repair key, target and JSON payload are required")
	}
	if e.EventID == "" {
		// A deterministic UUIDv8 derived from the globally unique repair key fits
		// PostgreSQL's UUID column and remains stable when an enqueue is retried.
		sum := sha256.Sum256([]byte("agi-assistant/memory-repair:" + key))
		value := sum[:16]
		value[6] = (value[6] & 0x0f) | 0x80
		value[8] = (value[8] & 0x3f) | 0x80
		e.EventID = fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:])
	} else if !validUUID(e.EventID) {
		return false, fmt.Errorf("outbox: invalid event UUID")
	}
	// An orphan repair may have no user ID; its authoritative row no longer exists.
	res, err := r.db.ExecContext(ctx, `INSERT INTO memory_outbox
  (event_id,aggregate_id,user_id,aggregate_version,event_type,target,payload,repair_dedupe_key)
  VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (repair_dedupe_key) DO NOTHING`,
		e.EventID, e.AggregateID, e.UserID, e.AggregateVersion, string(e.Type), string(e.Target), []byte(e.Payload), key)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func validTarget(target consistency.Target) bool {
	return target == consistency.TargetMilvus || target == consistency.TargetNeo4j || target == consistency.TargetLTMCache
}
func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil && len(decoded) == 16
}
