// Package memorytx implements authoritative long-term-memory mutations.
// Every mutation and its projection events commit in one PostgreSQL
// transaction.
package memorytx

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"agi-assistant/internal/domain/memory/consistency"

	"github.com/lib/pq"
)

type Repository struct {
	db *sql.DB
}

var ErrVersionConflict = errors.New("memorytx: version conflict")

type Store interface {
	Create(context.Context, CreateCommand) (consistency.CommittedChangeSet, error)
	Update(context.Context, UpdateCommand) (consistency.CommittedChangeSet, error)
	Tombstone(context.Context, DeleteCommand) (consistency.CommittedChangeSet, error)
	LoadActive(context.Context) ([]consistency.MemoryRecord, error)
	LoadPage(context.Context, int64, int) ([]consistency.MemoryRecord, error)
	ApplyConsolidation(context.Context, consistency.ConsolidationPlan) (consistency.CommittedChangeSet, error)
}

// ApplyConsolidation locks rows in ID order and rejects the whole plan if any
// owner or version changed. No uncommitted changes escape on any failure.
func (r *Repository) ApplyConsolidation(ctx context.Context, plan consistency.ConsolidationPlan) (changes consistency.CommittedChangeSet, err error) {
	if r.db == nil {
		return changes, errorsUnavailable()
	}
	ids := make([]int64, 0, len(plan.Updates)+len(plan.Deletes))
	expected := map[int64]int64{}
	owners := map[int64]string{}
	add := func(id, version int64, user string) error {
		if id <= 0 || version <= 0 || user == "" {
			return fmt.Errorf("memorytx: positive id/version and user id are required")
		}
		if _, ok := expected[id]; ok {
			return fmt.Errorf("memorytx: duplicate memory id %d", id)
		}
		ids = append(ids, id)
		expected[id] = version
		owners[id] = user
		return nil
	}
	for _, u := range plan.Updates {
		if u.Record.DeletedAt != nil {
			return changes, fmt.Errorf("memorytx: consolidation update cannot contain a tombstone")
		}
		if err = add(u.Record.ID, u.ExpectedVersion, u.Record.UserID); err != nil {
			return changes, err
		}
	}
	for _, d := range plan.Deletes {
		if err = add(d.ID, d.ExpectedVersion, d.UserID); err != nil {
			return changes, err
		}
	}
	if len(ids) == 0 {
		return changes, nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return changes, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
			changes = consistency.CommittedChangeSet{}
		}
	}()
	rows, err := tx.QueryContext(ctx, `SELECT id, version, user_id FROM long_term_memory WHERE id = ANY($1) AND deleted_at IS NULL ORDER BY id FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return changes, err
	}
	seen := 0
	for rows.Next() {
		var id, version int64
		var user string
		if err = rows.Scan(&id, &version, &user); err != nil {
			rows.Close()
			return changes, err
		}
		seen++
		if expected[id] != version || owners[id] != user {
			rows.Close()
			return changes, ErrVersionConflict
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return changes, err
	}
	if closeErr != nil {
		return changes, closeErr
	}
	if seen != len(expected) {
		return changes, ErrVersionConflict
	}
	for _, u := range plan.Updates {
		rec := u.Record
		emb, e := json.Marshal(rec.Embedding)
		if e != nil {
			return changes, e
		}
		rec, err = scanRecord(tx.QueryRowContext(ctx, `UPDATE long_term_memory
   SET content=$1,importance=$2,embedding=$3,category=$4,tags=$5,slot_hint=NULLIF($6,''),
       embedding_model=$7,embedding_revision=$8,last_accessed=$9,
       quarantined=$10,quarantine_reason=NULLIF($11,''),superseded=$12,superseded_at=$13,supersedes=$14,
       version=version+1,updated_at=NOW(),content_hash=''
   WHERE id=$15 AND version=$16 AND user_id=$17 AND deleted_at IS NULL RETURNING `+memoryColumns,
			rec.Content, rec.Importance, emb, rec.Category, pq.Array(append([]string{}, rec.Tags...)), rec.SlotHint,
			rec.EmbeddingModel, rec.EmbeddingRevision, rec.LastAccessed, rec.Quarantined, rec.QuarantineReason,
			rec.Superseded, rec.SupersededAt, pq.Array(append([]int64{}, rec.Supersedes...)), rec.ID, u.ExpectedVersion, rec.UserID))
		if errors.Is(err, sql.ErrNoRows) {
			return changes, ErrVersionConflict
		}
		if err != nil {
			return changes, err
		}
		eventIDs, e := saveRecordEvents(ctx, tx, &rec, []eventSpec{{consistency.EventUpsertMemoryVector, consistency.TargetMilvus}, {consistency.EventUpsertMemoryGraphNode, consistency.TargetNeo4j}, {consistency.EventUpsertMemoryGraphEdges, consistency.TargetNeo4j}})
		if e != nil {
			return changes, e
		}
		changes.Upserts = append(changes.Upserts, rec)
		changes.EventIDs = append(changes.EventIDs, eventIDs...)
	}
	for _, d := range plan.Deletes {
		rec, e := scanRecord(tx.QueryRowContext(ctx, `UPDATE long_term_memory
   SET deleted_at=NOW(),superseded=TRUE,superseded_at=NOW(),version=version+1,updated_at=NOW(),content_hash=''
   WHERE id=$1 AND version=$2 AND user_id=$3 AND deleted_at IS NULL RETURNING `+memoryColumns, d.ID, d.ExpectedVersion, d.UserID))
		if errors.Is(e, sql.ErrNoRows) {
			return changes, ErrVersionConflict
		}
		if e != nil {
			return changes, e
		}
		eventIDs, e := saveRecordEvents(ctx, tx, &rec, deleteEvents())
		if e != nil {
			return changes, e
		}
		changes.Deletes = append(changes.Deletes, rec)
		changes.EventIDs = append(changes.EventIDs, eventIDs...)
	}
	if err = tx.Commit(); err != nil {
		return changes, err
	}
	committed = true
	return changes, nil
}

func New(db *sql.DB) *Repository {
	return &Repository{db: db}
}

type CreateCommand struct {
	UserID            string
	Content           string
	Importance        float64
	Embedding         []float64
	EmbeddingModel    string
	EmbeddingRevision string
	Category          string
	Tags              []string
	SlotHint          string
	EmitGraphEdges    bool
}

type UpdateCommand struct {
	ID                int64
	UserID            string
	ExpectedVersion   int64
	Content           string
	Importance        float64
	Embedding         []float64
	EmbeddingModel    string
	EmbeddingRevision string
	Category          string
	Tags              []string
	SlotHint          string
	EmitGraphEdges    bool
}

type DeleteCommand struct {
	ID              int64
	UserID          string
	ExpectedVersion int64
}

func (r *Repository) Create(ctx context.Context, cmd CreateCommand) (consistency.CommittedChangeSet, error) {
	if r.db == nil {
		return consistency.CommittedChangeSet{}, errorsUnavailable()
	}
	if cmd.UserID == "" {
		return consistency.CommittedChangeSet{}, fmt.Errorf("memorytx: user id is required")
	}
	if cmd.Category == "" {
		cmd.Category = "general"
	}
	emb, err := json.Marshal(cmd.Embedding)
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	defer tx.Rollback()
	rec, err := scanRecord(tx.QueryRowContext(ctx, `INSERT INTO long_term_memory
  (user_id,content,importance,embedding,category,tags,slot_hint,embedding_model,embedding_revision,version,updated_at)
  VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,$9,1,NOW()) RETURNING `+memoryColumns,
		cmd.UserID, cmd.Content, cmd.Importance, emb, cmd.Category, pq.Array(append([]string{}, cmd.Tags...)), cmd.SlotHint, cmd.EmbeddingModel, cmd.EmbeddingRevision))
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	ids, err := saveRecordEvents(ctx, tx, &rec, upsertEvents(cmd.EmitGraphEdges))
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	if err = tx.Commit(); err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	return consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{rec}, EventIDs: ids}, nil
}

func (r *Repository) Update(ctx context.Context, cmd UpdateCommand) (consistency.CommittedChangeSet, error) {
	if r.db == nil {
		return consistency.CommittedChangeSet{}, errorsUnavailable()
	}
	if cmd.UserID == "" || cmd.ID <= 0 || cmd.ExpectedVersion <= 0 {
		return consistency.CommittedChangeSet{}, fmt.Errorf("memorytx: positive id/version and user id are required")
	}
	if cmd.Category == "" {
		cmd.Category = "general"
	}
	emb, err := json.Marshal(cmd.Embedding)
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	defer tx.Rollback()
	rec, err := scanRecord(tx.QueryRowContext(ctx, `UPDATE long_term_memory
  SET content=$1,importance=$2,embedding=$3,category=$4,tags=$5,slot_hint=NULLIF($6,''),
      embedding_model=$7,embedding_revision=$8,version=version+1,updated_at=NOW(),content_hash=''
  WHERE id=$9 AND version=$10 AND user_id=$11 AND deleted_at IS NULL RETURNING `+memoryColumns,
		cmd.Content, cmd.Importance, emb, cmd.Category, pq.Array(append([]string{}, cmd.Tags...)), cmd.SlotHint, cmd.EmbeddingModel, cmd.EmbeddingRevision, cmd.ID, cmd.ExpectedVersion, cmd.UserID))
	if errors.Is(err, sql.ErrNoRows) {
		return consistency.CommittedChangeSet{}, ErrVersionConflict
	}
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	ids, err := saveRecordEvents(ctx, tx, &rec, upsertEvents(cmd.EmitGraphEdges))
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	if err = tx.Commit(); err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	return consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{rec}, EventIDs: ids}, nil
}

func (r *Repository) Tombstone(ctx context.Context, cmd DeleteCommand) (consistency.CommittedChangeSet, error) {
	if r.db == nil {
		return consistency.CommittedChangeSet{}, errorsUnavailable()
	}
	if cmd.UserID == "" || cmd.ID <= 0 || cmd.ExpectedVersion <= 0 {
		return consistency.CommittedChangeSet{}, fmt.Errorf("memorytx: positive id/version and user id are required")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	defer tx.Rollback()
	rec, err := scanRecord(tx.QueryRowContext(ctx, `UPDATE long_term_memory
  SET deleted_at=NOW(),version=version+1,updated_at=NOW(),content_hash=''
  WHERE id=$1 AND version=$2 AND user_id=$3 AND deleted_at IS NULL RETURNING `+memoryColumns, cmd.ID, cmd.ExpectedVersion, cmd.UserID))
	if errors.Is(err, sql.ErrNoRows) {
		return consistency.CommittedChangeSet{}, ErrVersionConflict
	}
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	ids, err := saveRecordEvents(ctx, tx, &rec, deleteEvents())
	if err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	if err = tx.Commit(); err != nil {
		return consistency.CommittedChangeSet{}, err
	}
	return consistency.CommittedChangeSet{Deletes: []consistency.MemoryRecord{rec}, EventIDs: ids}, nil
}

func upsertEvents(edges bool) []eventSpec {
	specs := []eventSpec{{consistency.EventUpsertMemoryVector, consistency.TargetMilvus}, {consistency.EventUpsertMemoryGraphNode, consistency.TargetNeo4j}}
	if edges {
		specs = append(specs, eventSpec{consistency.EventUpsertMemoryGraphEdges, consistency.TargetNeo4j})
	}
	return specs
}

func deleteEvents() []eventSpec {
	return []eventSpec{{consistency.EventDeleteMemoryVector, consistency.TargetMilvus}, {consistency.EventDeleteMemoryGraphEdges, consistency.TargetNeo4j}, {consistency.EventDeleteMemoryGraphNode, consistency.TargetNeo4j}}
}

// Hash the database-returned row, including fields not changed by this command.
// This keeps authoritative reloads and projection payloads identical.
func saveRecordEvents(ctx context.Context, tx *sql.Tx, rec *consistency.MemoryRecord, specs []eventSpec) ([]string, error) {
	var err error
	rec.ContentHash, err = consistency.ComputeContentHash(*rec)
	if err != nil {
		return nil, err
	}
	if err = updateContentHash(ctx, tx, *rec); err != nil {
		return nil, err
	}
	return insertEvents(ctx, tx, *rec, specs)
}

const memoryColumns = `id, user_id, content, importance, embedding,
	embedding_model, embedding_revision,
	created_at, updated_at, last_accessed,
	category, tags, COALESCE(slot_hint, ''),
	quarantined, COALESCE(quarantine_reason, ''),
	superseded, superseded_at, supersedes,
	version, content_hash, deleted_at`

func (r *Repository) LoadActive(ctx context.Context) ([]consistency.MemoryRecord, error) {
	if r.db == nil {
		return nil, errorsUnavailable()
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+memoryColumns+`
		 FROM long_term_memory WHERE deleted_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("memorytx: load active: %w", err)
	}
	return scanRecords(rows)
}

// LoadPage returns authoritative rows including tombstones. Reconciliation
// needs tombstones to distinguish target orphans from delayed deletes.
func (r *Repository) LoadPage(ctx context.Context, afterID int64, limit int) ([]consistency.MemoryRecord, error) {
	if r.db == nil {
		return nil, errorsUnavailable()
	}
	if limit <= 0 {
		return nil, fmt.Errorf("memorytx: page limit must be positive")
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+memoryColumns+`
		 FROM long_term_memory WHERE id > $1 ORDER BY id LIMIT $2`,
		afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("memorytx: load page: %w", err)
	}
	return scanRecords(rows)
}

func scanRecords(rows *sql.Rows) ([]consistency.MemoryRecord, error) {
	defer rows.Close()
	var records []consistency.MemoryRecord
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memorytx: iterate memories: %w", err)
	}
	return records, nil
}

func scanRecord(row interface{ Scan(...any) error }) (consistency.MemoryRecord, error) {
	var rec consistency.MemoryRecord
	var emb []byte
	var tags pq.StringArray
	var supersedes pq.Int64Array
	var supersededAt, deletedAt sql.NullTime
	err := row.Scan(&rec.ID, &rec.UserID, &rec.Content, &rec.Importance, &emb,
		&rec.EmbeddingModel, &rec.EmbeddingRevision, &rec.CreatedAt, &rec.UpdatedAt, &rec.LastAccessed,
		&rec.Category, &tags, &rec.SlotHint, &rec.Quarantined, &rec.QuarantineReason, &rec.Superseded, &supersededAt, &supersedes,
		&rec.Version, &rec.ContentHash, &deletedAt)
	if err != nil {
		return consistency.MemoryRecord{}, fmt.Errorf("memorytx: scan memory: %w", err)
	}
	if len(emb) > 0 {
		if err = json.Unmarshal(emb, &rec.Embedding); err != nil {
			return consistency.MemoryRecord{}, fmt.Errorf("memorytx: decode embedding for %d: %w", rec.ID, err)
		}
	}
	rec.Tags = append([]string(nil), tags...)
	rec.Supersedes = append([]int64(nil), supersedes...)
	if rec.Version <= 0 {
		rec.Version = 1
	}
	if supersededAt.Valid {
		rec.SupersededAt = &supersededAt.Time
	}
	if deletedAt.Valid {
		rec.DeletedAt = &deletedAt.Time
	}
	return rec, nil
}

type eventSpec struct {
	eventType consistency.EventType
	target    consistency.Target
}

func updateContentHash(ctx context.Context, tx *sql.Tx, record consistency.MemoryRecord) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE long_term_memory SET content_hash = $1 WHERE id = $2 AND version = $3`,
		record.ContentHash, record.ID, record.Version)
	if err != nil {
		return fmt.Errorf("memorytx: save content hash: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("memorytx: content hash rows affected: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("memorytx: content hash update affected %d rows", affected)
	}
	return nil
}

func insertEvents(ctx context.Context, tx *sql.Tx, record consistency.MemoryRecord, specs []eventSpec) ([]string, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("memorytx: encode projection payload: %w", err)
	}
	eventIDs := make([]string, 0, len(specs))
	for _, spec := range specs {
		eventID, idErr := newEventID()
		if idErr != nil {
			return nil, fmt.Errorf("memorytx: event id: %w", idErr)
		}
		if _, err = tx.ExecContext(ctx,
			`INSERT INTO memory_outbox
				(event_id, aggregate_id, user_id, aggregate_version, event_type, target, payload)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			eventID, record.ID, record.UserID, record.Version,
			string(spec.eventType), string(spec.target), payload,
		); err != nil {
			return nil, fmt.Errorf("memorytx: insert %s outbox: %w", spec.eventType, err)
		}
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs, nil
}

func newEventID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	var dst [36]byte
	hex.Encode(dst[0:8], value[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], value[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], value[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], value[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], value[10:16])
	return string(dst[:]), nil
}

func errorsUnavailable() error {
	return fmt.Errorf("memorytx: postgres unavailable")
}
