package memorytx_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"regexp"
	"testing"
	"time"

	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/infrastructure/persistence/memorytx"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

var testTime = time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

func fixture(id, version int64) consistency.MemoryRecord {
	return consistency.MemoryRecord{ID: id, UserID: "u1", Version: version, Content: "remember tea", Importance: .8, Embedding: []float64{.1, .2}, EmbeddingModel: "model", EmbeddingRevision: "r1", Category: "preference", Tags: []string{"merged", "src:user"}, SlotHint: "profile", CreatedAt: testTime.Add(-time.Hour), UpdatedAt: testTime, LastAccessed: testTime.Add(-time.Minute), Quarantined: true, QuarantineReason: "review", Superseded: true, SupersededAt: &testTime, Supersedes: []int64{2, 3}}
}
func memoryRows(records ...consistency.MemoryRecord) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"id", "user_id", "content", "importance", "embedding", "embedding_model", "embedding_revision", "created_at", "updated_at", "last_accessed", "category", "tags", "slot_hint", "quarantined", "quarantine_reason", "superseded", "superseded_at", "supersedes", "version", "content_hash", "deleted_at"})
	for _, r := range records {
		emb, _ := json.Marshal(r.Embedding)
		tags, _ := pq.Array(append([]string{}, r.Tags...)).Value()
		supersedes, _ := pq.Array(append([]int64{}, r.Supersedes...)).Value()
		var supersededAt, deletedAt any
		if r.SupersededAt != nil {
			supersededAt = *r.SupersededAt
		}
		if r.DeletedAt != nil {
			deletedAt = *r.DeletedAt
		}
		rows.AddRow(r.ID, r.UserID, r.Content, r.Importance, emb, r.EmbeddingModel, r.EmbeddingRevision, r.CreatedAt, r.UpdatedAt, r.LastAccessed, r.Category, tags, r.SlotHint, r.Quarantined, r.QuarantineReason, r.Superseded, supersededAt, supersedes, r.Version, r.ContentHash, deletedAt)
	}
	return rows
}
func setup(t *testing.T) (*memorytx.Repository, sqlmock.Sqlmock) {
	t.Helper()
	db, m, e := sqlmock.New()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := m.ExpectationsWereMet(); e != nil {
			t.Error(e)
		}
		db.Close()
	})
	return memorytx.New(db), m
}

type payloadOf struct{ record consistency.MemoryRecord }

func (p payloadOf) Match(v driver.Value) bool {
	b, ok := v.([]byte)
	if !ok {
		return false
	}
	var got consistency.MemoryRecord
	if json.Unmarshal(b, &got) != nil {
		return false
	}
	want := p.record
	want.ContentHash, _ = consistency.ComputeContentHash(want)
	return reflect.DeepEqual(got, want)
}
func expectHash(m sqlmock.Sqlmock, r consistency.MemoryRecord) {
	h, _ := consistency.ComputeContentHash(r)
	m.ExpectExec(`UPDATE long_term_memory SET content_hash`).WithArgs(h, r.ID, r.Version).WillReturnResult(sqlmock.NewResult(0, 1))
}
func expectEvents(m sqlmock.Sqlmock, r consistency.MemoryRecord, del, edges bool) {
	types := []consistency.EventType{consistency.EventUpsertMemoryVector, consistency.EventUpsertMemoryGraphNode}
	targets := []string{"milvus", "neo4j"}
	if edges {
		types = append(types, consistency.EventUpsertMemoryGraphEdges)
		targets = append(targets, "neo4j")
	}
	if del {
		types = []consistency.EventType{consistency.EventDeleteMemoryVector, consistency.EventDeleteMemoryGraphEdges, consistency.EventDeleteMemoryGraphNode}
		targets = []string{"milvus", "neo4j", "neo4j"}
	}
	for i, typ := range types {
		m.ExpectExec(`INSERT INTO memory_outbox`).WithArgs(sqlmock.AnyArg(), r.ID, r.UserID, r.Version, string(typ), targets[i], payloadOf{r}).WillReturnResult(sqlmock.NewResult(1, 1))
	}
}
func emptyChanges(t *testing.T, c consistency.CommittedChangeSet, e error) {
	t.Helper()
	if e == nil || len(c.Upserts)+len(c.Deletes)+len(c.EventIDs) != 0 {
		t.Fatalf("uncommitted changes=%+v err=%v", c, e)
	}
}
func assertRecord(t *testing.T, got, want consistency.MemoryRecord) {
	t.Helper()
	want.ContentHash, _ = consistency.ComputeContentHash(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("record mismatch:\ngot=%+v\nwant=%+v", got, want)
	}
}

func TestRepositoryCreateCommitsMemoryAndOutboxAtomically(t *testing.T) {
	for _, edges := range []bool{false, true} {
		t.Run(map[bool]string{false: "nodes", true: "edges"}[edges], func(t *testing.T) {
			repo, m := setup(t)
			r := fixture(41, 1)
			r.Quarantined = false
			r.QuarantineReason = ""
			r.Superseded = false
			r.SupersededAt = nil
			r.Supersedes = nil
			m.ExpectBegin()
			m.ExpectQuery(`INSERT INTO long_term_memory`).WithArgs("u1", r.Content, r.Importance, sqlmock.AnyArg(), r.Category, sqlmock.AnyArg(), r.SlotHint, r.EmbeddingModel, r.EmbeddingRevision).WillReturnRows(memoryRows(r))
			expectHash(m, r)
			expectEvents(m, r, false, edges)
			m.ExpectCommit()
			c, e := repo.Create(context.Background(), memorytx.CreateCommand{UserID: r.UserID, Content: r.Content, Importance: r.Importance, Embedding: r.Embedding, Category: r.Category, Tags: r.Tags, SlotHint: r.SlotHint, EmbeddingModel: r.EmbeddingModel, EmbeddingRevision: r.EmbeddingRevision, EmitGraphEdges: edges})
			if e != nil || len(c.Upserts) != 1 || len(c.EventIDs) != 2+map[bool]int{false: 0, true: 1}[edges] {
				t.Fatalf("changes=%+v err=%v", c, e)
			}
			assertRecord(t, c.Upserts[0], r)
			for _, id := range c.EventIDs {
				if ok, _ := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id); !ok {
					t.Fatalf("invalid UUID %q", id)
				}
			}
		})
	}
}
func TestRepositoryCreateRollsBackWhenOutboxInsertFails(t *testing.T) {
	repo, m := setup(t)
	r := fixture(42, 1)
	m.ExpectBegin()
	m.ExpectQuery(`INSERT INTO long_term_memory`).WillReturnRows(memoryRows(r))
	expectHash(m, r)
	m.ExpectExec(`INSERT INTO memory_outbox`).WillReturnResult(sqlmock.NewResult(1, 1))
	m.ExpectExec(`INSERT INTO memory_outbox`).WillReturnError(errors.New("second target failed"))
	m.ExpectRollback()
	c, e := repo.Create(context.Background(), memorytx.CreateCommand{UserID: "u1"})
	emptyChanges(t, c, e)
}
func TestRepositoryUpdateReturnsVersionConflictWithoutOutbox(t *testing.T) {
	repo, m := setup(t)
	m.ExpectBegin()
	m.ExpectQuery(`WHERE id=\$9 AND version=\$10 AND user_id=\$11 AND deleted_at IS NULL`).WithArgs("changed", .9, sqlmock.AnyArg(), "general", "{}", "", "", "", int64(41), int64(1), "other-user").WillReturnError(sql.ErrNoRows)
	m.ExpectRollback()
	c, e := repo.Update(context.Background(), memorytx.UpdateCommand{ID: 41, UserID: "other-user", ExpectedVersion: 1, Content: "changed", Importance: .9})
	emptyChanges(t, c, e)
	if !errors.Is(e, memorytx.ErrVersionConflict) {
		t.Fatalf("err=%v", e)
	}
}
func TestRepositoryUpdateCommitsNewVersionAndOutbox(t *testing.T) {
	repo, m := setup(t)
	r := fixture(41, 2)
	m.ExpectBegin()
	m.ExpectQuery(`WHERE id=\$9 AND version=\$10 AND user_id=\$11 AND deleted_at IS NULL RETURNING`).WillReturnRows(memoryRows(r))
	expectHash(m, r)
	expectEvents(m, r, false, false)
	m.ExpectCommit()
	c, e := repo.Update(context.Background(), memorytx.UpdateCommand{ID: 41, UserID: "u1", ExpectedVersion: 1})
	if e != nil || len(c.Upserts) != 1 {
		t.Fatalf("%+v %v", c, e)
	}
	assertRecord(t, c.Upserts[0], r)
}
func TestRepositoryTombstoneCommitsAllDeleteEvents(t *testing.T) {
	repo, m := setup(t)
	r := fixture(41, 4)
	r.DeletedAt = &testTime
	m.ExpectBegin()
	m.ExpectQuery(`WHERE id=\$1 AND version=\$2 AND user_id=\$3 AND deleted_at IS NULL RETURNING`).WithArgs(int64(41), int64(3), "u1").WillReturnRows(memoryRows(r))
	expectHash(m, r)
	expectEvents(m, r, true, false)
	m.ExpectCommit()
	c, e := repo.Tombstone(context.Background(), memorytx.DeleteCommand{ID: 41, UserID: "u1", ExpectedVersion: 3})
	if e != nil || len(c.Deletes) != 1 || len(c.EventIDs) != 3 {
		t.Fatalf("%+v %v", c, e)
	}
	assertRecord(t, c.Deletes[0], r)
}
func TestRepositoryTombstoneRejectsWrongOwner(t *testing.T) {
	repo, m := setup(t)
	m.ExpectBegin()
	m.ExpectQuery(`user_id=\$3 AND deleted_at IS NULL`).WithArgs(int64(41), int64(3), "other").WillReturnError(sql.ErrNoRows)
	m.ExpectRollback()
	c, e := repo.Tombstone(context.Background(), memorytx.DeleteCommand{ID: 41, UserID: "other", ExpectedVersion: 3})
	emptyChanges(t, c, e)
	if !errors.Is(e, memorytx.ErrVersionConflict) {
		t.Fatal(e)
	}
}
func TestRepositoryCreateCommitAndHashFailuresReturnNoChanges(t *testing.T) {
	for _, point := range []string{"begin", "insert", "hash", "hash_count", "hash_rows_error", "commit"} {
		t.Run(point, func(t *testing.T) {
			repo, m := setup(t)
			r := fixture(41, 1)
			failure := errors.New("injected failure")
			if point == "begin" {
				m.ExpectBegin().WillReturnError(failure)
			} else {
				m.ExpectBegin()
				q := m.ExpectQuery(`INSERT INTO long_term_memory`)
				if point == "insert" {
					q.WillReturnError(failure)
				} else {
					q.WillReturnRows(memoryRows(r))
					switch point {
					case "hash":
						m.ExpectExec(`UPDATE long_term_memory SET content_hash`).WillReturnError(failure)
					case "hash_count":
						m.ExpectExec(`UPDATE long_term_memory SET content_hash`).WillReturnResult(sqlmock.NewResult(0, 0))
					case "hash_rows_error":
						m.ExpectExec(`UPDATE long_term_memory SET content_hash`).WillReturnResult(sqlmock.NewErrorResult(failure))
					case "commit":
						expectHash(m, r)
						expectEvents(m, r, false, false)
						m.ExpectCommit().WillReturnError(failure)
					}
				}
				if point != "commit" {
					m.ExpectRollback()
				}
			}
			c, e := repo.Create(context.Background(), memorytx.CreateCommand{UserID: "u1"})
			emptyChanges(t, c, e)
		})
	}
}
func TestRepositoryNilTagsUseNonNullArray(t *testing.T) {
	repo, m := setup(t)
	r := fixture(41, 1)
	m.ExpectBegin()
	m.ExpectQuery(`INSERT INTO long_term_memory`).WithArgs("u1", "", 0.0, sqlmock.AnyArg(), "general", "{}", "", "", "").WillReturnRows(memoryRows(r))
	expectHash(m, r)
	expectEvents(m, r, false, false)
	m.ExpectCommit()
	if _, e := repo.Create(context.Background(), memorytx.CreateCommand{UserID: "u1"}); e != nil {
		t.Fatal(e)
	}
}
func TestApplyConsolidationPersistsMergedMetadata(t *testing.T) {
	repo, m := setup(t)
	r := fixture(41, 5)
	m.ExpectBegin()
	m.ExpectQuery(`SELECT id, version, user_id.*deleted_at IS NULL ORDER BY id FOR UPDATE`).WithArgs("{41}").WillReturnRows(sqlmock.NewRows([]string{"id", "version", "user_id"}).AddRow(41, 4, "u1"))
	m.ExpectQuery(`tags=\$5.*supersedes=\$14.*user_id=\$17 AND deleted_at IS NULL RETURNING`).WithArgs(r.Content, r.Importance, sqlmock.AnyArg(), r.Category, `{"merged","src:user"}`, r.SlotHint, r.EmbeddingModel, r.EmbeddingRevision, r.LastAccessed, true, r.QuarantineReason, true, testTime, "{2,3}", r.ID, int64(4), r.UserID).WillReturnRows(memoryRows(r))
	expectHash(m, r)
	expectEvents(m, r, false, true)
	m.ExpectCommit()
	c, e := repo.ApplyConsolidation(context.Background(), consistency.ConsolidationPlan{Updates: []consistency.MemoryUpdate{{Record: r, ExpectedVersion: 4}}})
	if e != nil || len(c.Upserts) != 1 || len(c.EventIDs) != 3 {
		t.Fatalf("%+v %v", c, e)
	}
	assertRecord(t, c.Upserts[0], r)
}
func TestApplyConsolidationRollsBackWhenVersionChanged(t *testing.T) {
	for _, point := range []string{"version", "owner", "missing", "stream", "scan"} {
		t.Run(point, func(t *testing.T) {
			repo, m := setup(t)
			rows := sqlmock.NewRows([]string{"id", "version", "user_id"})
			switch point {
			case "version":
				rows.AddRow(1, 3, "u1")
			case "owner":
				rows.AddRow(1, 2, "other")
			case "stream":
				rows.AddRow(1, 2, "u1").RowError(0, errors.New("read interrupted"))
			case "scan":
				rows.AddRow("bad-id", 2, "u1")
			}
			m.ExpectBegin()
			m.ExpectQuery(`SELECT id, version, user_id`).WillReturnRows(rows)
			m.ExpectRollback()
			c, e := repo.ApplyConsolidation(context.Background(), consistency.ConsolidationPlan{Deletes: []consistency.MemoryDelete{{ID: 1, UserID: "u1", ExpectedVersion: 2}}})
			emptyChanges(t, c, e)
			if point == "version" || point == "owner" || point == "missing" {
				if !errors.Is(e, memorytx.ErrVersionConflict) {
					t.Fatal(e)
				}
			} else if errors.Is(e, memorytx.ErrVersionConflict) {
				t.Fatalf("read error hidden: %v", e)
			}
		})
	}
}
func TestApplyConsolidationRejectsDuplicateAndInvalidPlans(t *testing.T) {
	r := fixture(1, 1)
	for _, p := range []consistency.ConsolidationPlan{
		{Updates: []consistency.MemoryUpdate{{Record: r, ExpectedVersion: 1}, {Record: r, ExpectedVersion: 1}}},
		{Updates: []consistency.MemoryUpdate{{Record: r, ExpectedVersion: 1}}, Deletes: []consistency.MemoryDelete{{ID: 1, UserID: "u1", ExpectedVersion: 1}}},
		{Deletes: []consistency.MemoryDelete{{ID: 1, ExpectedVersion: 1}}},
		{Deletes: []consistency.MemoryDelete{{ID: 1, UserID: "u1"}}},
	} {
		repo, _ := setup(t)
		c, e := repo.ApplyConsolidation(context.Background(), p)
		emptyChanges(t, c, e)
	}
	repo, _ := setup(t)
	if c, e := repo.ApplyConsolidation(context.Background(), consistency.ConsolidationPlan{}); e != nil || len(c.EventIDs) != 0 {
		t.Fatalf("%+v %v", c, e)
	}
}
func TestConsolidationDoesNotExposeRolledBackPartialChanges(t *testing.T) {
	for _, point := range []string{"second_row", "outbox", "commit"} {
		t.Run(point, func(t *testing.T) {
			repo, m := setup(t)
			r := fixture(1, 2)
			r.DeletedAt = &testTime
			m.ExpectBegin()
			m.ExpectQuery(`SELECT id, version, user_id`).WithArgs("{1,2}").WillReturnRows(sqlmock.NewRows([]string{"id", "version", "user_id"}).AddRow(1, 1, "u1").AddRow(2, 1, "u1"))
			m.ExpectQuery(`UPDATE long_term_memory`).WillReturnRows(memoryRows(r))
			expectHash(m, r)
			expectEvents(m, r, true, false)
			if point == "second_row" {
				m.ExpectQuery(`UPDATE long_term_memory`).WillReturnError(errors.New("write failed"))
				m.ExpectRollback()
			} else {
				r.ID = 2
				m.ExpectQuery(`UPDATE long_term_memory`).WillReturnRows(memoryRows(r))
				expectHash(m, r)
				if point == "outbox" {
					m.ExpectExec(`INSERT INTO memory_outbox`).WillReturnError(errors.New("outbox failed"))
					m.ExpectRollback()
				} else {
					expectEvents(m, r, true, false)
					m.ExpectCommit().WillReturnError(errors.New("commit failed"))
				}
			}
			c, e := repo.ApplyConsolidation(context.Background(), consistency.ConsolidationPlan{Deletes: []consistency.MemoryDelete{{ID: 1, UserID: "u1", ExpectedVersion: 1}, {ID: 2, UserID: "u1", ExpectedVersion: 1}}})
			emptyChanges(t, c, e)
		})
	}
}
func TestRepositoryLoadActiveReturnsVersionedNonDeletedRows(t *testing.T) {
	repo, m := setup(t)
	r := fixture(41, 4)
	m.ExpectQuery(`FROM long_term_memory WHERE deleted_at IS NULL`).WillReturnRows(memoryRows(r))
	rows, e := repo.LoadActive(context.Background())
	if e != nil || len(rows) != 1 {
		t.Fatalf("%+v %v", rows, e)
	}
	if !reflect.DeepEqual(rows[0], r) {
		t.Fatalf("%+v", rows[0])
	}
}
func TestRepositoryLoadPageIncludesTombstonesForReconciliation(t *testing.T) {
	repo, m := setup(t)
	r := fixture(41, 5)
	r.DeletedAt = &testTime
	m.ExpectQuery(`FROM long_term_memory WHERE id > \$1 ORDER BY id LIMIT \$2`).WithArgs(int64(40), 100).WillReturnRows(memoryRows(r))
	rows, e := repo.LoadPage(context.Background(), 40, 100)
	if e != nil || len(rows) != 1 || rows[0].DeletedAt == nil {
		t.Fatalf("%+v %v", rows, e)
	}
}
func TestRepositoryLoadRejectsPartialOrMalformedResults(t *testing.T) {
	for _, point := range []string{"query", "scan", "stream", "embedding"} {
		t.Run(point, func(t *testing.T) {
			repo, m := setup(t)
			q := m.ExpectQuery(`FROM long_term_memory WHERE deleted_at IS NULL`)
			r := fixture(1, 2)
			switch point {
			case "query":
				q.WillReturnError(errors.New("query failed"))
			case "scan":
				q.WillReturnRows(memoryRows(r).AddRow("invalid", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil))
			case "stream":
				q.WillReturnRows(memoryRows(r, fixture(2, 1)).RowError(1, errors.New("late read error")))
			case "embedding":
				q.WillReturnRows(memoryRows(r).AddRow(int64(2), "u1", "", .5, []byte(`not-json`), "", "", testTime, testTime, testTime, "general", "{}", "", false, "", false, nil, "{}", int64(1), "", nil))
			}
			rows, e := repo.LoadActive(context.Background())
			if e == nil || len(rows) != 0 {
				t.Fatalf("partial data escaped: %+v %v", rows, e)
			}
		})
	}
}
func TestRepositoryUnavailableAndInvalidCommands(t *testing.T) {
	repo := memorytx.New(nil)
	if _, e := repo.LoadActive(context.Background()); e == nil {
		t.Fatal("nil database accepted")
	}
	if _, e := repo.LoadPage(context.Background(), 0, 1); e == nil {
		t.Fatal("nil database accepted")
	}
	if _, e := repo.Create(context.Background(), memorytx.CreateCommand{}); e == nil {
		t.Fatal("nil database accepted")
	}
	if _, e := repo.Update(context.Background(), memorytx.UpdateCommand{}); e == nil {
		t.Fatal("nil database accepted")
	}
	if _, e := repo.Tombstone(context.Background(), memorytx.DeleteCommand{}); e == nil {
		t.Fatal("nil database accepted")
	}
	if _, e := repo.ApplyConsolidation(context.Background(), consistency.ConsolidationPlan{}); e == nil {
		t.Fatal("nil database accepted")
	}
	repo, _ = setup(t)
	if _, e := repo.LoadPage(context.Background(), 0, 0); e == nil {
		t.Fatal("zero limit accepted")
	}
	if _, e := repo.Create(context.Background(), memorytx.CreateCommand{}); e == nil {
		t.Fatal("missing owner accepted")
	}
	if _, e := repo.Create(context.Background(), memorytx.CreateCommand{UserID: "u1", Embedding: []float64{math.NaN()}}); e == nil {
		t.Fatal("NaN accepted")
	}
	if _, e := repo.Update(context.Background(), memorytx.UpdateCommand{UserID: "u1"}); e == nil {
		t.Fatal("invalid version accepted")
	}
	if _, e := repo.Tombstone(context.Background(), memorytx.DeleteCommand{ID: 1, ExpectedVersion: 1}); e == nil {
		t.Fatal("missing owner accepted")
	}
}
