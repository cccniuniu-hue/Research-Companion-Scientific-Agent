package memoryoutbox_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/infrastructure/persistence/memoryoutbox"
	"github.com/DATA-DOG/go-sqlmock"
)

var outboxNow = time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

func outboxSetup(t *testing.T) (*memoryoutbox.Repository, sqlmock.Sqlmock) {
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
	return memoryoutbox.New(db), m
}
func eventRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "event_id", "aggregate_id", "user_id", "aggregate_version", "event_type", "target", "payload", "attempts", "created_at"})
}
func addEvent(rows *sqlmock.Rows, id int64) *sqlmock.Rows {
	return rows.AddRow(id, "e1", 41, "u1", 2, "upsert_memory_vector", "milvus", []byte(`{"memory_id":41}`), 2, outboxNow)
}
func TestClaimUsesSkipLockedAndReturnsClaimedEvents(t *testing.T) {
	repo, m := outboxSetup(t)
	m.ExpectBegin()
	m.ExpectQuery(`status='pending'.*status='processing'.*available_at<=NOW\(\).*FOR UPDATE SKIP LOCKED`).WithArgs("milvus", 10).WillReturnRows(addEvent(eventRows(), 1))
	m.ExpectExec(`UPDATE memory_outbox SET status = 'processing'.*available_at=NOW\(\)\+\(\$3 \* INTERVAL '1 second'\)`).WithArgs("w1", "{1}", 60.0).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	events, e := repo.Claim(context.Background(), consistency.TargetMilvus, "w1", 10, time.Minute)
	if e != nil || len(events) != 1 || events[0].ID != 1 || events[0].EventID != "e1" || events[0].UserID != "u1" || events[0].AggregateVersion != 2 || events[0].Attempts != 2 || events[0].Type != consistency.EventUpsertMemoryVector || events[0].Target != consistency.TargetMilvus || string(events[0].Payload) != `{"memory_id":41}` || !events[0].CreatedAt.Equal(outboxNow) {
		t.Fatalf("events=%+v err=%v", events, e)
	}
}
func TestClaimEmptyBatchCommitsWithoutUpdating(t *testing.T) {
	repo, m := outboxSetup(t)
	m.ExpectBegin()
	m.ExpectQuery(`FOR UPDATE SKIP LOCKED`).WillReturnRows(eventRows())
	m.ExpectCommit()
	events, e := repo.Claim(context.Background(), consistency.TargetNeo4j, "w1", 10, time.Second)
	if e != nil || len(events) != 0 {
		t.Fatalf("%+v %v", events, e)
	}
}
func TestClaimRollsBackAndDiscardsPartialEvents(t *testing.T) {
	for _, point := range []string{"begin", "query", "scan", "stream", "update", "count", "rows_error", "commit"} {
		t.Run(point, func(t *testing.T) {
			repo, m := outboxSetup(t)
			failure := errors.New("injected failure")
			if point == "begin" {
				m.ExpectBegin().WillReturnError(failure)
			} else {
				m.ExpectBegin()
				q := m.ExpectQuery(`FOR UPDATE SKIP LOCKED`)
				if point == "query" {
					q.WillReturnError(failure)
				} else {
					rows := addEvent(eventRows(), 1)
					if point == "scan" {
						rows.AddRow("invalid", "e2", 41, "u1", 2, "upsert_memory_vector", "milvus", []byte(`{}`), 0, outboxNow)
					}
					if point == "stream" {
						addEvent(rows, 2).RowError(1, failure)
					}
					q.WillReturnRows(rows)
					if point != "scan" && point != "stream" {
						u := m.ExpectExec(`UPDATE memory_outbox SET status = 'processing'`)
						switch point {
						case "update":
							u.WillReturnError(failure)
						case "count":
							u.WillReturnResult(sqlmock.NewResult(0, 0))
						case "rows_error":
							u.WillReturnResult(sqlmock.NewErrorResult(failure))
						case "commit":
							u.WillReturnResult(sqlmock.NewResult(0, 1))
							m.ExpectCommit().WillReturnError(failure)
						}
					}
				}
				if point != "commit" {
					m.ExpectRollback()
				}
			}
			events, e := repo.Claim(context.Background(), consistency.TargetMilvus, "w1", 10, time.Second)
			if e == nil || len(events) != 0 {
				t.Fatalf("partial claim escaped: %+v %v", events, e)
			}
		})
	}
}
func TestClaimRejectsInvalidConfiguration(t *testing.T) {
	for _, v := range []struct {
		target consistency.Target
		worker string
		limit  int
		lease  time.Duration
	}{{"bad", "w", 1, time.Second}, {consistency.TargetMilvus, "", 1, time.Second}, {consistency.TargetMilvus, "w", 0, time.Second}, {consistency.TargetMilvus, "w", 1, 0}, {consistency.TargetMilvus, "w", -1, time.Second}, {consistency.TargetMilvus, "w", 1, -time.Second}} {
		repo, _ := outboxSetup(t)
		if _, e := repo.Claim(context.Background(), v.target, v.worker, v.limit, v.lease); e == nil || !strings.Contains(e.Error(), "target, worker, positive limit and lease") {
			t.Fatalf("invalid config not rejected before SQL: %+v err=%v", v, e)
		}
	}
}
func TestStatusUpdatesRequireProcessingAndOneRow(t *testing.T) {
	for _, kind := range []string{"processed", "retry", "dead"} {
		t.Run(kind, func(t *testing.T) {
			for _, point := range []string{"success", "missing", "exec_error", "rows_error"} {
				t.Run(point, func(t *testing.T) {
					repo, m := outboxSetup(t)
					var q *sqlmock.ExpectedExec
					switch kind {
					case "processed":
						q = m.ExpectExec(`status='processed'.*processed_at=NOW\(\).*locked_at=NULL,locked_by=NULL WHERE event_id=\$1 AND status='processing'`).WithArgs("e1")
					case "retry":
						q = m.ExpectExec(`status='pending',attempts=attempts\+1,available_at=\$2,last_error=\$3.*WHERE event_id=\$1 AND status='processing'`).WithArgs("e1", outboxNow, "retry reason")
					case "dead":
						q = m.ExpectExec(`status='dead',attempts=attempts\+1,last_error=\$2.*WHERE event_id=\$1 AND status='processing'`).WithArgs("e1", "dead reason")
					}
					switch point {
					case "success":
						q.WillReturnResult(sqlmock.NewResult(0, 1))
					case "missing":
						q.WillReturnResult(sqlmock.NewResult(0, 0))
					case "exec_error":
						q.WillReturnError(errors.New("write failed"))
					case "rows_error":
						q.WillReturnResult(sqlmock.NewErrorResult(errors.New("count failed")))
					}
					var e error
					switch kind {
					case "processed":
						e = repo.MarkProcessed(context.Background(), "e1")
					case "retry":
						e = repo.MarkRetry(context.Background(), "e1", "retry reason", outboxNow)
					case "dead":
						e = repo.MarkDead(context.Background(), "e1", "dead reason")
					}
					if (e == nil) != (point == "success") {
						t.Fatalf("err=%v", e)
					}
				})
			}
		})
	}
}

type repairUUID struct{ captured *string }

func (u repairUUID) Match(v driver.Value) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	valid, _ := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, s)
	if valid && u.captured != nil {
		*u.captured = s
	}
	return valid
}
func repairEvent() consistency.OutboxEvent {
	return consistency.OutboxEvent{AggregateID: 41, AggregateVersion: 2, UserID: "u1", Type: consistency.EventUpsertMemoryVector, Target: consistency.TargetMilvus, Payload: []byte(`{"memory_id":41}`)}
}
func TestEnqueueRepairUsesValidUUIDAndDedupeKey(t *testing.T) {
	repo, m := outboxSetup(t)
	e := repairEvent()
	var generated string
	m.ExpectExec(`ON CONFLICT \(repair_dedupe_key\) DO NOTHING`).WithArgs(repairUUID{&generated}, int64(41), "u1", int64(2), "upsert_memory_vector", "milvus", []byte(e.Payload), "repair-key").WillReturnResult(sqlmock.NewResult(1, 1))
	ok, err := repo.EnqueueRepair(context.Background(), e, "repair-key")
	if err != nil || !ok || generated == "" {
		t.Fatalf("insert=%v err=%v", ok, err)
	}
	m.ExpectExec(`ON CONFLICT \(repair_dedupe_key\) DO NOTHING`).WithArgs(generated, int64(41), "u1", int64(2), "upsert_memory_vector", "milvus", []byte(e.Payload), "repair-key").WillReturnResult(sqlmock.NewResult(0, 0))
	ok, err = repo.EnqueueRepair(context.Background(), e, "repair-key")
	if err != nil || ok {
		t.Fatalf("dedupe=%v err=%v", ok, err)
	}
	var other string
	m.ExpectExec(`INSERT INTO memory_outbox`).WithArgs(repairUUID{&other}, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), "other-key").WillReturnResult(sqlmock.NewResult(1, 1))
	if _, err = repo.EnqueueRepair(context.Background(), e, "other-key"); err != nil || other == generated {
		t.Fatalf("independent repair=%q err=%v", other, err)
	}
}
func TestEnqueueRepairPreservesProvidedUUIDAndErrors(t *testing.T) {
	for _, point := range []string{"success", "exec_error", "rows_error"} {
		t.Run(point, func(t *testing.T) {
			repo, m := outboxSetup(t)
			e := repairEvent()
			e.EventID = "12345678-1234-4234-8234-123456789abc"
			q := m.ExpectExec(`INSERT INTO memory_outbox`).WithArgs(e.EventID, e.AggregateID, e.UserID, e.AggregateVersion, string(e.Type), string(e.Target), []byte(e.Payload), "key")
			switch point {
			case "success":
				q.WillReturnResult(sqlmock.NewResult(1, 1))
			case "exec_error":
				q.WillReturnError(errors.New("write failed"))
			case "rows_error":
				q.WillReturnResult(sqlmock.NewErrorResult(errors.New("count failed")))
			}
			ok, err := repo.EnqueueRepair(context.Background(), e, "key")
			if point == "success" {
				if !ok || err != nil {
					t.Fatalf("%v %v", ok, err)
				}
			} else if ok || err == nil {
				t.Fatalf("%v %v", ok, err)
			}
		})
	}
}
func TestEnqueueRepairRejectsInvalidInput(t *testing.T) {
	for _, point := range []string{"key", "target", "payload", "uuid", "uuid_extra_separators"} {
		repo, _ := outboxSetup(t)
		e := repairEvent()
		key := "key"
		switch point {
		case "key":
			key = ""
		case "target":
			e.Target = "bad"
		case "payload":
			e.Payload = []byte(`bad-json`)
		case "uuid":
			e.EventID = "repair-not-a-uuid"
		case "uuid_extra_separators":
			e.EventID = "--345678-1234-4234-8234-123456789abc"
		}
		if _, err := repo.EnqueueRepair(context.Background(), e, key); err == nil || !strings.HasPrefix(err.Error(), "outbox:") {
			t.Fatalf("invalid %s not rejected before SQL: %v", point, err)
		}
	}
}
func TestUnavailableRepositoryReturnsErrors(t *testing.T) {
	repo := memoryoutbox.New(nil)
	if _, e := repo.Claim(context.Background(), consistency.TargetMilvus, "w", 1, time.Second); e == nil {
		t.Fatal("nil db accepted")
	}
	if e := repo.MarkProcessed(context.Background(), "e"); e == nil {
		t.Fatal("nil db accepted")
	}
	if e := repo.MarkRetry(context.Background(), "e", "", outboxNow); e == nil {
		t.Fatal("nil db accepted")
	}
	if e := repo.MarkDead(context.Background(), "e", ""); e == nil {
		t.Fatal("nil db accepted")
	}
	if _, e := repo.EnqueueRepair(context.Background(), repairEvent(), "key"); e == nil {
		t.Fatal("nil db accepted")
	}
}
