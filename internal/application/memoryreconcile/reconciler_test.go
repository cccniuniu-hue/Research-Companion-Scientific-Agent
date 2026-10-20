package memoryreconcile

import (
	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/infrastructure/projection/versioned"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type source struct{ rows []consistency.MemoryRecord }

func (s source) LoadPage(_ context.Context, after int64, limit int) ([]consistency.MemoryRecord, error) {
	var out []consistency.MemoryRecord
	for _, r := range s.rows {
		if r.ID > after {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

type target struct{ rows []consistency.ProjectionState }

func (t target) ListPage(_ context.Context, after int64, limit int) ([]consistency.ProjectionState, error) {
	var out []consistency.ProjectionState
	for _, r := range t.rows {
		if r.ID > after {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

type repairs struct{ events []consistency.OutboxEvent }

func (r *repairs) EnqueueRepair(_ context.Context, e consistency.OutboxEvent, _ string) (bool, error) {
	r.events = append(r.events, e)
	return true, nil
}
func TestReconcilerEnqueuesMissingAndStaleRepairs(t *testing.T) {
	q := &repairs{}
	r := New(source{[]consistency.MemoryRecord{{ID: 1, UserID: "u", Version: 2, ContentHash: "a"}, {ID: 2, UserID: "u", Version: 3, ContentHash: "b"}}}, target{[]consistency.ProjectionState{{ID: 2, Version: 2, ContentHash: "old"}}}, q, consistency.TargetMilvus, 100)
	report, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Missing != 1 || report.Stale != 1 || len(q.events) != 2 {
		t.Fatalf("report=%+v events=%d", report, len(q.events))
	}
}

type pagedSource struct {
	rows  []consistency.MemoryRecord
	calls []int64
	load  func(int64, int) ([]consistency.MemoryRecord, error)
}

func (s *pagedSource) LoadPage(_ context.Context, after int64, limit int) ([]consistency.MemoryRecord, error) {
	s.calls = append(s.calls, after)
	if s.load != nil {
		return s.load(after, limit)
	}
	out := []consistency.MemoryRecord{}
	for _, r := range s.rows {
		if r.ID > after {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

type pagedTarget struct {
	rows  []consistency.ProjectionState
	calls []int64
	load  func(int64, int) ([]consistency.ProjectionState, error)
}

func (s *pagedTarget) ListPage(_ context.Context, after int64, limit int) ([]consistency.ProjectionState, error) {
	s.calls = append(s.calls, after)
	if s.load != nil {
		return s.load(after, limit)
	}
	out := []consistency.ProjectionState{}
	for _, r := range s.rows {
		if r.ID > after {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

type repairQueue struct {
	events []consistency.OutboxEvent
	keys   []string
	skip   bool
	err    error
}

func (q *repairQueue) EnqueueRepair(_ context.Context, e consistency.OutboxEvent, key string) (bool, error) {
	if q.err != nil {
		return false, q.err
	}
	q.events = append(q.events, e)
	q.keys = append(q.keys, key)
	return !q.skip, nil
}
func record(id, version int64) consistency.MemoryRecord {
	r := consistency.MemoryRecord{ID: id, UserID: "alice", Version: version, Content: "memory"}
	r.ContentHash, _ = consistency.ComputeContentHash(r)
	return r
}
func targetState(id, version int64) consistency.ProjectionState {
	return consistency.ProjectionState{ID: id, Version: version, ContentHash: record(id, version).ContentHash}
}

func TestReconcilePaginatesBothSidesBeforeClassifyingOrphans(t *testing.T) {
	source := &pagedSource{rows: []consistency.MemoryRecord{record(1, 1), record(2, 1), record(3, 1), record(5, 1)}}
	target := &pagedTarget{rows: []consistency.ProjectionState{targetState(2, 1), targetState(3, 1), targetState(4, 1), targetState(5, 1)}}
	q := &repairQueue{}
	out, e := New(source, target, q, consistency.TargetMilvus, 1).RunOnce(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if out.Checked != 4 || out.Missing != 1 || out.Orphan != 1 || out.RepairEnqueued != 2 || len(q.events) != 2 {
		t.Fatalf("bad classification: %+v events=%+v", out, q.events)
	}
	if q.events[0].AggregateID != 1 || q.events[1].AggregateID != 4 {
		t.Fatalf("repair IDs=%+v", q.events)
	}
}
func TestReconcileCountsOnlyInsertedRepairs(t *testing.T) {
	source := &pagedSource{rows: []consistency.MemoryRecord{record(1, 1)}}
	q := &repairQueue{skip: true}
	out, e := New(source, &pagedTarget{}, q, consistency.TargetNeo4j, 2).RunOnce(context.Background())
	if e != nil || out.Missing != 1 || out.RepairEnqueued != 0 || out.RepairSkipped != 1 {
		t.Fatalf("report=%+v err=%v", out, e)
	}
}
func TestReconcileHandlesTombstonesAndEventPayloads(t *testing.T) {
	for _, targetName := range []consistency.Target{consistency.TargetMilvus, consistency.TargetNeo4j} {
		t.Run(string(targetName), func(t *testing.T) {
			now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
			tomb := record(2, 2)
			tomb.DeletedAt = &now
			tomb.ContentHash, _ = consistency.ComputeContentHash(tomb)
			source := &pagedSource{rows: []consistency.MemoryRecord{record(1, 2), tomb}}
			orphanTomb := targetState(4, 1)
			orphanTomb.Deleted = true
			q := &repairQueue{}
			out, e := New(source, &pagedTarget{rows: []consistency.ProjectionState{targetState(1, 1), targetState(2, 1), orphanTomb}}, q, targetName, 1).RunOnce(context.Background())
			if e != nil || out.Stale != 2 || out.Orphan != 0 || len(q.events) != 2 {
				t.Fatalf("report=%+v events=%+v err=%v", out, q.events, e)
			}
			for i, event := range q.events {
				var got consistency.MemoryRecord
				if e = json.Unmarshal(event.Payload, &got); e != nil {
					t.Fatal(e)
				}
				want := source.rows[i]
				if !reflect.DeepEqual(got, want) || event.AggregateID != want.ID || event.AggregateVersion != want.Version || event.UserID != "alice" || event.Target != targetName {
					t.Fatalf("envelope=%+v got=%+v", event, got)
				}
				if i == 0 && !strings.HasPrefix(string(event.Type), "upsert_") {
					t.Fatal("active row deleted")
				}
				if i == 1 && !strings.HasPrefix(string(event.Type), "delete_") {
					t.Fatal("tombstone upserted")
				}
			}
		})
	}
}
func TestReconcileRechecksAuthoritativeOrphans(t *testing.T) {
	s := &pagedSource{load: func(after int64, limit int) ([]consistency.MemoryRecord, error) {
		if after == 4 && limit == 1 {
			return []consistency.MemoryRecord{record(5, 1)}, nil
		}
		return nil, nil
	}}
	q := &repairQueue{}
	out, e := New(s, &pagedTarget{rows: []consistency.ProjectionState{targetState(5, 1)}}, q, consistency.TargetMilvus, 10).RunOnce(context.Background())
	if e != nil || out.Orphan != 0 || len(q.events) != 0 {
		t.Fatalf("concurrently present row deleted: %+v %+v %v", out, q.events, e)
	}
}
func TestReconcileReportsVersionConflictsWithoutUnusableRepairs(t *testing.T) {
	source := &pagedSource{rows: []consistency.MemoryRecord{record(1, 1), record(2, 1), record(3, 1)}}
	a := targetState(1, 2)
	b := targetState(2, 1)
	b.ContentHash = "conflict"
	c := targetState(3, 1)
	c.Deleted = true
	q := &repairQueue{}
	out, e := New(source, &pagedTarget{rows: []consistency.ProjectionState{a, b, c}}, q, consistency.TargetMilvus, 1).RunOnce(context.Background())
	if e != nil || len(q.events) != 0 || out.RepairEnqueued != 0 || out.Conflicts != 3 {
		t.Fatalf("unrepairable events queued: %+v %+v %v", out, q.events, e)
	}
}
func TestReconcileReadErrorsAbortBeforeRepair(t *testing.T) {
	for _, kind := range []string{"source_late", "target_late", "source_repeat", "target_repeat"} {
		t.Run(kind, func(t *testing.T) {
			s := &pagedSource{rows: []consistency.MemoryRecord{record(1, 1)}}
			target := &pagedTarget{rows: []consistency.ProjectionState{targetState(2, 1)}}
			q := &repairQueue{}
			if strings.HasPrefix(kind, "source") {
				s.load = func(after int64, _ int) ([]consistency.MemoryRecord, error) {
					if after == 0 || kind == "source_repeat" {
						return []consistency.MemoryRecord{record(1, 1)}, nil
					}
					return nil, errors.New("late source read")
				}
			} else {
				target.load = func(after int64, _ int) ([]consistency.ProjectionState, error) {
					if after == 0 || kind == "target_repeat" {
						return []consistency.ProjectionState{targetState(2, 1)}, nil
					}
					return nil, errors.New("late target read")
				}
			}
			_, e := New(s, target, q, consistency.TargetMilvus, 1).RunOnce(context.Background())
			if e == nil || len(q.events) != 0 {
				t.Fatalf("incomplete read repaired: events=%v err=%v", q.events, e)
			}
		})
	}
}
func TestReconcilePropagatesQueueEncodingAndDependencyFailures(t *testing.T) {
	s := &pagedSource{rows: []consistency.MemoryRecord{record(1, 1)}}
	q := &repairQueue{err: errors.New("queue down")}
	if _, e := New(s, &pagedTarget{}, q, consistency.TargetMilvus, 1).RunOnce(context.Background()); !errors.Is(e, q.err) {
		t.Fatalf("err=%v", e)
	}
	bad := record(1, 1)
	bad.Importance = math.NaN()
	q = &repairQueue{}
	if _, e := New(&pagedSource{rows: []consistency.MemoryRecord{bad}}, &pagedTarget{}, q, consistency.TargetMilvus, 1).RunOnce(context.Background()); e == nil || len(q.events) != 0 {
		t.Fatal("invalid payload queued")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.calls = nil
	if _, e := New(s, &pagedTarget{}, q, consistency.TargetMilvus, 1).RunOnce(ctx); !errors.Is(e, context.Canceled) || len(s.calls) != 0 {
		t.Fatalf("canceled run: err=%v calls=%v", e, s.calls)
	}
	if _, e := New(nil, &pagedTarget{}, q, consistency.TargetMilvus, 1).RunOnce(context.Background()); e == nil {
		t.Fatal("nil source accepted")
	}
	if _, e := New(s, &pagedTarget{}, q, "unsupported", 1).RunOnce(context.Background()); e == nil {
		t.Fatal("unsupported target accepted")
	}
}

type repairProjectionStore struct {
	states map[int64]consistency.ProjectionState
}

func (s *repairProjectionStore) Get(_ context.Context, id int64) (consistency.ProjectionState, bool, error) {
	v, ok := s.states[id]
	return v, ok, nil
}
func (s *repairProjectionStore) Upsert(_ context.Context, m consistency.MemoryRecord) error {
	s.states[m.ID] = consistency.ProjectionState{ID: m.ID, Version: m.Version, ContentHash: m.ContentHash}
	return nil
}
func (s *repairProjectionStore) Delete(_ context.Context, id, version int64) error {
	s.states[id] = consistency.ProjectionState{ID: id, Version: version, Deleted: true}
	return nil
}
func (s *repairProjectionStore) DeleteRecord(_ context.Context, m consistency.MemoryRecord) error {
	s.states[m.ID] = consistency.ProjectionState{ID: m.ID, Version: m.Version, ContentHash: m.ContentHash, Deleted: true}
	return nil
}
func TestReconcileRepairsPassRealVersionedRulesAndConverge(t *testing.T) {
	now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	deleted := record(2, 2)
	deleted.DeletedAt = &now
	deleted.ContentHash, _ = consistency.ComputeContentHash(deleted)
	source := &pagedSource{rows: []consistency.MemoryRecord{record(1, 2), deleted}}
	store := &repairProjectionStore{states: map[int64]consistency.ProjectionState{1: targetState(1, 1), 3: targetState(3, 4)}}
	target := &pagedTarget{load: func(after int64, limit int) ([]consistency.ProjectionState, error) {
		var ids []int64
		for id := range store.states {
			if id > after {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) > limit {
			ids = ids[:limit]
		}
		var rows []consistency.ProjectionState
		for _, id := range ids {
			rows = append(rows, store.states[id])
		}
		return rows, nil
	}}
	q := &repairQueue{}
	r := New(source, target, q, consistency.TargetMilvus, 1)
	report, err := r.RunOnce(context.Background())
	if err != nil || report.RepairEnqueued != 3 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	p := &versioned.Projector{Store: store, TargetValue: consistency.TargetMilvus, Upserts: map[consistency.EventType]bool{consistency.EventUpsertMemoryVector: true}, Deletes: map[consistency.EventType]bool{consistency.EventDeleteMemoryVector: true}}
	for _, event := range q.events {
		if err = p.Apply(context.Background(), event); err != nil {
			t.Fatalf("repair envelope rejected: %v", err)
		}
	}
	if store.states[1].Version != 2 || !store.states[2].Deleted || !store.states[3].Deleted {
		t.Fatalf("repaired states=%v", store.states)
	}
	q.events = nil
	report, err = r.RunOnce(context.Background())
	if err != nil || report.Missing+report.Stale+report.Orphan+report.Conflicts+report.RepairEnqueued != 0 || len(q.events) != 0 {
		t.Fatalf("did not converge: %+v err=%v", report, err)
	}
}
