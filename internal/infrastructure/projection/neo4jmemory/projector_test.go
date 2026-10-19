package neo4jmemory

import (
	"agi-assistant/internal/domain/memory/consistency"
	"context"
	"encoding/json"
	"errors"
	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"reflect"
	"strings"
	"testing"
)

type fakeStore struct{ state consistency.ProjectionState }

func (f *fakeStore) Get(context.Context, int64) (consistency.ProjectionState, bool, error) {
	return f.state, f.state.ID != 0, nil
}
func (f *fakeStore) Upsert(_ context.Context, r consistency.MemoryRecord) error {
	f.state = consistency.ProjectionState{ID: r.ID, Version: r.Version, ContentHash: r.ContentHash}
	return nil
}
func (f *fakeStore) Delete(_ context.Context, id, version int64) error {
	f.state = consistency.ProjectionState{}
	return nil
}
func TestStaleUpsertDoesNotOverwriteNewerNode(t *testing.T) {
	s := &fakeStore{state: consistency.ProjectionState{ID: 1, Version: 3, ContentHash: "v3"}}
	p := New(s)
	b, _ := json.Marshal(consistency.MemoryRecord{ID: 1, UserID: "u1", Version: 2})
	if err := p.Apply(context.Background(), consistency.OutboxEvent{Target: consistency.TargetNeo4j, AggregateID: 1, AggregateVersion: 2, UserID: "u1", Type: consistency.EventUpsertMemoryGraphNode, Payload: b}); err != nil {
		t.Fatal(err)
	}
	if s.state.Version != 3 {
		t.Fatal("stale upsert overwrote v3")
	}
}

func TestNeo4jUnavailableReturnsErrors(t *testing.T) {
	s := NewNeo4jStore(nil)
	ctx := context.Background()
	if _, _, e := s.Get(ctx, 1); e == nil {
		t.Error("unavailable get succeeded")
	}
	if e := s.Delete(ctx, 1, 1); e == nil {
		t.Error("unavailable delete succeeded")
	}
	if _, e := s.ListPage(ctx, 0, 1); e == nil {
		t.Error("unavailable list succeeded")
	}
}

type graphMock struct {
	sessions, closed, committed, rolledBack int
	run                                     func(string, map[string]any) (driver.ResultWithContext, error)
}

func (g *graphMock) Available() bool                    { return true }
func (g *graphMock) Session() driver.SessionWithContext { g.sessions++; return &graphSession{g: g} }

type graphSession struct {
	driver.SessionWithContext
	g *graphMock
}

func (s *graphSession) Close(context.Context) error { s.g.closed++; return nil }
func (s *graphSession) Run(_ context.Context, q string, p map[string]any, _ ...func(*driver.TransactionConfig)) (driver.ResultWithContext, error) {
	return s.g.run(q, p)
}
func (s *graphSession) ExecuteWrite(_ context.Context, work driver.ManagedTransactionWork, _ ...func(*driver.TransactionConfig)) (any, error) {
	v, e := work(&graphTx{g: s.g})
	if e != nil {
		s.g.rolledBack++
	} else {
		s.g.committed++
	}
	return v, e
}

type graphTx struct {
	driver.ManagedTransaction
	g *graphMock
}

func (s *graphTx) Run(_ context.Context, q string, p map[string]any) (driver.ResultWithContext, error) {
	return s.g.run(q, p)
}

type graphResult struct {
	driver.ResultWithContext
	rows            []*driver.Record
	pos             int
	err, consumeErr error
}

func (r *graphResult) Next(context.Context) bool {
	if r.pos >= len(r.rows) {
		return false
	}
	r.pos++
	return true
}
func (r *graphResult) Record() *driver.Record { return r.rows[r.pos-1] }
func (r *graphResult) Err() error             { return r.err }
func (r *graphResult) Consume(context.Context) (driver.ResultSummary, error) {
	return nil, r.consumeErr
}
func graphRows(values ...any) *graphResult {
	return &graphResult{rows: []*driver.Record{{Values: values}}}
}
func setupGraph(t *testing.T, run func(string, map[string]any) (driver.ResultWithContext, error)) (*Neo4jStore, *graphMock) {
	t.Helper()
	g := &graphMock{run: run}
	t.Cleanup(func() {
		if g.sessions != g.closed {
			t.Errorf("leaked sessions: %d/%d", g.sessions, g.closed)
		}
	})
	return &Neo4jStore{client: g}, g
}
func TestNeo4jGetValidatesRowsAndReadsLateErrors(t *testing.T) {
	for _, kind := range []string{"valid", "missing", "type", "short", "stream", "consume", "run"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := setupGraph(t, func(q string, p map[string]any) (driver.ResultWithContext, error) {
				if !strings.Contains(q, "memory_id:$id") || p["id"] != int64(1) {
					t.Errorf("query=%s p=%v", q, p)
				}
				switch kind {
				case "missing":
					return &graphResult{}, nil
				case "type":
					return graphRows("bad", "hash", false), nil
				case "short":
					return graphRows(int64(2)), nil
				case "stream":
					r := graphRows(int64(2), "hash", false)
					r.err = errors.New("late read")
					return r, nil
				case "consume":
					r := graphRows(int64(2), "hash", false)
					r.consumeErr = errors.New("commit read")
					return r, nil
				case "run":
					return nil, errors.New("run failed")
				default:
					return graphRows(int64(2), "hash", true), nil
				}
			})
			state, ok, err := s.Get(context.Background(), 1)
			if kind == "valid" {
				if err != nil || !ok || state.Version != 2 || state.ContentHash != "hash" || !state.Deleted {
					t.Fatalf("%+v %v %v", state, ok, err)
				}
			} else if kind == "missing" {
				if err != nil || ok {
					t.Fatalf("%+v %v %v", state, ok, err)
				}
			} else if err == nil || ok {
				t.Fatalf("bad read accepted: %+v %v %v", state, ok, err)
			}
		})
	}
}
func TestNeo4jUpsertAtomicallySavesMetadataAndSupersedes(t *testing.T) {
	seen := map[string]bool{}
	s, g := setupGraph(t, func(q string, p map[string]any) (driver.ResultWithContext, error) {
		if strings.Contains(q, "_projection_lock") {
			seen["lock"] = true
			return graphRows(int64(1), "old", false, "alice"), nil
		}
		if strings.Contains(q, "SUPERSEDES") {
			seen["edges"] = true
			if !strings.Contains(q, "old.user_id=$user") || !reflect.DeepEqual(p["supersedes"], []int64{2, 3}) {
				t.Errorf("unsafe edges query=%s p=%v", q, p)
			}
			return graphRows(int64(2)), nil
		}
		seen["metadata"] = true
		if p["user"] != "alice" || p["version"] != int64(2) || p["hash"] != "new" || p["content"] != "tea" || p["deleted"] != false || !reflect.DeepEqual(p["tags"], []string{"preference"}) {
			t.Errorf("metadata=%v", p)
		}
		return &graphResult{}, nil
	})
	err := s.Upsert(context.Background(), consistency.MemoryRecord{ID: 1, UserID: "alice", Version: 2, ContentHash: "new", Content: "tea", Tags: []string{"preference"}, Supersedes: []int64{3, 2, 3}})
	if err != nil || g.committed != 1 || !seen["lock"] || !seen["edges"] || !seen["metadata"] {
		t.Fatalf("seen=%v committed=%d err=%v", seen, g.committed, err)
	}
}
func TestNeo4jWriteRollsBackOnMissingRelationsOrLateErrors(t *testing.T) {
	for _, kind := range []string{"missing_edge", "consume", "owner", "conflict", "run"} {
		t.Run(kind, func(t *testing.T) {
			s, g := setupGraph(t, func(q string, p map[string]any) (driver.ResultWithContext, error) {
				if kind == "run" {
					return nil, errors.New("write failed")
				}
				if strings.Contains(q, "_projection_lock") {
					if kind == "owner" {
						return graphRows(int64(1), "old", false, "bob"), nil
					}
					if kind == "conflict" {
						return graphRows(int64(2), "other", false, "alice"), nil
					}
					return graphRows(int64(1), "old", false, "alice"), nil
				}
				if strings.Contains(q, "SUPERSEDES") {
					return graphRows(int64(0)), nil
				}
				r := &graphResult{}
				if kind == "consume" {
					r.consumeErr = errors.New("late write")
				}
				return r, nil
			})
			err := s.Upsert(context.Background(), consistency.MemoryRecord{ID: 1, UserID: "alice", Version: 2, ContentHash: "new", Supersedes: []int64{2}})
			if err == nil || g.committed != 0 || g.rolledBack != 1 {
				t.Fatalf("commit=%d rollback=%d err=%v", g.committed, g.rolledBack, err)
			}
		})
	}
}
func TestNeo4jDeleteKeepsVersionAndDetachesRelationships(t *testing.T) {
	detached := false
	s, g := setupGraph(t, func(q string, p map[string]any) (driver.ResultWithContext, error) {
		if strings.Contains(q, "_projection_lock") {
			return graphRows(int64(2), "old", false, "alice"), nil
		}
		if strings.Contains(q, "DELETE rel") {
			detached = true
			if !strings.Contains(q, "type(rel) <> 'SUPERSEDES'") {
				t.Error("tombstone would remove supersession audit links")
			}
			if strings.Contains(q, "DETACH DELETE m") {
				t.Error("deleted watermark")
			}
		}
		return &graphResult{}, nil
	})
	if e := s.Delete(context.Background(), 1, 3); e != nil || !detached || g.committed != 1 {
		t.Fatalf("detached=%v committed=%d err=%v", detached, g.committed, e)
	}
}
func TestNeo4jListRejectsPartialRowsAndInvalidLimit(t *testing.T) {
	for _, kind := range []string{"valid", "stream", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := setupGraph(t, func(q string, p map[string]any) (driver.ResultWithContext, error) {
				if !strings.Contains(q, "ORDER BY m.memory_id") || p["after"] != int64(4) || p["limit"] != int64(10) {
					t.Errorf("q=%s p=%v", q, p)
				}
				r := graphRows(int64(5), int64(2), "hash", true)
				if kind == "stream" {
					r.err = errors.New("late list")
				}
				if kind == "malformed" {
					r = graphRows("bad", int64(2), "hash", false)
				}
				return r, nil
			})
			rows, e := s.ListPage(context.Background(), 4, 10)
			if kind == "valid" {
				if e != nil || len(rows) != 1 || !rows[0].Deleted {
					t.Fatalf("%+v %v", rows, e)
				}
			} else if e == nil || len(rows) != 0 {
				t.Fatalf("partial=%+v err=%v", rows, e)
			}
		})
	}
	s, _ := setupGraph(t, func(string, map[string]any) (driver.ResultWithContext, error) {
		t.Error("invalid page queried")
		return nil, nil
	})
	if _, e := s.ListPage(context.Background(), 0, 0); e == nil {
		t.Fatal("invalid limit accepted")
	}
}
