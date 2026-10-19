package milvusmemory

import (
	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/infrastructure/projection/versioned"
	"context"
	"encoding/json"
	"errors"
	mc "github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
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
func TestStaleDeleteDoesNotRemoveNewerVector(t *testing.T) {
	s := &fakeStore{state: consistency.ProjectionState{ID: 1, Version: 2}}
	p := New(s)
	b, _ := json.Marshal(consistency.MemoryRecord{ID: 1, Version: 1})
	if err := p.Apply(context.Background(), consistency.OutboxEvent{Target: consistency.TargetMilvus, AggregateID: 1, AggregateVersion: 1, Type: consistency.EventDeleteMemoryVector, Payload: b}); err != nil {
		t.Fatal(err)
	}
	if s.state.Version != 2 {
		t.Fatal("stale delete removed v2")
	}
}

type mockMilvus struct {
	mc.Client
	has     bool
	schema  *entity.Schema
	calls   []string
	columns []entity.Column
	query   func(string, []string, []mc.SearchQueryOptionFunc) (mc.ResultSet, error)
	failure string
}

func (c *mockMilvus) HasCollection(context.Context, string) (bool, error) {
	c.calls = append(c.calls, "has")
	if c.failure == "has" {
		return false, errors.New("has failed")
	}
	return c.has, nil
}
func (c *mockMilvus) CreateCollection(_ context.Context, s *entity.Schema, _ int32, _ ...mc.CreateCollectionOption) error {
	c.calls = append(c.calls, "create")
	c.schema = s
	return c.err("create")
}
func (c *mockMilvus) DescribeCollection(context.Context, string) (*entity.Collection, error) {
	c.calls = append(c.calls, "describe")
	return &entity.Collection{Schema: c.schema}, c.err("describe")
}
func (c *mockMilvus) DescribeIndex(context.Context, string, string, ...mc.IndexOption) ([]entity.Index, error) {
	c.calls = append(c.calls, "describe-index")
	return nil, c.err("describe-index")
}
func (c *mockMilvus) CreateIndex(_ context.Context, _ string, _ string, _ entity.Index, _ bool, _ ...mc.IndexOption) error {
	c.calls = append(c.calls, "index")
	return c.err("index")
}
func (c *mockMilvus) LoadCollection(context.Context, string, bool, ...mc.LoadCollectionOption) error {
	c.calls = append(c.calls, "load")
	return c.err("load")
}
func (c *mockMilvus) Upsert(_ context.Context, _ string, _ string, cols ...entity.Column) (entity.Column, error) {
	c.columns = cols
	c.calls = append(c.calls, "upsert")
	return entity.NewColumnInt64("memory_id", []int64{1}), c.err("upsert")
}
func (c *mockMilvus) Delete(context.Context, string, string, string) error {
	c.calls = append(c.calls, "physical-delete")
	return nil
}
func (c *mockMilvus) Query(_ context.Context, _ string, _ []string, expr string, fields []string, opts ...mc.SearchQueryOptionFunc) (mc.ResultSet, error) {
	c.calls = append(c.calls, "query")
	return c.query(expr, fields, opts)
}
func (c *mockMilvus) err(point string) error {
	if c.failure == point {
		return errors.New(point + " failed")
	}
	return nil
}
func vectorRows(ids, versions []int64, hashes []string, deleted []bool) mc.ResultSet {
	return mc.ResultSet{entity.NewColumnInt64("memory_id", ids), entity.NewColumnInt64("version", versions), entity.NewColumnVarChar("content_hash", hashes), entity.NewColumnBool("deleted", deleted)}
}

func TestMilvusInitCreatesTombstoneSchemaAndIndexBeforeLoad(t *testing.T) {
	c := &mockMilvus{}
	s := NewMilvusStore(c, 2)
	if e := s.Init(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(c.calls, []string{"has", "create", "index", "load"}) {
		t.Fatalf("init calls=%v", c.calls)
	}
	fields := map[string]*entity.Field{}
	for _, f := range c.schema.Fields {
		fields[f.Name] = f
	}
	if fields["deleted"] == nil || fields["deleted"].DataType != entity.FieldTypeBool || fields["embedding"].TypeParams["dim"] != "2" {
		t.Fatalf("schema=%+v", fields)
	}
	c.has = true
	c.calls = nil
	if e := s.Init(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(c.calls, []string{"has", "describe", "describe-index", "index", "load"}) {
		t.Fatalf("existing init=%v", c.calls)
	}
}
func TestMilvusInitRejectsIncompatibleSchemaAndErrors(t *testing.T) {
	for _, point := range []string{"has", "create", "index", "load"} {
		t.Run(point, func(t *testing.T) {
			c := &mockMilvus{failure: point}
			if e := NewMilvusStore(c, 2).Init(context.Background()); e == nil {
				t.Fatal("failure ignored")
			}
		})
	}
	c := &mockMilvus{has: true, schema: &entity.Schema{}}
	if e := NewMilvusStore(c, 2).Init(context.Background()); e == nil {
		t.Fatal("incompatible schema loaded")
	}
}
func TestMilvusRejectsBadVectorsBeforeWrite(t *testing.T) {
	for _, vector := range [][]float64{{1}, {1, 2, 3}, {math.NaN(), 1}, {math.Inf(1), 1}, {math.MaxFloat64, 1}} {
		c := &mockMilvus{}
		e := NewMilvusStore(c, 2).Upsert(context.Background(), consistency.MemoryRecord{ID: 1, UserID: "u", Version: 1, Embedding: vector})
		if e == nil || len(c.calls) != 0 {
			t.Fatalf("invalid vector accepted: %v calls=%v err=%v", vector, c.calls, e)
		}
	}
}
func TestMilvusWritesExactVectorAndReturnsErrors(t *testing.T) {
	c := &mockMilvus{}
	s := NewMilvusStore(c, 2)
	r := consistency.MemoryRecord{ID: 1, UserID: "alice", Version: 3, ContentHash: "hash", Embedding: []float64{.5, 1}}
	if e := s.Upsert(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	cols := mc.ResultSet(c.columns)
	for name, want := range map[string]any{"memory_id": int64(1), "user_id": "alice", "version": int64(3), "content_hash": "hash", "deleted": false} {
		col := cols.GetColumn(name)
		if col == nil {
			t.Fatalf("missing %s", name)
		}
		got, _ := col.Get(0)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s=%v want %v", name, got, want)
		}
	}
	v, _ := cols.GetColumn("embedding").Get(0)
	if !reflect.DeepEqual(v, []float32{.5, 1}) {
		t.Fatalf("embedding=%v", v)
	}
	c.failure = "upsert"
	if e := s.Upsert(context.Background(), r); e == nil {
		t.Fatal("write failure ignored")
	}
}
func TestMilvusDeletePersistsVersionWatermark(t *testing.T) {
	c := &mockMilvus{}
	s := NewMilvusStore(c, 2)
	if e := s.Delete(context.Background(), 1, 4); e != nil {
		t.Fatal(e)
	}
	cols := mc.ResultSet(c.columns)
	if cols.GetColumn("deleted") == nil {
		t.Fatal("delete did not persist tombstone")
	}
	deleted, _ := cols.GetColumn("deleted").Get(0)
	version, _ := cols.GetColumn("version").Get(0)
	if deleted != true || version != int64(4) {
		t.Fatalf("delete=%v version=%v", deleted, version)
	}
}
func TestMilvusGetUsesStrongReadAndRestoresTombstone(t *testing.T) {
	c := &mockMilvus{query: func(expr string, fields []string, opts []mc.SearchQueryOptionFunc) (mc.ResultSet, error) {
		if expr != "memory_id == 1" || !strings.Contains(strings.Join(fields, ","), "deleted") {
			t.Errorf("query=%s fields=%v", expr, fields)
		}
		o := &mc.SearchQueryOption{}
		for _, f := range opts {
			f(o)
		}
		if o.ConsistencyLevel != entity.ClStrong {
			t.Error("read is not strong")
		}
		return vectorRows([]int64{1}, []int64{4}, []string{"hash"}, []bool{true}), nil
	}}
	state, ok, e := NewMilvusStore(c, 2).Get(context.Background(), 1)
	if e != nil || !ok || state.Version != 4 || !state.Deleted || state.ContentHash != "hash" {
		t.Fatalf("state=%+v exists=%v err=%v", state, ok, e)
	}
}
func TestMilvusReadRejectsMissingColumnsAndBackendErrors(t *testing.T) {
	for _, point := range []string{"query", "missing_column", "wrong_id"} {
		t.Run(point, func(t *testing.T) {
			c := &mockMilvus{query: func(string, []string, []mc.SearchQueryOptionFunc) (mc.ResultSet, error) {
				switch point {
				case "query":
					return nil, errors.New("query failed")
				case "missing_column":
					return mc.ResultSet{entity.NewColumnInt64("memory_id", []int64{1})}, nil
				default:
					return vectorRows([]int64{2}, []int64{1}, []string{"h"}, []bool{false}), nil
				}
			}}
			if _, _, e := NewMilvusStore(c, 2).Get(context.Background(), 1); e == nil {
				t.Fatal("bad result accepted")
			}
		})
	}
}
func TestMilvusUnavailableAndInvalidLimits(t *testing.T) {
	s := NewMilvusStore(nil, 2)
	if e := s.Init(context.Background()); e == nil {
		t.Fatal("nil client accepted")
	}
	if _, _, e := s.Get(context.Background(), 1); e == nil {
		t.Fatal("nil client accepted")
	}
	if e := s.Upsert(context.Background(), consistency.MemoryRecord{}); e == nil {
		t.Fatal("nil client accepted")
	}
	if e := s.Delete(context.Background(), 1, 1); e == nil {
		t.Fatal("nil client accepted")
	}
	if _, e := s.ListPage(context.Background(), 0, 1); e == nil {
		t.Fatal("nil client accepted")
	}
	c := &mockMilvus{}
	if e := NewMilvusStore(c, 0).Init(context.Background()); e == nil || len(c.calls) != 0 {
		t.Fatal("zero dimension accepted")
	}
	if _, e := NewMilvusStore(c, 2).ListPage(context.Background(), 0, 0); e == nil || len(c.calls) != 0 {
		t.Fatal("zero limit queried")
	}
}

type checkStore struct {
	state                  consistency.ProjectionState
	exists                 bool
	gets, upserts, deletes int
	getErr, writeErr       error
}

func (s *checkStore) Get(context.Context, int64) (consistency.ProjectionState, bool, error) {
	s.gets++
	return s.state, s.exists, s.getErr
}
func (s *checkStore) Upsert(_ context.Context, r consistency.MemoryRecord) error {
	s.upserts++
	if s.writeErr != nil {
		return s.writeErr
	}
	s.state = consistency.ProjectionState{ID: r.ID, Version: r.Version, ContentHash: r.ContentHash}
	s.exists = true
	return nil
}
func (s *checkStore) Delete(context.Context, int64, int64) error {
	s.deletes++
	s.exists = false
	return s.writeErr
}
func (s *checkStore) DeleteRecord(_ context.Context, r consistency.MemoryRecord) error {
	s.deletes++
	if s.writeErr != nil {
		return s.writeErr
	}
	s.state = consistency.ProjectionState{ID: r.ID, Version: r.Version, ContentHash: r.ContentHash, Deleted: true}
	s.exists = true
	return nil
}
func projectionRecord(version int64) consistency.MemoryRecord {
	r := consistency.MemoryRecord{ID: 1, UserID: "alice", Version: version, Content: "memory", Embedding: []float64{1, 0}}
	r.ContentHash, _ = consistency.ComputeContentHash(r)
	return r
}
func projectionEvent(r consistency.MemoryRecord, del bool) consistency.OutboxEvent {
	typ := consistency.EventUpsertMemoryVector
	if del {
		typ = consistency.EventDeleteMemoryVector
	}
	b, _ := json.Marshal(r)
	return consistency.OutboxEvent{AggregateID: r.ID, AggregateVersion: r.Version, UserID: r.UserID, Target: consistency.TargetMilvus, Type: typ, Payload: b}
}
func projector(s versioned.Store) *versioned.Projector {
	return &versioned.Projector{Store: s, TargetValue: consistency.TargetMilvus, Upserts: map[consistency.EventType]bool{consistency.EventUpsertMemoryVector: true}, Deletes: map[consistency.EventType]bool{consistency.EventDeleteMemoryVector: true}}
}

func TestProjectionPreservesDeleteVersionAndRejectsResurrection(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "present"}[existing], func(t *testing.T) {
			r := projectionRecord(3)
			now := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
			r.DeletedAt = &now
			r.ContentHash, _ = consistency.ComputeContentHash(r)
			s := &checkStore{state: consistency.ProjectionState{ID: 1, Version: 1}, exists: existing}
			p := projector(s)
			if err := p.Apply(context.Background(), projectionEvent(r, true)); err != nil {
				t.Fatal(err)
			}
			if !s.exists || !s.state.Deleted || s.state.Version != 3 {
				t.Fatalf("delete watermark lost: %+v", s)
			}
			if err := p.Apply(context.Background(), projectionEvent(projectionRecord(2), false)); err != nil {
				t.Fatal(err)
			}
			if !s.state.Deleted || s.upserts != 0 {
				t.Fatal("stale record resurrected")
			}
			if err := p.Apply(context.Background(), projectionEvent(r, true)); err != nil {
				t.Fatal(err)
			}
			if s.deletes != 1 {
				t.Fatalf("duplicate deletes=%d", s.deletes)
			}
		})
	}
}
func TestProjectionValidatesEnvelopeBeforeStore(t *testing.T) {
	for _, kind := range []string{"target", "type", "id", "version", "user", "hash", "zero_id", "tombstone_upsert", "json"} {
		t.Run(kind, func(t *testing.T) {
			r := projectionRecord(2)
			e := projectionEvent(r, false)
			switch kind {
			case "target":
				e.Target = consistency.TargetNeo4j
			case "type":
				e.Type = "unknown"
			case "id":
				e.AggregateID = 2
			case "version":
				e.AggregateVersion = 3
			case "user":
				e.UserID = "bob"
			case "hash":
				r.Content = "tampered"
				e = projectionEvent(r, false)
			case "zero_id":
				r.ID = 0
				e = projectionEvent(r, false)
			case "tombstone_upsert":
				now := time.Now()
				r.DeletedAt = &now
				r.ContentHash, _ = consistency.ComputeContentHash(r)
				e = projectionEvent(r, false)
			case "json":
				e.Payload = []byte(`bad-json`)
			}
			s := &checkStore{}
			if err := projector(s).Apply(context.Background(), e); err == nil {
				t.Fatalf("invalid %s accepted", kind)
			}
			if s.gets+s.upserts+s.deletes != 0 {
				t.Fatal("invalid event reached store")
			}
		})
	}
}
func TestProjectionVersionsDuplicatesAndFailures(t *testing.T) {
	for _, kind := range []string{"new", "stale", "duplicate", "conflict", "deleted", "get_error", "write_error", "legacy_delete"} {
		t.Run(kind, func(t *testing.T) {
			r := projectionRecord(2)
			s := &checkStore{exists: true, state: consistency.ProjectionState{ID: 1, Version: 2, ContentHash: r.ContentHash}}
			wantErr, writes := false, 0
			del := false
			switch kind {
			case "new":
				s.state.Version = 1
				writes = 1
			case "stale":
				s.state.Version = 3
			case "conflict":
				s.state.ContentHash = "different"
				wantErr = true
			case "deleted":
				s.state.Deleted = true
				wantErr = true
			case "get_error":
				s.getErr = errors.New("read failed")
				wantErr = true
			case "write_error":
				s.state.Version = 1
				s.writeErr = errors.New("write failed")
				wantErr = true
				writes = 1
			case "legacy_delete":
				r.ContentHash = ""
				del = true
				writes = 1
			}
			err := projector(s).Apply(context.Background(), projectionEvent(r, del))
			if (err != nil) != wantErr {
				t.Fatalf("err=%v", err)
			}
			if s.upserts+s.deletes != writes {
				t.Fatalf("writes=%d", s.upserts+s.deletes)
			}
		})
	}
}
func TestProjectionCancellationAndUnavailable(t *testing.T) {
	s := &checkStore{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := projector(s).Apply(ctx, projectionEvent(projectionRecord(1), false)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if s.gets != 0 {
		t.Fatal("canceled query")
	}
	if err := projector(nil).Apply(context.Background(), projectionEvent(projectionRecord(1), false)); err == nil {
		t.Fatal("nil store accepted")
	}
}

type serialStore struct {
	checkStore
	mu          sync.Mutex
	active, max int
}

func (s *serialStore) Get(ctx context.Context, id int64) (consistency.ProjectionState, bool, error) {
	s.mu.Lock()
	s.active++
	if s.active > s.max {
		s.max = s.active
	}
	s.mu.Unlock()
	time.Sleep(time.Millisecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkStore.Get(ctx, id)
}
func (s *serialStore) Upsert(ctx context.Context, r consistency.MemoryRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	return s.checkStore.Upsert(ctx, r)
}
func TestProjectionSerializesReadModifyWrite(t *testing.T) {
	s := &serialStore{}
	p := projector(s)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Apply(context.Background(), projectionEvent(projectionRecord(1), false)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if s.max != 1 {
		t.Fatalf("concurrent store cycles=%d", s.max)
	}
}
