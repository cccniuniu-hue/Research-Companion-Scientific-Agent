package chat

import (
	"agi-assistant/config"
	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/rag"
	ltmrepo "agi-assistant/internal/infrastructure/persistence/longterm"
	"agi-assistant/internal/infrastructure/persistence/memorytx"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeMemoryStore struct {
	createChanges consistency.CommittedChangeSet
	createErr     error
	active        []consistency.MemoryRecord
	loadErr       error
}

func (f *fakeMemoryStore) Create(context.Context, memorytx.CreateCommand) (consistency.CommittedChangeSet, error) {
	return f.createChanges, f.createErr
}

func (f *fakeMemoryStore) Update(context.Context, memorytx.UpdateCommand) (consistency.CommittedChangeSet, error) {
	return consistency.CommittedChangeSet{}, errors.New("not used")
}

func (f *fakeMemoryStore) Tombstone(context.Context, memorytx.DeleteCommand) (consistency.CommittedChangeSet, error) {
	return consistency.CommittedChangeSet{}, errors.New("not used")
}

func (f *fakeMemoryStore) LoadActive(context.Context) ([]consistency.MemoryRecord, error) {
	return f.active, f.loadErr
}

func (f *fakeMemoryStore) LoadPage(context.Context, int64, int) ([]consistency.MemoryRecord, error) {
	return nil, errors.New("not used")
}
func (f *fakeMemoryStore) ApplyConsolidation(context.Context, consistency.ConsolidationPlan) (consistency.CommittedChangeSet, error) {
	return consistency.CommittedChangeSet{}, errors.New("not used")
}

func TestCommitMemoryFailureDoesNotMutateCache(t *testing.T) {
	cache := longterm.New()
	store := &fakeMemoryStore{createErr: errors.New("postgres unavailable")}
	agent := &UnifiedAgent{
		mem:   &memoryStack{ltm: cache},
		repos: &repoBundle{memoryTx: store},
	}

	_, err := agent.commitMemory(context.Background(), memorytx.CreateCommand{
		UserID:  "u1",
		Content: "用户喜欢咖啡",
	})

	if err == nil {
		t.Fatal("commitMemory succeeded after PostgreSQL failure")
	}
	if cache.Count() != 0 {
		t.Fatalf("cache changed before commit: count=%d", cache.Count())
	}
}

func TestCommitMemoryAppliesAuthoritativeIDAndVersionAfterCommit(t *testing.T) {
	cache := longterm.New()
	store := &fakeMemoryStore{
		createChanges: consistency.CommittedChangeSet{
			Upserts: []consistency.MemoryRecord{{
				ID:          91,
				UserID:      "u1",
				Content:     "用户喜欢咖啡",
				Version:     4,
				ContentHash: "hash-v4",
			}},
		},
	}
	agent := &UnifiedAgent{
		mem:   &memoryStack{ltm: cache},
		repos: &repoBundle{memoryTx: store},
	}

	record, err := agent.commitMemory(context.Background(), memorytx.CreateCommand{
		UserID:  "u1",
		Content: "用户喜欢咖啡",
	})
	if err != nil {
		t.Fatal(err)
	}

	if record.ID != 91 || record.Version != 4 {
		t.Fatalf("returned non-authoritative record: %+v", record)
	}
	item, ok := cache.FindByID(91)
	if !ok || item.Version != 4 || item.ContentHash != "hash-v4" {
		t.Fatalf("committed row not visible in cache: %+v, ok=%v", item, ok)
	}
}

func TestCommitMemoryReloadsAuthoritativeCacheAfterApplyConflict(t *testing.T) {
	cache := longterm.New()
	cache.StoreItem(longterm.Item{
		ID: 91, UserID: "u1", Content: "cache-v5", Version: 5, ContentHash: "hash-v5",
	})
	store := &fakeMemoryStore{
		createChanges: consistency.CommittedChangeSet{
			Upserts: []consistency.MemoryRecord{{
				ID: 91, UserID: "u1", Content: "committed-v4", Version: 4, ContentHash: "hash-v4",
			}},
		},
		active: []consistency.MemoryRecord{{
			ID: 92, UserID: "u1", Content: "reloaded-v6", Version: 6, ContentHash: "hash-v6",
		}},
	}
	agent := &UnifiedAgent{
		mem:   &memoryStack{ltm: cache},
		repos: &repoBundle{memoryTx: store},
	}

	if _, err := agent.commitMemory(context.Background(), memorytx.CreateCommand{
		UserID: "u1", Content: "committed-v4",
	}); err != nil {
		t.Fatal(err)
	}

	if _, ok := cache.FindByID(91); ok {
		t.Fatal("conflicting cache was not replaced from PostgreSQL")
	}
	item, ok := cache.FindByID(92)
	if !ok || item.Version != 6 {
		t.Fatalf("authoritative reload missing: %+v, ok=%v", item, ok)
	}
}

type restoreMemoryFixture struct {
	memorytx.Store
	rows  []consistency.MemoryRecord
	err   error
	calls int
}

func (f *restoreMemoryFixture) LoadActive(context.Context) ([]consistency.MemoryRecord, error) {
	f.calls++
	return f.rows, f.err
}

type restoreLegacyFixture struct {
	ltmrepo.Repo
	rows  []ltmrepo.Row
	calls int
}

func (f *restoreLegacyFixture) Load() []ltmrepo.Row { f.calls++; return f.rows }

type restoreRAGFixture struct {
	ragchunk.Repo
	rows []ragchunk.Row
	err  error
}

func (f *restoreRAGFixture) LoadAll() ([]ragchunk.Row, error) { return f.rows, f.err }
func (f *restoreRAGFixture) PGAvailable() bool                { return true }
func (f *restoreRAGFixture) ESAvailable() bool                { return false }
func (f *restoreRAGFixture) MilvusAvailable() bool            { return false }

func TestRestoreUsesAuthoritativeRowsAndDoesNotFallBackOnFailure(t *testing.T) {
	cache := longterm.New()
	cache.StoreItem(longterm.Item{ID: 9, UserID: "alice", Content: "old"})
	source := &restoreMemoryFixture{rows: []consistency.MemoryRecord{{ID: 1, UserID: "alice", Content: "active", Version: 3, Category: "fact"}, {ID: 2, UserID: "bob", Content: "quarantined", Version: 2, Quarantined: true}}}
	legacy := &restoreLegacyFixture{}
	agent := &UnifiedAgent{mem: &memoryStack{ltm: cache}, repos: &repoBundle{memoryTx: source, ltm: legacy}}
	agent.restoreFromDB()
	if cache.Count() != 2 || legacy.calls != 0 || source.calls != 1 {
		t.Fatal("incorrect authoritative restoration")
	}
	if row, ok := cache.FindByID(1); !ok || row.Version != 3 || row.UserID != "alice" {
		t.Fatalf("lost authoritative metadata: %+v", row)
	}
	if got := cache.RecallByFilter("quarantined", []float64{1}, longterm.RecallFilter{UserID: "bob", TopK: 10}); len(got) != 0 {
		t.Fatal("quarantined row recalled")
	}
	source.err = errors.New("unavailable")
	agent.restoreFromDB()
	if cache.Count() != 2 || legacy.calls != 0 {
		t.Fatal("failed authoritative read replaced cache or used legacy fallback")
	}
	source.err = nil
	source.rows = nil
	agent.restoreFromDB()
	if cache.Count() != 0 {
		t.Fatal("empty authoritative state did not replace cache")
	}
}

func TestRestoreLegacyPreservesOwnerAndClassifications(t *testing.T) {
	source := &restoreLegacyFixture{rows: []ltmrepo.Row{{ID: 7, UserID: "alice", Content: "legacy", Category: "identity", Tags: []string{"name"}, SlotHint: "profile", Quarantined: true, QuarantineReason: "audit", Superseded: true, Supersedes: []int{3}}}}
	cache := longterm.New()
	agent := &UnifiedAgent{mem: &memoryStack{ltm: cache}, repos: &repoBundle{ltm: source}}
	agent.restoreFromDB()
	row, ok := cache.FindByID(7)
	if !ok || row.UserID != "alice" || row.Category != "identity" || !row.Quarantined || !row.Superseded || len(row.Supersedes) != 1 {
		t.Fatalf("lost legacy metadata: %+v", row)
	}
}

func TestRestoreRAGRetainsLoadedFlagAfterReadFailure(t *testing.T) {
	source := &restoreRAGFixture{rows: []ragchunk.Row{{ID: 8, Content: "one"}, {ID: 40, Content: "two"}}}
	cfg := &config.APIConfig{}
	engine := rag.NewEngine(cfg, source, nil)
	agent := &UnifiedAgent{rag: engine, repos: &repoBundle{ragChunk: source}}
	agent.restoreRAGFromDB()
	if !engine.Loaded || len(engine.Chunks()) != 2 || engine.Chunks()[1].Content != "two" {
		t.Fatal("RAG preview not restored")
	}
	source.rows = []ragchunk.Row{{Content: "partial"}}
	source.err = errors.New("stream failure")
	agent.restoreRAGFromDB()
	if !engine.Loaded || len(engine.Chunks()) != 0 {
		t.Fatal("failed read changed loaded state or exposed a partial preview")
	}
}

type memoryLedger struct {
	memorytx.Store
	create func(context.Context, memorytx.CreateCommand) (consistency.CommittedChangeSet, error)
	load   func(context.Context) ([]consistency.MemoryRecord, error)
	page   func(context.Context, int64, int) ([]consistency.MemoryRecord, error)
	apply  func(context.Context, consistency.ConsolidationPlan) (consistency.CommittedChangeSet, error)
}

func (f *memoryLedger) Create(ctx context.Context, c memorytx.CreateCommand) (consistency.CommittedChangeSet, error) {
	return f.create(ctx, c)
}
func (f *memoryLedger) LoadActive(ctx context.Context) ([]consistency.MemoryRecord, error) {
	return f.load(ctx)
}
func (f *memoryLedger) LoadPage(ctx context.Context, a int64, n int) ([]consistency.MemoryRecord, error) {
	return f.page(ctx, a, n)
}
func (f *memoryLedger) ApplyConsolidation(ctx context.Context, p consistency.ConsolidationPlan) (consistency.CommittedChangeSet, error) {
	return f.apply(ctx, p)
}

func TestMemoryCommitRejectsForeignReceipt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record consistency.MemoryRecord
	}{
		{"foreign", consistency.MemoryRecord{ID: 8, UserID: "bob", Version: 1}},
		{"missing_id", consistency.MemoryRecord{UserID: "alice", Version: 1}},
		{"missing_version", consistency.MemoryRecord{ID: 8, UserID: "alice"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeMemoryStore{createChanges: consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{tc.record}}}
			cache := longterm.New()
			agent := &UnifiedAgent{mem: &memoryStack{ltm: cache}, repos: &repoBundle{memoryTx: store}}
			if _, err := agent.commitMemory(context.Background(), memorytx.CreateCommand{UserID: "alice", Content: "fact"}); err == nil {
				t.Error("invalid committed receipt accepted")
			}
			if cache.Count() != 0 {
				t.Fatal("invalid or foreign receipt entered cache")
			}
		})
	}
}

func TestMemoryCommitClearsStaleCacheWhenRefreshFails(t *testing.T) {
	cache := longterm.New()
	cache.StoreItem(longterm.Item{ID: 1, UserID: "alice", Content: "stale", Version: 4})
	store := &fakeMemoryStore{createChanges: consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{{ID: 1, UserID: "alice", Content: "committed", Version: 2}}}, loadErr: errors.New("read unavailable")}
	agent := &UnifiedAgent{mem: &memoryStack{ltm: cache}, repos: &repoBundle{memoryTx: store}}
	row, err := agent.commitMemory(context.Background(), memorytx.CreateCommand{UserID: "alice", Content: "committed"})
	if err != nil || row.ID != 1 {
		t.Fatalf("committed write was reported uncommitted: %+v %v", row, err)
	}
	if cache.Count() != 0 {
		t.Fatal("stale cache stayed available after failed authoritative refresh")
	}
}

func TestMemoryCommitsSerializeRepositoryAndCacheApplication(t *testing.T) {
	entered := make(chan struct{})
	second := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	store := &memoryLedger{create: func(_ context.Context, c memorytx.CreateCommand) (consistency.CommittedChangeSet, error) {
		n := calls.Add(1)
		if n == 1 {
			close(entered)
			<-release
		} else {
			close(second)
		}
		return consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{{ID: int64(n), UserID: c.UserID, Content: c.Content, Version: 1}}}, nil
	}}
	agent := &UnifiedAgent{mem: &memoryStack{ltm: longterm.New()}, repos: &repoBundle{memoryTx: store}}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = agent.commitMemory(context.Background(), memorytx.CreateCommand{UserID: "alice", Content: "one"})
	}()
	<-entered
	go func() {
		defer wg.Done()
		_, _ = agent.commitMemory(context.Background(), memorytx.CreateCommand{UserID: "bob", Content: "two"})
	}()
	early := false
	select {
	case <-second:
		early = true
	case <-time.After(80 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if early {
		t.Fatal("second transaction entered while first commit/cache operation was pending")
	}
	if agent.mem.ltm.Count() != 2 {
		t.Fatal("a serialized commit disappeared")
	}
}

func TestConsolidationMissingRepositoryDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("consolidation panicked: %v", r)
		}
	}()
	cache := longterm.New()
	cache.SetConsolidationConfig(&longterm.ConsolidationConfig{TTLDays: 1, MinImportance: 0.1})
	cache.StoreItem(longterm.Item{ID: 1, UserID: "alice", Content: "old", Version: 1, CreatedAt: time.Now().Add(-48 * time.Hour)})
	agent := &UnifiedAgent{mem: &memoryStack{ltm: cache}}
	if len(cache.PlanConsolidation(time.Now()).Deletes) == 0 {
		t.Fatal("fixture does not require persistence")
	}
	if err := agent.consolidateCommitted(context.Background()); err == nil {
		t.Fatal("missing repository accepted for a nonempty plan")
	}
	if cache.Count() != 1 {
		t.Fatal("cache changed before transaction")
	}
}

func TestConsolidationPreservesAuthoritativeEmbeddingMetadata(t *testing.T) {
	cache := longterm.New()
	cache.SetConsolidationConfig(&longterm.ConsolidationConfig{DecayRate: 0.1})
	cache.StoreItem(longterm.Item{ID: 1, UserID: "alice", Content: "fact", Importance: 1, Version: 1, UpdatedAt: time.Now().Add(-48 * time.Hour)})
	called := false
	store := &memoryLedger{page: func(context.Context, int64, int) ([]consistency.MemoryRecord, error) {
		return []consistency.MemoryRecord{{ID: 1, UserID: "alice", Content: "fact", Version: 1, EmbeddingModel: "vector-model", EmbeddingRevision: "rev-2"}}, nil
	}, apply: func(_ context.Context, p consistency.ConsolidationPlan) (consistency.CommittedChangeSet, error) {
		called = true
		if len(p.Updates) != 1 {
			t.Fatalf("wrong fixture plan: %+v", p)
		}
		r := p.Updates[0].Record
		if r.EmbeddingModel != "vector-model" || r.EmbeddingRevision != "rev-2" {
			t.Error("consolidation erased source embedding metadata")
		}
		r.Version = 2
		return consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{r}}, nil
	}}
	agent := &UnifiedAgent{mem: &memoryStack{ltm: cache}, repos: &repoBundle{memoryTx: store}}
	if err := agent.consolidateCommitted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("fixture did not produce an update")
	}
}
