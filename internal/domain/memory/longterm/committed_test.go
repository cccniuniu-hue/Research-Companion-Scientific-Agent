package longterm

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"agi-assistant/internal/domain/memory/consistency"
)

func TestApplyCommittedRejectsOlderVersion(t *testing.T) {
	memory := New()
	memory.StoreItem(Item{
		ID:          41,
		UserID:      "u1",
		Content:     "v3",
		Version:     3,
		ContentHash: "hash-v3",
	})

	err := memory.ApplyCommitted(consistency.CommittedChangeSet{
		Upserts: []consistency.MemoryRecord{{
			ID:          41,
			UserID:      "u1",
			Content:     "v2",
			Version:     2,
			ContentHash: "hash-v2",
		}},
	})
	if !errors.Is(err, ErrStaleCommittedVersion) {
		t.Fatalf("error=%v, want ErrStaleCommittedVersion", err)
	}
	item, ok := memory.FindByID(41)
	if !ok || item.Content != "v3" || item.Version != 3 {
		t.Fatalf("stale update changed cache: %+v, ok=%v", item, ok)
	}
}

func TestApplyCommittedTombstoneRemovesItem(t *testing.T) {
	memory := New()
	memory.StoreItem(Item{
		ID:          41,
		UserID:      "u1",
		Content:     "active",
		Version:     1,
		ContentHash: "hash-v1",
	})
	deletedAt := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

	err := memory.ApplyCommitted(consistency.CommittedChangeSet{
		Deletes: []consistency.MemoryRecord{{
			ID:          41,
			UserID:      "u1",
			Version:     2,
			ContentHash: "delete-v2",
			DeletedAt:   &deletedAt,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := memory.FindByID(41); ok {
		t.Fatal("tombstoned item remains in active cache")
	}
}

func TestReplaceCommittedDropsLocalOnlyAndDeletedRows(t *testing.T) {
	memory := New()
	memory.StoreItem(Item{ID: 1, UserID: "u1", Content: "local-only", Version: 1})
	deletedAt := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)

	memory.ReplaceCommitted([]consistency.MemoryRecord{
		{ID: 2, UserID: "u1", Content: "authoritative", Version: 4, ContentHash: "hash-v4"},
		{ID: 3, UserID: "u1", Content: "deleted", Version: 2, DeletedAt: &deletedAt},
	})

	if memory.Count() != 1 {
		t.Fatalf("count=%d, want 1", memory.Count())
	}
	if _, ok := memory.FindByID(1); ok {
		t.Fatal("local-only item survived authoritative replacement")
	}
	item, ok := memory.FindByID(2)
	if !ok || item.Version != 4 {
		t.Fatalf("authoritative item missing: %+v, ok=%v", item, ok)
	}
	if _, ok := memory.FindByID(3); ok {
		t.Fatal("deleted row entered active cache")
	}
}

func TestApplyCommittedEqualVersionDifferentHashFails(t *testing.T) {
	memory := New()
	memory.StoreItem(Item{ID: 41, UserID: "u1", Content: "cached", Version: 2, ContentHash: "hash-a"})

	err := memory.ApplyCommitted(consistency.CommittedChangeSet{
		Upserts: []consistency.MemoryRecord{{
			ID:          41,
			UserID:      "u1",
			Content:     "different",
			Version:     2,
			ContentHash: "hash-b",
		}},
	})
	if !errors.Is(err, ErrCommittedHashConflict) {
		t.Fatalf("error=%v, want ErrCommittedHashConflict", err)
	}
}

func TestLongTermOwnsInputsAndReadResults(t *testing.T) {
	m := New()
	cfg := DefaultConsolidationConfig()
	m.SetConsolidationConfig(cfg)
	cfg.DedupThreshold = .1
	if m.ConsolidationCfg().DedupThreshold != .95 {
		t.Fatal("configuration input aliases cache")
	}
	copyCfg := m.ConsolidationCfg()
	copyCfg.DedupThreshold = .1
	if m.ConsolidationCfg().DedupThreshold != .95 {
		t.Fatal("configuration result aliases cache")
	}
	emb := []float64{1, 0}
	tags := []string{"tag"}
	m.StoreClassified("alice", "synthetic", .8, emb, "fact", tags, "")
	emb[0] = 99
	tags[0] = "changed"
	id := m.LastID()
	readers := []func() Item{func() Item { return m.Snapshot()[0] }, func() Item { x, _ := m.LastItem(); return x }, func() Item { x, _ := m.FindByID(id); return x }, func() Item { return m.FilterByCategory("alice", []string{"fact"}, 1)[0] }, func() Item { return m.Recall("alice", "synthetic", 1, []float64{1, 0})[0] }}
	for _, read := range readers {
		it := read()
		if it.Embedding[0] != 1 || it.Tags[0] != "tag" {
			t.Fatal("input mutation changed cache")
		}
		it.Embedding[0] = 99
		it.Tags[0] = "changed"
	}
	it, _ := m.FindByID(id)
	if it.Embedding[0] != 1 || it.Tags[0] != "tag" {
		t.Fatal("read-result mutation changed cache")
	}
	now := time.Now()
	item := Item{ID: 20, UserID: "alice", Embedding: []float64{1}, Tags: []string{"tag"}, Supersedes: []int{5}, DeletedAt: &now}
	m.StoreItem(item)
	item.Embedding[0] = 99
	item.Tags[0] = "changed"
	item.Supersedes[0] = 99
	*item.DeletedAt = time.Time{}
	it, _ = m.FindByID(20)
	if it.Embedding[0] != 1 || it.Tags[0] != "tag" || it.Supersedes[0] != 5 || it.DeletedAt.IsZero() {
		t.Fatal("restored item aliases cache")
	}
}

func TestCommittedBatchValidationIsAtomic(t *testing.T) {
	for _, kind := range []string{"duplicate", "upsert-delete", "owner", "stale"} {
		t.Run(kind, func(t *testing.T) {
			m := New()
			m.StoreItem(Item{ID: 1, UserID: "alice", Content: "cached", Version: 3, ContentHash: "v3"})
			changes := consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{{ID: 2, UserID: "alice", Content: "new", Version: 1}}}
			switch kind {
			case "duplicate":
				changes.Upserts = append(changes.Upserts, consistency.MemoryRecord{ID: 2, UserID: "alice", Version: 2})
			case "upsert-delete":
				changes.Deletes = []consistency.MemoryRecord{{ID: 2, UserID: "alice", Version: 2}}
			case "owner":
				changes.Upserts = append(changes.Upserts, consistency.MemoryRecord{ID: 1, UserID: "bob", Version: 4})
			case "stale":
				changes.Upserts = append(changes.Upserts, consistency.MemoryRecord{ID: 1, UserID: "alice", Version: 2})
			}
			before := m.Snapshot()
			if err := m.ApplyCommitted(changes); err == nil {
				t.Fatal("invalid batch accepted")
			}
			if !reflect.DeepEqual(before, m.Snapshot()) {
				t.Fatal("failed batch partially applied")
			}
		})
	}
}

func TestInactiveMemoryDoesNotSuppressNewStore(t *testing.T) {
	for _, kind := range []string{"quarantined", "superseded", "deleted"} {
		t.Run(kind, func(t *testing.T) {
			m := New()
			m.SetConsolidationConfig(DefaultConsolidationConfig())
			m.Store("alice", "old", .8, []float64{1, 0})
			id := m.LastID()
			switch kind {
			case "quarantined":
				m.Quarantine(id, "synthetic")
			case "superseded":
				m.MarkSuperseded([]int{id}, 0)
			case "deleted":
				now := time.Now()
				m.Items[0].DeletedAt = &now
			}
			if !m.Store("alice", "new", .8, []float64{1, 0}) || m.Count() != 2 {
				t.Fatal("inactive record blocked new memory")
			}
			got := m.Recall("alice", "", 10, []float64{1, 0})
			if len(got) != 1 || got[0].Content != "new" {
				t.Fatalf("inactive record recalled: %+v", got)
			}
		})
	}
}

func TestSupersessionRejectsMissingSelfAndForeignTargets(t *testing.T) {
	for _, kind := range []string{"missing", "self", "foreign"} {
		t.Run(kind, func(t *testing.T) {
			m := New()
			m.StoreItem(Item{ID: 1, UserID: "alice", Content: "old"})
			newID := 2
			switch kind {
			case "self":
				newID = 1
			case "foreign":
				m.StoreItem(Item{ID: 2, UserID: "bob", Content: "other"})
			}
			before := m.Snapshot()
			if marked := m.MarkSuperseded([]int{1}, newID); len(marked) != 0 {
				t.Fatalf("invalid relation marked: %v", marked)
			}
			if !reflect.DeepEqual(before, m.Snapshot()) {
				t.Fatal("invalid relation changed cache")
			}
		})
	}
}

func TestLongTermConcurrentPublicMethods(t *testing.T) {
	m := New()
	m.SetConsolidationConfig(DefaultConsolidationConfig())
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				m.Store("alice", "synthetic", .8, []float64{1, 0})
				m.Recall("alice", "synthetic", 2, nil)
				m.ConflictCandidates("alice", []float64{.8, .6}, "general", 0, 0)
				cfg := m.ConsolidationCfg()
				cfg.TriggerInterval = 5
				m.SetConsolidationConfig(cfg)
				m.PlanConsolidation(time.Now())
				m.Snapshot()
			}
		}()
	}
	wg.Wait()
	if m.Count() != 1 {
		t.Fatalf("concurrent dedup count=%d", m.Count())
	}
}
