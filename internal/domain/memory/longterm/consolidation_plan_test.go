package longterm

import (
	"math"
	"reflect"
	"testing"
	"time"

	"agi-assistant/internal/domain/memory/consistency"
)

func TestPlanConsolidationDoesNotMutateCache(t *testing.T) {
	memory := New()
	memory.SetConsolidationConfig(&ConsolidationConfig{
		SimilarityThreshold: 0.8,
		DedupThreshold:      0.95,
		TTLDays:             30,
		DecayRate:           1,
		MinImportance:       0.3,
		TriggerInterval:     1,
	})
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	memory.StoreItem(Item{
		ID: 1, UserID: "u1", Content: "用户喜欢咖啡", Importance: 0.8,
		Embedding: []float64{1, 0}, CreatedAt: now.Add(-time.Hour),
		Version: 2, ContentHash: "hash-1",
	})
	memory.StoreItem(Item{
		ID: 2, UserID: "u1", Content: "用户非常喜欢咖啡", Importance: 0.7,
		Embedding: []float64{1, 0}, CreatedAt: now.Add(-time.Hour),
		Version: 5, ContentHash: "hash-2",
	})
	before := memory.Snapshot()

	plan := memory.PlanConsolidation(now)

	after := memory.Snapshot()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("planning mutated cache:\nbefore=%+v\nafter=%+v", before, after)
	}
	if len(plan.Deletes) != 1 {
		t.Fatalf("deletes=%d, want 1: %+v", len(plan.Deletes), plan)
	}
	if plan.Deletes[0].ID != 2 || plan.Deletes[0].ExpectedVersion != 5 {
		t.Fatalf("delete lacks authoritative identity/version: %+v", plan.Deletes[0])
	}
}

func TestPlanConsolidationKeepsHigherImportanceRecordID(t *testing.T) {
	memory := New()
	memory.SetConsolidationConfig(&ConsolidationConfig{
		SimilarityThreshold: 0.8,
		DedupThreshold:      0.99,
		TTLDays:             30,
		DecayRate:           1,
		MinImportance:       0.3,
		TriggerInterval:     1,
	})
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	memory.StoreItem(Item{
		ID: 1, UserID: "u1", Content: "偏好咖啡", Importance: 0.5,
		Embedding: []float64{1, 0}, CreatedAt: now, Version: 2,
	})
	memory.StoreItem(Item{
		ID: 2, UserID: "u1", Content: "非常偏好咖啡", Importance: 0.9,
		Embedding: []float64{0.9, 0.435889894}, CreatedAt: now, Version: 7,
	})

	plan := memory.PlanConsolidation(now)

	if len(plan.Updates) != 1 || plan.Updates[0].Record.ID != 2 || plan.Updates[0].ExpectedVersion != 7 {
		t.Fatalf("higher-importance survivor was not updated: %+v", plan)
	}
	if len(plan.Deletes) != 1 || plan.Deletes[0].ID != 1 || plan.Deletes[0].ExpectedVersion != 2 {
		t.Fatalf("lower-importance row was not deleted: %+v", plan)
	}
}

func TestConsolidationPreservesBoundaries(t *testing.T) {
	for _, mode := range []string{"plan", "legacy"} {
		for _, boundary := range []string{"user", "category", "quarantined", "superseded", "deleted"} {
			t.Run(mode+"/"+boundary, func(t *testing.T) {
				m := New()
				cfg := DefaultConsolidationConfig()
				cfg.DecayRate = 1
				m.SetConsolidationConfig(cfg)
				a := Item{ID: 1, UserID: "alice", Category: "fact", Content: "synthetic", Importance: .8, Embedding: []float64{1, 0}, CreatedAt: time.Now(), Version: 1}
				b := a
				b.ID = 2
				switch boundary {
				case "user":
					b.UserID = "bob"
				case "category":
					b.Category = "preference"
				case "quarantined":
					b.Quarantined = true
				case "superseded":
					b.Superseded = true
				case "deleted":
					now := time.Now()
					b.DeletedAt = &now
				}
				m.StoreItem(a)
				m.StoreItem(b)
				before := m.Snapshot()
				if mode == "plan" {
					p := m.PlanConsolidation(time.Now())
					if len(p.Updates)+len(p.Deletes) != 0 {
						t.Fatalf("crossed %s boundary: %+v", boundary, p)
					}
				} else {
					p := m.Consolidate()
					if len(p.DeleteFromDB)+len(p.UpdateInDB) != 0 {
						t.Fatalf("crossed %s boundary: %+v", boundary, p)
					}
				}
				if !reflect.DeepEqual(before, m.Snapshot()) {
					t.Fatal("boundary records changed")
				}
			})
		}
	}
}

func TestLegacyConsolidationKeepsSurvivorAndFinalUpdates(t *testing.T) {
	m := New()
	cfg := DefaultConsolidationConfig()
	cfg.DecayRate = 1
	cfg.DedupThreshold = 1.1
	cfg.SimilarityThreshold = .5
	m.SetConsolidationConfig(cfg)
	now := time.Now()
	for _, it := range []Item{
		{ID: 1, UserID: "alice", Content: "first", Importance: .4, Embedding: []float64{1, 0}, CreatedAt: now, Category: "fact", Version: 2},
		{ID: 2, UserID: "alice", Content: "second", Importance: .9, Embedding: []float64{.9, .435889894}, CreatedAt: now, Category: "fact", Tags: []string{"kept"}, SlotHint: "profile", Version: 7, ContentHash: "cached"},
		{ID: 3, UserID: "alice", Content: "third", Importance: .3, Embedding: []float64{1, 0}, CreatedAt: now, Category: "fact", Version: 1},
	} {
		m.StoreItem(it)
	}
	result := m.Consolidate()
	if m.Count() != 1 || len(result.UpdateInDB) != 1 || len(result.DeleteFromDB) != 2 {
		t.Fatalf("inconsistent final result: %+v, %+v", result, m.Snapshot())
	}
	it, ok := m.FindByID(2)
	if !ok || it.UserID != "alice" || it.Category != "fact" || it.SlotHint != "profile" || it.Version != 7 || len(it.Tags) != 1 || len(it.Supersedes) != 2 {
		t.Fatalf("survivor metadata lost: %+v", it)
	}
	if !reflect.DeepEqual(result.UpdateInDB[0], it) {
		t.Fatal("returned update is an intermediate state")
	}
	for _, id := range result.DeleteFromDB {
		if id == 2 {
			t.Fatal("surviving ID is also deleted")
		}
	}
}

func TestConsolidationHandlesSingletonAndElapsedDecay(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	m := New()
	cfg := DefaultConsolidationConfig()
	cfg.DecayRate = .9
	cfg.TTLDays = 0
	m.SetConsolidationConfig(cfg)
	m.StoreItem(Item{ID: 1, UserID: "alice", Content: "synthetic", Importance: 1, CreatedAt: now.Add(-24 * time.Hour), Version: 1})
	p := m.PlanConsolidation(now)
	if len(p.Updates) != 1 || math.Abs(p.Updates[0].Record.Importance-.9) > 1e-9 {
		t.Fatalf("singleton decay missing: %+v", p)
	}
	record := p.Updates[0].Record
	record.Version = 2
	record.UpdatedAt = now
	if err := m.ApplyCommitted(consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{record}}); err != nil {
		t.Fatal(err)
	}
	p = m.PlanConsolidation(now.Add(24 * time.Hour))
	if len(p.Updates) != 1 || math.Abs(p.Updates[0].Record.Importance-.81) > 1e-9 {
		t.Fatalf("decay reapplied entire lifetime: %+v", p)
	}
	for _, mode := range []string{"plan", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			m := New()
			cfg := DefaultConsolidationConfig()
			cfg.DecayRate = 1
			m.SetConsolidationConfig(cfg)
			m.StoreItem(Item{ID: 1, UserID: "alice", Content: "old", Importance: .1, CreatedAt: time.Now().Add(-31 * 24 * time.Hour), Version: 1})
			if mode == "plan" {
				if p := m.PlanConsolidation(time.Now()); len(p.Deletes) != 1 || p.Deletes[0].Reason != "expired" {
					t.Fatalf("singleton not expired: %+v", p)
				}
			} else if p := m.Consolidate(); p.Expired != 1 || m.Count() != 0 {
				t.Fatalf("singleton not expired: %+v", p)
			}
		})
	}
}

func TestConsolidationProtectionAndNonMutation(t *testing.T) {
	m := New()
	cfg := DefaultConsolidationConfig()
	cfg.DecayRate = 1
	cfg.ProtectFn = func(ids []int) []int { return ids }
	m.SetConsolidationConfig(cfg)
	for _, id := range []int{1, 2} {
		m.StoreItem(Item{ID: id, UserID: "alice", Content: "synthetic", Importance: .8, Embedding: []float64{1, 0}, Tags: []string{"tag"}, Version: 1})
	}
	before := m.Snapshot()
	p := m.PlanConsolidation(time.Now())
	if len(p.Deletes) != 0 || !reflect.DeepEqual(before, m.Snapshot()) {
		t.Fatalf("protected plan mutated/deleted cache: %+v", p)
	}
	if r := m.Consolidate(); len(r.DeleteFromDB) != 0 || m.Count() != 2 || r.Deduped != 0 {
		t.Fatalf("protected records deleted: %+v", r)
	}
}
