package consistency_test

import (
	"math"
	"testing"
	"time"

	"agi-assistant/internal/domain/memory/consistency"
)

func TestComputeContentHashCanonicalRepresentations(t *testing.T) {
	instant := time.Date(2026, 10, 15, 12, 0, 0, 123, time.UTC)
	local := instant.In(time.FixedZone("UTC+8", 8*60*60))
	a := consistency.MemoryRecord{ID: 1, UserID: "synthetic", Version: 1, DeletedAt: &instant, Supersedes: []int64{4, 2}}
	b := a
	b.DeletedAt, b.Supersedes = &local, []int64{2, 4}
	b.Tags, b.Embedding = []string{}, []float64{}
	ha, err := consistency.ComputeContentHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := consistency.ComputeContentHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb || len(ha) != 64 {
		t.Fatalf("canonical hashes: %q != %q", ha, hb)
	}
}

func TestComputeContentHashRejectsNonFiniteValues(t *testing.T) {
	for _, record := range []consistency.MemoryRecord{
		{Importance: math.NaN()}, {Importance: math.Inf(1)}, {Embedding: []float64{math.Inf(-1)}},
	} {
		if hash, err := consistency.ComputeContentHash(record); err == nil || hash != "" {
			t.Fatalf("invalid number accepted: %q, %v", hash, err)
		}
	}
}

func TestComputeContentHashTracksUserContentAndState(t *testing.T) {
	base := consistency.MemoryRecord{ID: 1, UserID: "alice", Content: "synthetic", Version: 1}
	hash, err := consistency.ComputeContentHash(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*consistency.MemoryRecord){
		func(r *consistency.MemoryRecord) { r.UserID = "bob" },
		func(r *consistency.MemoryRecord) { r.Content = "changed" },
		func(r *consistency.MemoryRecord) { r.Quarantined = true },
		func(r *consistency.MemoryRecord) { r.Superseded = true },
		func(r *consistency.MemoryRecord) { r.EmbeddingRevision = "new" },
	} {
		r := base
		change(&r)
		got, err := consistency.ComputeContentHash(r)
		if err != nil || got == hash {
			t.Fatalf("changed record has unchanged/invalid hash: %+v, %v", r, err)
		}
	}
}

func TestComputeContentHashIsStableAcrossTagOrder(t *testing.T) {
	a := consistency.MemoryRecord{
		ID:      7,
		UserID:  "u1",
		Content: "用户喜欢咖啡",
		Version: 2,
		Tags:    []string{"src:user", "preference"},
	}
	b := a
	b.Tags = []string{"preference", "src:user"}

	hashA, err := consistency.ComputeContentHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := consistency.ComputeContentHash(b)
	if err != nil {
		t.Fatal(err)
	}

	if hashA != hashB {
		t.Fatalf("tag order changed canonical hash: %s != %s", hashA, hashB)
	}
}

func TestComputeContentHashChangesWithVersion(t *testing.T) {
	a := consistency.MemoryRecord{
		ID:      7,
		UserID:  "u1",
		Content: "用户喜欢咖啡",
		Version: 1,
	}
	b := a
	b.Version = 2

	hashA, err := consistency.ComputeContentHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := consistency.ComputeContentHash(b)
	if err != nil {
		t.Fatal(err)
	}

	if hashA == hashB {
		t.Fatal("version change did not change projection hash")
	}
}

func TestComputeContentHashDoesNotMutateCallerSlices(t *testing.T) {
	record := consistency.MemoryRecord{
		ID:         9,
		UserID:     "u1",
		Content:    "x",
		Version:    3,
		Tags:       []string{"z", "a"},
		Supersedes: []int64{8, 2},
	}

	if _, err := consistency.ComputeContentHash(record); err != nil {
		t.Fatal(err)
	}

	if record.Tags[0] != "z" || record.Tags[1] != "a" {
		t.Fatalf("hashing mutated tags: %v", record.Tags)
	}
	if record.Supersedes[0] != 8 || record.Supersedes[1] != 2 {
		t.Fatalf("hashing mutated supersedes: %v", record.Supersedes)
	}
}
