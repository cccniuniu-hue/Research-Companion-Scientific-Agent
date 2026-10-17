package promptctx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"agi-assistant/internal/domain/sandbox"
	"agi-assistant/internal/domain/tool"
)

func TestConstraintsSource_BlockBeforeWarn(t *testing.T) {
	policies := sandbox.PolicySnapshot()
	src := NewConstraintsSource(policies)

	slot := Slot{Kind: SlotConstraints, Filter: SlotFilter{}}
	items, err := src.Fetch(context.Background(), slot, Query{})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("expected policies, got none")
	}

	// 第一批应该是 Block（score=1.0），之后才是 Warn（score=0.5）
	seenWarn := false
	for _, it := range items {
		if it.Score < 1.0 {
			seenWarn = true
		}
		if seenWarn && it.Score == 1.0 {
			t.Errorf("Block policy found after Warn policy — ordering violated")
		}
	}
}

func TestConstraintsSource_TopKTruncation(t *testing.T) {
	policies := sandbox.PolicySnapshot()
	src := NewConstraintsSource(policies)

	topK := 5
	slot := Slot{Kind: SlotConstraints, Filter: SlotFilter{TopK: topK}}
	items, _ := src.Fetch(context.Background(), slot, Query{})
	if len(items) > topK {
		t.Errorf("expected ≤%d items, got %d", topK, len(items))
	}
}

func TestConstraintsSource_ContainsRmRf(t *testing.T) {
	policies := sandbox.PolicySnapshot()
	src := NewConstraintsSource(policies)

	slot := Slot{Kind: SlotConstraints, Filter: SlotFilter{}}
	items, _ := src.Fetch(context.Background(), slot, Query{})

	found := false
	for _, it := range items {
		if strings.Contains(it.Text, "rm") || strings.Contains(it.Text, "删除") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected rm-related policy in constraints, not found")
	}
}

func TestConstraintsSource_EmptyPolicies(t *testing.T) {
	src := NewConstraintsSource(nil)
	slot := Slot{Kind: SlotConstraints, Filter: SlotFilter{}}
	items, err := src.Fetch(context.Background(), slot, Query{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected 0 items for empty policy list, got %d", len(items))
	}
}

func TestUnownedTaskAndToolDataIsHidden(t *testing.T) {
	b := NewTaskMemBuffer(3)
	b.Push(StepObservation{Result: "unowned", Success: true})
	tr := NewToolStateTracker(3)
	tr.Record(ToolCallTrace{Summary: "unowned", Success: true})
	sources := []ContextSource{NewTaskMemSource(b), NewToolStateSource(nil, tr), NewPlannerSource(func() *PlannerSnapshot { return &PlannerSnapshot{TaskID: "task", Query: "unowned"} })}
	for _, src := range sources {
		t.Run(src.ID(), func(t *testing.T) {
			got, err := src.Fetch(context.Background(), Slot{}, Query{UserID: "alice", TaskID: "task"})
			if err != nil || len(got) != 0 {
				t.Fatalf("unowned data leaked: %+v %v", got, err)
			}
		})
	}
}

func TestTaskMemoryScopesBeforeAgeAndTopK(t *testing.T) {
	b := NewTaskMemBuffer(10)
	now := time.Now()
	b.Push(StepObservation{UserID: "alice", TaskID: "task", Result: "valid", Success: true, CreatedAt: now})
	b.Push(StepObservation{UserID: "bob", TaskID: "task", Result: "foreign", Success: true})
	b.Push(StepObservation{UserID: "alice", TaskID: "other", Result: "other-task", Success: true})
	b.Push(StepObservation{UserID: "alice", TaskID: "task", Result: "expired", Success: true, CreatedAt: now.Add(-48 * time.Hour)})
	src := NewTaskMemSource(b)
	slot := Slot{Filter: SlotFilter{TopK: 1, MaxAgeHours: 24}}
	got, err := src.Fetch(context.Background(), slot, Query{UserID: "alice", TaskID: "task"})
	if err != nil || len(got) != 1 || !strings.Contains(got[0].Text, "valid") {
		t.Fatalf("wrong task observation: %+v %v", got, err)
	}
	for _, q := range []Query{{TaskID: "task"}, {UserID: "alice"}} {
		if got, _ := src.Fetch(context.Background(), slot, q); len(got) != 0 {
			t.Fatal("missing scope returned observations")
		}
	}
}

func TestPlannerOwnerTaskAndFirstInterruptedStep(t *testing.T) {
	snap := &PlannerSnapshot{UserID: "alice", TaskID: "task", Status: "interrupted", TotalSteps: 2, InterruptedAt: 0, NextStepName: "next", NextStepTool: "synthetic"}
	src := NewPlannerSource(func() *PlannerSnapshot { return snap })
	for _, q := range []Query{{UserID: "bob", TaskID: "task"}, {UserID: "alice", TaskID: "other"}, {TaskID: "task"}, {UserID: "alice"}} {
		got, _ := src.Fetch(context.Background(), Slot{}, q)
		if len(got) != 0 {
			t.Errorf("foreign planner returned: %+v", got)
		}
	}
	got, _ := src.Fetch(context.Background(), Slot{}, Query{UserID: "alice", TaskID: "task"})
	var all string
	for _, it := range got {
		all += it.Text
	}
	if !strings.Contains(all, "第 1 步被中断") || !strings.Contains(all, "next") {
		t.Fatalf("planner step zero/render failed: %q", all)
	}
}

func TestToolHistoryScopesAndRegistryOrder(t *testing.T) {
	tr := NewToolStateTracker(10)
	now := time.Now()
	for _, it := range []ToolCallTrace{{UserID: "alice", TaskID: "task", Summary: "valid", CreatedAt: now}, {UserID: "bob", TaskID: "task", Summary: "foreign"}, {UserID: "alice", TaskID: "other", Summary: "other-task"}, {UserID: "alice", TaskID: "task", Summary: "expired", CreatedAt: now.Add(-48 * time.Hour)}} {
		tr.Record(it)
	}
	src := NewToolStateSource(func() map[string]tool.Tool {
		return map[string]tool.Tool{"z": {Description: "last"}, "a": {Description: "first", Parameters: []tool.Param{{Name: "arg", Required: true}}}}
	}, tr)
	got, err := src.Fetch(context.Background(), Slot{Filter: SlotFilter{TopK: 1, MaxAgeHours: 24}}, Query{UserID: "alice", TaskID: "task"})
	if err != nil || len(got) != 3 || !strings.HasPrefix(got[0].Text, "a —") || !strings.Contains(got[0].Text, "必填 arg") || !strings.Contains(got[2].Text, "valid") {
		t.Fatalf("tool scope/order failed: %+v %v", got, err)
	}
	got, _ = src.Fetch(context.Background(), Slot{}, Query{})
	if len(got) != 2 {
		t.Fatal("anonymous query received private history")
	}
	tr.Record(ToolCallTrace{UserID: "alice", TaskID: "", Summary: "single-tool"})
	got, _ = src.Fetch(context.Background(), Slot{}, Query{UserID: "alice"})
	if len(got) != 3 || !strings.Contains(got[2].Text, "single-tool") {
		t.Fatal("single-tool namespace mixed task history")
	}
}

func TestSourceTruncationPreservesUnicode(t *testing.T) {
	t.Run("task", func(t *testing.T) {
		b := NewTaskMemBuffer(1)
		b.Push(StepObservation{UserID: "alice", TaskID: "task", Result: strings.Repeat("中", 201), Success: true})
		got, _ := NewTaskMemSource(b).Fetch(context.Background(), Slot{}, Query{UserID: "alice", TaskID: "task"})
		if len(got) != 1 || !utf8.ValidString(got[0].Text) || !strings.Contains(got[0].Text, strings.Repeat("中", 200)+"…") {
			t.Fatal("task result split UTF-8 or counted bytes")
		}
	})
	t.Run("tool", func(t *testing.T) {
		tr := NewToolStateTracker(1)
		tr.Record(ToolCallTrace{Summary: strings.Repeat("中", 121)})
		if got := tr.Snapshot()[0].Summary; !utf8.ValidString(got) || utf8.RuneCountInString(got) != 121 {
			t.Fatalf("tool summary counted bytes: %q", got)
		}
	})
}

func TestBuffersOwnSnapshotsAndBoundConcurrentWrites(t *testing.T) {
	b := NewTaskMemBuffer(2)
	tr := NewToolStateTracker(2)
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				b.Push(StepObservation{Result: "synthetic"})
				tr.Record(ToolCallTrace{Summary: "synthetic"})
				b.Snapshot()
				tr.Snapshot()
			}
		}()
	}
	wg.Wait()
	bs, ts := b.Snapshot(), tr.Snapshot()
	if len(bs) != 2 || len(ts) != 2 || bs[0].CreatedAt.IsZero() || ts[0].CreatedAt.IsZero() {
		t.Fatal("FIFO/timestamps failed")
	}
	bs[0].Result = "changed"
	ts[0].Summary = "changed"
	if b.Snapshot()[0].Result == "changed" || tr.Snapshot()[0].Summary == "changed" {
		t.Fatal("snapshot shares storage")
	}
	b.Reset()
	if len(b.Snapshot()) != 0 {
		t.Fatal("reset failed")
	}
}

func TestConstraintSnapshotAndCanceledSources(t *testing.T) {
	policies := []sandbox.Policy{{Reason: "synthetic", Level: sandbox.RiskBlock}}
	constraints := NewConstraintsSource(policies)
	policies[0].Reason = "changed"
	got, _ := constraints.Fetch(context.Background(), Slot{}, Query{})
	if len(got) != 1 || strings.Contains(got[0].Text, "changed") {
		t.Fatal("policy snapshot aliases input")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	sources := []ContextSource{constraints, NewPlannerSource(func() *PlannerSnapshot { calls++; return &PlannerSnapshot{UserID: "alice", TaskID: "task"} }), NewTaskMemSource(NewTaskMemBuffer(1)), NewToolStateSource(func() map[string]tool.Tool { calls++; return nil }, nil)}
	for _, src := range sources {
		items, err := src.Fetch(ctx, Slot{}, Query{UserID: "alice", TaskID: "task"})
		if !errors.Is(err, context.Canceled) || len(items) != 0 {
			t.Errorf("canceled %s returned success/data", src.ID())
		}
	}
	if calls != 0 {
		t.Fatal("canceled request invoked provider")
	}
}
