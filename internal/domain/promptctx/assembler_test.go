package promptctx

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/memory/preference"
	"agi-assistant/internal/domain/sandbox"
	"agi-assistant/internal/domain/tool"
)

func buildAssembler() *ContextAssembler {
	pref := preference.New()
	pref.Save("城市", "北京")
	pref.Save("语言", "中文")

	ltm := longterm.New()
	ltm.StoreClassified("u1", "用户叫张三", 0.9, nil, "identity", []string{"name"}, "profile")
	ltm.StoreClassified("u1", "用户喜欢喝咖啡", 0.85, nil, "preference", nil, "profile")
	ltm.StoreClassified("u1", "上次问过天气API", 0.7, nil, "episodic", nil, "recall_memory")
	ltm.StoreClassified("u1", "python 是一门动态语言", 0.6, nil, "fact", nil, "recall_memory")

	reg := NewSourceRegistry()
	// 单测用固定 provider：所有 userID 都映射到同一个 *preference.Preference
	reg.Register(NewProfileSource(func(_ string) *preference.Preference { return pref }, ltm))
	reg.Register(NewConstraintsSource(sandbox.PolicySnapshot()))
	reg.Register(NewRecallSource(ltm))
	reg.Register(NewToolStateSource(
		func() map[string]tool.Tool {
			return map[string]tool.Tool{
				"get_time": {Name: "get_time", Description: "获取当前时间"},
			}
		},
		NewToolStateTracker(5),
	))
	reg.Register(NewTaskMemSource(NewTaskMemBuffer(5)))
	reg.Register(NewPlannerSource(func() *PlannerSnapshot { return nil }))

	return NewAssembler(DefaultSchemas(), reg)
}

// testQuery 构造带 UserID 的 Query——RecallSource/ProfileSource 现在
// 在 UserID 为空时 fail-closed，所有 assembler 单测都需要显式带上。
func testQuery(mode, text string) Query {
	return Query{Mode: mode, Text: text, UserID: "u1"}
}

func TestAssembler_ChatMode_HasFewSlots(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), testQuery("chat", "你好"))
	if rc == nil {
		t.Fatal("nil RuntimeContext")
	}

	rendered := rc.Render()

	// Chat 模式不应包含 Planner / TaskMem / ToolState
	if strings.Contains(rendered, "【任务规划】") {
		t.Error("chat mode should not render Planner slot")
	}
	if strings.Contains(rendered, "【任务记忆】") {
		t.Error("chat mode should not render TaskMem slot")
	}
	if strings.Contains(rendered, "【可用工具】") {
		t.Error("chat mode should not render ToolState slot")
	}
}

func TestAssembler_ReactMode_RendersPlannerAndTools(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), testQuery("react", "查天气并写诗"))

	rendered := rc.Render()

	// ReAct 模式应至少渲染 Constraints + Tools（Planner 当前 task=nil 返回空，可被跳过）
	if !strings.Contains(rendered, "【硬性约束】") {
		t.Error("react mode must include Constraints slot")
	}
	if !strings.Contains(rendered, "【可用工具】") {
		t.Error("react mode must include ToolState slot")
	}
}

func TestAssembler_UnknownModeFallsBackToChat(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), testQuery("nonexistent", "hi"))

	if rc.Schema.Mode != "chat" {
		t.Errorf("expected fallback to chat mode, got %s", rc.Schema.Mode)
	}
}

func TestAssembler_GlobalBudgetTruncation(t *testing.T) {
	asm := buildAssembler()
	asm.globalLimit = 200 // 极小预算，强制裁剪

	rc := asm.Assemble(context.Background(), testQuery("react", "test"))
	rendered := rc.Render()

	total := 0
	for _, slot := range rc.Filled {
		for _, item := range slot.Items {
			total += utf8.RuneCountInString(item.Text)
		}
	}
	if total > asm.globalLimit {
		t.Errorf("body character budget exceeded: %d > %d", total, asm.globalLimit)
	}

	// Constraints 优先级最高，在小预算下应仍然保留
	if !strings.Contains(rendered, "【硬性约束】") {
		t.Error("Constraints slot must survive global budget truncation (highest priority)")
	}
}

func TestAssembler_RenderProducesValidPrefix(t *testing.T) {
	asm := buildAssembler()
	rc := asm.Assemble(context.Background(), testQuery("chat", "你好"))
	rendered := rc.Render()

	if rendered == "" {
		t.Fatal("expected non-empty render")
	}
	// 偏好 + 至少一个其他 slot
	if !strings.Contains(rendered, "城市") {
		t.Error("expected preference 城市 in render")
	}
}

func TestDefaultSchemasOwnNestedSlices(t *testing.T) {
	a, b := DefaultSchemas(), DefaultSchemas()
	a["chat"].Slots[1].Filter.Categories[0] = "changed"
	a["chat"].Slots[0].Filter.TokenBudget = 1
	if b["chat"].Slots[1].Filter.Categories[0] != "identity" || b["chat"].Slots[0].Filter.TokenBudget != 200 {
		t.Fatal("independent default schemas share mutable storage")
	}
	if DefaultSchemas()["chat"].Slots[1].Filter.Categories[0] != "identity" {
		t.Fatal("caller mutation changed later defaults")
	}
}

func TestContextRenderingSkipsBlankSections(t *testing.T) {
	rc := &RuntimeContext{Filled: []FilledSlot{{Kind: SlotProfile, Items: []ContextItem{{Text: "  "}}}, {Kind: SlotRecall, Items: []ContextItem{{Text: "synthetic"}}}, {Kind: SlotPlanner, Skipped: true, Items: []ContextItem{{Text: "hidden"}}}}}
	if got := rc.Render(); got != "【相关回忆】\n- synthetic" {
		t.Fatalf("blank/skipped sections rendered: %q", got)
	}
	if rc.SlotByKind(SlotRecall) == nil || rc.SlotByKind(SlotConstraints) != nil {
		t.Fatal("slot lookup failed")
	}
	var empty *RuntimeContext
	if empty.Render() != "" || empty.SlotByKind(SlotProfile) != nil {
		t.Fatal("nil context failed")
	}
}

func TestSchemaModesAndPriority(t *testing.T) {
	schemas := DefaultSchemas()
	if len(schemas) != 4 {
		t.Fatal("expected four modes")
	}
	for mode, s := range schemas {
		if s.Mode != mode || len(s.Slots) == 0 || s.Slots[0].Kind != SlotConstraints {
			t.Fatalf("bad schema: %+v", s)
		}
	}
	if len(schemas["react"].Slots) != 6 || slotPriority(SlotConstraints) >= slotPriority(SlotRecall) {
		t.Fatal("schema/priority mismatch")
	}
	rc := &RuntimeContext{Filled: []FilledSlot{{Kind: SlotConstraints, Items: []ContextItem{{Text: "synthetic policy"}}}, {Kind: SlotProfile, Items: []ContextItem{{Text: "synthetic profile"}}}}}
	if !strings.HasPrefix(rc.Render(), "【硬性约束】") {
		t.Fatal("filled slot order changed")
	}
}

type assemblyProbe struct {
	kind  SlotKind
	name  string
	fetch func(context.Context, Slot, Query) ([]ContextItem, error)
}

func (s *assemblyProbe) ID() string               { return s.name }
func (s *assemblyProbe) Supports(k SlotKind) bool { return k == s.kind }
func (s *assemblyProbe) Fetch(c context.Context, slot Slot, q Query) ([]ContextItem, error) {
	return s.fetch(c, slot, q)
}
func assemblyFixture(slots []Slot, sources ...ContextSource) *ContextAssembler {
	r := NewSourceRegistry()
	for _, s := range sources {
		r.Register(s)
	}
	return NewAssembler(map[string]RuntimeContextSchema{"chat": {Mode: "chat", Slots: slots}}, r)
}

func TestAssemblyBudgetCountsUnicodeAndPriority(t *testing.T) {
	items := []ContextItem{{Text: "中文"}, {Text: "abc"}}
	if got := trimByBudget(items, 2); len(got) != 1 {
		t.Fatalf("budget counted UTF-8 bytes: %+v", got)
	}
	rc := &RuntimeContext{Schema: RuntimeContextSchema{Slots: []Slot{{Kind: SlotConstraints}, {Kind: SlotRecall}}}, Filled: []FilledSlot{{Kind: SlotConstraints, Items: []ContextItem{{Text: "硬性"}}}, {Kind: SlotRecall, Items: []ContextItem{{Text: "候选"}}}}}
	a := NewAssembler(nil, NewSourceRegistry())
	a.globalLimit = 4
	a.applyGlobalBudget(rc)
	if len(rc.Filled[0].Items) != 1 || len(rc.Filled[1].Items) != 1 {
		t.Fatal("equal rune budget removed fitting content")
	}
	a.globalLimit = 2
	a.applyGlobalBudget(rc)
	if len(rc.Filled[0].Items) != 1 || len(rc.Filled[1].Items) != 0 {
		t.Fatal("lower-priority content was not removed first")
	}
}

func TestAssemblerOwnsConstructorAndReturnedSchemas(t *testing.T) {
	s := map[string]RuntimeContextSchema{"chat": {Mode: "chat", Slots: []Slot{{Kind: SlotProfile, Filter: SlotFilter{Categories: []string{"identity"}, RequireTags: []string{"keep"}}}}}}
	a := NewAssembler(s, NewSourceRegistry())
	s["chat"].Slots[0].Filter.Categories[0] = "changed"
	delete(s, "chat")
	rc := a.Assemble(context.Background(), Query{Mode: "chat"})
	if rc.Schema.Mode != "chat" || rc.Schema.Slots[0].Filter.Categories[0] != "identity" {
		t.Fatal("constructor schema aliases caller")
	}
	rc.Schema.Slots[0].Filter.RequireTags[0] = "changed"
	if a.Assemble(context.Background(), Query{Mode: "chat"}).Schema.Slots[0].Filter.RequireTags[0] != "keep" {
		t.Fatal("returned schema aliases assembler")
	}
}

func TestAssemblySourcesCannotMutateOtherSourceInputs(t *testing.T) {
	a := assemblyFixture([]Slot{{Kind: SlotRecall, Filter: SlotFilter{Categories: []string{"fact"}}}},
		&assemblyProbe{kind: SlotRecall, name: "mutating", fetch: func(_ context.Context, s Slot, q Query) ([]ContextItem, error) {
			s.Filter.Categories[0] = "changed"
			q.Embedding[0] = 9
			return nil, nil
		}},
		&assemblyProbe{kind: SlotRecall, name: "checking", fetch: func(_ context.Context, s Slot, q Query) ([]ContextItem, error) {
			if s.Filter.Categories[0] != "fact" || q.Embedding[0] != 1 {
				t.Error("source inputs share mutable slices")
			}
			return []ContextItem{{Text: "synthetic"}}, nil
		}})
	emb := []float64{1}
	a.Assemble(context.Background(), Query{Mode: "chat", Embedding: emb})
	if emb[0] != 1 {
		t.Fatal("source modified query caller")
	}
}

func TestAssemblySourceErrorAllowsOtherProviders(t *testing.T) {
	a := assemblyFixture([]Slot{{Kind: SlotRecall}}, &assemblyProbe{kind: SlotRecall, name: "failed", fetch: func(context.Context, Slot, Query) ([]ContextItem, error) { return nil, errors.New("synthetic error") }}, &assemblyProbe{kind: SlotRecall, name: "valid", fetch: func(context.Context, Slot, Query) ([]ContextItem, error) {
		return []ContextItem{{Text: "fallback"}}, nil
	}})
	rc := a.Assemble(context.Background(), Query{Mode: "chat"})
	if got := rc.Render(); !strings.Contains(got, "fallback") {
		t.Fatalf("valid provider lost after unrelated error: %q", got)
	}
}

func TestAssemblyCancellationDoesNotPublishPartialData(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before", false: "during"}[before], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			a := assemblyFixture([]Slot{{Kind: SlotRecall}}, &assemblyProbe{kind: SlotRecall, name: "probe", fetch: func(context.Context, Slot, Query) ([]ContextItem, error) {
				calls.Add(1)
				cancel()
				return []ContextItem{{Text: "must disappear"}}, nil
			}})
			if before {
				cancel()
			}
			if rc := a.Assemble(ctx, Query{Mode: "chat"}); rc.Render() != "" {
				t.Fatal("canceled assembly published content")
			}
			if before && calls.Load() != 0 {
				t.Fatal("canceled assembly called provider")
			}
		})
	}
}

func TestAssemblyTopKAndMetadataOwnership(t *testing.T) {
	shared := []ContextItem{{Text: "中文", Meta: map[string]string{"owner": "source"}}, {Text: "second"}}
	a := assemblyFixture([]Slot{{Kind: SlotRecall, Filter: SlotFilter{TopK: 1, TokenBudget: 2}}}, &assemblyProbe{kind: SlotRecall, name: "shared", fetch: func(context.Context, Slot, Query) ([]ContextItem, error) { return shared, nil }})
	rc := a.Assemble(context.Background(), Query{Mode: "chat"})
	if len(rc.Filled[0].Items) != 1 || !utf8.ValidString(rc.Render()) {
		t.Fatal("slot TopK/rune budget failed")
	}
	rc.Filled[0].Items[0].Meta["owner"] = "changed"
	if shared[0].Meta["owner"] != "source" {
		t.Fatal("output metadata aliases source")
	}
	a = assemblyFixture([]Slot{{Kind: SlotRecall, Filter: SlotFilter{TopK: 1}}}, &assemblyProbe{kind: SlotRecall, name: "topk", fetch: func(context.Context, Slot, Query) ([]ContextItem, error) { return shared, nil }})
	if len(a.Assemble(context.Background(), Query{Mode: "chat"}).Filled[0].Items) != 1 {
		t.Fatal("assembler omitted final TopK")
	}
}

func TestNilRegistryAndRegistrySnapshot(t *testing.T) {
	t.Run("nil assembler registry", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("nil registry panicked: %v", r)
			}
		}()
		rc := NewAssembler(nil, nil).Assemble(context.Background(), Query{Mode: "chat"})
		if rc.Render() != "" {
			t.Fatal("empty registry produced content")
		}
	})
	t.Run("nil source and independent snapshot", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("registry panicked: %v", r)
			}
		}()
		r := NewSourceRegistry()
		r.Register(nil)
		src := &assemblyProbe{kind: SlotRecall, name: "one", fetch: func(context.Context, Slot, Query) ([]ContextItem, error) { return nil, nil }}
		r.Register(src)
		list := r.For(SlotRecall)
		list[0] = nil
		if r.For(SlotRecall)[0] != src {
			t.Fatal("registry snapshot aliases source list")
		}
	})
}
