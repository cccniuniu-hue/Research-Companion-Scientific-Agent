package promptctx

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/memory/preference"
)

// fixedPrefProvider 适配单测：把固定 *Preference 包成"按 userID 取桶"的 provider。
// 测试场景里只有一个 user，所有 userID 都映射到同一个 *Preference。
func fixedPrefProvider(p *preference.Preference) PreferenceProvider {
	return func(_ string) *preference.Preference { return p }
}

func TestProfileSource_OnlyIdentityPreference(t *testing.T) {
	pref := preference.New()
	pref.Save("城市", "北京")
	pref.Save("语言", "中文")

	ltm := longterm.New()
	ltm.StoreClassified("u1", "我叫张三", 0.9, nil, "identity", []string{"name"}, "profile")
	ltm.StoreClassified("u1", "用户喜欢晚睡", 0.8, nil, "preference", nil, "profile")
	ltm.StoreClassified("u1", "今天天气很好", 0.7, nil, "episodic", nil, "recall_memory")

	src := NewProfileSource(fixedPrefProvider(pref), ltm)

	slot := Slot{
		Kind:   SlotProfile,
		Filter: SlotFilter{Categories: []string{"identity", "preference"}, TopK: 10},
	}
	items, err := src.Fetch(context.Background(), slot, Query{UserID: "u1"})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	// pref 输出 2 项，ltm 中只有 identity/preference 2 条
	if len(items) != 4 {
		t.Errorf("expected 4 items, got %d", len(items))
	}
	for _, it := range items {
		if it.Source != "profile" {
			t.Errorf("unexpected source: %s", it.Source)
		}
	}
	// 确认 episodic 条目没有混入
	for _, it := range items {
		if it.Text == "今天天气很好" {
			t.Errorf("episodic item should not appear in profile slot")
		}
	}
}

func TestProfileSource_EmptyFilter_NoLTMItems(t *testing.T) {
	pref := preference.New()
	ltm := longterm.New()
	ltm.StoreClassified("u1", "some fact", 0.9, nil, "fact", nil, "")

	src := NewProfileSource(fixedPrefProvider(pref), ltm)

	// 没有 Categories 过滤，LTM 不输出
	slot := Slot{Kind: SlotProfile, Filter: SlotFilter{}}
	items, err := src.Fetch(context.Background(), slot, Query{UserID: "u1"})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	// pref 空，ltm filter 为空 → 都为空
	if len(items) != 0 {
		t.Errorf("expected 0 items, got %d", len(items))
	}
}

func TestProfileSource_StableOrder(t *testing.T) {
	pref := preference.New()
	pref.Save("z_key", "v1")
	pref.Save("a_key", "v2")
	pref.Save("m_key", "v3")

	src := NewProfileSource(fixedPrefProvider(pref), nil)
	slot := Slot{Kind: SlotProfile, Filter: SlotFilter{}}
	items, _ := src.Fetch(context.Background(), slot, Query{UserID: "u1"})

	if len(items) != 3 {
		t.Fatalf("expected 3 pref items, got %d", len(items))
	}
	// 应该按字母序：a_key, m_key, z_key
	keys := []string{"a_key", "m_key", "z_key"}
	for i, expected := range keys {
		if items[i].Text[:5] != expected[:5] {
			t.Errorf("item[%d] expected to start with %s, got %s", i, expected, items[i].Text)
		}
	}
	_ = time.Now() // suppress unused import
}

// TestProfileSource_AnonymousNoLeak 验证未登录请求不会拿到任何 profile 内容。
// 这是 Bug 1 引入的 fail-closed 防御：q.UserID 为空 → 返 nil
func TestProfileSource_AnonymousNoLeak(t *testing.T) {
	pref := preference.New()
	pref.Save("城市", "北京")
	ltm := longterm.New()
	ltm.StoreClassified("u1", "should-not-leak", 0.9, nil, "identity", nil, "profile")

	src := NewProfileSource(fixedPrefProvider(pref), ltm)
	slot := Slot{Kind: SlotProfile, Filter: SlotFilter{Categories: []string{"identity"}, TopK: 10}}
	items, _ := src.Fetch(context.Background(), slot, Query{}) // 没传 UserID

	if len(items) != 0 {
		t.Errorf("UserID 为空时不应返回任何 profile 数据, 得到 %d 条", len(items))
	}
}

// TestProfileSource_PerUserBucket 验证 PreferenceProvider 按 userID 取不同桶。
// alice 写偏好 → bob 调用 Fetch 时不应看到 alice 的偏好。
func TestProfileSource_PerUserBucket(t *testing.T) {
	alicePref := preference.New()
	alicePref.Save("姓名", "Alice")
	bobPref := preference.New()
	bobPref.Save("姓名", "Bob")

	provider := func(userID string) *preference.Preference {
		switch userID {
		case "alice":
			return alicePref
		case "bob":
			return bobPref
		}
		return nil
	}

	src := NewProfileSource(provider, nil)
	slot := Slot{Kind: SlotProfile, Filter: SlotFilter{}}

	aliceOut, _ := src.Fetch(context.Background(), slot, Query{UserID: "alice"})
	if len(aliceOut) != 1 || aliceOut[0].Text != "姓名: Alice" {
		t.Errorf("alice 应只看到自己: %+v", aliceOut)
	}

	bobOut, _ := src.Fetch(context.Background(), slot, Query{UserID: "bob"})
	if len(bobOut) != 1 || bobOut[0].Text != "姓名: Bob" {
		t.Errorf("bob 应只看到自己: %+v", bobOut)
	}
}

func TestProfileFiltersBeforeSelectingTopK(t *testing.T) {
	m := longterm.New()
	now := time.Now()
	for id := 1; id <= 6; id++ {
		it := longterm.Item{ID: id, UserID: "alice", Content: "valid", Category: "identity", Importance: .9, Tags: []string{"verified"}, CreatedAt: now}
		switch id {
		case 1:
			it.Tags = nil
		case 2:
			it.CreatedAt = now.Add(-48 * time.Hour)
		case 3:
			it.Importance = .2
		case 5:
			it.UserID = "bob"
		case 6:
			it.Quarantined = true
		}
		if id != 4 {
			it.Content = "excluded"
		}
		m.StoreItem(it)
	}
	src := NewProfileSource(nil, m)
	slot := Slot{Kind: SlotProfile, Filter: SlotFilter{Categories: []string{"identity"}, RequireTags: []string{"verified"}, MaxAgeHours: 24, MinScore: .5, TopK: 1}}
	items, err := src.Fetch(context.Background(), slot, Query{UserID: "alice"})
	if err != nil || len(items) != 1 || items[0].Text != "valid" {
		t.Fatalf("profile filter/topK failed: %+v %v", items, err)
	}
}

func TestProfileTopKCapsCombinedPreferencesAndMemory(t *testing.T) {
	pref := preference.New()
	pref.Save("b", "second")
	pref.Save("a", "first")
	m := longterm.New()
	m.Store("alice", "synthetic", .9, nil)
	m.StoreClassified("alice", "identity", .9, nil, "identity", nil, "")
	src := NewProfileSource(func(string) *preference.Preference { return pref }, m)
	items, err := src.Fetch(context.Background(), Slot{Kind: SlotProfile, Filter: SlotFilter{Categories: []string{"identity"}, TopK: 2}}, Query{UserID: "alice"})
	if err != nil || len(items) != 2 || items[0].Text != "a: first" || items[1].Text != "b: second" {
		t.Fatalf("combined profile TopK/order failed: %+v %v", items, err)
	}
}

type probeRecaller struct {
	called    bool
	text      string
	embedding []float64
	filter    longterm.RecallFilter
}

func (r *probeRecaller) RecallByFilter(text string, emb []float64, f longterm.RecallFilter) []longterm.Item {
	r.called = true
	r.text = text
	r.embedding = emb
	r.filter = f
	return []longterm.Item{{UserID: f.UserID, Content: "synthetic", Importance: .9, Score: .8, Category: "fact", SlotHint: "recall_memory"}}
}
func TestRecallForwardsQueryAndTagFilters(t *testing.T) {
	r := &probeRecaller{}
	src := NewRecallSource(r)
	emb := []float64{1, 0}
	tags := []string{"verified"}
	items, err := src.Fetch(context.Background(), Slot{Kind: SlotRecall, Filter: SlotFilter{RequireTags: tags, TopK: 1}}, Query{Text: "synthetic query", Embedding: emb, UserID: "alice"})
	if err != nil || !r.called || r.text != "synthetic query" || !reflect.DeepEqual(r.embedding, emb) || !reflect.DeepEqual(r.filter.RequireTags, tags) || r.filter.UserID != "alice" {
		t.Fatal("query/filter forwarding failed")
	}
	if len(items) != 1 || items[0].Meta["slot_hint"] != "recall_memory" {
		t.Fatal("recall metadata lost")
	}
}

func TestProfileAndRecallSkipCanceledRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	r := &probeRecaller{}
	sources := []ContextSource{NewProfileSource(func(string) *preference.Preference { calls++; return preference.New() }, nil), NewRecallSource(r)}
	for _, src := range sources {
		items, err := src.Fetch(ctx, Slot{}, Query{UserID: "alice"})
		if !errors.Is(err, context.Canceled) || len(items) != 0 {
			t.Errorf("canceled %s returned data/success", src.ID())
		}
	}
	if calls != 0 || r.called {
		t.Fatal("canceled source called provider")
	}
}

func TestStructuredPreferencesRequireKnownMetadata(t *testing.T) {
	pref := preference.New()
	pref.Save("name", "synthetic")
	src := NewProfileSource(func(string) *preference.Preference { return pref }, nil)
	for name, f := range map[string]SlotFilter{"tags": {RequireTags: []string{"verified"}}, "score": {MinScore: 1.1}} {
		t.Run(name, func(t *testing.T) {
			items, err := src.Fetch(context.Background(), Slot{Kind: SlotProfile, Filter: f}, Query{UserID: "alice"})
			if err != nil || len(items) != 0 {
				t.Fatalf("unqualified preference included: %+v %v", items, err)
			}
		})
	}
}
