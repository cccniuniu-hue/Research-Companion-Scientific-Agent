package chat

import (
	"agi-assistant/config"
	"agi-assistant/internal/domain/promptctx"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/pkg/logger"
	"agi-assistant/internal/usercontext"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSearchWebLogsTavilyFailureBeforeLLMFallback(t *testing.T) {
	cfg := &config.APIConfig{
		SearchConfig: config.SearchConfig{
			SearchAPIKey: "test-key",
			SearchAPIURL: "://invalid-url",
		},
	}
	agent := &UnifiedAgent{
		cfg:   cfg,
		llm:   llm.New(cfg),
		tools: newToolRegistry(nil),
	}
	agent.registerBuiltinTools()
	assertSearchFailureFallback(t, agent.tools.snapshot()["search_web"])
}

func TestBuiltinSearchLogsTavilyFailureBeforeLLMFallback(t *testing.T) {
	cfg := &config.APIConfig{SearchConfig: config.SearchConfig{
		SearchAPIKey: "test-key", SearchAPIURL: "://invalid-url",
	}}
	assertSearchFailureFallback(t, builtinSearchTool(cfg, llm.New(cfg)))
}

func assertSearchFailureFallback(t *testing.T, search tool.Tool) {
	t.Helper()

	previousLogger := logger.L()
	var logs bytes.Buffer
	logger.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer logger.SetDefault(previousLogger)

	result, err := search.Execute(map[string]interface{}{
		"query": "今天是几月几号",
	})

	if err != nil {
		t.Fatalf("search_web returned error: %v", err)
	}
	if result == "" {
		t.Fatal("expected LLM fallback result")
	}
	if !strings.Contains(result, "未获取实时搜索结果") {
		t.Fatalf("fallback did not disclose unavailable search: %q", result)
	}
	for _, want := range []string{
		"Tavily search failed, falling back to LLM",
		`"query":"今天是几月几号"`,
		"Tavily 请求失败",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("expected log to contain %q, got %s", want, logs.String())
		}
	}
}

func TestTaskOwnerSurvivesSnapshotRoundTrip(t *testing.T) {
	data := []byte(`{"state":{"task_id":"alice-task","user_id":"alice","query":"private query","steps":[{"name":"private step"}]},"timestamp":"now"}`)
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var fields struct {
		State map[string]interface{} `json:"state"`
	}
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if fields.State["user_id"] != "alice" {
		t.Fatalf("snapshot lost trusted task owner: %s", encoded)
	}
}

func TestLegacySnapshotDoesNotGainOwner(t *testing.T) {
	var snap Snapshot
	if err := json.Unmarshal([]byte(`{"state":{"task_id":"legacy-task"}}`), &snap); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var fields struct {
		State map[string]interface{} `json:"state"`
	}
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if owner, ok := fields.State["user_id"]; ok && owner != "" {
		t.Fatalf("legacy task acquired owner: %v", owner)
	}
}

func TestTaskCreationBindsTrustedContext(t *testing.T) {
	ctx := usercontext.With(context.Background(), "alice", "Alice")
	task := newTaskState(ctx, "task-a", "research")
	if task.UserID != "alice" || task.TaskID != "task-a" || task.Query != "research" || task.Status != "running" || task.Phase != "executing" {
		t.Fatalf("wrong initial task: %+v", task)
	}
	for _, ctx := range []context.Context{nil, context.Background(), context.WithValue(context.Background(), "user_id", "forged")} {
		if got := newTaskState(ctx, "t", "q").UserID; got != "" {
			t.Fatalf("untrusted context gained owner: %q", got)
		}
	}
}

func TestPlannerSnapshotRetainsOwnerAndRejectsForeignQueries(t *testing.T) {
	var snap Snapshot
	if err := json.Unmarshal([]byte(`{"state":{"user_id":"alice","task_id":"task-a","query":"private","status":"interrupted","phase":"interrupted","current_step":0,"interrupted_at":0,"steps":[{"name":"first"},{"name":"private next","tool_name":"search_web"}]}}`), &snap); err != nil {
		t.Fatal(err)
	}
	view := snap.State.plannerSnapshot()
	if view == nil || view.UserID != "alice" || view.NextStepName != "private next" {
		t.Fatalf("bad planner view: %+v", view)
	}
	source := promptctx.NewPlannerSource(snap.State.plannerSnapshot)
	for _, tc := range []struct {
		user, task string
		allowed    bool
	}{{"alice", "task-a", true}, {"bob", "task-a", false}, {"alice", "other", false}, {"", "task-a", false}, {"alice", "", false}} {
		t.Run(tc.user+"/"+tc.task, func(t *testing.T) {
			items, err := source.Fetch(context.Background(), promptctx.Slot{}, promptctx.Query{UserID: tc.user, TaskID: tc.task})
			if err != nil {
				t.Fatal(err)
			}
			if (len(items) > 0) != tc.allowed {
				t.Fatalf("scope leaked or missing: %v", items)
			}
		})
	}
	view.UserID = "bob"
	view.NextStepName = "mutated"
	if snap.State.UserID != "alice" || snap.State.Steps[1].Name != "private next" {
		t.Fatal("planner aliases task")
	}
	for _, task := range []*TaskState{nil, {TaskID: "legacy"}, {UserID: "alice"}} {
		if task.plannerSnapshot() != nil {
			t.Fatal("unowned task exposed")
		}
	}
	snap.State.CurrentStep = -2
	if snap.State.plannerSnapshot().NextStepName != "" {
		t.Fatal("invalid cursor exposes next step")
	}
	snap.State.CurrentStep = -1
	if snap.State.plannerSnapshot().NextStepName != "first" {
		t.Fatal("pre-execution cursor lost first step")
	}
}

func TestRepoBundleOwnsInfraHealthSnapshot(t *testing.T) {
	input := map[string]string{"postgres": "ready"}
	bundle := newRepoBundle(Deps{InfraStatus: input})
	input["postgres"] = "mutated"
	delete(input, "postgres")
	if got := bundle.infraSnapshot()["postgres"]; got != "ready" {
		t.Fatalf("caller changed startup health: %q", got)
	}
	view := bundle.infraSnapshot()
	view["postgres"] = "changed"
	if bundle.infraSnapshot()["postgres"] != "ready" {
		t.Fatal("returned health snapshot aliases state")
	}
	empty := newRepoBundle(Deps{}).infraSnapshot()
	if empty == nil || len(empty) != 0 {
		t.Fatal("expected empty independent map")
	}
}

func TestSearchRejectsBlankOrNonStringQuery(t *testing.T) {
	cfg := &config.APIConfig{}
	search := builtinSearchTool(cfg, llm.New(cfg))
	for _, query := range []interface{}{nil, "", " \t\n", 42} {
		t.Run(strings.TrimSpace(searchFixtureJSON(query)), func(t *testing.T) {
			result, err := search.Execute(map[string]interface{}{"query": query})
			if err == nil || result != "" {
				t.Fatalf("invalid query accepted: %q %v", result, err)
			}
		})
	}
}

func searchFixtureJSON(v interface{}) string { b, _ := json.Marshal(v); return string(b) }

func TestSearchLabelsOfflineFallback(t *testing.T) {
	cfg := &config.APIConfig{}
	search := builtinSearchTool(cfg, llm.New(cfg))
	result, err := search.Execute(map[string]interface{}{"query": "research"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "未获取实时搜索结果") || !strings.Contains(result, "演示") {
		t.Fatalf("fallback falsely looks like live search: %q", result)
	}
}

func TestSearchUsesConfiguredEndpoint(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("wrong HTTP request")
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["query"] != "science" || body["api_key"] != "fake-search-key" {
			t.Errorf("wrong query/key: %v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"answer":"verified fixture","results":[{"title":"paper","url":"https://example.invalid/paper"}]}`))
	}))
	defer server.Close()
	cfg := &config.APIConfig{SearchConfig: config.SearchConfig{SearchAPIKey: "fake-search-key", SearchAPIURL: server.URL}}
	result, err := builtinSearchTool(cfg, llm.New(cfg)).Execute(map[string]interface{}{"query": "  science \n"})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !strings.HasPrefix(result, "verified fixture") || !strings.Contains(result, "来源") || strings.Contains(result, "未获取") {
		t.Fatalf("live result was replaced: %q calls=%d", result, calls)
	}
}

func TestSearchModelFallbackDoesNotClaimLiveResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fake-model-key" {
			t.Error("configured model was not used")
		}
		var body struct {
			Messages []llm.Message `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if len(body.Messages) != 2 || !strings.Contains(body.Messages[0].Content, "实时搜索不可用") {
			t.Errorf("missing honest fallback instructions: %+v", body.Messages)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"fixture model answer"}}]}`))
	}))
	defer server.Close()
	cfg := &config.APIConfig{LLMConfig: config.LLMConfig{
		LLMAPIUrl: server.URL, LLMAPIKey: "fake-model-key", LLMModel: "fixture-model",
	}}
	result, err := builtinSearchTool(cfg, llm.New(cfg)).Execute(map[string]interface{}{"query": "research"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "未经实时核验") || !strings.Contains(result, "fixture model answer") || strings.Contains(result, "本地演示") {
		t.Fatalf("misleading model fallback: %q", result)
	}
}
