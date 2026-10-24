package chat

import (
	"agi-assistant/config"
	"agi-assistant/internal/domain/graph"
	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/domain/promptctx"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	"agi-assistant/internal/infrastructure/persistence/memorytx"
	"agi-assistant/internal/usercontext"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type noopSnapshotRepo struct{}

func (noopSnapshotRepo) Save(taskID string, stateJSON []byte) {}

func TestGraphRuntimeReplacesSkillPlaceholderWithUpstreamResult(t *testing.T) {
	tg := graph.NewTaskGraph([]*graph.Node{
		{
			ID:       "n1",
			Type:     graph.NodeTypeTool,
			ToolName: "source",
			Params:   map[string]string{"query": "draft"},
		},
		{
			ID:        "n2",
			Type:      graph.NodeTypeTool,
			ToolName:  "skill_doc_polish",
			Params:    map[string]string{"input": "基于n1搜索结果撰写的初稿"},
			DependsOn: []graph.NodeID{"n1"},
		},
	})

	var skillInput string
	agent := &UnifiedAgent{cfg: &config.APIConfig{
		HarnessConfig: config.HarnessConfig{MaxRetries: 1, StepTimeoutMs: 1000},
	}, runtime: newTaskRuntime(), repos: &repoBundle{snap: noopSnapshotRepo{}}}
	ctx := usercontext.With(context.Background(), "skill-user", "")
	task := newTaskState(ctx, "skill-task", "润色报告")
	rt := NewGraphRuntime(tg, agent, GraphConfig{MaxParallel: 1}, map[string]tool.Tool{
		"source": {
			Name: "source",
			ExecuteCtx: func(ctx context.Context, params map[string]interface{}) (string, error) {
				return "这是来自 n1 的真实搜索结果", nil
			},
		},
		"skill_doc_polish": {
			Name: "skill_doc_polish",
			Parameters: []tool.Param{
				{Name: "input", Type: "string", Required: true},
			},
			ExecuteCtx: func(ctx context.Context, params map[string]interface{}) (string, error) {
				skillInput, _ = params["input"].(string)
				return "polished", nil
			},
		},
	}, task, nil)

	result := rt.Execute(ctx)
	if result.Interrupted {
		t.Fatalf("runtime interrupted: %s", result.InterruptedMsg)
	}
	if !strings.Contains(skillInput, "这是来自 n1 的真实搜索结果") {
		t.Fatalf("expected upstream result in skill input, got %q", skillInput)
	}
	if strings.Contains(skillInput, "基于n1搜索结果撰写的初稿") {
		t.Fatalf("placeholder was not replaced: %q", skillInput)
	}
}
func plannerAgent(t *testing.T, output string) *UnifiedAgent {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]string{"content": output}}}})
	}))
	t.Cleanup(srv.Close)
	cfg := &config.APIConfig{}
	cfg.LLMAPIKey = "fictional"
	cfg.LLMAPIUrl = srv.URL
	cfg.LLMModel = "test"
	return &UnifiedAgent{cfg: cfg, llm: llm.New(cfg), subagents: newSubAgentRegistry()}
}
func TestPlannerRejectsToolDisguisedSubAgentAndUnknownType(t *testing.T) {
	for _, output := range []string{`[{"id":"n1","type":"tool","tool":"read","agent":"unapproved"}]`, `[{"id":"n1","type":"invalid","tool":"read"}]`} {
		t.Run(output, func(t *testing.T) {
			a := plannerAgent(t, output)
			nodes := a.llmPlanGraph(context.Background(), "question", map[string]tool.Tool{"read": {Name: "read"}}, "", false)
			if len(nodes) != 0 {
				t.Fatalf("unapproved executor: %+v", nodes[0])
			}
		})
	}
}
func TestPlannerNormalizesSupportedFormats(t *testing.T) {
	for _, output := range []string{`[{"tool":"read","params":{"query":"q"},"reason":"why"}]`, `[{"name":"read","parameters":{"query":"q"}}]`, "```json\n[{\"id\":\"n1\",\"type\":\"tool\",\"tool\":\"read\",\"params\":{\"query\":\"q\"}}]\n```"} {
		t.Run(output, func(t *testing.T) {
			a := plannerAgent(t, output)
			nodes := a.llmPlanGraph(context.Background(), "question", map[string]tool.Tool{"read": {Name: "read"}}, "", false)
			if len(nodes) != 1 || nodes[0].Type != graph.NodeTypeTool || nodes[0].ToolName != "read" || nodes[0].Params["query"] != "q" || nodes[0].ID == "" {
				t.Fatalf("wrong normalized nodes %+v", nodes)
			}
		})
	}
}
func TestReplannerEnforcesAppendLimitAndExecutors(t *testing.T) {
	output := `[{"id":"r1","tool":"read"},{"id":"r2","tool":"read"},{"id":"r3","tool":"read"},{"id":"r4","tool":"read"}]`
	a := plannerAgent(t, output)
	nodes := a.llmReplan(context.Background(), replanContext{Query: "question"}, map[string]tool.Tool{"read": {Name: "read"}}, "", false)
	if len(nodes) != 3 {
		t.Fatalf("append limit got %d", len(nodes))
	}
	a = plannerAgent(t, `[{"id":"r1","type":"tool","agent":"unapproved","tool":"read"}]`)
	if n := a.llmReplan(context.Background(), replanContext{}, map[string]tool.Tool{"read": {Name: "read"}}, "", false); len(n) != 0 {
		t.Fatal("disguised subagent replanned")
	}
}
func TestPlannerCancellationDoesNotFallbackToToolExecutionPlan(t *testing.T) {
	a := plannerAgent(t, `[]`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nodes := a.llmPlanGraph(ctx, "如何查询", map[string]tool.Tool{"search_web": {Name: "search_web"}}, "", false)
	if len(nodes) != 0 {
		t.Fatalf("canceled request produced %d fallback nodes", len(nodes))
	}
}
func TestPlannerRuleAndNodeMapping(t *testing.T) {
	cfg := &config.APIConfig{}
	a := &UnifiedAgent{cfg: cfg, llm: llm.New(cfg)}
	nodes := a.llmPlanGraph(context.Background(), "如何查询", map[string]tool.Tool{"search_web": {Name: "search_web"}}, "", false)
	if len(nodes) != 1 || nodes[0].ToolName != "search_web" {
		t.Fatal(nodes)
	}
	pn := planNode{ID: "n", Tool: "read", Params: map[string]string{"q": "value"}, DependsOn: []string{"prior"}}
	n := pn.toGraphNode()
	if fmt.Sprint(n.DependsOn) != "[prior]" || n.ID != "n" {
		t.Fatal(n)
	}
}
func TestRuntimeOwnsCurrentTaskAndNestedSnapshots(t *testing.T) {
	r := newTaskRuntime()
	task := &TaskState{UserID: "alice", TaskID: "a", Steps: []TaskStep{{Params: map[string]string{"q": "original"}}}, Graph: graph.NewTaskGraph([]*graph.Node{{ID: "n1", Params: map[string]string{"q": "original"}}})}
	r.setTask(task)
	task.UserID = "mutated"
	task.Graph.Nodes["n1"].Params["q"] = "changed"
	got := r.currentTask()
	if got.UserID != "alice" || got.Graph.Nodes["n1"].Params["q"] != "original" {
		t.Fatal("runtime stores caller-owned task")
	}
	got.Steps[0].Params["q"] = "reader"
	if r.currentTask().Steps[0].Params["q"] != "original" {
		t.Fatal("current task aliases")
	}
	r.appendSnapshot(Snapshot{State: *got})
	snaps := r.snapshotList()
	snaps[0].State.Graph.Nodes["n1"].Params["q"] = "reader"
	if r.snapshotList()[0].State.Graph.Nodes["n1"].Params["q"] != "original" {
		t.Fatal("snapshot aliases")
	}
	r.setTask(&TaskState{UserID: "bob", TaskID: "b"})
	if len(r.snapshotList()) != 1 {
		t.Fatal("new user task erased other snapshots")
	}
}
func TestRuntimeCancelRegistrationsAreIndependent(t *testing.T) {
	r := newTaskRuntime()
	a, ca := context.WithCancel(context.Background())
	b, cb := context.WithCancel(context.Background())
	doneA := r.registerCancel(ca)
	doneB := r.registerCancel(cb)
	doneA()
	if a.Err() == nil || b.Err() != nil {
		t.Fatal("cancel registrations shared")
	}
	r.cancelAll()
	if b.Err() == nil {
		t.Fatal("remaining registration not canceled")
	}
	doneB()
	doneA()
}

type preferenceCommitStore struct {
	memorytx.Store
	mu       sync.Mutex
	fail     bool
	next     int
	commands []memorytx.CreateCommand
}

func (s *preferenceCommitStore) Create(_ context.Context, c memorytx.CreateCommand) (consistency.CommittedChangeSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, c)
	if s.fail {
		return consistency.CommittedChangeSet{}, errors.New("commit rejected")
	}
	s.next++
	return consistency.CommittedChangeSet{Upserts: []consistency.MemoryRecord{{ID: int64(s.next), UserID: c.UserID, Version: 1, Content: c.Content, Category: c.Category}}}, nil
}

type preferenceWrites struct {
	mu     sync.Mutex
	values map[string]string
	notify chan struct{}
}

func (p *preferenceWrites) Save(user, key, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.values[user+":"+key] = value
	if p.notify != nil {
		select {
		case p.notify <- struct{}{}:
		default:
		}
	}
}
func (p *preferenceWrites) Load(string) map[string]string { return nil }
func prepareAgent(fail bool) (*UnifiedAgent, *preferenceCommitStore, *preferenceWrites) {
	cfg := &config.APIConfig{}
	cfg.ShortTermMaxTurns = 10
	s := &preferenceCommitStore{fail: fail}
	pref := &preferenceWrites{values: map[string]string{}}
	return &UnifiedAgent{cfg: cfg, llm: llm.New(cfg), rag: &rag.Engine{}, mem: newMemoryStack(cfg, nil, nil), runtime: newTaskRuntime(), tools: newToolRegistry(nil), repos: &repoBundle{memoryTx: s, pref: pref, chat: chathistory.NewPGRepo(nil)}}, s, pref
}
func TestPrepareNeverPublishesPreferenceBeforeFailedCommit(t *testing.T) {
	a, _, writes := prepareAgent(true)
	ctx := usercontext.With(context.Background(), "alice", "")
	pr := a.prepare(ctx, "我喜欢咖啡", ChatOptions{})
	if len(a.mem.prefSnapshot("alice")) != 0 || pr.extracted != "" {
		t.Fatal("preference visible despite failed memory commit")
	}
	writes.mu.Lock()
	defer writes.mu.Unlock()
	if len(writes.values) != 0 {
		t.Fatal("preference repository wrote before memory commit")
	}
}
func TestPrepareRejectsUnsafeRuleBeforeCacheAndEcho(t *testing.T) {
	a, _, _ := prepareAgent(false)
	pr := a.prepare(usercontext.With(context.Background(), "alice", ""), "我叫 000000000000000000", ChatOptions{})
	if pr.extracted != "" || len(a.mem.prefSnapshot("alice")) != 0 {
		t.Fatal("unsafe rule bypassed inspection")
	}
}
func TestPrepareCanceledRequestHasNoMemorySideEffects(t *testing.T) {
	a, _, _ := prepareAgent(false)
	ctx, cancel := context.WithCancel(usercontext.With(context.Background(), "alice", ""))
	cancel()
	pr := a.prepare(ctx, "我喜欢咖啡", ChatOptions{})
	if a.mem.stmCount("alice") != 0 || len(a.mem.prefSnapshot("alice")) != 0 || pr.extracted != "" {
		t.Fatal("canceled prepare changed memory")
	}
}
func TestPrepareAnonymousAndUserBuckets(t *testing.T) {
	a, _, _ := prepareAgent(true)
	a.prepare(context.Background(), "我喜欢咖啡", ChatOptions{})
	if a.mem.stmCount("") != 0 {
		t.Fatal("anonymous bucket")
	}
	a.mem.Pref("bob").Save("喜好", "tea")
	pr := a.prepare(usercontext.With(context.Background(), "alice", ""), "hello", ChatOptions{})
	if strings.Contains(pr.memPrefix, "tea") || len(a.mem.prefSnapshot("alice")) != 0 || a.mem.prefSnapshot("bob")["喜好"] != "tea" {
		t.Fatal("cross-user preference")
	}
}

func TestRuntimeRequestScopeAndStalePublisher(t *testing.T) {
	ctx := usercontext.With(context.Background(), "alice", "")
	task := newTaskState(ctx, "a", "query")
	scoped := withTaskContext(ctx, task)
	if taskIDFromContext(ctx) != "" || taskIDFromContext(scoped) != "a" || taskIDFromContext(usercontext.With(scoped, "bob", "")) != "" || taskIDFromContext(withTaskContext(ctx, &TaskState{TaskID: "legacy"})) != "" {
		t.Fatal("request task identity not isolated")
	}
	r := newTaskRuntime()
	r.setTask(task)
	r.setTask(&TaskState{UserID: "bob", TaskID: "b"})
	task.Status = "completed"
	r.publishTask(task)
	if r.currentTask().UserID != "bob" {
		t.Fatal("older completion replaced newer task")
	}
}
func TestPrepareAsyncModelPreservesOwnerAndCommitBoundary(t *testing.T) {
	a, store, writes := prepareAgent(false)
	writes.notify = make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]string{"content": `{"城市":"杭州"}`}}}})
	}))
	defer srv.Close()
	a.cfg.LLMAPIKey = "fictional"
	a.cfg.LLMAPIUrl = srv.URL
	a.prepare(usercontext.With(context.Background(), "alice", ""), "长期偏好说明", ChatOptions{})
	select {
	case <-writes.notify:
	case <-time.After(3 * time.Second):
		t.Fatal("background preference did not finish")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.commands) != 1 || store.commands[0].UserID != "alice" || a.mem.prefSnapshot("alice")["城市"] != "杭州" || len(a.mem.prefSnapshot("bob")) != 0 {
		t.Fatal("async preference owner/commit boundary")
	}
}
func TestPreferenceWriterRejectsAnonymousUnsafeAndBlank(t *testing.T) {
	a, store, _ := prepareAgent(false)
	ctx := usercontext.With(context.Background(), "alice", "")
	for _, c := range []struct {
		ctx  context.Context
		k, v string
	}{{context.Background(), "城市", "杭州"}, {ctx, " ", "value"}, {ctx, "姓名", "000000000000000000"}} {
		if a.storePreference(c.ctx, c.k, c.v) {
			t.Fatal("unsafe preference saved")
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.commands) != 0 {
		t.Fatal("invalid input reached commit")
	}
}

type graphSnapshotSink struct{}

func (graphSnapshotSink) Save(string, []byte) {}
func graphFixture(nodes []*graph.Node, tools map[string]tool.Tool) (*GraphRuntime, context.Context) {
	a, _, _ := prepareAgent(false)
	a.cfg.HarnessConfig = config.HarnessConfig{MaxRetries: 1}
	a.pctx = &promptCtx{taskMem: promptctx.NewTaskMemBuffer(30), toolTracker: promptctx.NewToolStateTracker(30)}
	a.repos.snap = graphSnapshotSink{}
	ctx := usercontext.With(context.Background(), "alice", "")
	task := newTaskState(ctx, "task-a", "query")
	a.runtime.setTask(task)
	return NewGraphRuntime(graph.NewTaskGraph(nodes), a, GraphConfig{MaxParallel: 4}, tools, task, nil), ctx
}
func TestGraphWritesTrustedObservationAndTraceScope(t *testing.T) {
	rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}}, map[string]tool.Tool{"read": {ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) { return "result", nil }}})
	got := rt.Execute(ctx)
	if got.Interrupted {
		t.Fatal(got)
	}
	obs := rt.agent.pctx.taskMem.Snapshot()
	traces := rt.agent.pctx.toolTracker.Snapshot()
	if len(obs) != 1 || len(traces) != 1 || obs[0].UserID != "alice" || obs[0].TaskID != "task-a" || traces[0].UserID != "alice" || traces[0].TaskID != "task-a" {
		t.Fatalf("missing trusted scope: %+v / %+v", obs, traces)
	}
}
func TestGraphRejectsForeignLegacyAndInvalidGraphsBeforeTools(t *testing.T) {
	for _, owner := range []string{"bob", ""} {
		t.Run(owner, func(t *testing.T) {
			calls := 0
			rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}}, map[string]tool.Tool{"read": {Execute: func(map[string]interface{}) (string, error) { calls++; return "foreign", nil }}})
			rt.task.UserID = owner
			got := rt.Execute(ctx)
			if !got.Interrupted || calls != 0 {
				t.Fatal("unowned task executed")
			}
		})
	}
	rt, ctx := graphFixture([]*graph.Node{{ID: "n1", DependsOn: []graph.NodeID{"missing"}}}, nil)
	if !rt.Execute(ctx).Interrupted {
		t.Fatal("invalid graph reported normal completion")
	}
}
func TestGraphRacePublishesOnlyWinningObservation(t *testing.T) {
	var ready sync.WaitGroup
	ready.Add(2)
	run := func(context.Context, map[string]interface{}) (string, error) {
		ready.Done()
		ready.Wait()
		return "simultaneous", nil
	}
	rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read", RaceGroup: "search"}, {ID: "n2", ToolName: "read", RaceGroup: "search"}}, map[string]tool.Tool{"read": {ExecuteCtx: run}})
	rt.cfg.EnableRacing = true
	got := rt.Execute(ctx)
	if len(got.Observations) != 1 {
		t.Fatalf("race published %d results", len(got.Observations))
	}
	if len(rt.agent.pctx.taskMem.Snapshot()) != 1 {
		t.Fatal("loser entered task memory")
	}
}
func TestGraphRetryDelayStopsOnCancellation(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}}, map[string]tool.Tool{"read": {ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) {
		once.Do(func() { close(started) })
		return "", errors.New("retry")
	}}})
	rt.agent.cfg.MaxRetries = 2
	rt.agent.cfg.RetryDelayMs = 2000
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan *GraphResult, 1)
	go func() { done <- rt.Execute(ctx) }()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		<-done
		t.Fatal("retry sleep ignored cancellation")
	}
}
func TestGraphStructuredFailureWithoutErrorIsFailure(t *testing.T) {
	rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}}, map[string]tool.Tool{"read": {ExecuteStructured: func(context.Context, map[string]interface{}) tool.ToolResult {
		return tool.ToolResult{Success: false, Payload: "not a success"}
	}}})
	got := rt.Execute(ctx)
	if got.NodeResults["n1"].Status != graph.StatusFailed || len(got.Observations) != 0 {
		t.Fatal("failed structured result became success")
	}
}
func TestGraphRestoresSchedulingFromStructuralDependencies(t *testing.T) {
	calls := 0
	rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}, {ID: "n2", ToolName: "read", DependsOn: []graph.NodeID{"n1"}}}, map[string]tool.Tool{"read": {ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) { calls++; return "next", nil }}})
	rt.graph.Nodes["n1"].Status = graph.StatusDone
	rt.graph.Nodes["n1"].Result = "previous"
	rt.graph.InDegree = map[graph.NodeID]int{"n1": 99, "n2": 99}
	got := rt.Execute(ctx)
	if got.Interrupted || calls != 1 || got.NodeResults["n2"].Status != graph.StatusDone {
		t.Fatalf("restored scheduling: %+v calls=%d", got, calls)
	}
}
func TestGraphSerializesCallbacksAndOwnsGraphEvent(t *testing.T) {
	rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}, {ID: "n2", ToolName: "read"}}, map[string]tool.Tool{"read": {ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) { return "ok", nil }}})
	var active, overlap atomic.Int32
	rt.onEvent = func(e StreamEvent) {
		if active.Add(1) > 1 {
			overlap.Store(1)
		}
		time.Sleep(time.Millisecond)
		if e.Type == "graph_ready" {
			e.Data.(map[string]interface{})["nodes"].(map[graph.NodeID]*graph.Node)["n1"].ToolName = "mutated"
		}
		active.Add(-1)
	}
	got := rt.Execute(ctx)
	if overlap.Load() != 0 {
		t.Fatal("concurrent callback access")
	}
	if got.NodeResults["n1"].Status != graph.StatusDone {
		t.Fatal("graph event aliases execution graph")
	}
}

func TestGraphConcurrentUsersKeepSeparateTaskScopes(t *testing.T) {
	tools := map[string]tool.Tool{"read": {ExecuteCtx: func(ctx context.Context, _ map[string]interface{}) (string, error) {
		return usercontext.UserIDFromContext(ctx), nil
	}}}
	a, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}}, tools)
	task := newTaskState(usercontext.With(context.Background(), "bob", ""), "task-b", "q")
	b := NewGraphRuntime(graph.NewTaskGraph([]*graph.Node{{ID: "n1", ToolName: "read"}}), a.agent, GraphConfig{MaxParallel: 1}, tools, task, nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.Execute(ctx) }()
	go func() { defer wg.Done(); b.Execute(usercontext.With(context.Background(), "bob", "")) }()
	wg.Wait()
	got := a.agent.pctx.taskMem.Snapshot()
	if len(got) != 2 {
		t.Fatalf("scoped observations %+v", got)
	}
	for _, o := range got {
		if (o.UserID == "alice" && o.TaskID != "task-a") || (o.UserID == "bob" && o.TaskID != "task-b") || o.Result != o.UserID {
			t.Fatalf("mixed ownership %+v", o)
		}
	}
}

func TestGraphReplanRetainsCompletedNodesAndRejectsInvalidBatch(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			output := `[{"id":"r1","tool":"read","depends_on":["n1"]}]`
			if invalid {
				output = `[{"id":"r1","tool":"read","depends_on":["missing"]}]`
			}
			planner := plannerAgent(t, output)
			calls := 0
			rt, ctx := graphFixture([]*graph.Node{{ID: "n1", ToolName: "read"}}, map[string]tool.Tool{"read": {ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) { calls++; return "new", nil }}})
			rt.agent.cfg = planner.cfg
			rt.agent.llm = planner.llm
			rt.graph.Nodes["n1"].Status = graph.StatusDone
			rt.graph.Nodes["n1"].Result = "prior"
			rt.graph.MarkDone("n1")
			added := rt.tryReplan(ctx, "layer_done", nil)
			if invalid {
				if len(added) != 0 || len(rt.graph.Nodes) != 1 {
					t.Fatal("invalid append modified graph")
				}
				return
			}
			if len(added) != 1 || rt.graph.Nodes["n1"].Result != "prior" {
				t.Fatal("completed state lost")
			}
			got := rt.Execute(ctx)
			if calls != 1 || got.NodeResults["r1"].Status != graph.StatusDone || len(got.Observations) != 2 {
				t.Fatalf("append execution %+v", got)
			}
		})
	}
}
func TestReActCreatesOwnedTaskAndCompletesSnapshots(t *testing.T) {
	rt, ctx := graphFixture(nil, nil)
	a := rt.agent
	tools := map[string]tool.Tool{"read": {Name: "read", ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) { return "evidence", nil }}}
	answer, _, task := a.runReAct(ctx, "question", tools, "", nil, nil, reactOpts{})
	if task == nil || task.UserID != "alice" || task.Status != "completed" || task.Phase != "done" || !strings.Contains(answer, "evidence") {
		t.Fatalf("wrong task lifecycle: %+v / %s", task, answer)
	}
	if len(task.Steps) != 1 || task.Steps[0].Status != StepDone || a.currentTask().Status != "completed" {
		t.Fatalf("stale task steps/publication %+v", task)
	}
	task.Graph.Nodes["n1"].ToolName = "changed"
	if a.currentTask().Graph.Nodes["n1"].ToolName == "changed" {
		t.Fatal("response task aliases current state")
	}
}
func TestSingleToolUsesContextInterfaceAndEmptyTaskScope(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("context-only tool panicked: %v", p)
		}
	}()
	rt, ctx := graphFixture(nil, nil)
	a := rt.agent
	calls := 0
	tools := map[string]tool.Tool{"read": {Name: "read", ExecuteCtx: func(c context.Context, p map[string]interface{}) (string, error) {
		if usercontext.UserIDFromContext(c) != "alice" {
			t.Error("tool lost user context")
		}
		calls++
		return "evidence", nil
	}}}
	a.runTool(ctx, "question", tools, "", nil, nil)
	traces := a.pctx.toolTracker.Snapshot()
	if calls != 1 || len(traces) != 1 || traces[0].UserID != "alice" || traces[0].TaskID != "" {
		t.Fatalf("single tool scope %+v", traces)
	}
}
func TestPreferenceSupplementHandlesNilAndPreservesExplicitValues(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("nil params panic: %v", p)
		}
	}()
	a, _, _ := prepareAgent(false)
	a.mem.Pref("alice").Save("城市", "杭州")
	tc := &tool.CallResult{}
	a.fillParamsFromPreference("alice", tc)
	if tc.Params["city"] != "杭州" {
		t.Fatal(tc.Params)
	}
	tc = &tool.CallResult{Params: map[string]interface{}{"city": "北京"}}
	a.fillParamsFromPreference("alice", tc)
	if tc.Params["city"] != "北京" {
		t.Fatal("explicit parameter overwritten")
	}
}
func TestReActInvalidGraphDoesNotEraseDependenciesAndRunTools(t *testing.T) {
	a := plannerAgent(t, `[{"id":"n1","tool":"read","depends_on":["n2"]},{"id":"n2","tool":"read","depends_on":["n1"]}]`)
	base, _, _ := prepareAgent(false)
	a.mem = base.mem
	a.repos = base.repos
	a.runtime = newTaskRuntime()
	a.repos.snap = graphSnapshotSink{}
	calls := 0
	answer, _, _ := a.runReAct(usercontext.With(context.Background(), "alice", ""), "question", map[string]tool.Tool{"read": {Name: "read", Execute: func(map[string]interface{}) (string, error) { calls++; return "unsafe", nil }}}, "", nil, nil, reactOpts{})
	if calls != 0 || !strings.Contains(answer, "规划失败") {
		t.Fatalf("invalid plan executed or hidden: %s / %d", answer, calls)
	}
}
func TestGraphStepConversionUsesStructuralOrderAndTerminalStatus(t *testing.T) {
	tg := graph.NewTaskGraph([]*graph.Node{{ID: "z", ToolName: "source"}, {ID: "a", ToolName: "consumer", DependsOn: []graph.NodeID{"z"}}})
	tg.SetNodeStatus("z", graph.StatusDone)
	tg.SetNodeResult("z", "evidence")
	tg.MarkDone("z")
	tg.SetNodeStatus("a", graph.StatusDone)
	tg.MarkDone("a")
	steps := graphToTaskSteps(tg)
	if len(steps) != 2 || steps[0].ToolName != "source" || steps[0].Status != StepDone || steps[0].Result != "evidence" {
		t.Fatalf("task conversion %+v", steps)
	}
	result := graphResultToReActSteps(tg, &GraphResult{})
	if len(result) < 2 || result[1].Tool != "source" {
		t.Fatal("ReAct actions reordered by ID instead of dependencies")
	}
}

func TestReActGeneratorCancellationKeepsInterruptedTask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		if req["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\ndata: [DONE]\n\n")
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]string{"content": `[{"id":"n1","tool":"read"}]`}}}})
	}))
	defer srv.Close()
	rt, ctx := graphFixture(nil, nil)
	a := rt.agent
	a.cfg.LLMAPIKey = "fictional"
	a.cfg.LLMAPIUrl = srv.URL
	a.llm = llm.New(a.cfg)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	answer, _, task := a.runReAct(ctx, "question", map[string]tool.Tool{"read": {Name: "read", ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) { return "evidence", nil }}}, "", nil, func(e StreamEvent) {
		if e.Type == "token" {
			cancel()
		}
	}, reactOpts{})
	if task == nil || task.Status != "interrupted" || a.currentTask().Status != "interrupted" || !strings.Contains(answer, "已中断") {
		t.Fatalf("canceled generation completed: %+v / %s", task, answer)
	}
}

type captureSource struct{ q chan promptctx.Query }

func (s captureSource) ID() string                         { return "capture" }
func (s captureSource) Supports(k promptctx.SlotKind) bool { return k == promptctx.SlotProfile }
func (s captureSource) Fetch(_ context.Context, _ promptctx.Slot, q promptctx.Query) ([]promptctx.ContextItem, error) {
	s.q <- q
	return []promptctx.ContextItem{{Text: "profile", Source: s.ID()}}, nil
}
func capturePrompt() (*promptCtx, chan promptctx.Query) {
	q := make(chan promptctx.Query, 1)
	r := promptctx.NewSourceRegistry()
	r.Register(captureSource{q})
	return &promptCtx{assembler: promptctx.NewAssembler(nil, r)}, q
}
func fixtureAgent() *UnifiedAgent {
	cfg := &config.APIConfig{}
	cfg.ShortTermMaxTurns = 20
	return &UnifiedAgent{llm: llm.New(cfg), mem: newMemoryStack(cfg, nil, nil), runtime: newTaskRuntime()}
}

func TestContextTaskOwnerBinding(t *testing.T) {
	cases := []struct {
		name, user string
		task       *TaskState
		want       string
	}{
		{"owned", "alice", &TaskState{UserID: "alice", TaskID: "a"}, "a"},
		{"foreign", "alice", &TaskState{UserID: "bob", TaskID: "b"}, ""},
		{"legacy", "alice", &TaskState{TaskID: "legacy"}, ""},
		{"anonymous", "", &TaskState{TaskID: "unowned"}, ""},
		{"anonymousOwned", "", &TaskState{UserID: "alice", TaskID: "a"}, ""},
		{"missing", "alice", nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := fixtureAgent()
			a.runtime.setTask(c.task)
			var qs chan promptctx.Query
			a.pctx, qs = capturePrompt()
			ctx := usercontext.With(context.Background(), c.user, "")
			ctx = withTaskContext(ctx, c.task)
			out := a.buildContextPrefix(ctx, "问题", "react")
			if !strings.Contains(out, "profile") {
				t.Fatal(out)
			}
			got := <-qs
			if got.TaskID != c.want || got.UserID != c.user || got.Text != "问题" || got.Mode != "react" {
				t.Fatalf("wrong scope/query: %+v", got)
			}
		})
	}
}
func TestNonTaskModesDoNotInheritCurrentTask(t *testing.T) {
	for _, mode := range []string{"chat", "rag", "tool"} {
		t.Run(mode, func(t *testing.T) {
			a := fixtureAgent()
			a.runtime.setTask(&TaskState{UserID: "alice", TaskID: "old"})
			var qs chan promptctx.Query
			a.pctx, qs = capturePrompt()
			a.buildContextPrefix(usercontext.With(context.Background(), "alice", ""), "问题", mode)
			if got := <-qs; got.TaskID != "" || got.Mode != mode {
				t.Fatalf("unrelated task scope %+v", got)
			}
		})
	}
}
func TestHistoryRetainsSameTextUserTurn(t *testing.T) {
	a := fixtureAgent()
	a.mem.STM("alice").Add("assistant", "same")
	got := a.buildHistoryMessages("alice", "same")
	if len(got) != 2 || got[1] != (llm.Message{Role: "user", Content: "same"}) {
		t.Fatalf("new user turn lost: %+v", got)
	}
}
func TestHistoryFiltersAndDeduplicatesCurrentUser(t *testing.T) {
	a := fixtureAgent()
	s := a.mem.STM("alice")
	s.Add("system", "hidden")
	s.Add("user", "prior")
	s.Add("assistant", "reply")
	s.Add("user", "current")
	a.mem.STM("bob").Add("assistant", "foreign")
	got := a.buildHistoryMessages("alice", "current")
	want := []llm.Message{{Role: "user", Content: "prior"}, {Role: "assistant", Content: "reply"}, {Role: "user", Content: "current"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v", got)
	}
	got[0].Content = "mutated"
	if s.Snapshot()[1].Content != "prior" {
		t.Fatal("history alias")
	}
	if got := a.buildHistoryMessages("", "current"); !reflect.DeepEqual(got, []llm.Message{{Role: "user", Content: "current"}}) {
		t.Fatal(got)
	}
}
func TestRAGUsesLatestSixConversationMessages(t *testing.T) {
	a := fixtureAgent()
	s := a.mem.STM("alice")
	for i := 0; i < 8; i++ {
		s.Add("user", fmt.Sprint(i))
		s.Add("system", "skip")
	}
	a.mem.STM("bob").Add("user", "foreign")
	got := a.recentHistoryForRAG("alice")
	if len(got) != 6 || got[0].Content != "2" || got[5].Content != "7" {
		t.Fatalf("wrong recent conversations: %+v", got)
	}
	if a.recentHistoryForRAG("") != nil {
		t.Fatal("anonymous history")
	}
	got[0].Content = "mutated"
	if a.recentHistoryForRAG("alice")[0].Content != "2" {
		t.Fatal("RAG alias")
	}
}
func TestSystemPromptPrefix(t *testing.T) {
	a := fixtureAgent()
	if a.buildSystemPrompt("", "base") != "base" || a.buildSystemPrompt("memory", "base") != "memory\n\nbase" {
		t.Fatal("prefix order")
	}
}
func TestContextEmbeddingAndCancellation(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		if req["input"] != "问题" {
			t.Errorf("input %+v", req)
		}
		fmt.Fprint(w, `{"data":[{"embedding":[0.25,0.5]}]}`)
	}))
	defer srv.Close()
	a := fixtureAgent()
	cfg := &config.APIConfig{}
	cfg.EmbeddingAPIKey = "fictional"
	cfg.EmbeddingAPIUrl = srv.URL
	cfg.EmbeddingModel = "test"
	a.llm = llm.New(cfg)
	var qs chan promptctx.Query
	a.pctx, qs = capturePrompt()
	ctx := usercontext.With(context.Background(), "alice", "")
	a.buildContextPrefix(ctx, "问题", "chat")
	if got := <-qs; !reflect.DeepEqual(got.Embedding, []float64{.25, .5}) {
		t.Fatalf("embedding %+v", got)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if got := a.buildContextPrefix(canceled, "问题", "chat"); got != "" {
		t.Fatal(got)
	}
	if calls.Load() != 1 {
		t.Fatal("canceled network request")
	}
	select {
	case <-qs:
		t.Fatal("canceled provider called")
	default:
	}
	a.pctx = nil
	if a.buildContextPrefix(ctx, "问题", "chat") != "" {
		t.Fatal("missing prompt")
	}
}
func TestPromptRejectsSpoofedQueryIdentity(t *testing.T) {
	p, qs := capturePrompt()
	ctx := usercontext.With(context.Background(), "alice", "")
	if got := p.assemble(ctx, promptctx.Query{UserID: "bob", Mode: "chat"}); got != "" {
		t.Fatalf("spoofed scope rendered %q", got)
	}
	select {
	case <-qs:
		t.Fatal("spoofed provider called")
	default:
	}
}
func TestPromptRetainsTaskAndToolScopes(t *testing.T) {
	p := &promptCtx{taskMem: promptctx.NewTaskMemBuffer(20), toolTracker: promptctx.NewToolStateTracker(20)}
	p.pushTaskMem(promptctx.StepObservation{UserID: "alice", TaskID: "a", Success: true, Result: "alice-a"})
	p.pushTaskMem(promptctx.StepObservation{UserID: "bob", TaskID: "b", Success: true, Result: "bob-b"})
	p.pushTaskMem(promptctx.StepObservation{UserID: "alice", TaskID: "other", Success: true, Result: "alice-other"})
	p.recordToolCall(promptctx.ToolCallTrace{UserID: "alice", TaskID: "a", ToolName: "run", Success: true, Summary: "alice-task"})
	p.recordToolCall(promptctx.ToolCallTrace{UserID: "alice", ToolName: "single", Success: true, Summary: "alice-single"})
	p.recordToolCall(promptctx.ToolCallTrace{UserID: "bob", TaskID: "a", ToolName: "foreign", Summary: "bob-a"})
	r := promptctx.NewSourceRegistry()
	r.Register(promptctx.NewTaskMemSource(p.taskMem))
	r.Register(promptctx.NewToolStateSource(nil, p.toolTracker))
	p.assembler = promptctx.NewAssembler(nil, r)
	ctx := usercontext.With(context.Background(), "alice", "")
	out := p.assemble(ctx, promptctx.Query{UserID: "alice", TaskID: "a", Mode: "react"})
	if !strings.Contains(out, "alice-a") || !strings.Contains(out, "alice-task") || strings.Contains(out, "bob-") || strings.Contains(out, "alice-other") || strings.Contains(out, "alice-single") {
		t.Fatalf("mixed scopes %q", out)
	}
	out = p.assemble(ctx, promptctx.Query{UserID: "alice", Mode: "tool"})
	if !strings.Contains(out, "alice-single") || strings.Contains(out, "alice-task") || strings.Contains(out, "bob-") {
		t.Fatalf("single tool scope %q", out)
	}
	if out = p.assemble(context.Background(), promptctx.Query{Mode: "react", TaskID: "a"}); out != "" {
		t.Fatalf("anonymous state %q", out)
	}
}
func TestPromptDropsUnownedObservationsAndTraces(t *testing.T) {
	p := &promptCtx{taskMem: promptctx.NewTaskMemBuffer(2), toolTracker: promptctx.NewToolStateTracker(2)}
	p.pushTaskMem(promptctx.StepObservation{TaskID: "a"})
	p.pushTaskMem(promptctx.StepObservation{UserID: "alice"})
	p.recordToolCall(promptctx.ToolCallTrace{TaskID: "a"})
	if len(p.taskMem.Snapshot()) != 0 || len(p.toolTracker.Snapshot()) != 0 {
		t.Fatal("unowned records entered shared buffers")
	}
}
func TestPromptNilGuardsAndCanceledAssembly(t *testing.T) {
	for _, p := range []*promptCtx{nil, {}} {
		if p.assemble(context.Background(), promptctx.Query{}) != "" {
			t.Fatal("unready assembler")
		}
		p.pushTaskMem(promptctx.StepObservation{})
		p.recordToolCall(promptctx.ToolCallTrace{})
	}
	p, qs := capturePrompt()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.assemble(ctx, promptctx.Query{Mode: "chat"}) != "" {
		t.Fatal("canceled render")
	}
	select {
	case <-qs:
		t.Fatal("canceled provider")
	default:
	}
}

func TestPromptResetOnlyAuthenticatedTask(t *testing.T) {
	p := &promptCtx{taskMem: promptctx.NewTaskMemBuffer(20)}
	p.pushTaskMem(promptctx.StepObservation{UserID: "alice", TaskID: "a", Result: "target"})
	p.pushTaskMem(promptctx.StepObservation{UserID: "alice", TaskID: "other", Result: "same-user-other-task"})
	p.pushTaskMem(promptctx.StepObservation{UserID: "bob", TaskID: "a", Result: "other-user-same-task-id"})
	reset, ok := interface{}(p).(interface{ resetTaskMem(context.Context, string) })
	if !ok {
		t.Fatal("reset has no authenticated task scope")
	}
	reset.resetTaskMem(context.Background(), "a")
	reset.resetTaskMem(usercontext.With(context.Background(), "alice", ""), "")
	if len(p.taskMem.Snapshot()) != 3 {
		t.Fatal("anonymous/empty-task reset changed shared buffer")
	}
	reset.resetTaskMem(usercontext.With(context.Background(), "alice", ""), "a")
	got := p.taskMem.Snapshot()
	if len(got) != 2 || got[0].Result != "same-user-other-task" || got[1].Result != "other-user-same-task-id" {
		t.Fatalf("reset damaged unrelated task: %+v", got)
	}
	reset.resetTaskMem(usercontext.With(context.Background(), "alice", ""), "a")
	if len(p.taskMem.Snapshot()) != 2 {
		t.Fatal("repeated reset")
	}
}

func TestPromptConcurrentTaskResetRetainsOtherUser(t *testing.T) {
	p := &promptCtx{taskMem: promptctx.NewTaskMemBuffer(400)}
	ctx := usercontext.With(context.Background(), "alice", "")
	var wg sync.WaitGroup
	for _, user := range []string{"alice", "bob"} {
		wg.Add(1)
		go func(user string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				p.pushTaskMem(promptctx.StepObservation{UserID: user, TaskID: "same", Result: user})
			}
		}(user)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			p.resetTaskMem(ctx, "same")
		}
	}()
	wg.Wait()
	p.resetTaskMem(ctx, "same")
	got := p.taskMem.Snapshot()
	if len(got) != 100 {
		t.Fatalf("other user lost records: %d", len(got))
	}
	for _, obs := range got {
		if obs.UserID != "bob" || obs.TaskID != "same" {
			t.Fatalf("wrong remaining scope %+v", obs)
		}
	}
}
func TestChatLLMForwardsPromptMessagesAndStreamEvents(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			requests := make(chan map[string]interface{}, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				requests <- req
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"你\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"好\"}}]}\n\ndata: [DONE]\n\n")
				} else {
					fmt.Fprint(w, `{"choices":[{"message":{"content":"你好"}}]}`)
				}
			}))
			defer srv.Close()
			cfg := &config.APIConfig{}
			cfg.LLMAPIKey = "fictional"
			cfg.LLMAPIUrl = srv.URL
			cfg.LLMModel = "primary"
			cfg.Temperature = .25
			a := fixtureAgent()
			a.llm = llm.New(cfg)
			msgs := []llm.Message{{Role: "user", Content: "history"}, {Role: "assistant", Content: "prior"}, {Role: "user", Content: "current"}}
			before := append([]llm.Message(nil), msgs...)
			var events []StreamEvent
			var callback func(StreamEvent)
			if stream {
				callback = func(e StreamEvent) { events = append(events, e) }
			}
			if got := a.chatLLM(context.Background(), "system", msgs, callback); got != "你好" {
				t.Fatal(got)
			}
			req := <-requests
			if req["model"] != "primary" || req["temperature"] != .25 {
				t.Fatalf("model parameters %+v", req)
			}
			if stream {
				if req["stream"] != true {
					t.Fatal("stream flag")
				}
				want := []StreamEvent{NewStreamEvent("token", map[string]string{"content": "你"}), NewStreamEvent("token", map[string]string{"content": "好"})}
				if !reflect.DeepEqual(events, want) {
					t.Fatalf("events %+v", events)
				}
			} else {
				if req["stream"] == true {
					t.Fatal("sync flag")
				}
			}
			messages := req["messages"].([]interface{})
			if len(messages) != 4 || messages[0].(map[string]interface{})["content"] != "system" || messages[3].(map[string]interface{})["content"] != "current" {
				t.Fatalf("request order %+v", messages)
			}
			if !reflect.DeepEqual(msgs, before) {
				t.Fatal("input messages mutated")
			}
		})
	}
}
func TestChatLLMForwardsInFlightCancellation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			entered := make(chan struct{})
			serverCanceled := make(chan struct{})
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				r.Body.Close()
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
					w.(http.Flusher).Flush()
				}
				close(entered)
				select {
				case <-r.Context().Done():
					close(serverCanceled)
				case <-release:
				}
			}))
			defer srv.Close()
			defer close(release)
			cfg := &config.APIConfig{}
			cfg.LLMAPIKey = "fictional"
			cfg.LLMAPIUrl = srv.URL
			cfg.LLMModel = "primary"
			a := fixtureAgent()
			a.llm = llm.New(cfg)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan string, 1)
			var callback func(StreamEvent)
			if stream {
				callback = func(StreamEvent) {}
			}
			go func() { done <- a.chatLLM(ctx, "system", []llm.Message{{Role: "user", Content: "query"}}, callback) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("request never reached local server")
			}
			cancel()
			select {
			case got := <-done:
				if !strings.Contains(got, "中断") {
					t.Fatalf("cancel result %q", got)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("model call ignored cancellation")
			}
			select {
			case <-serverCanceled:
			case <-time.After(3 * time.Second):
				t.Fatal("HTTP context did not cancel")
			}
		})
	}
}
func TestEmitNilAndPayload(t *testing.T) {
	emit(nil, "done", map[string]string{"content": "ok"})
	var got StreamEvent
	data := map[string]string{"content": "ok"}
	emit(func(e StreamEvent) { got = e }, "done", data)
	if !reflect.DeepEqual(got, NewStreamEvent("done", data)) {
		t.Fatal(got)
	}
}

func TestChatLLMPreCanceledMockHasNoEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := fixtureAgent()
	events := 0
	got := a.chatLLM(ctx, "system", []llm.Message{{Role: "user", Content: "hello"}}, func(StreamEvent) { events++ })
	if got != "[已中断]" || events != 0 {
		t.Fatalf("canceled call returned %q with %d events", got, events)
	}
}
