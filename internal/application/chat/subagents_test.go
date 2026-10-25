package chat

import (
	"agi-assistant/config"
	"agi-assistant/internal/domain/document"
	"agi-assistant/internal/domain/graph"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/domain/sandbox"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	"agi-assistant/internal/infrastructure/persistence/documentrepo"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	"agi-assistant/internal/usercontext"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ltmrepo "agi-assistant/internal/infrastructure/persistence/longterm"
	prefrepo "agi-assistant/internal/infrastructure/persistence/preference"
)

func TestDocumentTitlePrefersMarkdownH1(t *testing.T) {
	content := "## n2\n\n# PPO算法研究综述：对比TRPO的优势、实现特性及应用领域分析\n\n## 摘要\n..."

	got := documentTitle(content, "保存报告到本地文档库并写入 RAG", "写一份 PPO 报告")
	want := "PPO算法研究综述：对比TRPO的优势、实现特性及应用领域分析"
	if got != want {
		t.Fatalf("documentTitle() = %q, want %q", got, want)
	}
}

func TestDocumentContentPrefersWriterAgent(t *testing.T) {
	task := SubAgentTask{
		Query: "生成报告",
		Upstream: map[string]string{
			"n3:review_agent": "Review: 需要补证据",
			"n2:writer_agent": "# 真正的报告\n\n正文",
		},
	}

	got := documentContent(task)
	want := "# 真正的报告\n\n正文"
	if got != want {
		t.Fatalf("documentContent() = %q, want %q", got, want)
	}
}

func TestDocumentTitlePrefersExplicitRequestedTitle(t *testing.T) {
	content := "# 模型生成的其他标题\n\n正文"

	got := documentTitle(content, "保存报告到本地文档库并写入 RAG", "生成一份标题为《子Agent联调测试》的简短报告")
	want := "子Agent联调测试"
	if got != want {
		t.Fatalf("documentTitle() = %q, want %q", got, want)
	}
}

func TestDocumentContentStripsMarkdownFence(t *testing.T) {
	task := SubAgentTask{
		Query: "生成报告",
		Upstream: map[string]string{
			"n2:writer_agent": "```markdown\n# 子Agent联调测试\n\n正文\n```",
		},
	}

	got := documentContent(task)
	want := "# 子Agent联调测试\n\n正文"
	if got != want {
		t.Fatalf("documentContent() = %q, want %q", got, want)
	}
}

func TestResearchQueryPlansSubAgents(t *testing.T) {
	agent := &UnifiedAgent{}

	nodes := agent.llmPlanGraph(context.Background(), "调研 PPO 的优势，两句话即可", map[string]tool.Tool{}, "", true)
	wantAgents := []string{"research_agent", "writer_agent", "review_agent"}
	if len(nodes) != len(wantAgents) {
		t.Fatalf("len(nodes) = %d, want %d", len(nodes), len(wantAgents))
	}
	for i, want := range wantAgents {
		if nodes[i].Type != graph.NodeTypeSubAgent || nodes[i].AgentName != want {
			t.Fatalf("nodes[%d] = (%s, %s), want sub_agent %s", i, nodes[i].Type, nodes[i].AgentName, want)
		}
	}
}

func TestReportSaveQueryPlansDocAgent(t *testing.T) {
	agent := &UnifiedAgent{}

	nodes := agent.llmPlanGraph(context.Background(), "生成一份 PPO 报告并保存到本地文档库", map[string]tool.Tool{}, "", true)
	wantAgents := []string{"research_agent", "writer_agent", "review_agent", "doc_agent"}
	if len(nodes) != len(wantAgents) {
		t.Fatalf("len(nodes) = %d, want %d", len(nodes), len(wantAgents))
	}
	for i, want := range wantAgents {
		if nodes[i].Type != graph.NodeTypeSubAgent || nodes[i].AgentName != want {
			t.Fatalf("nodes[%d] = (%s, %s), want sub_agent %s", i, nodes[i].Type, nodes[i].AgentName, want)
		}
	}
}
func TestRegistryOwnsInputRegistrationAndReadParameterSlices(t *testing.T) {
	input := map[string]tool.Tool{"read": {Name: "read", Parameters: []tool.Param{{Name: "query"}}}}
	r := newToolRegistry(input)
	input["read"].Parameters[0].Name = "caller"
	if r.snapshot()["read"].Parameters[0].Name != "query" {
		t.Fatal("constructor aliases parameter slice")
	}
	delete(input, "read")
	if len(r.snapshot()) != 1 {
		t.Fatal("registry aliases initial map")
	}
	params := []tool.Param{{Name: "original"}}
	r.register(tool.Tool{Name: "write", Parameters: params})
	params[0].Name = "caller"
	if r.snapshot()["write"].Parameters[0].Name != "original" {
		t.Fatal("registration aliases caller parameters")
	}
	snapshot := r.snapshot()
	snapshot["write"].Parameters[0].Name = "snapshot"
	filtered := r.filter([]string{"write"})
	filtered["write"].Parameters[0].Name = "filter"
	if r.snapshot()["write"].Parameters[0].Name != "original" {
		t.Fatal("read aliases parameter metadata")
	}
}
func TestRegistryConcurrentRegistrationAndSnapshots(t *testing.T) {
	r := newToolRegistry(nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				r.register(tool.Tool{Name: "read", Parameters: []tool.Param{{Name: "query"}}})
				snapshot := r.snapshot()
				if tool, ok := snapshot["read"]; ok {
					tool.Parameters[0].Name = "local"
				}
				r.filter([]string{"read"})
			}
		}()
	}
	wg.Wait()
	if r.snapshot()["read"].Parameters[0].Name != "query" {
		t.Fatal("snapshot changes escaped")
	}
}
func TestRegisteredSearchCancelsTavilyWithoutFallback(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var llmCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		llmCalls.Add(1)
		json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]string{"content": "fallback"}}}})
	}))
	defer model.Close()
	cfg := &config.APIConfig{}
	cfg.SearchAPIKey = "fictional"
	cfg.SearchAPIURL = srv.URL
	cfg.LLMAPIKey = "fictional"
	cfg.LLMAPIUrl = model.URL
	search := builtinSearchTool(cfg, llm.New(cfg))
	if search.ExecuteCtx == nil {
		t.Fatal("registered search has no context execution interface")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := search.ExecuteCtx(ctx, map[string]interface{}{"query": "question"}); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled search succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("search ignored cancellation")
	}
	if llmCalls.Load() != 0 {
		t.Fatal("canceled search triggered fallback")
	}
}
func TestRegisteredSearchContextFallbackAndLegacyShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadGateway) }))
	defer srv.Close()
	cfg := &config.APIConfig{}
	cfg.SearchAPIKey = "fictional"
	cfg.SearchAPIURL = srv.URL
	t1 := builtinSearchTool(cfg, llm.New(cfg))
	if t1.ExecuteCtx == nil {
		t.Fatal("missing context path")
	}
	got, err := t1.ExecuteCtx(context.Background(), map[string]interface{}{"query": " q "})
	if err != nil || !strings.Contains(got, "未获取实时搜索结果") {
		t.Fatal(got, err)
	}
	if _, err = t1.Execute(map[string]interface{}{"query": " "}); err == nil {
		t.Fatal("legacy path accepted blank query")
	}
}
func TestSandboxInitializationDisabledAndMockAudit(t *testing.T) {
	a := &UnifiedAgent{cfg: &config.APIConfig{}}
	a.initSandbox()
	if a.sandbox != nil {
		t.Fatal("disabled sandbox initialized")
	}
	cfg := &config.APIConfig{}
	cfg.SandboxEnabled = true
	cfg.SandboxBackend = "mock"
	a = &UnifiedAgent{cfg: cfg, repos: &repoBundle{events: eventbus.NewKafkaPublisher(nil, false)}}
	a.initSandbox()
	if a.sandbox == nil || a.sandbox.Backend() != "mock" {
		t.Fatal("mock backend not initialized")
	}
	a = &UnifiedAgent{cfg: cfg}
	a.initSandbox()
	a.sandbox.Exec(context.Background(), sandbox.ExecRequest{Command: "echo synthetic"})
}

type documentChunkFixture struct {
	ragchunk.Repo
	rows []ragchunk.Row
	meta []ragchunk.Metadata
}

func (f *documentChunkFixture) PGAvailable() bool     { return true }
func (f *documentChunkFixture) ESAvailable() bool     { return false }
func (f *documentChunkFixture) MilvusAvailable() bool { return false }
func (f *documentChunkFixture) SavePGWithMetadata(_ string, _ int, content, parent string, _ []byte, metadata ragchunk.Metadata) (int64, error) {
	id := int64(len(f.rows) + 1)
	f.rows = append(f.rows, ragchunk.Row{ID: id, Content: content, ParentContent: parent, DocumentID: metadata.DocumentID, VersionID: metadata.VersionID, Section: metadata.Section})
	f.meta = append(f.meta, metadata)
	return id, nil
}
func (f *documentChunkFixture) LoadAll() ([]ragchunk.Row, error) { return f.rows, nil }
func documentAgent(t *testing.T) (*UnifiedAgent, *documentChunkFixture) {
	t.Helper()
	cfg := &config.APIConfig{}
	cfg.ChunkSize = 200
	cfg.ChunkOverlap = 20
	chunks := &documentChunkFixture{}
	return &UnifiedAgent{repos: &repoBundle{docs: documentrepo.NewStore(nil, t.TempDir())}, rag: rag.NewEngine(cfg, chunks, nil)}, chunks
}
func TestDocumentWriteAndIngestKeepAuthoritativeVersionMetadata(t *testing.T) {
	a, chunks := documentAgent(t)
	out, err := a.WriteDocument(document.WriteRequest{Title: "Report", DocType: "report", ContentMD: "# Report\n\nEvidence belongs to this version."}, true)
	if err != nil || out.Ingest == nil || out.Ingest.IndexedCount == 0 || len(chunks.meta) == 0 {
		t.Fatal(out, err)
	}
	for _, m := range chunks.meta {
		if m.DocumentID != out.Document.ID || m.VersionID != out.Version.ID || m.Section != "report" {
			t.Fatalf("wrong metadata %+v", m)
		}
	}
	docs, err := a.ListDocuments()
	if err != nil || len(docs) != 1 {
		t.Fatal(docs, err)
	}
	doc, version, err := a.GetDocument(out.Document.ID)
	if err != nil || doc.ID != out.Document.ID || version.ID != out.Version.ID {
		t.Fatal(doc, version, err)
	}
}
func TestDocumentIngestRejectsMismatchedDocumentVersion(t *testing.T) {
	a, chunks := documentAgent(t)
	first, _ := a.WriteDocument(document.WriteRequest{Title: "A", ContentMD: "alpha"}, false)
	other, _ := a.WriteDocument(document.WriteRequest{Title: "B", ContentMD: "beta"}, false)
	if _, err := a.IngestDocument(first.Document.ID, other.Version.ID); err == nil {
		t.Fatal("foreign document version relabeled")
	}
	if len(chunks.meta) != 0 {
		t.Fatal("invalid version reached index")
	}
	if _, err := a.IngestDocument("", other.Version.ID); err != nil {
		t.Fatal(err)
	}
	if chunks.meta[0].DocumentID != other.Document.ID {
		t.Fatal("version-only ingest lost document identity")
	}
}
func TestDocumentMissingRAGDoesNotPersistOrPanic(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("missing RAG panicked: %v", p)
		}
	}()
	a, _ := documentAgent(t)
	a.rag = nil
	if _, err := a.WriteDocument(document.WriteRequest{Title: "A", ContentMD: "alpha"}, true); err == nil {
		t.Fatal("missing RAG accepted")
	}
	docs, _ := a.ListDocuments()
	if len(docs) != 0 {
		t.Fatal("known missing dependency persisted partial document")
	}
}
func TestDocumentUnconfiguredAPIsReturnErrors(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("unconfigured document service panicked: %v", p)
		}
	}()
	a := &UnifiedAgent{}
	if _, err := a.ListDocuments(); err == nil {
		t.Fatal("list missing library")
	}
	if _, _, err := a.GetDocument("x"); err == nil {
		t.Fatal("get missing library")
	}
	if _, err := a.IngestDocument("x", ""); err == nil {
		t.Fatal("ingest missing library")
	}
}
func TestSubAgentsStopBeforeCanceledModelOrDocumentWork(t *testing.T) {
	a, _ := documentAgent(t)
	base, _, _ := prepareAgent(false)
	a.cfg = base.cfg
	a.llm = base.llm
	ctx, cancel := context.WithCancel(usercontext.With(context.Background(), "alice", ""))
	cancel()
	for _, agent := range []SubAgent{&writerAgent{agent: a}, &reviewAgent{agent: a}, &docAgent{agent: a}} {
		t.Run(agent.Name(), func(t *testing.T) {
			if _, err := agent.Run(ctx, SubAgentTask{Query: "question"}); err == nil {
				t.Fatal("canceled child task returned success")
			}
		})
	}
	docs, _ := a.ListDocuments()
	if len(docs) != 0 {
		t.Fatal("canceled document task wrote library")
	}
}
func TestResearchAgentUsesContextOnlySearchInterface(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("research context-only search panicked: %v", p)
		}
	}()
	a, _, _ := prepareAgent(false)
	calls := 0
	a.tools = newToolRegistry(map[string]tool.Tool{"search_web": {Name: "search_web", ExecuteCtx: func(ctx context.Context, _ map[string]interface{}) (string, error) {
		if usercontext.UserIDFromContext(ctx) != "alice" {
			t.Error("user context lost")
		}
		calls++
		return "owned evidence", nil
	}}})
	got, err := (&researchAgent{agent: a}).Run(usercontext.With(context.Background(), "alice", ""), SubAgentTask{Goal: "question"})
	if err != nil || calls != 1 || !strings.Contains(got, "owned evidence") {
		t.Fatal(got, err)
	}
}
func TestSubAgentDocumentUsesExactWriterRoleAndTrustedAuditScope(t *testing.T) {
	task := SubAgentTask{ID: "n4", Query: "report", Upstream: map[string]string{"n1:writer_agent_extra": "wrong draft", "n2:writer_agent": "# Real report\n\nEvidence."}}
	if got := documentContent(task); !strings.Contains(got, "Real report") || strings.Contains(got, "wrong draft") {
		t.Fatalf("wrong writer selected: %q", got)
	}
	a, _ := documentAgent(t)
	ctx := usercontext.With(context.Background(), "alice", "")
	ctx = withTaskContext(ctx, newTaskState(ctx, "parent-task", "report"))
	if _, err := (&docAgent{agent: a}).Run(ctx, task); err != nil {
		t.Fatal(err)
	}
	docs, _ := a.ListDocuments()
	_, version, err := a.GetDocument(docs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if version.Metadata["user_id"] != "alice" || version.Metadata["task_id"] != "parent-task" || version.Metadata["node_id"] != "n4" {
		t.Fatalf("untrusted/missing audit scope %+v", version.Metadata)
	}
}
func TestAnonymousDocumentAgentDoesNotWrite(t *testing.T) {
	a, _ := documentAgent(t)
	if _, err := (&docAgent{agent: a}).Run(context.Background(), SubAgentTask{Query: "report"}); err == nil {
		t.Fatal("anonymous agent write accepted")
	}
	docs, _ := a.ListDocuments()
	if len(docs) != 0 {
		t.Fatal("anonymous document persisted")
	}
}
func artifactAgent(t *testing.T) *UnifiedAgent {
	t.Helper()
	cfg := &config.APIConfig{}
	cfg.SandboxArtifactHostDir = t.TempDir()
	return &UnifiedAgent{cfg: cfg, llm: llm.New(cfg)}
}
func TestArtifactWorkspaceRejectsEmptyOrRemappedIdentity(t *testing.T) {
	a := artifactAgent(t)
	for _, c := range [][2]string{{"", "task"}, {"alice", ""}, {"../alice", "task"}, {"alice", "../task"}} {
		if got := a.prepareArtifactWorkspace(c[0], c[1]); got != "" {
			t.Fatalf("unsafe scope accepted %q", got)
		}
	}
	ws := a.prepareArtifactWorkspace("alice", "task")
	root, err := filepath.EvalSymlinks(a.cfg.SandboxArtifactHostDir)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, ws)
	if ws == "" || err != nil || rel != filepath.Join("alice", "task") {
		t.Fatalf("workspace=%q root=%q relative=%q err=%v", ws, root, rel, err)
	}
}
func TestArtifactFilenameIsLocalAcrossWindowsSpecialNames(t *testing.T) {
	for _, name := range []string{"../escape.md", "report.md:stream", "NUL", "CON.txt", "bad?.md", "trailing. ", "line\nbreak.md"} {
		got := sanitizeFilename(name)
		if got == "" || !filepath.IsLocal(got) || filepath.Base(got) != got || strings.ContainsAny(got, "<>:\"/\\|?*\n\r") || strings.HasSuffix(got, ".") || strings.HasSuffix(got, " ") {
			t.Fatalf("unsafe filename %q -> %q", name, got)
		}
	}
}
func TestArtifactCancellationDoesNotWriteLocalFallback(t *testing.T) {
	a := artifactAgent(t)
	ws := a.prepareArtifactWorkspace("alice", "task")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok, _ := a.materialize(ctx, ws, "report.md", "body"); ok {
		t.Fatal("canceled materialization succeeded")
	}
	entries, _ := os.ReadDir(ws)
	if len(entries) != 0 {
		t.Fatal("canceled materialization wrote files")
	}
}
func TestArtifactMaterializeStaysWithinRootAndDeliversExactContent(t *testing.T) {
	a := artifactAgent(t)
	outside := t.TempDir()
	if ok, _ := a.materialize(context.Background(), outside, "report.md", "body"); ok {
		t.Fatal("workspace outside configured root accepted")
	}
	ws := a.prepareArtifactWorkspace("alice", "task")
	if ok, backend := a.materialize(context.Background(), ws, "report.md", "# 报告\n\nEvidence"); !ok || backend != "" {
		t.Fatal(ok, backend)
	}
	data, err := os.ReadFile(filepath.Join(ws, "report.md"))
	if err != nil || string(data) != "# 报告\n\nEvidence" {
		t.Fatal(string(data), err)
	}
	entries, _ := os.ReadDir(ws)
	if len(entries) != 1 {
		t.Fatal("staging files leaked")
	}
}
func TestArtifactFailureDoesNotEmitDoneStatus(t *testing.T) {
	a := artifactAgent(t)
	var done map[string]string
	step := a.produceArtifact(context.Background(), "写一份报告", "# Report", filepath.Join(a.cfg.SandboxArtifactHostDir, "missing", "task"), func(e StreamEvent) {
		if e.Type == "node_done" {
			done = e.Data.(map[string]string)
		}
	})
	if step == nil || !strings.Contains(step.Content, "失败") || done["status"] != "failed" {
		t.Fatalf("failed output reported success: %+v / %+v", step, done)
	}
}

// Exercises real application wiring with disabled external backends, mock LLM
// and a temporary local document library. This is not an HTTP/service smoke test.
func TestConstructedAgentRunsFourStageDocumentPipeline(t *testing.T) {
	cfg := &config.APIConfig{}
	cfg.ShortTermMaxTurns = 10
	cfg.ChunkSize, cfg.ChunkOverlap = 200, 20
	cfg.GraphMaxParallel = 2
	a := New(cfg, Deps{
		ChatRepo: chathistory.NewPGRepo(nil), PrefRepo: prefrepo.NewPGRepo(nil),
		SnapRepo: snapshot.NewPGRepo(nil), LTMRepo: ltmrepo.NewPGRepo(nil),
		RAGChunkRepo: ragchunk.NewStore(nil, nil, nil),
		DocumentRepo: documentrepo.NewStore(nil, t.TempDir()),
		Events:       eventbus.NewKafkaPublisher(nil, false),
	})
	ctx := usercontext.With(context.Background(), "alice", "")
	query := "生成一份标题为《子Agent离线验证》的报告并保存到本地文档库"
	answer, _, task := a.runReAct(ctx, query, a.tools.snapshot(), "", nil, nil, reactOpts{allowSubAgents: true, forceSubAgentPlan: true})
	if task == nil || task.Status != "completed" || task.Phase != "done" || task.UserID != "alice" || answer == "" {
		t.Fatalf("pipeline did not complete: task=%+v answer=%q", task, answer)
	}
	if task.Graph == nil || len(task.Graph.Nodes) != 4 {
		t.Fatalf("wrong pipeline graph: %+v", task.Graph)
	}
	for _, n := range task.Graph.Nodes {
		if n.Status != graph.StatusDone || strings.TrimSpace(n.Result) == "" {
			t.Fatalf("unsuccessful stage: %+v", n)
		}
	}
	docs, err := a.ListDocuments()
	if err != nil || len(docs) != 1 {
		t.Fatalf("saved documents=%+v err=%v", docs, err)
	}
	doc, v, err := a.GetDocument(docs[0].ID)
	if err != nil || doc.Title != "子Agent离线验证" || strings.TrimSpace(v.ContentMD) == "" {
		t.Fatalf("bad saved document: doc=%+v version=%+v err=%v", doc, v, err)
	}
	if v.Metadata["user_id"] != "alice" || v.Metadata["task_id"] != task.TaskID || v.Metadata["node_id"] != "n4" {
		t.Fatalf("wrong persisted audit scope: %+v", v.Metadata)
	}
}
