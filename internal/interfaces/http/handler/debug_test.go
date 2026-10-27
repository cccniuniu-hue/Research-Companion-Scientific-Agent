package handler

import (
	"agi-assistant/config"
	"agi-assistant/internal/application/chat"
	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	"agi-assistant/internal/infrastructure/persistence/documentrepo"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/skillrepo"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/go-chi/chi/v5"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authdomain "agi-assistant/internal/domain/auth"

	skillapp "agi-assistant/internal/application/skill"
	ltmrepo "agi-assistant/internal/infrastructure/persistence/longterm"
	prefrepo "agi-assistant/internal/infrastructure/persistence/preference"

	skilldomain "agi-assistant/internal/domain/skill"
)

// TestMountPprof_RejectsWithoutToken 验证：即使 pprof 已挂载，
// 请求不带匹配 token 也拿不到内容——避免因端口误开放导致内部信息泄漏。
func TestMountPprof_RejectsWithoutToken(t *testing.T) {
	cfg := &config.APIConfig{}
	cfg.PprofEnabled = true
	cfg.PprofAdminToken = "secret-abc-123"
	s := &Server{cfg: cfg}

	r := chi.NewRouter()
	s.mountPprof(r)

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("no-token access: expected 404, got %d body=%q", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.Header.Set("X-Admin-Token", "wrong")
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("wrong-token access: expected 404, got %d", rr.Code)
	}
}

// TestMountPprof_AllowsWithMatchingToken 验证正确 token 能访问 pprof index。
// 只跑 index 页避免长耗时的 profile 采样进 CI。
func TestMountPprof_AllowsWithMatchingToken(t *testing.T) {
	cfg := &config.APIConfig{}
	cfg.PprofEnabled = true
	cfg.PprofAdminToken = "secret-abc-123"
	s := &Server{cfg: cfg}

	r := chi.NewRouter()
	s.mountPprof(r)

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.Header.Set("X-Admin-Token", "secret-abc-123")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("with-token access: expected 200, got %d body=%q", rr.Code, rr.Body.String())
	}

	if !strings.Contains(rr.Body.String(), "profiles available") {
		t.Errorf("expected pprof index page, got body=%q", rr.Body.String())
	}
}

// TestMountPprof_NamedProfile 验证 goroutine 这类具名 profile 能通过 admin token 访问。
func TestMountPprof_NamedProfile(t *testing.T) {
	cfg := &config.APIConfig{}
	cfg.PprofEnabled = true
	cfg.PprofAdminToken = "tok"
	s := &Server{cfg: cfg}

	r := chi.NewRouter()
	s.mountPprof(r)

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutine?debug=1", nil)
	req.Header.Set("X-Admin-Token", "tok")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("named profile with token: expected 200, got %d body=%q", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "goroutine profile") {
		t.Errorf("expected goroutine profile output, got body=%q", rr.Body.String())
	}
}
func day26Server(t *testing.T) (*Server, *chat.UnifiedAgent) {
	t.Helper()
	cfg := &config.APIConfig{}
	cfg.ShortTermMaxTurns = 10
	cfg.ChunkSize = 200
	cfg.ChunkOverlap = 20
	cfg.GraphMaxParallel = 2
	cfg.PprofAdminToken = "fictional-admin-for-http-tests"
	a := chat.New(cfg, chat.Deps{ChatRepo: chathistory.NewPGRepo(nil), PrefRepo: prefrepo.NewPGRepo(nil), SnapRepo: snapshot.NewPGRepo(nil), LTMRepo: ltmrepo.NewPGRepo(nil), RAGChunkRepo: ragchunk.NewStore(nil, nil, nil), DocumentRepo: documentrepo.NewStore(nil, t.TempDir()), Events: eventbus.NewKafkaPublisher(nil, false)})
	issuer, err := authdomain.NewTokenIssuer("fictional-signing-secret-for-http-tests", time.Hour, "http-tests")
	if err != nil {
		t.Fatal(err)
	}
	return New(a, nil, issuer, cfg, nil), a
}

func TestHTTPConcurrentKnowledgeUploadAndStatus(t *testing.T) {
	s, _ := day26Server(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, upload := range []bool{true, false} {
		wg.Add(1)
		go func(upload bool) {
			defer wg.Done()
			<-start
			for i := 0; i < 12; i++ {
				var w *httptest.ResponseRecorder
				if upload {
					w = day26Request(s, "POST", "/api/upload", `{"content":"synthetic shared knowledge fixture"}`, 1, true)
				} else {
					w = day26Request(s, "GET", "/api/status", "", 1, true)
				}
				if w.Code != 200 {
					t.Errorf("knowledge operation status=%d", w.Code)
				}
			}
		}(upload)
	}
	close(start)
	wg.Wait()
}
func day26Request(s *Server, method, path, body string, id int64, admin bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if id > 0 {
		token, _, _ := s.issuer.Sign(authdomain.User{ID: id, Username: "fixture"})
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if admin {
		req.Header.Set("X-Admin-Token", s.cfg.PprofAdminToken)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	return w
}
func TestHTTPProtectedRoutesRequireJWT(t *testing.T) {
	s, _ := day26Server(t)
	for _, route := range [][2]string{{"POST", "/api/chat"}, {"GET", "/api/memory/"}, {"GET", "/api/status"}, {"GET", "/api/snapshots"}, {"POST", "/api/upload"}, {"GET", "/api/documents/"}} {
		if w := day26Request(s, route[0], route[1], `{}`, 0, true); w.Code != 401 {
			t.Fatalf("%s status=%d", route[1], w.Code)
		}
	}
	if w := day26Request(s, "GET", "/api/auth/me", "", 1, false); w.Code != 200 || !strings.Contains(w.Body.String(), `"user_id":"1"`) {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestHTTPSharedManagementRequiresAdminInAdditionToJWT(t *testing.T) {
	s, _ := day26Server(t)
	for _, r := range []struct{ method, path, body string }{{"POST", "/api/chat/cancel", `{}`}, {"POST", "/api/upload", `{"content":"fixture"}`}, {"POST", "/api/docs/delete", `{"doc_hash":"fixture"}`}, {"GET", "/api/documents/", ""}, {"POST", "/api/documents/", `{"title":"x","content_md":"x"}`}, {"GET", "/api/documents/other", ""}, {"POST", "/api/documents/other/ingest", `{}`}, {"GET", "/api/memory/quarantined", ""}, {"GET", "/api/memory/superseded", ""}, {"POST", "/api/memory/quarantine", `{"id":1}`}, {"POST", "/api/memory/unquarantine", `{"id":1}`}, {"POST", "/api/tools/mcp", `{"name":"fixture","endpoint":"http://127.0.0.1:1"}`}, {"GET", "/api/snapshots", ""}, {"GET", "/api/status", ""}} {
		t.Run(r.path+r.method, func(t *testing.T) {
			if w := day26Request(s, r.method, r.path, r.body, 1, false); w.Code != 403 {
				t.Fatalf("unprivileged management status=%d", w.Code)
			}
		})
	}
}
func TestHTTPEmptyAdminConfigurationFailsClosed(t *testing.T) {
	s, _ := day26Server(t)
	s.cfg.PprofAdminToken = ""
	w := day26Request(s, "GET", "/api/status", "", 1, true)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
}
func TestHTTPMemoryContainsOnlyOwnActiveRecords(t *testing.T) {
	s, a := day26Server(t)
	for _, item := range []longterm.Item{{ID: 1, UserID: "1", Content: "own-visible"}, {ID: 2, UserID: "2", Content: "foreign-hidden"}, {ID: 3, UserID: "1", Content: "quarantined-hidden", Quarantined: true}, {ID: 4, UserID: "1", Content: "superseded-hidden", Superseded: true}, {ID: 5, Content: "legacy-hidden"}} {
		a.LongTerm().StoreItem(item)
	}
	w := day26Request(s, "GET", "/api/memory/", "", 1, false)
	body := w.Body.String()
	if w.Code != 200 || !strings.Contains(body, "own-visible") {
		t.Fatal(w.Code, body)
	}
	for _, hidden := range []string{"foreign-hidden", "quarantined-hidden", "superseded-hidden", "legacy-hidden"} {
		if strings.Contains(body, hidden) {
			t.Fatalf("memory leaked %s", hidden)
		}
	}
}
func TestHTTPSharedRAGRequiresManagementAuthorization(t *testing.T) {
	s, _ := day26Server(t)
	for _, path := range []string{"/api/chat", "/api/chat/stream"} {
		if w := day26Request(s, "POST", path, `{"message":"查询资料","use_rag":true}`, 1, false); w.Code != 403 {
			t.Fatal(path, w.Code)
		}
	}
}
func TestHTTPChatJSONRejectsAmbiguousEmptyAndOversizedBodies(t *testing.T) {
	s, _ := day26Server(t)
	for _, body := range []string{`{"message":"ok"}{"message":"other"}`, `{"message":"ok","user_id":"2"}`, `{"message":"  "}`, `broken`, `{"message":"` + strings.Repeat("x", (1<<20)+1) + `"}`} {
		w := day26Request(s, "POST", "/api/chat", body, 1, false)
		if w.Code != 400 && w.Code != 413 {
			t.Fatalf("invalid chat body accepted: %d", w.Code)
		}
	}
}
func TestHTTPAdminDocumentAuditsTrustedUserAndRejectsBrokenIngest(t *testing.T) {
	s, _ := day26Server(t)
	w := day26Request(s, "POST", "/api/documents/", `{"title":"fixture","content_md":"# Fixture\n\nEvidence","created_by":"forged","metadata":{"user_id":"forged"}}`, 1, true)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var result chat.DocumentWriteResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Document.CreatedBy != "1" || result.Version.Metadata["user_id"] != "1" {
		t.Fatalf("untrusted document attribution: %+v", result)
	}
	path := "/api/documents/" + result.Document.ID
	if got := day26Request(s, "GET", path, "", 1, true); got.Code != 200 {
		t.Fatal(got.Code)
	}
	if got := day26Request(s, "POST", path+"/ingest", "broken", 1, true); got.Code != 400 {
		t.Fatalf("malformed ingest accepted: %d", got.Code)
	}
}

type day26BrokenStream struct {
	header http.Header
	mode   string
}

func (w *day26BrokenStream) Header() http.Header { return w.header }
func (*day26BrokenStream) WriteHeader(int)       {}
func (w *day26BrokenStream) Write(p []byte) (int, error) {
	if w.mode == "write" {
		return 0, errors.New("synthetic disconnect")
	}
	return len(p), nil
}
func (w *day26BrokenStream) FlushError() error { return errors.New("synthetic flush failure") }
func TestHTTPStreamWriteOrFlushFailureStopsBeforeToolWork(t *testing.T) {
	for _, mode := range []string{"write", "flush"} {
		t.Run(mode, func(t *testing.T) {
			s, a := day26Server(t)
			var calls atomic.Int32
			a.RegisterTool(tool.Tool{Name: "fixture", Parameters: []tool.Param{{Name: "input", Required: true}}, ExecuteCtx: func(context.Context, map[string]interface{}) (string, error) {
				calls.Add(1)
				return "should-not-run", nil
			}})
			token, _, _ := s.issuer.Sign(authdomain.User{ID: 1})
			req := httptest.NewRequest("POST", "/api/chat/stream", bytes.NewBufferString(`{"message":"test"}`))
			req.Header.Set("Authorization", "Bearer "+token)
			s.Handler().ServeHTTP(&day26BrokenStream{header: make(http.Header), mode: mode}, req)
			if calls.Load() != 0 {
				t.Fatal("failed stream continued tool work")
			}
		})
	}
}
func TestHTTPNormalStreamAndHealth(t *testing.T) {
	s, _ := day26Server(t)
	w := day26Request(s, "POST", "/api/chat/stream", `{"message":"hello"}`, 1, false)
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(w.Body.String(), "event: start") || !strings.Contains(w.Body.String(), "event: done") {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		if w := day26Request(s, "GET", path, "", 0, false); w.Code != 200 {
			t.Fatal(path, w.Code)
		}
	}
}
func TestHTTPMissingServicesAreUnavailable(t *testing.T) {
	s, _ := day26Server(t)
	if w := day26Request(s, "POST", "/api/auth/login", `{"username":"alice","password":"fictional-password"}`, 0, false); w.Code != 503 {
		t.Fatal(w.Code)
	}
	s.agent = nil
	if w := day26Request(s, "GET", "/readyz", "", 0, false); w.Code != 503 {
		t.Fatal("nil agent ready", w.Code)
	}
}

func TestHTTPCanceledKnowledgeWriteDoesNotDispatch(t *testing.T) {
	s, a := day26Server(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("POST", "/api/upload", strings.NewReader(`{"content":"canceled fixture"}`)).WithContext(ctx)
	token, _, _ := s.issuer.Sign(authdomain.User{ID: 1})
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Admin-Token", s.cfg.PprofAdminToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusRequestTimeout || a.RAG().Loaded {
		t.Fatal("canceled upload dispatched", w.Code)
	}
}

type day26SkillRepo struct {
	mu      sync.Mutex
	items   map[string]skilldomain.Skill
	failure error
}

func (r *day26SkillRepo) ListByUser(user string) ([]skilldomain.Skill, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != nil {
		return nil, r.failure
	}
	var out []skilldomain.Skill
	for _, s := range r.items {
		if s.UserID == user {
			out = append(out, s)
		}
	}
	return out, nil
}
func (r *day26SkillRepo) ListEnabled(user string) ([]skilldomain.Skill, error) {
	all, err := r.ListByUser(user)
	if err != nil {
		return nil, err
	}
	var out []skilldomain.Skill
	for _, s := range all {
		if s.Enabled {
			out = append(out, s)
		}
	}
	return out, nil
}
func (r *day26SkillRepo) Install(user string, m skilldomain.Manifest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != nil {
		return r.failure
	}
	key := user + "\x00" + m.ID
	old := r.items[key]
	r.items[key] = skilldomain.Skill{Manifest: m, UserID: user, Enabled: old.Enabled}
	return nil
}
func (r *day26SkillRepo) SetEnabled(user, id string, enabled bool) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != nil {
		return false, r.failure
	}
	key := user + "\x00" + id
	s, ok := r.items[key]
	if ok {
		s.Enabled = enabled
		r.items[key] = s
	}
	return ok, nil
}
func (r *day26SkillRepo) Uninstall(user, id string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != nil {
		return false, r.failure
	}
	key := user + "\x00" + id
	_, ok := r.items[key]
	delete(r.items, key)
	return ok, nil
}
func day26SkillsServer(t *testing.T) (*Server, *day26SkillRepo) {
	s, _ := day26Server(t)
	repo := &day26SkillRepo{items: make(map[string]skilldomain.Skill)}
	s.skills = skillapp.NewService(repo, nil, llm.New(s.cfg), false)
	return s, repo
}
func TestHTTPSkillLifecycleUsesJWTUser(t *testing.T) {
	s, repo := day26SkillsServer(t)
	if w := day26Request(s, "GET", "/api/skills/marketplace", "", 1, false); w.Code != 200 || !strings.Contains(w.Body.String(), `"hub_status":"disabled"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, r := range [][2]string{{"install", `{"skill_id":"builtin:translate"}`}, {"toggle", `{"skill_id":"builtin:translate","enabled":true}`}} {
		if w := day26Request(s, "POST", "/api/skills/"+r[0], r[1], 1, false); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if own, _ := repo.ListEnabled("1"); len(own) != 1 {
		t.Fatal("installation owner lost")
	}
	if w := day26Request(s, "GET", "/api/skills/installed", "", 2, false); w.Code != 200 || strings.Contains(w.Body.String(), "builtin:translate") {
		t.Fatal("foreign installation exposed")
	}
	_ = day26Request(s, "POST", "/api/skills/toggle", `{"skill_id":"builtin:translate","enabled":false}`, 2, false)
	_ = day26Request(s, "POST", "/api/skills/uninstall", `{"skill_id":"builtin:translate"}`, 2, false)
	if own, _ := repo.ListEnabled("1"); len(own) != 1 {
		t.Fatal("foreign mutation changed owner state")
	}
	if w := day26Request(s, "POST", "/api/skills/uninstall", `{"skill_id":"builtin:translate"}`, 1, false); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if own, _ := repo.ListByUser("1"); len(own) != 0 {
		t.Fatal("uninstall did not persist")
	}
}
func TestHTTPSkillToggleRequiresExplicitSwitch(t *testing.T) {
	s, repo := day26SkillsServer(t)
	_ = repo.Install("1", skilldomain.Manifest{ID: "builtin:translate"})
	_, _ = repo.SetEnabled("1", "builtin:translate", true)
	if w := day26Request(s, "POST", "/api/skills/toggle", `{"skill_id":"builtin:translate"}`, 1, false); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if own, _ := repo.ListEnabled("1"); len(own) != 1 {
		t.Fatal("missing switch disabled tool")
	}
}
func TestHTTPSkillMutationsRejectUnknownAndTrailingJSON(t *testing.T) {
	s, _ := day26SkillsServer(t)
	for _, op := range []string{"install", "uninstall", "toggle"} {
		for _, body := range []string{`{"skill_id":"builtin:translate","enabled":true,"user_id":"2"}`, `{"skill_id":"builtin:translate","enabled":true} {}`} {
			if w := day26Request(s, "POST", "/api/skills/"+op, body, 1, false); w.Code != 400 {
				t.Fatal(op, w.Code)
			}
		}
	}
}
func TestHTTPSkillErrorsAreExplicitAndDoNotLeakBackendDetails(t *testing.T) {
	s, repo := day26SkillsServer(t)
	if w := day26Request(s, "POST", "/api/skills/install", `{"skill_id":"github:unknown/unknown"}`, 1, false); w.Code != 404 {
		t.Fatal(w.Code)
	}
	repo.failure = errors.New("synthetic-private-storage-detail")
	if w := day26Request(s, "GET", "/api/skills/installed", "", 1, false); w.Code != 500 || strings.Contains(w.Body.String(), "synthetic-private") {
		t.Fatal(w.Code, w.Body.String())
	}
	s.skills = skillapp.NewService(skillrepo.NewPGRepo(nil), nil, nil, false)
	if w := day26Request(s, "GET", "/api/skills/installed", "", 1, false); w.Code != 503 {
		t.Fatal(w.Code)
	}
	s.skills = nil
	if w := day26Request(s, "GET", "/api/skills/marketplace", "", 1, false); w.Code != 503 {
		t.Fatal(w.Code)
	}
}
func TestHTTPSkillCanceledInstallPreservesContextAndDoesNotWrite(t *testing.T) {
	s, repo := day26SkillsServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("POST", "/api/skills/install", strings.NewReader(`{"skill_id":"builtin:translate"}`)).WithContext(ctx)
	token, _, _ := s.issuer.Sign(authdomain.User{ID: 1})
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 408 || len(repo.items) != 0 {
		t.Fatal(w.Code, repo.items)
	}
}
func TestHTTPPprofDisabledAndEnabledMounting(t *testing.T) {
	s, a := day26Server(t)
	if w := day26Request(s, "GET", "/debug/pprof/", "", 0, true); w.Code != 404 {
		t.Fatal("disabled pprof exposed", w.Code)
	}
	cfg := *s.cfg
	cfg.PprofEnabled = true
	s = New(a, nil, s.issuer, &cfg, nil)
	if w := day26Request(s, "GET", "/debug/pprof/", "", 0, false); w.Code != 404 {
		t.Fatal(w.Code)
	}
	if w := day26Request(s, "GET", "/debug/pprof/", "", 0, true); w.Code != 200 {
		t.Fatal(w.Code)
	}
}
func TestHTTPPprofMissingAdminTokenFailsConstruction(t *testing.T) {
	s, a := day26Server(t)
	cfg := *s.cfg
	cfg.PprofEnabled = true
	cfg.PprofAdminToken = ""
	defer func() {
		if recover() == nil {
			t.Error("misconfigured pprof did not fail construction")
		}
	}()
	_ = New(a, nil, s.issuer, &cfg, nil)
}
func TestHTTPDuplicateAdminHeadersDoNotAuthorize(t *testing.T) {
	s, a := day26Server(t)
	cfg := *s.cfg
	cfg.PprofEnabled = true
	s = New(a, nil, s.issuer, &cfg, nil)
	token, _, _ := s.issuer.Sign(authdomain.User{ID: 1})
	for _, path := range []string{"/api/status", "/debug/pprof/"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Add("X-Admin-Token", s.cfg.PprofAdminToken)
		req.Header.Add("X-Admin-Token", "wrong")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		want := 403
		if path == "/debug/pprof/" {
			want = 404
		}
		if w.Code != want {
			t.Fatalf("duplicate admin token accepted at %s: %d", path, w.Code)
		}
	}
}
