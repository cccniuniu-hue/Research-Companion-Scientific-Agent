package skill

import (
	"agi-assistant/config"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	"agi-assistant/internal/infrastructure/persistence/documentrepo"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/skillrepo"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	"agi-assistant/internal/infrastructure/skillhub"
	"agi-assistant/internal/usercontext"
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appchat "agi-assistant/internal/application/chat"

	skilldomain "agi-assistant/internal/domain/skill"
	ltmrepo "agi-assistant/internal/infrastructure/persistence/longterm"
	prefrepo "agi-assistant/internal/infrastructure/persistence/preference"
)

type memoryRepo struct {
	items []skilldomain.Skill
}

func (r *memoryRepo) ListByUser(userID string) ([]skilldomain.Skill, error) {
	var out []skilldomain.Skill
	for _, sk := range r.items {
		if sk.UserID == userID {
			out = append(out, sk)
		}
	}
	return out, nil
}

func (r *memoryRepo) ListEnabled(userID string) ([]skilldomain.Skill, error) {
	var out []skilldomain.Skill
	for _, sk := range r.items {
		if sk.UserID == userID && sk.Enabled {
			out = append(out, sk)
		}
	}
	return out, nil
}

func (r *memoryRepo) Install(userID string, m skilldomain.Manifest) error {
	r.items = append(r.items, skilldomain.Skill{Manifest: m, UserID: userID, Enabled: false})
	return nil
}

func (r *memoryRepo) SetEnabled(userID, skillID string, enabled bool) (bool, error) {
	for i := range r.items {
		if r.items[i].UserID == userID && r.items[i].ID == skillID {
			r.items[i].Enabled = enabled
			return true, nil
		}
	}
	return false, nil
}

func (r *memoryRepo) Uninstall(userID, skillID string) (bool, error) {
	for i := range r.items {
		if r.items[i].UserID == userID && r.items[i].ID == skillID {
			r.items = append(r.items[:i], r.items[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}

func TestEnabledToolsExecutesGitHubPromptSkill(t *testing.T) {
	repo := &memoryRepo{items: []skilldomain.Skill{{
		Manifest: skilldomain.Manifest{
			ID:             "github:clhikari/astrbot_plugin_office_assistant",
			Name:           "clhikari/astrbot_plugin_office_assistant",
			Description:    "Office assistant test skill",
			Category:       "office",
			Source:         skilldomain.SourceGitHub,
			Invocation:     skilldomain.InvokePrompt,
			PromptTemplate: "GitHub skill background. Input: {{input}}",
			Parameters:     []tool.Param{{Name: "input", Type: "string", Description: "task", Required: true}},
		},
		UserID:  "u1",
		Enabled: true,
	}}}
	svc := NewService(repo, nil, llm.New(&config.APIConfig{}), false)

	tools := svc.EnabledTools("u1")
	ghTool, ok := tools["skill_clhikari_astrbot_plugin_office_assistant"]
	if !ok {
		t.Fatalf("expected github skill tool, got keys %#v", tools)
	}

	out, err := ghTool.ExecuteCtx(usercontext.With(context.Background(), "u1", ""), map[string]interface{}{"input": "把报告润色成正式文档"})
	if err != nil {
		t.Fatalf("ExecuteCtx returned error: %v", err)
	}
	if !strings.Contains(out, "把报告润色成正式文档") {
		t.Fatalf("expected mock output to include input, got %q", out)
	}
}

func TestPromptSkillCancellationReturnsError(t *testing.T) {
	repo := &memoryRepo{items: []skilldomain.Skill{{
		Manifest: skilldomain.Manifest{
			ID:             "builtin:doc_polish",
			Name:           "doc polish",
			Description:    "polish",
			Source:         skilldomain.SourceBuiltin,
			Invocation:     skilldomain.InvokePrompt,
			PromptTemplate: "{{input}}",
			Parameters:     []tool.Param{{Name: "input", Type: "string", Description: "task", Required: true}},
		},
		UserID:  "u1",
		Enabled: true,
	}}}
	svc := NewService(repo, nil, llm.New(&config.APIConfig{
		LLMConfig: config.LLMConfig{
			LLMAPIKey: "fictional-key-for-cancel-path",
			LLMAPIUrl: "http://127.0.0.1:1/v1/chat/completions",
			LLMModel:  "test-model",
		},
	}), false)
	tools := svc.EnabledTools("u1")
	polishTool := tools["skill_doc_polish"]

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := polishTool.ExecuteCtx(ctx, map[string]interface{}{"input": "abc"})
	if err == nil {
		t.Fatalf("expected cancellation error, got nil with output %q", out)
	}
}
func TestSkillToolNamesAreStableAndNamespaced(t *testing.T) {
	for _, c := range [][2]string{{"builtin:meeting_minutes", "skill_meeting_minutes"}, {"github:Owner/Some-Repo", "skill_owner_some_repo"}, {"mcp:研发 / 工具", "skill_unnamed"}, {"", "skill_unnamed"}} {
		m := skilldomain.Manifest{ID: c[0]}
		if got := m.ToolName(); got != c[1] || !regexp.MustCompile(`^skill_[a-z0-9_]+$`).MatchString(got) || got != m.ToolName() {
			t.Fatalf("id=%q name=%q", c[0], got)
		}
	}
}
func TestInstalledSkillJSONPreservesManifestOwnerAndSwitch(t *testing.T) {
	original := skilldomain.Skill{Manifest: skilldomain.Manifest{ID: "builtin:translate", Name: "中英互译", Source: skilldomain.SourceBuiltin, Invocation: skilldomain.InvokePrompt, PromptTemplate: "翻译 {{input}}", Parameters: []tool.Param{{Name: "input", Type: "string", Required: true}}}, UserID: "alice", Enabled: true}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var got skilldomain.Skill
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != original.ID || got.UserID != "alice" || !got.Enabled || got.Invocation != skilldomain.InvokePrompt || len(got.Parameters) != 1 || !got.Parameters[0].Required {
		t.Fatalf("round trip lost protocol: %+v", got)
	}
}

type day25RoundTripper func(*http.Request) (*http.Response, error)

func (f day25RoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func day25HubTransport(t *testing.T, f day25RoundTripper) {
	t.Helper()
	old := http.DefaultTransport
	http.DefaultTransport = f
	t.Cleanup(func() { http.DefaultTransport = old })
}
func day25HubResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

const day25HubJSON = `{"total_count":1,"items":[{"full_name":"owner/report","description":"办公资料","html_url":"https://github.com/owner/report","stargazers_count":12}]}`

func TestSkillHubSearchContractAndBuiltinCopies(t *testing.T) {
	day25HubTransport(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != "GET" || r.URL.Scheme != "https" || r.URL.Host != "api.github.com" || r.URL.Path != "/search/repositories" || r.URL.Query().Get("q") != "office testing" || r.URL.Query().Get("per_page") != "20" || r.Header.Get("Authorization") != "Bearer fictional-token" || r.Header.Get("User-Agent") == "" {
			t.Errorf("unexpected request contract: %s", r.URL)
		}
		return day25HubResponse(200, day25HubJSON), nil
	})
	items, err := skillhub.NewClient("fictional-token", "office testing", time.Hour).SearchOfficeSkills(context.Background())
	if err != nil || len(items) != 1 || items[0].ID != "github:owner/report" || items[0].Stars != 12 || !strings.Contains(items[0].PromptTemplate, "{{input}}") {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	first := skillhub.BuiltinOfficeSkills()
	if len(first) != 7 {
		t.Fatal(len(first))
	}
	first[0].Parameters[0].Name = "changed"
	if skillhub.BuiltinOfficeSkills()[0].Parameters[0].Name != "input" {
		t.Fatal("builtin metadata shared across calls")
	}
}
func TestSkillHubCacheOwnsReturnedMetadata(t *testing.T) {
	calls := 0
	day25HubTransport(t, func(*http.Request) (*http.Response, error) { calls++; return day25HubResponse(200, day25HubJSON), nil })
	c := skillhub.NewClient("", "", time.Hour)
	first, err := c.SearchOfficeSkills(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first[0].ID = "changed"
	first[0].Parameters[0].Name = "changed"
	second, err := c.SearchOfficeSkills(context.Background())
	if err != nil || second[0].ID != "github:owner/report" || second[0].Parameters[0].Name != "input" {
		t.Fatalf("cache aliases response: %+v err=%v", second, err)
	}
	second[0].Parameters[0].Name = "changed-again"
	third, _ := c.SearchOfficeSkills(context.Background())
	if calls != 1 || third[0].Parameters[0].Name != "input" {
		t.Fatal("cached response exposes nested metadata")
	}
}
func TestSkillHubCachesSuccessfulEmptyResults(t *testing.T) {
	calls := 0
	day25HubTransport(t, func(*http.Request) (*http.Response, error) {
		calls++
		return day25HubResponse(200, `{"total_count":0,"items":[]}`), nil
	})
	c := skillhub.NewClient("", "", time.Hour)
	for i := 0; i < 2; i++ {
		items, err := c.SearchOfficeSkills(context.Background())
		if err != nil || len(items) != 0 {
			t.Fatal(items, err)
		}
	}
	if calls != 1 {
		t.Fatalf("empty cache missed: %d calls", calls)
	}
}
func TestSkillHubCanceledCacheLookupReturnsCancellation(t *testing.T) {
	day25HubTransport(t, func(*http.Request) (*http.Response, error) { return day25HubResponse(200, day25HubJSON), nil })
	c := skillhub.NewClient("", "", time.Hour)
	_, _ = c.SearchOfficeSkills(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.SearchOfficeSkills(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cached lookup: %v", err)
	}
}
func TestSkillHubDoesNotFollowSearchRedirects(t *testing.T) {
	calls := 0
	day25HubTransport(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls > 1 {
			return day25HubResponse(200, day25HubJSON), nil
		}
		r := day25HubResponse(302, "")
		r.Header.Set("Location", "http://127.0.0.1/redirected")
		return r, nil
	})
	if _, err := skillhub.NewClient("fictional-token", "", time.Hour).SearchOfficeSkills(context.Background()); err == nil || calls != 1 {
		t.Fatalf("redirect followed: calls=%d err=%v", calls, err)
	}
}
func TestSkillHubErrorDoesNotEchoRemoteBody(t *testing.T) {
	day25HubTransport(t, func(*http.Request) (*http.Response, error) {
		return day25HubResponse(403, "fictional-sensitive-response"), nil
	})
	_, err := skillhub.NewClient("", "", time.Hour).SearchOfficeSkills(context.Background())
	if err == nil || !strings.Contains(err.Error(), "403") || strings.Contains(err.Error(), "fictional-sensitive-response") {
		t.Fatalf("unsafe status error: %v", err)
	}
}
func TestSkillHubRejectsMalformedAndOversizedResponses(t *testing.T) {
	for _, c := range []struct{ name, body string }{{"malformed", "not-json"}, {"oversized", strings.Repeat(" ", 1<<20) + "{}"}} {
		t.Run(c.name, func(t *testing.T) {
			day25HubTransport(t, func(*http.Request) (*http.Response, error) { return day25HubResponse(200, c.body), nil })
			if _, err := skillhub.NewClient("", "", time.Hour).SearchOfficeSkills(context.Background()); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}
func TestSkillRepoUnavailableIsExplicit(t *testing.T) {
	r := skillrepo.NewPGRepo(nil)
	if _, err := r.ListByUser("alice"); !errors.Is(err, skillrepo.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := r.ListEnabled("alice"); !errors.Is(err, skillrepo.ErrUnavailable) {
		t.Fatal(err)
	}
	if err := r.Install("alice", skilldomain.Manifest{ID: "builtin:x"}); !errors.Is(err, skillrepo.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := r.SetEnabled("alice", "builtin:x", true); !errors.Is(err, skillrepo.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := r.Uninstall("alice", "builtin:x"); !errors.Is(err, skillrepo.ErrUnavailable) {
		t.Fatal(err)
	}
}
func TestSkillRepoRejectsBlankUserBeforeBackendAccess(t *testing.T) {
	r := skillrepo.NewPGRepo(nil)
	for _, user := range []string{"", " \t"} {
		_, a := r.ListByUser(user)
		_, b := r.ListEnabled(user)
		c := r.Install(user, skilldomain.Manifest{})
		_, d := r.SetEnabled(user, "x", true)
		_, e := r.Uninstall(user, "x")
		for _, err := range []error{a, b, c, d, e} {
			if err == nil || !strings.Contains(err.Error(), "user ID") {
				t.Fatalf("blank user not rejected first: %v", err)
			}
		}
	}
}
func TestSkillInstallSQLPreservesEnabledAndScopesOwner(t *testing.T) {
	matcher := sqlmock.QueryMatcherFunc(func(_, actual string) error {
		compact := strings.Join(strings.Fields(actual), " ")
		if !strings.Contains(compact, "ON CONFLICT (user_id, skill_id) DO UPDATE SET") || !strings.Contains(compact, "$11,FALSE)") || strings.Contains(strings.Split(compact, "DO UPDATE SET")[1], "enabled=") {
			return errors.New("install must preserve existing switch and default new install off")
		}
		return nil
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := skilldomain.Manifest{ID: "builtin:x", Name: "X", Description: "desc", Category: "office", Source: skilldomain.SourceBuiltin, Invocation: skilldomain.InvokePrompt, PromptTemplate: "{{input}}", Parameters: []tool.Param{{Name: "input", Type: "string", Required: true}}}
	mock.ExpectExec("install").WithArgs("alice", m.ID, m.Name, m.Description, m.Category, string(m.Source), "", string(m.Invocation), "", m.PromptTemplate, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := skillrepo.NewPGRepo(db).Install("alice", m); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestSkillRepoPropagatesRowsAffectedErrors(t *testing.T) {
	for _, op := range []string{"toggle", "uninstall"} {
		t.Run(op, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			r := skillrepo.NewPGRepo(db)
			want := errors.New("synthetic affected-row error")
			if op == "toggle" {
				mock.ExpectExec("UPDATE installed_skills").WithArgs("alice", "id", true).WillReturnResult(sqlmock.NewErrorResult(want))
				_, err := r.SetEnabled("alice", "id", true)
				if !errors.Is(err, want) {
					t.Fatal(err)
				}
			} else {
				mock.ExpectExec("DELETE FROM installed_skills").WithArgs("alice", "id").WillReturnResult(sqlmock.NewErrorResult(want))
				_, err := r.Uninstall("alice", "id")
				if !errors.Is(err, want) {
					t.Fatal(err)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func day25SkillRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"skill_id", "name", "description", "category", "source", "source_url", "invocation", "endpoint", "prompt_template", "params", "enabled"})
}
func TestSkillRepoEnabledQueryAndNullableMetadata(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(`WHERE user_id=\$1 AND enabled=TRUE`).WithArgs("alice").WillReturnRows(day25SkillRows().AddRow("builtin:x", "X", "desc", "office", "builtin", nil, "prompt", nil, nil, nil, true))
	items, err := skillrepo.NewPGRepo(db).ListEnabled("alice")
	if err != nil || len(items) != 1 || items[0].UserID != "alice" || !items[0].Enabled || items[0].Endpoint != "" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestSkillRepoRejectsMalformedParameterMetadata(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("FROM installed_skills WHERE user_id").WithArgs("alice").WillReturnRows(day25SkillRows().AddRow("builtin:x", "X", "desc", "office", "builtin", nil, "prompt", nil, "{{input}}", []byte("not-json"), true))
	items, err := skillrepo.NewPGRepo(db).ListByUser("alice")
	if err == nil || items != nil {
		t.Fatalf("malformed parameters accepted: %+v err=%v", items, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestSkillRepoRowFailureDoesNotExposePartialList(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	want := errors.New("synthetic row failure")
	rows := day25SkillRows().AddRow("a", "X", "desc", "office", "builtin", nil, "prompt", nil, nil, nil, true).AddRow("b", "X", "desc", "office", "builtin", nil, "prompt", nil, nil, nil, true).RowError(1, want)
	mock.ExpectQuery("FROM installed_skills WHERE user_id").WithArgs("alice").WillReturnRows(rows)
	items, err := skillrepo.NewPGRepo(db).ListByUser("alice")
	if !errors.Is(err, want) || items != nil {
		t.Fatalf("partial list leaked: %+v err=%v", items, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func day25PromptSkill(id string) skilldomain.Skill {
	return skilldomain.Skill{Manifest: skilldomain.Manifest{ID: id, Name: id, Source: skilldomain.SourceBuiltin, Invocation: skilldomain.InvokePrompt, PromptTemplate: "Task: {{input}}", Parameters: []tool.Param{{Name: "input", Type: "string", Required: true}}}, UserID: "alice", Enabled: true}
}
func day25SkillContext(user string) context.Context {
	return usercontext.With(context.Background(), user, "")
}

type day25LeakySkillRepo struct{ memoryRepo }

func (r *day25LeakySkillRepo) ListEnabled(string) ([]skilldomain.Skill, error) { return r.items, nil }

func TestSkillLifecycleDefaultsOffAndIsolatesUsers(t *testing.T) {
	repo := &memoryRepo{}
	s := NewService(repo, nil, llm.New(&config.APIConfig{}), false)
	featured, github, status := s.Marketplace(context.Background())
	if len(featured) != 7 || len(github) != 0 || status != "disabled" {
		t.Fatal("bad disabled marketplace")
	}
	if err := s.Install(context.Background(), "alice", "builtin:doc_polish"); err != nil {
		t.Fatal(err)
	}
	if len(s.EnabledTools("alice")) != 0 {
		t.Fatal("new install was enabled")
	}
	if ok, err := s.Toggle("bob", "builtin:doc_polish", true); ok || err != nil {
		t.Fatal("foreign toggle changed owner")
	}
	if ok, err := s.Toggle("alice", "builtin:doc_polish", true); !ok || err != nil {
		t.Fatal(ok, err)
	}
	tools := s.EnabledTools("alice")
	if len(tools) != 1 || len(s.EnabledTools("bob")) != 0 {
		t.Fatal("enabled scope incorrect")
	}
	if out, err := tools["skill_doc_polish"].ExecuteCtx(day25SkillContext("alice"), map[string]interface{}{"input": "润色测试"}); err != nil || !strings.Contains(out, "润色测试") {
		t.Fatal(out, err)
	}
	if err := s.Install(context.Background(), "alice", "github:unknown/unknown"); !errors.Is(err, ErrManifestNotFound) {
		t.Fatal(err)
	}
	if ok, err := s.Uninstall("alice", "builtin:doc_polish"); !ok || err != nil || len(s.EnabledTools("alice")) != 0 {
		t.Fatal("uninstall did not remove tool")
	}
}
func TestSkillCanceledInstallDoesNotPersist(t *testing.T) {
	repo := &memoryRepo{}
	s := NewService(repo, nil, nil, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Install(ctx, "alice", "builtin:translate"); !errors.Is(err, context.Canceled) || len(repo.items) != 0 {
		t.Fatalf("canceled install mutated repo: %v %+v", err, repo.items)
	}
}
func TestSkillToolsRejectForeignDisabledUnknownAndAmbiguousEntries(t *testing.T) {
	a := day25PromptSkill("github:owner/a-b")
	b := day25PromptSkill("github:owner/a_b")
	foreign := day25PromptSkill("builtin:foreign")
	foreign.UserID = "bob"
	disabled := day25PromptSkill("builtin:off")
	disabled.Enabled = false
	unknown := day25PromptSkill("builtin:unknown")
	unknown.Invocation = "shell"
	valid := day25PromptSkill("builtin:valid")
	repo := &day25LeakySkillRepo{memoryRepo{items: []skilldomain.Skill{a, b, foreign, disabled, unknown, valid}}}
	tools := NewService(repo, nil, llm.New(&config.APIConfig{}), false).EnabledTools("alice")
	if len(tools) != 1 || tools[valid.ToolName()].ExecuteCtx == nil {
		t.Fatalf("unsafe entries assembled: %+v", tools)
	}
}
func TestSkillToolMetadataDoesNotAliasRepository(t *testing.T) {
	repo := &memoryRepo{items: []skilldomain.Skill{day25PromptSkill("builtin:one")}}
	s := NewService(repo, nil, llm.New(&config.APIConfig{}), false)
	first := s.EnabledTools("alice")
	first["skill_one"].Parameters[0].Name = "changed"
	if repo.items[0].Parameters[0].Name != "input" || s.EnabledTools("alice")["skill_one"].Parameters[0].Name != "input" {
		t.Fatal("tool params alias stored skill")
	}
}
func TestSkillPromptExecutionRequiresOwnerAndHonorsCancellation(t *testing.T) {
	s := NewService(&memoryRepo{items: []skilldomain.Skill{day25PromptSkill("builtin:one")}}, nil, llm.New(&config.APIConfig{}), false)
	tool := s.EnabledTools("alice")["skill_one"]
	for _, ctx := range []context.Context{context.Background(), day25SkillContext("bob")} {
		if _, err := tool.ExecuteCtx(ctx, map[string]interface{}{"input": "private"}); err == nil {
			t.Fatal("unowned prompt execution allowed")
		}
	}
	ctx, cancel := context.WithCancel(day25SkillContext("alice"))
	cancel()
	if _, err := tool.ExecuteCtx(ctx, map[string]interface{}{"input": "private"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if out, err := tool.Execute(map[string]interface{}{"input": "legacy-owned"}); err != nil || !strings.Contains(out, "legacy-owned") {
		t.Fatal(out, err)
	}
}
func TestSkillPromptInputIsExplicitAndDeterministic(t *testing.T) {
	s := NewService(&memoryRepo{items: []skilldomain.Skill{day25PromptSkill("builtin:one")}}, nil, llm.New(&config.APIConfig{}), false)
	tool := s.EnabledTools("alice")["skill_one"]
	ctx := day25SkillContext("alice")
	for _, params := range []map[string]interface{}{nil, {"input": "", "query": "must-not-replace-empty"}, {"input": 42, "query": "must-not-replace-type"}} {
		if _, err := tool.ExecuteCtx(ctx, params); err == nil {
			t.Fatal("invalid explicit input accepted")
		}
	}
	for i := 0; i < 20; i++ {
		out, err := tool.ExecuteCtx(ctx, map[string]interface{}{"z": "last-value", "a": "first-value"})
		if err != nil || !strings.Contains(out, "first-value") || strings.Contains(out, "last-value") {
			t.Fatalf("nondeterministic fallback: %q %v", out, err)
		}
	}
}
func TestSkillTruncatedTemplateStillIncludesUserInput(t *testing.T) {
	sk := day25PromptSkill("builtin:one")
	sk.PromptTemplate = strings.Repeat("测", 4000) + "{{input}}"
	sanitizeManifest(&sk.Manifest)
	s := NewService(&memoryRepo{items: []skilldomain.Skill{sk}}, nil, llm.New(&config.APIConfig{}), false)

	bodies := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies <- string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()
	cfg := &config.APIConfig{LLMConfig: config.LLMConfig{LLMAPIKey: "fictional-key", LLMAPIUrl: server.URL, LLMModel: "fixture"}}
	s.llm = llm.New(cfg)
	_, err := s.EnabledTools("alice")["skill_one"].ExecuteCtx(day25SkillContext("alice"), map[string]interface{}{"input": "retained-user-input"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-bodies:
		if !strings.Contains(body, "retained-user-input") {
			t.Fatal("sanitized template lost request input")
		}
	case <-time.After(time.Second):
		t.Fatal("model request not observed")
	}
}
func TestSkillMCPPreservesStructuredResultsAndOwner(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"echo":"ok"}`))
	}))
	defer server.Close()
	sk := day25PromptSkill("mcp:one")
	sk.Source = skilldomain.SourceMCP
	sk.Invocation = skilldomain.InvokeMCP
	sk.Endpoint = server.URL
	s := NewService(&memoryRepo{items: []skilldomain.Skill{sk}}, nil, nil, false)
	tool := s.EnabledTools("alice")["skill_one"]
	if tool.ExecuteStructured == nil || !tool.IsMCP {
		t.Fatal("MCP structured interface lost")
	}
	denied := tool.ExecuteStructured(day25SkillContext("bob"), map[string]interface{}{"input": "private"})
	if denied.Success || denied.Error == nil || calls.Load() != 0 {
		t.Fatal("foreign structured call reached endpoint")
	}
	if _, err := tool.ExecuteCtx(context.Background(), map[string]interface{}{"input": "private"}); err == nil || calls.Load() != 0 {
		t.Fatal("anonymous context call reached endpoint")
	}
	deadline, cancel := context.WithDeadline(day25SkillContext("alice"), time.Now().Add(-time.Second))
	defer cancel()
	expired := tool.ExecuteStructured(deadline, map[string]interface{}{"input": "private"})
	if expired.Error == nil || expired.Error.Code != "timeout" || !errors.Is(expired.Error.Cause, context.DeadlineExceeded) || calls.Load() != 0 {
		t.Fatalf("deadline classification or side effect changed: %+v", expired)
	}
	result := tool.ExecuteStructured(day25SkillContext("alice"), map[string]interface{}{"input": "value"})
	if !result.Success || result.Metadata["backend"] != "mcp" || result.PayloadJSON["echo"] != "ok" {
		t.Fatalf("structured result lost: %+v", result)
	}
	if out, err := tool.Execute(map[string]interface{}{"input": "legacy"}); err != nil || !strings.Contains(out, "ok") || calls.Load() != 2 {
		t.Fatal(out, err, calls.Load())
	}
}
func TestSkillProviderIntegratesWithActualAgentLoop(t *testing.T) {
	repo := &memoryRepo{items: []skilldomain.Skill{day25PromptSkill("builtin:doc_polish")}}
	cfg := &config.APIConfig{}
	cfg.ShortTermMaxTurns = 10
	cfg.ChunkSize = 200
	cfg.ChunkOverlap = 20
	cfg.GraphMaxParallel = 2
	s := NewService(repo, nil, llm.New(cfg), false)
	agent := appchat.New(cfg, appchat.Deps{ChatRepo: chathistory.NewPGRepo(nil), PrefRepo: prefrepo.NewPGRepo(nil), SnapRepo: snapshot.NewPGRepo(nil), LTMRepo: ltmrepo.NewPGRepo(nil), RAGChunkRepo: ragchunk.NewStore(nil, nil, nil), DocumentRepo: documentrepo.NewStore(nil, t.TempDir()), Events: eventbus.NewKafkaPublisher(nil, false), EnabledSkillTools: s.EnabledTools})
	response := agent.ProcessContext(day25SkillContext("alice"), "润色这段文字", appchat.ChatOptions{})
	if response.Task == nil || response.Task.Status != "completed" || len(response.Task.Graph.Nodes) != 1 {
		t.Fatalf("skill was not executed: %+v", response)
	}
	for _, node := range response.Task.Graph.Nodes {
		if node.ToolName != "skill_doc_polish" || !strings.Contains(node.Result, "润色这段文字") {
			t.Fatalf("wrong skill observation: %+v", node)
		}
	}
	other := agent.ProcessContext(day25SkillContext("bob"), "润色这段文字", appchat.ChatOptions{})
	if other.Task != nil {
		t.Fatal("foreign user received installed tool")
	}
}
func TestSkillServiceMissingDependenciesAndBlankUsersFailSafely(t *testing.T) {
	s := NewService(nil, nil, nil, false)
	for _, call := range []func(){func() {
		if _, err := s.ListInstalled("alice"); !errors.Is(err, skillrepo.ErrUnavailable) {
			t.Error(err)
		}
	}, func() {
		if err := s.Install(context.Background(), "alice", "builtin:translate"); !errors.Is(err, skillrepo.ErrUnavailable) {
			t.Error(err)
		}
	}, func() {
		if _, err := s.Toggle("alice", "x", true); !errors.Is(err, skillrepo.ErrUnavailable) {
			t.Error(err)
		}
	}, func() {
		if _, err := s.Uninstall("alice", "x"); !errors.Is(err, skillrepo.ErrUnavailable) {
			t.Error(err)
		}
	}, func() {
		if len(s.EnabledTools("alice")) != 0 {
			t.Error("nil repo produced tools")
		}
	}} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("dependency panic: %v", r)
				}
			}()
			call()
		}()
	}
	repo := &memoryRepo{}
	s = NewService(repo, nil, nil, false)
	if err := s.Install(context.Background(), " \t", "builtin:translate"); err == nil || len(repo.items) != 0 {
		t.Fatal("blank user installed skill")
	}
	if _, err := s.ListInstalled(""); err == nil {
		t.Fatal("blank user read skills")
	}
	if _, err := s.Toggle("", "x", true); err == nil {
		t.Fatal("blank toggle")
	}
	if _, err := s.Uninstall("", "x"); err == nil {
		t.Fatal("blank uninstall")
	}
	repo.items = []skilldomain.Skill{day25PromptSkill("builtin:one")}
	if _, err := s.EnabledTools("alice")["skill_one"].ExecuteCtx(day25SkillContext("alice"), map[string]interface{}{"input": "value"}); err == nil {
		t.Fatal("nil model succeeded")
	}
}

func TestSkillInstallCanceledDuringLookupPreservesContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	day25HubTransport(t, func(r *http.Request) (*http.Response, error) {
		cancel()
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	repo := &memoryRepo{}
	svc := NewService(repo, skillhub.NewClient("", "", time.Hour), nil, true)
	if err := svc.Install(ctx, "alice", "github:owner/report"); !errors.Is(err, context.Canceled) || len(repo.items) != 0 {
		t.Fatalf("lookup cancellation lost: %v installs=%d", err, len(repo.items))
	}
}
