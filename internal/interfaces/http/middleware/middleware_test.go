package httpmw

import (
	"agi-assistant/internal/domain/auth"
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestRequestID_GeneratesWhenMissing 验证缺 header 时生成 ID 并写回 response header。
func TestRequestID_GeneratesWhenMissing(t *testing.T) {
	var seenID string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenID = RequestIDFromContext(r.Context())
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req)

	if seenID == "" || seenID == "-" {
		t.Errorf("应生成非空 requestID, 得到 %q", seenID)
	}
	if got := rec.Header().Get(RequestIDHeader); got != seenID {
		t.Errorf("response header 应回显 ID, want=%q got=%q", seenID, got)
	}

	if len(seenID) != 32 {
		t.Errorf("生成 ID 应为 32 字符十六进制, 得到长度 %d", len(seenID))
	}
}

// TestRequestID_PreservesIncoming 验证客户端传入的 ID 被尊重（用于跨服务追踪）。
func TestRequestID_PreservesIncoming(t *testing.T) {
	const incoming = "external-trace-abc123"
	var seenID string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenID = RequestIDFromContext(r.Context())
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(RequestIDHeader, incoming)
	h.ServeHTTP(rec, req)

	if seenID != incoming {
		t.Errorf("应保留入站 ID, want=%q got=%q", incoming, seenID)
	}
}

// TestPanicRecover_ReturnsJSON 验证 panic 时返回 500 + JSON + 包含 requestID。
func TestPanicRecover_ReturnsJSON(t *testing.T) {
	chain := func(h http.Handler) http.Handler {
		return RequestID(PanicRecover(h))
	}
	h := chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status: want 500, got %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"error"`) || !strings.Contains(bodyStr, `"request_id"`) {
		t.Errorf("响应应是含 error+request_id 的 JSON, 得到 %q", bodyStr)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Errorf("Content-Type 应为 application/json")
	}
}

// TestPanicRecover_DoesNotLeakStack 验证 stack trace 不会泄漏给客户端
// （只能在服务端日志看到；body 里只有通用错误信息）。
func TestPanicRecover_DoesNotLeakStack(t *testing.T) {
	h := PanicRecover(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("secret-internal-detail")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req)

	if strings.Contains(rec.Body.String(), "secret-internal-detail") {
		t.Errorf("response body 不应包含原 panic 内容")
	}
}

// TestAccessLog_FlusherTransparent 验证 responseRecorder 实现 http.Flusher,
// 让 SSE 路由能正常工作。
func TestAccessLog_FlusherTransparent(t *testing.T) {
	var sawFlusher bool
	h := AccessLog(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawFlusher = w.(http.Flusher)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req)
	if !sawFlusher {
		t.Error("AccessLog 包装后必须仍实现 http.Flusher（SSE 依赖）")
	}
}

// TestCORS_Preflight 验证 OPTIONS 预检直接返回 204。
func TestCORS_Preflight(t *testing.T) {
	mw := CORS(DefaultCORSConfig())
	called := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/api/chat", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("OPTIONS 应返回 204, 得到 %d", rec.Code)
	}
	if called {
		t.Error("OPTIONS 不应进入 handler")
	}
	if rec.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Error("应设置 Access-Control-Allow-Methods")
	}
}

// TestCORS_Whitelist 验证显式 origin 列表只放行匹配的来源。
func TestCORS_Whitelist(t *testing.T) {
	cfg := DefaultCORSConfig()
	cfg.AllowedOrigins = []string{"https://app.example.com"}
	mw := CORS(cfg)
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://evil.com")
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("非白名单 origin 不应得到 Allow-Origin, 得到 %q", got)
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Origin", "https://app.example.com")
	h.ServeHTTP(rec2, req2)
	if got := rec2.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("白名单 origin 应被回显, 得到 %q", got)
	}
}

// TestClientIP_XFF 验证 X-Forwarded-For 解析（反向代理后部署的常见情况）。
func TestClientIP_XFF(t *testing.T) {
	cases := []struct {
		name string
		xff  string
		real string
		want string
	}{
		{"single", "1.2.3.4", "", "1.2.3.4"},
		{"chain", "1.2.3.4, 5.6.7.8, 9.10.11.12", "", "1.2.3.4"},
		{"real_ip_fallback", "", "10.0.0.5", "10.0.0.5"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if c.xff != "" {
				req.Header.Set("X-Forwarded-For", c.xff)
			}
			if c.real != "" {
				req.Header.Set("X-Real-Ip", c.real)
			}
			if got := clientIP(req); got != c.want {
				t.Errorf("want=%q got=%q", c.want, got)
			}
		})
	}
}

// TestChain 验证三件套组合后行为正确：RequestID → PanicRecover → AccessLog
func TestChain(t *testing.T) {
	h := RequestID(PanicRecover(AccessLog(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		w.Write([]byte("ok"))
	}))))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Errorf("status leaked: %d", rec.Code)
	}
	if rec.Header().Get(RequestIDHeader) == "" {
		t.Error("requestID header 缺失")
	}
}
func TestRequestIDRejectsUnsafeAndOversizedHeader(t *testing.T) {
	for _, id := range []string{strings.Repeat("a", 129), "bad\tvalue", "bad\nvalue"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set(RequestIDHeader, id)
		RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if got := RequestIDFromContext(r.Context()); got == id || len(got) != 32 {
				t.Fatalf("unsafe trace ID retained: %q", got)
			}
		})).ServeHTTP(w, req)
	}
}

func TestAuthRejectsWrongIssuerAndMissingExpiry(t *testing.T) {
	const secret = "fictional-signing-secret-for-http-tests"
	issuer, _ := auth.NewTokenIssuer(secret, time.Hour, "expected-service")
	other, _ := auth.NewTokenIssuer(secret, time.Hour, "another-service")
	wrong, _, _ := other.Sign(auth.User{ID: 1})
	missing, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": "expected-service", "sub": "1"}).SignedString([]byte(secret))
	for _, token := range []string{wrong, missing} {
		request := httptest.NewRequest("GET", "/", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		RequireAuth(issuer)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unverified claims passed") })).ServeHTTP(w, request)
		if w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
}
func TestAuthRejectsAmbiguousHeadersAndInvalidSubject(t *testing.T) {
	issuer := newTestIssuer(t)
	valid, _, _ := issuer.Sign(auth.User{ID: 1, Username: "alice"})
	invalid, _, _ := issuer.Sign(auth.User{ID: 0, Username: "empty"})
	for _, headers := range [][]string{{"Bearer " + valid, "Bearer other"}, {"Bearer " + invalid}} {
		req := httptest.NewRequest("GET", "/", nil)
		for _, h := range headers {
			req.Header.Add("Authorization", h)
		}
		w := httptest.NewRecorder()
		RequireAuth(issuer)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid auth reached handler") })).ServeHTTP(w, req)
		if w.Code != 401 {
			t.Fatalf("ambiguous or unowned token status=%d", w.Code)
		}
	}
}
func TestRecorderFlushCommitsStatusBeforeLateHeaders(t *testing.T) {
	base := httptest.NewRecorder()
	rec := &responseRecorder{ResponseWriter: base}
	rec.Flush()
	rec.WriteHeader(201)
	if rec.status != 200 || base.Code != 200 {
		t.Fatalf("flush status diverged: logged=%d actual=%d", rec.status, base.Code)
	}
}

type day26PlainWriter struct{ header http.Header }

func (w *day26PlainWriter) Header() http.Header       { return w.header }
func (*day26PlainWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*day26PlainWriter) WriteHeader(int)             {}
func TestRecorderReportsUnsupportedFlush(t *testing.T) {
	rec := &responseRecorder{ResponseWriter: &day26PlainWriter{header: make(http.Header)}}
	if err := http.NewResponseController(rec).Flush(); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("unsupported flush hidden: %v", err)
	}
}
func TestCORSMixedWildcardAndExistingVary(t *testing.T) {
	for _, origins := range [][]string{{"https://one.example", "*"}, {"https://one.example"}} {
		cfg := DefaultCORSConfig()
		cfg.AllowedOrigins = origins
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Origin", "https://one.example")
		w := httptest.NewRecorder()
		w.Header().Set("Vary", "Accept-Encoding")
		CORS(cfg)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(w, req)
		want := "https://one.example"
		if len(origins) == 2 {
			want = "*"
		}
		if w.Header().Get("Access-Control-Allow-Origin") != want {
			t.Fatal("wildcard depends on list order")
		}
		vary := strings.Join(w.Header().Values("Vary"), ",")
		if !strings.Contains(vary, "Accept-Encoding") {
			t.Fatal("CORS erased existing Vary")
		}
	}
}
