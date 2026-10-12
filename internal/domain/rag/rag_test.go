package rag

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"agi-assistant/config"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/pkg/logger"
)

func TestRecursiveSplitter_RespectsParagraphBoundary(t *testing.T) {
	s := NewRecursiveSplitter(50, 0, nil)
	text := "第一段内容比较短。\n\n第二段开头，这里写一些东西，凑够长度。\n\n第三段。"
	chunks := s.Split(text)
	if len(chunks) == 0 {
		t.Fatal("expected non-empty chunks")
	}
	// 不应该把任何"第N段"标识切到中间
	for _, c := range chunks {
		if strings.Contains(c.Content, "第二段开") && !strings.Contains(c.Content, "第二段开头") {
			t.Errorf("split inside a sentence: %q", c.Content)
		}
	}
}

func TestRecursiveSplitter_PreservesCodeBlock(t *testing.T) {
	s := NewRecursiveSplitter(30, 0, nil)
	text := "前文说明。\n\n```go\nfunc Foo() {\n  println(\"hello\")\n}\n```\n\n后文总结。"
	chunks := s.Split(text)
	// 至少有一个 chunk 包含完整的代码块
	foundFull := false
	for _, c := range chunks {
		if strings.Contains(c.Content, "func Foo()") && strings.Contains(c.Content, "```") {
			foundFull = true
		}
	}
	if !foundFull {
		t.Errorf("code block was split; chunks=%v", chunks)
	}
}

func TestRecursiveSplitter_SizeBounded(t *testing.T) {
	s := NewRecursiveSplitter(40, 5, nil)
	text := strings.Repeat("中文内容反复出现，", 50)
	chunks := s.Split(text)
	for i, c := range chunks {
		// overlap 会让某些 chunk 略超 chunkSize，但不应超太多
		if runeLen(c.Content) > 60 {
			t.Errorf("chunk[%d] too long: len=%d", i, runeLen(c.Content))
		}
	}
}

func TestRecursiveSplitter_OverlapApplied(t *testing.T) {
	s := NewRecursiveSplitter(20, 5, nil)
	text := "AAAAAAAAAA。BBBBBBBBBB。CCCCCCCCCC。DDDDDDDDDD。"
	chunks := s.Split(text)
	if len(chunks) < 2 {
		t.Fatalf("expected ≥2 chunks, got %d", len(chunks))
	}
	// 第二段开头应该带上一段尾部 5 字符
	prevTail := tailRunes(chunks[0].Content, 5)
	if !strings.HasPrefix(chunks[1].Content, prevTail) {
		t.Errorf("overlap missing: prev tail=%q, next prefix=%q",
			prevTail, string([]rune(chunks[1].Content)[:5]))
	}
}

func TestParseRewriteJSON_HandlesCodeFence(t *testing.T) {
	raw := "```json\n{\"queries\": [\"a\", \"b\", \"c\"]}\n```"
	out := parseRewriteJSON(raw)
	if len(out) != 3 || out[0] != "a" {
		t.Errorf("unexpected parse: %v", out)
	}
}

func TestParseRerankJSON_HandlesPlain(t *testing.T) {
	raw := `{"scores":[{"idx":0,"score":9},{"idx":1,"score":3}]}`
	out := parseRerankJSON(raw)
	if len(out) != 2 || out[0].Score != 9 || out[1].Idx != 1 {
		t.Errorf("unexpected parse: %v", out)
	}
}

func TestLLMRewriter_FallbackOnNoLLM(t *testing.T) {
	r := NewLLMRewriter(nil, 3)
	got := r.Rewrite("你好", nil)
	if len(got) != 1 || got[0] != "你好" {
		t.Errorf("expected fallback to original, got %v", got)
	}
}

func TestLLMReranker_FallbackOnNoLLM(t *testing.T) {
	r := NewLLMReranker(nil, 0)
	in := []HybridResult{
		{Chunk: Chunk{Content: "a"}, Score: 0.5},
		{Chunk: Chunk{Content: "b"}, Score: 0.4},
		{Chunk: Chunk{Content: "c"}, Score: 0.3},
	}
	got := r.Rerank("q", in, 2)
	if len(got) != 2 || got[0].Chunk.Content != "a" {
		t.Errorf("expected truncate to topK, got %v", got)
	}
}

func TestSplitter_InvalidParameters(t *testing.T) {
	for _, size := range []int{0, -2} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			old, recursive := NewTextSplitter(size, 0), NewRecursiveSplitter(size, 0, nil)
			// Check the termination precondition before exercising formerly unbounded loops.
			if old.chunkSize <= 0 || recursive.chunkSize <= 0 {
				t.Fatal("invalid size leaves a non-advancing splitter")
			}
			for _, s := range []Splitter{old, recursive} {
				if len(s.Split("合成边界文本")) == 0 {
					t.Error("normalized splitter must terminate with nonempty chunks")
				}
			}
		})
	}
	text := "abcdefghijklmnop"
	for _, s := range []Splitter{NewTextSplitter(4, -3), NewRecursiveSplitter(4, -3, nil)} {
		var rebuilt string
		for _, c := range s.Split(text) {
			rebuilt += c.Content
		}
		if rebuilt != text {
			t.Errorf("negative overlap must not drop text: %q", rebuilt)
		}
	}
}

func TestSplitter_UnicodeAndCodeFence(t *testing.T) {
	text := "中文🙂段落。第二段内容。\n\n最后一段。"
	for _, s := range []Splitter{NewTextSplitter(5, 0), NewRecursiveSplitter(5, 0, nil)} {
		var rebuilt string
		for i, c := range s.Split(text) {
			if c.ID != i || !utf8.ValidString(c.Content) || runeLen(c.Content) > 5 {
				t.Errorf("invalid Unicode chunk: %+v", c)
			}
			rebuilt += c.Content
		}
		if strings.ReplaceAll(rebuilt, "\n", "") != strings.ReplaceAll(text, "\n", "") {
			t.Error("non-whitespace content was lost")
		}
	}
	code := "```go\n" + strings.Repeat("println(\"synthetic\")\n", 5) + "```"
	found := false
	for _, c := range NewRecursiveSplitter(10, 0, nil).Split("prefix\n" + code + "\nsuffix") {
		found = found || strings.Contains(c.Content, code)
	}
	if !found {
		t.Error("paired code fence must remain whole even when larger than the target size")
	}
}

func TestLLMRewriter_OriginalQueryRetained(t *testing.T) {
	r := NewLLMRewriter(func(_, _ string) string {
		return `{"queries":["independent","variant one","variant two","variant three"]}`
	}, 3)
	got := r.Rewrite("original", nil)
	if len(got) != 3 || got[0] != "independent" || !reflect.DeepEqual(got, []string{"independent", "variant one", "original"}) {
		t.Errorf("bounded rewrites must preserve the original query: %v", got)
	}
}

func TestLLMRewriter_HistoryAndFallback(t *testing.T) {
	logger.Init(logger.Config{Output: io.Discard})
	var history []HistoryMessage
	for i := 0; i < 8; i++ {
		history = append(history, HistoryMessage{Content: string(rune('a'+i)) + strings.Repeat("中", 210)})
	}
	r := NewLLMRewriter(func(system, user string) string {
		if !strings.Contains(system, "总条数严格等于 3") || strings.Contains(user, "[user] a") || strings.Contains(user, "[user] b") || !strings.Contains(user, "[user] c") || strings.Contains(user, strings.Repeat("中", 201)) {
			t.Error("history should retain six messages, use default roles and bound Unicode previews")
		}
		return "```json\n{\"queries\":[\" Independent \",\"independent\",\" \" ]}\n```"
	}, 0)
	if got := r.Rewrite(" original ", history); !reflect.DeepEqual(got, []string{"Independent", "original"}) {
		t.Errorf("cleanup/dedup/default count failed: %v", got)
	}
	for _, raw := range []string{"", "synthetic malformed response", `{"queries":[]}`} {
		got := NewLLMRewriter(func(_, _ string) string { return raw }, 3).Rewrite("original", nil)
		if !reflect.DeepEqual(got, []string{"original"}) {
			t.Error("invalid model response must preserve the input")
		}
	}
	calls := 0
	disabled := NewLLMRewriter(func(_, _ string) string { calls++; return "{}" }, 1)
	if got := disabled.Rewrite(" original ", nil); !reflect.DeepEqual(got, []string{"original"}) || calls != 0 || len(disabled.Rewrite(" ", nil)) != 0 {
		t.Error("disabled/empty rewrite must skip the callback")
	}
}

var testRepoError = errors.New("synthetic repository failure")

type testChunkRepo struct {
	ragchunk.Repo
	pg, vector, keyword         bool
	rows                        map[int64]ragchunk.Row
	esHits                      func(string) []ragchunk.ESHit
	vectorHits                  []ragchunk.MilvusHit
	saveErr, loadErr, allErr    error
	esErr, vectorErr, deleteErr error
	searchCalls                 atomic.Int32
	loadAllCalls                int
	saved                       []ragchunk.Row
	savedEmb                    [][]byte
	vectorIDs, esIDs            []int64
	deleted                     string
}

func newTestChunkRepo() *testChunkRepo {
	return &testChunkRepo{pg: true, keyword: true, rows: make(map[int64]ragchunk.Row)}
}

func (r *testChunkRepo) PGAvailable() bool     { return r.pg }
func (r *testChunkRepo) MilvusAvailable() bool { return r.vector }
func (r *testChunkRepo) ESAvailable() bool     { return r.keyword }
func (r *testChunkRepo) SavePGWithMetadata(_ string, _ int, content, parent string, emb []byte, m ragchunk.Metadata) (int64, error) {
	if r.saveErr != nil {
		return 0, r.saveErr
	}
	id := int64(len(r.rows) + 1)
	row := ragchunk.Row{ID: id, Content: content, ParentContent: parent, DocumentID: m.DocumentID, VersionID: m.VersionID, Section: m.Section}
	r.rows[id] = row
	r.saved = append(r.saved, row)
	r.savedEmb = append(r.savedEmb, append([]byte(nil), emb...))
	return id, nil
}
func (r *testChunkRepo) IndexES(id int64, _, _ string, _ int) error {
	r.esIDs = append(r.esIDs, id)
	return r.esErr
}
func (r *testChunkRepo) InsertMilvus(ids []int64, _ []string, _ [][]float32) error {
	r.vectorIDs = append(r.vectorIDs, ids...)
	return r.vectorErr
}
func (r *testChunkRepo) LoadByIDs(ids []int64) ([]ragchunk.Row, error) {
	var rows []ragchunk.Row
	for _, id := range ids {
		if row, ok := r.rows[id]; ok {
			rows = append(rows, row)
		}
	}
	return rows, r.loadErr
}
func (r *testChunkRepo) LoadAll() ([]ragchunk.Row, error) {
	r.loadAllCalls++
	var rows []ragchunk.Row
	for _, row := range r.rows {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, r.allErr
}
func (r *testChunkRepo) SearchES(query string, limit int) ([]ragchunk.ESHit, error) {
	r.searchCalls.Add(1)
	if r.esHits != nil {
		hits := r.esHits(query)
		if limit > 0 && len(hits) > limit {
			hits = hits[:limit]
		}
		return hits, r.esErr
	}
	var hits []ragchunk.ESHit
	for id := range r.rows {
		hits = append(hits, ragchunk.ESHit{PGID: id, Score: 1})
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].PGID < hits[j].PGID })
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, r.esErr
}
func (r *testChunkRepo) SearchMilvus(_ []float32, limit int) ([]ragchunk.MilvusHit, error) {
	r.searchCalls.Add(1)
	hits := r.vectorHits
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits, r.vectorErr
}
func (r *testChunkRepo) Delete(hash string) error {
	r.deleted = hash
	if r.deleteErr == nil {
		clear(r.rows)
	}
	return r.deleteErr
}

func TestHybridStore_LimitsAndMissingEmbedding(t *testing.T) {
	for _, mode := range []string{"keyword", "hybrid", "semantic"} {
		t.Run(mode, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("invalid limit or missing embedding callback panicked: %v", p)
				}
			}()
			r := newTestChunkRepo()
			r.rows[1] = ragchunk.Row{ID: 1, Content: "synthetic"}
			h := NewHybridStore(&config.APIConfig{}, r)
			h.mode = mode
			for _, limit := range []int{0, -1} {
				if len(h.Search("q", limit)) != 0 || len(h.SearchMulti([]string{"a", "b"}, limit)) != 0 {
					t.Error("nonpositive limits must produce no results")
				}
			}
			if len(h.Search(" \n ", 1)) != 0 || r.searchCalls.Load() != 0 {
				t.Error("invalid limits and blank query must skip backends")
			}
			got := h.Search("q", 1)
			if mode == "semantic" {
				if len(got) != 0 {
					t.Error("semantic search needs an embedding callback")
				}
			} else if len(got) != 1 || got[0].Source != "keyword" {
				t.Errorf("missing embedding must retain keyword fallback: %v", got)
			}
		})
	}
}

func TestHybridStore_HydrationError(t *testing.T) {
	for _, mode := range []string{"keyword", "semantic", "hybrid"} {
		r := newTestChunkRepo()
		r.rows[1] = ragchunk.Row{ID: 1, Content: "partial row"}
		r.vectorHits = []ragchunk.MilvusHit{{ID: 1, Distance: 0.8}}
		r.loadErr = testRepoError
		h := NewHybridStore(&config.APIConfig{}, r)
		h.mode = mode
		h.SetEmbedFn(func(string) ([]float64, error) { return []float64{1, 2}, nil })
		if got := h.Search("q", 2); len(got) != 0 {
			t.Errorf("%s returned partial rows after a read failure: %v", mode, got)
		}
	}
}

func TestHybridStore_MultiQueryIdentity(t *testing.T) {
	r := newTestChunkRepo()
	r.rows[1] = ragchunk.Row{ID: 1, Content: "shared text", ParentContent: "document one"}
	r.rows[2] = ragchunk.Row{ID: 2, Content: "shared text", ParentContent: "document two"}
	r.esHits = func(q string) []ragchunk.ESHit {
		if q == "one" {
			return []ragchunk.ESHit{{PGID: 1, Score: 4}, {PGID: 2, Score: 1}}
		}
		return []ragchunk.ESHit{{PGID: 1, Score: 3}}
	}
	got := NewHybridStore(&config.APIConfig{}, r).SearchMulti([]string{"one", "two"}, 2)
	if len(got) != 2 || got[0].PGID != 1 || got[1].PGID != 2 || got[0].Parent != "document one" || got[1].Parent != "document two" {
		t.Fatalf("distinct persisted chunks must retain their own parents: %v", got)
	}
	if math.Abs(got[0].Score-2.0/61) > 1e-12 {
		t.Errorf("repeated hit did not receive cross-query RRF credit: %v", got)
	}
}

func TestHybridStore_IndexMetadataAndEmbeddingFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		r := newTestChunkRepo()
		r.vector = true
		h := NewHybridStore(&config.APIConfig{}, r)
		h.SetEmbedFn(func(string) ([]float64, error) {
			if fail {
				return []float64{1, 2}, testRepoError
			}
			return []float64{1, 2}, nil
		})
		hash, indexed := h.IndexWithParentsAndMetadata([]Chunk{{Content: "child"}}, []string{"full parent"}, "synthetic document", IngestMetadata{DocumentID: "doc", VersionID: "v1", Section: "intro"})
		if len(hash) != 16 || len(indexed) != 1 || indexed[0].PGID != 1 || len(r.saved) != 1 || r.saved[0].ParentContent != "full parent" || r.saved[0].DocumentID != "doc" || r.saved[0].VersionID != "v1" || r.saved[0].Section != "intro" || !reflect.DeepEqual(r.esIDs, []int64{1}) {
			t.Fatal("indexing lost persisted identity, metadata, parent or keyword index")
		}
		if fail {
			if len(r.vectorIDs) != 0 || string(r.savedEmb[0]) != "null" {
				t.Error("failed embedding must not reach the vector index")
			}
		} else if !reflect.DeepEqual(r.vectorIDs, []int64{1}) || string(r.savedEmb[0]) != "[1,2]" {
			t.Error("valid embedding was not indexed")
		}
	}
}

func TestHybridStore_FusionAndBackendFallback(t *testing.T) {
	r := newTestChunkRepo()
	r.vector = true
	r.rows[1] = ragchunk.Row{ID: 1, Content: "first", ParentContent: "parent one"}
	r.rows[2] = ragchunk.Row{ID: 2, Content: "second", ParentContent: "parent two"}
	r.vectorHits = []ragchunk.MilvusHit{{ID: 1, Distance: 0.9}, {ID: 2, Distance: 0.7}}
	r.esHits = func(string) []ragchunk.ESHit { return []ragchunk.ESHit{{PGID: 2, Score: 1000}} }
	h := NewHybridStore(&config.APIConfig{}, r)
	h.SetEmbedFn(func(string) ([]float64, error) { return []float64{1, 2}, nil })
	got := h.Search("q", 2)
	if len(got) != 2 || got[0].PGID != 2 || got[0].Parent != "parent two" || got[0].Source != "hybrid" || math.Abs(got[0].Score-(1.0/61+1.0/62)) > 1e-12 {
		t.Fatalf("RRF must combine ranks rather than incompatible raw scores: %v", got)
	}
	r.vectorErr = testRepoError
	if got := h.Search("q", 1); len(got) != 1 || got[0].Source != "keyword" {
		t.Error("vector failure must fall back to keywords")
	}
	r.vectorErr, r.esErr = nil, testRepoError
	if got := h.Search("q", 1); len(got) != 1 || got[0].Source != "semantic" {
		t.Error("keyword failure must fall back to semantic results")
	}
	r.vectorErr = testRepoError
	if len(h.Search("q", 1)) != 0 {
		t.Error("both backend failures must produce no results")
	}
}

func TestHybridStore_UnavailableStorage(t *testing.T) {
	r := newTestChunkRepo()
	r.pg = false
	r.vector = true
	h := NewHybridStore(&config.APIConfig{}, r)
	_, indexed := h.Index([]Chunk{{Content: "synthetic"}}, "synthetic document")
	if h.Mode() != "unavailable" || len(indexed) != 0 || len(r.saved) != 0 || len(h.Search("q", 2)) != 0 || r.searchCalls.Load() != 0 {
		t.Error("missing source storage must skip writes and searches")
	}
}

func TestLLMReranker_NonpositiveLimit(t *testing.T) {
	calls := 0
	r := NewLLMReranker(func(_, _ string) string { calls++; return "{}" }, 10)
	for _, results := range [][]HybridResult{
		{{Chunk: Chunk{Content: "one"}}},
		{{Chunk: Chunk{Content: "one"}}, {Chunk: Chunk{Content: "two"}}},
	} {
		for _, limit := range []int{0, -1} {
			if len(r.Rerank("q", results, limit)) != 0 || len(NewLLMReranker(nil, 0).Rerank("q", results, limit)) != 0 {
				t.Error("nonpositive limits must produce no candidates")
			}
		}
	}
	if calls != 0 {
		t.Error("invalid limits must skip model calls")
	}
}

func TestLLMReranker_ScoresPreviewsAndTies(t *testing.T) {
	in := []HybridResult{
		{PGID: 1, Chunk: Chunk{Content: strings.Repeat("中", 10)}, Score: .4, Source: "hybrid", Parent: "parent one"},
		{PGID: 2, Chunk: Chunk{Content: "second"}, Score: .8, Source: "hybrid", Parent: "parent two"},
		{PGID: 3, Chunk: Chunk{Content: "third"}, Score: .1, Source: "hybrid", Parent: "parent three"},
	}
	r := NewLLMReranker(func(system, user string) string {
		if !strings.Contains(system, "0~10") || !strings.Contains(user, "问题：q") || !strings.Contains(user, "[0] 中中中中…") || strings.Contains(user, strings.Repeat("中", 5)) {
			t.Error("listwise prompt or Unicode preview limit is incorrect")
		}
		return "```json\n{\"scores\":[{\"idx\":0,\"score\":8},{\"idx\":1,\"score\":8},{\"idx\":2,\"score\":2}]}\n```"
	}, 4)
	got := r.Rerank("q", in, 2)
	if len(got) != 2 || got[0].PGID != 2 || got[1].PGID != 1 || got[0].Score != .8 || got[0].Source != "hybrid+rerank" || got[0].Parent != "parent two" {
		t.Errorf("model scores, RRF tie-break or identity preservation failed: %v", got)
	}
	if in[0].Score != .4 || in[0].Source != "hybrid" {
		t.Error("reranking mutated the caller's input")
	}
}

func TestLLMReranker_InvalidScoresFallback(t *testing.T) {
	in := []HybridResult{
		{Chunk: Chunk{Content: "first"}, Score: .8, Source: "keyword"},
		{Chunk: Chunk{Content: "second"}, Score: .4, Source: "keyword"},
	}
	for _, raw := range []string{
		"synthetic malformed response", `{"scores":[]}`,
		`{"scores":[{"idx":1,"score":99}]}`,
		`{"scores":[{"idx":0,"score":-2}]}`,
		`{"scores":[{"idx":7,"score":9}]}`,
	} {
		got := NewLLMReranker(func(_, _ string) string { return raw }, 0).Rerank("q", in, 1)
		if !reflect.DeepEqual(got, in[:1]) {
			t.Errorf("invalid model scores must retain the original order and scores: %v", got)
		}
	}
}

func TestLLMReranker_PartialScoresKeepUnscoredCandidates(t *testing.T) {
	in := []HybridResult{
		{PGID: 1, Chunk: Chunk{Content: "first"}, Score: .8},
		{PGID: 2, Chunk: Chunk{Content: "second"}, Score: .4},
		{PGID: 3, Chunk: Chunk{Content: "third"}, Score: .2},
	}
	r := NewLLMReranker(func(_, _ string) string {
		return `{"scores":[{"idx":1,"score":9},{"idx":3,"score":5}]}`
	}, 0)
	got := r.Rerank("q", in, 3)
	if len(got) != 3 || got[0].PGID != 2 || got[1].PGID != 1 || got[2].PGID != 3 || got[1].Score != .8 {
		t.Error("partially scored results must retain unscored candidates in RRF order")
	}
}

type testRAGEvents struct {
	topic, payload string
}

func (e *testRAGEvents) Publish(topic, payload string) {
	e.topic, e.payload = topic, payload
}

func TestEngine_IngestAndAnswer(t *testing.T) {
	r := newTestChunkRepo()
	events := &testRAGEvents{}
	e := NewEngine(&config.APIConfig{RAGConfig: config.RAGConfig{ChunkSize: 12, TopK: 1}}, r, events)
	doc := "合成资料包含关键术语。\n\n第二段提供上下文和细节。"
	got := e.IngestWithMetadata(doc, IngestMetadata{DocumentID: "doc", VersionID: "v1", Section: "intro"})
	if !e.Loaded || got.ChunkCount < 2 || got.IndexedCount != got.ChunkCount || got.ParentCount != 1 || len(got.DocHash) != 16 || len(got.ChunkPreview) == 0 {
		t.Fatalf("unexpected ingestion result: %+v", got)
	}
	for _, row := range r.saved {
		if row.ParentContent != doc || row.DocumentID != "doc" || row.VersionID != "v1" || row.Section != "intro" {
			t.Error("engine failed to propagate full parent and metadata")
		}
	}
	var event struct {
		ChunkCount   int    `json:"chunk_count"`
		IndexedCount int    `json:"indexed_count"`
		DocHash      string `json:"doc_hash"`
	}
	if events.topic != "rag.ingest" || json.Unmarshal([]byte(events.payload), &event) != nil || event.ChunkCount != got.ChunkCount || event.IndexedCount != got.IndexedCount || event.DocHash != got.DocHash {
		t.Error("ingestion event does not reflect persisted results")
	}
	e.SetRewriter(NewLLMRewriter(func(_, user string) string {
		if !strings.Contains(user, "recent context") {
			t.Error("engine did not forward query history")
		}
		return `{"queries":["independent synthetic query"]}`
	}, 3))
	rerankCalls := 0
	e.SetReranker(NewLLMReranker(func(_, user string) string {
		rerankCalls++
		if !strings.Contains(user, "independent synthetic query") {
			t.Error("engine did not pass the rewritten query to reranking")
		}
		return `{"scores":[{"idx":0,"score":9}]}`
	}, 20))
	e.SetGenerateFn(func(system, user string) string {
		if !strings.Contains(system, "仅根据提供的上下文") || !strings.Contains(user, doc) || !strings.Contains(user, "问题：independent synthetic query") {
			t.Error("answer synthesis did not receive the full parent and independent query")
		}
		return "synthetic grounded answer"
	})
	answer, results := e.QueryWithHistory("original", []HistoryMessage{{Role: "user", Content: "recent context"}})
	if answer != "synthetic grounded answer" || len(results) != 1 || results[0].Chunk.Content != doc || rerankCalls != 1 {
		t.Fatalf("unexpected query result: %q, %+v", answer, results)
	}
	if len(e.Chunks()) != got.IndexedCount {
		t.Error("persisted chunk preview is incomplete")
	}
}

func TestEngine_FailedIngestPreservesKnowledge(t *testing.T) {
	r := newTestChunkRepo()
	e := NewEngine(&config.APIConfig{RAGConfig: config.RAGConfig{ChunkSize: 20, TopK: 1}}, r, nil)
	if e.Ingest("existing synthetic knowledge").IndexedCount == 0 {
		t.Fatal("initial ingestion failed")
	}
	r.saveErr = testRepoError
	if e.Ingest("new synthetic document").IndexedCount != 0 || !e.Loaded {
		t.Error("failed ingestion must not disable an existing knowledge base")
	}
	if answer, results := e.Query("q"); len(results) == 0 || strings.Contains(answer, "知识库为空") {
		t.Error("existing knowledge must remain queryable after a failed new ingestion")
	}
}

func TestEngine_DeleteErrorsPreserveState(t *testing.T) {
	t.Run("primary-delete-failure", func(t *testing.T) {
		r := newTestChunkRepo()
		r.deleteErr = testRepoError
		e := NewEngine(&config.APIConfig{}, r, nil)
		e.RestoreChunks([]Chunk{{Content: "existing"}})
		if !errors.Is(e.Delete("synthetic-hash"), testRepoError) || !e.Loaded || r.loadAllCalls != 0 {
			t.Error("failed primary deletion must stop before side effects and state refresh")
		}
	})
	t.Run("refresh-failure", func(t *testing.T) {
		r := newTestChunkRepo()
		r.allErr = testRepoError
		e := NewEngine(&config.APIConfig{}, r, nil)
		e.RestoreChunks([]Chunk{{Content: "existing"}})
		if !errors.Is(e.Delete("synthetic-hash"), testRepoError) || !e.Loaded {
			t.Error("failed refresh must report the error and preserve the last known loaded state")
		}
	})
	t.Run("success", func(t *testing.T) {
		r := newTestChunkRepo()
		r.rows[1] = ragchunk.Row{ID: 1, Content: "existing"}
		e := NewEngine(&config.APIConfig{}, r, nil)
		e.RestoreChunks([]Chunk{{Content: "existing"}})
		if e.Delete("synthetic-hash") != nil || e.Loaded || r.deleted != "synthetic-hash" {
			t.Error("successful deletion must refresh the empty knowledge base state")
		}
	})
}

func TestEngine_QueryFallbacks(t *testing.T) {
	r := newTestChunkRepo()
	e := NewEngine(&config.APIConfig{RAGConfig: config.RAGConfig{TopK: 2}}, r, nil)
	if answer, results := e.Query("q"); !strings.Contains(answer, "知识库为空") || len(results) != 0 || r.searchCalls.Load() != 0 {
		t.Error("empty knowledge must skip retrieval")
	}
	r.rows[1] = ragchunk.Row{ID: 1, Content: "synthetic old child"}
	e.RestoreChunks([]Chunk{{Content: "synthetic old child"}})
	if answer, results := e.Query("q"); !strings.Contains(answer, "synthetic old child") || len(results) != 1 || results[0].Chunk.Content != "synthetic old child" {
		t.Error("missing parent and model must return retrieved source text")
	}
	r.esErr = testRepoError
	if answer, _ := e.Query("q"); !strings.Contains(answer, "未找到相关内容") {
		t.Error("failed retrieval must return the no-result message")
	}
	r.allErr = testRepoError
	if len(e.Chunks()) != 0 {
		t.Error("chunk preview must reject a failed source read")
	}
}
