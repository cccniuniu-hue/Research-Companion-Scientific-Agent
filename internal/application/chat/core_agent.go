// Package chat 提供 UnifiedAgent 的依赖装配和初始化入口。
//
// 请求路由与执行由运行模块接入；这里装配模型、记忆、工具和仓储。
//
// 记忆系统作为基础层注入所有模式（偏好 + 长期记忆 → System Prompt，STM → 对话历史）
package chat

import (
	"agi-assistant/config"
	"agi-assistant/internal/domain/knowledge"
	"agi-assistant/internal/domain/memory/shortterm"
	"agi-assistant/internal/domain/promptctx"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/domain/sandbox"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/eventbus"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/chathistory"
	docrepo "agi-assistant/internal/infrastructure/persistence/documentrepo"
	ltmrepo "agi-assistant/internal/infrastructure/persistence/longterm"
	"agi-assistant/internal/infrastructure/persistence/memorytx"
	prefrepo "agi-assistant/internal/infrastructure/persistence/preference"
	"agi-assistant/internal/infrastructure/persistence/ragchunk"
	"agi-assistant/internal/infrastructure/persistence/snapshot"
	toolimpl "agi-assistant/internal/infrastructure/tool"
	"agi-assistant/internal/pkg/logger"
	"context"
	"fmt"
	"strings"
	"sync"
)

// ─────────────────────────────── Unified Agent ───────────────────────────

// UnifiedAgent 整合全部能力，是系统的核心调度入口
type UnifiedAgent struct {
	cfg     *config.APIConfig
	llm     *llm.Client
	rag     *rag.Engine
	sandbox *sandbox.Sandbox
	kg      *knowledge.KGStore // 知识图谱（RAG + 记忆图共享）

	// 三层记忆 + 偏好聚合（mem.stm / mem.ltm / mem.graphMem / mem.pref）
	mem *memoryStack

	// 数据访问层 + 事件 + 基础设施健康状态（repos.chat / repos.pref / ... / repos.events / repos.infra）
	repos *repoBundle

	// RAG 维度（启动期 ragchunk repo 初始化用）
	ragMilvusDim int

	// Schema-driven Runtime Context Assembly（assembler / taskMem / toolTracker）
	// 由 promptCtx 聚合管理，所有 nil 守卫集中在 promptCtx 方法内。
	pctx *promptCtx

	// 工具集：可被 RegisterTool（MCP 热插）并发写入，被 ReAct/Decide 并发读取。
	// 由 toolRegistry 统一管理 RWMutex + map，避免在 agent struct 上铺锁字段。
	tools *toolRegistry

	// 子 Agent 集：research / writer / review / doc 等同进程 worker。
	subagents *subAgentRegistry

	// per-request 共享状态：snapshots、当前任务、in-flight cancel funcs。
	// 由 taskRuntime 聚合并管理 sync.Mutex，避免在 agent struct 上铺锁字段。
	runtime *taskRuntime

	// skillProvider 按 userID 返回「已安装且开启」的 skill 工具集，由 main.go
	// 注入（application/skill.Service.EnabledTools）。可为 nil（未接入广场时）。
	skillProvider func(userID string) map[string]tool.Tool
}

// Deps 是 UnifiedAgent 的依赖注入容器，由 main.go 在启动期组装。
type Deps struct {
	ChatRepo     chathistory.Repo
	PrefRepo     prefrepo.Repo
	SnapRepo     snapshot.Repo
	LTMRepo      ltmrepo.Repo
	MemoryTxRepo memorytx.Store
	RAGChunkRepo ragchunk.Repo
	DocumentRepo docrepo.Repo
	Events       eventbus.Publisher
	// InfraStatus 平台层连接健康快照
	InfraStatus map[string]string
	// EnabledSkillTools 按 userID 返回已开启 skill 的工具集（Skill 广场）。
	// 可为 nil；由 main.go 用 application/skill.Service.EnabledTools 装配。
	EnabledSkillTools func(userID string) map[string]tool.Tool
}

// New 创建并初始化 UnifiedAgent。
//
// 启动序列分四步：
//  1. 装配核心依赖（cfg / llm / rag engine / 5 个聚合：mem / repos / tools / runtime / pctx 之外的字段）
//  2. wireRAGCallbacks：把 LLM/Embed/Rewriter/Reranker 回调注入 rag.Engine
//  3. bootstrapConcurrent：长期记忆/RAG 恢复、沙箱探测、检索索引初始化
//  4. initKnowledgeGraph + buildPromptCtx：依赖前一阶段的 ltm/graphMem，必须串行
//
// 内置 search_web 注册穿插在并发组里，
// 因为 RegisterTool 是持锁的，与同期的 initSandbox 注册 exec_command 不会冲突。
func New(cfg *config.APIConfig, deps Deps) *UnifiedAgent {
	// hydrator 闭包：把 PG repo 包装成 memoryStack 期望的纯函数签名，
	// 让 domain/application 层不直接 import infrastructure。
	stmHydrator := func(userID string) []shortterm.ConversationMessage {
		// 从 chat_history 表加载最近 N 条历史；按时间正序返回。
		// repo Load 已经内部排序 + 反转，这里直接转结构。
		// 取 ShortTermMaxTurns*2 条（每轮 user+assistant）。
		entries := deps.ChatRepo.Load(userID, cfg.ShortTermMaxTurns*2)
		if len(entries) == 0 {
			return nil
		}
		msgs := make([]shortterm.ConversationMessage, 0, len(entries))
		for _, e := range entries {
			msgs = append(msgs, shortterm.ConversationMessage{
				Role:      e.Role,
				Content:   e.Content,
				Timestamp: e.CreatedAt,
			})
		}
		return msgs
	}
	prefHydrator := func(userID string) map[string]string {
		return deps.PrefRepo.Load(userID)
	}

	a := &UnifiedAgent{
		cfg:          cfg,
		llm:          llm.New(cfg),
		rag:          rag.NewEngine(cfg, deps.RAGChunkRepo, deps.Events),
		tools:        newToolRegistry(toolimpl.DefaultTools()),
		subagents:    newSubAgentRegistry(),
		runtime:      newTaskRuntime(),
		mem:          newMemoryStack(cfg, stmHydrator, prefHydrator),
		repos:        newRepoBundle(deps),
		ragMilvusDim: cfg.RAGMilvusDim,
	}
	a.skillProvider = deps.EnabledSkillTools
	a.wireRAGCallbacks()
	a.registerBuiltinSubAgents()
	a.bootstrapConcurrent()
	a.initKnowledgeGraph()
	a.pctx = a.buildPromptCtx()
	return a
}

// wireRAGCallbacks 把 LLM 合成 / Embedding / 可选 Rewriter / 可选 Reranker
// 等回调注入 rag.Engine。所有回调闭包捕获 a，运行时再读 a.pctx 等组件——
// 因此该方法必须在并发组启动前调用，但运行期触发时 pctx 必定已就绪。
func (a *UnifiedAgent) wireRAGCallbacks() {
	cfg := a.cfg
	// LLM 合成回调（携带记忆上下文）
	a.rag.SetGenerateFn(func(systemPrompt, userMsg string) string {
		memPrefix := a.buildContextPrefix(context.Background(), userMsg, "rag")
		fullSystem := systemPrompt
		if memPrefix != "" {
			fullSystem = memPrefix + "\n\n" + systemPrompt + "\n结合用户偏好和记忆，用用户熟悉的方式回答。"
		}
		return a.llm.Chat(fullSystem, []llm.Message{{Role: "user", Content: userMsg}})
	})
	// Embedding 回调
	a.rag.SetEmbedFn(func(text string) ([]float64, error) {
		return a.llm.Embed(text)
	})
	// Query Rewriter（history-aware + multi-query）。用独立 LLM 闭包不带
	// 记忆前缀，避免改写 prompt 被偏好污染。改写属内部步骤 → 走快模型。
	if cfg.RAGRewriteEnabled && cfg.RAGRewriteNumQueries > 1 {
		rewriteLLM := func(systemPrompt, userMsg string) string {
			return a.llm.ChatFast(systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
		}
		a.rag.SetRewriter(rag.NewLLMRewriter(rewriteLLM, cfg.RAGRewriteNumQueries))
	}
	// Reranker（LLM listwise 精排）——内部步骤 → 走快模型
	if cfg.RAGRerankEnabled {
		rerankLLM := func(systemPrompt, userMsg string) string {
			return a.llm.ChatFast(systemPrompt, []llm.Message{{Role: "user", Content: userMsg}})
		}
		a.rag.SetReranker(rag.NewLLMReranker(rerankLLM, cfg.RAGRerankPreviewLen))
	}
}

// bootstrapConcurrent 启动期 IO 并发：4 项互不依赖的 init 任务并行执行。
//
// 串行总耗时 = PG 全量加载 + Milvus 建表 + ES 建索引 + Docker probe 1.5s
// + Neo4j 5s 验证；并行后压缩到最慢一项的耗时。
//
//   - InitRAGInfra      建 Milvus collection + ES 索引
//   - restoreFromDB     从 PG 恢复长期记忆；偏好和聊天按用户懒加载
//   - restoreRAGFromDB  从 PG 恢复 RAG chunks
//   - initSandbox       Docker daemon 探测 + exec_command 工具注册
//
// 在并发组运行期间同步注册 search_web：
// RegisterTool 持锁串行，与 initSandbox 写 exec_command 不冲突。
//
// 注意：initKnowledgeGraph 依赖 restoreFromDB 完成后的 ltm，必须放在 wg.Wait() 之后单独执行。
func (a *UnifiedAgent) bootstrapConcurrent() {
	cfg := a.cfg
	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); a.repos.ragChunk.Init(cfg.RAGMilvusDim) }()
	go func() { defer wg.Done(); a.restoreFromDB() }()
	go func() { defer wg.Done(); a.restoreRAGFromDB() }()
	go func() { defer wg.Done(); a.initSandbox() }()
	a.registerBuiltinTools()
	wg.Wait()
}

// registerBuiltinTools 注册内置工具：裁剪后只保留 search_web
// （Tavily 优先、LLM 知识降级）。rag_search / 文档工具已下线，
// 知识检索改由「知识库」开关走 rag 模式，文档保存仍由 doc_agent 直接调用 WriteDocument。
func (a *UnifiedAgent) registerBuiltinTools() {
	a.RegisterTool(builtinSearchTool(a.cfg, a.llm))
}

// builtinSearchTool 构造搜索能力，注册和执行使用同一个工具定义。
func builtinSearchTool(cfg *config.APIConfig, client *llm.Client) tool.Tool {
	// Tavily 优先 + LLM 知识降级（替换默认 mock search_web）
	return tool.Tool{
		Name:        "search_web",
		Description: "搜索互联网获取最新信息",
		Parameters: []tool.Param{
			{Name: "query", Type: "string", Description: "搜索关键词", Required: true},
		},
		Execute: func(params map[string]interface{}) (string, error) {
			q, _ := params["query"].(string)
			q = strings.TrimSpace(q)
			if q == "" {
				return "", fmt.Errorf("搜索关键词不能为空")
			}
			if cfg.SearchAPIKey != "" {
				if result, err := toolimpl.TavilySearch(q, cfg.SearchAPIKey, cfg.SearchAPIURL); err == nil {
					return result, nil
				} else {
					logger.L().Warn("Tavily search failed, falling back to LLM", "query", q, "err", err)
				}
			}
			notice := "未获取实时搜索结果，以下回答未经实时核验。"
			if !cfg.IsRealLLM() {
				notice = "未获取实时搜索结果，以下为本地演示回答。"
			}
			return notice + "\n" + client.Chat(
				"实时搜索不可用。请基于已有知识回答，并明确不确定性；不要声称已检索或核验最新信息。",
				[]llm.Message{{Role: "user", Content: "搜索：" + q}},
			), nil
		},
	}
}

// buildPromptCtx 构造 Schema-driven 的 prompt 装配器。
// 必须在 mem / tools / sandbox / kg 全部就绪后调用——RecallSource 依赖
// graphMem，PlannerSource 闭包读 currentTask，ToolStateSource 闭包读 toolsSnapshot。
func (a *UnifiedAgent) buildPromptCtx() *promptCtx {
	pc := &promptCtx{
		taskMem:     promptctx.NewTaskMemBuffer(20),
		toolTracker: promptctx.NewToolStateTracker(10),
	}

	reg := promptctx.NewSourceRegistry()
	// 多租户：把"按 userID 取偏好桶"作为 provider 注入，
	// ProfileSource 在每次 Fetch 时按 q.UserID 拿当前用户的桶
	reg.Register(promptctx.NewProfileSource(a.mem.Pref, a.mem.ltm))
	reg.Register(promptctx.NewPlannerSource(func() *promptctx.PlannerSnapshot {
		return a.currentTask().plannerSnapshot()
	}))
	reg.Register(promptctx.NewTaskMemSource(pc.taskMem))
	reg.Register(promptctx.NewToolStateSource(
		// 持读锁拷贝供 ToolStateSource 装配 prompt：每次调用都拿一致的工具集快照
		a.toolsSnapshot,
		pc.toolTracker,
	))
	reg.Register(promptctx.NewConstraintsSource(sandbox.PolicySnapshot()))
	// RecallSource 优先用图记忆；graphMem 在 initKnowledgeGraph 中就绪
	if a.mem.graphMem != nil {
		reg.Register(promptctx.NewRecallSource(a.mem.graphMem))
	} else {
		reg.Register(promptctx.NewRecallSource(a.mem.ltm))
	}
	pc.assembler = promptctx.NewAssembler(promptctx.DefaultSchemas(), reg)
	return pc
}
