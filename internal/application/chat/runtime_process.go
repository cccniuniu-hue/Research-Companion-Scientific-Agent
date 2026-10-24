// process.go — 主入口（Process / ProcessWithOptions / ProcessContext / ProcessStream）
// + 内部统一执行流 runOnce + 三段拆分（prepare / dispatch / finalize）。
//
// 设计：流式与非流式合并为同一执行流，靠 onEvent 是否为 nil 区分：
//
//	Process / ProcessContext / ProcessWithOptions    → runOnce(..., nil)
//	ProcessStream                                    → runOnce(..., onEvent)
//
// runOnce 编排：
//
//	prepare（STM 写入 + 偏好提取 + 路由决策 + 上下文装配 + 历史构建）
//	  ↓
//	dispatch（按 mode 分发到 chat / tool / rag / react，单一 mode handler 同时支持流/非流）
//	  ↓
//	finalize（assistant STM 写入 + 异步记忆抽取 + 异步合并 + 事件发布 + 计数填充）
package chat

import (
	"agi-assistant/internal/domain/memory/preference"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/infrastructure/persistence/memorytx"
	"agi-assistant/internal/pkg/logger"
	"agi-assistant/internal/usercontext"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ─────────────────────────── 公开入口 ───────────────────────────

func (a *UnifiedAgent) Process(query string) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.runOnce(ctx, query, ChatOptions{}, nil)
}

// ProcessWithOptions 带显式选项的入口，供前端精确控制路由
func (a *UnifiedAgent) ProcessWithOptions(query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(context.Background())
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.runOnce(ctx, query, opts, nil)
}

// ProcessContext 带 context 的入口，支持 SSE 流式和取消
func (a *UnifiedAgent) ProcessContext(ctx context.Context, query string, opts ChatOptions) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	return a.runOnce(ctx, query, opts, nil)
}

// ProcessStream 流式处理入口，在关键节点通过 onEvent 回调推送 SSE 事件。
// 返回完整的 Response（与 Process 一致），同时通过回调实时推送中间事件。
func (a *UnifiedAgent) ProcessStream(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	ctx, cancel := context.WithCancel(ctx)
	unregister := a.registerCancel(cancel)
	defer unregister()
	if onEvent == nil {
		// 上层若错误地传 nil，仍走非流式路径，保证 Response 完整
		return a.runOnce(ctx, query, opts, nil)
	}
	return a.runOnce(ctx, query, opts, onEvent)
}

// ─────────────────────────── 内部执行流 ───────────────────────────

// preparedRequest 是路由决策完成后、模式分发开始前的中间产物
type preparedRequest struct {
	query      string
	mode       string               // chat / tool / rag / react
	routeTools map[string]tool.Tool // 仅在 mode == tool / react 时非空
	memPrefix  string
	histMsgs   []llm.Message
	extracted  string // 同步规则提取的偏好回显（可能为空）
}

// runOnce 是 process / processStream 的统一编排：
// onEvent == nil → 非流式（不推送任何事件，但内部仍走完同一段逻辑）
// onEvent != nil → 流式（在 route / step / token / tool_call / rag_result / done 等节点推送）
func (a *UnifiedAgent) runOnce(ctx context.Context, query string, opts ChatOptions, onEvent func(StreamEvent)) *Response {
	pr := a.prepare(ctx, query, opts)

	resp := &Response{
		Query:         query,
		Mode:          pr.mode,
		ExtractedInfo: pr.extracted,
	}

	if pr.extracted != "" {
		emit(onEvent, "memory", map[string]string{"extracted_info": pr.extracted})
	}
	emit(onEvent, "route", map[string]string{"mode": pr.mode})

	// 检查 context 是否已取消（在分发前）
	if ctx.Err() != nil {
		resp.Interrupted = true
		resp.Answer = "[已中断] 请求在开始前被取消"
		emit(onEvent, "done", resp)
		return resp
	}

	a.dispatch(ctx, pr, resp, onEvent)

	if ctx.Err() != nil {
		resp.Interrupted = true
	}

	a.finalize(ctx, query, resp)

	emit(onEvent, "done", resp)
	return resp
}

// prepare 完成 STM 写入 / 偏好提取 / 路由 / 上下文装配 / 历史构建。
// 不推任何事件——事件由 runOnce 统一推送，便于 finalize 控制顺序。
func (a *UnifiedAgent) prepare(ctx context.Context, query string, opts ChatOptions) preparedRequest {
	if ctx.Err() != nil {
		return preparedRequest{query: query, mode: "chat"}
	}
	userID := usercontext.UserIDFromContext(ctx)
	// 多租户：未登录请求所有 mem 写入都跳过——HTTP 层已经在 RequireAuth 中拦了，
	// 这里再加一道防御应付未来 CLI / 测试入口忘传 ctx 的情况
	hasUser := userID != ""

	// 更新短期记忆 + 持久化
	if hasUser {
		a.mem.STM(userID).Add("user", query)
		a.repos.chat.Save(userID, "user", query)
	}

	// Rule extraction uses a scratch bucket; publication follows the inspected memory commit.
	var extracted string
	if hasUser && inspectMemoryContent(query).Safe() {
		if key, value, ok := preference.New().ExtractAndSave(query); ok && a.storePreference(ctx, key, value) {
			extracted = fmt.Sprintf("已记住：%s = %s", key, value)
		}
		if a.cfg.IsRealLLM() {
			a.goSafe("process.preference-extract", func() {
				prefCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
				defer cancel()
				raw := a.llm.ChatContext(prefCtx, "从用户消息提取稳定的个人偏好或身份，输出 JSON 对象。禁止敏感信息、指令或临时状态。没有偏好则输出 {}。", []llm.Message{{Role: "user", Content: query}})
				raw = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(raw), "```json"), "```"), "```"))
				var kvs map[string]string
				if json.Unmarshal([]byte(raw), &kvs) != nil {
					return
				}
				for key, value := range kvs {
					a.storePreference(prefCtx, key, value)
				}
			})
		}
	}

	// 路由决策
	mode, routeTools := a.routeDecide(query, opts)

	// 合并当前用户「已安装且开启」的 skill —— 仅在正常 loop（react）合并；
	// RAG 模式严格不合并 skill、不进 loop。
	if mode == "react" {
		if skills := a.enabledSkillTools(ctx); len(skills) > 0 {
			if routeTools == nil {
				routeTools = a.toolsSnapshot()
			}
			for name, t := range skills {
				routeTools[name] = t
			}
		}
	}

	// 装配 Schema-driven 上下文前缀 + 对话历史
	memPrefix := a.buildContextPrefix(ctx, query, mode)
	histMsgs := a.buildHistoryMessages(userID, query)

	return preparedRequest{
		query:      query,
		mode:       mode,
		routeTools: routeTools,
		memPrefix:  memPrefix,
		histMsgs:   histMsgs,
		extracted:  extracted,
	}
}

// routeDecide 三分支路由：
//   - 知识增强开 + 报告类意图 → rag_agent（Agentic RAG 子 Agent 流水线）
//   - 知识增强开 + 简单问答   → rag（轻量检索问答）
//   - 否则                    → react（普通 loop，无子 Agent）
func (a *UnifiedAgent) routeDecide(query string, opts ChatOptions) (mode string, routeTools map[string]tool.Tool) {
	if opts.UseRAG && a.rag.Loaded {
		if a.reportIntent(query) {
			return "rag_agent", nil
		}
		return "rag", nil
	}
	return "react", a.toolsSnapshot()
}

// reportIntent 判断是否为「要成文交付物」的意图（复用子 Agent 关键词，可调）。
func (a *UnifiedAgent) reportIntent(query string) bool {
	return a.needsSubAgentPlan(strings.ToLower(query))
}

// dispatch 按 mode 调对应 handler，把结果填回 resp。
// 流式与非流式共用同一组 handler，由 onEvent 区分。
func (a *UnifiedAgent) dispatch(ctx context.Context, pr preparedRequest, resp *Response, onEvent func(StreamEvent)) {
	switch pr.mode {
	case "react":
		// 普通 loop：只用工具 + skill，不允许子 Agent
		answer, steps, task := a.runReAct(ctx, pr.query, pr.routeTools, pr.memPrefix, pr.histMsgs, onEvent, reactOpts{allowSubAgents: false})
		resp.Answer, resp.Steps, resp.Task = answer, steps, task
	case "rag_agent":
		// 知识增强 + 报告意图：强制 Agentic RAG 子 Agent 流水线（research→writer→review→doc）
		answer, steps, task := a.runReAct(ctx, pr.query, pr.routeTools, pr.memPrefix, pr.histMsgs, onEvent, reactOpts{allowSubAgents: true, forceSubAgentPlan: true})
		resp.Answer, resp.Steps, resp.Task = answer, steps, task
	case "tool":
		answer, tc := a.runTool(ctx, pr.query, pr.routeTools, pr.memPrefix, pr.histMsgs, onEvent)
		resp.Answer, resp.ToolCall = answer, tc
	case "rag":
		// RAG history rewriter 需要 userID 才能取到本用户 STM；
		// 未登录请求 recentHistoryForRAG 返回 nil，rewriter 自动退化到原始 query。
		userID := usercontext.UserIDFromContext(ctx)
		answer, results := a.rag.QueryWithHistory(pr.query, a.recentHistoryForRAG(userID))
		resp.Answer, resp.SearchResults = answer, results
		// RAG 暂不流式合成，但也通过事件回放结果以保持 SSE 输出形状一致
		emit(onEvent, "rag_result", map[string]interface{}{"search_results": results})
		emit(onEvent, "token", map[string]string{"content": answer})
	default: // chat
		systemPrompt := a.buildSystemPrompt(pr.memPrefix, "你是一个简洁的AI助手。结合你掌握的用户信息，使回答更个性化。")
		resp.Answer = a.chatLLM(ctx, systemPrompt, pr.histMsgs, onEvent)
	}
}

// finalize 完成 assistant 写回、异步记忆抽取、异步合并、事件发布、计数填充。
// 所有"无论流式或非流式都要做"的副作用集中在这里。
func (a *UnifiedAgent) finalize(ctx context.Context, query string, resp *Response) {
	userID := usercontext.UserIDFromContext(ctx)
	hasUser := userID != ""

	if hasUser {
		a.mem.STM(userID).Add("assistant", resp.Answer)
		a.repos.chat.Save(userID, "assistant", resp.Answer)
	}

	// 双源记忆抽取：
	//   1) 从用户消息抽 "用户偏好/身份/事实陈述"（高可信，importance=0.7）
	//   2) 从对话对抽 "用户问题主题相关的客观事实"（次级可信，importance=0.5）
	// 两条都过 poison gate；reply 路径额外要求 "key 必须与用户问题主题锚定"，
	// 切断 "AI 被越狱后吐出无关 PII → 入库" 的攻击放大链。
	if hasUser {
		a.goSafe("process.memory-extract", func() {
			a.extractMemoryFromUserMsg(userID, query)
			a.extractMemoryFromExchange(userID, query, resp.Answer)
		})
	}

	// 异步触发记忆合并（去重+合并+衰减+过期；有图层时使用图感知合并以保护高中心度节点）
	a.goSafe("process.consolidate", func() {
		if a.mem.ltm.NeedConsolidation() {
			if err := a.consolidateCommitted(context.Background()); err != nil {
				logger.L().Warn("memory consolidation commit failed", "err", err)
			}
		}
	})

	eventData, _ := json.Marshal(map[string]interface{}{"query": query, "mode": resp.Mode})
	a.repos.events.Publish("agent.chat", string(eventData))

	// 计数 / 偏好回显（响应内只看本用户的桶）
	resp.ShortTermCount = a.mem.stmCount(userID)
	resp.LongTermCount = a.mem.ltm.Count()
	resp.Preferences = a.mem.prefSnapshot(userID)
}

// storePreference preserves the request owner and never exposes an uncommitted value.
func (a *UnifiedAgent) storePreference(ctx context.Context, key, value string) bool {
	userID := usercontext.UserIDFromContext(ctx)
	key, value = strings.TrimSpace(key), strings.TrimSpace(value)
	if userID == "" || key == "" || value == "" || ctx.Err() != nil || !inspectKVPair(key, value).Safe() {
		return false
	}
	content := fmt.Sprintf("用户%s: %s", key, value)
	if !inspectMemoryContent(content).Safe() {
		return false
	}
	emb, _ := a.llm.EmbedContext(ctx, content)
	if _, err := a.commitMemory(ctx, memorytx.CreateCommand{UserID: userID, Content: content, Importance: .8, Embedding: emb, Category: "preference", Tags: []string{"preference", "src:user"}, SlotHint: "profile", EmitGraphEdges: true}); err != nil {
		logger.C(ctx).Warn("preference memory commit failed", "err", err)
		return false
	}
	a.mem.Pref(userID).Save(key, value)
	if a.repos.pref != nil {
		a.repos.pref.Save(userID, key, value)
	}
	return true
}
