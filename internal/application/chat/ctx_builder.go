// context_builder.go — 装配每轮推理需要的提示前缀和对话历史。
//
//   - buildContextPrefix   调用 schema-driven ContextAssembler 输出 system prompt 前缀
//   - buildSystemPrompt    把记忆前缀拼到 base prompt 之前
//   - buildHistoryMessages 把 STM 转成 LLM 消息列表
//   - recentHistoryForRAG  把 STM 截断成 RAG Rewriter 用的最小历史结构
//   - filterTools          按名称白名单过滤工具集
package chat

import (
	"agi-assistant/internal/domain/promptctx"
	"agi-assistant/internal/domain/rag"
	"agi-assistant/internal/domain/tool"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/usercontext"
	"context"
)

func (a *UnifiedAgent) buildContextPrefix(ctx context.Context, query string, mode string) string {
	if a.pctx == nil || a.pctx.assembler == nil || ctx.Err() != nil {
		return ""
	}
	emb, _ := a.llm.EmbedContext(ctx, query)
	userID := usercontext.UserIDFromContext(ctx)
	taskID := ""
	if mode == "react" && userID != "" && a.runtime != nil {
		if t := a.currentTask(); t != nil && t.UserID == userID {
			taskID = t.TaskID
		}
	}
	return a.pctx.assemble(ctx, promptctx.Query{
		Text:      query,
		Embedding: emb,
		TaskID:    taskID,
		Mode:      mode,
		// 多租户隔离主键——透传到 ProfileSource / RecallSource，
		// 让它们在 LTM 中只取本用户的条目。空字符串等价于"未登录"，下游会跳过。
		UserID: userID,
	})
}

// buildSystemPrompt 构建带记忆前缀的 system prompt
func (a *UnifiedAgent) buildSystemPrompt(memPrefix, basePrompt string) string {
	if memPrefix == "" {
		return basePrompt
	}
	return memPrefix + "\n\n" + basePrompt
}

// buildHistoryMessages 将 STM 历史消息转为 LLM 消息列表（末尾附上当前 user query）。
// userID 为空（未登录）时只返回当前 query，不注入任何历史——避免泄漏其他用户上下文。
func (a *UnifiedAgent) buildHistoryMessages(userID, query string) []llm.Message {
	var msgs []llm.Message
	if stm := a.mem.STM(userID); stm != nil {
		// 通过 Snapshot 拿到一致性副本，避免遍历期间 Add 并发改写底层切片
		for _, m := range stm.Snapshot() {
			if m.Role == "user" || m.Role == "assistant" {
				msgs = append(msgs, llm.Message{Role: m.Role, Content: m.Content})
			}
		}
	}
	// 只有末尾的用户消息才可能是已经写入 STM 的本次请求。
	if len(msgs) == 0 || msgs[len(msgs)-1].Role != "user" || msgs[len(msgs)-1].Content != query {
		msgs = append(msgs, llm.Message{Role: "user", Content: query})
	}
	return msgs
}

// recentHistoryForRAG 把 STM 转成 RAG Rewriter 需要的最小结构。
// 取最近 6 条（3 轮）足够 history-aware 改写消除指代，再多反而拖长 prompt。
// userID 为空时返回 nil（不带历史改写）——RAG 路径会退化到原始 query。
func (a *UnifiedAgent) recentHistoryForRAG(userID string) []rag.HistoryMessage {
	stm := a.mem.STM(userID)
	if stm == nil {
		return nil
	}
	snap := stm.Snapshot()
	const maxTurns = 6
	out := make([]rag.HistoryMessage, 0, min(len(snap), maxTurns))
	for _, m := range snap {
		if m.Role != "user" && m.Role != "assistant" {
			continue
		}
		out = append(out, rag.HistoryMessage{Role: m.Role, Content: m.Content})
	}
	if len(out) > maxTurns {
		out = out[len(out)-maxTurns:]
	}
	return out
}

// filterTools 按名称列表过滤可用工具集（toolRegistry 内部持锁）
func (a *UnifiedAgent) filterTools(names []string) map[string]tool.Tool {
	return a.tools.filter(names)
}
