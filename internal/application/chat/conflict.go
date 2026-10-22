// conflict.go — 在新记忆提交后判定与已有候选是否冲突，
// 命中后把旧条目标 Superseded（不删除，便于审计回滚）。
//
// 设计要点：
//   - 仅对 identity / preference / fact 三类启用——episodic（一次性事件）天然共存；
//     policy 类涉及用户给 agent 的硬约束，需要走专门的二次确认路径（暂未做）。
//   - 候选筛选在 domain 层（ConflictCandidates，纯计算），LLM-judge 在 application 层
//     （Chat 调用，不能在 LTM 锁内）。
//   - LLM 不可用 / 候选为空时退化为 no-op，不影响现有 Store 行为。
//   - 判定保守：默认认为不矛盾，只有 LLM 明确说"对立"才标记 superseded。
//     宁可漏判也不误伤——误伤会导致正确的事实被压制。
package chat

import (
	"agi-assistant/internal/domain/memory/consistency"
	"agi-assistant/internal/usercontext"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"agi-assistant/internal/domain/memory/longterm"
	"agi-assistant/internal/infrastructure/llm"
	"agi-assistant/internal/pkg/logger"
)

// 冲突检测仅对这些 category 启用。其它类型默认共存。
var conflictDetectableCategories = map[string]bool{
	"identity":   true,
	"preference": true,
	"fact":       true,
}

// 候选筛选区间：cosine 在 [minSim, maxSim) 之间——与 dedup 阈值（≥maxSim）拼接，
// 形成"重复 / 冲突 / 无关"的三段切分。
const (
	conflictMinSim = 0.75
	conflictMaxSim = 0.95
)

// detectAndResolveConflict 仅替代候选中的同用户记录；模型判定不具备写入权限。
// 判定后重新核对源版本，在同一个记忆事务中保存旧状态、新链接和 Outbox。
func (a *UnifiedAgent) detectAndResolveConflict(ctx context.Context, userID, newContent string, newEmb []float64, category string, newID int) []int {
	if userID == "" || newID <= 0 || !conflictDetectableCategories[category] || len(newEmb) == 0 ||
		a.cfg == nil || !a.cfg.IsRealLLM() || a.mem == nil || a.mem.ltm == nil || a.repos == nil || a.repos.memoryTx == nil || ctx.Err() != nil {
		return nil
	}
	if owner := usercontext.UserIDFromContext(ctx); owner != "" && owner != userID {
		return nil
	}
	eligible := func(r consistency.MemoryRecord) bool {
		return r.UserID == userID && r.Category == category && !r.Quarantined && !r.Superseded && r.DeletedAt == nil
	}
	initial, err := a.authoritativeMemory(ctx, int64(newID))
	if err != nil || !eligible(initial) || initial.Content != newContent {
		return nil
	}
	candidates := a.mem.ltm.ConflictCandidates(userID, newEmb, category, conflictMinSim, conflictMaxSim)
	if len(candidates) > 3 {
		candidates = candidates[:3]
	}
	verdicts := a.llmJudgeConflict(ctx, newContent, candidates)
	if ctx.Err() != nil || len(verdicts) == 0 {
		return nil
	}
	versions := map[int]int64{}
	for _, c := range candidates {
		versions[c.Item.ID] = c.Item.Version
	}
	a.mem.commitMu.Lock()
	defer a.mem.commitMu.Unlock()
	current, err := a.authoritativeMemory(ctx, int64(newID))
	if err != nil || !eligible(current) || current.Version != initial.Version || current.Content != newContent {
		return nil
	}
	plan := consistency.ConsolidationPlan{}
	var oldIDs []int
	now := time.Now().UTC()
	linked := map[int64]bool{}
	for _, id := range current.Supersedes {
		linked[id] = true
	}
	for _, v := range verdicts {
		if !v.IsContradicting {
			continue
		}
		old, err := a.authoritativeMemory(ctx, int64(v.OldID))
		if err != nil || v.OldID == newID || !eligible(old) || versions[v.OldID] <= 0 || old.Version != versions[v.OldID] {
			return nil
		}
		old.Superseded = true
		old.SupersededAt = &now
		plan.Updates = append(plan.Updates, consistency.MemoryUpdate{Record: old, ExpectedVersion: old.Version})
		oldIDs = append(oldIDs, v.OldID)
		if !linked[old.ID] {
			current.Supersedes = append(current.Supersedes, old.ID)
			linked[old.ID] = true
		}
	}
	if len(oldIDs) == 0 {
		return nil
	}
	plan.Updates = append(plan.Updates, consistency.MemoryUpdate{Record: current, ExpectedVersion: current.Version})
	changes, err := a.repos.memoryTx.ApplyConsolidation(ctx, plan)
	if err != nil {
		logger.C(ctx).Warn("conflict transaction failed", "new_id", newID, "err", err)
		return nil
	}
	if err := a.applyCommittedMemory(ctx, changes); err != nil {
		logger.C(ctx).Error("conflict committed but cache unavailable", "new_id", newID, "err", err)
	}
	logger.C(ctx).Info("conflict committed", "new_id", newID, "old_ids", oldIDs, "category", category)
	return oldIDs
}

// conflictVerdict 是 LLM 对单个 (新, 旧) 对的判定结果
type conflictVerdict struct {
	OldID           int    `json:"old_id"`
	IsContradicting bool   `json:"is_contradicting"`
	Reason          string `json:"reason"`
}

// llmJudgeConflict 给 LLM 一次性看新记忆 + 多个候选，让它逐条返回判定。
// 批量调用比逐对调用便宜，且让 LLM 看到全局上下文（候选间的关系也能识别）。
//
// 解析失败 / LLM 不可用时返回空切片——保守策略：宁可漏判，不误伤。
func (a *UnifiedAgent) llmJudgeConflict(
	ctx context.Context,
	newContent string,
	candidates []longterm.ConflictCandidate,
) []conflictVerdict {
	if len(candidates) == 0 || ctx.Err() != nil {
		return nil
	}

	var lines []string
	for _, c := range candidates {
		lines = append(lines, fmt.Sprintf(`{"id":%d,"content":%q}`, c.Item.ID, c.Item.Content))
	}
	candidateBlock := "[\n  " + strings.Join(lines, ",\n  ") + "\n]"

	prompt := `判断"新记忆"是否与每条"旧记忆"语义对立（contradicting）。

判定标准：
- "我喜欢猫" 与 "我不喜欢猫" → 对立
- "用户在北京" 与 "用户搬到了上海" → 对立（新事实取代旧事实）
- "用户喜欢咖啡" 与 "用户喜欢茶" → 不对立（可以同时喜欢两者）
- "用户工作时不喝酒" 与 "用户周末喝酒" → 不对立（场景不同）
- 不确定 / 信息不足 → 不对立（保守）

新记忆：` + newContent + `

旧记忆候选列表：
` + candidateBlock + `

请逐条输出判定，严格 JSON 数组格式：
[{"old_id":<id>,"is_contradicting":true|false,"reason":"<简短理由>"}]
只输出 JSON，不要任何其他内容。`

	raw := a.llm.ChatContext(ctx, "", []llm.Message{{Role: "user", Content: prompt}})
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var out []conflictVerdict
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		logger.C(ctx).Warn("conflict: LLM verdict parse failed", "err", err)
		return nil
	}
	if ctx.Err() != nil {
		return nil
	}
	allowed := map[int]bool{}
	for _, candidate := range candidates {
		allowed[candidate.Item.ID] = true
	}
	seen := map[int]bool{}
	for _, verdict := range out {
		if !allowed[verdict.OldID] || seen[verdict.OldID] {
			return nil
		}
		seen[verdict.OldID] = true
	}
	return out
}
