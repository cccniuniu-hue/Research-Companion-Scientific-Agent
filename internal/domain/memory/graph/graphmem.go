// Package graphmem 图增强长期记忆 —— 在 longterm.LongTerm 之上叠加 Neo4j 图层。
//
// 节点：(:Memory {memory_id, content, importance})
// 边类型：
//   - FOLLOWS      时序相邻（上一条记忆 → 当前）
//   - SIMILAR_TO   语义相似关联（兼容 Store 当前不创建图关系）
//   - CAUSES       因果（LLM 提取，预留未实装）
//   - BELONGS_TO   话题归属（预留未实装）
//
// 核心能力：
//   - Store           兼容写入入口，仅修改本地 LTM；图写入由后续已提交事件投影处理
//   - RecallByFilter  向量召回 + 1-hop 图扩展，发现关联但不直接相似的历史
package graphmem

import (
	"context"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"agi-assistant/internal/domain/knowledge"
	"agi-assistant/internal/domain/memory/longterm"
	pneo4j "agi-assistant/internal/infrastructure/platform/neo4j"
	"agi-assistant/internal/pkg/logger"
)

// goSafe 启动一个带 panic recover 的后台 goroutine。
// graphmem 里的所有图层异步操作（Neo4j Upsert/AddEdge/Delete）都用它包：
// Neo4j 断连后驱动可能 panic，裸 go 会让整个进程崩。
func goSafe(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.L().Error("goroutine panic",
					"task", name, "panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

// GraphMemory 是 LongTerm 的图增强包装层。
//
// 召回读取已有 Neo4j 投影；旧写图辅助方法不在兼容 Store 路径中调用。
// kg 保持构造器兼容，当前没有把记忆自动关联到文档实体。
type GraphMemory struct {
	mu          sync.Mutex // 协调本实例的兼容写入与 ID 读取
	ltm         *longterm.LongTerm
	kg          *knowledge.KGStore // 文档知识图谱（保留以便记忆联动文档实体）
	neo         *pneo4j.Client     // 直接驱动 Memory 节点/边
	simThresh   float64            // 建立 SIMILAR_TO 边的相似度阈值
	prevID      int                // 上一条存入记忆的 ID（用于 FOLLOWS 边）
	protectOnce sync.Once          // 保证 ProtectFn 只挂一次
}

// New 创建图记忆层；neo 为 nil 或不可用时退化为纯 LongTerm
func New(ltm *longterm.LongTerm, kg *knowledge.KGStore, neo *pneo4j.Client, simThreshold float64) *GraphMemory {
	if simThreshold <= 0 {
		simThreshold = 0.7
	}
	return &GraphMemory{
		ltm:       ltm,
		kg:        kg,
		neo:       neo,
		simThresh: simThreshold,
		prevID:    -1,
	}
}

// LTM 暴露底层 LongTerm；读取内容应使用 Snapshot 等持锁方法。
func (gm *GraphMemory) LTM() *longterm.LongTerm { return gm.ltm }

// neoAvailable 报告记忆图所需的 Neo4j 连接是否可用
func (gm *GraphMemory) neoAvailable() bool {
	return gm.neo != nil && gm.neo.Available()
}

// ─────────────────────────────── 记忆图原子操作 ──────────────────────────────

// upsertMemoryNode 插入或更新记忆节点
func (gm *GraphMemory) upsertMemoryNode(memID int, content string, importance float64) {
	if !gm.neoAvailable() {
		return
	}
	ctx := context.Background()
	sess := gm.neo.Session()
	defer sess.Close(ctx)
	_, err := sess.Run(ctx,
		`MERGE (m:Memory {memory_id: $id})
		 SET m.content = $content, m.importance = $importance`,
		map[string]any{"id": int64(memID), "content": content, "importance": importance})
	if err != nil {
		logger.L().Warn("neo4j upsertMemoryNode failed", "memory_id", memID, "err", err)
	}
}

// addMemoryEdge 在两条记忆之间添加关系边
// edgeType: FOLLOWS | SIMILAR_TO | CAUSES | BELONGS_TO
func (gm *GraphMemory) addMemoryEdge(fromID, toID int, edgeType string, weight float64) {
	if !gm.neoAvailable() {
		return
	}
	ctx := context.Background()
	sess := gm.neo.Session()
	defer sess.Close(ctx)
	query := `MATCH (a:Memory {memory_id: $from}), (b:Memory {memory_id: $to})
	          MERGE (a)-[r:` + edgeType + `]->(b)
	          SET r.weight = $weight`
	_, err := sess.Run(ctx, query, map[string]any{
		"from": int64(fromID), "to": int64(toID), "weight": weight,
	})
	if err != nil {
		logger.L().Warn("neo4j addMemoryEdge failed",
			"from", fromID, "to", toID, "err", err)
	}
}

// expandMemoryNeighbors 从种子记忆 ID 出发，按 hops 跳扩展邻居 ID
func (gm *GraphMemory) expandMemoryNeighbors(seedIDs []int, hops int) []int {
	if !gm.neoAvailable() || len(seedIDs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess := gm.neo.Session()
	defer sess.Close(ctx)

	int64Seeds := make([]int64, len(seedIDs))
	for i, id := range seedIDs {
		int64Seeds[i] = int64(id)
	}
	hopStr := "1"
	if hops > 1 {
		hopStr = "1.." + intStr(hops)
	}
	records, err := sess.Run(ctx,
		`MATCH (m:Memory) WHERE m.memory_id IN $ids
		 MATCH (m)-[:FOLLOWS|SIMILAR_TO|CAUSES|BELONGS_TO*`+hopStr+`]-(n:Memory)
		 WHERE NOT n.memory_id IN $ids
		 RETURN DISTINCT n.memory_id AS id`,
		map[string]any{"ids": int64Seeds})
	if err != nil {
		return nil
	}

	var result []int
	for records.Next(ctx) {
		rec := records.Record()
		if rec != nil && len(rec.Values) > 0 {
			if id := toInt(rec.Values[0]); id >= 0 {
				result = append(result, id)
			}
		}
	}
	if records.Err() != nil {
		return nil
	}
	return result
}

// deleteMemoryNode 删除一条记忆节点及其所有边
func (gm *GraphMemory) deleteMemoryNode(memID int) {
	if !gm.neoAvailable() {
		return
	}
	ctx := context.Background()
	sess := gm.neo.Session()
	defer sess.Close(ctx)
	_, err := sess.Run(ctx,
		`MATCH (m:Memory {memory_id: $id}) DETACH DELETE m`,
		map[string]any{"id": int64(memID)})
	if err != nil {
		logger.L().Warn("neo4j deleteMemoryNode failed", "memory_id", memID, "err", err)
	}
}

// getHighCentralityMemoryIDs 在候选列表中找出图中入度 >= threshold 的节点
func (gm *GraphMemory) getHighCentralityMemoryIDs(candidates []int, threshold int) []int {
	if !gm.neoAvailable() || len(candidates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess := gm.neo.Session()
	defer sess.Close(ctx)

	int64IDs := make([]int64, len(candidates))
	for i, id := range candidates {
		int64IDs[i] = int64(id)
	}
	records, err := sess.Run(ctx,
		`MATCH (m:Memory) WHERE m.memory_id IN $ids
		 WITH m, size([(m)<-[]-() | 1]) AS indegree
		 WHERE indegree >= $threshold
		 RETURN m.memory_id AS id`,
		map[string]any{"ids": int64IDs, "threshold": int64(threshold)})
	if err != nil {
		return nil
	}
	var result []int
	for records.Next(ctx) {
		rec := records.Record()
		if rec != nil && len(rec.Values) > 0 {
			if id := toInt(rec.Values[0]); id >= 0 {
				result = append(result, id)
			}
		}
	}
	if records.Err() != nil {
		return nil
	}
	return result
}

// 内部辅助
func toInt(v any) int {
	switch x := v.(type) {
	case int64:
		return int(x)
	case int:
		return x
	}
	return -1
}

func intStr(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + intStr(-n)
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ─────────────────────────────── Store ───────────────────────────────────────

// Store 将记忆写入本地 LTM，不直接创建图节点或关联边。
// 返回：(newItem bool, itemID int)
//   - newItem=false 表示因去重被跳过
//   - itemID 是写入的条目 ID（新增或已有更新后的 ID）
//
// userID 是多租户隔离主键，透传到 ltm。
func (gm *GraphMemory) Store(userID, content string, importance float64, embedding []float64) (bool, int) {
	return gm.StoreClassified(userID, content, importance, embedding, "general", nil, "")
}

// StoreClassified 带 Schema-driven 分类信息的写入。userID 同上。
func (gm *GraphMemory) StoreClassified(userID, content string, importance float64, embedding []float64,
	category string, tags []string, slotHint string) (bool, int) {
	// shortcut: IDs are coordinated only through this instance; synchronize other LTM writers or use committed records.
	gm.mu.Lock()
	defer gm.mu.Unlock()
	added := gm.ltm.StoreClassified(userID, content, importance, embedding, category, tags, slotHint)
	if !added {
		return false, gm.findDedupID(userID, embedding)
	}

	newItem, ok := gm.ltm.LastItem()
	if !ok {
		return true, -1
	}
	newID := newItem.ID

	// Neo4j is a projection. Authoritative runtime writes enqueue graph events
	// transactionally; this compatibility method only updates the local LTM.
	gm.prevID = newID
	return true, newID
}

// linkSimilarEdges 找出与 newItem 语义相近的已有条目，建立 SIMILAR_TO 边
func (gm *GraphMemory) linkSimilarEdges(newItem longterm.Item, newID int) {
	// 扫描最近 50 条（避免全量扫描）—— 通过 Snapshot 拿到一致性快照，
	// 不再直接读 ltm.Items 字段（避免与 LTM 内部并发写产生数据竞争）
	items := gm.ltm.Snapshot()
	start := len(items) - 51
	if start < 0 {
		start = 0
	}
	for i := start; i < len(items)-1; i++ {
		old := items[i]
		if old.ID == newID {
			continue
		}
		if len(old.Embedding) == 0 || len(newItem.Embedding) == 0 {
			continue
		}
		sim := longterm.Cosine(old.Embedding, newItem.Embedding)
		if sim >= gm.simThresh {
			gm.addMemoryEdge(old.ID, newID, "SIMILAR_TO", sim)
		}
	}
}

// findDedupID 匹配 LTM 写入去重的首个有效条目，不跨用户或选择未更新的更近邻。
func (gm *GraphMemory) findDedupID(userID string, embedding []float64) int {
	cfg := gm.ltm.ConsolidationCfg()
	if len(embedding) == 0 || cfg == nil {
		return -1
	}
	if userID == "" {
		userID = "legacy"
	}
	for _, item := range gm.ltm.Snapshot() {
		if item.UserID != userID || item.Quarantined || item.Superseded || item.DeletedAt != nil || len(item.Embedding) != len(embedding) {
			continue
		}
		if longterm.Cosine(embedding, item.Embedding) >= cfg.DedupThreshold {
			return item.ID
		}
	}
	return -1
}

// ─────────────────────────────── Recall ──────────────────────────────────────

// Recall 保留旧签名；缺少 UserID 时返回空结果，请使用 RecallByFilter。
func (gm *GraphMemory) Recall(query string, topK int, queryEmbedding []float64) []longterm.Item {
	return gm.RecallByFilter(query, queryEmbedding, longterm.RecallFilter{TopK: topK, MinScore: 0.4})
}

// RecallByFilter Schema-driven 召回：先按过滤条件做语义召回，再图扩展兜底
func (gm *GraphMemory) RecallByFilter(query string, queryEmbedding []float64, filter longterm.RecallFilter) []longterm.Item {
	seedItems := gm.ltm.RecallByFilter(query, queryEmbedding, filter)

	const graphScore = 0.45
	if !gm.neoAvailable() || len(seedItems) == 0 || filter.MinScore > graphScore {
		return seedItems
	}

	seedIDs := make([]int, len(seedItems))
	for i, item := range seedItems {
		seedIDs[i] = item.ID
	}
	expandedIDs := gm.expandMemoryNeighbors(seedIDs, 1)
	if len(expandedIDs) == 0 {
		return seedItems
	}

	idSet := make(map[int]bool)
	for _, id := range seedIDs {
		idSet[id] = true
	}
	var expanded []longterm.Item
	now := time.Now()
	for _, id := range expandedIDs {
		if idSet[id] {
			continue
		}
		// 通过 LTM 安全访问器查找条目（持读锁，避免直接遍历 .Items）
		if item, ok := gm.ltm.FindByID(id); ok {
			if (!filter.IncludeAllUsers && (filter.UserID == "" || item.UserID != filter.UserID)) ||
				(item.Quarantined && !filter.IncludeQuarantined) ||
				(item.Superseded && !filter.IncludeSuperseded) || item.DeletedAt != nil {
				continue
			}
			// 图扩展条目同样需通过 category 过滤（如果有限制）
			if len(filter.Categories) > 0 && !longterm.ContainsString(filter.Categories, item.Category) {
				continue
			}
			if filter.MaxAgeHours > 0 && now.Sub(item.CreatedAt).Hours() > float64(filter.MaxAgeHours) {
				continue
			}
			matchesTags := true
			for _, tag := range filter.RequireTags {
				if !longterm.ContainsString(item.Tags, tag) {
					matchesTags = false
					break
				}
			}
			if !matchesTags {
				continue
			}
			item.Score = graphScore
			expanded = append(expanded, item)
			idSet[id] = true
		}
	}

	all := append(seedItems, expanded...)
	sort.Slice(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	if filter.TopK > 0 && len(all) > filter.TopK {
		all = all[:filter.TopK]
	}
	return all
}

// ─────────────────────────────── Consolidate ─────────────────────────────────

// installProtectHookOnce 把"图中心度保护"挂进 LongTerm 的 ProtectFn 钩子。
// 幂等：多次调用只生效一次。
func (gm *GraphMemory) installProtectHookOnce() {
	gm.protectOnce.Do(func() {
		cfg := gm.ltm.ConsolidationCfg()
		if cfg == nil {
			return
		}
		cfg.ProtectFn = func(candidates []int) []int {
			if !gm.neoAvailable() || len(candidates) == 0 {
				return nil
			}
			protected := gm.getHighCentralityMemoryIDs(candidates, 3)
			if len(protected) > 0 {
				logger.L().Info("graph centrality protected memories from deletion",
					"protected_count", len(protected))
			}
			return protected
		}
		gm.ltm.SetConsolidationConfig(cfg)
	})
}

// SyncPrevID 在从 DB 恢复记忆后调用，将 prevID 对齐到最新条目
func (gm *GraphMemory) SyncPrevID() {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if id := gm.ltm.LastID(); id >= 0 {
		gm.prevID = id
	}
}

// Len 返回当前记忆条目数（等同 LTM）
func (gm *GraphMemory) Len() int { return gm.ltm.Count() }

// SetConsolidationConfig 代理到 LTM
func (gm *GraphMemory) SetConsolidationConfig(cfg *longterm.ConsolidationConfig) {
	gm.ltm.SetConsolidationConfig(cfg)
}

// NeedConsolidation 代理到 LTM
func (gm *GraphMemory) NeedConsolidation() bool { return gm.ltm.NeedConsolidation() }
