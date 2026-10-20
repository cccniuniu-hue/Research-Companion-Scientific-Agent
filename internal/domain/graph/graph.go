// graph.go — 任务图（DAG）领域模型。
//
// 将 ReAct 从串行链路升级为可并行、可竞速的图结构调度。
// 核心概念：Node（执行单元）+ DependsOn（依赖边）+ RaceGroup（竞速组）。
// TaskGraph 计算拓扑层级，供后续运行时按层调度。
package graph

import (
	"fmt"
	"sort"
	"strings"
)

// ─────────────────────────────── 类型常量 ──────────────────────────────────

type NodeID string

type NodeType string // "tool" | "sub_agent" | "think" | "aggregate"

const (
	NodeTypeTool      NodeType = "tool"
	NodeTypeSubAgent  NodeType = "sub_agent"
	NodeTypeThink     NodeType = "think"
	NodeTypeAggregate NodeType = "aggregate"
)

type NodeStatus string // "pending" | "running" | "done" | "failed" | "skipped" | "cancelled"

const (
	StatusPending   NodeStatus = "pending"
	StatusRunning   NodeStatus = "running"
	StatusDone      NodeStatus = "done"
	StatusFailed    NodeStatus = "failed"
	StatusSkipped   NodeStatus = "skipped"
	StatusCancelled NodeStatus = "cancelled"
)

// ─────────────────────────────── Node ──────────────────────────────────────

// Node 是任务图中的执行单元，对应一次工具调用、推理或数据聚合。
type Node struct {
	ID         NodeID            `json:"id"`
	Type       NodeType          `json:"type"`
	Name       string            `json:"name"`                 // Planner 给出的 reason
	ToolName   string            `json:"tool_name,omitempty"`  // 工具节点必填
	AgentName  string            `json:"agent_name,omitempty"` // 子 Agent 节点必填
	Goal       string            `json:"goal,omitempty"`       // 子 Agent 任务目标
	Params     map[string]string `json:"params,omitempty"`
	DependsOn  []NodeID          `json:"depends_on"`           // 入边：依赖哪些节点
	RaceGroup  string            `json:"race_group,omitempty"` // 空=独立；同 group=竞速
	Status     NodeStatus        `json:"status"`
	Result     string            `json:"result,omitempty"`
	Error      string            `json:"error,omitempty"`
	RetryCount int               `json:"retry_count"`
}

// ─────────────────────────────── TaskGraph ──────────────────────────────────

// TaskGraph 是有向无环图，通过拓扑排序决定节点执行顺序。
type TaskGraph struct {
	Nodes      map[NodeID]*Node    `json:"nodes"`
	AdjList    map[NodeID][]NodeID `json:"adj_list"`  // node → 下游节点
	InDegree   map[NodeID]int      `json:"in_degree"` // 入度表
	invalidErr error               // 构造时无法由 map 保留的输入错误（如重复 ID）
}

// NewTaskGraph 从节点列表构建图，自动计算邻接表和入度。
func NewTaskGraph(nodes []*Node) *TaskGraph {
	tg := &TaskGraph{Nodes: make(map[NodeID]*Node, len(nodes)), AdjList: map[NodeID][]NodeID{}, InDegree: map[NodeID]int{}}
	for _, n := range nodes {
		if n == nil || strings.TrimSpace(string(n.ID)) == "" {
			tg.invalidErr = fmt.Errorf("task graph contains a nil node or blank ID")
			continue
		}
		if _, ok := tg.Nodes[n.ID]; ok {
			tg.invalidErr = fmt.Errorf("task graph contains duplicate node %s", n.ID)
			continue
		}
		copy := cloneNode(n)
		copy.Status = StatusPending
		tg.Nodes[n.ID] = copy
		tg.AdjList[n.ID] = nil
		tg.InDegree[n.ID] = 0
	}
	for id, n := range tg.Nodes {
		for _, dep := range n.DependsOn {
			if _, ok := tg.Nodes[dep]; ok {
				tg.AdjList[dep] = append(tg.AdjList[dep], id)
				tg.InDegree[id]++
			}
		}
	}
	return tg
}
func cloneNode(n *Node) *Node {
	copy := *n
	copy.DependsOn = nil
	seen := map[NodeID]bool{}
	for _, dep := range n.DependsOn {
		if !seen[dep] {
			seen[dep] = true
			copy.DependsOn = append(copy.DependsOn, dep)
		}
	}
	if n.Params != nil {
		copy.Params = make(map[string]string, len(n.Params))
		for k, v := range n.Params {
			copy.Params[k] = v
		}
	}
	return &copy
}

// TopologicalLevels uses structural dependencies, independent of the mutable
// execution in-degrees. Returned levels are fresh and sorted on every call.
func (tg *TaskGraph) TopologicalLevels() ([][]NodeID, error) {
	if tg.invalidErr != nil {
		return nil, tg.invalidErr
	}
	degrees := make(map[NodeID]int, len(tg.Nodes))
	adj := map[NodeID][]NodeID{}
	for id, n := range tg.Nodes {
		if n == nil || id != n.ID || strings.TrimSpace(string(id)) == "" {
			return nil, fmt.Errorf("task graph contains invalid node %s", id)
		}
		degrees[id] = 0
		seen := map[NodeID]bool{}
		for _, dep := range n.DependsOn {
			if tg.Nodes[dep] == nil {
				return nil, fmt.Errorf("node %s depends on nonexistent node %s", id, dep)
			}
			if !seen[dep] {
				seen[dep] = true
				degrees[id]++
				adj[dep] = append(adj[dep], id)
			}
		}
	}
	var levels [][]NodeID
	processed := 0
	for {
		var ready []NodeID
		for id, d := range degrees {
			if d == 0 {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			break
		}
		sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
		levels = append(levels, ready)
		processed += len(ready)
		for _, id := range ready {
			degrees[id] = -1
			for _, next := range adj[id] {
				degrees[next]--
			}
		}
	}
	if processed != len(tg.Nodes) {
		return nil, fmt.Errorf("task graph has cycle: processed %d/%d nodes", processed, len(tg.Nodes))
	}
	return levels, nil
}
func (tg *TaskGraph) ReadyNodes() []NodeID {
	var ready []NodeID
	for id, d := range tg.InDegree {
		if n := tg.Nodes[id]; n != nil && d == 0 && n.Status == StatusPending {
			ready = append(ready, id)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	return ready
}

// MarkDone consumes a ready node once. Failed/skipped/cancelled terminal states
// remain available for audit while their completion can unblock downstream work.
func (tg *TaskGraph) MarkDone(id NodeID) []NodeID {
	n := tg.Nodes[id]
	degree, ok := tg.InDegree[id]
	if n == nil || !ok || degree != 0 {
		return nil
	}
	tg.InDegree[id] = -1
	if n.Status == StatusPending || n.Status == StatusRunning {
		n.Status = StatusDone
	}
	var ready []NodeID
	for _, next := range tg.AdjList[id] {
		if tg.InDegree[next] > 0 {
			tg.InDegree[next]--
			if node := tg.Nodes[next]; node != nil && tg.InDegree[next] == 0 && node.Status == StatusPending {
				ready = append(ready, next)
			}
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	return ready
}
func (tg *TaskGraph) RaceGroups() map[string][]NodeID {
	groups := map[string][]NodeID{}
	for id, n := range tg.Nodes {
		if n != nil && n.RaceGroup != "" {
			groups[n.RaceGroup] = append(groups[n.RaceGroup], id)
		}
	}
	for _, ids := range groups {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	return groups
}
func (tg *TaskGraph) Validate() error { _, err := tg.TopologicalLevels(); return err }

// SetNodeStatus 更新已有节点状态；并发访问需要调用方同步。
func (tg *TaskGraph) SetNodeStatus(id NodeID, status NodeStatus) {
	if n, ok := tg.Nodes[id]; ok && n != nil {
		n.Status = status
	}
}

// SetNodeResult 更新已有节点结果。
func (tg *TaskGraph) SetNodeResult(id NodeID, result string) {
	if n, ok := tg.Nodes[id]; ok && n != nil {
		n.Result = result
	}
}

// SetNodeError 更新已有节点错误。
func (tg *TaskGraph) SetNodeError(id NodeID, errMsg string) {
	if n, ok := tg.Nodes[id]; ok && n != nil {
		n.Error = errMsg
	}
}

// SetNodeRetryCount 更新已有节点重试计数。
func (tg *TaskGraph) SetNodeRetryCount(id NodeID, count int) {
	if n, ok := tg.Nodes[id]; ok && n != nil {
		n.RetryCount = count
	}
}

// SuccessfulResults 返回所有成功节点的结果列表（用于 Generator LLM）。
func (tg *TaskGraph) SuccessfulResults() []string {
	ids := make([]NodeID, 0, len(tg.Nodes))
	for id := range tg.Nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var results []string
	for _, id := range ids {
		n := tg.Nodes[id]
		if n != nil && n.Status == StatusDone && n.Result != "" {
			name := n.ToolName
			if n.Type == NodeTypeSubAgent {
				name = n.AgentName
			}
			results = append(results, fmt.Sprintf("[%s] %s", name, n.Result))
		}
	}
	return results
}

// AddNodes registers a whole batch before checking dependencies, including
// forward references. Invalid batches leave the graph unchanged; existing IDs
// keep their prior results. The caller owns synchronization for graph access.
func (tg *TaskGraph) AddNodes(nodes []*Node) []NodeID {
	var all []*Node
	for _, n := range tg.Nodes {
		all = append(all, n)
	}
	seen := map[NodeID]bool{}
	var added []NodeID
	for _, n := range nodes {
		if n == nil || strings.TrimSpace(string(n.ID)) == "" {
			return nil
		}
		if _, exists := tg.Nodes[n.ID]; exists || seen[n.ID] {
			continue
		}
		seen[n.ID] = true
		added = append(added, n.ID)
		all = append(all, n)
	}
	if len(added) == 0 {
		return nil
	}
	if tg.invalidErr != nil {
		return nil
	}
	candidate := NewTaskGraph(all)
	if err := candidate.Validate(); err != nil {
		return nil
	}
	if tg.Nodes == nil {
		tg.Nodes = map[NodeID]*Node{}
	}
	if tg.AdjList == nil {
		tg.AdjList = map[NodeID][]NodeID{}
	}
	if tg.InDegree == nil {
		tg.InDegree = map[NodeID]int{}
	}
	for _, id := range added {
		tg.Nodes[id] = candidate.Nodes[id]
		tg.AdjList[id] = nil
		tg.InDegree[id] = 0
	}
	for _, id := range added {
		for _, dep := range tg.Nodes[id].DependsOn {
			tg.AdjList[dep] = append(tg.AdjList[dep], id)
			if tg.InDegree[dep] >= 0 {
				tg.InDegree[id]++
			}
		}
	}
	return added
}

// Summary 返回图的可读摘要。
func (tg *TaskGraph) Summary() string {
	levels, err := tg.TopologicalLevels()
	if err != nil {
		return fmt.Sprintf("graph invalid: %v", err)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "graph: %d nodes, %d levels\n", len(tg.Nodes), len(levels))
	for i, level := range levels {
		var names []string
		for _, id := range level {
			n := tg.Nodes[id]
			name := n.ToolName
			if n.Type == NodeTypeSubAgent {
				name = n.AgentName
			}
			names = append(names, fmt.Sprintf("%s(%s)", n.ID, name))
		}
		fmt.Fprintf(&b, "  L%d: %s\n", i, strings.Join(names, ", "))
	}
	return b.String()
}
