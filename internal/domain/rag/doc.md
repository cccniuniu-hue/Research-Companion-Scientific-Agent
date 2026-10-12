RAG 引擎流程（顶层文件 rag.go 的 Engine 编排）：

	Ingest:
	  ParentSplitter (大块) ── 子块 RecursiveSplitter ──► PG / Milvus / ES + Neo4j

	Query:
	  Rewriter (history-aware + multi-query)
	      │
	      ▼
	  HybridStore.SearchMulti（每条 query 三路 RRF → 跨查询 RRF 合并）
	      │
	      ▼
	  Reranker (LLM listwise 精排，可关闭)
	      │
	      ▼
	  父块回填 (small-to-big) ──► LLM 合成

文件分组：

	rag.go        Engine 编排
	splitter.go   字符滑窗 + 递归切分（Markdown 感知 + 代码块保护）
	hybrid.go     HybridStore：Milvus 语义 + ES BM25 + Neo4j 图 + RRF 融合
	rewriter.go   Rewriter 接口 + LLM 改写实现
	reranker.go   Reranker 接口 + LLM listwise 实现

学习入口建议：先看 splitter（理解父子块结构），再看 hybrid（RRF 融合是核心），
最后看 rewriter / reranker（增强层，可关闭）。

实现边界：

- 父块大小为 max(600, ChunkSize * 4)，子块按 ChunkSize 切分并携带父块原文。切分优先使用段落和句子边界；长句仍可按字符切开，完整代码块可以超过目标大小，重叠内容也会增加块长度。
- PG 是原文与元数据的来源。写入每个子块后尽力更新 ES 和 Milvus，再异步抽取并写入图谱；这些后端之间没有原子事务。向量生成失败时保留 PG 文本和关键词索引，IndexedCount 只表示 PG 成功写入数量。
- 检索模式依据 PG、Milvus 和 ES 可用性决定。图检索仅在 hybrid 路径且两路搜索成功时参与融合；缺少向量回调或向量调用失败时降级为关键词检索。跨查询按真实 PGID 合并，保留相同文本的独立来源。
- 查询改写保留原问题并限制查询数量；没有有效模型输出时使用原问题。重排只接受候选范围内的 0~10 评分，没有有效评分时保持检索顺序，部分评分保留未评分候选。
- 新文档摄入失败不会清空已有 Loaded 状态。主仓储删除失败会终止后续删除；删除后的状态刷新失败返回错误并保留最近状态，不代表此前删除已回滚。Loaded 的并发读写需要调用方同步。
- 包测试使用合成数据、仓储和模型回调，未验证真实数据库、图谱服务、模型服务或应用 HTTP 联通。
