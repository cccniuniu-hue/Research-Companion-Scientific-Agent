Package knowledge 提供文档实体关系的抽取、Neo4j 写入与查询。

types.go 定义实体、关系与带 ChunkID/PGID 的图检索结果。extractor.go 通过注入的模型回调解析 JSON，清理名称并规范七类实体和七类关系；无模型、空文本或无效 JSON 返回空结果。

kgstore.go 的 IndexDocument 为文本块抽取实体关系，用参数化 Cypher 保存文档哈希、块编号和 PostgreSQL 行 ID。该方法同步执行，异步调度由调用方负责。DeleteDocument 尽力删除文档关系及孤立实体；单篇文档的多次写入没有统一事务。

Search 抽取查询实体，用 APOC 扩展子图，默认两跳、最多三跳。按种子命中数与节点度评分，再应用 KGWeight，按 PGID 去重并跳过缺少 PGID 的旧节点。APOC 执行或结果读取失败时降级为直接实体匹配；直接匹配的结果流失败返回空结果。非正结果上限直接跳过模型和数据库调用。

实体按 name 合并，doc_hash、chunk_id 与 pg_id 会被后续写入覆盖，当前只保留最近一次出现的位置。需要跨文档完整召回时，应改为独立的出现记录，不能把当前模型当作完整的多文档来源索引。

Neo4j 不可用时跳过图操作。Client() 允许记忆图共享驱动；文档图使用 Entity 节点，后续 memory/graph 模块使用 Memory 节点，两者的 schema 不同。当前仓库的 RAG 和记忆业务接入仍待后续整合。

当前检查使用合成模型回调与驱动模拟，覆盖写入参数、删除范围、评分去重、跳数边界和读取失败；尚未对真实 Neo4j、APOC 或模型服务进行验证。
