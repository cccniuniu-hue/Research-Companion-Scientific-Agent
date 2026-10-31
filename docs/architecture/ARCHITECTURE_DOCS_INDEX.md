# 架构文档索引

更新于 2026-10-31，按当前 Go/Vue 目录核对，替代旧 agent/infra 顶层布局和固定行数统计。

| 阅读目的 | 文档 |
| --- | --- |
| 入口与职责 | [架构概览](ARCHITECTURE_README.md) |
| 请求与数据流 | [架构图](ARCHITECTURE_MAP.md) |
| 依赖方向 | [依赖说明](DEPENDENCY_GRAPH.md) |
| 上传与 PDF | [文档管线](PDF_INGESTION_PIPELINE.md) |
| 子 Agent 和文档保存 | [子 Agent 说明](SUBAGENT_DOCUMENT_LIBRARY_UPDATE.md) |
| 风险与下一步 | [运行与改进边界](REFACTOR_READINESS.md) |
| 记忆事务与投影 | [一致性说明](../solve/长期记忆更改涉及多库如何保证一致性.md) |

启动、部署与最终验证范围见 [根 README](../../README.md)。历史设计计划位于 `docs/superpowers/plans/`，其中的任务框和目标不是当前完成状态。
