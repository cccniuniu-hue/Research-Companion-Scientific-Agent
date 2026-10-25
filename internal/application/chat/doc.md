# Chat 应用层

`UnifiedAgent` 装配依赖并编排请求、记忆、工具和任务。HTTP、Skill 管理与部署入口按后续计划接入；聊天包可编译不等于完整服务已启动或外部设施已连通。

## 文件与边界

| 文件 | 职责 |
| --- | --- |
| `core_agent.go`、`core_types.go` | 依赖装配、请求和任务类型、可信任务创建 |
| `runtime_process.go` | Process 入口与 prepare / dispatch / finalize |
| `runtime_task.go`、`infra_cancel.go` | 请求取消、任务与快照副本、请求任务范围 |
| `plan_graph.go`、`replanner.go`、`runtime_graph.go` | 计划解析、追加规划、按层并发与竞速 |
| `mode_react.go`、`mode_tool.go` | 图任务与单工具执行、结果合成 |
| `ctx_builder.go`、`ctx_prompt.go`、`ctx_llm.go` | 用户历史、上下文槽位、模型与 token 事件 |
| `mem_stack.go`、`mem_restore.go` | 用户历史和偏好分桶、长期记忆恢复 |
| `mem_commit.go`、`mem_writer.go`、`conflict.go`、`poison.go` | 记忆提交、抽取、版本化冲突处理和内容检查 |
| `infra_repos.go`、`infra_accessor.go`、`infra_router.go`、`infra_status.go` | 仓储聚合、访问器、路由辅助与状态 |
| `tool_registry.go`、`tool_sandbox.go`、`tool_documents.go` | 工具元数据副本、产物沙箱装配、JSON 辅助 |
| `documents.go`、`subagents.go`、`artifact.go` | 文档保存/入库、子任务链路、任务产物 |

内部聚合为 `mem`、`repos`、`tools`、`subagents`、`runtime` 和 `pctx`。工具注册表复制 map 与参数切片，执行函数闭包仍由工具同步。文档对话工具和 exec_command 没有注册为内置聊天工具；默认搜索使用 Tavily 或明确标记的模型降级。

## 执行与身份

请求依次经过 prepare、模式 dispatch 和 finalize。ReAct 从请求 context 创建用户归属，再绑定独立任务范围；上下文查询不借用全局最近任务。每个请求持有私有执行状态，在层间发布任务和图的副本。图结构追加在层间协调，事件回调串行执行；竞速只保留胜出观察，但工具副作用不会回滚，旧 Execute 函数不能被强制终止。

用户归属随快照保留，无归属或外部用户任务不能从读取者补填身份。Cancel、历史快照列表、长期记忆审计和最近任务视图仍是全局管理接口。文档库当前也是全局存储，元数据中的用户/任务/节点用于审计，不能替代访问授权；HTTP 接入必须单独处理这些边界。

## 记忆、检索与子任务

短期历史和偏好按用户懒加载。记忆先经事务仓储提交，再更新缓存；偏好仍是独立写入。规则和模型抽取均检查后再发布，规则不保证穷尽敏感或错误内容。

研究子任务使用知识库或可取消的搜索工具，写作与审阅消费上游结果，文档子任务优先保存准确匹配的 writer_agent 内容。入库保留真实文档与版本 ID，拒绝不匹配版本；文档和检索索引没有统一事务，IndexedCount 描述实际入库情况。RAG 同步接口与全局 Loaded 状态尚无完整的并发与取消契约，部署前需结合真实服务验证。

## 产物

产物按配置根目录、用户和任务分目录，空归属或会被改写的目录段会被拒绝。文件名清理路径分隔符、Windows 特殊字符与保留名，输出检查位于配置根内。取消时停止后续本地写入；本地输出使用独立暂存文件和同目录 rename，失败事件标为 failed。

Docker 可用时可在挂载目录复制，否则沿用本地落盘。尚未验证真实 Docker 挂载或所有平台文件系统行为。路径检查不能消除检查与使用之间的目录替换竞态，根目录应仅由服务进程写入。已经完成的文件、工具和存储副作用不会因后续取消而撤回。
