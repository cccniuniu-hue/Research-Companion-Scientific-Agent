# 架构概览

这是一个 Go 后端和 Vue 3 前端组成的研究助手。Go module 仍为 `agi-assistant`。后端单进程装配各领域和后台投影工作者，数据库是外部依赖，不是独立的 Go 微服务。

| 路径 | 职责 |
| --- | --- |
| `cmd/server/main.go` | 校验 JWT/pprof 配置、连接基础设施、装配、监听与关停 |
| `config/` | YAML 与环境变量；进程工作目录下固定读取 config/config.yaml |
| `internal/interfaces/http/` | chi 路由、JWT、管理权限、JSON/SSE 与请求范围 |
| `internal/application/chat/` | Agent 装配、模式路由、上下文、图执行、文档与产物 |
| `internal/application/auth/`、`skill/` | 身份业务和按用户安装的 Skill |
| `internal/application/memoryprojection/`、`memoryreconcile/` | 投影消费与周期修复入队 |
| `internal/domain/` | auth、document、memory、rag、task、runtime、promptctx、tool、skill、sandbox 等领域 |
| `internal/infrastructure/` | LLM、数据库仓储、投影、搜索/MCP、沙箱和事件适配 |
| `web/src/` | API/SSE、Pinia、消息/文档/Skill 组件 |

请求从 HTTP 认证进入应用层，可信用户身份通过 context 传递。ReAct 每个请求持有自己的任务执行状态；全局最近任务视图不等同于用户任务管理 API。数据库连接失败存在降级路径，但注册登录、持久化、检索等能力会受影响；`/readyz` 只表明 Agent 已装配，不证明各后端健康。

内置工具注册与 sandbox 执行能力是两条链路：不能从沙箱存在推断普通用户可直接调用任意 exec_command。Skill 工具保存 Prompt 定义并绑定用户；MCP 工具保持端点信任要求。

## 运行边界

- HTTP 普通用户只能访问自己的认证、会话相关业务、Skill 与活跃记忆。文档/RAG、全局状态、取消、快照、审计和 MCP 注册要求 JWT 加已配置的 `X-Admin-Token`。该令牌来自 `PPROF_ADMIN_TOKEN`，即使 pprof 关闭也用于管理权限；为空时拒绝管理操作。浏览器不内置此令牌。
- 每个共享 Agent 使用一个 HTTP Server 串行访问知识状态。直接调用核心 RAG 时仍需自行同步；同步 SQL/RAG 操作不保证及时取消，也不能撤回已发生的写入。
- 每个投影目标只运行一个活跃消费者，Milvus 保持单写者排序；缺少跨进程 fencing/CAS，不能水平扩容为多实例后宣称 exactly-once。
- 重试、竞速和快照恢复不保证外部副作用幂等。工具执行、模型/MCP 端点、工作目录和容器挂载必须另行建立信任与权限边界。
- PostgreSQL、Milvus、Neo4j、Elasticsearch、Kafka、模型、GitHub、MCP 和 Docker 尚未做真实服务联调。测试中的 mock/loopback 不能替代生产 schema、索引、权限、断线恢复与操作系统信号验收。
