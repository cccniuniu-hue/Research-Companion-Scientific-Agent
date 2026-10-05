# Research Companion Scientific Agent

面向研究与资料整理场景的智能助手，使用 Go 构建后端，Vue 3 构建交互界面。

项目围绕流式对话、文档检索、会话记忆、工具调用与多步骤任务执行逐步完善。技术路线采用 PostgreSQL 持久化、Milvus 向量检索、Elasticsearch 全文检索和 Neo4j 知识图谱。

## 当前阶段

当前完成工程基础、用户请求上下文、结构化日志、配置加载、用户认证，以及 PostgreSQL 表结构、Kafka 事件发布、会话历史和任务快照仓储。HTTP 接口与其余业务模块仍在逐步整合，这个阶段尚不能启动完整应用。

后端模块路径为 `agi-assistant`，后续内部包继续沿用该路径。

## 用户认证

认证模块提供用户名与密码长度校验、bcrypt 密码摘要、HS256 JWT 签发与验证，以及注册和登录服务。用户不存在与密码不匹配返回统一错误；JWT 签发器拒绝少于 32 字节的密钥。

PostgreSQL 用户仓储提供用户创建、查询与登录时间更新。领域和服务测试使用固定虚构凭证与内存仓储；数据库仓储目前只做编译检查，尚未进行真实 PostgreSQL 联通验证。

## 基础持久化

已提供 PostgreSQL 连接与启动期表结构初始化、会话历史仓储和任务快照仓储，以及 Kafka 事件发布。会话历史按用户保存和查询，任务快照按任务 ID 更新；Kafka 未配置或连接不可用时，事件发布降级为日志记录。

表结构测试检查记忆版本字段和 Outbox DDL 必需项；Kafka、会话历史和任务快照模块目前做编译检查。尚未执行真实 PostgreSQL 表结构初始化或 Kafka 消息收发验证。

## 技术路线

| 模块 | 技术与目标 |
| --- | --- |
| 后端 | Go 1.24、chi、JWT、SSE |
| 模型调用 | OpenAI-compatible API、流式输出、Embedding |
| 检索 | Milvus、Elasticsearch、Neo4j、混合召回与重排 |
| 记忆 | 短期会话、长期记忆、用户偏好、事务与异步投影 |
| 任务执行 | ReAct、任务 DAG、子 Agent、工具与沙箱 |
| 前端 | Vue 3、Vite、Pinia |

## 环境要求

后端使用 Go 1.24.1，工具链固定为 Go 1.24.13。前端与部署模块整合后补充启动说明。

真实 API Key、JWT 密钥和本地运行数据通过本地配置或环境变量提供。

## 本地配置

`config/config.yaml` 通过 `LLM_API_KEY`、`EMBEDDING_API_KEY`、`TAVILY_API_KEY` 读取模型和搜索密钥；未设置时保持为空。`JWT_SECRET`、`PPROF_ADMIN_TOKEN`、`GITHUB_TOKEN` 也使用环境变量。可在已忽略的本地 `.env` 中配置；已有进程环境变量优先。

数据库地址和密码为本地开发示例，部署时应在本地覆盖。日志模块已验证请求 ID、用户 ID、空上下文与日志级别降级行为。
