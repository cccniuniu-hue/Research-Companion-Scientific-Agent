# Research Companion Scientific Agent

面向研究与资料整理场景的智能助手，使用 Go 构建后端，Vue 3 构建交互界面。

项目围绕流式对话、文档检索、会话记忆、工具调用与多步骤任务执行逐步完善。技术路线采用 PostgreSQL 持久化、Milvus 向量检索、Elasticsearch 全文检索和 Neo4j 知识图谱。

## 当前阶段

当前完成工程基础与后端依赖配置。业务模块仍在逐步整合，这个阶段尚不能启动完整应用。

后端模块路径为 `agi-assistant`，后续内部包继续沿用该路径。

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
