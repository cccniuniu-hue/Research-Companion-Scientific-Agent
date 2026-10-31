# 包依赖与装配

本说明使用当前目录，不沿用历史顶层 `agent/`、`infra/` 的路径和行数。

```text
cmd/server
  -> config
  -> interfaces/http/handler -> application services
  -> application/{chat,auth,skill,memoryprojection,memoryreconcile}
  -> infrastructure/{platform,persistence,projection,eventbus,llm,skillhub}
application/chat
  -> domain/{memory,promptctx,rag,runtime,task,document,tool,skill,sandbox}
  -> infrastructure adapters (constructor wiring and callbacks)
web/src/App.vue
  -> components + stores -> api/client.js + composables/useSSE.js
```

这是模块导航而非自动生成的完整 import 图；不要据此假设所有领域包都完全独立于配置/适配器。精确依赖应在仓库根目录通过 `go list -deps ./cmd/server`、`go list -json ./...` 与源文件 import 核对。循环依赖由 Go 编译器拒绝。

改动身份范围时追踪 HTTP/context/应用/仓储四层；改动记忆时同时检查事务、Outbox、缓存、投影和对账；改动流式接口时同时检查服务事件与前端 useSSE。测试通过范围和真实服务验证范围应分开记录。
