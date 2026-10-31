# Research Companion Web

Vue 3、Pinia、Vite 前端。Node 24/npm 11 是本次验证环境；依赖由 package-lock.json 固定。

## 开发与构建

先按根目录 README 配置并启动后端，再在本目录执行：

```sh
npm ci
npm run dev
```

Vite 的 `/api`、`/healthz`、`/readyz` 代理到本机 8090。生产构建使用 `npm run build`，输出为 `dist/`，不提交依赖目录或构建产物。`npm ci` 若提示未批准依赖脚本，不要全局放行；先查看具体包及构建结果。

## 同源部署

构建时保持 `VITE_API_BASE` 为空，静态文件放入 nginx 的 root，参考 [nginx.conf.example](nginx.conf.example)。示例只监听本机 8080，API 转发到本机 8090；关闭 SSE 代理缓冲，保留浏览器的 Authorization，但不注入管理令牌。修改 root 后应在部署机执行 `nginx -t` 再加载。

后端仍有开发用的宽松 CORS 默认值；同源代理隐藏其允许跨域响应头，后端端口应继续限制到 loopback。公网部署还需 TLS、来源与网络访问策略、请求限额和镜像审查。直接跨域部署前须在服务装配处配置准确的 CORS 白名单；仅设置 VITE_API_BASE 不会修改后端策略。

## 界面与权限

- 登录恢复后才激活该用户的本地会话；历史存储未加密，不构成同一浏览器内的用户隔离边界。
- 文档库和 RAG 是共享管理功能，需要 JWT 与服务端配置的 X-Admin-Token。普通用户看到权限提示；前端不保存或自动附加管理令牌。
- Skill 安装保存 Prompt 定义，不下载执行 GitHub 仓库。搜索框筛选已加载条目；开关只影响后续调用。
- SSE 完成、取消和中断分别显示；已发生的工具副作用无法撤回。
- 文档内容按纯文本显示，索引数量不表示跨存储事务成功。

`src/api` 负责请求，`composables/useSSE.js` 解析流，`stores` 管理 auth/sessions/chat/docs/skills，`components` 提供登录、导航、消息、输入、执行过程、文档查看与 Skill 弹窗，`utils` 负责 Markdown 和转义。

完整构建和实际模块/组件回归在本地验证；浏览器检查使用合成 API。真实后端及模型、数据库联调不在已验证范围内。最终验证结果见根目录 README。
