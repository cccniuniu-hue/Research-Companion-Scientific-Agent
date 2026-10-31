# 文档与 PDF 入库

入口为管理权限保护的 `POST /api/upload`，支持 multipart 的 `file` 和 JSON 文本回退。服务端请求体上限 64 MiB，multipart 内存阈值 8 MiB；nginx 示例进一步限制为 32 MiB。上传者归属来自认证用户，客户端 author 不能充当用户 ACL。

1. `internal/domain/document/parser.go` 依据扩展名/MIME 选择 PDF 或普通文本。
2. PDF 优先尝试外部 pdfplumber、pdftotext，再尝试 Go PDF 解析；每个外部提取设置超时。容器模板只安装 pdftotext，不预装 pdfplumber。
3. 文本经过空白/断行规范化；空内容失败，缺少可提取文字时提供 needs_ocr 提示。没有 OCR 实现，也不保证复杂表格或图片内容恢复。
4. 应用层与 RAG 切分父/子块，保存 PostgreSQL 原文元数据，尝试向量、关键词和可选图谱写入。
5. 响应提供 doc_hash、parent_count、chunk_count、indexed_count 等结果；indexed_count 代表 PG 成功写入数量，不表示所有索引完成。

文档文件库位于服务工作目录 `.data/documents`。文档保存、RAG 写入、向量/全文索引、异步图谱没有统一事务；部分成功不会自动回滚，删除也可能留下需要对账的派生数据。缺少 PG 时可能返回零索引数；界面必须如实显示。

HTTP 对共享知识状态串行访问，核心 RAG 的直接并发调用需要额外同步。普通用户上传/查看共享文档返回权限错误；部署不能通过给所有浏览器附加同一管理令牌来绕过。

相关代码：`internal/interfaces/http/handler/handler.go`、`doc.go`，`internal/application/chat/`，`internal/domain/document/`、`internal/domain/rag/`、`internal/infrastructure/persistence/documentrepo/`、`ragchunk/`。复杂 PDF、真实索引及跨后端恢复尚未联调。
