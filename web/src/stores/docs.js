// 共享文档属于管理功能；403 和存储失败都保留明确的错误状态。
import { defineStore } from 'pinia'
import { fetchJSON, getToken } from '../api/client'
const viewer = () => ({ open: false, id: null, title: '', meta: '', content: '', ingesting: false })
function scope(store) {
  const revision = store.revision, token = getToken()
  return () => revision === store.revision && token === getToken()
}
export const useDocs = defineStore('docs', {
  state: () => ({ uploaded: [], library: [], viewer: viewer(), error: '', forbidden: false, revision: 0, viewerRequest: 0 }),
  actions: {
    failure(e) { this.error = e.message; this.forbidden = e.status === 403 },
    async uploadFile(file) {
      const current = scope(this), form = new FormData(); form.append('file', file)
      try {
        const res = await fetchJSON('/api/upload', { method: 'POST', body: form })
        if (!current()) return false
        const doc = { name: file.name, chunks: res.chunk_count, indexed: res.indexed_count || 0, parser: res.parser || '', pages: res.pages || 0, textChars: res.text_chars || 0, needsOCR: !!res.needs_ocr, docHash: res.doc_hash, ts: Date.now() }
        this.uploaded = this.uploaded.filter(d => d.name !== file.name); this.uploaded.unshift(doc)
        this.error = ''; this.forbidden = false; return true
      } catch (e) { if (current()) this.failure(e); throw e }
    },
    async deleteDoc(docHash) {
      const current = scope(this)
      try {
        const res = await fetchJSON('/api/docs/delete', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ doc_hash: docHash }) })
        if (!current()) return false
        if (!res.ok) throw new Error('删除未完成')
        this.uploaded = this.uploaded.filter(d => d.docHash !== docHash); this.error = ''; this.forbidden = false; return true
      } catch (e) { if (current()) this.failure(e); throw e }
    },
    async loadLibrary() {
      const current = scope(this)
      try {
        const res = await fetchJSON('/api/documents')
        if (!('documents' in res) || (res.documents !== null && !Array.isArray(res.documents))) throw new Error('无效文档列表')
        if (!current()) return false
        this.library = res.documents || []; this.error = ''; this.forbidden = false; return true
      } catch (e) { if (current()) { this.library = []; this.failure(e) } return false }
    },
    async openViewer(id) {
      const current = scope(this), request = ++this.viewerRequest
      this.viewer = viewer()
      try {
        const res = await fetchJSON('/api/documents/' + encodeURIComponent(id))
        if (!current() || request !== this.viewerRequest) return false
        const doc = res.document || {}, ver = res.version || {}, version = doc.latest_version || ver.version || 0
        this.viewer = { open: true, id: doc.id || null, title: doc.title || '本地文档', meta: `v${version} · ${doc.doc_type || 'document'} · ${doc.source || 'local'}`, content: ver.content_md || '（空文档）', ingesting: false }
        this.error = ''; this.forbidden = false; return true
      } catch (e) {
        if (current() && request === this.viewerRequest) { this.failure(e); this.viewer = { ...viewer(), open: true, title: '读取文档失败', content: e.message } }
        return false
      }
    },
    closeViewer() { this.viewerRequest++; this.viewer = viewer() },
    async ingest(id) {
      const current = scope(this)
      try {
        const res = await fetchJSON('/api/documents/' + encodeURIComponent(id) + '/ingest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
        if (current()) { this.error = ''; this.forbidden = false; await this.loadLibrary() }
        return res
      } catch (e) { if (current()) this.failure(e); throw e }
    },
    async ingestViewer() {
      const target = this.viewer, current = scope(this)
      if (!target.id || target.ingesting) return
      target.ingesting = true
      try {
        const res = await this.ingest(target.id)
        if (current() && this.viewer === target) target.meta += ` · 索引结果 ${res.indexed_count || 0}/${res.chunk_count || 0}`
      } catch (e) { if (current() && this.viewer === target) target.meta += ` · 入库失败：${e.message}` }
      finally { target.ingesting = false }
    },
    reset() { this.revision++; this.viewerRequest++; this.uploaded = []; this.library = []; this.viewer = viewer(); this.error = ''; this.forbidden = false },
  },
})
