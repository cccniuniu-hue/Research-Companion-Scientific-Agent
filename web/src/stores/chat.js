// 请求持有自己的流与消息；旧请求完成不能修改新请求的状态。
import { defineStore } from 'pinia'
import { apiFetch, fetchJSON, responseJSON, getToken } from '../api/client'
import { readSSE } from '../composables/useSSE'
import { useSessions } from './sessions'
export const useChat = defineStore('chat', {
  state: () => ({ loading: false, abort: null, activeAI: null, requestId: 0, ragOn: false }),
  getters: {
    modeHint(s) { return s.ragOn ? { text: '📚 知识库增强模式（需管理权限）', cls: 'hint-pink' } : { text: '💬 直接对话模式', cls: 'hint-muted' } },
  },
  actions: {
    toggleRag() { this.ragOn = !this.ragOn },
    abortInflight() {
      this.requestId++
      if (this.abort) this.abort.abort()
      if (this.activeAI) { this.activeAI.interrupted = true; this.activeAI.streaming = false }
      this.abort = null; this.activeAI = null; this.loading = false
    },
    // 关闭本次请求即可取消其后端 context；全局 cancel 属于管理操作。
    stop() { this.abortInflight() },
    handleEvent(ai, evt, data) {
      if (!data || typeof data !== 'object' || Array.isArray(data)) throw new Error('无效流式事件')
      switch (evt) {
        case 'route': ai.mode = data.mode || 'chat'; break
        case 'memory': ai.memory = data.extracted_info || ''; break
        case 'sandbox_ready': ai.sandbox = data.workspace || ''; break
        case 'step': ai.steps.push({ type: data.type, content: data.content || '', params: data.params || null }); break
        case 'tool_call': ai.toolCall = { tool_name: data.tool_name, params: data.params, tool_result: data.tool_result }; break
        case 'rag_result': ai.ragResults = data.search_results || []; break
        case 'token': ai.answer += (data.content || ''); break
        case 'error': throw new Error(data.error || '流式服务执行失败')
        case 'done':
          if (!ai.answer && data.answer) ai.answer = data.answer
          if (data.interrupted) ai.interrupted = true
          break
      }
    },
    async send(text) {
      const msg = (text || '').trim(), sess = useSessions()
      if (!msg || this.loading || !sess.userId || !getToken()) return
      if (!sess.currentId) sess.newSession()
      const sessionId = sess.currentId, owner = sess.userId, token = getToken()
      sess.addMessage(sessionId, { role: 'user', text: msg })
      const ai = sess.addMessage(sessionId, { role: 'ai', mode: 'chat', steps: [], memory: '', toolCall: null, ragResults: null, answer: '', error: '', interrupted: false, sandbox: '', streaming: true })
      if (!ai) return
      const id = ++this.requestId, controller = new AbortController()
      const current = () => this.requestId === id && sess.userId === owner && getToken() === token
      this.loading = true; this.abort = controller; this.activeAI = ai
      const body = JSON.stringify({ message: msg, use_rag: this.ragOn })
      const options = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body, signal: controller.signal }
      try {
        const resp = await apiFetch('/api/chat/stream', options)
        if (!current()) { await resp.body?.cancel(); return }
        // 仅在路由确实不存在时回落；权限、执行失败或断流不得重复发起任务。
        if ([404, 405].includes(resp.status)) {
          await resp.body?.cancel()
          const data = await fetchJSON('/api/chat', options)
          if (!current()) return
          ai.mode = data.mode || 'chat'; ai.memory = data.extracted_info || ''
          ai.steps = Array.isArray(data.steps) ? data.steps : []
          ai.toolCall = data.tool_call || null; ai.ragResults = data.search_results || null
          ai.answer = data.answer || ''; ai.interrupted = !!data.interrupted
        } else {
          if (!resp.ok) await responseJSON(resp)
          if (!resp.headers.get('Content-Type')?.includes('text/event-stream')) throw new Error('服务未返回 SSE')
          let completed = false
          await readSSE(resp, (evt, data) => {
            if (!current()) return
            this.handleEvent(ai, evt, data)
            if (evt === 'done') completed = true
          })
          if (current() && !completed) throw new Error('连接提前结束，回答可能不完整')
        }
      } catch (e) {
        if (current()) {
          ai.interrupted = true; ai.error = e.name === 'AbortError' ? '已中断' : e.message
          ai.answer += (ai.answer ? '\n\n' : '') + '⚠️ ' + (e.status === 401 ? '登录已过期，请重新登录' : ai.error)
        }
      } finally {
        ai.streaming = false
        if (current()) { this.loading = false; this.abort = null; this.activeAI = null }
        if (sess.userId === owner && getToken() === token) sess.save()
      }
    },
  },
})
