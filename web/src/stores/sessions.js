// 会话仅按服务端确认的 user_id 恢复；旧的无归属 ai_sessions 不自动迁移。
import { defineStore } from 'pinia'
const key = id => 'ai_sessions:' + id
function load(id) {
  try {
    const data = JSON.parse(localStorage.getItem(key(id)) || '[]')
    if (!Array.isArray(data)) return []
    return data.filter(s => s && typeof s.id === 'string' && Array.isArray(s.messages) && s.messages.every(m => m && ['user', 'ai'].includes(m.role)))
      .slice(0, 5).map(s => ({ ...s, messages: s.messages.map(m => m.role === 'ai' ? { ...m, steps: Array.isArray(m.steps) ? m.steps : [], answer: String(m.answer || ''), streaming: false } : m) }))
  } catch { return [] }
}
export const useSessions = defineStore('sessions', {
  state: () => ({ userId: '', sessions: [], currentId: null, error: '' }),
  getters: {
    current: s => s.sessions.find(x => x.id === s.currentId) || null,
    messages() { return this.current ? this.current.messages : [] },
  },
  actions: {
    setUser(id) {
      this.userId = String(id || ''); this.sessions = this.userId ? load(this.userId) : []
      this.currentId = this.sessions[0]?.id || null; this.error = ''
    },
    save() {
      if (!this.userId) return
      try { localStorage.setItem(key(this.userId), JSON.stringify(this.sessions)); this.error = '' }
      catch { this.error = '浏览器存储不可用，当前对话仅保留在内存中' }
    },
    newSession() {
      if (!this.userId) return null
      const id = crypto.randomUUID()
      this.sessions.unshift({ id, title: '新对话', messages: [], ts: Date.now() })
      this.sessions = this.sessions.slice(0, 5); this.currentId = id; this.save(); return id
    },
    switchSession(id) { if (this.sessions.some(s => s.id === id)) this.currentId = id },
    deleteSession(id) {
      this.sessions = this.sessions.filter(s => s.id !== id)
      if (this.currentId === id) this.currentId = this.sessions[0]?.id || null
      this.save()
    },
    addMessage(sessionId, msg) {
      const s = this.sessions.find(x => x.id === sessionId)
      if (!s) return null
      s.messages.push(msg)
      if (s.messages.filter(m => m.role === 'user').length === 1 && msg.role === 'user') s.title = (msg.text || '').slice(0, 20) || '新对话'
      this.save(); return s.messages[s.messages.length - 1]
    },
    reset() { this.userId = ''; this.sessions = []; this.currentId = null; this.error = '' },
  },
})
