// 身份只由登录或 auth/me 的响应确认；持久化 token 不代表验证完成。
import { defineStore } from 'pinia'
import { fetchJSON, getToken } from '../api/client'

const TOKEN_KEY = 'agi_auth_token'
const USER_KEY = 'agi_auth_user'
function validIdentity(data) {
  return /^[1-9]\d*$/.test(String(data.user_id ?? '')) && typeof data.username === 'string' && !!data.username
}
export const useAuth = defineStore('auth', {
  state: () => ({ token: getToken(), username: '', userId: '', mode: 'login', error: '', overlay: true, submitting: false, generation: 0 }),
  getters: { loggedIn: s => !!s.token && !!s.userId },
  actions: {
    setMode(m) { this.mode = m; this.error = '' },
    showOverlay(msg) { this.overlay = true; if (msg) this.error = msg },
    hideOverlay() { this.overlay = false; this.error = '' },
    persist() {
      localStorage.setItem(TOKEN_KEY, this.token)
      localStorage.setItem(USER_KEY, this.username)
    },
    async restore() {
      const token = this.token, generation = ++this.generation
      if (!token) return false
      try {
        const data = await fetchJSON('/api/auth/me')
        if (generation !== this.generation || token !== this.token) return false
        if (!validIdentity(data)) throw new Error('服务返回无效身份')
        this.username = data.username
        this.userId = String(data.user_id)
        this.hideOverlay()
        return true
      } catch (e) {
        if (generation === this.generation) this.showOverlay(e.status === 401 ? '登录已过期' : '无法验证身份，请重新登录')
        return false
      }
    },
    async submit(username, password) {
      if (this.submitting) return false
      if (!username || !password) { this.error = '用户名和密码不能为空'; return false }
      const generation = ++this.generation
      this.submitting = true; this.error = ''
      try {
        const data = await fetchJSON(this.mode === 'login' ? '/api/auth/login' : '/api/auth/register', {
          auth: false, method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username, password }),
        })
        if (generation !== this.generation) return false
        if (!validIdentity(data) || typeof data.token !== 'string' || !data.token.trim()) throw new Error('服务返回无效身份')
        // 存储不可用时不能宣称成功，因为请求封装从同一存储读取 Bearer。
        try {
          localStorage.setItem(TOKEN_KEY, data.token)
          localStorage.setItem(USER_KEY, data.username)
        } catch { this.logout(); this.error = '浏览器无法保存登录状态'; return false }
        this.token = data.token; this.username = data.username; this.userId = String(data.user_id)
        this.hideOverlay()
        return true
      } catch (e) {
        if (generation === this.generation) this.error = e.message || '网络错误，请稍后再试'
        return false
      } finally { if (generation === this.generation) this.submitting = false }
    },
    logout() {
      this.generation++; this.token = ''; this.username = ''; this.userId = ''; this.submitting = false
      try { localStorage.removeItem(TOKEN_KEY); localStorage.removeItem(USER_KEY) } catch { /* 已清理内存身份。 */ }
      this.showOverlay()
    },
  },
})
