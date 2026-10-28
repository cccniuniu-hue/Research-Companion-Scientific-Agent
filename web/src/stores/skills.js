// Skill 由 JWT 用户管理；迟到响应不能重新填充已退出的账号状态。
import { defineStore } from 'pinia'
import { fetchJSON, getToken } from '../api/client'
function scope(store) {
  const revision = store.revision, token = getToken()
  return () => revision === store.revision && token === getToken()
}
export const useSkills = defineStore('skills', {
  state: () => ({ installed: [], featured: [], github: [], hubStatus: '', loadingMarket: false, error: '', revision: 0, installedRequest: 0, marketRequest: 0 }),
  getters: { enabledCount: s => s.installed.filter(x => x.enabled).length },
  actions: {
    async loadInstalled() {
      const current = scope(this), request = ++this.installedRequest
      try {
        const data = await fetchJSON('/api/skills/installed')
        if (!('skills' in data) || (data.skills !== null && !Array.isArray(data.skills))) throw new Error('无效 Skill 列表')
        if (!current() || request !== this.installedRequest) return false
        this.installed = data.skills || []; this.error = ''; return true
      } catch (e) { if (current() && request === this.installedRequest) { this.installed = []; this.error = e.message } return false }
    },
    async loadMarketplace() {
      const current = scope(this), request = ++this.marketRequest
      this.loadingMarket = true
      try {
        const data = await fetchJSON('/api/skills/marketplace')
        if (!Array.isArray(data.featured) || (data.github != null && !Array.isArray(data.github))) throw new Error('无效 Skill 广场数据')
        if (!current() || request !== this.marketRequest) return false
        this.featured = data.featured; this.github = data.hub_status === 'ok' ? (data.github || []) : []; this.hubStatus = data.hub_status || ''; this.error = ''; return true
      } catch (e) {
        if (current() && request === this.marketRequest) { this.featured = []; this.github = []; this.hubStatus = 'error'; this.error = e.message }
        return false
      } finally { if (current() && request === this.marketRequest) this.loadingMarket = false }
    },
    async mutate(action, payload) {
      const current = scope(this)
      try {
        const res = await fetchJSON('/api/skills/' + action, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
        if (!current()) return false
        if (!res.ok) throw new Error('Skill 操作未完成')
        return await this.loadInstalled()
      } catch (e) { if (current()) this.error = e.message; return false }
    },
    install(skillId) { return this.mutate('install', { skill_id: skillId }) },
    uninstall(skillId) { return this.mutate('uninstall', { skill_id: skillId }) },
    toggle(skillId, enabled) {
      if (typeof enabled !== 'boolean') { this.error = '必须明确指定 Skill 开关'; return false }
      return this.mutate('toggle', { skill_id: skillId, enabled })
    },
    reset() { this.revision++; this.installedRequest++; this.marketRequest++; this.installed = []; this.featured = []; this.github = []; this.hubStatus = ''; this.loadingMarket = false; this.error = '' },
  },
})
