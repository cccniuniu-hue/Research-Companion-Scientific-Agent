// api/client.js — 统一 fetch 封装：注入 Bearer token、401 回调、baseURL。
// 开发期 BASE 为空走 vite proxy；生产可用 VITE_API_BASE 直连后端。

const BASE = import.meta.env.VITE_API_BASE || ''

let unauthorizedHandler = null
export function setUnauthorizedHandler(fn) { unauthorizedHandler = fn }

export function getToken() { return localStorage.getItem('agi_auth_token') || '' }

export async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {})
  const { auth = true, ...request } = options
  const tok = auth ? getToken() : ''
  if (tok) headers.set('Authorization', 'Bearer ' + tok)
  const resp = await fetch(BASE + path, { ...request, headers })
  if (auth && resp.status === 401) {
    if (tok === getToken() && unauthorizedHandler) unauthorizedHandler()
    throw Object.assign(new Error('unauthorized'), { status: 401 })
  }
  return resp
}

// fetchJSON — 读文本再解析，兼容空 body / 非 JSON 错误体。
export async function fetchJSON(path, options) {
  const resp = await apiFetch(path, options)
  return responseJSON(resp)
}

export async function responseJSON(resp) {
  const raw = await resp.text()
  let data = {}
  if (raw) {
    try { data = JSON.parse(raw) } catch {
      if (resp.ok) throw new Error('服务返回无效 JSON')
    }
  }
  if (!resp.ok) {
    const message = resp.status === 403 ? '无权访问此功能，需要管理权限' : (data?.error || `HTTP ${resp.status}`)
    throw Object.assign(new Error(message), { status: resp.status })
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) throw new Error('服务返回无效 JSON 对象')
  return data
}
