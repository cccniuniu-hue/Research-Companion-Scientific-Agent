<template>
  <button type="button" class="sidebar-toggle" aria-label="对话与文档菜单" aria-controls="conversation-sidebar" :aria-expanded="mobileOpen" @click="mobileOpen = !mobileOpen" @keydown.esc="mobileOpen = false">{{ mobileOpen ? '收起菜单' : '对话菜单' }}</button>
  <button v-if="mobileOpen" type="button" class="sidebar-backdrop" aria-label="关闭菜单" @click="mobileOpen = false"></button>
  <aside id="conversation-sidebar" class="sidebar" :class="{ 'mobile-open': mobileOpen }" @keydown.esc="mobileOpen = false">
    <div class="sidebar-logo">
      <div class="logo-mark">◈</div>
      <div>
        <div class="logo-name">Research Companion</div>
        <div class="logo-sub">研究与写作助手</div>
      </div>
    </div>

    <div class="sec-label">共享文档 · 管理功能</div>
    <p class="access-note">文档由服务端共享管理；当前页面未配置管理凭证。</p>
    <p v-if="actionError || docs.error" class="sidebar-error" role="alert">{{ actionError || docs.error }}</p>
    <div class="blackhole">
      <button type="button" class="upload-zone" :class="{ drag: dragging }" :disabled="blocked"
           @click="fileInput?.click()"
           @dragover.prevent="dragging = !blocked"
           @dragleave="dragging = false"
           @drop.prevent="onDrop">
        <div class="uz-icon">⬆</div>
        <div class="uz-text">上传共享文档</div>
        <div class="uz-sub">点击或拖拽到此处</div>
      </button>
      <input ref="fileInput" type="file" accept=".txt,.md,.pdf" multiple style="display:none" @change="onPick" />
      <div class="doc-list">
        <div v-if="!docs.uploaded.length && !docs.error" class="doc-empty">暂无上传记录</div>
        <div v-for="d in docs.uploaded.slice(0, 6)" :key="d.name" class="doc-item">
          <span class="doc-icon">{{ d.needsOCR ? '!' : '✓' }}</span>
          <span class="doc-name" :title="docTitle(d)">{{ d.name }}</span>
          <span class="doc-chunks">{{ d.needsOCR ? '需 OCR' : `${d.chunks || 0} 块${d.indexed ? '/' + d.indexed : ''}` }}</span>
          <button v-if="d.docHash" type="button" class="doc-del icon-button" :aria-label="'删除文档 ' + d.name" :disabled="blocked" @click="removeDoc(d)">×</button>
        </div>
      </div>
    </div>

    <!-- 本地文档库 -->
    <div class="sec-label sec-label-row">
      <span>本地文档库</span>
      <button class="mini-refresh" type="button" :disabled="busy || !auth.loggedIn" @click="run(() => docs.loadLibrary())">刷新</button>
    </div>
    <div class="blackhole library-panel">
      <div class="doc-list">
        <div v-if="!docs.library.length && !docs.error" class="doc-empty">暂无共享文档</div>
        <div v-for="d in docs.library.slice(0, 8)" :key="d.id" class="doc-item">
          <span class="doc-icon">§</span>
          <button type="button" class="doc-name text-button" :title="d.title || ''" :disabled="blocked" @click="docs.openViewer(d.id)">{{ d.title || '未命名文档' }}</button>
          <span class="doc-chunks">v{{ d.latest_version || 0 }}</span>
          <button type="button" class="doc-ingest icon-button" :aria-label="'重新索引 ' + (d.title || '文档')" :disabled="blocked" @click="run(() => docs.ingest(d.id))">↻</button>
        </div>
      </div>
    </div>

    <!-- 近期对话 -->
    <div class="sec-label">近期对话</div>
    <p v-if="sessions.error" class="sidebar-error" role="alert">{{ sessions.error }}</p>
    <div class="recent">
      <div v-if="!sessions.sessions.length" class="sess-empty">暂无对话记录</div>
      <div v-for="s in sessions.sessions" :key="s.id"
           class="session-item" :class="{ active: s.id === sessions.currentId }">
        <span class="sess-dot">▹</span>
        <button type="button" class="sess-info text-button" :aria-current="s.id === sessions.currentId ? 'true' : undefined" @click="switchTo(s.id)">
          <div class="sess-title">{{ s.title }}</div>
          <div class="sess-meta">{{ fmtDate(s.ts) }}</div>
        </button>
        <button type="button" class="sess-del icon-button" :aria-label="'删除对话 ' + s.title" @click="del(s.id)">✕</button>
      </div>
    </div>

    <button type="button" class="new-chat-btn" :disabled="!auth.loggedIn" @click="newChat">＋ 新建对话</button>

    <div v-if="auth.loggedIn" class="user-bar" style="display:flex">
      <span class="uname">{{ auth.username }}</span>
      <button class="logout-btn" type="button" @click="logout">登出</button>
    </div>
  </aside>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { useDocs } from '../stores/docs'
import { useSessions } from '../stores/sessions'
import { useAuth } from '../stores/auth'
import { useChat } from '../stores/chat'

const docs = useDocs()
const sessions = useSessions()
const auth = useAuth()
const chat = useChat()
const fileInput = ref(null)
const dragging = ref(false)
const mobileOpen = ref(false)
const busy = ref(false)
const actionError = ref('')
const blocked = computed(() => busy.value || docs.forbidden || !auth.loggedIn)
let revision = 0
watch(() => auth.userId, () => { revision++; actionError.value = ''; busy.value = false; dragging.value = false }, { flush: 'sync' })

async function run(operation) {
  if (busy.value) return
  const current = revision
  busy.value = true; actionError.value = ''
  try { await operation() }
  catch (error) { if (current === revision) actionError.value = error.message || '操作失败，请重试' }
  finally { if (current === revision) busy.value = false }
}
async function upload(files) {
  if (blocked.value) return
  const current = revision
  await run(async () => {
    for (const file of files) {
      if (current !== revision) break
      if (await docs.uploadFile(file) === false) break
    }
  })
}

async function onPick(e) { try { await upload(Array.from(e.target.files || [])) } finally { e.target.value = '' } }
async function onDrop(e) { dragging.value = false; await upload(Array.from(e.dataTransfer.files || [])) }
function docTitle(d) { return `${d.name}${d.pages ? ' · ' + d.pages + ' 页' : ''}${d.textChars ? ' · ' + d.textChars + ' 字' : ''}${d.parser ? ' · ' + d.parser : ''}` }
async function removeDoc(d) { if (!blocked.value && confirm('确定删除共享文档「' + d.name + '」？')) await run(() => docs.deleteDoc(d.docHash)) }
function fmtDate(ts) { return new Date(ts).toLocaleDateString('zh', { month: 'short', day: 'numeric' }) }
function switchTo(id) { chat.abortInflight(); sessions.switchSession(id); mobileOpen.value = false }
function del(id) { if (id === sessions.currentId) chat.abortInflight(); sessions.deleteSession(id) }
function newChat() { chat.abortInflight(); sessions.newSession(); mobileOpen.value = false }
function logout() { chat.abortInflight(); auth.logout(); mobileOpen.value = false }
</script>

<style scoped>
.upload-zone { width: 100%; font: inherit; color: inherit; }
.text-button, .icon-button { background: transparent; border: 0; font: inherit; color: inherit; cursor: pointer; text-align: left; }
.text-button { min-width: 0; padding: 0; }
.icon-button { flex-shrink: 0; }
.access-note, .sidebar-error { font-size: 14px; line-height: 1.6; padding: 0 14px; overflow-wrap: anywhere; }
.access-note { color: var(--text2); }
.sidebar-error { color: #b91c1c; }
button:disabled { opacity: .6; cursor: not-allowed; }
button:focus-visible { outline: 2px solid var(--primary-d); outline-offset: 2px; }
.session-item:focus-within .sess-del { opacity: 1; }
.logo-name { overflow-wrap: anywhere; }
.sidebar-toggle, .sidebar-backdrop { display: none; }
@media (max-width: 640px) {
  .sidebar-toggle { display: block; position: fixed; top: 10px; left: 10px; z-index: 70; min-height: 40px; padding: 0 10px; border: 1px solid var(--border); border-radius: 8px; background: var(--card-solid); color: var(--text); font: inherit; }
  .sidebar { position: fixed; inset: 0 auto 0 0; width: min(290px, calc(100vw - 48px)); min-width: 0; z-index: 60; padding-top: 56px; background: var(--card-solid); overflow-y: auto; transform: translateX(-100%); visibility: hidden; }
  .sidebar.mobile-open { transform: translateX(0); visibility: visible; }
  .sidebar-backdrop { display: block; position: fixed; inset: 0; z-index: 55; border: 0; background: rgba(15, 23, 42, .4); }
  .recent { min-height: 100px; }
}
</style>
