<template>
  <div class="doc-viewer-backdrop open" @click.self="close" @keydown="onKey">
    <section ref="dialog" class="doc-viewer" role="dialog" aria-modal="true" aria-labelledby="skill-hub-title">
      <div class="doc-viewer-head">
        <div class="doc-viewer-title-wrap">
          <h2 id="skill-hub-title" class="doc-viewer-title">Skill 广场</h2>
          <p class="doc-viewer-meta">安装保存 Prompt 配置，不下载或执行 GitHub 仓库代码。开启后供后续对话选择，不撤销已开始的调用。</p>
        </div>
        <button ref="closeButton" class="doc-viewer-close" type="button" aria-label="关闭 Skill 广场" @click="close">×</button>
      </div>
      <div class="doc-viewer-body">
        <div class="skill-search">
          <label for="skill-query">筛选已加载的 Skill</label>
          <input id="skill-query" type="search" v-model="query" placeholder="名称或描述" />
          <button type="button" class="doc-viewer-action" :disabled="locked" @click="refresh">{{ refreshing ? '刷新中…' : '刷新列表' }}</button>
        </div>
        <p v-if="pageError || skills.error" class="skill-error" role="alert">{{ pageError || skills.error }}</p>
        <p v-if="pending" class="s-meta" role="status">正在确认操作结果…</p>
        <h3 class="skill-section-title">已安装</h3>
        <div class="skill-grid">
          <p v-if="refreshing" class="s-meta">正在读取安装状态…</p>
          <p v-else-if="!installed.length && !pageError && !skills.error" class="s-meta">{{ query ? '没有匹配的已安装 Skill。' : '还没有安装 Skill，可从下方选择。' }}</p>
          <div v-for="s in installed" :key="s.id" class="skill-card">
            <div class="s-name">{{ s.name }}</div>
            <div class="s-desc">{{ s.description || '' }}</div>
            <div class="s-foot">
              <label class="skill-switch">
                <input type="checkbox" :aria-label="'启用 ' + s.name" :checked="s.enabled" :disabled="locked" @change="toggle(s, $event)" />
                <span class="track"></span>
              </label>
              <span class="s-meta">{{ s.enabled ? '已启用' : '已关闭' }}</span>
              <button type="button" class="s-btn secondary" :disabled="locked" @click="mutate('uninstall', s.id)">卸载</button>
            </div>
          </div>
        </div>
        <div class="picker-divider"></div>
        <h3 class="skill-section-title">内置模板</h3>
        <div class="skill-grid">
          <p v-if="skills.loadingMarket" class="s-meta">加载中…</p>
          <p v-else-if="!featured.length && !pageError && !skills.error" class="s-meta">暂无匹配模板。</p>
          <div v-for="m in featured" :key="m.id" class="skill-card">
            <div class="s-name">{{ m.name }}</div>
            <div class="s-desc">{{ m.description || '' }}</div>
            <div class="s-foot">
              <span class="s-meta">内置 Prompt</span>
              <button type="button" class="s-btn" :disabled="locked || installedIds.has(m.id)" @click="mutate('install', m.id)">{{ installedIds.has(m.id) ? '已安装' : '安装' }}</button>
            </div>
          </div>
        </div>
        <div class="picker-divider"></div>
        <h3 class="skill-section-title">GitHub 检索结果</h3>
        <div class="skill-grid">
          <p v-if="skills.loadingMarket" class="s-meta">正在读取仓库描述…</p>
          <template v-else-if="skills.hubStatus === 'ok'">
            <p v-if="!github.length" class="s-meta">暂无匹配结果。</p>
            <div v-for="m in github" :key="m.id" class="skill-card">
              <div class="s-name">{{ m.name }}</div>
              <div class="s-desc">{{ m.description || '' }}</div>
              <div class="s-foot">
                <span class="s-meta">★ {{ m.stars || 0 }} <a v-if="sourceLink(m.source_url)" :href="sourceLink(m.source_url)" target="_blank" rel="noopener noreferrer">GitHub</a></span>
                <button type="button" class="s-btn" :disabled="locked || installedIds.has(m.id)" @click="mutate('install', m.id)">{{ installedIds.has(m.id) ? '已安装' : '安装' }}</button>
              </div>
            </div>
          </template>
          <p v-else-if="skills.hubStatus === 'disabled'" class="s-meta">GitHub 来源当前未启用，仍可使用内置模板。</p>
          <p v-else class="s-meta">GitHub 来源暂不可用，请稍后刷新。</p>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, computed, watch, nextTick, onMounted, onBeforeUnmount } from 'vue'
import { useSkills } from '../stores/skills'

const emit = defineEmits(['close'])
const skills = useSkills()
const query = ref(''), pageError = ref(''), pending = ref(''), refreshing = ref(false)
const dialog = ref(null), closeButton = ref(null)
const locked = computed(() => refreshing.value || !!pending.value)
const filtered = items => items.filter(m => [m.name, m.description, m.id].some(value => String(value || '').toLocaleLowerCase().includes(query.value.trim().toLocaleLowerCase())))
const installed = computed(() => filtered(skills.installed))
const featured = computed(() => filtered(skills.featured))
const github = computed(() => filtered(skills.github))
const installedIds = computed(() => new Set(skills.installed.map(s => s.id)))
let active = true, previousFocus
watch(locked, async busy => { if (busy) { await nextTick(); if (active) closeButton.value?.focus() } })
const scope = () => { const revision = skills.revision; return () => active && revision === skills.revision }
watch(() => skills.revision, () => { query.value = ''; pageError.value = ''; pending.value = ''; refreshing.value = false }, { flush: 'sync' })

async function refresh() {
  if (locked.value) return
  const current = scope(), errors = []
  refreshing.value = true; pageError.value = ''
  try {
    // 两种列表共享 store.error，逐项保留失败，避免后一项成功掩盖前一项错误。
    if (!await skills.loadInstalled()) errors.push(skills.error || '未能读取安装状态')
    if (!current()) return
    if (!await skills.loadMarketplace()) errors.push(skills.error || '未能读取 Skill 来源')
    if (current()) pageError.value = errors.join('；')
  } catch (error) { if (current()) pageError.value = error.message || '读取失败，请刷新重试' }
  finally { if (current()) refreshing.value = false }
}
async function mutate(action, id, enabled) {
  if (locked.value) return false
  const current = scope()
  pending.value = `${action}:${id}`; pageError.value = ''
  try {
    const ok = await skills[action](id, enabled)
    if (!current()) return false
    if (!ok) pageError.value = (skills.error || '操作结果未确认') + '；请刷新后核对安装状态。'
    return ok
  } catch (error) { if (current()) pageError.value = error.message || '操作结果未确认，请刷新核对'; return false }
  finally { if (current()) pending.value = '' }
}
async function toggle(skill, event) {
  const target = event.target, before = !!skill.enabled, enabled = target.checked, current = scope()
  target.checked = before
  await mutate('toggle', skill.id, enabled)
  if (current()) target.checked = skills.installed.find(s => s.id === skill.id)?.enabled ?? before
}
function sourceLink(value) {
  try { const url = new URL(value); return url.protocol === 'https:' && url.hostname === 'github.com' && !url.username && !url.password ? url.href : '' }
  catch { return '' }
}
function close() { active = false; emit('close') }
function onKey(e) {
  if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); close(); return }
  if (e.key !== 'Tab' || typeof document === 'undefined') return
  const fields = [...dialog.value.querySelectorAll('button:not(:disabled), input:not(:disabled), a[href]')]
  const first = fields[0], last = fields.at(-1)
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus() }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus() }
}
onMounted(() => {
  previousFocus = typeof document === 'undefined' ? null : document.activeElement
  closeButton.value?.focus()
  refresh()
})
onBeforeUnmount(() => { active = false; previousFocus?.focus() })
</script>

<style scoped>
.doc-viewer { max-height: calc(100dvh - 32px); }
.doc-viewer-meta, .s-meta { color: var(--text2); font-size: 14px; line-height: 1.6; }
.skill-card .s-desc, .skill-card .s-meta, .doc-viewer-body .skill-section-title { color: var(--text2); font-size: 14px; line-height: 1.6; }
.skill-error { color: #b91c1c; margin-bottom: 16px; overflow-wrap: anywhere; }
.skill-search { display: flex; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 16px; }
.skill-search input { flex: 1; min-width: 150px; padding: 8px 10px; border: 1px solid var(--border); border-radius: 8px; font: inherit; font-size: 16px; }
.skill-search input::placeholder { color: #64748b; }
.skill-card { min-width: 0; }
.s-name, .s-desc, .s-meta { overflow-wrap: anywhere; }
.skill-card .s-btn { background: var(--primary-d); font-size: 14px; min-height: 36px; }
.skill-card .s-btn.secondary { background: transparent; color: var(--text2); }
.s-foot { flex-wrap: wrap; }
.skill-switch:focus-within .track, button:focus-visible, a:focus-visible, input:focus-visible { outline: 2px solid var(--primary-d); outline-offset: 2px; }
.skill-switch input:disabled + .track { cursor: wait; opacity: .6; }
.skill-switch { flex-shrink: 0; }
.skill-switch input { position: absolute; inset: 0; width: 100%; height: 100%; margin: 0; z-index: 1; cursor: pointer; }
@media (max-width: 640px) {
  .doc-viewer-backdrop { padding: 12px; }
  .doc-viewer-body, .doc-viewer-head { padding: 14px; }
  .skill-grid { grid-template-columns: 1fr; }
}
</style>
