<template>
  <div v-if="docs.viewer.open" class="doc-viewer-backdrop open" @click.self="docs.closeViewer()" @keydown="onKey">
    <section ref="dialog" class="doc-viewer" role="dialog" aria-modal="true" aria-labelledby="document-title" aria-describedby="document-permissions">
      <div class="doc-viewer-head">
        <div class="doc-viewer-title-wrap">
          <div class="doc-viewer-title" id="document-title">{{ docs.viewer.title }}</div>
          <div class="doc-viewer-meta" role="status">{{ docs.viewer.meta }}</div>
        </div>
        <div class="doc-viewer-actions">
          <button class="doc-viewer-action" type="button" :disabled="!docs.viewer.id || docs.viewer.ingesting || docs.forbidden" @click="docs.ingestViewer()">{{ docs.viewer.ingesting ? '入库中…' : '↻ 入库' }}</button>
          <button ref="closeButton" class="doc-viewer-close" type="button" aria-label="关闭文档" @click="docs.closeViewer()">×</button>
        </div>
      </div>
      <div class="doc-viewer-body">
        <p id="document-permissions" class="viewer-note">共享文档需管理权限。索引计数为零或部分完成，不表示全部入库。</p>
        <p v-if="docs.error" class="viewer-error" role="alert">{{ docs.error }}</p>
        <pre class="doc-viewer-content">{{ docs.viewer.content }}</pre>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, watch, nextTick, onBeforeUnmount } from 'vue'
import { useDocs } from '../stores/docs'

const docs = useDocs()
const dialog = ref(null)
const closeButton = ref(null)
let previousFocus
// 入库按钮禁用时浏览器会移走焦点，保留在仍可操作的关闭按钮上。
watch(() => docs.viewer.ingesting, async busy => {
  if (busy) { await nextTick(); if (docs.viewer.open) closeButton.value?.focus() }
})
watch(() => docs.viewer.open, async open => {
  if (open) previousFocus = typeof document === 'undefined' ? null : document.activeElement
  await nextTick()
  if (open && docs.viewer.open) closeButton.value?.focus()
  else if (!docs.viewer.open) previousFocus?.focus()
}, { immediate: true, flush: 'post' })
onBeforeUnmount(() => previousFocus?.focus())
function onKey(e) {
  if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); docs.closeViewer(); return }
  if (e.key !== 'Tab' || typeof document === 'undefined') return
  const buttons = [...dialog.value.querySelectorAll('button:not(:disabled)')]
  const first = buttons[0], last = buttons.at(-1)
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus() }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus() }
}
</script>

<style scoped>
.doc-viewer-meta, .viewer-note { color: var(--text2); font-size: 14px; line-height: 1.6; overflow-wrap: anywhere; }
.viewer-note, .viewer-error { margin-bottom: 12px; }
.viewer-error { color: #b91c1c; overflow-wrap: anywhere; }
.doc-viewer-content { font-size: 14px; }
.doc-viewer-head { flex-wrap: wrap; }
.doc-viewer { max-height: calc(100dvh - 32px); }
button:focus-visible { outline: 2px solid var(--primary-d); outline-offset: 2px; }
@media (max-width: 640px) {
  .doc-viewer-backdrop { padding: 12px; }
  .doc-viewer-body, .doc-viewer-head { padding: 14px; }
}
</style>
